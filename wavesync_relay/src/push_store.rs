//! SQLite-backed storage for push notification tokens using sqlx.

use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, Row, SqlitePool};

/// Maximum distinct `(topic, token)` rows a single peer may register. Bounds the
/// persistent token store against an unauthenticated peer flooding registrations.
/// Generous enough for a real multi-group device (a handful of groups × platforms).
pub const MAX_TOKENS_PER_PEER: i64 = 32;

/// Per-token silent-push budget per UTC day. Kept at/under the platform ceiling
/// (APNs background pushes are throttled to only a few per device per day) so a
/// hostile or misbehaving notifier cannot burn a device's budget and silence
/// legitimate wakes. Bursts within a day are additionally coalesced by the
/// notifier's debounce; this is the hard sustained-rate cap.
pub const MAX_PUSHES_PER_TOKEN_PER_DAY: i64 = 5;

/// Failure modes of [`PushStore::register_token`].
#[derive(Debug)]
pub enum RegisterError {
    /// The registering peer already holds [`MAX_TOKENS_PER_PEER`] rows.
    TooManyRegistrations,
    /// Underlying storage error.
    Db(sqlx::Error),
}

impl From<sqlx::Error> for RegisterError {
    fn from(e: sqlx::Error) -> Self {
        RegisterError::Db(e)
    }
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegisterError::TooManyRegistrations => {
                write!(f, "too many token registrations for this peer")
            }
            RegisterError::Db(e) => write!(f, "{e}"),
        }
    }
}

/// A registered push token entry.
#[derive(Debug, Clone)]
pub struct PushToken {
    pub platform: String,
    pub token: String,
}

/// A row from the `push_retries` table — one pending retry per
/// `(topic, token)`. Drained by the retry worker once
/// `next_attempt_at <= now`.
#[derive(Debug, Clone)]
pub struct RetryRow {
    pub topic: String,
    pub token: String,
    pub platform: String,
    /// Latest peer addresses, decoded from the JSON column. Newer
    /// notifications for the same `(topic, token)` overwrite this via
    /// `enqueue_retry`'s UPSERT — older addresses are stale and worse
    /// than dropping.
    pub peer_addrs: Vec<String>,
    /// 1-based attempt count for the *next* retry. After the worker
    /// fires the dial, this is the attempt number passed to
    /// `compute_delay`.
    pub attempts: u32,
    /// Read in tests and used inside SQLite ordering; production code
    /// doesn't read this back into a variable but the column itself is
    /// load-bearing for `fetch_due_retries`.
    #[allow(dead_code)]
    pub next_attempt_at: i64,
    pub first_failed_at: i64,
}

/// Async wrapper around an sqlx SQLite pool for push token storage.
pub struct PushStore {
    pool: SqlitePool,
}

impl PushStore {
    /// Open (or create) the push token database at the given path.
    ///
    /// PRAGMAs and pool sizing target the upcoming retry-queue workload
    /// (commit B4/B5): WAL so retry-loop reads don't block fresh-send
    /// writes; 5s busy_timeout to survive burst contention; pool size 8
    /// (up from 4) since the retry queue ~doubles the concurrent reader
    /// count (peek_next_attempt_at + fetch_due_retries alongside the
    /// existing get_tokens_for_topic).
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        let url = if path == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite:{path}?mode=rwc")
        };

        // SqliteConnectOptions lets us set per-connection PRAGMAs that
        // SqlitePoolOptions::connect(url) doesn't expose. WAL is a no-op
        // on `:memory:` databases (sqlite returns the journal mode as
        // `memory`), so existing in-memory tests are unaffected.
        let connect_opts = SqliteConnectOptions::from_str(&url)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
            .create_if_missing(true)
            // sqlx logs every query at INFO by default. Drop to debug so
            // a chatty retry loop doesn't drown out actual signals.
            .log_statements(log::LevelFilter::Debug);

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(connect_opts)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS push_tokens (
                topic TEXT NOT NULL,
                platform TEXT NOT NULL,
                token TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                registered_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (topic, token)
            )",
        )
        .execute(&pool)
        .await?;

        // Retry queue. One row per (topic, token) — UPSERT replaces
        // peer_addrs and resets attempts when a fresh NotifyTopic
        // arrives for a topic that's already mid-retry. No FK to
        // push_tokens: explicit cleanup via `purge_retries_for_token`
        // is preferred (avoids `PRAGMA foreign_keys=ON` per-connection
        // footgun, allows logging on cascade events).
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS push_retries (
                topic            TEXT NOT NULL,
                token            TEXT NOT NULL,
                platform         TEXT NOT NULL,
                peer_addrs       TEXT NOT NULL,
                attempts         INTEGER NOT NULL,
                next_attempt_at  INTEGER NOT NULL,
                first_failed_at  INTEGER NOT NULL,
                last_error_kind  TEXT NOT NULL,
                last_error_code  INTEGER,
                last_error_body  TEXT,
                PRIMARY KEY (topic, token)
            )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_push_retries_next_attempt
             ON push_retries(next_attempt_at)",
        )
        .execute(&pool)
        .await?;

        // Per-token silent-push budget ledger. One row per token per UTC day,
        // holding the count of wakes fired that day. Enforces the platform push
        // budget (APNs throttles background pushes to only a few per device per
        // day) so a spammy or hostile notifier cannot exhaust it and silence
        // legitimate wakes for the whole group. Old rows are pruned lazily.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS push_budget (
                token   TEXT NOT NULL,
                day     INTEGER NOT NULL,
                count   INTEGER NOT NULL,
                PRIMARY KEY (token, day)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Register a push token for a topic.
    ///
    /// Rejected with `TooManyRegistrations` once a single peer already holds
    /// [`MAX_TOKENS_PER_PEER`] distinct `(topic, token)` rows — an unauthenticated
    /// peer must not be able to grow the persistent token store without bound.
    /// Re-registering an existing `(topic, token)` is always allowed (it refreshes
    /// `registered_at` / `peer_id` and does not add a row).
    pub async fn register_token(
        &self,
        topic: &str,
        platform: &str,
        token: &str,
        peer_id: &str,
    ) -> Result<(), RegisterError> {
        // Count this peer's existing rows, excluding the one being (re)written.
        let existing: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM push_tokens
             WHERE peer_id = ?1 AND NOT (topic = ?2 AND token = ?3)",
        )
        .bind(peer_id)
        .bind(topic)
        .bind(token)
        .fetch_one(&self.pool)
        .await?;
        if existing >= MAX_TOKENS_PER_PEER {
            return Err(RegisterError::TooManyRegistrations);
        }

        sqlx::query(
            "INSERT OR REPLACE INTO push_tokens (topic, platform, token, peer_id, registered_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))",
        )
        .bind(topic)
        .bind(platform)
        .bind(token)
        .bind(peer_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Whether `peer_id` currently holds a registered token for `topic`.
    ///
    /// The relay cannot verify group membership (it has no group key), but
    /// requiring a `NotifyTopic` sender to first hold a token for the topic forces
    /// an evictable, rate-limitable handle and blocks drive-by wake-spam from a
    /// peer that merely learned the topic string.
    pub async fn peer_registered_on_topic(
        &self,
        topic: &str,
        peer_id: &str,
    ) -> Result<bool, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM push_tokens WHERE topic = ?1 AND peer_id = ?2",
        )
        .bind(topic)
        .bind(peer_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(count > 0)
    }

    /// Atomically charge one wake against `token`'s daily budget.
    ///
    /// Returns `true` if the wake is within [`MAX_PUSHES_PER_TOKEN_PER_DAY`] and was
    /// counted, `false` if the token has already hit its budget for the current UTC
    /// day (caller must skip the send). `day` is `unix_secs / 86400`.
    pub async fn charge_daily_budget(&self, token: &str, day: i64) -> Result<bool, sqlx::Error> {
        // Upsert-increment, then read back the post-increment count in one txn so
        // concurrent notifiers can't both squeak past the cap.
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO push_budget (token, day, count) VALUES (?1, ?2, 1)
             ON CONFLICT(token, day) DO UPDATE SET count = count + 1",
        )
        .bind(token)
        .bind(day)
        .execute(&mut *tx)
        .await?;
        let count: i64 =
            sqlx::query_scalar("SELECT count FROM push_budget WHERE token = ?1 AND day = ?2")
                .bind(token)
                .bind(day)
                .fetch_one(&mut *tx)
                .await?;
        // Prune ledger rows older than yesterday to keep the table bounded.
        sqlx::query("DELETE FROM push_budget WHERE day < ?1")
            .bind(day - 1)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(count <= MAX_PUSHES_PER_TOKEN_PER_DAY)
    }

    /// Unregister a specific token from a topic.
    pub async fn unregister_token(&self, topic: &str, token: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM push_tokens WHERE topic = ?1 AND token = ?2")
            .bind(topic)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get tokens registered for a given topic.
    ///
    /// When `exclude_peer_id` is `Some`, tokens registered by that libp2p peer
    /// are skipped. A device uses the same peer id for its `RegisterToken` and
    /// its `NotifyTopic`, so passing the notifying peer here stops a local write
    /// from waking the device that made it (self-wake).
    pub async fn get_tokens_for_topic(
        &self,
        topic: &str,
        exclude_peer_id: Option<&str>,
    ) -> Result<Vec<PushToken>, sqlx::Error> {
        let rows =
            match exclude_peer_id {
                Some(peer) => sqlx::query(
                    "SELECT platform, token FROM push_tokens WHERE topic = ?1 AND peer_id <> ?2",
                )
                .bind(topic)
                .bind(peer)
                .fetch_all(&self.pool)
                .await?,
                None => {
                    sqlx::query("SELECT platform, token FROM push_tokens WHERE topic = ?1")
                        .bind(topic)
                        .fetch_all(&self.pool)
                        .await?
                }
            };

        Ok(rows
            .iter()
            .map(|row| PushToken {
                platform: row.get("platform"),
                token: row.get("token"),
            })
            .collect())
    }

    /// Remove a specific token across all topics (used when push provider reports invalid).
    pub async fn remove_token(&self, token: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_tokens WHERE token = ?1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    // ── Retry queue CRUD ───────────────────────────────────────────────

    /// Insert or update a retry row. UPSERT semantics: if a row for the
    /// same `(topic, token)` already exists, replace peer_addrs and
    /// last_error_*, and overwrite attempts/next_attempt_at with the
    /// supplied values. Caller controls whether this is a fresh
    /// enqueue (`attempts = 1`) or a re-arming after a retry attempt.
    ///
    /// `peer_addrs` is JSON-encoded by the caller to keep this method
    /// platform-agnostic (the relay never inspects the array contents).
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue_retry(
        &self,
        topic: &str,
        token: &str,
        platform: &str,
        peer_addrs_json: &str,
        attempts: u32,
        next_attempt_at: i64,
        first_failed_at: i64,
        last_error_kind: &str,
        last_error_code: Option<i64>,
        last_error_body: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO push_retries (
                topic, token, platform, peer_addrs,
                attempts, next_attempt_at, first_failed_at,
                last_error_kind, last_error_code, last_error_body
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             ON CONFLICT(topic, token) DO UPDATE SET
                platform = excluded.platform,
                peer_addrs = excluded.peer_addrs,
                attempts = excluded.attempts,
                next_attempt_at = excluded.next_attempt_at,
                first_failed_at = excluded.first_failed_at,
                last_error_kind = excluded.last_error_kind,
                last_error_code = excluded.last_error_code,
                last_error_body = excluded.last_error_body",
        )
        .bind(topic)
        .bind(token)
        .bind(platform)
        .bind(peer_addrs_json)
        .bind(attempts as i64)
        .bind(next_attempt_at)
        .bind(first_failed_at)
        .bind(last_error_kind)
        .bind(last_error_code)
        .bind(last_error_body)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Delete the retry row for the given `(topic, token)` pair. Called
    /// after a successful send and on permanent failure.
    pub async fn remove_retry(&self, topic: &str, token: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_retries WHERE topic = ?1 AND token = ?2")
            .bind(topic)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Fetch retry rows whose `next_attempt_at` is at or before `now`,
    /// soonest first. Caller passes a `limit` to bound memory use under
    /// burst conditions.
    pub async fn fetch_due_retries(
        &self,
        now: i64,
        limit: i64,
    ) -> Result<Vec<RetryRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT topic, token, platform, peer_addrs,
                    attempts, next_attempt_at, first_failed_at
             FROM push_retries
             WHERE next_attempt_at <= ?1
             ORDER BY next_attempt_at ASC
             LIMIT ?2",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let peer_addrs_json: String = row.get("peer_addrs");
                let peer_addrs: Vec<String> =
                    serde_json::from_str(&peer_addrs_json).unwrap_or_default();
                let attempts: i64 = row.get("attempts");
                RetryRow {
                    topic: row.get("topic"),
                    token: row.get("token"),
                    platform: row.get("platform"),
                    peer_addrs,
                    attempts: attempts as u32,
                    next_attempt_at: row.get("next_attempt_at"),
                    first_failed_at: row.get("first_failed_at"),
                }
            })
            .collect())
    }

    /// Soonest `next_attempt_at` across the whole queue, or `None` if
    /// the queue is empty. The retry worker uses this to size its
    /// `sleep_until` between batches.
    pub async fn peek_next_attempt_at(&self) -> Result<Option<i64>, sqlx::Error> {
        // `SELECT MIN(...)` on an empty table returns a single row with
        // the column value as NULL. Extract as `Option<i64>` so we
        // surface `None` rather than the column's default (0).
        let row = sqlx::query("SELECT MIN(next_attempt_at) AS soonest FROM push_retries")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| r.try_get::<Option<i64>, _>("soonest").ok().flatten()))
    }

    /// Delete all retry rows for a given token across all topics.
    /// Called after the provider reports the token invalid (410 /
    /// UNREGISTERED) and `remove_token` has run.
    pub async fn purge_retries_for_token(&self, token: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_retries WHERE token = ?1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Push any retry rows whose `next_attempt_at` is in the past to
    /// `now + grace + uniform(0, jitter_secs)`. Smears overdue rows
    /// across a window instead of firing them all at once on relay
    /// restart after a long outage. Returns the row count touched.
    pub async fn reschedule_overdue_retries(
        &self,
        now: i64,
        grace_secs: i64,
        jitter_secs: i64,
    ) -> Result<u64, sqlx::Error> {
        // SQLite's `RANDOM()` returns a signed 64-bit int; ABS + mod
        // gives a uniform distribution in [0, jitter_secs). When
        // jitter_secs is 0 the modulus is undefined, so guard.
        let result = if jitter_secs > 0 {
            sqlx::query(
                "UPDATE push_retries
                 SET next_attempt_at = ?1 + (ABS(RANDOM()) % ?2)
                 WHERE next_attempt_at < ?1",
            )
            .bind(now + grace_secs)
            .bind(jitter_secs)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                "UPDATE push_retries
                 SET next_attempt_at = ?1
                 WHERE next_attempt_at < ?1",
            )
            .bind(now + grace_secs)
            .execute(&self.pool)
            .await?
        };
        Ok(result.rows_affected())
    }

    /// Delete retry rows whose `first_failed_at` is older than
    /// `max_age_secs` ago. Hard wall-clock cap on retry lifetime —
    /// matches `crate::push_retry::MAX_AGE_SECS` (24 h).
    pub async fn purge_stale_retries(
        &self,
        now: i64,
        max_age_secs: i64,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_retries WHERE first_failed_at < ?1")
            .bind(now - max_age_secs)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Total number of currently registered push tokens, across all topics
    /// and platforms. Feeds the `relay_registered_tokens` gauge so it stays
    /// in sync with the persistent store after every register/unregister.
    pub async fn count_tokens(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COUNT(*) FROM push_tokens")
            .fetch_one(&self.pool)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem_store() -> PushStore {
        PushStore::open(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_registration_cap_per_peer() {
        let store = mem_store().await;
        // Fill the peer's quota with distinct tokens.
        for i in 0..MAX_TOKENS_PER_PEER {
            store
                .register_token("topic1", "Fcm", &format!("tok-{i}"), "peer-1")
                .await
                .unwrap();
        }
        // One more distinct token is rejected.
        let err = store
            .register_token("topic1", "Fcm", "tok-overflow", "peer-1")
            .await;
        assert!(matches!(err, Err(RegisterError::TooManyRegistrations)));
        // Re-registering an existing (topic, token) still works (no new row).
        store
            .register_token("topic1", "Fcm", "tok-0", "peer-1")
            .await
            .unwrap();
        // A different peer has its own quota.
        store
            .register_token("topic1", "Fcm", "other", "peer-2")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn count_tokens_counts() {
        let store = mem_store().await;
        assert_eq!(store.count_tokens().await.unwrap(), 0);
        store
            .register_token("topic1", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        assert_eq!(store.count_tokens().await.unwrap(), 1);
        store
            .register_token("topic2", "Apns", "tok2", "peer-2")
            .await
            .unwrap();
        assert_eq!(store.count_tokens().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_peer_registered_on_topic() {
        let store = mem_store().await;
        store
            .register_token("topicX", "Fcm", "tok", "peer-1")
            .await
            .unwrap();
        assert!(
            store
                .peer_registered_on_topic("topicX", "peer-1")
                .await
                .unwrap()
        );
        assert!(
            !store
                .peer_registered_on_topic("topicX", "peer-2")
                .await
                .unwrap()
        );
        assert!(
            !store
                .peer_registered_on_topic("topicY", "peer-1")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_daily_budget_cap() {
        let store = mem_store().await;
        let day = 20_000;
        // First MAX allowed, then over budget.
        for _ in 0..MAX_PUSHES_PER_TOKEN_PER_DAY {
            assert!(store.charge_daily_budget("tok", day).await.unwrap());
        }
        assert!(!store.charge_daily_budget("tok", day).await.unwrap());
        // A new day resets the budget.
        assert!(store.charge_daily_budget("tok", day + 1).await.unwrap());
        // A different token has its own budget.
        assert!(store.charge_daily_budget("tok2", day).await.unwrap());
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        store
            .register_token("topic1", "Apns", "token-b", "peer-2")
            .await
            .unwrap();
        store
            .register_token("topic2", "Fcm", "token-c", "peer-1")
            .await
            .unwrap();

        let tokens = store.get_tokens_for_topic("topic1", None).await.unwrap();
        assert_eq!(tokens.len(), 2);

        let tokens = store.get_tokens_for_topic("topic2", None).await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].token, "token-c");
    }

    #[tokio::test]
    async fn test_get_tokens_excluding_peer() {
        let store = mem_store().await;
        // Two devices on the same topic, plus the same writer also on another topic.
        store
            .register_token("topic1", "Fcm", "token-writer", "peer-writer")
            .await
            .unwrap();
        store
            .register_token("topic1", "Fcm", "token-other", "peer-other")
            .await
            .unwrap();

        // Excluding the writer's peer id drops only the writer's token.
        let tokens = store
            .get_tokens_for_topic("topic1", Some("peer-writer"))
            .await
            .unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].token, "token-other");

        // Excluding an unrelated peer keeps both.
        let tokens = store
            .get_tokens_for_topic("topic1", Some("peer-nobody"))
            .await
            .unwrap();
        assert_eq!(tokens.len(), 2);
    }

    #[tokio::test]
    async fn test_unregister() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        store.unregister_token("topic1", "token-a").await.unwrap();
        let tokens = store.get_tokens_for_topic("topic1", None).await.unwrap();
        assert!(tokens.is_empty());
    }

    #[tokio::test]
    async fn test_upsert_on_duplicate() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        // Re-register same (topic, token) with different peer — should upsert
        store
            .register_token("topic1", "Fcm", "token-a", "peer-2")
            .await
            .unwrap();
        let tokens = store.get_tokens_for_topic("topic1", None).await.unwrap();
        assert_eq!(tokens.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_token() {
        let store = mem_store().await;
        store
            .register_token("t1", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        store
            .register_token("t2", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        let removed = store.remove_token("tok1").await.unwrap();
        assert_eq!(removed, 2);
    }

    // ── Retry queue tests ─────────────────────────────────────────────

    async fn enqueue_simple(store: &PushStore, topic: &str, token: &str, next: i64) {
        store
            .enqueue_retry(
                topic,
                token,
                "Fcm",
                r#"["/ip4/1.2.3.4/udp/4001/quic-v1"]"#,
                1,
                next,
                next - 30,
                "http_status",
                Some(500),
                Some("server error"),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn retry_enqueue_then_fetch() {
        let store = mem_store().await;
        enqueue_simple(&store, "topic1", "token-a", 1000).await;
        let due = store.fetch_due_retries(1000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].topic, "topic1");
        assert_eq!(due[0].token, "token-a");
        assert_eq!(due[0].attempts, 1);
        assert_eq!(due[0].peer_addrs.len(), 1);
    }

    #[tokio::test]
    async fn retry_upsert_on_same_topic_token() {
        let store = mem_store().await;
        enqueue_simple(&store, "topic1", "token-a", 1000).await;
        // Second enqueue with attempts=3 should replace, not duplicate.
        store
            .enqueue_retry(
                "topic1",
                "token-a",
                "Fcm",
                r#"["/ip4/9.9.9.9/udp/4001/quic-v1"]"#,
                3,
                2000,
                500,
                "transport",
                None,
                None,
            )
            .await
            .unwrap();
        let due = store.fetch_due_retries(5000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].attempts, 3);
        assert_eq!(due[0].next_attempt_at, 2000);
        // Latest peer_addrs wins.
        assert_eq!(due[0].peer_addrs[0], "/ip4/9.9.9.9/udp/4001/quic-v1");
    }

    #[tokio::test]
    async fn fetch_due_filters_by_time_and_orders() {
        let store = mem_store().await;
        enqueue_simple(&store, "topic1", "tok-soon", 100).await;
        enqueue_simple(&store, "topic2", "tok-later", 500).await;
        enqueue_simple(&store, "topic3", "tok-soonest", 50).await;

        let due_at_200 = store.fetch_due_retries(200, 10).await.unwrap();
        // Only the two with next_attempt_at <= 200 are returned, sorted soonest-first.
        assert_eq!(due_at_200.len(), 2);
        assert_eq!(due_at_200[0].token, "tok-soonest");
        assert_eq!(due_at_200[1].token, "tok-soon");

        let due_at_1000 = store.fetch_due_retries(1000, 10).await.unwrap();
        assert_eq!(due_at_1000.len(), 3);
    }

    #[tokio::test]
    async fn fetch_due_respects_limit() {
        let store = mem_store().await;
        for i in 0..5 {
            enqueue_simple(&store, "topic1", &format!("tok-{i}"), 100).await;
        }
        let due = store.fetch_due_retries(1000, 2).await.unwrap();
        assert_eq!(due.len(), 2);
    }

    #[tokio::test]
    async fn peek_next_attempt_returns_min() {
        let store = mem_store().await;
        assert_eq!(store.peek_next_attempt_at().await.unwrap(), None);
        enqueue_simple(&store, "topic1", "tok-a", 500).await;
        enqueue_simple(&store, "topic2", "tok-b", 200).await;
        enqueue_simple(&store, "topic3", "tok-c", 800).await;
        assert_eq!(store.peek_next_attempt_at().await.unwrap(), Some(200));
    }

    #[tokio::test]
    async fn remove_retry_deletes_one_pair() {
        let store = mem_store().await;
        enqueue_simple(&store, "topic1", "tok-a", 100).await;
        enqueue_simple(&store, "topic1", "tok-b", 100).await;
        let removed = store.remove_retry("topic1", "tok-a").await.unwrap();
        assert_eq!(removed, 1);
        let due = store.fetch_due_retries(1000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].token, "tok-b");
    }

    #[tokio::test]
    async fn purge_retries_for_token_crosses_topics() {
        let store = mem_store().await;
        enqueue_simple(&store, "topic1", "tok-bad", 100).await;
        enqueue_simple(&store, "topic2", "tok-bad", 200).await;
        enqueue_simple(&store, "topic1", "tok-good", 100).await;
        let removed = store.purge_retries_for_token("tok-bad").await.unwrap();
        assert_eq!(removed, 2);
        let due = store.fetch_due_retries(1000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].token, "tok-good");
    }

    #[tokio::test]
    async fn reschedule_overdue_smears() {
        let store = mem_store().await;
        // 50 rows all overdue (next_attempt_at = 0).
        for i in 0..50 {
            enqueue_simple(&store, "topic1", &format!("tok-{i}"), 0).await;
        }
        let touched = store
            .reschedule_overdue_retries(1000, 30, 15)
            .await
            .unwrap();
        assert_eq!(touched, 50);

        // After: all rows are at >= 1030 (now + grace), < 1030 + 15 (jitter).
        let due = store.fetch_due_retries(10_000, 100).await.unwrap();
        for row in &due {
            assert!(
                row.next_attempt_at >= 1030 && row.next_attempt_at < 1045,
                "expected 1030..1045, got {}",
                row.next_attempt_at
            );
        }
        // Jitter actually spread the rows — they're not all on the
        // exact same second.
        let distinct: std::collections::HashSet<_> =
            due.iter().map(|r| r.next_attempt_at).collect();
        assert!(
            distinct.len() > 1,
            "expected jitter to spread 50 rows; all landed on the same second"
        );
    }

    #[tokio::test]
    async fn purge_stale_drops_old_rows() {
        let store = mem_store().await;
        // first_failed_at = next - 30 (per enqueue_simple)
        enqueue_simple(&store, "topic1", "old", 30).await; // first_failed_at = 0
        enqueue_simple(&store, "topic1", "new", 86_500).await; // first_failed_at = 86_470
        // now = 86_500, max_age = 86_400 → "old" (first_failed=0) is older than threshold.
        let removed = store.purge_stale_retries(86_500, 86_400).await.unwrap();
        assert_eq!(removed, 1);
        let due = store.fetch_due_retries(100_000, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].token, "new");
    }
}
