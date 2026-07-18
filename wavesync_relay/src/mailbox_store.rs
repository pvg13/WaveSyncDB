//! SQLite-backed durable store for the encrypted per-topic mailbox.
//!
//! Append-only log of client-sealed changesets, keyed by the (opaque, derived)
//! topic string. The relay assigns each topic's monotonic sequence numbers and
//! stores only ciphertext plus minimal routing metadata — it holds no group
//! key and cannot read entry contents. Sender PeerIds are deliberately NOT
//! persisted (metadata minimization); the append rate limiter is in-memory.
//!
//! Durability model — this store intentionally deviates from `push_store`'s
//! `synchronous=NORMAL`: an `Append` is acknowledged to the writer as "your
//! change is saved", so an acked entry must survive power loss, not just a
//! process crash. Under WAL + `synchronous=FULL` every commit fsyncs the WAL,
//! making commit == durable == ackable. Push tokens are reconstructable and
//! keep the cheaper setting; mailbox entries are not.
//!
//! Gap-detection contract (the client-facing invariant): `mailbox_topics`
//! rows are NEVER deleted, and each carries a random `epoch` minted at row
//! creation. `first_retained_seq` survives full GC (it advances to
//! `next_seq`), so a reader whose cursor predates the retained window — or
//! whose cursor belongs to a wiped/recreated store (epoch mismatch) — can
//! detect the loss and fall back to a full reconcile instead of silently
//! believing it is caught up.

use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, Row, SqlitePool};

/// Tunable limits, populated from CLI/env in `main` (defaults there).
#[derive(Debug, Clone)]
pub struct MailboxLimits {
    /// Entries older than this are GC'd.
    pub ttl: Duration,
    /// Max size of one sealed entry (nonce + ciphertext bytes).
    pub max_entry_bytes: u64,
    /// Per-topic entry-count cap; exceeding appends evict the oldest entries.
    pub max_topic_entries: u64,
    /// Per-topic byte cap; exceeding appends evict the oldest entries.
    pub max_topic_bytes: u64,
    /// Global byte cap across all topics; exceeding appends are rejected.
    pub max_total_bytes: u64,
}

/// Failure modes of [`MailboxStore::append`].
#[derive(Debug)]
pub enum AppendError {
    /// Entry exceeds `max_entry_bytes`.
    TooLarge,
    /// The global byte cap is exhausted.
    QuotaExceeded,
    /// Underlying storage error.
    Db(sqlx::Error),
}

impl From<sqlx::Error> for AppendError {
    fn from(e: sqlx::Error) -> Self {
        AppendError::Db(e)
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::TooLarge => write!(f, "entry exceeds per-entry size cap"),
            AppendError::QuotaExceeded => write!(f, "global mailbox storage quota exhausted"),
            AppendError::Db(e) => write!(f, "{e}"),
        }
    }
}

/// Result of a durably committed append.
#[derive(Debug, Clone, Copy)]
pub struct Appended {
    pub seq: u64,
    pub epoch: u64,
    /// Entries evicted from the head of this topic's log to stay under the
    /// per-topic caps (metrics; laggard readers detect this as a gap).
    pub evicted: u64,
}

/// One fetched entry.
#[derive(Debug, Clone)]
pub struct FetchedEntry {
    pub seq: u64,
    pub nonce: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

/// Result of a fetch: entries after the cursor plus the topic's log state.
#[derive(Debug, Clone)]
pub struct Fetched {
    pub entries: Vec<FetchedEntry>,
    pub latest_seq: u64,
    pub first_retained_seq: u64,
    pub epoch: u64,
    pub truncated: bool,
}

/// Async wrapper around an sqlx SQLite pool for the mailbox log.
pub struct MailboxStore {
    pool: SqlitePool,
    limits: MailboxLimits,
}

impl MailboxStore {
    /// Open (or create) the mailbox database at the given path.
    pub async fn open(path: &str, limits: MailboxLimits) -> Result<Self, sqlx::Error> {
        let url = if path == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite:{path}?mode=rwc")
        };

        // WAL + FULL: see module doc — commit must imply fsync so the append
        // ack can promise durability across power loss.
        let connect_opts = SqliteConnectOptions::from_str(&url)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5))
            .create_if_missing(true)
            .log_statements(log::LevelFilter::Debug);

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(connect_opts)
            .await?;

        // Per-topic log heads. Rows are never deleted: `epoch` +
        // `first_retained_seq` must outlive the entries themselves for
        // readers to distinguish "nothing new" from "you missed entries".
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mailbox_topics (
                topic               TEXT NOT NULL PRIMARY KEY,
                epoch               INTEGER NOT NULL,
                next_seq            INTEGER NOT NULL DEFAULT 1,
                first_retained_seq  INTEGER NOT NULL DEFAULT 1,
                entry_count         INTEGER NOT NULL DEFAULT 0,
                total_bytes         INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await?;

        // The log itself: ciphertext + nonce as opaque blobs. No sender
        // identity column — see module doc.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS mailbox_entries (
                topic        TEXT NOT NULL,
                seq          INTEGER NOT NULL,
                received_at  INTEGER NOT NULL,
                size         INTEGER NOT NULL,
                nonce        BLOB NOT NULL,
                ciphertext   BLOB NOT NULL,
                PRIMARY KEY (topic, seq)
            )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_mailbox_entries_received
             ON mailbox_entries(received_at)",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool, limits })
    }

    /// Append one sealed entry to `topic`'s log at `now_secs` (unix seconds).
    ///
    /// Runs in one `BEGIN IMMEDIATE` transaction (sqlx's `pool.begin()` is
    /// DEFERRED; a deferred read→write upgrade under WAL fails with
    /// SQLITE_BUSY instead of waiting on busy_timeout — same landmine the
    /// client dodges with its own immediate-begin helper). The returned ack
    /// means the entry is fsynced (WAL + synchronous=FULL).
    ///
    /// Per-topic caps evict the OLDEST entries rather than rejecting: new
    /// writes must always land (that is the durability promise), and evicted
    /// history is recoverable by laggards via the gap → reconcile fallback.
    /// The global byte cap rejects instead — it is the operator's disk
    /// protection, and eviction there would let one topic starve the rest.
    pub async fn append(
        &self,
        topic: &str,
        nonce: &[u8],
        ciphertext: &[u8],
        now_secs: i64,
    ) -> Result<Appended, AppendError> {
        let size = (nonce.len() + ciphertext.len()) as u64;
        if size > self.limits.max_entry_bytes {
            return Err(AppendError::TooLarge);
        }

        let mut conn = self.pool.acquire().await.map_err(AppendError::Db)?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(AppendError::Db)?;

        let result: Result<Appended, AppendError> = async {
            // Global cap first — reject before assigning a seq.
            let total: i64 =
                sqlx::query("SELECT COALESCE(SUM(total_bytes), 0) FROM mailbox_topics")
                    .fetch_one(&mut *conn)
                    .await?
                    .get(0);
            if total as u64 + size > self.limits.max_total_bytes {
                return Err(AppendError::QuotaExceeded);
            }

            let (epoch, seq) = get_or_create_topic_head(&mut conn, topic).await?;

            sqlx::query(
                "INSERT INTO mailbox_entries (topic, seq, received_at, size, nonce, ciphertext)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(topic)
            .bind(seq as i64)
            .bind(now_secs)
            .bind(size as i64)
            .bind(nonce)
            .bind(ciphertext)
            .execute(&mut *conn)
            .await?;

            sqlx::query(
                "UPDATE mailbox_topics
                 SET next_seq = next_seq + 1,
                     entry_count = entry_count + 1,
                     total_bytes = total_bytes + ?2
                 WHERE topic = ?1",
            )
            .bind(topic)
            .bind(size as i64)
            .execute(&mut *conn)
            .await?;

            let evicted = evict_over_caps(&mut conn, topic, &self.limits).await?;

            Ok(Appended {
                seq,
                epoch,
                evicted,
            })
        }
        .await;

        match result {
            Ok(appended) => {
                sqlx::query("COMMIT")
                    .execute(&mut *conn)
                    .await
                    .map_err(AppendError::Db)?;
                Ok(appended)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// Fetch entries with `seq > after_seq`, oldest first, bounded by
    /// `max_entries` and `max_bytes` of ciphertext. Reads run under WAL
    /// snapshot isolation — a fetch never observes a half-committed append.
    ///
    /// A fetch on a topic with no head row creates one (minting the epoch):
    /// a reader that drains before the first-ever append must still receive
    /// a stable epoch to store beside its cursor.
    pub async fn fetch(
        &self,
        topic: &str,
        after_seq: u64,
        max_entries: u32,
        max_bytes: u64,
    ) -> Result<Fetched, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;

        // Head lookup (and lazy creation — needs a write txn only then).
        let head = sqlx::query(
            "SELECT epoch, next_seq, first_retained_seq FROM mailbox_topics WHERE topic = ?1",
        )
        .bind(topic)
        .fetch_optional(&mut *conn)
        .await?;
        let (epoch, next_seq, first_retained_seq) = match head {
            Some(row) => {
                let epoch: i64 = row.get(0);
                let next_seq: i64 = row.get(1);
                let first: i64 = row.get(2);
                (epoch as u64, next_seq as u64, first as u64)
            }
            None => {
                sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
                let created = get_or_create_topic_head(&mut conn, topic).await;
                match created {
                    Ok((epoch, next_seq)) => {
                        sqlx::query("COMMIT").execute(&mut *conn).await?;
                        (epoch, next_seq, 1)
                    }
                    Err(e) => {
                        let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                        return Err(e);
                    }
                }
            }
        };

        // Over-fetch by one row to learn whether more remain past the caps.
        let rows = sqlx::query(
            "SELECT seq, nonce, ciphertext FROM mailbox_entries
             WHERE topic = ?1 AND seq > ?2
             ORDER BY seq ASC
             LIMIT ?3",
        )
        .bind(topic)
        .bind(after_seq as i64)
        .bind(max_entries as i64 + 1)
        .fetch_all(&mut *conn)
        .await?;

        let mut entries = Vec::new();
        let mut bytes: u64 = 0;
        let mut truncated = rows.len() > max_entries as usize;
        for row in rows.into_iter().take(max_entries as usize) {
            let seq: i64 = row.get(0);
            let nonce: Vec<u8> = row.get(1);
            let ciphertext: Vec<u8> = row.get(2);
            let entry_bytes = (nonce.len() + ciphertext.len()) as u64;
            // Always return at least one entry so a single over-budget entry
            // can't wedge the drain loop.
            if !entries.is_empty() && bytes + entry_bytes > max_bytes {
                truncated = true;
                break;
            }
            bytes += entry_bytes;
            entries.push(FetchedEntry {
                seq: seq as u64,
                nonce,
                ciphertext,
            });
        }

        Ok(Fetched {
            entries,
            latest_seq: next_seq.saturating_sub(1),
            first_retained_seq,
            epoch,
            truncated,
        })
    }

    /// Age out entries older than the TTL. Returns the number purged.
    /// Head rows survive with `first_retained_seq` advanced — that is what
    /// lets a long-offline reader detect the loss (gap → reconcile).
    pub async fn gc(&self, now_secs: i64) -> Result<u64, sqlx::Error> {
        let cutoff = now_secs - self.limits.ttl.as_secs() as i64;
        let mut conn = self.pool.acquire().await?;
        sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

        let result: Result<u64, sqlx::Error> = async {
            let purged = sqlx::query("DELETE FROM mailbox_entries WHERE received_at < ?1")
                .bind(cutoff)
                .execute(&mut *conn)
                .await?
                .rows_affected();

            if purged > 0 {
                reconcile_topic_heads(&mut conn).await?;
            }
            Ok(purged)
        }
        .await;

        match result {
            Ok(purged) => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
                Ok(purged)
            }
            Err(e) => {
                let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
                Err(e)
            }
        }
    }

    /// Current totals across all topics, for the metrics gauges.
    pub async fn stats(&self) -> Result<(u64, u64), sqlx::Error> {
        let row = sqlx::query(
            "SELECT COALESCE(SUM(entry_count), 0), COALESCE(SUM(total_bytes), 0)
             FROM mailbox_topics",
        )
        .fetch_one(&self.pool)
        .await?;
        let entries: i64 = row.get(0);
        let bytes: i64 = row.get(1);
        Ok((entries as u64, bytes as u64))
    }
}

/// Read the topic head, creating it (with a fresh random epoch) if absent.
/// Must run inside an IMMEDIATE transaction. Returns `(epoch, next_seq)`.
async fn get_or_create_topic_head(
    conn: &mut sqlx::SqliteConnection,
    topic: &str,
) -> Result<(u64, u64), sqlx::Error> {
    let row = sqlx::query("SELECT epoch, next_seq FROM mailbox_topics WHERE topic = ?1")
        .bind(topic)
        .fetch_optional(&mut *conn)
        .await?;
    if let Some(row) = row {
        let epoch: i64 = row.get(0);
        let next_seq: i64 = row.get(1);
        return Ok((epoch as u64, next_seq as u64));
    }
    // Random epoch, stored as the u64's i64 bit pattern. A wiped store mints
    // new epochs, which is exactly how clients detect the reset.
    let epoch: u64 = rand::random();
    sqlx::query("INSERT INTO mailbox_topics (topic, epoch) VALUES (?1, ?2)")
        .bind(topic)
        .bind(epoch as i64)
        .execute(&mut *conn)
        .await?;
    Ok((epoch, 1))
}

/// Evict oldest entries until `topic` is back under the per-topic caps.
/// Must run inside the append's IMMEDIATE transaction.
async fn evict_over_caps(
    conn: &mut sqlx::SqliteConnection,
    topic: &str,
    limits: &MailboxLimits,
) -> Result<u64, sqlx::Error> {
    let row = sqlx::query("SELECT entry_count, total_bytes FROM mailbox_topics WHERE topic = ?1")
        .bind(topic)
        .fetch_one(&mut *conn)
        .await?;
    let mut count: i64 = row.get(0);
    let mut bytes: i64 = row.get(1);

    let mut evicted = 0u64;
    while count as u64 > limits.max_topic_entries || bytes as u64 > limits.max_topic_bytes {
        let oldest = sqlx::query(
            "SELECT seq, size FROM mailbox_entries WHERE topic = ?1 ORDER BY seq ASC LIMIT 1",
        )
        .bind(topic)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(oldest) = oldest else { break };
        let seq: i64 = oldest.get(0);
        let size: i64 = oldest.get(1);

        sqlx::query("DELETE FROM mailbox_entries WHERE topic = ?1 AND seq = ?2")
            .bind(topic)
            .bind(seq)
            .execute(&mut *conn)
            .await?;
        count -= 1;
        bytes -= size;
        evicted += 1;

        sqlx::query(
            "UPDATE mailbox_topics
             SET entry_count = ?2, total_bytes = ?3, first_retained_seq = ?4
             WHERE topic = ?1",
        )
        .bind(topic)
        .bind(count)
        .bind(bytes)
        .bind(seq + 1)
        .execute(&mut *conn)
        .await?;
    }
    Ok(evicted)
}

/// After a GC delete, restore every head row's invariants from the surviving
/// entries: `first_retained_seq` = min surviving seq (or `next_seq` when the
/// log emptied), counters recomputed. Head rows are never deleted.
async fn reconcile_topic_heads(conn: &mut sqlx::SqliteConnection) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE mailbox_topics SET
             first_retained_seq = COALESCE(
                 (SELECT MIN(seq) FROM mailbox_entries e WHERE e.topic = mailbox_topics.topic),
                 next_seq),
             entry_count = (SELECT COUNT(*) FROM mailbox_entries e
                            WHERE e.topic = mailbox_topics.topic),
             total_bytes = COALESCE(
                 (SELECT SUM(size) FROM mailbox_entries e WHERE e.topic = mailbox_topics.topic),
                 0)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> MailboxLimits {
        MailboxLimits {
            ttl: Duration::from_secs(7 * 24 * 3600),
            max_entry_bytes: 4 * 1024 * 1024,
            max_topic_entries: 10_000,
            max_topic_bytes: 64 * 1024 * 1024,
            max_total_bytes: 1024 * 1024 * 1024,
        }
    }

    async fn mem_store() -> MailboxStore {
        MailboxStore::open(":memory:", limits()).await.unwrap()
    }

    async fn mem_store_with(limits: MailboxLimits) -> MailboxStore {
        MailboxStore::open(":memory:", limits).await.unwrap()
    }

    #[tokio::test]
    async fn append_assigns_monotonic_seqs_and_stable_epoch() {
        let store = mem_store().await;
        let a = store.append("t", &[1; 24], b"ct-1", 1000).await.unwrap();
        let b = store.append("t", &[2; 24], b"ct-2", 1001).await.unwrap();
        assert_eq!(a.seq, 1);
        assert_eq!(b.seq, 2);
        assert_eq!(a.epoch, b.epoch);

        // Independent topics get independent seqs and epochs.
        let other = store.append("u", &[3; 24], b"ct-3", 1002).await.unwrap();
        assert_eq!(other.seq, 1);
        assert_ne!(other.epoch, a.epoch);
    }

    #[tokio::test]
    async fn fetch_pages_in_order_with_truncation() {
        let store = mem_store().await;
        for i in 0..5u8 {
            store
                .append("t", &[i; 24], format!("ct-{i}").as_bytes(), 1000 + i as i64)
                .await
                .unwrap();
        }
        let first = store.fetch("t", 0, 2, u64::MAX).await.unwrap();
        assert_eq!(
            first.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(first.truncated);
        assert_eq!(first.latest_seq, 5);
        assert_eq!(first.first_retained_seq, 1);

        let rest = store.fetch("t", 2, 10, u64::MAX).await.unwrap();
        assert_eq!(
            rest.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert!(!rest.truncated);
        assert_eq!(rest.entries[0].ciphertext, b"ct-2".to_vec());
    }

    #[tokio::test]
    async fn fetch_byte_budget_truncates_but_never_wedges() {
        let store = mem_store().await;
        store
            .append("t", &[0; 24], &[0u8; 100], 1000)
            .await
            .unwrap();
        store
            .append("t", &[1; 24], &[1u8; 100], 1001)
            .await
            .unwrap();

        // Budget below one entry: still returns the first entry (no wedge).
        let r = store.fetch("t", 0, 10, 10).await.unwrap();
        assert_eq!(r.entries.len(), 1);
        assert!(r.truncated);
    }

    #[tokio::test]
    async fn fetch_before_first_append_mints_stable_epoch() {
        let store = mem_store().await;
        let a = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(a.latest_seq, 0);
        assert_eq!(a.first_retained_seq, 1);
        let b = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(a.epoch, b.epoch);
        // And the first append continues under the same epoch.
        let appended = store.append("t", &[0; 24], b"ct", 1000).await.unwrap();
        assert_eq!(appended.epoch, a.epoch);
    }

    #[tokio::test]
    async fn ttl_gc_purges_and_advances_first_retained() {
        let store = mem_store_with(MailboxLimits {
            ttl: Duration::from_secs(100),
            ..limits()
        })
        .await;
        store.append("t", &[0; 24], b"old-1", 1000).await.unwrap();
        store.append("t", &[1; 24], b"old-2", 1010).await.unwrap();
        store.append("t", &[2; 24], b"new-3", 1500).await.unwrap();

        let purged = store.gc(1550).await.unwrap();
        assert_eq!(purged, 2);

        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(r.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3]);
        assert_eq!(r.first_retained_seq, 3);
        assert_eq!(r.latest_seq, 3);
    }

    #[tokio::test]
    async fn full_gc_keeps_head_row_gap_detectable() {
        let store = mem_store_with(MailboxLimits {
            ttl: Duration::from_secs(100),
            ..limits()
        })
        .await;
        let appended = store.append("t", &[0; 24], b"ct", 1000).await.unwrap();
        let purged = store.gc(5000).await.unwrap();
        assert_eq!(purged, 1);

        // Log emptied, but the head survives: first_retained == next_seq,
        // epoch unchanged. A reader at cursor 0 sees 1 + 0 < 2 → gap.
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert!(r.entries.is_empty());
        assert_eq!(r.first_retained_seq, 2);
        assert_eq!(r.latest_seq, 1);
        assert_eq!(r.epoch, appended.epoch);
        let (entries, bytes) = store.stats().await.unwrap();
        assert_eq!(entries, 0);
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn per_topic_count_cap_evicts_oldest() {
        let store = mem_store_with(MailboxLimits {
            max_topic_entries: 3,
            ..limits()
        })
        .await;
        for i in 0..5u8 {
            store.append("t", &[i; 24], b"ct", 1000).await.unwrap();
        }
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(
            r.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
        assert_eq!(r.first_retained_seq, 3);
    }

    #[tokio::test]
    async fn per_topic_byte_cap_evicts_oldest() {
        let store = mem_store_with(MailboxLimits {
            max_topic_bytes: 300,
            ..limits()
        })
        .await;
        // Each entry is 24 (nonce) + 100 = 124 bytes; the third append
        // (372 total) must evict the first.
        for i in 0..3u8 {
            store.append("t", &[i; 24], &[i; 100], 1000).await.unwrap();
        }
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(
            r.entries.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(r.first_retained_seq, 2);
    }

    #[tokio::test]
    async fn per_entry_size_cap_rejects() {
        let store = mem_store_with(MailboxLimits {
            max_entry_bytes: 64,
            ..limits()
        })
        .await;
        let err = store.append("t", &[0; 24], &[0; 100], 1000).await;
        assert!(matches!(err, Err(AppendError::TooLarge)));
        // Nothing was assigned.
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(r.latest_seq, 0);
    }

    #[tokio::test]
    async fn global_byte_cap_rejects() {
        let store = mem_store_with(MailboxLimits {
            max_total_bytes: 200,
            ..limits()
        })
        .await;
        store.append("t", &[0; 24], &[0; 100], 1000).await.unwrap();
        let err = store.append("u", &[1; 24], &[1; 100], 1001).await;
        assert!(matches!(err, Err(AppendError::QuotaExceeded)));
    }

    #[tokio::test]
    async fn ack_implies_persisted_across_reopen() {
        let dir =
            std::env::temp_dir().join(format!("wavesync-mailbox-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mailbox.db");
        let path_str = path.to_str().unwrap();

        let appended = {
            let store = MailboxStore::open(path_str, limits()).await.unwrap();
            store.append("t", &[7; 24], b"durable", 1000).await.unwrap()
        };

        // Fresh pool over the same file: the acked entry and the epoch must
        // both be there.
        let store = MailboxStore::open(path_str, limits()).await.unwrap();
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_eq!(r.entries.len(), 1);
        assert_eq!(r.entries[0].ciphertext, b"durable".to_vec());
        assert_eq!(r.epoch, appended.epoch);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn recreated_store_mints_new_epoch() {
        let dir =
            std::env::temp_dir().join(format!("wavesync-mailbox-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mailbox.db");
        let path_str = path.to_str().unwrap();

        let first = {
            let store = MailboxStore::open(path_str, limits()).await.unwrap();
            store.append("t", &[0; 24], b"ct", 1000).await.unwrap()
        };

        // Operator wipes the store: clients must be able to detect this.
        std::fs::remove_file(&path).unwrap();
        let store = MailboxStore::open(path_str, limits()).await.unwrap();
        let r = store.fetch("t", 0, 10, u64::MAX).await.unwrap();
        assert_ne!(r.epoch, first.epoch);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
