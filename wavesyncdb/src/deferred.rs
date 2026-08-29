//! Quarantine for column changes dropped because the column isn't registered.
//!
//! Closes [issue #117].
//!
//! ## Why this exists
//!
//! The apply path rejects a `ColumnChange` whose `cid` is not in the table's
//! registered column list (`sync_handler::apply_remote_column_changes`). That
//! check is load-bearing security — the column id arrives unauthenticated and
//! is interpolated into raw SQL downstream (WSDB-PoC-1) — but it also fires for
//! an entirely benign case: a **staged rollout**, where one device is on a build
//! that added a column and the other isn't there yet.
//!
//! The drop is silent and permanent. `shadow::increment_db_version` has already
//! run for the chunk by the time the cell is examined, so the receiver acks past
//! the sender's watermark; the sender re-derives with `WHERE s.db_version > $1`
//! and never offers the cell again — **not even after the receiver is upgraded
//! and its schema gains the column.** The row looks complete with one field
//! permanently wrong, and only a full resync recovers it.
//!
//! ## Why quarantine rather than re-request
//!
//! The two other obvious fixes both fail on the same fact: an unknown column is
//! indistinguishable from a hostile injected one. "Don't advance the clock until
//! it applies" and "reject the whole row" each let any peer in the group pin a
//! receiver into a permanent re-request loop with one bogus column name.
//!
//! We don't need to re-request anything — **the value is already in hand at drop
//! time.** Keeping the bytes locally needs no new message type, no `ColumnChange`
//! field, and no protocol-identifier bump, so this is not a mixed-fleet hazard.
//!
//! ## Shape
//!
//! * One row per `(tbl, pk, cid)`. A second drop for the same cell keeps
//!   whichever change wins [`conflict::should_apply_column`] — the same total
//!   order the real apply path uses, so replicas that quarantined the same cells
//!   replay identical values (Rule 2.6 is preserved through the quarantine).
//! * The `ColumnChange` is stored as serde JSON, verbatim. Future wire fields
//!   survive a round-trip through the quarantine without a migration here.
//! * Capped at [`MAX_DEFERRED_CHANGES`] rows, evicting oldest-inserted first.
//!   **No time-based expiry**: a staged rollout can take weeks, and an expiry
//!   would silently reintroduce the exact bug this module closes.
//! * Only *columns* are quarantined. An unregistered **table** stays dropped on
//!   the push / version-vector paths, and the mailbox path already defers those
//!   entries without consuming them (`mailbox_manager`, #104) — quarantining
//!   them here would mean holding data for tables the app may never own.
//!
//! The `_wavesync` prefix keeps this table out of change capture
//! (`capture::trigger_sql` refuses it) and out of the drain planner
//! (`capture::plan_logical_ops`) — Rule 2.2, inherited, not re-implemented.
//!
//! [issue #117]: https://github.com/pvg13/WaveSyncDB/issues/117

use crate::conflict;
use crate::messages::ColumnChange;
use sea_orm::{ConnectionTrait, DbErr, FromQueryResult, Statement};

pub(crate) const DEFERRED_TABLE: &str = "_wavesync_deferred_changes";

/// Hard cap on quarantined cells, across every table.
///
/// A rollout touching more cells than this on one device is not a rollout we
/// can serve from memory of dropped bytes; past the cap the oldest entries are
/// evicted and those cells revert to the pre-fix behaviour (lost until a full
/// resync). Sized to be far above any plausible staged-rollout backlog while
/// still bounding a misbehaving peer that streams junk column names.
pub(crate) const MAX_DEFERRED_CHANGES: i64 = 10_000;

/// Create the quarantine table (idempotent).
///
/// Called from `capture::ensure_capture_tables` so it exists everywhere the
/// other internal tables do.
pub(crate) async fn ensure_deferred_table(db: &impl ConnectionTrait) -> Result<(), DbErr> {
    // AUTOINCREMENT, as in the capture table: eviction is "lowest id first", and
    // rowid reuse after a delete could place a fresh row below an older one and
    // make eviction discard the wrong entry.
    db.execute_unprepared(&format!(
        "CREATE TABLE IF NOT EXISTS {DEFERRED_TABLE} (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            tbl       TEXT NOT NULL,
            pk        TEXT NOT NULL,
            cid       TEXT NOT NULL,
            change    TEXT NOT NULL,
            stored_at INTEGER NOT NULL,
            UNIQUE(tbl, pk, cid)
        )"
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE INDEX IF NOT EXISTS {DEFERRED_TABLE}_tbl ON {DEFERRED_TABLE}(tbl)"
    ))
    .await?;
    Ok(())
}

#[derive(Debug, FromQueryResult)]
struct StoredChange {
    id: i64,
    change: String,
}

#[derive(Debug, FromQueryResult)]
struct CountRow {
    n: i64,
}

/// Serialized value bytes for the conflict comparator.
///
/// Matches `apply_remote_column_changes`' `serde_json::to_vec(&change.val)` —
/// the comparator's tiebreak is byte-order sensitive, so the two sites must
/// serialize the same thing (the `Option`, not its contents).
fn val_bytes(change: &ColumnChange) -> Vec<u8> {
    serde_json::to_vec(&change.val).unwrap_or_default()
}

/// Store a change whose column isn't registered yet, to be replayed once it is.
///
/// MUST be called inside the apply transaction, so the quarantine write and the
/// `db_version` advance that strands the change commit together — a crash
/// between them would lose the cell exactly as before the fix.
///
/// Keeps the comparator winner when the cell is already quarantined; a losing
/// change is dropped without touching the stored row.
pub(crate) async fn quarantine(
    db: &impl ConnectionTrait,
    change: &ColumnChange,
    now_secs: u64,
) -> Result<(), DbErr> {
    let existing = StoredChange::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        format!("SELECT id, change FROM {DEFERRED_TABLE} WHERE tbl = $1 AND pk = $2 AND cid = $3"),
        [
            change.table.0.as_str().into(),
            change.pk.0.as_str().into(),
            change.cid.0.as_str().into(),
        ],
    ))
    .one(db)
    .await?;

    if let Some(row) = existing {
        // A stored entry that no longer deserializes (hand-edited DB, or a
        // downgrade that wrote a shape we can't read) is treated as absent and
        // overwritten — keeping an unreadable row would strand the cell forever,
        // which is the bug we're fixing.
        match serde_json::from_str::<ColumnChange>(&row.change) {
            Ok(stored) => {
                if !conflict::should_apply_column(
                    change.col_version,
                    &val_bytes(change),
                    &change.site_id,
                    stored.col_version,
                    &val_bytes(&stored),
                    &stored.site_id,
                ) {
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::warn!(
                    table = %change.table.0,
                    pk = %change.pk.0,
                    cid = %change.cid.0,
                    "quarantined change is unreadable, replacing: {e}"
                );
            }
        }
    }

    let encoded = serde_json::to_string(change)
        .map_err(|e| DbErr::Custom(format!("failed to serialize deferred column change: {e}")))?;

    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        format!(
            "INSERT INTO {DEFERRED_TABLE} (tbl, pk, cid, change, stored_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT(tbl, pk, cid) DO UPDATE SET
                change    = excluded.change,
                stored_at = excluded.stored_at"
        ),
        [
            change.table.0.as_str().into(),
            change.pk.0.as_str().into(),
            change.cid.0.as_str().into(),
            encoded.into(),
            (now_secs as i64).into(),
        ],
    ))
    .await?;

    enforce_cap(db).await
}

/// Evict oldest-inserted rows until the table is back within
/// [`MAX_DEFERRED_CHANGES`].
async fn enforce_cap(db: &impl ConnectionTrait) -> Result<(), DbErr> {
    let count = CountRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        format!("SELECT COUNT(*) AS n FROM {DEFERRED_TABLE}"),
        Vec::<sea_orm::Value>::new(),
    ))
    .one(db)
    .await?
    .map(|r| r.n)
    .unwrap_or(0);

    let excess = count - MAX_DEFERRED_CHANGES;
    if excess <= 0 {
        return Ok(());
    }

    let res = db
        .execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            format!(
                "DELETE FROM {DEFERRED_TABLE} WHERE id IN
                    (SELECT id FROM {DEFERRED_TABLE} ORDER BY id ASC LIMIT $1)"
            ),
            [excess.into()],
        ))
        .await?;

    // Loud on purpose: past the cap, evicted cells are lost until a full resync,
    // which is the pre-fix behaviour this module exists to prevent.
    tracing::warn!(
        evicted = res.rows_affected(),
        cap = MAX_DEFERRED_CHANGES,
        "deferred-change quarantine is full; evicted oldest entries"
    );
    Ok(())
}

/// Load quarantined changes for `table` whose column is now in `columns`.
///
/// Returns `(row id, change)` pairs ordered by `col_version` then insertion, so
/// a replay applies older changes to a cell before newer ones. Rows whose column
/// is still unregistered are left in place; rows that fail to deserialize are
/// reported so the caller can purge them rather than retry forever.
pub(crate) async fn load_replayable(
    db: &impl ConnectionTrait,
    table: &str,
    columns: &[String],
) -> Result<(Vec<(i64, ColumnChange)>, Vec<i64>), DbErr> {
    let rows = StoredChange::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        format!("SELECT id, change FROM {DEFERRED_TABLE} WHERE tbl = $1 ORDER BY id ASC"),
        [table.into()],
    ))
    .all(db)
    .await?;

    let mut replayable = Vec::new();
    let mut undecodable = Vec::new();
    for row in rows {
        match serde_json::from_str::<ColumnChange>(&row.change) {
            Ok(change) => {
                if columns.iter().any(|c| c == &change.cid.0) {
                    replayable.push((row.id, change));
                }
            }
            Err(e) => {
                tracing::warn!(
                    table = %table,
                    id = row.id,
                    "dropping undecodable quarantined change: {e}"
                );
                undecodable.push(row.id);
            }
        }
    }

    replayable.sort_by_key(|(id, c)| (c.col_version, *id));
    Ok((replayable, undecodable))
}

/// Delete quarantined rows by id.
///
/// Call ONLY after the replay's `apply_remote_changeset` returned
/// `committed == true`. A changeset that was applied-but-rolled-back is not
/// delivered, and purging on anything weaker turns this consume-once store into
/// the same silent data loss it exists to prevent (the cursor-vs-commit rule
/// that produced #104 and #84).
pub(crate) async fn purge(db: &impl ConnectionTrait, ids: &[i64]) -> Result<(), DbErr> {
    if ids.is_empty() {
        return Ok(());
    }
    let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("${i}")).collect();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        format!(
            "DELETE FROM {DEFERRED_TABLE} WHERE id IN ({})",
            placeholders.join(", ")
        ),
        ids.iter().map(|id| (*id).into()).collect::<Vec<_>>(),
    ))
    .await?;
    Ok(())
}

/// Number of quarantined changes for `table`. Unit-test helper; integration
/// tests query `_wavesync_deferred_changes` directly (this is crate-private).
#[cfg(test)]
pub(crate) async fn count_for_table(db: &impl ConnectionTrait, table: &str) -> Result<i64, DbErr> {
    Ok(CountRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        &format!("SELECT COUNT(*) AS n FROM {DEFERRED_TABLE} WHERE tbl = $1"),
        [table.into()],
    ))
    .one(db)
    .await?
    .map(|r| r.n)
    .unwrap_or(0))
}

pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ColumnName, NodeId, PrimaryKey, TableName};
    use sea_orm::{Database, DatabaseConnection};

    const SITE_A: NodeId = NodeId([1u8; 16]);
    const SITE_B: NodeId = NodeId([2u8; 16]);

    // Single-connection in-memory DB, as in `shadow.rs`'s unit tests. The
    // file-based rule in CLAUDE.md §6 exists for MULTI-connection tests, where
    // SeaORM's pool would hand each connection its own empty in-memory database;
    // nothing here opens a second connection.
    async fn test_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        ensure_deferred_table(&db).await.unwrap();
        db
    }

    fn change(cid: &str, col_version: u64, val: &str, site: NodeId) -> ColumnChange {
        ColumnChange {
            table: TableName("tasks".into()),
            pk: PrimaryKey("row1".into()),
            cid: ColumnName(cid.into()),
            val: Some(serde_json::Value::String(val.into())),
            site_id: site,
            col_version,
            cl: 0,
            seq: 0,
            db_version: 1,
            deleted_ts: None,
        }
    }

    #[tokio::test]
    async fn stores_and_replays_a_dropped_column() {
        let db = test_db().await;
        quarantine(&db, &change("outcome", 1, "skipped", SITE_A), 100)
            .await
            .unwrap();

        // Still unregistered: nothing to replay.
        let (replay, _) = load_replayable(&db, "tasks", &["title".into()])
            .await
            .unwrap();
        assert!(replay.is_empty());

        // Column now registered: the change comes back.
        let (replay, _) = load_replayable(&db, "tasks", &["title".into(), "outcome".into()])
            .await
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].1.val, Some(serde_json::json!("skipped")));

        purge(&db, &[replay[0].0]).await.unwrap();
        assert_eq!(count_for_table(&db, "tasks").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn dedupes_to_one_row_per_cell_keeping_the_comparator_winner() {
        let db = test_db().await;
        quarantine(&db, &change("outcome", 5, "old", SITE_A), 100)
            .await
            .unwrap();
        // Higher col_version wins.
        quarantine(&db, &change("outcome", 9, "new", SITE_A), 101)
            .await
            .unwrap();
        // Lower col_version must NOT overwrite the winner.
        quarantine(&db, &change("outcome", 2, "stale", SITE_B), 102)
            .await
            .unwrap();

        assert_eq!(count_for_table(&db, "tasks").await.unwrap(), 1);
        let (replay, _) = load_replayable(&db, "tasks", &["outcome".into()])
            .await
            .unwrap();
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].1.col_version, 9);
        assert_eq!(replay[0].1.val, Some(serde_json::json!("new")));
    }

    #[tokio::test]
    async fn distinct_cells_get_distinct_rows() {
        let db = test_db().await;
        quarantine(&db, &change("outcome", 1, "a", SITE_A), 100)
            .await
            .unwrap();
        let mut other = change("notes", 1, "b", SITE_A);
        other.pk = PrimaryKey("row2".into());
        quarantine(&db, &other, 100).await.unwrap();
        assert_eq!(count_for_table(&db, "tasks").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn replay_order_is_by_col_version() {
        let db = test_db().await;
        for (pk, cv) in [("r3", 7u64), ("r1", 2), ("r2", 5)] {
            let mut c = change("outcome", cv, "v", SITE_A);
            c.pk = PrimaryKey(pk.into());
            quarantine(&db, &c, 100).await.unwrap();
        }
        let (replay, _) = load_replayable(&db, "tasks", &["outcome".into()])
            .await
            .unwrap();
        let versions: Vec<u64> = replay.iter().map(|(_, c)| c.col_version).collect();
        assert_eq!(versions, vec![2, 5, 7]);
    }

    #[tokio::test]
    async fn cap_evicts_oldest_first() {
        let db = test_db().await;
        // Seed one over the cap without going through `quarantine` for every row
        // (10k round-trips); insert directly, then let one real call trim.
        for i in 0..=MAX_DEFERRED_CHANGES {
            let mut c = change("outcome", 1, "v", SITE_A);
            c.pk = PrimaryKey(format!("row{i}"));
            let encoded = serde_json::to_string(&c).unwrap();
            db.execute_raw(Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                &format!(
                    "INSERT INTO {DEFERRED_TABLE} (tbl, pk, cid, change, stored_at)
                     VALUES ($1, $2, $3, $4, $5)"
                ),
                [
                    "tasks".into(),
                    c.pk.0.as_str().into(),
                    "outcome".into(),
                    encoded.into(),
                    100i64.into(),
                ],
            ))
            .await
            .unwrap();
        }
        assert_eq!(
            count_for_table(&db, "tasks").await.unwrap(),
            MAX_DEFERRED_CHANGES + 1
        );

        enforce_cap(&db).await.unwrap();
        assert_eq!(
            count_for_table(&db, "tasks").await.unwrap(),
            MAX_DEFERRED_CHANGES
        );

        // The oldest (row0) is the one that went.
        let (replay, _) = load_replayable(&db, "tasks", &["outcome".into()])
            .await
            .unwrap();
        assert!(!replay.iter().any(|(_, c)| c.pk.0 == "row0"));
        assert!(replay.iter().any(|(_, c)| c.pk.0 == "row1"));
    }

    #[tokio::test]
    async fn undecodable_rows_are_reported_not_replayed() {
        let db = test_db().await;
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            &format!(
                "INSERT INTO {DEFERRED_TABLE} (tbl, pk, cid, change, stored_at)
                 VALUES ('tasks', 'r1', 'outcome', 'not json', 100)"
            ),
            Vec::<sea_orm::Value>::new(),
        ))
        .await
        .unwrap();

        let (replay, undecodable) = load_replayable(&db, "tasks", &["outcome".into()])
            .await
            .unwrap();
        assert!(replay.is_empty());
        assert_eq!(undecodable.len(), 1);

        purge(&db, &undecodable).await.unwrap();
        assert_eq!(count_for_table(&db, "tasks").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn purge_of_nothing_is_a_noop() {
        let db = test_db().await;
        purge(&db, &[]).await.unwrap();
    }
}
