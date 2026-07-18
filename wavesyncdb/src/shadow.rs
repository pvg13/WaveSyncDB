//! Shadow tables for per-column CRDT metadata.
//!
//! Each synced table gets a companion `_wavesync_{table}_clock` table that stores
//! per-column Lamport clocks. A global `_wavesync_meta` table stores the monotonic
//! `db_version` counter and persistent `site_id`.
//!
//! Shadow tables replace the old `_wavesync_log` — metadata lives alongside
//! current state and overwrites in place, so no compaction is needed.

use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, FromQueryResult, Statement};

use crate::messages::{ColumnChange, NodeId};
use crate::registry::TableRegistry;

/// Begin one of this crate's bookkeeping transactions as `BEGIN IMMEDIATE`.
///
/// Managed DBs run in WAL (`connection::connect_sqlite`). A *deferred*
/// transaction that reads before its first write takes its snapshot at that
/// read; if any other connection commits before the write, SQLite fails the
/// write-lock upgrade **immediately** with `SQLITE_BUSY` ("database is
/// locked") — the busy handler is deliberately bypassed, because waiting
/// cannot un-stale the snapshot. Both bookkeeping transactions in this crate
/// (the local-write drain and the remote-changeset apply) are
/// read-then-write, so under concurrent pool traffic a plain `begin()` fails
/// constantly. `IMMEDIATE` takes the write lock at `BEGIN`, where
/// `busy_timeout` *does* apply — concurrent writers queue instead of failing.
pub(crate) async fn begin_write_txn(
    db: &sea_orm::DatabaseConnection,
) -> Result<sea_orm::DatabaseTransaction, DbErr> {
    use sea_orm::TransactionTrait;
    db.begin_with_options(sea_orm::TransactionOptions {
        sqlite_transaction_mode: Some(sea_orm::SqliteTransactionMode::Immediate),
        ..Default::default()
    })
    .await
}

/// A single clock entry from a shadow table.
#[derive(Debug, Clone)]
pub struct ClockEntry {
    pub pk: String,
    pub cid: String,
    pub col_version: u64,
    pub db_version: u64,
    pub site_id: NodeId,
    pub seq: u32,
}

/// Create the `_wavesync_meta` key-value table.
pub async fn create_meta_table(db: &impl ConnectionTrait) -> Result<ExecResult, DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS _wavesync_meta (
            key   TEXT PRIMARY KEY,
            value BLOB
        )",
    )
    .await
}

/// Name of the per-column clock shadow table for a user table. The single
/// source of truth for the `_wavesync_{table}_clock` spelling so callers never
/// re-derive it.
pub(crate) fn shadow_table_name(table_name: &str) -> String {
    format!("_wavesync_{}_clock", table_name)
}

/// Create the shadow clock table for a specific user table.
pub async fn create_shadow_table(
    db: &impl ConnectionTrait,
    table_name: &str,
) -> Result<ExecResult, DbErr> {
    let shadow_name = shadow_table_name(table_name);
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (
            pk          TEXT NOT NULL,
            cid         TEXT NOT NULL,
            col_version INTEGER NOT NULL,
            db_version  INTEGER NOT NULL,
            site_id     BLOB NOT NULL,
            seq         INTEGER NOT NULL DEFAULT 0,
            deleted_ts  INTEGER,
            PRIMARY KEY (pk, cid)
        )",
        shadow_name
    );
    db.execute_unprepared(&sql).await?;

    // Migrate pre-retention shadow tables in place. CREATE IF NOT EXISTS
    // does nothing for an existing table, so the column must be added
    // explicitly; SQLite has no ADD COLUMN IF NOT EXISTS, so the duplicate
    // error is the idempotence signal and is the only error swallowed.
    let alter = format!(
        "ALTER TABLE \"{}\" ADD COLUMN deleted_ts INTEGER",
        shadow_name
    );
    if let Err(e) = db.execute_unprepared(&alter).await {
        let msg = e.to_string();
        if !msg.contains("duplicate column name") {
            return Err(e);
        }
    }
    // Start legacy tombstones' retention clock at upgrade time: they can't
    // be older than "now" from every peer's perspective (no shared stamp
    // exists for them), and leaving them NULL would exempt them from GC
    // forever. Idempotent — nothing writes NULL after this migration.
    let backfill = format!(
        "UPDATE \"{}\" SET deleted_ts = {} WHERE cid = '__deleted' AND deleted_ts IS NULL",
        shadow_name,
        unix_now_secs()
    );
    db.execute_unprepared(&backfill).await?;

    // Index on db_version for efficient get_changes_since queries
    let idx_sql = format!(
        "CREATE INDEX IF NOT EXISTS \"idx_{}_db_version\" ON \"{}\" (db_version)",
        shadow_name, shadow_name
    );
    db.execute_unprepared(&idx_sql).await
}

/// Get the current `db_version` counter.
///
/// Returns the max of the persisted `_wavesync_meta` value and
/// `MAX(db_version)` across every existing shadow table. Reading from
/// both sources is what lets the local-write hot path (`dispatch_sync`)
/// skip the meta write entirely while still recovering correctly on
/// startup or whenever the engine refreshes its counter — the shadow
/// row that *is* fsync'd in the same tx as the entity write carries the
/// authoritative db_version, so MAX(shadow.db_version) is always
/// monotonic with respect to actual writes that landed.
pub async fn get_db_version(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
    let from_meta = get_db_version_from_meta(db).await?;
    let from_shadow = max_db_version_across_shadow_tables(db).await?;
    Ok(from_meta.max(from_shadow))
}

/// Read just the `_wavesync_meta`-stored value (used internally).
async fn get_db_version_from_meta(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MetaRow {
        value: Vec<u8>,
    }

    let row = MetaRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT value FROM _wavesync_meta WHERE key = $1",
        ["db_version".into()],
    ))
    .one(db)
    .await?;

    if let Some(row) = row
        && row.value.len() == 8
    {
        return Ok(u64::from_le_bytes(row.value.try_into().unwrap()));
    }

    Ok(0)
}

/// Compute `MAX(db_version)` across every `_wavesync_*_clock` shadow
/// table currently in the database. Discovered via `sqlite_master`, so
/// this works correctly without prior knowledge of which entities are
/// registered. Returns 0 when no shadow tables exist (fresh database).
pub async fn max_db_version_across_shadow_tables(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct TableName {
        name: String,
    }

    let tables = TableName::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name LIKE '_wavesync_%_clock'",
        [],
    ))
    .all(db)
    .await?;

    if tables.is_empty() {
        return Ok(0);
    }

    // One UNION ALL keeps SQLite's planner happy and avoids N round trips.
    let parts: Vec<String> = tables
        .iter()
        .map(|t| {
            format!(
                "SELECT MAX(db_version) AS m FROM \"{}\"",
                t.name.replace('"', "\"\"")
            )
        })
        .collect();
    let sql = format!(
        "SELECT COALESCE(MAX(m), 0) AS m FROM ({})",
        parts.join(" UNION ALL ")
    );

    #[derive(Debug, FromQueryResult)]
    struct MaxRow {
        m: i64,
    }
    let row = MaxRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [],
    ))
    .one(db)
    .await?;

    Ok(row.map(|r| r.m as u64).unwrap_or(0))
}

/// Atomically read, increment, and persist the `db_version` counter.
/// Returns the new version.
pub async fn increment_db_version(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
    let current = get_db_version(db).await?;
    let new_version = current + 1;
    let bytes = new_version.to_le_bytes().to_vec();

    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT OR REPLACE INTO _wavesync_meta (key, value) VALUES ($1, $2)",
        ["db_version".into(), bytes.into()],
    ))
    .await?;

    Ok(new_version)
}

/// Set the `db_version` to a specific value (used when applying remote changes with Lamport semantics).
pub async fn set_db_version(db: &impl ConnectionTrait, version: u64) -> Result<(), DbErr> {
    let bytes = version.to_le_bytes().to_vec();
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT OR REPLACE INTO _wavesync_meta (key, value) VALUES ($1, $2)",
        ["db_version".into(), bytes.into()],
    ))
    .await?;
    Ok(())
}

/// Load the persistent libp2p keypair from `_wavesync_meta`, or generate
/// and store a fresh one on first call. Persisting the keypair makes the
/// libp2p PeerId stable across process restarts — without this, every
/// app launch would create a new identity, which (a) causes the relay's
/// push-token store to accumulate stale entries (one per peer-id, all
/// pointing at the same physical FCM token), (b) defeats `last_seen`
/// tracking in `_wavesync_peer_versions`, and (c) makes log analysis
/// impossible since the peer-id changes each run.
///
/// Stored as protobuf-encoded bytes (the format produced by
/// `Keypair::to_protobuf_encoding`). If the stored value is corrupt or
/// no longer parseable, fall back to generating a fresh one — better
/// than crashing on startup. The corrupted bytes are overwritten.
pub async fn get_or_create_libp2p_keypair(
    db: &impl ConnectionTrait,
) -> Result<libp2p::identity::Keypair, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MetaRow {
        value: Vec<u8>,
    }

    let row = MetaRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT value FROM _wavesync_meta WHERE key = $1",
        ["libp2p_keypair".into()],
    ))
    .one(db)
    .await?;

    if let Some(row) = row {
        match libp2p::identity::Keypair::from_protobuf_encoding(&row.value) {
            Ok(kp) => return Ok(kp),
            Err(e) => {
                tracing::warn!(
                    "stored libp2p keypair is unparseable ({e}); regenerating. \
                     PeerId will change once."
                );
            }
        }
    }

    let keypair = libp2p::identity::Keypair::generate_ed25519();
    let bytes = keypair
        .to_protobuf_encoding()
        .map_err(|e| DbErr::Custom(format!("failed to encode libp2p keypair: {e}")))?;
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT OR REPLACE INTO _wavesync_meta (key, value) VALUES ($1, $2)",
        ["libp2p_keypair".into(), bytes.into()],
    ))
    .await?;
    Ok(keypair)
}

/// Get or generate a persistent site_id.
pub async fn get_site_id(db: &impl ConnectionTrait) -> Result<NodeId, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MetaRow {
        value: Vec<u8>,
    }

    let row = MetaRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT value FROM _wavesync_meta WHERE key = $1",
        ["site_id".into()],
    ))
    .one(db)
    .await?;

    if let Some(row) = row
        && row.value.len() == 16
    {
        let mut id = [0u8; 16];
        id.copy_from_slice(&row.value);
        return Ok(NodeId(id));
    }

    // Generate new site_id
    let mut id = [0u8; 16];
    let pid = std::process::id().to_le_bytes();
    id[..4].copy_from_slice(&pid);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_le_bytes();
    id[4..].copy_from_slice(&now[..12]);

    // Persist it
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT OR REPLACE INTO _wavesync_meta (key, value) VALUES ($1, $2)",
        ["site_id".into(), id.to_vec().into()],
    ))
    .await?;

    Ok(NodeId(id))
}

/// Get the current col_version for a specific column of a row.
pub async fn get_col_version(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    cid: &str,
) -> Result<u64, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct VersionRow {
        col_version: i64,
    }

    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "SELECT col_version FROM \"{}\" WHERE pk = $1 AND cid = $2",
        shadow_name
    );

    let row = VersionRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into(), cid.into()],
    ))
    .one(db)
    .await?;

    Ok(row.map(|r| r.col_version as u64).unwrap_or(0))
}

/// Get the current col_version and site_id for a specific column of a row.
pub async fn get_col_version_with_site(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    cid: &str,
) -> Result<(u64, NodeId), DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct VersionSiteRow {
        col_version: i64,
        site_id: Vec<u8>,
    }

    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "SELECT col_version, site_id FROM \"{}\" WHERE pk = $1 AND cid = $2",
        shadow_name
    );

    let row = VersionSiteRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into(), cid.into()],
    ))
    .one(db)
    .await?;

    match row {
        Some(r) => {
            let mut id = [0u8; 16];
            let len = r.site_id.len().min(16);
            id[..len].copy_from_slice(&r.site_id[..len]);
            Ok((r.col_version as u64, NodeId(id)))
        }
        None => Ok((0, NodeId([0u8; 16]))),
    }
}

/// Insert or replace a clock entry in the shadow table.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_clock_entry(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    cid: &str,
    col_version: u64,
    db_version: u64,
    site_id: &NodeId,
    seq: u32,
) -> Result<ExecResult, DbErr> {
    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" (pk, cid, col_version, db_version, site_id, seq)
         VALUES ($1, $2, $3, $4, $5, $6)",
        shadow_name
    );

    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [
            pk.into(),
            cid.into(),
            (col_version as i64).into(),
            (db_version as i64).into(),
            site_id.0.to_vec().into(),
            (seq as i32).into(),
        ],
    ))
    .await
}

/// Atomically upsert N clock entries for a single (pk, multiple cid) batch
/// in one statement, returning the resolved `col_version` for each `cid`.
///
/// Semantics match the per-column path: if the `(pk, cid)` row doesn't
/// exist, `col_version = 1`; if it does, `col_version = existing + 1`.
/// This collapses what used to be N reads + N writes into a single
/// `INSERT … ON CONFLICT(pk,cid) DO UPDATE … RETURNING`, matching what
/// the local-write path needs every time it dispatches a sync.
///
/// The returned map is keyed by `cid` because SQLite's `RETURNING`
/// ordering for multi-row inserts is unspecified — never rely on input
/// order.
pub async fn upsert_clock_entries_batch(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    columns: &[(String, u32)], // (cid, seq)
    db_version: u64,
    site_id: &NodeId,
    // Lower bound on the resulting col_version. Pass 0 for a normal write. On a
    // local re-insert after a *won* delete, pass `tombstone.cl + 1` so the revived
    // cells outrank the tombstone that a DeleteWins peer still holds — otherwise
    // the re-insert lands exactly at `cl` (a tie the peer's delete wins) and the
    // row diverges. See N8 / the resurrection-floor note in `connection.rs`.
    floor: u64,
) -> Result<std::collections::HashMap<String, u64>, DbErr> {
    if columns.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let shadow_name = format!("_wavesync_{}_clock", table);
    // Initial col_version for a brand-new (pk, cid): at least 1, at least the floor.
    let initial_cv = 1u64.max(floor) as i64;

    // Build the multi-row VALUES clause with positional placeholders.
    let mut placeholders = String::new();
    let mut values: Vec<sea_orm::Value> = Vec::with_capacity(columns.len() * 6);
    for (i, (cid, seq)) in columns.iter().enumerate() {
        if i > 0 {
            placeholders.push(',');
        }
        let base = i * 6;
        placeholders.push_str(&format!(
            "(${},${},${},${},${},${})",
            base + 1,
            base + 2,
            base + 3,
            base + 4,
            base + 5,
            base + 6,
        ));
        values.push(pk.into());
        values.push(cid.clone().into());
        // Initial col_version for new (pk, cid) pairs. The ON CONFLICT
        // branch overrides this with `max(existing + 1, floor)`.
        values.push(initial_cv.into());
        values.push((db_version as i64).into());
        values.push(site_id.0.to_vec().into());
        values.push((*seq as i32).into());
    }

    // `floor` is a validated u64 (never remote-controlled), safe to inline.
    let sql = format!(
        r#"INSERT INTO "{shadow}" (pk, cid, col_version, db_version, site_id, seq)
           VALUES {values}
           ON CONFLICT(pk, cid) DO UPDATE SET
               col_version = MAX("{shadow}".col_version + 1, {floor}),
               db_version = excluded.db_version,
               site_id    = excluded.site_id,
               seq        = excluded.seq
           RETURNING cid, col_version"#,
        shadow = shadow_name,
        values = placeholders,
        floor = floor,
    );

    #[derive(Debug, FromQueryResult)]
    struct Returned {
        cid: String,
        col_version: i64,
    }

    let rows = Returned::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        values,
    ))
    .all(db)
    .await?;

    let mut out = std::collections::HashMap::with_capacity(rows.len());
    for r in rows {
        out.insert(r.cid, r.col_version as u64);
    }
    Ok(out)
}

/// Get all clock entries for a specific row.
pub async fn get_clock_entries_for_row(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
) -> Result<Vec<ClockEntry>, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct ClockRow {
        pk: String,
        cid: String,
        col_version: i64,
        db_version: i64,
        site_id: Vec<u8>,
        seq: i32,
    }

    let cutoff = tombstone_cutoff(db).await?;
    let shadow_name = format!("_wavesync_{}_clock", table);
    // Aged tombstones must not contribute to a row's max col_version (a new
    // local delete's tombstone_cv would otherwise differ between a peer that
    // physically GC'd and one that hasn't).
    let sql = format!(
        "SELECT pk, cid, col_version, db_version, site_id, seq FROM \"{}\" WHERE pk = $1{}",
        shadow_name,
        aged_tombstone_predicate(cutoff, "")
    );

    let rows = ClockRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let mut id = [0u8; 16];
            let len = r.site_id.len().min(16);
            id[..len].copy_from_slice(&r.site_id[..len]);
            ClockEntry {
                pk: r.pk,
                cid: r.cid,
                col_version: r.col_version as u64,
                db_version: r.db_version as u64,
                site_id: NodeId(id),
                seq: r.seq as u32,
            }
        })
        .collect())
}

/// Get all changes since a given db_version across all shadow tables.
///
/// Joins shadow clock tables with actual user tables to get current column values.
/// Returns changes ordered by (db_version, seq).
///
/// Uses a single JOIN query per table instead of per-row lookups.
pub async fn get_changes_since(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
    since_db_version: u64,
) -> Result<Vec<ColumnChange>, DbErr> {
    let mut all_changes = Vec::new();
    let cutoff = tombstone_cutoff(db).await?;

    for meta in registry.all_tables() {
        let shadow_name = format!("_wavesync_{}_clock", meta.table_name);
        let pk_col = &meta.primary_key_column;

        // Build json_object() with all columns so the JOIN returns all
        // values in a single round trip — O(1) queries per table vs the
        // old O(rows) approach. Each column goes through the shared
        // blob-safe expression so this read, the capture triggers, and the
        // tiebreak read all produce the same JSON spelling.
        let json_cols: String = meta
            .columns
            .iter()
            .map(|c| format!("'{}', {}", c, crate::capture::json_col_expr("t", c)))
            .collect::<Vec<_>>()
            .join(", ");

        // Aged tombstones never travel: excluded here, they vanish from
        // catch-up responses, reconcile digests, and RBSR enumeration all
        // at once (every one of those surfaces reads through this query).
        let sql = format!(
            "SELECT s.pk, s.cid, s.col_version, s.db_version, s.seq, s.site_id, s.deleted_ts, \
             json_object({json_cols}) as row_json \
             FROM \"{shadow_name}\" s \
             LEFT JOIN \"{table}\" t ON t.\"{pk_col}\" = s.pk \
             WHERE s.db_version > $1{aged} \
             ORDER BY s.db_version, s.seq",
            table = meta.table_name,
            aged = aged_tombstone_predicate(cutoff, "s."),
        );

        let stmt = Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            &sql,
            [(since_db_version as i64).into()],
        );
        let rows = db.query_all_raw(stmt).await?;

        for row in rows {
            let pk: String = row.try_get("", "pk")?;
            let cid: String = row.try_get("", "cid")?;
            let col_version: i64 = row.try_get("", "col_version")?;
            let db_version: i64 = row.try_get("", "db_version")?;
            let seq: i32 = row.try_get("", "seq")?;
            let site_id_bytes: Vec<u8> = row.try_get("", "site_id")?;
            let deleted_ts: Option<i64> = row.try_get("", "deleted_ts").unwrap_or(None);

            let val = if cid == "__deleted" {
                None
            } else {
                let raw: Option<String> = row.try_get("", "row_json").ok();
                raw.and_then(|s| {
                    let obj: serde_json::Value = serde_json::from_str(&s).ok()?;
                    let v = obj.get(&cid)?.clone();
                    if v.is_null() { None } else { Some(v) }
                })
            };

            if val.is_none() && cid != "__deleted" {
                continue;
            }

            let mut id = [0u8; 16];
            let len = site_id_bytes.len().min(16);
            id[..len].copy_from_slice(&site_id_bytes[..len]);

            all_changes.push(ColumnChange {
                table: meta.table_name.clone().into(),
                pk: pk.into(),
                cid: cid.into(),
                val,
                site_id: NodeId(id),
                col_version: col_version as u64,
                cl: col_version as u64,
                seq: seq as u32,
                db_version: db_version as u64,
                deleted_ts: deleted_ts.map(|t| t as u64),
            });
        }
    }

    all_changes.sort_by_key(|c| (c.db_version, c.seq));

    Ok(all_changes)
}

/// Insert a tombstone entry in the shadow table.
///
/// `deleted_ts` is the DELETER's wall-clock (unix seconds), carried on the
/// wire so every replica stores the same value — the shared basis for
/// retention/GC eligibility. It never participates in conflict ordering.
pub async fn insert_tombstone(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    col_version: u64,
    db_version: u64,
    site_id: &NodeId,
    deleted_ts: u64,
) -> Result<ExecResult, DbErr> {
    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "INSERT OR REPLACE INTO \"{}\" (pk, cid, col_version, db_version, site_id, seq, deleted_ts)
         VALUES ($1, '__deleted', $2, $3, $4, 0, $5)",
        shadow_name
    );
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [
            pk.into(),
            (col_version as i64).into(),
            (db_version as i64).into(),
            site_id.0.to_vec().into(),
            (deleted_ts as i64).into(),
        ],
    ))
    .await
}

/// Current unix time in seconds. Used exclusively to stamp and age
/// tombstones for retention/GC — wall-clock never participates in conflict
/// resolution.
pub(crate) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub use crate::messages::DEFAULT_TOMBSTONE_RETENTION_SECS;

/// Persist the tombstone retention window (seconds; 0 = GC disabled) in
/// `_wavesync_meta`. Stored IN the database rather than passed through the
/// engine so every reader — including a background-sync process that opens
/// the same file — ages tombstones by the same rule. An aged tombstone must
/// be invisible on every surface of every process or replicas diverge.
pub async fn set_tombstone_retention(
    db: &impl ConnectionTrait,
    retention: Option<std::time::Duration>,
) -> Result<(), DbErr> {
    let secs: u64 = retention.map(|d| d.as_secs().max(1)).unwrap_or(0);
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT OR REPLACE INTO _wavesync_meta (key, value) VALUES ($1, $2)",
        [
            "tombstone_retention".into(),
            secs.to_le_bytes().to_vec().into(),
        ],
    ))
    .await?;
    Ok(())
}

/// The exclusion cutoff: tombstones with `deleted_ts < cutoff` are treated
/// as nonexistent EVERYWHERE (wire, digests, RBSR, conflict gates). `None`
/// disables retention. Absent key = the 7-day default.
pub(crate) async fn tombstone_cutoff(db: &impl ConnectionTrait) -> Result<Option<u64>, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct MetaRow {
        value: Vec<u8>,
    }
    let row = MetaRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT value FROM _wavesync_meta WHERE key = $1",
        ["tombstone_retention".into()],
    ))
    .one(db)
    .await?;
    let secs = row
        .and_then(|r| r.value.try_into().ok().map(u64::from_le_bytes))
        .unwrap_or(DEFAULT_TOMBSTONE_RETENTION_SECS);
    if secs == 0 {
        return Ok(None);
    }
    Ok(Some(unix_now_secs().saturating_sub(secs)))
}

/// SQL predicate excluding aged tombstones from a shadow scan aliased `s`.
/// Aged means absent EVERYWHERE — this predicate (or its equivalent) must
/// guard every reader of `__deleted` cells, or peers that physically GC at
/// different times resolve conflicts differently and diverge.
fn aged_tombstone_predicate(cutoff: Option<u64>, alias: &str) -> String {
    match cutoff {
        Some(c) => format!(
            " AND NOT ({a}cid = '__deleted' AND {a}deleted_ts IS NOT NULL AND {a}deleted_ts < {c})",
            a = alias,
        ),
        None => String::new(),
    }
}

/// Physically delete aged tombstones from every registered table's shadow.
/// Returns the number of rows collected. Exclusion already hides these
/// rows from every surface — sync, digests, RBSR, and the conflict gates —
/// so when this sweep runs is a purely local storage concern. Incremental
/// catch-up stays complete at any cursor: shadow tables are
/// upsert-in-place, so live cells always travel regardless of `since`;
/// only deletes older than the retention window are lost, by design (the
/// documented resurrection window) — no full-resync fallback is needed.
pub async fn gc_aged_tombstones(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
) -> Result<u64, DbErr> {
    let Some(cutoff) = tombstone_cutoff(db).await? else {
        return Ok(0);
    };

    let mut collected = 0u64;
    for meta in registry.all_tables() {
        let shadow_name = format!("_wavesync_{}_clock", meta.table_name);
        let sql = format!(
            "DELETE FROM \"{shadow_name}\" \
             WHERE cid = '__deleted' AND deleted_ts IS NOT NULL AND deleted_ts < {cutoff}"
        );
        let res = db.execute_unprepared(&sql).await?;
        collected += res.rows_affected();
    }
    Ok(collected)
}

/// Read the causal length (`col_version`) of a row's `__deleted` tombstone, if
/// one is present. Used by the remote apply path to decide whether an incoming
/// column change provably outlives a local delete (and thus must clear it).
pub async fn get_tombstone_cl(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
) -> Result<Option<u64>, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct Cl {
        col_version: i64,
    }
    let cutoff = tombstone_cutoff(db).await?;
    let shadow_name = format!("_wavesync_{}_clock", table);
    // Aged tombstones are absent everywhere: if this gate still honored an
    // expired tombstone, a peer that already physically collected it would
    // resolve the same incoming change differently and the replicas would
    // permanently diverge.
    let sql = format!(
        "SELECT col_version FROM \"{}\" WHERE pk = $1 AND cid = '__deleted'{}",
        shadow_name,
        aged_tombstone_predicate(cutoff, "")
    );
    let row = Cl::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .one(db)
    .await?;
    Ok(row.map(|r| r.col_version as u64))
}

/// Remove just the `__deleted` tombstone sentinel for a row.
///
/// Used when a row is re-inserted or updated after deletion — preserves
/// per-column clock entries so that CRDT col_versions continue from their
/// previous values instead of resetting to 0.
pub async fn clear_tombstone(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
) -> Result<ExecResult, DbErr> {
    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "DELETE FROM \"{}\" WHERE pk = $1 AND cid = '__deleted'",
        shadow_name
    );

    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .await
}

/// Remove all shadow clock entries for a row (used when delete wins).
pub async fn delete_clock_entries(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
) -> Result<ExecResult, DbErr> {
    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!("DELETE FROM \"{}\" WHERE pk = $1", shadow_name);

    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .await
}

/// One-time repair of rows left in the N8 anomalous state: a live user row that
/// still carries a `__deleted` tombstone (a losing delete whose tombstone was
/// never cleared by the pre-fix remote-apply path). Such a row's cell set is a
/// strict superset of a converged peer's, so reconciliation can never close the
/// gap on its own — it only ever ships the tombstone toward the peer, which
/// correctly rejects it. This sweep clears the defeated tombstone directly so the
/// digest can match again.
///
/// Runs at engine start, once, across every registered table. Only touches rows
/// where the user row exists AND a tombstone is present AND the delete provably
/// lost (`!should_apply_delete(cl, max_col_version, policy)`) — a normal deleted
/// row (no user row) is left untouched. Returns the number of tombstones cleared.
pub async fn heal_lost_tombstones(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
) -> Result<usize, DbErr> {
    let mut healed = 0usize;
    let cutoff = tombstone_cutoff(db).await?;

    for meta in registry.all_tables() {
        if !shadow_table_exists(db, &meta.table_name).await? {
            continue;
        }
        let shadow_name = format!("_wavesync_{}_clock", meta.table_name);
        let pk_col = &meta.primary_key_column;

        // Tombstones whose user row still exists, with the row's max non-tombstone
        // col_version alongside (NULL → 0 when the row has no column entries).
        // Aged tombstones are skipped — they are pending physical GC, not
        // anomalies to heal.
        let sql = format!(
            "SELECT s.pk AS pk, s.col_version AS cl, \
                    (SELECT MAX(s2.col_version) FROM \"{shadow}\" s2 \
                     WHERE s2.pk = s.pk AND s2.cid != '__deleted') AS max_cv \
             FROM \"{shadow}\" s \
             JOIN \"{table}\" t ON t.\"{pk_col}\" = s.pk \
             WHERE s.cid = '__deleted'{aged}",
            shadow = shadow_name,
            table = meta.table_name,
            aged = aged_tombstone_predicate(cutoff, "s."),
        );
        let rows = db
            .query_all_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                &sql,
                [],
            ))
            .await?;

        for row in rows {
            let pk: String = row.try_get("", "pk")?;
            let cl: i64 = row.try_get("", "cl")?;
            let max_cv: Option<i64> = row.try_get("", "max_cv").ok().flatten();
            let max_cv = max_cv.unwrap_or(0) as u64;
            if !crate::conflict::should_apply_delete(cl as u64, max_cv, &meta.delete_policy) {
                clear_tombstone(db, &meta.table_name, &pk).await?;
                healed += 1;
            }
        }
    }

    if healed > 0 {
        tracing::info!("Healed {healed} row(s) stuck with a defeated tombstone (N8 repair)");
    }
    Ok(healed)
}

/// Check if a shadow table exists for the given table name. Used at
/// registration time to tell a returning (previously-synced) table apart from
/// a first-ever registration — `register_table` is the sole creator of the
/// shadow table, so its prior existence means the table synced in an earlier
/// run.
pub async fn shadow_table_exists(
    db: &impl ConnectionTrait,
    table_name: &str,
) -> Result<bool, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct CountRow {
        cnt: i64,
    }

    let shadow_name = shadow_table_name(table_name);
    let row = CountRow::find_by_statement(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name=$1",
        [shadow_name.into()],
    ))
    .one(db)
    .await?;

    Ok(row.is_some_and(|r| r.cnt > 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::NodeId;
    use sea_orm::Database;

    async fn setup_db() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        create_meta_table(&db).await.unwrap();
        db
    }

    async fn setup_with_shadow() -> sea_orm::DatabaseConnection {
        let db = setup_db().await;
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)",
        )
        .await
        .unwrap();
        create_shadow_table(&db, "tasks").await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_create_meta_table() {
        let db = setup_db().await;
        // Should be idempotent
        create_meta_table(&db).await.unwrap();
    }

    #[tokio::test]
    async fn test_db_version_default_zero() {
        let db = setup_db().await;
        assert_eq!(get_db_version(&db).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_increment_db_version() {
        let db = setup_db().await;
        assert_eq!(increment_db_version(&db).await.unwrap(), 1);
        assert_eq!(increment_db_version(&db).await.unwrap(), 2);
        assert_eq!(increment_db_version(&db).await.unwrap(), 3);
        assert_eq!(get_db_version(&db).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_set_db_version() {
        let db = setup_db().await;
        set_db_version(&db, 42).await.unwrap();
        assert_eq!(get_db_version(&db).await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_get_site_id_generates_and_persists() {
        let db = setup_db().await;
        let id1 = get_site_id(&db).await.unwrap();
        let id2 = get_site_id(&db).await.unwrap();
        assert_eq!(id1, id2, "site_id should be persisted and stable");
        assert_ne!(id1, NodeId([0u8; 16]), "site_id should be non-zero");
    }

    #[tokio::test]
    async fn test_create_shadow_table() {
        let db = setup_db().await;
        create_shadow_table(&db, "tasks").await.unwrap();
        assert!(shadow_table_exists(&db, "tasks").await.unwrap());
    }

    #[tokio::test]
    async fn test_shadow_table_idempotent() {
        let db = setup_db().await;
        create_shadow_table(&db, "tasks").await.unwrap();
        create_shadow_table(&db, "tasks").await.unwrap();
    }

    #[tokio::test]
    async fn test_upsert_and_get_col_version() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        // No entry yet
        assert_eq!(
            get_col_version(&db, "tasks", "pk1", "title").await.unwrap(),
            0
        );

        // Insert
        upsert_clock_entry(&db, "tasks", "pk1", "title", 1, 1, &site_id, 0)
            .await
            .unwrap();
        assert_eq!(
            get_col_version(&db, "tasks", "pk1", "title").await.unwrap(),
            1
        );

        // Update (upsert)
        upsert_clock_entry(&db, "tasks", "pk1", "title", 5, 2, &site_id, 0)
            .await
            .unwrap();
        assert_eq!(
            get_col_version(&db, "tasks", "pk1", "title").await.unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn test_get_clock_entries_for_row() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        upsert_clock_entry(&db, "tasks", "pk1", "title", 1, 1, &site_id, 0)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk1", "done", 2, 1, &site_id, 1)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk2", "title", 1, 2, &site_id, 0)
            .await
            .unwrap();

        let entries = get_clock_entries_for_row(&db, "tasks", "pk1")
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .any(|e| e.cid == "title" && e.col_version == 1)
        );
        assert!(
            entries
                .iter()
                .any(|e| e.cid == "done" && e.col_version == 2)
        );
    }

    #[tokio::test]
    async fn test_insert_tombstone() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        insert_tombstone(&db, "tasks", "pk1", 3, 5, &site_id, unix_now_secs())
            .await
            .unwrap();

        let entries = get_clock_entries_for_row(&db, "tasks", "pk1")
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].cid, "__deleted");
        assert_eq!(entries[0].col_version, 3);
    }

    #[tokio::test]
    async fn test_insert_tombstone_persists_deleted_ts_on_wire() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);
        let stamp = unix_now_secs();
        insert_tombstone(&db, "tasks", "pk1", 3, 5, &site_id, stamp)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
        let changes = get_changes_since(&db, &registry, 0).await.unwrap();
        let tomb = changes.iter().find(|c| c.cid == "__deleted").unwrap();
        // The stored stamp rides the wire so every replica ages this
        // tombstone from the same instant.
        assert_eq!(tomb.deleted_ts, Some(stamp));
    }

    #[tokio::test]
    async fn test_shadow_migration_adds_deleted_ts_and_backfills_once() {
        let db = setup_db().await;
        // Simulate a pre-retention shadow table: old DDL, no deleted_ts.
        db.execute_unprepared(
            "CREATE TABLE \"_wavesync_legacy_clock\" (
                pk TEXT NOT NULL, cid TEXT NOT NULL, col_version INTEGER NOT NULL,
                db_version INTEGER NOT NULL, site_id BLOB NOT NULL,
                seq INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (pk, cid))",
        )
        .await
        .unwrap();
        db.execute_unprepared(
            "INSERT INTO \"_wavesync_legacy_clock\" (pk, cid, col_version, db_version, site_id)
             VALUES ('gone', '__deleted', 1, 1, x'00')",
        )
        .await
        .unwrap();

        // Migration adds the column and backfills the legacy tombstone.
        create_shadow_table(&db, "legacy").await.unwrap();
        #[derive(FromQueryResult)]
        struct TsRow {
            deleted_ts: Option<i64>,
        }
        let ts = TsRow::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT deleted_ts FROM \"_wavesync_legacy_clock\" WHERE pk = 'gone'".to_string(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .deleted_ts;
        assert!(ts.is_some(), "legacy tombstone must be backfilled");

        // Backfill is one-shot: an explicit stamp survives re-migration.
        db.execute_unprepared(
            "UPDATE \"_wavesync_legacy_clock\" SET deleted_ts = 42 WHERE pk = 'gone'",
        )
        .await
        .unwrap();
        create_shadow_table(&db, "legacy").await.unwrap();
        let ts = TsRow::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT deleted_ts FROM \"_wavesync_legacy_clock\" WHERE pk = 'gone'".to_string(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap()
        .deleted_ts;
        assert_eq!(ts, Some(42), "non-NULL stamps must never be rewritten");
    }

    #[tokio::test]
    async fn test_aged_tombstone_excluded_everywhere() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);
        // Retention 100s; one fresh and one aged tombstone.
        set_tombstone_retention(&db, Some(std::time::Duration::from_secs(100)))
            .await
            .unwrap();
        let now = unix_now_secs();
        insert_tombstone(&db, "tasks", "fresh", 1, 1, &site_id, now)
            .await
            .unwrap();
        insert_tombstone(&db, "tasks", "aged", 1, 2, &site_id, now - 1000)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });

        // Wire/digest surface: the aged tombstone never travels.
        let changes = get_changes_since(&db, &registry, 0).await.unwrap();
        assert!(changes.iter().any(|c| c.pk == "fresh"));
        assert!(
            !changes.iter().any(|c| c.pk == "aged"),
            "aged tombstone must be excluded from every wire surface"
        );

        // Conflict surface: the aged tombstone no longer defends.
        assert!(
            get_tombstone_cl(&db, "tasks", "fresh")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            get_tombstone_cl(&db, "tasks", "aged")
                .await
                .unwrap()
                .is_none(),
            "aged means absent for conflict resolution too"
        );
        assert!(
            get_clock_entries_for_row(&db, "tasks", "aged")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_gc_sweep_collects_only_aged() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);
        set_tombstone_retention(&db, Some(std::time::Duration::from_secs(100)))
            .await
            .unwrap();
        let now = unix_now_secs();
        insert_tombstone(&db, "tasks", "aged1", 1, 5, &site_id, now - 1000)
            .await
            .unwrap();
        insert_tombstone(&db, "tasks", "aged2", 1, 9, &site_id, now - 1000)
            .await
            .unwrap();
        insert_tombstone(&db, "tasks", "fresh", 1, 12, &site_id, now)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });

        let collected = gc_aged_tombstones(&db, &registry).await.unwrap();
        assert_eq!(collected, 2, "both aged tombstones collected");
        // Fresh tombstone survives physically.
        assert!(
            get_tombstone_cl(&db, "tasks", "fresh")
                .await
                .unwrap()
                .is_some()
        );
        // Idempotent: nothing left to collect.
        assert_eq!(gc_aged_tombstones(&db, &registry).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_retention_disabled_keeps_everything() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);
        set_tombstone_retention(&db, None).await.unwrap();
        insert_tombstone(&db, "tasks", "ancient", 1, 1, &site_id, 1)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });

        assert!(
            get_tombstone_cl(&db, "tasks", "ancient")
                .await
                .unwrap()
                .is_some()
        );
        let changes = get_changes_since(&db, &registry, 0).await.unwrap();
        assert!(changes.iter().any(|c| c.pk == "ancient"));
        assert_eq!(gc_aged_tombstones(&db, &registry).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_delete_clock_entries() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        upsert_clock_entry(&db, "tasks", "pk1", "title", 1, 1, &site_id, 0)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk1", "done", 2, 1, &site_id, 1)
            .await
            .unwrap();

        delete_clock_entries(&db, "tasks", "pk1").await.unwrap();

        let entries = get_clock_entries_for_row(&db, "tasks", "pk1")
            .await
            .unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_clear_tombstone_preserves_column_clocks() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        // Set up column clock entries
        upsert_clock_entry(&db, "tasks", "pk1", "title", 5, 1, &site_id, 0)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk1", "done", 3, 1, &site_id, 1)
            .await
            .unwrap();

        // Add a tombstone (simulating a DELETE)
        insert_tombstone(&db, "tasks", "pk1", 6, 2, &site_id, unix_now_secs())
            .await
            .unwrap();

        let entries = get_clock_entries_for_row(&db, "tasks", "pk1")
            .await
            .unwrap();
        assert_eq!(entries.len(), 3); // title + done + __deleted

        // Clear only the tombstone (simulating a re-INSERT)
        clear_tombstone(&db, "tasks", "pk1").await.unwrap();

        let entries = get_clock_entries_for_row(&db, "tasks", "pk1")
            .await
            .unwrap();
        assert_eq!(entries.len(), 2); // title + done preserved
        assert!(entries.iter().all(|e| e.cid != "__deleted"));
        assert_eq!(
            entries
                .iter()
                .find(|e| e.cid == "title")
                .unwrap()
                .col_version,
            5,
            "col_version for title should be preserved"
        );
        assert_eq!(
            entries
                .iter()
                .find(|e| e.cid == "done")
                .unwrap()
                .col_version,
            3,
            "col_version for done should be preserved"
        );
    }

    #[tokio::test]
    async fn test_db_version_persistence_across_operations() {
        let db = setup_db().await;
        increment_db_version(&db).await.unwrap(); // 1
        increment_db_version(&db).await.unwrap(); // 2
        increment_db_version(&db).await.unwrap(); // 3

        // Simulate "restart" by re-reading
        let version = get_db_version(&db).await.unwrap();
        assert_eq!(version, 3);
    }

    #[tokio::test]
    async fn test_get_changes_since() {
        let db = setup_with_shadow().await;
        let site_id = NodeId([1u8; 16]);

        // Insert some data in the actual table
        db.execute_unprepared("INSERT INTO tasks VALUES ('pk1', 'Task 1', 0)")
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO tasks VALUES ('pk2', 'Task 2', 1)")
            .await
            .unwrap();

        // Add clock entries at different db_versions
        upsert_clock_entry(&db, "tasks", "pk1", "title", 1, 1, &site_id, 0)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk1", "done", 1, 1, &site_id, 1)
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "pk2", "title", 1, 3, &site_id, 0)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });

        // Get changes since db_version 1 (should only get pk2's change at db_version 3)
        let changes = get_changes_since(&db, &registry, 1).await.unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].pk, "pk2");
        assert_eq!(changes[0].cid, "title");

        // Get all changes (since 0)
        let all_changes = get_changes_since(&db, &registry, 0).await.unwrap();
        assert_eq!(all_changes.len(), 3);
    }

    #[tokio::test]
    async fn test_get_changes_since_blob_column_as_hex() {
        let db = setup_db().await;
        db.execute_unprepared("CREATE TABLE files (id TEXT PRIMARY KEY, data BLOB)")
            .await
            .unwrap();
        create_shadow_table(&db, "files").await.unwrap();
        db.execute_unprepared("INSERT INTO files (id, data) VALUES ('f1', X'DEADBEEF')")
            .await
            .unwrap();
        let site_id = NodeId([2u8; 16]);
        upsert_clock_entry(&db, "files", "f1", "data", 1, 1, &site_id, 0)
            .await
            .unwrap();

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "files".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "data".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });

        // BLOB cells must ship as lowercase hex strings — json_object()
        // errors on raw blobs, which used to make the whole catch-up fail.
        let changes = get_changes_since(&db, &registry, 0).await.unwrap();
        let data_change = changes.iter().find(|c| c.cid == "data").unwrap();
        assert_eq!(data_change.val, Some(serde_json::json!("deadbeef")));
    }

    #[tokio::test]
    async fn test_heal_lost_tombstones() {
        let db = setup_with_shadow().await;
        let site = NodeId([7u8; 16]);

        let registry = TableRegistry::new();
        registry.register(crate::registry::TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::AddWins,
        });

        // Anomalous row "live": a live user row that still carries a defeated
        // tombstone (title cv=2, tombstone cl=2 → AddWins tie, delete lost).
        db.execute_unprepared("INSERT INTO tasks (id, title, done) VALUES ('live', 'x', 0)")
            .await
            .unwrap();
        upsert_clock_entry(&db, "tasks", "live", "title", 2, 1, &site, 0)
            .await
            .unwrap();
        insert_tombstone(&db, "tasks", "live", 2, 1, &site, unix_now_secs())
            .await
            .unwrap();

        // Normally-deleted row "gone": tombstone present, NO user row. Must be left
        // alone (it is a correct deleted state, not the anomaly).
        insert_tombstone(&db, "tasks", "gone", 3, 1, &site, unix_now_secs())
            .await
            .unwrap();

        let healed = heal_lost_tombstones(&db, &registry).await.unwrap();
        assert_eq!(healed, 1, "only the live-row anomaly should be healed");

        // "live" tombstone cleared, its column clock preserved.
        assert!(
            get_tombstone_cl(&db, "tasks", "live")
                .await
                .unwrap()
                .is_none()
        );
        let live_entries = get_clock_entries_for_row(&db, "tasks", "live")
            .await
            .unwrap();
        assert!(live_entries.iter().any(|e| e.cid == "title"));
        // "gone" tombstone untouched.
        assert_eq!(
            get_tombstone_cl(&db, "tasks", "gone").await.unwrap(),
            Some(3)
        );
    }
}
