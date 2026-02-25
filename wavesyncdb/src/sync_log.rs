//! Persistent operation log for sync history.
//!
//! The `_wavesync_log` table stores every [`SyncOperation`] (local and remote) so that:
//! - Conflict resolution can look up the latest operation for a given row
//! - Incremental sync can replay operations since a given HLC timestamp
//! - New peers can receive a full history on first connect
//!
//! The table is created automatically by [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build).

use sea_orm::{
    ConnectionTrait, DbErr, ExecResult, FromQueryResult, Statement,
};

use crate::messages::{NodeId, SyncOperation, WriteKind};

/// Create the `_wavesync_log` table if it does not already exist.
///
/// Called automatically during [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build).
pub async fn create_log_table(db: &impl ConnectionTrait) -> Result<ExecResult, DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS _wavesync_log (
            op_id TEXT PRIMARY KEY,
            hlc_time INTEGER NOT NULL,
            hlc_counter INTEGER NOT NULL,
            node_id BLOB NOT NULL,
            table_name TEXT NOT NULL,
            kind TEXT NOT NULL,
            primary_key TEXT NOT NULL,
            data BLOB,
            columns TEXT,
            created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )",
    )
    .await
}

/// Insert a sync operation into the log (INSERT OR REPLACE by `op_id`).
///
/// Called by the P2P engine for both locally-originated and remotely-received operations.
pub async fn insert_op(db: &impl ConnectionTrait, op: &SyncOperation) -> Result<ExecResult, DbErr> {
    let kind_str = match op.kind {
        WriteKind::Insert => "insert",
        WriteKind::Update => "update",
        WriteKind::Delete => "delete",
    };
    let columns_json = op
        .columns
        .as_ref()
        .map(|cols| serde_json::to_string(cols).unwrap_or_default());

    db.execute_raw(
        Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "INSERT OR REPLACE INTO _wavesync_log (op_id, hlc_time, hlc_counter, node_id, table_name, kind, primary_key, data, columns)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            [
                op.op_id.to_string().into(),
                (op.hlc_time as i64).into(),
                (op.hlc_counter as i32).into(),
                op.node_id.to_vec().into(),
                op.table.clone().into(),
                kind_str.into(),
                op.primary_key.clone().into(),
                op.data.clone().map(|d| sea_orm::Value::Bytes(Some(d))).unwrap_or(sea_orm::Value::Bytes(None)),
                columns_json.map(|c| sea_orm::Value::String(Some(c))).unwrap_or(sea_orm::Value::String(None)),
            ],
        ),
    )
    .await
}

#[derive(Debug, FromQueryResult)]
struct LogRow {
    op_id: String,
    hlc_time: i64,
    hlc_counter: i32,
    node_id: Vec<u8>,
    table_name: String,
    kind: String,
    primary_key: String,
    data: Option<Vec<u8>>,
    columns: Option<String>,
}

impl LogRow {
    fn into_sync_op(self) -> SyncOperation {
        let kind = match self.kind.as_str() {
            "insert" => WriteKind::Insert,
            "update" => WriteKind::Update,
            "delete" => WriteKind::Delete,
            _ => WriteKind::Insert,
        };
        let mut node_id: NodeId = [0u8; 16];
        let len = self.node_id.len().min(16);
        node_id[..len].copy_from_slice(&self.node_id[..len]);

        let columns: Option<Vec<String>> = self
            .columns
            .and_then(|c| serde_json::from_str(&c).ok());

        SyncOperation {
            op_id: self.op_id.parse().unwrap_or(0),
            hlc_time: self.hlc_time as u64,
            hlc_counter: self.hlc_counter as u32,
            node_id,
            table: self.table_name,
            kind,
            primary_key: self.primary_key,
            data: self.data,
            columns,
        }
    }
}

/// Retrieve all operations with `hlc_time` greater than `since_hlc`, ordered ascending.
///
/// Used for incremental sync: a returning peer sends its last-seen HLC timestamp
/// and receives all operations that happened after that point.
pub async fn get_ops_since(
    db: &impl ConnectionTrait,
    since_hlc: u64,
) -> Result<Vec<SyncOperation>, DbErr> {
    let rows = LogRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT op_id, hlc_time, hlc_counter, node_id, table_name, kind, primary_key, data, columns
         FROM _wavesync_log WHERE hlc_time > $1 ORDER BY hlc_time ASC, hlc_counter ASC",
        [sea_orm::Value::BigInt(Some(since_hlc as i64))],
    ))
    .all(db)
    .await?;

    Ok(rows.into_iter().map(|r| r.into_sync_op()).collect())
}

/// Get the latest operation for a specific table row, identified by table name and primary key.
///
/// Used by the P2P engine during LWW conflict resolution to compare an incoming
/// remote operation against the most recent local state for that row.
pub async fn get_latest_for_row(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
) -> Result<Option<SyncOperation>, DbErr> {
    let row = LogRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT op_id, hlc_time, hlc_counter, node_id, table_name, kind, primary_key, data, columns
         FROM _wavesync_log WHERE table_name = $1 AND primary_key = $2
         ORDER BY hlc_time DESC, hlc_counter DESC LIMIT 1",
        [table.into(), pk.into()],
    ))
    .one(db)
    .await?;

    Ok(row.map(|r| r.into_sync_op()))
}

/// Delete all log entries with `hlc_time` older than `before_hlc`.
///
/// Garbage collection for the sync log. After compaction, incremental sync
/// requests for timestamps older than `before_hlc` will miss those operations.
pub async fn compact(db: &impl ConnectionTrait, before_hlc: u64) -> Result<ExecResult, DbErr> {
    db.execute_raw(
        Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "DELETE FROM _wavesync_log WHERE hlc_time < $1",
            [sea_orm::Value::BigInt(Some(before_hlc as i64))],
        ),
    )
    .await
}
