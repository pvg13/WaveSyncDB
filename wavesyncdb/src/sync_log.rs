//! Persistent operation log for sync history.
//!
//! The `_wavesync_log` table stores every [`SyncOperation`] (local and remote) so that:
//! - Conflict resolution can look up the latest operation for a given row
//! - Incremental sync can replay operations since a given HLC timestamp
//! - New peers can receive a full history on first connect
//!
//! The table is created automatically by [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build).

use sea_orm::{ConnectionTrait, DbErr, ExecResult, FromQueryResult, Statement};

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
                format!("{:032x}", op.op_id).into(),
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

        let columns: Option<Vec<String>> = self.columns.and_then(|c| serde_json::from_str(&c).ok());

        SyncOperation {
            op_id: u128::from_str_radix(&self.op_id, 16).unwrap_or(0),
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
         FROM _wavesync_log WHERE hlc_time >= $1 ORDER BY hlc_time ASC, hlc_counter ASC",
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

#[derive(Debug, FromQueryResult)]
struct MaxHlcRow {
    max_hlc: Option<i64>,
}

/// Get the maximum HLC time from the sync log.
pub async fn get_max_hlc(db: &impl ConnectionTrait) -> Result<Option<u64>, DbErr> {
    let row = MaxHlcRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT MAX(hlc_time) as max_hlc FROM _wavesync_log",
        [],
    ))
    .one(db)
    .await?;

    Ok(row.and_then(|r| r.max_hlc).map(|v| v as u64))
}

/// Info about the latest operation for a single (table, primary_key) pair.
#[derive(Debug, FromQueryResult)]
pub struct LatestRowInfo {
    pub table_name: String,
    pub primary_key: String,
    pub hlc_time: i64,
    pub hlc_counter: i32,
    pub node_id: Vec<u8>,
}

/// Get the latest operation per (table_name, primary_key) pair.
///
/// Used at startup to build Merkle trees. Returns one entry per row,
/// ordered by table_name then primary_key.
pub async fn get_latest_per_row(
    db: &impl ConnectionTrait,
) -> Result<Vec<LatestRowInfo>, DbErr> {
    LatestRowInfo::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT table_name, primary_key, hlc_time, hlc_counter, node_id
         FROM (
             SELECT *, ROW_NUMBER() OVER (
                 PARTITION BY table_name, primary_key
                 ORDER BY hlc_time DESC, hlc_counter DESC
             ) as rn FROM _wavesync_log
         ) WHERE rn = 1
         ORDER BY table_name, primary_key",
        [],
    ))
    .all(db)
    .await
}

/// Delete all log entries with `hlc_time` older than `before_hlc`.
///
/// Garbage collection for the sync log. After compaction, incremental sync
/// requests for timestamps older than `before_hlc` will miss those operations.
pub async fn compact(db: &impl ConnectionTrait, before_hlc: u64) -> Result<ExecResult, DbErr> {
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "DELETE FROM _wavesync_log WHERE hlc_time < $1",
        [sea_orm::Value::BigInt(Some(before_hlc as i64))],
    ))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::WriteKind;
    use sea_orm::Database;

    async fn setup_log_db() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        create_log_table(&db).await.unwrap();
        db
    }

    fn make_op(
        op_id: u128,
        hlc_time: u64,
        hlc_counter: u32,
        table: &str,
        pk: &str,
        kind: WriteKind,
    ) -> SyncOperation {
        SyncOperation {
            op_id,
            hlc_time,
            hlc_counter,
            node_id: [1u8; 16],
            table: table.to_string(),
            kind,
            primary_key: pk.to_string(),
            data: Some(b"INSERT INTO tasks VALUES ('1')".to_vec()),
            columns: None,
        }
    }

    #[tokio::test]
    async fn test_create_log_table() {
        let db = setup_log_db().await;
        let result = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) as cnt FROM _wavesync_log",
            ))
            .await
            .unwrap()
            .unwrap();
        let count: i32 = result.try_get("", "cnt").unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_create_log_table_idempotent() {
        let db = setup_log_db().await;
        // Call create_log_table a second time — should not error.
        create_log_table(&db).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_op_insert_kind() {
        let db = setup_log_db().await;
        let op = make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert);
        insert_op(&db, &op).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT kind FROM _wavesync_log WHERE op_id = '00000000000000000000000000000001'",
            ))
            .await
            .unwrap()
            .unwrap();
        let kind: String = row.try_get("", "kind").unwrap();
        assert_eq!(kind, "insert");
    }

    #[tokio::test]
    async fn test_insert_op_update_kind() {
        let db = setup_log_db().await;
        let op = make_op(2, 100, 0, "tasks", "pk1", WriteKind::Update);
        insert_op(&db, &op).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT kind FROM _wavesync_log WHERE op_id = '00000000000000000000000000000002'",
            ))
            .await
            .unwrap()
            .unwrap();
        let kind: String = row.try_get("", "kind").unwrap();
        assert_eq!(kind, "update");
    }

    #[tokio::test]
    async fn test_insert_op_delete_kind() {
        let db = setup_log_db().await;
        let op = make_op(3, 100, 0, "tasks", "pk1", WriteKind::Delete);
        insert_op(&db, &op).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT kind FROM _wavesync_log WHERE op_id = '00000000000000000000000000000003'",
            ))
            .await
            .unwrap()
            .unwrap();
        let kind: String = row.try_get("", "kind").unwrap();
        assert_eq!(kind, "delete");
    }

    #[tokio::test]
    async fn test_insert_op_with_columns() {
        let db = setup_log_db().await;
        let mut op = make_op(4, 100, 0, "tasks", "pk1", WriteKind::Insert);
        op.columns = Some(vec!["id".to_string(), "name".to_string()]);
        insert_op(&db, &op).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT columns FROM _wavesync_log WHERE op_id = '00000000000000000000000000000004'",
            ))
            .await
            .unwrap()
            .unwrap();
        let columns: String = row.try_get("", "columns").unwrap();
        let parsed: Vec<String> = serde_json::from_str(&columns).unwrap();
        assert_eq!(parsed, vec!["id".to_string(), "name".to_string()]);
    }

    #[tokio::test]
    async fn test_insert_op_without_data() {
        let db = setup_log_db().await;
        let mut op = make_op(5, 100, 0, "tasks", "pk1", WriteKind::Insert);
        op.data = None;
        insert_op(&db, &op).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT data FROM _wavesync_log WHERE op_id = '00000000000000000000000000000005'",
            ))
            .await
            .unwrap()
            .unwrap();
        let data: Option<Vec<u8>> = row.try_get("", "data").ok();
        assert!(data.is_none() || data == Some(vec![]));
    }

    #[tokio::test]
    async fn test_insert_op_replaces_on_same_op_id() {
        let db = setup_log_db().await;
        let op1 = make_op(10, 100, 0, "tasks", "pk1", WriteKind::Insert);
        let op2 = make_op(10, 200, 1, "tasks", "pk1", WriteKind::Update);
        insert_op(&db, &op1).await.unwrap();
        insert_op(&db, &op2).await.unwrap();

        let row = db
            .query_one_raw(Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) as cnt FROM _wavesync_log WHERE op_id = '0000000000000000000000000000000a'",
            ))
            .await
            .unwrap()
            .unwrap();
        let count: i32 = row.try_get("", "cnt").unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_get_ops_since_returns_newer() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(3, 300, 0, "tasks", "pk3", WriteKind::Insert)).await.unwrap();

        let ops = get_ops_since(&db, 150).await.unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].hlc_time, 200);
        assert_eq!(ops[1].hlc_time, 300);
    }

    #[tokio::test]
    async fn test_get_ops_since_includes_boundary() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(3, 200, 1, "tasks", "pk3", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(4, 300, 0, "tasks", "pk4", WriteKind::Insert)).await.unwrap();

        // Ops at hlc_time=200 should be included (>= boundary)
        let ops = get_ops_since(&db, 200).await.unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].hlc_time, 200);
        assert_eq!(ops[1].hlc_time, 200);
        assert_eq!(ops[2].hlc_time, 300);
    }

    #[tokio::test]
    async fn test_get_ops_since_empty() {
        let db = setup_log_db().await;
        let ops = get_ops_since(&db, 0).await.unwrap();
        assert!(ops.is_empty());
    }

    #[tokio::test]
    async fn test_get_ops_since_ordering() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(3, 300, 0, "tasks", "pk3", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();

        let ops = get_ops_since(&db, 0).await.unwrap();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].hlc_time, 100);
        assert_eq!(ops[1].hlc_time, 200);
        assert_eq!(ops[2].hlc_time, 300);
    }

    #[tokio::test]
    async fn test_get_latest_for_row_found() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 0, "tasks", "pk1", WriteKind::Update)).await.unwrap();

        let latest = get_latest_for_row(&db, "tasks", "pk1").await.unwrap().unwrap();
        assert_eq!(latest.hlc_time, 200);
        assert_eq!(latest.op_id, 2);
    }

    #[tokio::test]
    async fn test_get_latest_for_row_not_found() {
        let db = setup_log_db().await;
        let result = get_latest_for_row(&db, "nonexistent", "pk1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_max_hlc_with_data() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 500, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(3, 300, 0, "tasks", "pk3", WriteKind::Insert)).await.unwrap();

        let max = get_max_hlc(&db).await.unwrap().unwrap();
        assert_eq!(max, 500);
    }

    #[tokio::test]
    async fn test_get_max_hlc_empty() {
        let db = setup_log_db().await;
        let max = get_max_hlc(&db).await.unwrap();
        assert!(max.is_none());
    }

    #[tokio::test]
    async fn test_compact_removes_old() {
        let db = setup_log_db().await;
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(3, 300, 0, "tasks", "pk3", WriteKind::Insert)).await.unwrap();

        compact(&db, 250).await.unwrap();

        let ops = get_ops_since(&db, 0).await.unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].hlc_time, 300);
    }

    #[tokio::test]
    async fn test_compact_empty_table() {
        let db = setup_log_db().await;
        compact(&db, 1000).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_latest_per_row_returns_latest() {
        let db = setup_log_db().await;
        // Insert multiple ops for same row — only latest should be returned
        insert_op(&db, &make_op(1, 100, 0, "tasks", "pk1", WriteKind::Insert)).await.unwrap();
        insert_op(&db, &make_op(2, 200, 1, "tasks", "pk1", WriteKind::Update)).await.unwrap();
        insert_op(&db, &make_op(3, 300, 0, "tasks", "pk1", WriteKind::Update)).await.unwrap();
        // Different row
        insert_op(&db, &make_op(4, 150, 0, "tasks", "pk2", WriteKind::Insert)).await.unwrap();
        // Different table
        insert_op(&db, &make_op(5, 250, 0, "users", "u1", WriteKind::Insert)).await.unwrap();

        let rows = get_latest_per_row(&db).await.unwrap();
        assert_eq!(rows.len(), 3, "Expected 3 unique (table, pk) pairs");

        // Should be ordered by table_name, primary_key
        assert_eq!(rows[0].table_name, "tasks");
        assert_eq!(rows[0].primary_key, "pk1");
        assert_eq!(rows[0].hlc_time, 300); // latest for pk1

        assert_eq!(rows[1].table_name, "tasks");
        assert_eq!(rows[1].primary_key, "pk2");
        assert_eq!(rows[1].hlc_time, 150);

        assert_eq!(rows[2].table_name, "users");
        assert_eq!(rows[2].primary_key, "u1");
        assert_eq!(rows[2].hlc_time, 250);
    }

    #[tokio::test]
    async fn test_get_latest_per_row_empty() {
        let db = setup_log_db().await;
        let rows = get_latest_per_row(&db).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn test_into_sync_op_unknown_kind() {
        let db = setup_log_db().await;
        db.execute_unprepared(
            "INSERT INTO _wavesync_log (op_id, hlc_time, hlc_counter, node_id, table_name, kind, primary_key, data, columns)
             VALUES ('42', 100, 0, X'01010101010101010101010101010101', 'tasks', 'unknown', 'pk1', NULL, NULL)",
        )
        .await
        .unwrap();

        let ops = get_ops_since(&db, 0).await.unwrap();
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0].kind, WriteKind::Insert));
    }

    #[tokio::test]
    async fn test_into_sync_op_short_node_id() {
        let db = setup_log_db().await;
        db.execute_unprepared(
            "INSERT INTO _wavesync_log (op_id, hlc_time, hlc_counter, node_id, table_name, kind, primary_key, data, columns)
             VALUES ('99', 100, 0, X'AABBCCDD', 'tasks', 'insert', 'pk1', NULL, NULL)",
        )
        .await
        .unwrap();

        let ops = get_ops_since(&db, 0).await.unwrap();
        assert_eq!(ops.len(), 1);
        let expected: [u8; 16] = [0xAA, 0xBB, 0xCC, 0xDD, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(ops[0].node_id, expected);
    }
}
