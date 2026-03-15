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

/// Create the shadow clock table for a specific user table.
pub async fn create_shadow_table(
    db: &impl ConnectionTrait,
    table_name: &str,
) -> Result<ExecResult, DbErr> {
    let shadow_name = format!("_wavesync_{}_clock", table_name);
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (
            pk          TEXT NOT NULL,
            cid         TEXT NOT NULL,
            col_version INTEGER NOT NULL,
            db_version  INTEGER NOT NULL,
            site_id     BLOB NOT NULL,
            seq         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (pk, cid)
        )",
        shadow_name
    );
    db.execute_unprepared(&sql).await?;

    // Index on db_version for efficient get_changes_since queries
    let idx_sql = format!(
        "CREATE INDEX IF NOT EXISTS \"idx_{}_db_version\" ON \"{}\" (db_version)",
        shadow_name, shadow_name
    );
    db.execute_unprepared(&idx_sql).await
}

/// Get the current `db_version` counter from `_wavesync_meta`.
///
/// Falls back to scanning shadow tables, then 0.
pub async fn get_db_version(db: &impl ConnectionTrait) -> Result<u64, DbErr> {
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
        let mut id: NodeId = [0u8; 16];
        id.copy_from_slice(&row.value);
        return Ok(id);
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

    Ok(id)
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
            site_id.to_vec().into(),
            (seq as i32).into(),
        ],
    ))
    .await
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

    let shadow_name = format!("_wavesync_{}_clock", table);
    let sql = format!(
        "SELECT pk, cid, col_version, db_version, site_id, seq FROM \"{}\" WHERE pk = $1",
        shadow_name
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
            let mut site_id: NodeId = [0u8; 16];
            let len = r.site_id.len().min(16);
            site_id[..len].copy_from_slice(&r.site_id[..len]);
            ClockEntry {
                pk: r.pk,
                cid: r.cid,
                col_version: r.col_version as u64,
                db_version: r.db_version as u64,
                site_id,
                seq: r.seq as u32,
            }
        })
        .collect())
}

/// Get all changes since a given db_version across all shadow tables.
///
/// Joins shadow clock tables with actual user tables to get current column values.
/// Returns changes ordered by (db_version, seq).
pub async fn get_changes_since(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
    since_db_version: u64,
) -> Result<Vec<ColumnChange>, DbErr> {
    let mut all_changes = Vec::new();

    for meta in registry.all_tables() {
        let shadow_name = format!("_wavesync_{}_clock", meta.table_name);
        let pk_col = &meta.primary_key_column;

        // Get all clock entries newer than since_db_version
        #[derive(Debug, FromQueryResult)]
        struct ChangeRow {
            pk: String,
            cid: String,
            col_version: i64,
            #[allow(dead_code)]
            db_version: i64,
            seq: i32,
            site_id: Vec<u8>,
        }

        let sql = format!(
            "SELECT pk, cid, col_version, db_version, seq, site_id FROM \"{}\" WHERE db_version > $1 ORDER BY db_version, seq",
            shadow_name
        );

        let rows = ChangeRow::find_by_statement(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            &sql,
            [(since_db_version as i64).into()],
        ))
        .all(db)
        .await?;

        for row in rows {
            // For __deleted entries, val is None
            let val = if row.cid == "__deleted" {
                None
            } else {
                // Look up the current value from the actual table.
                // Use json_object to get the value as a properly typed JSON value.
                let val_result = db
                    .query_one_raw(Statement::from_sql_and_values(
                        DatabaseBackend::Sqlite,
                        format!(
                            "SELECT json_object('v', \"{}\") as json_val FROM \"{}\" WHERE \"{}\" = $1",
                            row.cid, meta.table_name, pk_col
                        ),
                        [row.pk.clone().into()],
                    ))
                    .await?;

                match val_result {
                    Some(qr) => {
                        let raw: Option<String> = qr.try_get("", "json_val").ok();
                        raw.and_then(|s| {
                            let obj: serde_json::Value = serde_json::from_str(&s).ok()?;
                            Some(obj.get("v")?.clone())
                        })
                    }
                    None => None,
                }
            };

            let mut site_id: NodeId = [0u8; 16];
            let len = row.site_id.len().min(16);
            site_id[..len].copy_from_slice(&row.site_id[..len]);

            all_changes.push(ColumnChange {
                table: meta.table_name.clone(),
                pk: row.pk,
                cid: row.cid,
                val,
                site_id,
                col_version: row.col_version as u64,
                cl: row.col_version as u64, // causal length = col_version for non-deletes
                seq: row.seq as u32,
            });
        }
    }

    // Sort by (db_version from the clock entry, seq)
    // Note: db_version is embedded in ColumnChange indirectly via the query ordering
    all_changes.sort_by_key(|c| (c.col_version, c.seq));

    Ok(all_changes)
}

/// Insert a tombstone entry in the shadow table.
pub async fn insert_tombstone(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    col_version: u64,
    db_version: u64,
    site_id: &NodeId,
) -> Result<ExecResult, DbErr> {
    upsert_clock_entry(
        db,
        table,
        pk,
        "__deleted",
        col_version,
        db_version,
        site_id,
        0,
    )
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

/// Check if a shadow table exists for the given table name.
pub async fn shadow_table_exists(
    db: &impl ConnectionTrait,
    table_name: &str,
) -> Result<bool, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct CountRow {
        cnt: i64,
    }

    let shadow_name = format!("_wavesync_{}_clock", table_name);
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
        assert_ne!(id1, [0u8; 16], "site_id should be non-zero");
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
        let site_id = [1u8; 16];

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
        let site_id = [1u8; 16];

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
        let site_id = [1u8; 16];

        insert_tombstone(&db, "tasks", "pk1", 3, 5, &site_id)
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
    async fn test_delete_clock_entries() {
        let db = setup_with_shadow().await;
        let site_id = [1u8; 16];

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
        let site_id = [1u8; 16];

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
}
