//! Snapshot generation and application for new peer sync.
//!
//! When a new peer joins the network, it requests a full snapshot from an
//! existing peer. The donor generates a snapshot of all registered tables
//! (via `SELECT *`) and sends it as a `SyncResponse::FullSnapshot`.
//! The new peer applies the snapshot by clearing and re-inserting all rows.

use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, FromQueryResult, JsonValue, Statement};

use crate::protocol::{SyncResponse, TableSnapshot};
use crate::registry::TableRegistry;

#[derive(Debug, FromQueryResult)]
struct JsonRow {
    json_data: String,
}

/// Returns true if `name` is a valid SQL identifier: `[a-zA-Z_][a-zA-Z0-9_]*`.
fn is_valid_sql_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Convert a `serde_json::Value` to a `sea_orm::Value` for parameterized queries.
fn json_to_sea_value(v: &JsonValue) -> sea_orm::Value {
    match v {
        JsonValue::Null => sea_orm::Value::String(None),
        JsonValue::Bool(b) => sea_orm::Value::Int(Some(if *b { 1 } else { 0 })),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                sea_orm::Value::BigInt(Some(i))
            } else if let Some(f) = n.as_f64() {
                sea_orm::Value::Double(Some(f))
            } else {
                sea_orm::Value::String(Some(n.to_string()))
            }
        }
        JsonValue::String(s) => sea_orm::Value::String(Some(s.clone())),
        other => sea_orm::Value::String(Some(other.to_string())),
    }
}

/// Generate a full snapshot of all registered tables.
///
/// Queries each table with `SELECT *`, serializes rows as JSON arrays,
/// and returns a `SyncResponse::FullSnapshot` with the current HLC time.
pub async fn generate_snapshot(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
    current_hlc: u64,
) -> Result<SyncResponse, DbErr> {
    let tables_meta = registry.all_tables();
    let mut table_snapshots = Vec::with_capacity(tables_meta.len());

    for meta in &tables_meta {
        let columns = &meta.columns;

        // Validate all identifiers before interpolation
        if !is_valid_sql_identifier(&meta.table_name) {
            log::warn!(
                "Skipping table with invalid name in snapshot generation: {}",
                meta.table_name
            );
            continue;
        }
        if columns.iter().any(|c| !is_valid_sql_identifier(c)) {
            log::warn!(
                "Skipping table {} with invalid column names in snapshot generation",
                meta.table_name
            );
            continue;
        }

        // Build a JSON array expression for each row
        let json_expr = format!(
            "json_array({})",
            columns
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let sql = format!(
            "SELECT {} as json_data FROM \"{}\"",
            json_expr, meta.table_name
        );

        let rows = JsonRow::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            sql,
        ))
        .all(db)
        .await?;

        let row_values: Vec<Vec<serde_json::Value>> = rows
            .into_iter()
            .filter_map(|r| serde_json::from_str(&r.json_data).ok())
            .collect();

        table_snapshots.push(TableSnapshot {
            table_name: meta.table_name.clone(),
            columns: columns.clone(),
            rows: row_values,
        });
    }

    Ok(SyncResponse::FullSnapshot {
        tables: table_snapshots,
        recent_ops: Vec::new(),
        current_hlc,
    })
}

/// Apply a full snapshot to the local database.
///
/// For each table in the snapshot:
/// 1. Validates against the local registry (table must be registered, columns must match)
/// 2. Validates all identifiers for SQL safety
/// 3. Deletes all existing rows
/// 4. Inserts all snapshot rows using parameterized queries
///
/// The entire operation is wrapped in a SQLite transaction — any error rolls back all changes.
///
/// Returns `true` if data was actually applied, `false` if the snapshot had no relevant data
/// (e.g., from a peer with different tables). A `false` return is not an error — the caller
/// should retry with a different peer.
pub async fn apply_snapshot(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
    tables: &[TableSnapshot],
) -> Result<bool, DbErr> {
    // Check if this snapshot has any data for locally registered tables.
    // If no registered table has rows, treat as a no-op (wrong peer or empty peer).
    let has_relevant_data = tables.iter().any(|t| {
        !t.rows.is_empty() && registry.is_registered(&t.table_name)
    });
    if !has_relevant_data {
        log::info!(
            "Snapshot has no relevant data for registered tables, skipping (no-op)"
        );
        return Ok(false);
    }

    // Begin transaction
    db.execute_unprepared("BEGIN").await?;

    let result = apply_snapshot_inner(db, registry, tables).await;

    match result {
        Ok(()) => {
            db.execute_unprepared("COMMIT").await?;
            Ok(true)
        }
        Err(e) => {
            if let Err(rollback_err) = db.execute_unprepared("ROLLBACK").await {
                log::error!("Failed to rollback snapshot transaction: {}", rollback_err);
            }
            Err(e)
        }
    }
}

async fn apply_snapshot_inner(
    db: &impl ConnectionTrait,
    registry: &TableRegistry,
    tables: &[TableSnapshot],
) -> Result<(), DbErr> {
    for table in tables {
        // Validate against local registry
        let meta = match registry.get(&table.table_name) {
            Some(m) => m,
            None => {
                log::warn!(
                    "Skipping unregistered table in snapshot: {}",
                    table.table_name
                );
                continue;
            }
        };

        // Validate columns match registry
        if table.columns != meta.columns {
            return Err(DbErr::Custom(format!(
                "Snapshot column mismatch for table '{}': expected {:?}, got {:?}",
                table.table_name, meta.columns, table.columns
            )));
        }

        // Validate all identifiers
        if !is_valid_sql_identifier(&table.table_name) {
            return Err(DbErr::Custom(format!(
                "Invalid table name in snapshot: {}",
                table.table_name
            )));
        }
        for col in &table.columns {
            if !is_valid_sql_identifier(col) {
                return Err(DbErr::Custom(format!(
                    "Invalid column name in snapshot for table '{}': {}",
                    table.table_name, col
                )));
            }
        }

        // Skip tables with no data — don't delete existing rows for empty snapshots
        if table.rows.is_empty() || table.columns.is_empty() {
            continue;
        }

        // Clear existing data (only when we have rows to insert)
        let delete_sql = format!("DELETE FROM \"{}\"", table.table_name);
        db.execute_unprepared(&delete_sql).await?;

        let col_list = table
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");

        let placeholders = (1..=table.columns.len())
            .map(|i| format!("${}", i))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_sql = format!(
            "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
            table.table_name, col_list, placeholders
        );

        // Insert each row with parameterized values
        for row in &table.rows {
            // Validate row shape
            if row.len() != table.columns.len() {
                return Err(DbErr::Custom(format!(
                    "Row shape mismatch for table '{}': expected {} columns, got {}",
                    table.table_name,
                    table.columns.len(),
                    row.len()
                )));
            }

            let values: Vec<sea_orm::Value> = row.iter().map(json_to_sea_value).collect();

            db.execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                &insert_sql,
                values,
            ))
            .await?;
        }

        // Post-insert count check
        #[derive(Debug, FromQueryResult)]
        struct CountRow {
            cnt: i64,
        }
        let count_sql = format!("SELECT COUNT(*) as cnt FROM \"{}\"", table.table_name);
        let count = CountRow::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            count_sql,
        ))
        .one(db)
        .await?;

        let actual_count = count.map(|c| c.cnt).unwrap_or(0);
        if actual_count != table.rows.len() as i64 {
            return Err(DbErr::Custom(format!(
                "Post-insert count mismatch for table '{}': expected {}, got {}",
                table.table_name,
                table.rows.len(),
                actual_count
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{TableMeta, TableRegistry};
    use sea_orm::Database;

    async fn setup() -> (sea_orm::DatabaseConnection, TableRegistry) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared(
            "CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0
            )",
        )
        .await
        .unwrap();

        let registry = TableRegistry::new();
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec![
                "id".to_string(),
                "title".to_string(),
                "completed".to_string(),
            ],
        });

        // Insert some test data
        db.execute_unprepared("INSERT INTO tasks VALUES ('1', 'Task One', 0)")
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO tasks VALUES ('2', 'Task Two', 1)")
            .await
            .unwrap();

        (db, registry)
    }

    #[tokio::test]
    async fn test_generate_snapshot() {
        let (db, registry) = setup().await;
        let snapshot = generate_snapshot(&db, &registry, 1000).await.unwrap();

        match snapshot {
            SyncResponse::FullSnapshot {
                tables,
                current_hlc,
                ..
            } => {
                assert_eq!(current_hlc, 1000);
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0].table_name, "tasks");
                assert_eq!(tables[0].rows.len(), 2);
            }
            _ => panic!("Expected FullSnapshot"),
        }
    }

    #[tokio::test]
    async fn test_apply_snapshot() {
        let (donor_db, registry) = setup().await;
        let snapshot = generate_snapshot(&donor_db, &registry, 1000).await.unwrap();

        // Create a new empty DB
        let new_db = Database::connect("sqlite::memory:").await.unwrap();
        new_db
            .execute_unprepared(
                "CREATE TABLE tasks (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    completed INTEGER NOT NULL DEFAULT 0
                )",
            )
            .await
            .unwrap();

        if let SyncResponse::FullSnapshot { tables, .. } = &snapshot {
            apply_snapshot(&new_db, &registry, tables).await.unwrap();
        }

        // Verify data was transferred
        #[derive(Debug, FromQueryResult)]
        struct Count {
            cnt: i64,
        }
        let count = Count::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) as cnt FROM tasks".to_string(),
        ))
        .one(&new_db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count.cnt, 2);
    }

    #[tokio::test]
    async fn test_apply_snapshot_rejects_unregistered_table() {
        let (_, registry) = setup().await;
        let new_db = Database::connect("sqlite::memory:").await.unwrap();
        new_db
            .execute_unprepared("CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, completed INTEGER)")
            .await
            .unwrap();

        // Snapshot with an unregistered table name — should be skipped (no error, just warning)
        let tables = vec![TableSnapshot {
            table_name: "unknown_table".to_string(),
            columns: vec!["id".to_string()],
            rows: vec![vec![JsonValue::String("1".to_string())]],
        }];

        // Should succeed (unregistered tables are skipped, not errored)
        apply_snapshot(&new_db, &registry, &tables).await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_snapshot_rejects_invalid_identifier() {
        let (_, _) = setup().await;
        let new_db = Database::connect("sqlite::memory:").await.unwrap();

        let registry = TableRegistry::new();
        // Register a table with an invalid name to test the identifier check
        registry.register(TableMeta {
            table_name: "tasks; DROP TABLE users".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string()],
        });

        let tables = vec![TableSnapshot {
            table_name: "tasks; DROP TABLE users".to_string(),
            columns: vec!["id".to_string()],
            rows: vec![vec![JsonValue::String("1".to_string())]],
        }];

        let result = apply_snapshot(&new_db, &registry, &tables).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_apply_snapshot_skips_all_empty() {
        let (_, registry) = setup().await;
        let new_db = Database::connect("sqlite::memory:").await.unwrap();

        let tables = vec![TableSnapshot {
            table_name: "tasks".to_string(),
            columns: vec![
                "id".to_string(),
                "title".to_string(),
                "completed".to_string(),
            ],
            rows: vec![], // Empty rows
        }];

        let result = apply_snapshot(&new_db, &registry, &tables).await;
        // Empty snapshots are treated as no-op (Ok(false)), not errors
        assert_eq!(result.unwrap(), false);
    }

    #[tokio::test]
    async fn test_apply_snapshot_rejects_mismatched_columns() {
        let (_, registry) = setup().await;
        let new_db = Database::connect("sqlite::memory:").await.unwrap();
        new_db
            .execute_unprepared("CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, completed INTEGER)")
            .await
            .unwrap();

        let tables = vec![TableSnapshot {
            table_name: "tasks".to_string(),
            columns: vec!["id".to_string(), "wrong_col".to_string()], // Mismatch
            rows: vec![vec![
                JsonValue::String("1".to_string()),
                JsonValue::String("test".to_string()),
            ]],
        }];

        let result = apply_snapshot(&new_db, &registry, &tables).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("column mismatch"));
    }

    #[tokio::test]
    async fn test_apply_snapshot_rolls_back_on_error() {
        let (donor_db, registry) = setup().await;

        // Generate a valid snapshot first
        let _snapshot = generate_snapshot(&donor_db, &registry, 1000).await.unwrap();

        let new_db = Database::connect("sqlite::memory:").await.unwrap();
        new_db
            .execute_unprepared("CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, completed INTEGER NOT NULL)")
            .await
            .unwrap();

        // Insert initial data that should be preserved on rollback
        new_db
            .execute_unprepared("INSERT INTO tasks VALUES ('orig', 'Original', 0)")
            .await
            .unwrap();

        // Create a snapshot with a row shape mismatch to trigger rollback
        let bad_tables = vec![TableSnapshot {
            table_name: "tasks".to_string(),
            columns: vec![
                "id".to_string(),
                "title".to_string(),
                "completed".to_string(),
            ],
            rows: vec![
                vec![
                    JsonValue::String("1".to_string()),
                    JsonValue::String("Good Row".to_string()),
                    JsonValue::Number(serde_json::Number::from(0)),
                ],
                vec![
                    // Only 2 values instead of 3 — shape mismatch
                    JsonValue::String("2".to_string()),
                    JsonValue::String("Bad Row".to_string()),
                ],
            ],
        }];

        let result = apply_snapshot(&new_db, &registry, &bad_tables).await;
        assert!(result.is_err());

        // Verify original data is still there (rollback worked)
        #[derive(Debug, FromQueryResult)]
        struct Count {
            cnt: i64,
        }
        let count = Count::find_by_statement(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) as cnt FROM tasks WHERE id = 'orig'".to_string(),
        ))
        .one(&new_db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count.cnt, 1, "Original data should be preserved after rollback");
    }

    #[test]
    fn test_is_valid_sql_identifier() {
        assert!(is_valid_sql_identifier("tasks"));
        assert!(is_valid_sql_identifier("my_table"));
        assert!(is_valid_sql_identifier("_private"));
        assert!(is_valid_sql_identifier("Table123"));
        assert!(!is_valid_sql_identifier(""));
        assert!(!is_valid_sql_identifier("123table"));
        assert!(!is_valid_sql_identifier("my table"));
        assert!(!is_valid_sql_identifier("table;drop"));
        assert!(!is_valid_sql_identifier("ta\"ble"));
    }

    #[test]
    fn test_json_to_sea_value_null() {
        let val = json_to_sea_value(&serde_json::Value::Null);
        assert_eq!(val, sea_orm::Value::String(None));
    }

    #[test]
    fn test_json_to_sea_value_bool() {
        let val = json_to_sea_value(&serde_json::Value::Bool(true));
        assert_eq!(val, sea_orm::Value::Int(Some(1)));
        let val = json_to_sea_value(&serde_json::Value::Bool(false));
        assert_eq!(val, sea_orm::Value::Int(Some(0)));
    }

    #[test]
    fn test_json_to_sea_value_integer() {
        let val = json_to_sea_value(&serde_json::json!(42));
        assert_eq!(val, sea_orm::Value::BigInt(Some(42)));
    }

    #[test]
    fn test_json_to_sea_value_float() {
        let val = json_to_sea_value(&serde_json::json!(3.14));
        assert_eq!(val, sea_orm::Value::Double(Some(3.14)));
    }

    #[test]
    fn test_json_to_sea_value_string() {
        let val = json_to_sea_value(&serde_json::Value::String("hello".to_string()));
        assert_eq!(val, sea_orm::Value::String(Some("hello".to_string())));
    }

    #[test]
    fn test_json_to_sea_value_array_fallback() {
        let arr = serde_json::json!([1, 2, 3]);
        let val = json_to_sea_value(&arr);
        assert_eq!(val, sea_orm::Value::String(Some("[1,2,3]".to_string())));
    }
}
