//! Trigger-based change capture.
//!
//! Every registered user table gets three SQLite `AFTER` triggers that record
//! row-level changes into the permanent `_wavesync_changes` capture table.
//! The connection wrapper drains that table after each intercepted write and
//! feeds the rows into the existing shadow bookkeeping. Capture happens
//! *inside* SQLite, in the same transaction as the user's write, so it is
//! immune to the failure modes of SQL-text parsing: expression values
//! (`SET count = count + 1`) capture their computed result, `INSERT OR
//! REPLACE`/CTE/multi-statement scripts capture correctly, and writes made by
//! a *separate process* sharing the database file (the iOS background-sync
//! process) are captured for the main process to drain later.
//!
//! Remote applies must NOT be captured — an applied changeset that re-entered
//! the capture table would be re-broadcast, and two peers would echo the same
//! changes back and forth forever. The `_wavesync_apply_guard` row suppresses
//! the triggers while (and only while) the remote-apply transaction is open;
//! the flag is transactional state, so a rolled-back apply also rolls the
//! flag back, and SQLite's single-writer locking guarantees no concurrent
//! local write can observe the suppressed state.
//!
//! Value encoding: captured values pass through `json_object()`, the same
//! SQLite JSON spelling the catch-up path (`shadow::get_changes_since`) and
//! the conflict-tiebreak read (`get_local_value_bytes`) use. This makes the
//! sender's wire bytes and every receiver's local re-read byte-identical for
//! the same logical value — booleans travel as `0`/`1`, REALs use SQLite's
//! internal `%!.15g` formatting (byte-identical across platforms). BLOB cells
//! are hex-encoded as lowercase hex strings (`json_object()` errors on raw
//! blobs); a blob column therefore round-trips as TEXT on remote peers — a
//! documented limitation until typed blob sync exists.

// The drain (connection.rs) and the remote-apply suppression bracket
// (engine/sync_handler.rs) land in follow-up commits on this branch; until
// they do, nothing outside the tests consumes these items.
#![allow(dead_code)]

use sea_orm::{ConnectionTrait, DbErr, FromQueryResult, Statement};

use crate::registry::{TableMeta, TableRegistry};

/// Capture table: one row per row-level change, in write order.
pub(crate) const CAPTURE_TABLE: &str = "_wavesync_changes";
/// One-row guard table; `suppressed = 1` while a remote apply is writing.
pub(crate) const GUARD_TABLE: &str = "_wavesync_apply_guard";

/// Create the capture + guard tables (idempotent). Must run before any
/// trigger exists: a trigger whose body references a missing table makes
/// every write on the host table fail.
pub(crate) async fn ensure_capture_tables(db: &impl ConnectionTrait) -> Result<(), DbErr> {
    // AUTOINCREMENT is load-bearing: the drain purges `seq <= watermark`,
    // and plain rowid reuse after a delete could place a fresh capture row
    // below the watermark, where the purge would eat it undrained.
    db.execute_unprepared(&format!(
        "CREATE TABLE IF NOT EXISTS {CAPTURE_TABLE} (
            seq     INTEGER PRIMARY KEY AUTOINCREMENT,
            tbl     TEXT NOT NULL,
            op      TEXT NOT NULL CHECK (op IN ('I','U','D')),
            pk      TEXT NOT NULL,
            new_row TEXT,
            old_row TEXT
        )"
    ))
    .await?;
    db.execute_unprepared(&format!(
        "CREATE TABLE IF NOT EXISTS {GUARD_TABLE} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            suppressed INTEGER NOT NULL DEFAULT 0
        )"
    ))
    .await?;
    db.execute_unprepared(&format!(
        "INSERT OR IGNORE INTO {GUARD_TABLE} (id, suppressed) VALUES (1, 0)"
    ))
    .await?;
    Ok(())
}

/// SQL expression yielding a JSON-safe value for one column: blobs become
/// lowercase hex strings (`json_object()` errors on raw BLOBs), everything
/// else passes through with SQLite's native JSON typing. `src` is `"NEW"`,
/// `"OLD"`, a table alias, or `""` for a bare column reference.
pub(crate) fn json_col_expr(src: &str, col: &str) -> String {
    let c = if src.is_empty() {
        format!("\"{col}\"")
    } else {
        format!("{src}.\"{col}\"")
    };
    format!("CASE WHEN typeof({c})='blob' THEN lower(hex({c})) ELSE {c} END")
}

/// `json_object('c1', <expr>, 'c2', <expr>, ...)` over all columns of `meta`.
fn json_row_expr(src: &str, meta: &TableMeta) -> String {
    let args: Vec<String> = meta
        .columns
        .iter()
        .map(|c| format!("'{}', {}", c, json_col_expr(src, c)))
        .collect();
    format!("json_object({})", args.join(", "))
}

/// Short fingerprint of the trigger-relevant schema (pk + column list).
/// Embedded in the trigger name so `ensure_triggers` can detect a stale
/// trigger (column added/removed) by name alone — no SQL-text comparison
/// against `sqlite_master`, which normalizes the stored text.
fn schema_fingerprint(meta: &TableMeta) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(meta.primary_key_column.as_bytes());
    for c in &meta.columns {
        hasher.update(&[0]);
        hasher.update(c.as_bytes());
    }
    hasher.finalize().to_hex()[..8].to_string()
}

fn trigger_prefix(table: &str) -> String {
    format!("_wavesync_tr_{table}_")
}

/// The three `(name, CREATE TRIGGER ...)` pairs for a table. Errors on
/// `_wavesync*` tables — internal tables must never be captured (capturing a
/// shadow write would re-enter the write path and loop).
pub(crate) fn trigger_sql(meta: &TableMeta) -> Result<[(String, String); 3], DbErr> {
    if meta.table_name.starts_with("_wavesync") {
        return Err(DbErr::Custom(format!(
            "refusing to create capture triggers on internal table {}",
            meta.table_name
        )));
    }
    let t = &meta.table_name;
    let p = &meta.primary_key_column;
    let fp = schema_fingerprint(meta);
    let prefix = trigger_prefix(t);
    // COALESCE: a missing guard row must mean "not suppressed" — losing
    // capture silently is the one failure direction this design forbids.
    let gate = format!("WHEN COALESCE((SELECT suppressed FROM {GUARD_TABLE} WHERE id = 1), 0) = 0");

    let i_name = format!("{prefix}i_{fp}");
    let i_sql = format!(
        "CREATE TRIGGER \"{i_name}\" AFTER INSERT ON \"{t}\"\n{gate}\nBEGIN\n  \
         INSERT INTO {CAPTURE_TABLE} (tbl, op, pk, new_row)\n  \
         VALUES ('{t}', 'I', CAST(NEW.\"{p}\" AS TEXT), {new_row});\nEND",
        new_row = json_row_expr("NEW", meta),
    );

    let u_name = format!("{prefix}u_{fp}");
    // pk column records the OLD pk; a pk-changing UPDATE is detected by the
    // drain planner from the NEW row's pk value inside new_row.
    let u_sql = format!(
        "CREATE TRIGGER \"{u_name}\" AFTER UPDATE ON \"{t}\"\n{gate}\nBEGIN\n  \
         INSERT INTO {CAPTURE_TABLE} (tbl, op, pk, new_row, old_row)\n  \
         VALUES ('{t}', 'U', CAST(OLD.\"{p}\" AS TEXT), {new_row}, {old_row});\nEND",
        new_row = json_row_expr("NEW", meta),
        old_row = json_row_expr("OLD", meta),
    );

    let d_name = format!("{prefix}d_{fp}");
    let d_sql = format!(
        "CREATE TRIGGER \"{d_name}\" AFTER DELETE ON \"{t}\"\n{gate}\nBEGIN\n  \
         INSERT INTO {CAPTURE_TABLE} (tbl, op, pk)\n  \
         VALUES ('{t}', 'D', CAST(OLD.\"{p}\" AS TEXT));\nEND",
    );

    Ok([(i_name, i_sql), (u_name, u_sql), (d_name, d_sql)])
}

/// Create (or recreate after schema change) the capture triggers for a table.
/// Idempotent: the schema fingerprint in each trigger's name makes "current"
/// detectable by existence alone; any `_wavesync_tr_{t}_*` trigger with a
/// different fingerprint is dropped first.
pub(crate) async fn ensure_triggers(
    db: &impl ConnectionTrait,
    meta: &TableMeta,
) -> Result<(), DbErr> {
    ensure_capture_tables(db).await?;
    let triggers = trigger_sql(meta)?;

    #[derive(FromQueryResult)]
    struct NameRow {
        name: String,
    }
    let existing: Vec<String> = NameRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE $1",
        [format!("{}%", trigger_prefix(&meta.table_name)).into()],
    ))
    .all(db)
    .await?
    .into_iter()
    .map(|r| r.name)
    .collect();

    let wanted: Vec<&str> = triggers.iter().map(|(n, _)| n.as_str()).collect();
    for stale in existing.iter().filter(|n| !wanted.contains(&n.as_str())) {
        db.execute_unprepared(&format!("DROP TRIGGER IF EXISTS \"{stale}\""))
            .await?;
    }
    for (name, sql) in &triggers {
        if !existing.iter().any(|n| n == name) {
            db.execute_unprepared(sql).await?;
        }
    }
    Ok(())
}

/// Flip the capture-suppression flag. Called inside the remote-apply
/// transaction only: set `true` right after `begin()`, `false` right before
/// `commit()`, so the flag can never leak past the transaction.
pub(crate) async fn set_capture_suppressed(
    db: &impl ConnectionTrait,
    on: bool,
) -> Result<(), DbErr> {
    db.execute_unprepared(&format!(
        "UPDATE {GUARD_TABLE} SET suppressed = {} WHERE id = 1",
        if on { 1 } else { 0 }
    ))
    .await?;
    Ok(())
}

/// One captured row-level change, as stored by the triggers.
#[derive(Debug, Clone, FromQueryResult)]
pub(crate) struct CaptureRow {
    pub seq: i64,
    pub tbl: String,
    pub op: String,
    pub pk: String,
    pub new_row: Option<String>,
    pub old_row: Option<String>,
}

/// Read all pending capture rows in write order.
pub(crate) async fn fetch_capture_rows(
    db: &impl ConnectionTrait,
) -> Result<Vec<CaptureRow>, DbErr> {
    CaptureRow::find_by_statement(Statement::from_string(
        sea_orm::DatabaseBackend::Sqlite,
        format!("SELECT seq, tbl, op, pk, new_row, old_row FROM {CAPTURE_TABLE} ORDER BY seq"),
    ))
    .all(db)
    .await
}

/// Delete drained rows up to and including the watermark. Runs in the same
/// transaction as the shadow bookkeeping: either both commit or neither, so
/// an undrained capture row can never be lost.
pub(crate) async fn purge_capture_rows(
    db: &impl ConnectionTrait,
    max_seq: i64,
) -> Result<(), DbErr> {
    db.execute_unprepared(&format!(
        "DELETE FROM {CAPTURE_TABLE} WHERE seq <= {max_seq}"
    ))
    .await?;
    Ok(())
}

/// A logical row operation derived from one or two capture rows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalOp {
    /// All columns, in `TableMeta.columns` order.
    Insert {
        table: String,
        pk: String,
        cols: Vec<(String, serde_json::Value)>,
    },
    /// Changed columns only, in `TableMeta.columns` order.
    Update {
        table: String,
        pk: String,
        cols: Vec<(String, serde_json::Value)>,
    },
    Delete {
        table: String,
        pk: String,
    },
}

/// Render a JSON scalar the way `CAST(x AS TEXT)` renders the same SQLite
/// value, so a pk extracted from `new_row` matches the trigger-captured `pk`.
fn json_scalar_to_pk_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// Turn ordered capture rows into logical ops. Pure — unit-testable without
/// a database. Unregistered and internal tables are skipped (their rows are
/// purged by the drain without spending a db_version).
pub(crate) fn plan_logical_ops(rows: &[CaptureRow], registry: &TableRegistry) -> Vec<LogicalOp> {
    let mut ops = Vec::new();
    for row in rows {
        // Defense in depth: triggers are never created on internal tables,
        // but a stray row here must not re-enter the sync path.
        if row.tbl.starts_with("_wavesync") {
            continue;
        }
        let Some(meta) = registry.get(&row.tbl) else {
            continue;
        };
        match row.op.as_str() {
            "I" => {
                let Some(new) = parse_row_json(row.new_row.as_deref()) else {
                    continue;
                };
                ops.push(LogicalOp::Insert {
                    table: row.tbl.clone(),
                    pk: row.pk.clone(),
                    cols: cols_in_meta_order(&meta, &new, None),
                });
            }
            "D" => ops.push(LogicalOp::Delete {
                table: row.tbl.clone(),
                pk: row.pk.clone(),
            }),
            "U" => {
                let (Some(new), Some(old)) = (
                    parse_row_json(row.new_row.as_deref()),
                    parse_row_json(row.old_row.as_deref()),
                ) else {
                    continue;
                };
                let new_pk = new
                    .get(&meta.primary_key_column)
                    .map(json_scalar_to_pk_string)
                    .unwrap_or_else(|| row.pk.clone());
                if new_pk != row.pk {
                    // pk-changing UPDATE: the old row identity dies, the new
                    // one is born with the full column set.
                    ops.push(LogicalOp::Delete {
                        table: row.tbl.clone(),
                        pk: row.pk.clone(),
                    });
                    ops.push(LogicalOp::Insert {
                        table: row.tbl.clone(),
                        pk: new_pk,
                        cols: cols_in_meta_order(&meta, &new, None),
                    });
                } else {
                    let changed = cols_in_meta_order(&meta, &new, Some(&old));
                    if !changed.is_empty() {
                        ops.push(LogicalOp::Update {
                            table: row.tbl.clone(),
                            pk: row.pk.clone(),
                            cols: changed,
                        });
                    }
                    // A no-op UPDATE (all values identical) emits nothing.
                }
            }
            _ => {}
        }
    }
    ops
}

fn parse_row_json(raw: Option<&str>) -> Option<serde_json::Map<String, serde_json::Value>> {
    match serde_json::from_str(raw?) {
        Ok(serde_json::Value::Object(map)) => Some(map),
        _ => None,
    }
}

/// Project a captured JSON row onto the registered column list, preserving
/// `TableMeta.columns` order (NOT JSON key order). With `old`, only columns
/// whose value differs are returned.
fn cols_in_meta_order(
    meta: &TableMeta,
    new: &serde_json::Map<String, serde_json::Value>,
    old: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Vec<(String, serde_json::Value)> {
    meta.columns
        .iter()
        .filter_map(|c| {
            let v = new.get(c)?;
            if old.is_some_and(|o| o.get(c) == Some(v)) {
                return None;
            }
            Some((c.clone(), v.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::DeletePolicy;
    use sea_orm::Database;
    use serde_json::json;

    fn meta(table: &str, pk: &str, cols: &[&str]) -> TableMeta {
        TableMeta {
            table_name: table.to_string(),
            primary_key_column: pk.to_string(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
            delete_policy: DeletePolicy::default(),
        }
    }

    /// File-based temp DB: the pool must see ONE database (in-memory DBs are
    /// per-connection under the pool), and triggers are schema state.
    async fn setup_db() -> sea_orm::DatabaseConnection {
        let path =
            std::env::temp_dir().join(format!("wavesync_capture_{}.db", uuid::Uuid::new_v4()));
        let db = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT, done INTEGER NOT NULL DEFAULT 0, score REAL, data BLOB)",
        )
        .await
        .unwrap();
        ensure_capture_tables(&db).await.unwrap();
        ensure_triggers(&db, &tasks_meta()).await.unwrap();
        db
    }

    fn tasks_meta() -> TableMeta {
        meta("tasks", "id", &["id", "title", "done", "score", "data"])
    }

    async fn capture_rows(db: &sea_orm::DatabaseConnection) -> Vec<CaptureRow> {
        fetch_capture_rows(db).await.unwrap()
    }

    #[tokio::test]
    async fn test_insert_captured_with_json_object_spelling() {
        let db = setup_db().await;
        db.execute_unprepared(
            "INSERT INTO tasks (id, title, done, score) VALUES ('t1', 'hello world', 1, 2.5)",
        )
        .await
        .unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].op, "I");
        assert_eq!(rows[0].tbl, "tasks");
        assert_eq!(rows[0].pk, "t1");
        let new: serde_json::Value =
            serde_json::from_str(rows[0].new_row.as_deref().unwrap()).unwrap();
        assert_eq!(new["id"], json!("t1"));
        assert_eq!(new["title"], json!("hello world"));
        assert_eq!(new["done"], json!(1)); // SQLite spelling: 0/1, not true/false
        assert_eq!(new["score"], json!(2.5));
        assert_eq!(new["data"], serde_json::Value::Null);
        assert!(rows[0].old_row.is_none());
    }

    #[tokio::test]
    async fn test_update_captures_old_and_new() {
        let db = setup_db().await;
        db.execute_unprepared("INSERT INTO tasks (id, title) VALUES ('t1', 'a')")
            .await
            .unwrap();
        db.execute_unprepared("UPDATE tasks SET title = 'b', done = done + 1 WHERE id = 't1'")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 2);
        let u = &rows[1];
        assert_eq!(u.op, "U");
        assert_eq!(u.pk, "t1");
        let old: serde_json::Value = serde_json::from_str(u.old_row.as_deref().unwrap()).unwrap();
        let new: serde_json::Value = serde_json::from_str(u.new_row.as_deref().unwrap()).unwrap();
        assert_eq!(old["title"], json!("a"));
        assert_eq!(new["title"], json!("b"));
        // Expression UPDATE captures the COMPUTED value — the whole point.
        assert_eq!(old["done"], json!(0));
        assert_eq!(new["done"], json!(1));
    }

    #[tokio::test]
    async fn test_delete_captured() {
        let db = setup_db().await;
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('t1')")
            .await
            .unwrap();
        db.execute_unprepared("DELETE FROM tasks WHERE id = 't1'")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].op, "D");
        assert_eq!(rows[1].pk, "t1");
        assert!(rows[1].new_row.is_none());
    }

    #[tokio::test]
    async fn test_blob_captured_as_lowercase_hex() {
        let db = setup_db().await;
        db.execute_unprepared("INSERT INTO tasks (id, data) VALUES ('t1', X'DEADBEEF')")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        let new: serde_json::Value =
            serde_json::from_str(rows[0].new_row.as_deref().unwrap()).unwrap();
        assert_eq!(new["data"], json!("deadbeef"));
    }

    #[tokio::test]
    async fn test_suppression_gates_capture() {
        let db = setup_db().await;
        set_capture_suppressed(&db, true).await.unwrap();
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('quiet')")
            .await
            .unwrap();
        assert!(capture_rows(&db).await.is_empty());
        set_capture_suppressed(&db, false).await.unwrap();
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('loud')")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk, "loud");
    }

    #[tokio::test]
    async fn test_or_replace_fires_single_insert_capture() {
        let db = setup_db().await;
        db.execute_unprepared("INSERT INTO tasks (id, title) VALUES ('t1', 'a')")
            .await
            .unwrap();
        // recursive_triggers is OFF (SQLite default): the REPLACE conflict
        // resolution fires no DELETE trigger; only the INSERT is captured,
        // and its full column set supersedes the old cells on receivers.
        db.execute_unprepared("INSERT OR REPLACE INTO tasks (id, title) VALUES ('t1', 'b')")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.op == "I"));
        let new: serde_json::Value =
            serde_json::from_str(rows[1].new_row.as_deref().unwrap()).unwrap();
        assert_eq!(new["title"], json!("b"));
    }

    #[tokio::test]
    async fn test_refuses_internal_tables() {
        let m = meta("_wavesync_meta", "key", &["key", "value"]);
        assert!(trigger_sql(&m).is_err());
    }

    #[tokio::test]
    async fn test_triggers_recreated_on_schema_change_only() {
        let db = setup_db().await;
        // Same meta again: no-op (names carry the schema fingerprint).
        ensure_triggers(&db, &tasks_meta()).await.unwrap();
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('t1')")
            .await
            .unwrap();
        assert_eq!(capture_rows(&db).await.len(), 1);

        // Schema evolves: add a column, re-ensure — new triggers capture it.
        db.execute_unprepared("ALTER TABLE tasks ADD COLUMN extra TEXT")
            .await
            .unwrap();
        let evolved = meta(
            "tasks",
            "id",
            &["id", "title", "done", "score", "data", "extra"],
        );
        ensure_triggers(&db, &evolved).await.unwrap();
        db.execute_unprepared("INSERT INTO tasks (id, extra) VALUES ('t2', 'x')")
            .await
            .unwrap();
        let rows = capture_rows(&db).await;
        let new: serde_json::Value =
            serde_json::from_str(rows.last().unwrap().new_row.as_deref().unwrap()).unwrap();
        assert_eq!(new["extra"], json!("x"));

        // Exactly three triggers remain for the table (stale set dropped).
        #[derive(FromQueryResult)]
        struct CountRow {
            cnt: i64,
        }
        let row = CountRow::find_by_statement(Statement::from_string(
            sea_orm::DatabaseBackend::Sqlite,
            "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='trigger' AND name LIKE '_wavesync_tr_tasks_%'".to_string(),
        ))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
        assert_eq!(row.cnt, 3);
    }

    #[tokio::test]
    async fn test_purge_is_watermark_bounded() {
        let db = setup_db().await;
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('t1')")
            .await
            .unwrap();
        let watermark = capture_rows(&db).await[0].seq;
        db.execute_unprepared("INSERT INTO tasks (id) VALUES ('t2')")
            .await
            .unwrap();
        purge_capture_rows(&db, watermark).await.unwrap();
        let rows = capture_rows(&db).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pk, "t2");
    }

    // ---- plan_logical_ops (pure) ----

    fn registry_with_tasks() -> TableRegistry {
        let r = TableRegistry::new();
        r.register(meta("tasks", "id", &["id", "title", "done"]));
        r
    }

    fn crow(seq: i64, op: &str, pk: &str, new: Option<&str>, old: Option<&str>) -> CaptureRow {
        CaptureRow {
            seq,
            tbl: "tasks".into(),
            op: op.into(),
            pk: pk.into(),
            new_row: new.map(String::from),
            old_row: old.map(String::from),
        }
    }

    #[test]
    fn test_plan_insert_all_columns_meta_order() {
        let ops = plan_logical_ops(
            &[crow(
                1,
                "I",
                "t1",
                Some(r#"{"done":0,"id":"t1","title":null}"#),
                None,
            )],
            &registry_with_tasks(),
        );
        assert_eq!(
            ops,
            vec![LogicalOp::Insert {
                table: "tasks".into(),
                pk: "t1".into(),
                // meta order (id, title, done), NOT JSON key order; NULL kept.
                cols: vec![
                    ("id".into(), json!("t1")),
                    ("title".into(), serde_json::Value::Null),
                    ("done".into(), json!(0)),
                ],
            }]
        );
    }

    #[test]
    fn test_plan_update_changed_columns_only_and_noop_skipped() {
        let ops = plan_logical_ops(
            &[
                crow(
                    1,
                    "U",
                    "t1",
                    Some(r#"{"id":"t1","title":"b","done":0}"#),
                    Some(r#"{"id":"t1","title":"a","done":0}"#),
                ),
                // no-op UPDATE: identical old/new emits nothing
                crow(
                    2,
                    "U",
                    "t1",
                    Some(r#"{"id":"t1","title":"b","done":0}"#),
                    Some(r#"{"id":"t1","title":"b","done":0}"#),
                ),
            ],
            &registry_with_tasks(),
        );
        assert_eq!(
            ops,
            vec![LogicalOp::Update {
                table: "tasks".into(),
                pk: "t1".into(),
                cols: vec![("title".into(), json!("b"))],
            }]
        );
    }

    #[test]
    fn test_plan_pk_change_becomes_delete_plus_insert() {
        let ops = plan_logical_ops(
            &[crow(
                1,
                "U",
                "old-pk",
                Some(r#"{"id":"new-pk","title":"a","done":1}"#),
                Some(r#"{"id":"old-pk","title":"a","done":1}"#),
            )],
            &registry_with_tasks(),
        );
        assert_eq!(ops.len(), 2);
        assert_eq!(
            ops[0],
            LogicalOp::Delete {
                table: "tasks".into(),
                pk: "old-pk".into()
            }
        );
        match &ops[1] {
            LogicalOp::Insert { pk, cols, .. } => {
                assert_eq!(pk, "new-pk");
                assert_eq!(cols.len(), 3);
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn test_plan_skips_unregistered_and_internal() {
        let mut stray = crow(1, "I", "x", Some(r#"{"id":"x"}"#), None);
        stray.tbl = "unknown".into();
        let mut internal = crow(2, "I", "y", Some(r#"{"id":"y"}"#), None);
        internal.tbl = "_wavesync_meta".into();
        let ops = plan_logical_ops(&[stray, internal], &registry_with_tasks());
        assert!(ops.is_empty());
    }

    #[test]
    fn test_plan_numeric_pk_string_matches_cast() {
        // integer pk arrives as JSON number in new_row; CAST(5 AS TEXT)='5'
        let r = TableRegistry::new();
        r.register(meta("nums", "n", &["n", "v"]));
        let mut row = crow(
            1,
            "U",
            "5",
            Some(r#"{"n":6,"v":"a"}"#),
            Some(r#"{"n":5,"v":"a"}"#),
        );
        row.tbl = "nums".into();
        let ops = plan_logical_ops(&[row], &r);
        assert_eq!(ops.len(), 2, "pk 5→6 must split into delete+insert");
        assert_eq!(
            ops[0],
            LogicalOp::Delete {
                table: "nums".into(),
                pk: "5".into()
            }
        );
    }
}
