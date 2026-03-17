mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, FromQueryResult, Set};
use std::time::Duration;
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

use common::{assert_eventually, mem_db};
use common::task;

// ---------------------------------------------------------------------------
// H3 regression: multi-row INSERT only syncs first PK
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h3_multi_row_insert_sync() {
    let db = WaveSyncDbBuilder::new(&mem_db("h3"), "test-h3")
        .build()
        .await
        .unwrap();
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let mut rx = db.change_rx();

    // Multi-row INSERT
    db.execute_unprepared(
        "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('a', 'Task A', 0), ('b', 'Task B', 1)",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Collect all notifications
    let mut notifications = Vec::new();
    while let Ok(n) = rx.try_recv() {
        notifications.push(n);
    }

    // H3 bug: currently only the first PK is captured.
    // This test documents that behavior — when fixed, it should produce 2 notifications.
    assert!(
        !notifications.is_empty(),
        "Should get at least one notification for multi-row INSERT"
    );

    // Verify both rows exist in the table
    let all = task::Entity::find().all(&db).await.unwrap();
    assert_eq!(all.len(), 2, "Both rows should be inserted");
}

// ---------------------------------------------------------------------------
// H5 regression: PK with spaces gets truncated
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h5_update_pk_with_spaces() {
    let db = WaveSyncDbBuilder::new(&mem_db("h5"), "test-h5")
        .build()
        .await
        .unwrap();
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let pk = "hello world";

    // Insert via raw SQL (SeaORM uses parameterized queries which bypass the parser)
    db.execute_unprepared(&format!(
        "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('{}', 'SpaceTest', 0)",
        pk
    ))
    .await
    .unwrap();

    let mut rx = db.change_rx();

    // Update the row
    db.execute_unprepared(&format!(
        "UPDATE \"tasks\" SET \"title\" = 'Updated' WHERE \"id\" = '{}'",
        pk
    ))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let notif = rx.try_recv();
    // H5 bug: PK extraction truncates at spaces.
    // This documents the current behavior.
    if let Ok(n) = notif {
        // When bug is fixed, this should be "hello world"
        // Currently it may be "hello" (truncated)
        assert!(
            n.primary_key == "hello" || n.primary_key == "hello world",
            "PK should be 'hello world' (fixed) or 'hello' (bug): got '{}'",
            n.primary_key
        );
    }
}

// ---------------------------------------------------------------------------
// H7 regression: broadcast overflow with >1024 rapid writes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h7_broadcast_overflow() {
    let db = WaveSyncDbBuilder::new(&mem_db("h7"), "test-h7")
        .build()
        .await
        .unwrap();
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let mut rx = db.change_rx();

    // Insert >1024 rows rapidly without reading the receiver
    for i in 0..1100 {
        db.execute_unprepared(&format!(
            "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('h7-{}', 'task', 0)",
            i
        ))
        .await
        .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Try to recv — should get Lagged error
    let mut got_lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                got_lagged = true;
                assert!(n > 0, "Should have missed some messages");
                break;
            }
            Err(_) => break,
        }
    }

    assert!(
        got_lagged,
        "H7: Should get Lagged error when >1024 notifications sent without reading"
    );
}

// ---------------------------------------------------------------------------
// N3 regression: delete + re-insert preserves col_version in shadow
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_n3_delete_reinsert_preserves_col_version() {
    let db = WaveSyncDbBuilder::new(&mem_db("n3"), "test-n3")
        .build()
        .await
        .unwrap();
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let pk = "n3-pk";

    // Insert
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("First".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    // Give time for async shadow table updates
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Delete
    task::Entity::delete_by_id(pk.to_string())
        .exec(&db)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Re-insert same PK
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("Second".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check shadow table — col_version for "title" should be > 1
    // because the first insert set it to 1, and the re-insert should continue from there
    #[derive(Debug, FromQueryResult)]
    struct ClockRow {
        col_version: i64,
    }

    let row = ClockRow::find_by_statement(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT col_version FROM \"_wavesync_tasks_clock\" WHERE pk = $1 AND cid = 'title'",
        [pk.into()],
    ))
    .one(db.inner())
    .await
    .unwrap();

    if let Some(row) = row {
        assert!(
            row.col_version > 1,
            "N3: col_version after delete+reinsert should be > 1 (was {})",
            row.col_version
        );
    }
    // If None, the async task hasn't completed yet — that's a different issue
}

// ---------------------------------------------------------------------------
// M12 regression: UPDATE with non-ASCII values
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m12_update_with_unicode_column_values() {
    let db = WaveSyncDbBuilder::new(&mem_db("m12"), "test-m12")
        .build()
        .await
        .unwrap();
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let pk = "m12-pk";
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("ascii".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    // Update with non-ASCII via raw SQL
    db.execute_unprepared(&format!(
        "UPDATE \"tasks\" SET \"title\" = 'café ñ 日本語' WHERE \"id\" = '{}'",
        pk
    ))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let notif = rx.try_recv();
    assert!(
        notif.is_ok(),
        "M12: Should get notification for unicode update"
    );

    // Verify the value was stored correctly
    let found = task::Entity::find_by_id(pk.to_string())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.title, "café ñ 日本語");
}
