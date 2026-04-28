#![cfg(feature = "dioxus")]

mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, Set};
use wavesyncdb::{ChangeNotification, SyncedModel, WaveSyncDbBuilder, WriteKind};

use common::mem_db;
use common::task;

// ---------------------------------------------------------------------------
// Test 1: change_rx table filter pattern (simulates hook recv loop)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_change_rx_table_filter_pattern() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_filt"), "test-dioxus-filt")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    // Create another table (unregistered)
    db.execute_unprepared("CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT)")
        .await
        .unwrap();

    let mut rx = db.change_rx();
    let target_table = "tasks";

    // Send a notification for a different table manually
    db.change_tx()
        .send(ChangeNotification {
            table: "notes".into(),
            kind: WriteKind::Insert,
            primary_key: "note-1".into(),
            changed_columns: Some(vec!["id".to_string(), "body".to_string()]),
            column_values: None,
        })
        .unwrap();

    // Send a notification for the target table
    db.change_tx()
        .send(ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Insert,
            primary_key: "task-1".into(),
            changed_columns: Some(vec!["id".to_string(), "title".to_string()]),
            column_values: None,
        })
        .unwrap();

    // Simulate the hook's filter loop
    let mut matched = Vec::new();
    for _ in 0..2 {
        match rx.try_recv() {
            Ok(notif) if notif.table == target_table => matched.push(notif),
            Ok(_) => { /* skip non-matching table */ }
            Err(_) => break,
        }
    }

    assert_eq!(matched.len(), 1, "Only tasks notification should match");
    assert_eq!(matched[0].primary_key, "task-1");
}

// ---------------------------------------------------------------------------
// Test 2: change_rx Lagged triggers re-query (simulated)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_change_rx_lagged_triggers_requery() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_lag"), "test-dioxus-lag")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    // Flood the channel past its 1024 capacity
    for i in 0..1100 {
        let _ = db.change_tx().send(ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Insert,
            primary_key: format!("task-{}", i).into(),
            changed_columns: None,
            column_values: None,
        });
    }

    // The Dioxus hook pattern: on Lagged, fall through to re-query
    let mut should_requery = false;
    loop {
        match rx.try_recv() {
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                should_requery = true;
                break;
            }
            Err(_) => break,
        }
    }

    assert!(
        should_requery,
        "Lagged error should trigger re-query in the hook pattern"
    );
}

// ---------------------------------------------------------------------------
// Test 3: column_values populated on local INSERT
// ---------------------------------------------------------------------------
#[tokio::test]
async fn notification_carries_column_values_for_local_insert() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_insert"), "test-dioxus-insert")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    task::ActiveModel {
        id: Set("task-7".into()),
        title: Set("Buy milk".into()),
        completed: Set(false),
    }
    .insert(&db)
    .await
    .unwrap();

    let notif = rx.recv().await.unwrap();
    assert_eq!(notif.table, "tasks");
    assert_eq!(notif.kind, WriteKind::Insert);
    assert_eq!(notif.primary_key, "task-7");
    let cols = notif
        .column_values
        .as_ref()
        .expect("column_values should be Some on a local Insert");
    let map: std::collections::HashMap<&str, &serde_json::Value> =
        cols.iter().map(|(c, v)| (c.0.as_str(), v)).collect();
    assert_eq!(
        map.get("title").map(|v| v.as_str().unwrap()),
        Some("Buy milk")
    );
    assert_eq!(map.get("completed").and_then(|v| v.as_bool()), Some(false));
}

// ---------------------------------------------------------------------------
// Test 4: column_values populated on local UPDATE
// ---------------------------------------------------------------------------
#[tokio::test]
async fn notification_carries_column_values_for_local_update() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_upd"), "test-dioxus-upd")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    task::ActiveModel {
        id: Set("task-1".into()),
        title: Set("old".into()),
        completed: Set(false),
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    task::ActiveModel {
        id: Set("task-1".into()),
        title: Set("new".into()),
        completed: Set(false),
    }
    .update(&db)
    .await
    .unwrap();

    let notif = rx.recv().await.unwrap();
    assert_eq!(notif.kind, WriteKind::Update);
    assert_eq!(notif.primary_key, "task-1");
    let cols = notif
        .column_values
        .as_ref()
        .expect("column_values should be Some on a local Update");
    let map: std::collections::HashMap<&str, &serde_json::Value> =
        cols.iter().map(|(c, v)| (c.0.as_str(), v)).collect();
    assert_eq!(map.get("title").and_then(|v| v.as_str()), Some("new"));
}

// ---------------------------------------------------------------------------
// Test 5: SyncedModel::wavesync_apply_change patches a model in place
// ---------------------------------------------------------------------------
#[tokio::test]
async fn synced_model_apply_change_roundtrips() {
    let mut model = task::Model {
        id: "task-42".into(),
        title: "old".into(),
        completed: false,
    };
    SyncedModel::wavesync_apply_change(&mut model, "title", &serde_json::json!("new"));
    assert_eq!(model.title, "new");
    SyncedModel::wavesync_apply_change(&mut model, "completed", &serde_json::json!(true));
    assert!(model.completed);

    // Unknown column is silently ignored.
    SyncedModel::wavesync_apply_change(&mut model, "nonexistent", &serde_json::json!("x"));
    assert_eq!(model.title, "new");
}

// ---------------------------------------------------------------------------
// Test 6: SyncedModel::wavesync_from_changes builds a complete model
// ---------------------------------------------------------------------------
#[tokio::test]
async fn synced_model_from_changes_builds_full_model() {
    let changes = vec![
        ("id".to_string(), serde_json::json!("task-9")),
        ("title".to_string(), serde_json::json!("Hello")),
        ("completed".to_string(), serde_json::json!(true)),
    ];
    let m = task::Model::wavesync_from_changes("id", "task-9", &changes)
        .expect("from_changes should succeed when all fields are present");
    assert_eq!(m.id, "task-9");
    assert_eq!(m.title, "Hello");
    assert!(m.completed);
}

// ---------------------------------------------------------------------------
// Test 7: from_changes returns None when a required field is missing
// ---------------------------------------------------------------------------
#[tokio::test]
async fn synced_model_from_changes_returns_none_when_required_field_missing() {
    let changes = vec![
        ("id".to_string(), serde_json::json!("task-9")),
        // title intentionally omitted
        ("completed".to_string(), serde_json::json!(true)),
    ];
    let m = task::Model::wavesync_from_changes("id", "task-9", &changes);
    assert!(m.is_none(), "Missing non-Option field should yield None");
}

// ---------------------------------------------------------------------------
// Test 8: from_changes recovers the pk from pk_value alone
// ---------------------------------------------------------------------------
#[tokio::test]
async fn synced_model_from_changes_pk_fallback() {
    let changes = vec![
        ("title".to_string(), serde_json::json!("Hi")),
        ("completed".to_string(), serde_json::json!(false)),
    ];
    let m = task::Model::wavesync_from_changes("id", "task-pk-fallback", &changes)
        .expect("pk_value should populate the missing pk field");
    assert_eq!(m.id, "task-pk-fallback");
}

// ---------------------------------------------------------------------------
// Test 9: row hook filter pattern — pk-mismatched notifs are skipped
// ---------------------------------------------------------------------------
#[tokio::test]
async fn row_hook_filter_by_pk_pattern() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_pk"), "test-dioxus-pk")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();
    let target_pk = "task-watch";

    // Different row in same table — should be skipped by the hook.
    db.change_tx()
        .send(ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Update,
            primary_key: "task-other".into(),
            changed_columns: Some(vec!["title".into()]),
            column_values: Some(vec![(
                wavesyncdb::ColumnName("title".into()),
                serde_json::json!("ignored"),
            )]),
        })
        .unwrap();

    // The watched row.
    db.change_tx()
        .send(ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Update,
            primary_key: target_pk.into(),
            changed_columns: Some(vec!["title".into()]),
            column_values: Some(vec![(
                wavesyncdb::ColumnName("title".into()),
                serde_json::json!("hello"),
            )]),
        })
        .unwrap();

    let mut matched = 0usize;
    for _ in 0..2 {
        let notif = rx.recv().await.unwrap();
        if notif.table == "tasks" && notif.primary_key.0 == target_pk {
            matched += 1;
        }
    }
    assert_eq!(
        matched, 1,
        "Only the watched-row notification should pass the (table, pk) filter"
    );
}
