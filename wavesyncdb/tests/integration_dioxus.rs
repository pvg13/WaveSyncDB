#![cfg(feature = "dioxus")]

mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use wavesyncdb::{ChangeNotification, WaveSyncDbBuilder, WriteKind};

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
        })
        .unwrap();

    // Send a notification for the target table
    db.change_tx()
        .send(ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Insert,
            primary_key: "task-1".into(),
            changed_columns: Some(vec!["id".to_string(), "title".to_string()]),
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
