// Native-only (real engine, SQLite, libp2p). `wasm-pack test` runs `cargo
// build --tests`, which builds every integration test binary in the crate
// regardless of which one is named to run — without this gate, this file's
// native-only imports would fail a wasm32 test build.
#![cfg(all(feature = "dioxus", not(target_arch = "wasm32")))]

mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, Set};
use wavesyncdb::{ChangeNotification, SyncedModel, WaveSyncDbBuilder, WriteKind};

use common::mem_db;
use common::task;

use std::sync::{Arc, Mutex};

/// Collects every published snapshot; the test asserts the exact sequence.
fn collector<T: Send + 'static>() -> (Arc<Mutex<Vec<T>>>, impl FnMut(T) + Send + 'static) {
    let sink: Arc<Mutex<Vec<T>>> = Arc::new(Mutex::new(Vec::new()));
    let s2 = sink.clone();
    (sink, move |v: T| s2.lock().unwrap().push(v))
}

async fn wait_for<F: Fn() -> bool>(cond: F, what: &str) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

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
            source: wavesyncdb::ChangeSource::Local,
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
            source: wavesyncdb::ChangeSource::Local,
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
            source: wavesyncdb::ChangeSource::Local,
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
    // Unified SQLite json_object() spelling: booleans travel as 0/1. The
    // lenient model decode bridges this back to `bool` for hooks.
    assert_eq!(map.get("completed").and_then(|v| v.as_i64()), Some(0));
    let rebuilt = <task::Model as SyncedModel>::wavesync_from_changes(
        "id",
        "task-7",
        &cols
            .iter()
            .map(|(c, v)| (c.0.clone(), v.clone()))
            .collect::<Vec<_>>(),
    )
    .expect("lenient decode must rebuild the model from 0/1 booleans");
    assert!(!rebuilt.completed);
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
            source: wavesyncdb::ChangeSource::Local,
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
            source: wavesyncdb::ChangeSource::Local,
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

// ---------------------------------------------------------------------------
// H7 knob: broadcast capacity is configurable; a small capacity laggs a
// slow subscriber quickly, proving the knob reaches the channel.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn change_channel_capacity_knob_is_respected() {
    let db = WaveSyncDbBuilder::new(&mem_db("dioxus_cap"), "test-dioxus-cap")
        .with_change_channel_capacity(32)
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();
    for i in 0..60 {
        db.execute_unprepared(&format!(
            "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('cap-{i}', 't', 0)"
        ))
        .await
        .unwrap();
    }

    let mut got_lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                got_lagged = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(
        got_lagged,
        "60 writes must overflow a capacity-32 channel; the knob did not reach the channel"
    );
}

// ---------------------------------------------------------------------------
// Driver: initial load publishes exactly once (even for an EMPTY table —
// the observable "loaded" moment issue #1 needs), then per-change
// snapshots arrive via in-place application.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn table_driver_publishes_initial_and_incremental_snapshots() {
    let db = WaveSyncDbBuilder::new(&mem_db("drv_tbl"), "test-drv-tbl")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let (sink, publish) = collector::<Vec<task::Model>>();
    let drv_db = db.clone();
    let drv = tokio::spawn(wavesyncdb::dioxus::run_table_driver::<task::Entity>(
        drv_db, publish,
    ));

    // 1. Initial publish fires even though the table is empty.
    wait_for(|| sink.lock().unwrap().len() == 1, "initial empty publish").await;
    assert!(sink.lock().unwrap()[0].is_empty());

    // 2. Insert → snapshot with the row.
    task::ActiveModel {
        id: Set("d1".into()),
        title: Set("one".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();
    wait_for(|| sink.lock().unwrap().len() >= 2, "insert publish").await;
    assert_eq!(sink.lock().unwrap().last().unwrap()[0].title, "one");

    // 3. Update → in-place snapshot.
    db.execute_unprepared("UPDATE tasks SET title = 'two' WHERE id = 'd1'")
        .await
        .unwrap();
    wait_for(
        || {
            sink.lock()
                .unwrap()
                .last()
                .unwrap()
                .first()
                .is_some_and(|m| m.title == "two")
        },
        "update publish",
    )
    .await;

    // 4. Delete → empty snapshot again.
    db.execute_unprepared("DELETE FROM tasks WHERE id = 'd1'")
        .await
        .unwrap();
    wait_for(
        || sink.lock().unwrap().last().unwrap().is_empty(),
        "delete publish",
    )
    .await;

    drv.abort();
}

// ---------------------------------------------------------------------------
// Driver under a lagged burst: with a small channel, >capacity writes force
// Lagged; the driver's debounced full reload must still converge the
// published snapshot to the true table.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn table_driver_lagged_burst_converges() {
    let db = WaveSyncDbBuilder::new(&mem_db("drv_lag"), "test-drv-lag")
        .with_change_channel_capacity(32)
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let (sink, publish) = collector::<Vec<task::Model>>();
    let drv = tokio::spawn(wavesyncdb::dioxus::run_table_driver::<task::Entity>(
        db.clone(),
        publish,
    ));
    wait_for(|| sink.lock().unwrap().len() == 1, "initial publish").await;

    for i in 0..120 {
        db.execute_unprepared(&format!(
            "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('lag-{i}', 't', 0)"
        ))
        .await
        .unwrap();
    }

    // LAGGED_DEBOUNCE is 500ms; give the reload path a couple of cycles.
    wait_for(
        || {
            sink.lock()
                .unwrap()
                .last()
                .is_some_and(|rows| rows.len() == 120)
        },
        "lagged full reload converges to 120 rows",
    )
    .await;
    drv.abort();
}

// ---------------------------------------------------------------------------
// M10 at the driver layer: cancelling the old driver and starting one on a
// fresh engine (what the generation-reactive effect does) must track the
// new DB — and dropping the old task must release the old engine.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn driver_swap_tracks_new_engine() {
    let db1 = WaveSyncDbBuilder::new(&mem_db("swap_1"), "test-swap-1")
        .build()
        .await
        .unwrap();
    db1.schema().register(task::Entity).sync().await.unwrap();

    let (sink, publish) = collector::<Vec<task::Model>>();
    let drv1 = tokio::spawn(wavesyncdb::dioxus::run_table_driver::<task::Entity>(
        db1.clone(),
        publish,
    ));
    wait_for(|| sink.lock().unwrap().len() == 1, "db1 initial publish").await;

    // Engine swap: cancel the old driver FIRST (as the effect does), then
    // shut down db1 and bring up db2.
    drv1.abort();
    db1.shutdown().await;
    drop(db1);

    let db2 = WaveSyncDbBuilder::new(&mem_db("swap_2"), "test-swap-2")
        .build()
        .await
        .unwrap();
    db2.schema().register(task::Entity).sync().await.unwrap();
    task::ActiveModel {
        id: Set("after-swap".into()),
        title: Set("from db2".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db2)
    .await
    .unwrap();

    let (sink2, publish2) = collector::<Vec<task::Model>>();
    let drv2 = tokio::spawn(wavesyncdb::dioxus::run_table_driver::<task::Entity>(
        db2.clone(),
        publish2,
    ));
    wait_for(
        || {
            sink2
                .lock()
                .unwrap()
                .last()
                .is_some_and(|rows| rows.iter().any(|m| m.id == "after-swap"))
        },
        "db2 rows published",
    )
    .await;

    // The old sink must not have received anything after the swap.
    let count_after_swap = sink.lock().unwrap().len();
    assert_eq!(count_after_swap, 1, "cancelled driver must not publish");
    drv2.abort();
}

// ---------------------------------------------------------------------------
// Row driver: initial publish (None for a missing row = the observable
// "loaded, absent" moment), then in-place update and delete snapshots.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn row_driver_publishes_initial_and_incremental_snapshots() {
    let db = WaveSyncDbBuilder::new(&mem_db("drv_row"), "test-drv-row")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let (sink, publish) = collector::<Option<task::Model>>();
    let drv = tokio::spawn(wavesyncdb::dioxus::run_row_driver::<task::Entity>(
        db.clone(),
        "r1".to_string(),
        publish,
    ));

    // Initial publish fires with None — the row doesn't exist yet.
    wait_for(|| sink.lock().unwrap().len() == 1, "initial None publish").await;
    assert!(sink.lock().unwrap()[0].is_none());

    task::ActiveModel {
        id: Set("r1".into()),
        title: Set("row".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();
    wait_for(
        || {
            sink.lock()
                .unwrap()
                .last()
                .unwrap()
                .as_ref()
                .is_some_and(|m| m.title == "row")
        },
        "insert publish",
    )
    .await;

    db.execute_unprepared("DELETE FROM tasks WHERE id = 'r1'")
        .await
        .unwrap();
    wait_for(
        || sink.lock().unwrap().last().unwrap().is_none(),
        "delete publish",
    )
    .await;

    drv.abort();
}

// ---------------------------------------------------------------------------
// Issue #1 contract: before the initial load resolves NOTHING is
// published (a _loaded consumer reads None = loading); the FIRST publish
// is the loaded snapshot, even when the table is legitimately empty.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn loaded_contract_none_until_first_publish_even_when_empty() {
    let db = WaveSyncDbBuilder::new(&mem_db("drv_loaded"), "test-drv-loaded")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    // Simulate the _loaded wrapper: publish wraps into Some(...).
    let state: Arc<Mutex<Option<Vec<task::Model>>>> = Arc::new(Mutex::new(None));
    let s2 = state.clone();
    let drv = tokio::spawn(wavesyncdb::dioxus::run_table_driver::<task::Entity>(
        db.clone(),
        move |rows| *s2.lock().unwrap() = Some(rows),
    ));

    // The distinguishable moment: None (loading) → Some([]) (loaded-empty).
    wait_for(
        || state.lock().unwrap().is_some(),
        "loaded-empty transition",
    )
    .await;
    assert_eq!(state.lock().unwrap().as_deref(), Some(&[][..]));
    drv.abort();
}
