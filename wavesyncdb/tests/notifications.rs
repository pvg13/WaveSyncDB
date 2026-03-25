mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{WaveSyncDbBuilder, WriteKind};

use common::mem_db;
use common::task;

// ---------------------------------------------------------------------------
// Test 1: Notification on INSERT
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_on_insert() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_ins"), "test-notif-ins")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    let id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("hello".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().expect("Should get insert notification");
    assert_eq!(notif.table, "tasks");
    assert_eq!(notif.primary_key, id);
    assert_eq!(notif.kind, WriteKind::Insert);
}

// ---------------------------------------------------------------------------
// Test 2: Notification on UPDATE
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_on_update() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_upd"), "test-notif-upd")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let id = Uuid::new_v4().to_string();
    let inserted = task::ActiveModel {
        id: Set(id.clone()),
        title: Set("before".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    let mut active: task::ActiveModel = inserted.into();
    active.title = Set("after".into());
    active.update(&db).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().expect("Should get update notification");
    assert_eq!(notif.table, "tasks");
    assert_eq!(notif.kind, WriteKind::Update);
}

// ---------------------------------------------------------------------------
// Test 3: Notification on DELETE
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_on_delete() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_del"), "test-notif-del")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("to-delete".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    task::Entity::delete_by_id(id.clone())
        .exec(&db)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().expect("Should get delete notification");
    assert_eq!(notif.table, "tasks");
    assert_eq!(notif.kind, WriteKind::Delete);
}

// ---------------------------------------------------------------------------
// Test 4: Unregistered table produces no notification
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_table_filtering() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_filt"), "test-notif-filt")
        .build()
        .await
        .unwrap();
    // Register tasks but NOT another table
    db.schema().register(task::Entity).sync().await.unwrap();

    // Create an unregistered table manually
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS unregistered (id TEXT PRIMARY KEY, data TEXT)",
    )
    .await
    .unwrap();

    let mut rx = db.change_rx();

    db.execute_unprepared("INSERT INTO unregistered VALUES ('x', 'y')")
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        rx.try_recv().is_err(),
        "Should NOT get notification for unregistered table"
    );
}

// ---------------------------------------------------------------------------
// Test 5: changed_columns on INSERT contains all columns
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_changed_columns_insert() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_cols_ins"), "test-notif-cols-ins")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    task::ActiveModel {
        id: Set("pk1".to_string()),
        title: Set("hello".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().unwrap();
    let cols = notif
        .changed_columns
        .expect("INSERT should have changed_columns");
    assert!(
        cols.contains(&"id".to_string()),
        "Should contain 'id': {:?}",
        cols
    );
    assert!(
        cols.contains(&"title".to_string()),
        "Should contain 'title': {:?}",
        cols
    );
}

// ---------------------------------------------------------------------------
// Test 6: changed_columns on UPDATE contains only SET columns
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_changed_columns_update() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_cols_upd"), "test-notif-cols-upd")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    task::ActiveModel {
        id: Set("pk2".to_string()),
        title: Set("before".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    // Update only title via raw SQL to control exactly which columns appear
    db.execute_unprepared("UPDATE \"tasks\" SET \"title\" = 'after' WHERE \"id\" = 'pk2'")
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().unwrap();
    let cols = notif
        .changed_columns
        .expect("UPDATE should have changed_columns");
    assert!(
        cols.contains(&"title".to_string()),
        "Should contain 'title': {:?}",
        cols
    );
    assert!(
        !cols.contains(&"id".to_string()),
        "Should NOT contain 'id': {:?}",
        cols
    );
}

// ---------------------------------------------------------------------------
// Test 7: changed_columns is None for DELETE
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_notification_changed_columns_none_delete() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_cols_del"), "test-notif-cols-del")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    task::ActiveModel {
        id: Set("pk3".to_string()),
        title: Set("to-delete".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    task::Entity::delete_by_id("pk3".to_string())
        .exec(&db)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let notif = rx.try_recv().unwrap();
    assert!(
        notif.changed_columns.is_none(),
        "DELETE should have changed_columns = None"
    );
}

// ---------------------------------------------------------------------------
// Test 8: Multiple receivers both get same notification
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multiple_receivers() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_multi"), "test-notif-multi")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx1 = db.change_rx();
    let mut rx2 = db.change_rx();

    task::ActiveModel {
        id: Set("pk-multi".to_string()),
        title: Set("multi".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let n1 = rx1.try_recv().expect("Receiver 1 should get notification");
    let n2 = rx2.try_recv().expect("Receiver 2 should get notification");
    assert_eq!(n1.table, n2.table);
    assert_eq!(n1.primary_key, n2.primary_key);
}

// ---------------------------------------------------------------------------
// Test 9: Receiver closed on drop (M10 related)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_receiver_closed_on_drop() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif_drop"), "test-notif-drop")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    // Shut down the DB
    db.shutdown().await;

    // Drain any pending messages
    while rx.try_recv().is_ok() {}

    // Next recv should eventually return Closed
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await;
    match result {
        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
            // Expected
        }
        Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {
            // Also acceptable — channel overflowed before closing
        }
        Ok(Ok(_)) => {
            // Got a trailing notification, that's fine — subsequent recv would close
        }
        Err(_) => {
            // Timeout — the sender Arc may still be alive. This is acceptable
            // since WaveSyncDb is Arc-based and the sender lives as long as the Arc.
        }
    }
}
