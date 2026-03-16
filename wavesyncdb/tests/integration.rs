mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

use common::{assert_eventually, make_node_id, mem_db, task};

#[tokio::test]
async fn test_wavesyncdb_basic_crud() {
    let db = WaveSyncDbBuilder::new(&mem_db("crud"), "test-crud")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");

    // INSERT
    let new_task = task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("Buy milk".into()),
        completed: Set(false),
        ..Default::default()
    };
    let inserted = new_task.insert(&db).await.expect("Failed to insert");
    assert_eq!(inserted.title, "Buy milk");
    assert!(!inserted.completed);
    assert!(!inserted.id.is_empty());

    // SELECT all
    let all = task::Entity::find()
        .all(&db)
        .await
        .expect("Failed to find all");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].title, "Buy milk");

    // SELECT by id
    let found = task::Entity::find_by_id(inserted.id.clone())
        .one(&db)
        .await
        .expect("Failed to find by id");
    assert!(found.is_some());
    assert_eq!(found.unwrap().title, "Buy milk");

    // UPDATE
    let mut active: task::ActiveModel = inserted.into();
    active.title = Set("Buy bread".into());
    active.completed = Set(true);
    let updated = active.update(&db).await.expect("Failed to update");
    assert_eq!(updated.title, "Buy bread");
    assert!(updated.completed);

    // DELETE
    let delete_result = task::Entity::delete_by_id(updated.id)
        .exec(&db)
        .await
        .expect("Failed to delete");
    assert_eq!(delete_result.rows_affected, 1);

    // Verify deleted
    let all_after_delete = task::Entity::find()
        .all(&db)
        .await
        .expect("Failed to find after delete");
    assert!(all_after_delete.is_empty());
}

#[tokio::test]
async fn test_wavesyncdb_meta_table_created() {
    let db = WaveSyncDbBuilder::new(&mem_db("meta"), "test-meta")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    let result = db
        .execute_unprepared("SELECT count(*) FROM _wavesync_meta")
        .await;
    assert!(result.is_ok(), "meta table should exist");

    let result = db
        .execute_unprepared("SELECT count(*) FROM _wavesync_peer_versions")
        .await;
    assert!(result.is_ok(), "peer versions table should exist");
}

#[tokio::test]
async fn test_wavesyncdb_change_notifications() {
    let db = WaveSyncDbBuilder::new(&mem_db("notif"), "test-notifications")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");

    let mut rx = db.change_rx();

    let new_task = task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("Test notification".into()),
        completed: Set(false),
        ..Default::default()
    };
    let _ = new_task.insert(&db).await.expect("Failed to insert");

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let notification = rx.try_recv();
    assert!(
        notification.is_ok(),
        "Should have received a change notification"
    );
    let notif = notification.unwrap();
    assert_eq!(notif.table, "tasks");
}

// ---------------------------------------------------------------------------
// P2P integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_fresh_peer_receives_snapshot() {
    let _ = env_logger::try_init();
    let topic = format!("test-snapshot-{}", Uuid::new_v4());
    let db_b_url = mem_db("snap_b");
    let db_a_url = mem_db("snap_a");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(2))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    let id1 = Uuid::new_v4().to_string();
    let id2 = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id1.clone()),
        title: Set("Task One".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert task 1");
    task::ActiveModel {
        id: Set(id2.clone()),
        title: Set("Task Two".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert task 2");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(1))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    let timeout = Duration::from_secs(15);
    assert_eventually("Peer A has 2 tasks", timeout, || async {
        let count = task::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0);
        count == 2
    })
    .await;

    let tasks = task::Entity::find()
        .all(&peer_a)
        .await
        .expect("Failed to query Peer A");
    let titles: Vec<&str> = tasks.iter().map(|t| t.title.as_str()).collect();
    assert!(titles.contains(&"Task One"), "Missing 'Task One'");
    assert!(titles.contains(&"Task Two"), "Missing 'Task Two'");
}

#[tokio::test]
async fn test_offline_peer_receives_updates_on_reconnect() {
    let _ = env_logger::try_init();
    let topic = format!("test-reconnect-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_b_url = mem_db("recon_b");
    let db_a1_url = mem_db("recon_a1");
    let db_a2_url = mem_db("recon_a2");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(10))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    let peer_a1 = WaveSyncDbBuilder::new(&db_a1_url, &topic)
        .with_node_id(make_node_id(11))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A1");

    peer_a1
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A1");

    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("before-offline".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert before-offline");

    assert_eventually("A1 has before-offline task", timeout, || async {
        task::Entity::find()
            .all(&peer_a1)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    drop(peer_a1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("while-offline".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert while-offline");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a2 = WaveSyncDbBuilder::new(&db_a2_url, &topic)
        .with_node_id(make_node_id(12))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A2");

    peer_a2
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A2");

    assert_eventually("A2 has both tasks", timeout, || async {
        task::Entity::find()
            .all(&peer_a2)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;

    let tasks = task::Entity::find()
        .all(&peer_a2)
        .await
        .expect("Failed to query Peer A2");
    let titles: Vec<&str> = tasks.iter().map(|t| t.title.as_str()).collect();
    assert!(
        titles.contains(&"before-offline"),
        "Missing 'before-offline'"
    );
    assert!(titles.contains(&"while-offline"), "Missing 'while-offline'");
}

#[tokio::test]
async fn test_registry_ready_fires_before_discovery() {
    let _ = env_logger::try_init();
    let topic = format!("test-registry-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_b_url = mem_db("reg_b");
    let db_a_url = mem_db("reg_a");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(20))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("registry-test".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert registry-test");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(21))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    assert_eventually("Peer A has registry-test task", timeout, || async {
        task::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    let tasks = task::Entity::find()
        .all(&peer_a)
        .await
        .expect("Failed to query Peer A");
    assert_eq!(tasks[0].title, "registry-test");
}

#[tokio::test]
async fn test_sync_with_matching_passphrase() {
    let _ = env_logger::try_init();
    let topic = format!("test-auth-match-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_b_url = mem_db("auth_match_b");
    let db_a_url = mem_db("auth_match_a");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(30))
        .with_passphrase("shared-secret")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    let id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("auth-task".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert auth-task");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(31))
        .with_passphrase("shared-secret")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    assert_eventually("Peer A has auth-task", timeout, || async {
        task::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    let tasks = task::Entity::find()
        .all(&peer_a)
        .await
        .expect("Failed to query Peer A");
    assert_eq!(tasks[0].title, "auth-task");
}

#[tokio::test]
async fn test_sync_with_mismatched_passphrase() {
    let _ = env_logger::try_init();
    let topic = format!("test-auth-mismatch-{}", Uuid::new_v4());
    let db_b_url = mem_db("auth_mis_b");
    let db_a_url = mem_db("auth_mis_a");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(40))
        .with_passphrase("secret-one")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("should-not-sync".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert should-not-sync");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(41))
        .with_passphrase("secret-two")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    tokio::time::sleep(Duration::from_secs(6)).await;

    let count = task::Entity::find()
        .all(&peer_a)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count, 0,
        "Peer A should NOT have received data from Peer B (different passphrases)"
    );
}

#[tokio::test]
async fn test_sync_one_passphrase_one_not() {
    let _ = env_logger::try_init();
    let topic = format!("test-auth-mixed-{}", Uuid::new_v4());
    let db_b_url = mem_db("auth_mixed_b");
    let db_a_url = mem_db("auth_mixed_a");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(50))
        .with_passphrase("my-secret")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("mixed-task".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert mixed-task");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(51))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    tokio::time::sleep(Duration::from_secs(6)).await;

    let count = task::Entity::find()
        .all(&peer_a)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(
        count, 0,
        "Peer A (no passphrase) should NOT have received data from Peer B (with passphrase)"
    );
}

#[tokio::test]
async fn test_same_db_reconnection_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-samedb-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_mobile_url = mem_db("samedb_mobile");
    let db_desktop_url = mem_db("samedb_desktop");

    // --- Phase 1: Both peers online, initial sync ---
    let mobile = WaveSyncDbBuilder::new(&db_mobile_url, &topic)
        .with_node_id(make_node_id(60))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Mobile");

    mobile
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Mobile");

    let desktop = WaveSyncDbBuilder::new(&db_desktop_url, &topic)
        .with_node_id(make_node_id(61))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Desktop");

    desktop
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Desktop");

    let id_initial = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id_initial.clone()),
        title: Set("initial-task".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&mobile)
    .await
    .expect("Insert initial-task");

    assert_eventually("Desktop has initial-task", timeout, || async {
        task::Entity::find()
            .all(&desktop)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    // --- Phase 2: Mobile goes offline ---
    drop(mobile);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let id_offline = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id_offline.clone()),
        title: Set("while-mobile-offline".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&desktop)
    .await
    .expect("Insert while-mobile-offline");

    // --- Phase 3: Desktop goes offline too ---
    drop(desktop);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // --- Phase 4: Both come back with SAME DBs ---
    let mobile2 = WaveSyncDbBuilder::new(&db_mobile_url, &topic)
        .with_node_id(make_node_id(62))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Mobile2");

    mobile2
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Mobile2");

    let desktop2 = WaveSyncDbBuilder::new(&db_desktop_url, &topic)
        .with_node_id(make_node_id(63))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Desktop2");

    desktop2
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Desktop2");

    assert_eventually("Mobile2 has both tasks", timeout, || async {
        task::Entity::find()
            .all(&mobile2)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;

    let tasks = task::Entity::find()
        .all(&mobile2)
        .await
        .expect("Failed to query Mobile2");
    let titles: Vec<&str> = tasks.iter().map(|t| t.title.as_str()).collect();
    assert!(titles.contains(&"initial-task"), "Missing 'initial-task'");
    assert!(
        titles.contains(&"while-mobile-offline"),
        "Missing 'while-mobile-offline'"
    );

    let desktop_tasks = task::Entity::find()
        .all(&desktop2)
        .await
        .expect("Failed to query Desktop2");
    assert_eq!(desktop_tasks.len(), 2, "Desktop2 should have both tasks");
}

#[tokio::test]
async fn test_resume_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-resume-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("resume_a");
    let db_b_url = mem_db("resume_b");

    // Create peers A and B
    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(70))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer A");

    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer A");

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(71))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create Peer B");

    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema on Peer B");

    // Verify initial connectivity: insert on A, wait for B
    let id_initial = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id_initial.clone()),
        title: Set("initial".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .expect("Insert initial");

    assert_eventually("B has initial task", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    // Simulate B going to background and coming back — call resume()
    peer_b.resume();

    // Insert a new task on A after B resumed
    let id_after = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id_after.clone()),
        title: Set("after-resume".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .expect("Insert after-resume");

    // B should receive the new task via the resume sync path
    assert_eventually("B has both tasks after resume", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;

    let tasks = task::Entity::find()
        .all(&peer_b)
        .await
        .expect("Failed to query Peer B");
    let titles: Vec<&str> = tasks.iter().map(|t| t.title.as_str()).collect();
    assert!(titles.contains(&"initial"), "Missing 'initial'");
    assert!(titles.contains(&"after-resume"), "Missing 'after-resume'");
}
