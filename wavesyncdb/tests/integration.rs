use std::future::Future;
use std::time::{Duration, Instant};

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

// Define a Task entity using standard SeaORM
mod task {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "tasks")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub title: String,
        pub completed: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// Generate a unique temp file SQLite URI.
fn mem_db(name: &str) -> String {
    let unique = Uuid::new_v4().simple().to_string();
    let path = std::env::temp_dir().join(format!("wavesync_test_{name}_{unique}.db"));
    format!("sqlite:{}?mode=rwc", path.display())
}

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
// Helpers for P2P integration tests
// ---------------------------------------------------------------------------

fn make_node_id(seed: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = seed;
    id[15] = 1;
    id
}

async fn assert_eventually<F, Fut>(desc: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    let mut interval = Duration::from_millis(50);
    loop {
        if check().await {
            return;
        }
        if start.elapsed() > timeout {
            panic!("Timed out ({}s) waiting for: {}", timeout.as_secs(), desc);
        }
        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(Duration::from_millis(500));
    }
}

// ---------------------------------------------------------------------------
// Test 1: A fresh peer receives changes from an established peer.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fresh_peer_receives_snapshot() {
    let _ = env_logger::try_init();
    let topic = format!("test-snapshot-{}", Uuid::new_v4());
    // Each peer needs its own unique DB name
    let db_b_url = mem_db("snap_b");
    let db_a_url = mem_db("snap_a");

    // Peer B — the established node with data
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

    // Insert 2 tasks on Peer B
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

    // Wait for shadow table entries to be populated
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Peer A — fresh node, should receive data via version vector sync
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

    // Assert that Peer A eventually has both tasks
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

    // Verify content
    let tasks = task::Entity::find()
        .all(&peer_a)
        .await
        .expect("Failed to query Peer A");
    let titles: Vec<&str> = tasks.iter().map(|t| t.title.as_str()).collect();
    assert!(titles.contains(&"Task One"), "Missing 'Task One'");
    assert!(titles.contains(&"Task Two"), "Missing 'Task Two'");
}

// ---------------------------------------------------------------------------
// Test 2: An offline peer receives updates on reconnect.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_offline_peer_receives_updates_on_reconnect() {
    let _ = env_logger::try_init();
    let topic = format!("test-reconnect-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_b_url = mem_db("recon_b");
    let db_a1_url = mem_db("recon_a1");
    let db_a2_url = mem_db("recon_a2");

    // Peer B — persistent node
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

    // Peer A1 — first instance
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

    // B inserts task "before-offline"
    task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("before-offline".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .expect("Insert before-offline");

    // Wait for A1 to receive it
    assert_eventually("A1 has before-offline task", timeout, || async {
        task::Entity::find()
            .all(&peer_a1)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    // Drop A1 to simulate going offline
    drop(peer_a1);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // B inserts task "while-offline"
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

    // Peer A2 — new instance with fresh DB
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

    // A2 should eventually have BOTH tasks
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

// ---------------------------------------------------------------------------
// Test 3: registry_ready fires before mDNS discovery, sync still works.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_registry_ready_fires_before_discovery() {
    let _ = env_logger::try_init();
    let topic = format!("test-registry-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_b_url = mem_db("reg_b");
    let db_a_url = mem_db("reg_a");

    // Peer B — already established with data
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

    // Peer A — sync immediately after build
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

// ---------------------------------------------------------------------------
// Test 4: Two peers with matching passphrase sync successfully.
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// Test 5: Two peers with mismatched passphrases do NOT sync.
// ---------------------------------------------------------------------------
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

    // Wait a reasonable time — data should NOT appear
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

// ---------------------------------------------------------------------------
// Test 6: One peer with passphrase and one without do NOT sync.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_sync_one_passphrase_one_not() {
    let _ = env_logger::try_init();
    let topic = format!("test-auth-mixed-{}", Uuid::new_v4());
    let db_b_url = mem_db("auth_mixed_b");
    let db_a_url = mem_db("auth_mixed_a");

    // Peer B — with passphrase
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

    // Peer A — without passphrase
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

    // Wait a reasonable time — data should NOT appear
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
