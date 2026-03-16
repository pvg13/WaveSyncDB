mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

use common::{assert_eventually, make_node_id, mem_db, task};

// ---------------------------------------------------------------------------
// Test 1: UPDATE propagation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_update_propagation() {
    let _ = env_logger::try_init();
    let topic = format!("test-update-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("upd_a");
    let db_b_url = mem_db("upd_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(100))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(101))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // A inserts
    let id = Uuid::new_v4().to_string();
    let inserted = task::ActiveModel {
        id: Set(id.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives
    assert_eventually("B has inserted task", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    // A updates title
    let mut active: task::ActiveModel = inserted.into();
    active.title = Set("Updated".into());
    active.update(&peer_a).await.unwrap();

    // B sees updated title
    assert_eventually("B sees updated title", timeout, || async {
        task::Entity::find_by_id(id.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "Updated")
    })
    .await;
}

// ---------------------------------------------------------------------------
// Test 2: DELETE propagation
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_delete_propagation() {
    let _ = env_logger::try_init();
    let topic = format!("test-delete-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("del_a");
    let db_b_url = mem_db("del_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(102))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(103))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // A inserts
    let id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("To Delete".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives
    assert_eventually("B has inserted task", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    // A deletes
    task::Entity::delete_by_id(id.clone())
        .exec(&peer_a)
        .await
        .unwrap();

    // B sees 0 rows
    assert_eventually("B sees 0 rows after delete", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(1)
            == 0
    })
    .await;
}

// ---------------------------------------------------------------------------
// Test 3: Concurrent same-column — higher version wins
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_concurrent_same_column_higher_version_wins() {
    let _ = env_logger::try_init();
    let topic = format!("test-conflict-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("conf_a");
    let db_b_url = mem_db("conf_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(104))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(105))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // A inserts, B receives
    let id = Uuid::new_v4().to_string();
    let inserted = task::ActiveModel {
        id: Set(id.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B has task", timeout, || async {
        task::Entity::find_by_id(id.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // A updates title twice (col_version = 3 for title)
    let mut active: task::ActiveModel = inserted.into();
    active.title = Set("A-first-update".into());
    let updated = active.update(&peer_a).await.unwrap();
    let mut active: task::ActiveModel = updated.into();
    active.title = Set("A-second-update".into());
    active.update(&peer_a).await.unwrap();

    // Both peers should converge to A's latest value
    assert_eventually("Both converge", timeout, || async {
        let a_title = task::Entity::find_by_id(id.clone())
            .one(&peer_a)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        let b_title = task::Entity::find_by_id(id.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        a_title == b_title && a_title == Some("A-second-update".to_string())
    })
    .await;
}

// ---------------------------------------------------------------------------
// Test 4: Concurrent different columns — both survive
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_concurrent_different_columns_both_survive() {
    let _ = env_logger::try_init();
    let topic = format!("test-diffcol-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("diffcol_a");
    let db_b_url = mem_db("diffcol_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(106))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(107))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // A inserts, B receives
    let id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B has task", timeout, || async {
        task::Entity::find_by_id(id.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // A updates title via raw SQL
    peer_a
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'A-title' WHERE \"id\" = '{}'",
            id
        ))
        .await
        .unwrap();

    // B updates completed via raw SQL
    peer_b
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"completed\" = 1 WHERE \"id\" = '{}'",
            id
        ))
        .await
        .unwrap();

    // Both should converge: title=A-title, completed=true
    assert_eventually("Both converge with both columns", timeout, || async {
        let a_task = task::Entity::find_by_id(id.clone())
            .one(&peer_a)
            .await
            .ok()
            .flatten();
        let b_task = task::Entity::find_by_id(id.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten();
        match (a_task, b_task) {
            (Some(a), Some(b)) => {
                a.title == "A-title"
                    && a.completed
                    && b.title == "A-title"
                    && b.completed
            }
            _ => false,
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// Test 5: Bidirectional sync
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_bidirectional_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-bidi-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("bidi_a");
    let db_b_url = mem_db("bidi_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(108))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(109))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // Wait for peers to discover each other
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts task-A
    task::ActiveModel {
        id: Set("task-a".to_string()),
        title: Set("From A".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B inserts task-B
    task::ActiveModel {
        id: Set("task-b".to_string()),
        title: Set("From B".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .unwrap();

    // Both end up with both tasks
    assert_eventually("A has 2 tasks", timeout, || async {
        task::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;

    assert_eventually("B has 2 tasks", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;
}

// ---------------------------------------------------------------------------
// Test 6: Three-peer convergence
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_three_peer_convergence() {
    let _ = env_logger::try_init();
    let topic = format!("test-3peer-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(20);

    let mut peers = Vec::new();
    for i in 0..3u8 {
        let url = mem_db(&format!("3peer_{}", i));
        let peer = WaveSyncDbBuilder::new(&url, &topic)
            .with_node_id(make_node_id(110 + i))
            .with_mdns_query_interval(Duration::from_millis(100))
            .with_mdns_ttl(Duration::from_secs(5))
            .with_sync_interval(Duration::from_secs(2))
            .build()
            .await
            .unwrap();
        peer.schema()
            .register(task::Entity)
            .sync()
            .await
            .unwrap();
        peers.push(peer);
    }

    // Wait for mesh to form
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Each peer inserts 1 task
    for (i, peer) in peers.iter().enumerate() {
        task::ActiveModel {
            id: Set(format!("peer-{}-task", i)),
            title: Set(format!("Task from peer {}", i)),
            completed: Set(false),
            ..Default::default()
        }
        .insert(peer)
        .await
        .unwrap();
    }

    // All converge to 3 tasks
    for (i, peer) in peers.iter().enumerate() {
        let peer = peer.clone();
        assert_eventually(&format!("Peer {} has 3 tasks", i), timeout, || async {
            task::Entity::find()
                .all(&peer)
                .await
                .map(|v| v.len())
                .unwrap_or(0)
                == 3
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// Test 7: Delete then re-insert same PK (N3 regression)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_delete_then_reinsert_same_pk() {
    let _ = env_logger::try_init();
    let topic = format!("test-reinsert-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);
    let db_a_url = mem_db("reinsert_a");
    let db_b_url = mem_db("reinsert_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(113))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(114))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let pk = "reinsert-pk";

    // A inserts
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("First".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives
    assert_eventually("B has task", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // A deletes
    task::Entity::delete_by_id(pk.to_string())
        .exec(&peer_a)
        .await
        .unwrap();

    // B sees deletion
    assert_eventually("B sees deletion", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_none()
    })
    .await;

    // A re-inserts same PK with new title
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("Reinserted".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B converges to re-inserted row
    assert_eventually("B sees re-inserted row", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "Reinserted")
    })
    .await;
}
