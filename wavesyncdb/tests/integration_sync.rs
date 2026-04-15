mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{DeletePolicy, TableMeta, WaveSyncDbBuilder};

use common::{assert_eventually, make_node_id, make_peer, mem_db, note, task};

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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(101))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(103))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(105))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(107))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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
                a.title == "A-title" && a.completed && b.title == "A-title" && b.completed
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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(109))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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
        peer.schema().register(task::Entity).sync().await.unwrap();
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
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(114))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

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

// ===========================================================================
// P1: Critical Sync Correctness
// ===========================================================================

// ---------------------------------------------------------------------------
// P1.1: Three peers update same column concurrently — deterministic winner
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_three_peer_same_column_deterministic_winner() {
    let _ = env_logger::try_init();
    let topic = format!("test-3col-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(20);

    let url_a = mem_db("3col_a");
    let url_b = mem_db("3col_b");
    let url_c = mem_db("3col_c");

    let peer_a = make_peer(&url_a, &topic, 120).await;
    let peer_b = make_peer(&url_b, &topic, 121).await;
    let peer_c = make_peer(&url_c, &topic, 122).await;

    // Wait for mesh to form
    tokio::time::sleep(Duration::from_secs(3)).await;

    // A inserts a row
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Wait for all to have the row
    for (name, peer) in [("B", &peer_b), ("C", &peer_c)] {
        let pk = pk.clone();
        let peer = peer.clone();
        assert_eventually(&format!("{name} has row"), timeout, || async {
            task::Entity::find_by_id(pk.clone())
                .one(&peer)
                .await
                .ok()
                .flatten()
                .is_some()
        })
        .await;
    }

    // Each peer updates title concurrently (all at col_version=2)
    peer_a
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'A-wins' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();
    peer_b
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'B-wins' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();
    peer_c
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'C-wins' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();

    // All three should converge to the same value (deterministic tiebreak)
    assert_eventually("All three converge", timeout, || async {
        let a = task::Entity::find_by_id(pk.clone())
            .one(&peer_a)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        let b = task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        let c = task::Entity::find_by_id(pk.clone())
            .one(&peer_c)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        a.is_some() && a == b && b == c
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.2: Three peers update different columns — all survive
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_three_peer_different_columns_all_survive() {
    let _ = env_logger::try_init();
    let topic = format!("test-3diffcol-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(20);

    let url_a = mem_db("3diff_a");
    let url_b = mem_db("3diff_b");
    let url_c = mem_db("3diff_c");

    let peer_a = make_peer(&url_a, &topic, 123).await;
    let peer_b = make_peer(&url_b, &topic, 124).await;
    let peer_c = make_peer(&url_c, &topic, 125).await;

    tokio::time::sleep(Duration::from_secs(3)).await;

    // A inserts a row
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Wait for all peers to have the row
    for (name, peer) in [("B", &peer_b), ("C", &peer_c)] {
        let pk = pk.clone();
        let peer = peer.clone();
        assert_eventually(&format!("{name} has row"), timeout, || async {
            task::Entity::find_by_id(pk.clone())
                .one(&peer)
                .await
                .ok()
                .flatten()
                .is_some()
        })
        .await;
    }

    // A updates title, B updates completed, C updates title again
    peer_a
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'A-title' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();
    peer_b
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"completed\" = 1 WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();
    // C updates title — this is at col_version=2 same as A's, but a different value
    peer_c
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'C-title' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();

    // All converge: completed=true from B, title is deterministic winner between A and C
    assert_eventually("All converge", timeout, || async {
        let a = task::Entity::find_by_id(pk.clone())
            .one(&peer_a)
            .await
            .ok()
            .flatten();
        let b = task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten();
        let c = task::Entity::find_by_id(pk.clone())
            .one(&peer_c)
            .await
            .ok()
            .flatten();
        match (a, b, c) {
            (Some(a), Some(b), Some(c)) => {
                // completed must be true on all
                a.completed && b.completed && c.completed
                // title must be the same on all
                && a.title == b.title && b.title == c.title
            }
            _ => false,
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.3: Burst inserts all propagate
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_burst_inserts_all_propagate() {
    let _ = env_logger::try_init();
    let topic = format!("test-burst-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(30);

    let url_a = mem_db("burst_a");
    let url_b = mem_db("burst_b");

    let peer_a = make_peer(&url_a, &topic, 126).await;
    let peer_b = make_peer(&url_b, &topic, 127).await;

    // Wait for mesh
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts 50 tasks rapidly
    for i in 0..50 {
        task::ActiveModel {
            id: Set(format!("burst-{i}")),
            title: Set(format!("Task {i}")),
            completed: Set(false),
            ..Default::default()
        }
        .insert(&peer_a)
        .await
        .unwrap();
    }

    // B receives all 50
    assert_eventually("B has all 50 tasks", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 50
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.4: Large payload sync
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_large_payload_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-large-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("large_a");
    let url_b = mem_db("large_b");

    let peer_a = make_peer(&url_a, &topic, 128).await;
    let peer_b = make_peer(&url_b, &topic, 129).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // 50KB title
    let big_title: String = "X".repeat(50 * 1024);
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set(big_title.clone()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B receives large payload", timeout, || async {
        task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title.len() == 50 * 1024)
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.5: AddWins policy integration
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_add_wins_policy_integration() {
    let _ = env_logger::try_init();
    let topic = format!("test-addwins-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("addwins_a");
    let url_b = mem_db("addwins_b");

    // Build peers manually to use AddWins policy
    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(make_node_id(130))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();

    // Create table and shadow table manually, then register with AddWins
    peer_a
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS \"tasks\" (
                \"id\" TEXT NOT NULL PRIMARY KEY,
                \"title\" TEXT NOT NULL,
                \"completed\" BOOLEAN NOT NULL
            )",
        )
        .await
        .unwrap();
    peer_a
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS \"_wavesync_tasks_clock\" (
                pk TEXT NOT NULL,
                cid TEXT NOT NULL,
                col_version INTEGER NOT NULL,
                db_version INTEGER NOT NULL,
                site_id BLOB NOT NULL,
                seq INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (pk, cid)
            )",
        )
        .await
        .unwrap();
    peer_a.register_table(TableMeta {
        table_name: "tasks".to_string(),
        primary_key_column: "id".to_string(),
        columns: vec![
            "id".to_string(),
            "title".to_string(),
            "completed".to_string(),
        ],
        delete_policy: DeletePolicy::AddWins,
    });

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(make_node_id(131))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();

    peer_b
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS \"tasks\" (
                \"id\" TEXT NOT NULL PRIMARY KEY,
                \"title\" TEXT NOT NULL,
                \"completed\" BOOLEAN NOT NULL
            )",
        )
        .await
        .unwrap();
    peer_b
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS \"_wavesync_tasks_clock\" (
                pk TEXT NOT NULL,
                cid TEXT NOT NULL,
                col_version INTEGER NOT NULL,
                db_version INTEGER NOT NULL,
                site_id BLOB NOT NULL,
                seq INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (pk, cid)
            )",
        )
        .await
        .unwrap();
    peer_b.register_table(TableMeta {
        table_name: "tasks".to_string(),
        primary_key_column: "id".to_string(),
        columns: vec![
            "id".to_string(),
            "title".to_string(),
            "completed".to_string(),
        ],
        delete_policy: DeletePolicy::AddWins,
    });

    // Signal registry ready on both peers
    peer_a.registry_ready();
    peer_b.registry_ready();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts a row
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("keep-me".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives
    assert_eventually("B has task", timeout, || async {
        task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // B updates title (col_version=2)
    peer_b
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'updated-by-b' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();

    // A deletes (causal_length=2, tie with B's col_version=2)
    task::Entity::delete_by_id(pk.clone())
        .exec(&peer_a)
        .await
        .unwrap();

    // With AddWins policy, tie means row survives
    // Wait for sync to propagate
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Row should survive on B (AddWins: tie keeps row)
    let b_row = task::Entity::find_by_id(pk.clone())
        .one(&peer_b)
        .await
        .ok()
        .flatten();
    assert!(
        b_row.is_some(),
        "AddWins: row should survive when delete ties with update"
    );
}

// ---------------------------------------------------------------------------
// P1.6: Concurrent delete vs update (default DeleteWins)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_concurrent_delete_vs_update() {
    let _ = env_logger::try_init();
    let topic = format!("test-delvupd-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("delvupd_a");
    let url_b = mem_db("delvupd_b");

    let peer_a = make_peer(&url_a, &topic, 132).await;
    let peer_b = make_peer(&url_b, &topic, 133).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("Original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives
    assert_eventually("B has task", timeout, || async {
        task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // A updates title, B deletes same row concurrently
    peer_a
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'A-updated' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();

    task::Entity::delete_by_id(pk.clone())
        .exec(&peer_b)
        .await
        .unwrap();

    // Both should converge to same state (default DeleteWins)
    assert_eventually("Both converge", timeout, || async {
        let a = task::Entity::find_by_id(pk.clone())
            .one(&peer_a)
            .await
            .ok()
            .flatten();
        let b = task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten();
        // Both should be in the same state (either both deleted or both present)
        (a.is_none() && b.is_none()) || (a.is_some() && b.is_some() && a == b)
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.7: Remote delete then re-insert (already tested above as test 7, but
// this variant has both peers delete first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_remote_delete_then_reinsert() {
    let _ = env_logger::try_init();
    let topic = format!("test-bothdelreins-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("bdri_a");
    let url_b = mem_db("bdri_b");

    let peer_a = make_peer(&url_a, &topic, 134).await;
    let peer_b = make_peer(&url_b, &topic, 135).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let pk = "both-del-reinsert";

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

    // Both peers delete the row
    task::Entity::delete_by_id(pk.to_string())
        .exec(&peer_a)
        .await
        .unwrap();
    task::Entity::delete_by_id(pk.to_string())
        .exec(&peer_b)
        .await
        .unwrap();

    // Wait for deletes to sync
    assert_eventually("A sees delete", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_a)
            .await
            .ok()
            .flatten()
            .is_none()
    })
    .await;

    // A re-inserts same PK
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("Reborn".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B receives re-inserted row
    assert_eventually("B sees re-inserted row", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "Reborn")
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.8: Multi-table sync (tasks + notes)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multi_table_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-multitable-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("mt_a");
    let url_b = mem_db("mt_b");

    // Build peers manually to register both entities
    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(make_node_id(136))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a
        .schema()
        .register(task::Entity)
        .register(note::Entity)
        .sync()
        .await
        .unwrap();

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(make_node_id(137))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b
        .schema()
        .register(task::Entity)
        .register(note::Entity)
        .sync()
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts 1 task + 1 note
    task::ActiveModel {
        id: Set("mt-task-a".to_string()),
        title: Set("Task from A".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    note::ActiveModel {
        id: Set("mt-note-a".to_string()),
        body: Set("Note from A".into()),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // B inserts 1 note
    note::ActiveModel {
        id: Set("mt-note-b".to_string()),
        body: Set("Note from B".into()),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .unwrap();

    // Both converge to 1 task + 2 notes
    assert_eventually("A has 1 task", timeout, || async {
        task::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    assert_eventually("A has 2 notes", timeout, || async {
        note::Entity::find()
            .all(&peer_a)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;

    assert_eventually("B has 1 task", timeout, || async {
        task::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 1
    })
    .await;

    assert_eventually("B has 2 notes", timeout, || async {
        note::Entity::find()
            .all(&peer_b)
            .await
            .map(|v| v.len())
            .unwrap_or(0)
            == 2
    })
    .await;
}

// ---------------------------------------------------------------------------
// P1.9: Network partition merge — two isolated groups write to same PK
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_network_partition_merge() {
    let _ = env_logger::try_init();
    let timeout = Duration::from_secs(20);

    // Group X: peers A+B on topic-X
    let topic_x = format!("test-partition-x-{}", Uuid::new_v4());
    let url_a = mem_db("part_a");
    let url_c = mem_db("part_c");

    let peer_a = make_peer(&url_a, &topic_x, 138).await;
    let peer_b = make_peer(&mem_db("part_b"), &topic_x, 139).await;

    // Group Y: peers C+D on topic-Y (isolated)
    let topic_y = format!("test-partition-y-{}", Uuid::new_v4());
    let peer_c = make_peer(&url_c, &topic_y, 140).await;
    let peer_d = make_peer(&mem_db("part_d"), &topic_y, 141).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let pk = "partition-pk";

    // A writes to the shared PK
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("From-Group-X".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // C writes to the same PK independently
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("From-Group-Y".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&peer_c)
    .await
    .unwrap();

    // Verify each group synced internally
    assert_eventually("B has X's data", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "From-Group-X")
    })
    .await;

    assert_eventually("D has Y's data", timeout, || async {
        task::Entity::find_by_id(pk.to_string())
            .one(&peer_d)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "From-Group-Y")
    })
    .await;

    // Drop all peers
    drop(peer_a);
    drop(peer_b);
    drop(peer_c);
    drop(peer_d);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Merge: recreate A and C on the same topic using their original DB files
    let merge_topic = format!("test-partition-merge-{}", Uuid::new_v4());

    let merged_a = WaveSyncDbBuilder::new(&url_a, &merge_topic)
        .with_node_id(make_node_id(142))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    merged_a
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let merged_c = WaveSyncDbBuilder::new(&url_c, &merge_topic)
        .with_node_id(make_node_id(143))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    merged_c
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // Both should converge to deterministic winner
    assert_eventually("Merged peers converge", timeout, || async {
        let a = task::Entity::find_by_id(pk.to_string())
            .one(&merged_a)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        let c = task::Entity::find_by_id(pk.to_string())
            .one(&merged_c)
            .await
            .ok()
            .flatten()
            .map(|t| t.title.clone());
        a.is_some() && a == c
    })
    .await;
}

// ===========================================================================
// P2: Edge Cases (in integration_sync.rs)
// ===========================================================================

// ---------------------------------------------------------------------------
// P2.13: Multiple updates before sync — B converges to final value
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multiple_updates_before_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-multiupd-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("multiupd_a");
    let url_b = mem_db("multiupd_b");

    let peer_a = make_peer(&url_a, &topic, 150).await;
    let peer_b = make_peer(&url_b, &topic, 151).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts a row then rapidly updates it — no waiting between insert and updates.
    // The out-of-order delivery bug (UPDATE before INSERT) is handled by deferred
    // shadow writes in apply_remote_changeset.
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("v0".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Rapid updates immediately after insert (no waiting for propagation)
    for i in 1..=10 {
        peer_a
            .execute_unprepared(&format!(
                "UPDATE \"tasks\" SET \"title\" = 'v{i}' WHERE \"id\" = '{pk}'"
            ))
            .await
            .unwrap();
    }

    // B should converge to v10
    assert_eventually("B has final value v10", timeout, || async {
        task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "v10")
    })
    .await;
}

// ---------------------------------------------------------------------------
// P2.14: Insert then immediate delete before sync — B ends up with 0 rows
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_insert_delete_before_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-insdelbs-{}", Uuid::new_v4());

    let url_a = mem_db("insdelbs_a");
    let url_b = mem_db("insdelbs_b");

    let peer_a = make_peer(&url_a, &topic, 152).await;
    let peer_b = make_peer(&url_b, &topic, 153).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // A inserts and immediately deletes
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("ephemeral".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    task::Entity::delete_by_id(pk.clone())
        .exec(&peer_a)
        .await
        .unwrap();

    // Wait for sync to propagate
    tokio::time::sleep(Duration::from_secs(8)).await;

    // B should have 0 rows
    let count = task::Entity::find()
        .all(&peer_b)
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    assert_eq!(count, 0, "B should have 0 rows after insert+delete");
}

// ---------------------------------------------------------------------------
// background_sync: Config persistence (registered_tables round-trip)
// Seeds: 200–201
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_background_sync_config_persists_tables() {
    let _ = env_logger::try_init();
    let topic = format!("test-bgcfg-{}", Uuid::new_v4());
    let db_url = mem_db("bgcfg");

    // Build DB and register tables manually (like FFI does)
    let db = WaveSyncDbBuilder::new(&db_url, &topic)
        .with_node_id(make_node_id(200))
        .with_sync_interval(Duration::from_secs(60))
        .build()
        .await
        .unwrap();

    // Create table
    db.inner()
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL DEFAULT '', completed INTEGER NOT NULL DEFAULT 0)",
        )
        .await
        .unwrap();
    wavesyncdb::shadow::create_shadow_table(db.inner(), "tasks")
        .await
        .unwrap();
    db.register_table(TableMeta {
        table_name: "tasks".to_string(),
        primary_key_column: "id".to_string(),
        columns: vec!["id".to_string(), "title".to_string(), "completed".to_string()],
        delete_policy: DeletePolicy::DeleteWins,
    });

    // Persist tables to config (mimics FFI registry_ready)
    let tables = db.registry().all_tables();
    assert_eq!(tables.len(), 1, "Should have 1 registered table");

    let mut config = wavesyncdb::connection::SyncConfig::load(&db_url).unwrap();
    config.registered_tables = Some(tables);
    config.save().unwrap();

    // Reload and verify
    let reloaded = wavesyncdb::connection::SyncConfig::load(&db_url).unwrap();
    let rt = reloaded.registered_tables.expect("registered_tables should be Some");
    assert_eq!(rt.len(), 1);
    assert_eq!(rt[0].table_name, "tasks");
    assert_eq!(rt[0].primary_key_column, "id");
    assert_eq!(rt[0].columns, vec!["id", "title", "completed"]);

    db.shutdown().await;
}

// ---------------------------------------------------------------------------
// background_sync: Full cold sync via mDNS (peer A live, peer B cold-starts)
// Seeds: 202–203
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_background_sync_pulls_data_from_live_peer() {
    let _ = env_logger::try_init();
    let topic = format!("test-bgsync-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    // --- Peer A: live peer with data ---
    let db_a_url = mem_db("bgsync_a");
    let peer_a = make_peer(&db_a_url, &topic, 202).await;

    let task_id = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(task_id.clone()),
        title: Set("Cold sync test".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Verify A has the task
    let a_tasks = task::Entity::find().all(&peer_a).await.unwrap();
    assert_eq!(a_tasks.len(), 1);

    // --- Peer B: simulate cold sync ---
    // Create a separate database URL for B
    let db_b_url = mem_db("bgsync_b");

    // First, B needs to have been "started once" so the config exists.
    // We simulate this by building, registering, saving config, then shutting down.
    let peer_b_init = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(203))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();

    // Register schema and persist to config (like FFI registry_ready)
    peer_b_init
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    // Also save registered_tables for the FFI path
    let tables = peer_b_init.registry().all_tables();
    let mut config_b = wavesyncdb::connection::SyncConfig::load(&db_b_url).unwrap();
    config_b.registered_tables = Some(tables);
    config_b.save().unwrap();

    // Shut down B completely (simulates app being killed)
    peer_b_init.shutdown().await;

    // Give a moment for the port to be released
    tokio::time::sleep(Duration::from_millis(500)).await;

    // --- Cold sync B ---
    let result = wavesyncdb::background_sync::background_sync_with_peers(
        &db_b_url,
        timeout,
        &[],
    )
    .await
    .unwrap();

    // Verify B got A's data
    match result {
        wavesyncdb::background_sync::BackgroundSyncResult::Synced { peers_synced } => {
            assert!(peers_synced > 0, "Should have synced with at least 1 peer");
        }
        other => {
            panic!("Expected Synced, got {:?}", other);
        }
    }

    // Open B's database to verify the data is there
    let db_b_verify = sea_orm::Database::connect(&db_b_url).await.unwrap();
    let b_tasks = task::Entity::find().all(&db_b_verify).await.unwrap();
    assert_eq!(b_tasks.len(), 1, "B should have 1 task after cold sync");
    assert_eq!(b_tasks[0].title, "Cold sync test");

    db_b_verify.close().await.unwrap();
    peer_a.shutdown().await;
}
