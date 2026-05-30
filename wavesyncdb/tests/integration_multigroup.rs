//! Multi-group sync (issue #62): one libp2p node/swarm serving N independent
//! sync groups, each backed by its own SQLite DB.
//!
//! Node ID seeds: 240–259 (see CLAUDE.md §6). Run single-threaded (Rule 2.13):
//!   cargo test -p wavesyncdb --test integration_multigroup -- --test-threads=1

mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{WaveSyncDb, WaveSyncDbBuilder};

use common::task;
use common::{assert_eventually, make_node_id, mem_db};

/// Build a single-group peer (its own engine) joined to `topic` with `psk`.
async fn single_group_peer(name: &str, topic: &str, psk: &str, seed: u8) -> WaveSyncDb {
    let db = WaveSyncDbBuilder::new(&mem_db(name), topic)
        .with_node_id(make_node_id(seed))
        .with_passphrase(psk)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("build peer");
    db.schema().register(task::Entity).sync().await.expect("sync schema");
    db
}

async fn insert_task(db: &WaveSyncDb, id: &str, title: &str) {
    task::ActiveModel {
        id: Set(id.to_string()),
        title: Set(title.to_string()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(db)
    .await
    .expect("insert task");
}

async fn has_task(db: &WaveSyncDb, id: &str) -> bool {
    task::Entity::find_by_id(id.to_string())
        .one(db)
        .await
        .ok()
        .flatten()
        .is_some()
}

// ---------------------------------------------------------------------------
// One node joins two groups (alpha + beta). A peer in alpha-only must receive
// alpha data and NEVER beta data; a peer in beta-only the reverse. Proves a
// single swarm routes per-group and that the groups are isolated.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multigroup_isolation() {
    let _ = env_logger::try_init();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_alpha = format!("alpha-{suffix}");
    let topic_beta = format!("beta-{suffix}");
    let timeout = Duration::from_secs(20);

    // Node with two groups over ONE engine.
    let node_alpha = WaveSyncDbBuilder::new(&mem_db("mg_node"), &topic_alpha)
        .with_node_id(make_node_id(240))
        .with_passphrase("pass-alpha")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("build node");
    node_alpha.schema().register(task::Entity).sync().await.unwrap();

    let node = node_alpha.node();
    let node_beta = node
        .join_group(&topic_beta, "pass-beta")
        .await
        .expect("join beta");
    node_beta.schema().register(task::Entity).sync().await.unwrap();

    // The two groups must have distinct effective (PSK-derived) topics.
    assert_ne!(node_alpha.effective_topic(), node_beta.effective_topic());

    // Independent peers, one per group.
    let peer_a = single_group_peer("mg_a", &topic_alpha, "pass-alpha", 241).await;
    let peer_b = single_group_peer("mg_b", &topic_beta, "pass-beta", 242).await;

    // alpha write → reaches the alpha-only peer.
    insert_task(&node_alpha, "a1", "alpha one").await;
    assert_eventually("peer_a receives alpha row a1", timeout, || async {
        has_task(&peer_a, "a1").await
    })
    .await;

    // beta write → reaches the beta-only peer.
    insert_task(&node_beta, "b1", "beta one").await;
    assert_eventually("peer_b receives beta row b1", timeout, || async {
        has_task(&peer_b, "b1").await
    })
    .await;

    // Both positive propagations are done and the network has been live for a
    // while; now assert the isolation holds (no leakage either direction).
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(!has_task(&peer_b, "a1").await, "beta peer must NOT see alpha data");
    assert!(!has_task(&peer_a, "b1").await, "alpha peer must NOT see beta data");

    // The node's two group DBs are themselves separate.
    assert!(has_task(&node_alpha, "a1").await && !has_task(&node_alpha, "b1").await);
    assert!(has_task(&node_beta, "b1").await && !has_task(&node_beta, "a1").await);
}

// ---------------------------------------------------------------------------
// leave_group stops syncing that group but preserves its DB; re-joining resumes
// from disk and picks up changes made while away. The OTHER group keeps syncing
// throughout (one group leaving must not disturb the others on the swarm).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multigroup_leave_and_rejoin() {
    let _ = env_logger::try_init();
    let suffix = Uuid::new_v4().simple().to_string();
    let topic_alpha = format!("alpha-{suffix}");
    let topic_beta = format!("beta-{suffix}");
    let timeout = Duration::from_secs(20);

    let node_alpha = WaveSyncDbBuilder::new(&mem_db("mg2_node"), &topic_alpha)
        .with_node_id(make_node_id(243))
        .with_passphrase("pass-alpha")
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("build node");
    node_alpha.schema().register(task::Entity).sync().await.unwrap();
    let node = node_alpha.node();
    let node_beta = node.join_group(&topic_beta, "pass-beta").await.unwrap();
    node_beta.schema().register(task::Entity).sync().await.unwrap();

    let peer_a = single_group_peer("mg2_a", &topic_alpha, "pass-alpha", 244).await;
    let peer_b = single_group_peer("mg2_b", &topic_beta, "pass-beta", 245).await;

    // Establish baseline sync in both groups.
    insert_task(&node_beta, "b1", "beta one").await;
    assert_eventually("peer_b has b1", timeout, || async { has_task(&peer_b, "b1").await }).await;

    // Leave beta. Its DB file is preserved.
    node.leave_group(&node_beta).await;
    drop(node_beta);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // While the node is out of beta, peer_b writes b2 — the node must NOT get it,
    // but alpha must keep working.
    insert_task(&peer_b, "b2", "beta two").await;
    insert_task(&node_alpha, "a1", "alpha one").await;
    assert_eventually("alpha still syncs after leaving beta", timeout, || async {
        has_task(&peer_a, "a1").await
    })
    .await;

    // Re-join beta: resumes from the preserved DB (still has b1) and catches b2.
    let node_beta2 = node.join_group(&topic_beta, "pass-beta").await.unwrap();
    node_beta2.schema().register(task::Entity).sync().await.unwrap();
    assert!(has_task(&node_beta2, "b1").await, "rejoined group resumes b1 from disk");
    assert_eventually("rejoined node catches up b2", timeout, || async {
        has_task(&node_beta2, "b2").await
    })
    .await;
}
