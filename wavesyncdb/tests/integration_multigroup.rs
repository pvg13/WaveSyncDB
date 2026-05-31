//! Multi-group sync (issue #62): one libp2p node/swarm serving N independent
//! sync groups, each backed by its own SQLite DB.
//!
//! Node ID seeds: 240–259 (see CLAUDE.md §6). Run single-threaded (Rule 2.13):
//!   cargo test -p wavesyncdb --test integration_multigroup -- --test-threads=1

mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{SyncConfig, WaveSyncDb, WaveSyncDbBuilder};

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
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("sync schema");
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

/// A db URL inside a unique temp *directory*, so the per-directory
/// `.wavesync_config.json` is isolated from other tests (mem_db puts every DB in
/// the shared temp dir, where they would share one config file).
fn isolated_db_url(name: &str) -> String {
    let dir =
        std::env::temp_dir().join(format!("wavesync_mg_{}_{}", name, Uuid::new_v4().simple()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    format!("sqlite:{}/app.db?mode=rwc", dir.display())
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
    node_alpha
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let node = node_alpha.node();
    let node_beta = node
        .join_group(&topic_beta, "pass-beta", None)
        .await
        .expect("join beta");
    node_beta
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

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
    assert!(
        !has_task(&peer_b, "a1").await,
        "beta peer must NOT see alpha data"
    );
    assert!(
        !has_task(&peer_a, "b1").await,
        "alpha peer must NOT see beta data"
    );

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
    node_alpha
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();
    let node = node_alpha.node();
    let node_beta = node
        .join_group(&topic_beta, "pass-beta", None)
        .await
        .unwrap();
    node_beta
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let peer_a = single_group_peer("mg2_a", &topic_alpha, "pass-alpha", 244).await;
    let peer_b = single_group_peer("mg2_b", &topic_beta, "pass-beta", 245).await;

    // Establish baseline sync in both groups.
    insert_task(&node_beta, "b1", "beta one").await;
    assert_eventually("peer_b has b1", timeout, || async {
        has_task(&peer_b, "b1").await
    })
    .await;

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
    let node_beta2 = node
        .join_group(&topic_beta, "pass-beta", None)
        .await
        .unwrap();
    node_beta2
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();
    assert!(
        has_task(&node_beta2, "b1").await,
        "rejoined group resumes b1 from disk"
    );
    assert_eventually("rejoined node catches up b2", timeout, || async {
        has_task(&node_beta2, "b2").await
    })
    .await;
}

// ---------------------------------------------------------------------------
// Config persistence: join_group/leave_group must update the on-disk
// `.wavesync_config.json` so a background wake can rebuild every group. This is
// what makes multi-group background_sync possible (issue #62 follow-up).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multigroup_config_persistence() {
    let _ = env_logger::try_init();
    let suffix = Uuid::new_v4().simple().to_string();
    let url = isolated_db_url("mg_cfg");
    let topic_alpha = format!("alpha-{suffix}");
    let topic_beta = format!("beta-{suffix}");

    let db = WaveSyncDbBuilder::new(&url, &topic_alpha)
        .with_node_id(make_node_id(246))
        .with_passphrase("pass-alpha")
        .build()
        .await
        .expect("build default group");

    // A fresh build writes a config with no extra groups.
    let cfg = SyncConfig::load(&url).expect("build() writes config");
    assert!(cfg.groups.is_empty(), "fresh build has no extra groups");

    // Joining a group records it in the config.
    let node = db.node();
    let beta = node
        .join_group(&topic_beta, "pass-beta", None)
        .await
        .expect("join beta");
    let cfg = SyncConfig::load(&url).expect("config");
    assert_eq!(cfg.groups.len(), 1, "join_group persists the group");
    assert_eq!(cfg.groups[0].user_topic, topic_beta);
    assert_eq!(cfg.groups[0].passphrase, "pass-beta");
    assert!(
        cfg.groups[0].database_url.contains("__wavesync-"),
        "group gets its own derived DB file: {}",
        cfg.groups[0].database_url
    );

    // Re-joining the same topic is idempotent in the config (no duplicate).
    let _beta_again = node
        .join_group(&topic_beta, "pass-beta", None)
        .await
        .unwrap();
    assert_eq!(SyncConfig::load(&url).unwrap().groups.len(), 1);

    // Leaving removes it.
    node.leave_group(&beta).await;
    assert!(
        SyncConfig::load(&url).unwrap().groups.is_empty(),
        "leave_group removes the group from config"
    );

    db.shutdown().await;
}

// ---------------------------------------------------------------------------
// build() runs on every app launch; it must PRESERVE groups joined in a
// previous session rather than wipe them (the bug that would otherwise make
// background_sync forget every runtime-joined group after a restart).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_multigroup_config_survives_rebuild() {
    let _ = env_logger::try_init();
    let suffix = Uuid::new_v4().simple().to_string();
    let url = isolated_db_url("mg_rebuild");
    let topic_alpha = format!("alpha-{suffix}");
    let topic_beta = format!("beta-{suffix}");

    {
        let db = WaveSyncDbBuilder::new(&url, &topic_alpha)
            .with_node_id(make_node_id(247))
            .with_passphrase("pass-alpha")
            .build()
            .await
            .unwrap();
        let node = db.node();
        let beta = node
            .join_group(&topic_beta, "pass-beta", None)
            .await
            .unwrap();
        assert_eq!(SyncConfig::load(&url).unwrap().groups.len(), 1);
        // Clean teardown before reopening the same DB file.
        db.shutdown().await;
        drop(beta);
        drop(node);
        drop(db);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // App restart: a fresh build() at the same URL must keep the joined group.
    let db2 = WaveSyncDbBuilder::new(&url, &topic_alpha)
        .with_node_id(make_node_id(247))
        .with_passphrase("pass-alpha")
        .build()
        .await
        .unwrap();
    let cfg = SyncConfig::load(&url).unwrap();
    assert_eq!(cfg.groups.len(), 1, "build() must preserve joined groups");
    assert_eq!(cfg.groups[0].user_topic, topic_beta);
    db2.shutdown().await;
}

// ---------------------------------------------------------------------------
// Per-struct sync scope (#[wavesync(scope = ...)]). Scope is a registration-time
// policy: an entity's table is only created+registered in a group whose
// (is_default, kind) satisfies the entity's scope. We assert table presence per
// group DB via sqlite_master — deterministic, no P2P. Seeds 248-259.
// ---------------------------------------------------------------------------
mod scope_entities {
    pub mod priv_e {
        use sea_orm::entity::prelude::*;
        use wavesyncdb_derive::SyncEntity;
        // No #[wavesync] attribute → defaults to EntityScope::Private.
        #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
        #[sea_orm(table_name = "scope_priv")]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false)]
            pub id: String,
            pub val: String,
        }
        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}
        impl ActiveModelBehavior for ActiveModel {}
    }

    pub mod all_e {
        use sea_orm::entity::prelude::*;
        use wavesyncdb_derive::SyncEntity;
        #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
        #[sea_orm(table_name = "scope_all")]
        #[wavesync(scope = all)]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false)]
            pub id: String,
            pub val: String,
        }
        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}
        impl ActiveModelBehavior for ActiveModel {}
    }

    pub mod house_e {
        use sea_orm::entity::prelude::*;
        use wavesyncdb_derive::SyncEntity;
        #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
        #[sea_orm(table_name = "scope_house")]
        #[wavesync(scope = groups("household"))]
        pub struct Model {
            #[sea_orm(primary_key, auto_increment = false)]
            pub id: String,
            pub val: String,
        }
        #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
        pub enum Relation {}
        impl ActiveModelBehavior for ActiveModel {}
    }
}

async fn table_exists(db: &WaveSyncDb, table: &str) -> bool {
    use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
    let stmt = Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=$1",
        [table.into()],
    );
    db.query_one(stmt).await.ok().flatten().is_some()
}

#[tokio::test]
async fn test_scope_controls_table_registration() {
    let _ = env_logger::try_init();
    let suffix = Uuid::new_v4().simple().to_string();
    let url = isolated_db_url("scope");
    let topic_house = format!("household-{suffix}");

    // Default group (is_default=true, kind=None).
    let default_db = WaveSyncDbBuilder::new(&url, &format!("personal-{suffix}"))
        .with_node_id(make_node_id(248))
        .with_passphrase("pass-personal")
        .build()
        .await
        .expect("build default");

    // Household group joined with kind="household".
    let house_db = default_db
        .node()
        .join_group(&topic_house, "pass-house", Some("household"))
        .await
        .expect("join household");

    // Auto-register the scoped entities into each group by scope.
    let prefix = "integration_multigroup::scope_entities";
    default_db.get_schema_registry(prefix).sync().await.unwrap();
    house_db.get_schema_registry(prefix).sync().await.unwrap();

    // Default group: Private + All present; Groups("household") absent.
    assert!(
        table_exists(&default_db, "scope_priv").await,
        "private in default"
    );
    assert!(
        table_exists(&default_db, "scope_all").await,
        "all in default"
    );
    assert!(
        !table_exists(&default_db, "scope_house").await,
        "household-scoped must NOT be in the default group"
    );

    // Household group: All + Groups("household") present; Private absent.
    assert!(
        !table_exists(&house_db, "scope_priv").await,
        "private must NOT be in a non-default group"
    );
    assert!(
        table_exists(&house_db, "scope_all").await,
        "all in household"
    );
    assert!(
        table_exists(&house_db, "scope_house").await,
        "household-scoped in household"
    );

    default_db.shutdown().await;
}
