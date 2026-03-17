use std::future::Future;
use std::time::{Duration, Instant};

use uuid::Uuid;
use wavesyncdb::WaveSyncDb;
use wavesyncdb::WaveSyncDbBuilder;

/// Generate a unique temp file SQLite URI.
pub fn mem_db(name: &str) -> String {
    let unique = Uuid::new_v4().simple().to_string();
    let path = std::env::temp_dir().join(format!("wavesync_test_{name}_{unique}.db"));
    format!("sqlite:{}?mode=rwc", path.display())
}

/// Create a peer with standard test configuration: mDNS fast discovery, 2s sync interval.
/// Registers `task::Entity` and calls `sync()`.
pub async fn make_peer(db_url: &str, topic: &str, seed: u8) -> WaveSyncDb {
    let peer = WaveSyncDbBuilder::new(db_url, topic)
        .with_node_id(make_node_id(seed))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create peer");
    peer.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");
    peer
}

/// Poll `check` until it returns `true` or `timeout` elapses.
pub async fn assert_eventually<F, Fut>(desc: &str, timeout: Duration, mut check: F)
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

/// Create a deterministic node ID from a seed byte.
pub fn make_node_id(seed: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = seed;
    id[15] = 1;
    id
}

/// SeaORM entity for the `tasks` test table.
pub mod task {
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

/// SeaORM entity for the `notes` test table (second table for multi-table tests).
pub mod note {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "notes")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub body: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
