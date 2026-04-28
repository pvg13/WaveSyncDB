//! Shared helpers for the benchmark binaries.
//!
//! Each bench binary builds against this crate (`bench_common`) for the
//! task entity, peer construction, and the temp-db URL helper.

use std::time::Duration;

use uuid::Uuid;
use wavesyncdb::{NodeId, WaveSyncDb, WaveSyncDbBuilder};

/// Generate a unique temp file SQLite URI.
pub fn temp_db_url(tag: &str) -> String {
    let unique = Uuid::new_v4().simple().to_string();
    let path = std::env::temp_dir().join(format!("wsdb_bench_{tag}_{unique}.db"));
    format!("sqlite:{}?mode=rwc", path.display())
}

/// Build a deterministic node id from a single seed byte.
pub fn make_node_id(seed: u8) -> NodeId {
    let mut id = [0u8; 16];
    id[0] = seed;
    id[15] = 1;
    NodeId(id)
}

/// Build a WaveSyncDB peer with bench-friendly defaults.
///
/// `mdns` controls whether mDNS is enabled (off for `bench_overhead`,
/// on for the multi-peer benches). `topic` must match across peers
/// that should sync.
pub async fn make_peer(db_url: &str, topic: &str, seed: u8, mdns: bool) -> WaveSyncDb {
    let builder = WaveSyncDbBuilder::new(db_url, topic).with_node_id(make_node_id(seed));

    let builder = if mdns {
        builder
            .with_mdns_query_interval(Duration::from_millis(100))
            .with_mdns_ttl(Duration::from_secs(5))
    } else {
        builder
    };

    let peer = builder
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to build bench peer");

    peer.schema()
        .register(task::Entity)
        .sync()
        .await
        .expect("Failed to sync schema");

    peer
}

/// Compute the p_n percentile of a slice of microseconds.
/// `pct` ∈ [0, 100]. Linear interpolation between adjacent ranks.
pub fn percentile(samples: &mut [u128], pct: f64) -> u128 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let rank = (pct / 100.0) * (samples.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        samples[lo]
    } else {
        let frac = rank - lo as f64;
        let lo_v = samples[lo] as f64;
        let hi_v = samples[hi] as f64;
        (lo_v + (hi_v - lo_v) * frac).round() as u128
    }
}

pub mod task {
    use sea_orm::entity::prelude::*;
    use wavesyncdb_derive::SyncEntity;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
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
