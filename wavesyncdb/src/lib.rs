//! # WaveSyncDB
//!
//! Transparent peer-to-peer sync for SeaORM applications.
//!
//! WaveSyncDB wraps a SeaORM [`DatabaseConnection`](sea_orm::DatabaseConnection) via
//! [`WaveSyncDb`], intercepting write operations (INSERT, UPDATE, DELETE) and replicating
//! them to peers over libp2p request-response. Conflicts are resolved automatically using
//! per-column Lamport clocks (CRDTs), allowing concurrent edits to different columns
//! on the same row to both survive.
//!
//! ## Quick start
//!
//! ```ignore
//! use sea_orm::*;
//! use wavesyncdb::WaveSyncDbBuilder;
//!
//! let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
//!     .build()
//!     .await?;
//!
//! // Auto-discover #[derive(SyncEntity)] entities
//! db.get_schema_registry(module_path!().split("::").next().unwrap())
//!     .sync()
//!     .await?;
//!
//! // Standard SeaORM — sync is transparent
//! let task = task::ActiveModel { /* ... */ };
//! task.insert(&db).await?;
//! ```
//!
//! ## Key types
//!
//! - [`WaveSyncDb`] — connection wrapper that intercepts writes
//! - [`WaveSyncDbBuilder`] — configures and builds the connection + P2P engine
//! - [`SchemaBuilder`] — fluent API for registering entities
//! - [`SyncChangeset`] — a batch of column-level CRDT changes sent over the network
//! - [`ChangeNotification`] — lightweight event emitted after every write

pub mod auth;
pub mod background_sync;
pub mod conflict;
pub mod connection;
pub mod engine;
#[cfg(feature = "mobile-ffi")]
mod ffi;
pub mod messages;
pub mod network_status;
pub mod peer_tracker;
pub mod protocol;
pub(crate) mod push;
pub mod registry;
pub mod shadow;

pub use auth::GroupKey;
pub use connection::{SchemaBuilder, SyncConfig, WaveSyncDb, WaveSyncDbBuilder};
pub use engine::EngineCommand;
pub use messages::{
    AppId, ChangeNotification, ColumnChange, ColumnName, DeletePolicy, HmacTag, NodeId, PrimaryKey,
    SyncChangeset, TableName, TopicString, WriteKind,
};
pub use network_status::{NatStatus, NetworkEvent, NetworkStatus, PeerId, PeerInfo, RelayStatus};
pub use registry::{SyncEntityInfo, TableMeta, TableRegistry};

/// Returns recommended log module filter tuples for silencing noisy dependencies.
///
/// Usage with `env_logger`:
/// ```rust,no_run
/// let mut builder = env_logger::Builder::from_env(
///     env_logger::Env::default().default_filter_or("info")
/// );
/// for (module, level) in wavesyncdb::recommended_log_filters() {
///     builder.filter_module(module, level);
/// }
/// builder.init();
/// ```
pub fn recommended_log_filters() -> Vec<(&'static str, log::LevelFilter)> {
    vec![
        ("hickory_resolver", log::LevelFilter::Warn),
        ("hickory_proto", log::LevelFilter::Warn),
        ("libp2p_autonat", log::LevelFilter::Warn),
        ("libp2p_mdns", log::LevelFilter::Warn),
        ("libp2p_swarm", log::LevelFilter::Warn),
        ("libp2p_dns", log::LevelFilter::Warn),
        ("libp2p_tcp", log::LevelFilter::Warn),
        ("libp2p_core", log::LevelFilter::Warn),
        ("libp2p_noise", log::LevelFilter::Warn),
        ("libp2p_quic", log::LevelFilter::Warn),
        ("libp2p_relay", log::LevelFilter::Warn),
        ("libp2p_identify", log::LevelFilter::Warn),
        ("multistream_select", log::LevelFilter::Warn),
        ("netlink_proto", log::LevelFilter::Warn),
    ]
}

// Re-export for use by the #[derive(SyncEntity)] macro
pub use inventory::submit as register_sync_entity;

// Re-export sea-orm for users of the library
pub use sea_orm;

#[cfg(feature = "derive")]
pub use wavesyncdb_derive::SyncEntity;

#[cfg(feature = "dioxus")]
pub mod dioxus;
