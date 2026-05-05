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

// Pure-data modules: types, conflict resolution, HMAC, protocol envelopes.
// These compile on every target — including wasm32 — and form the surface
// shared with browser builds.
pub mod auth;
pub mod conflict;
pub mod diagnostics;
pub mod messages;
pub mod network_status;
pub mod protocol;
pub mod registry;
pub mod synced_model;

// Native-only modules: anything that touches sea-orm (SQLite), libp2p
// transports, tokio I/O, the local filesystem, or platform FFI. The
// browser/wasm32 build skips all of these — a future browser sync path
// (WebSocket/WebRTC/WebTransport + sqlite-wasm) will live behind its own
// feature.
#[cfg(not(target_arch = "wasm32"))]
pub mod background_sync;
#[cfg(not(target_arch = "wasm32"))]
pub mod connection;
#[cfg(not(target_arch = "wasm32"))]
pub mod engine;
#[cfg(all(not(target_arch = "wasm32"), feature = "mobile-ffi"))]
mod ffi;
#[cfg(not(target_arch = "wasm32"))]
pub mod peer_addrs;
#[cfg(not(target_arch = "wasm32"))]
pub mod peer_tracker;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod push;
#[cfg(not(target_arch = "wasm32"))]
pub mod shadow;

// Browser/wasm32 engine. Minimal real-time changeset fan-out over a
// WebSocket libp2p transport — see module docs for what's in scope and
// what's deferred. Public API: [`web_engine::WebSyncClient`].
#[cfg(target_arch = "wasm32")]
pub mod web_engine;
#[cfg(target_arch = "wasm32")]
pub mod web_entity;
#[cfg(target_arch = "wasm32")]
pub mod web_store;
#[cfg(target_arch = "wasm32")]
pub use web_engine::{
    LoopbackEnd, LoopbackLink, LoopbackPair, WebSyncClient, WebSyncError, WebSyncStatus,
};
#[cfg(target_arch = "wasm32")]
pub use web_entity::BrowserEntity;
#[cfg(target_arch = "wasm32")]
pub use web_store::{BrowserStore, ResolvedRow, ShadowRow, StoreError};

pub use auth::GroupKey;
#[cfg(not(target_arch = "wasm32"))]
pub use connection::{SchemaBuilder, SyncConfig, WaveSyncDb, WaveSyncDbBuilder};
#[cfg(not(target_arch = "wasm32"))]
pub use engine::EngineCommand;
pub use messages::{
    AppId, ChangeNotification, ColumnChange, ColumnName, DeletePolicy, HmacTag, NodeId, PrimaryKey,
    SyncChangeset, TableName, TopicString, WriteKind,
};
pub use network_status::{NatStatus, NetworkEvent, NetworkStatus, PeerId, PeerInfo, RelayStatus};
#[cfg(not(target_arch = "wasm32"))]
pub use registry::SyncEntityInfo;
pub use registry::{TableMeta, TableRegistry};
pub use synced_model::SyncedModel;

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
        ("libp2p_dcutr", log::LevelFilter::Info),
        ("libp2p_mdns", log::LevelFilter::Warn),
        ("libp2p_swarm", log::LevelFilter::Warn),
        ("libp2p_dns", log::LevelFilter::Warn),
        ("libp2p_tcp", log::LevelFilter::Warn),
        // libp2p_core emits long type-name debug stack traces ("Failed to
        // listen/dial using libp2p_core::transport::map::Map<...>"). The full
        // generic type name fills tens of lines per failed dial attempt and
        // adds nothing actionable. Drop to warn.
        ("libp2p_core", log::LevelFilter::Warn),
        ("libp2p_noise", log::LevelFilter::Warn),
        ("libp2p_quic", log::LevelFilter::Warn),
        ("libp2p_relay", log::LevelFilter::Warn),
        ("libp2p_identify", log::LevelFilter::Warn),
        ("libp2p_yamux", log::LevelFilter::Warn),
        ("libp2p_ping", log::LevelFilter::Warn),
        ("libp2p_request_response", log::LevelFilter::Warn),
        ("multistream_select", log::LevelFilter::Warn),
        ("netlink_proto", log::LevelFilter::Warn),
        // sqlx logs every query at INFO by default. We also set
        // SeaORM's `ConnectOptions::sqlx_logging_level(Debug)` in
        // `connection.rs` so the events themselves are emitted at debug —
        // this filter is the second line of defence in case anything routes
        // through the `log` crate at info regardless. Set
        // RUST_LOG=sqlx::query=info to re-enable when debugging slow queries.
        ("sqlx::query", log::LevelFilter::Warn),
        ("sqlx_core::logger", log::LevelFilter::Warn),
    ]
}

/// The crate's semver version string (from `CARGO_PKG_VERSION`).
///
/// Available on every target — including wasm32 — so consumer crates (e.g.
/// the documentation website) can render a single source of truth for the
/// shipped version without re-declaring it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// Re-export for use by the #[derive(SyncEntity)] macro
pub use inventory::submit as register_sync_entity;

// Re-export sea-orm for users of the library. sea-orm is not available on
// wasm32 (sqlx-sqlite pulls in libsqlite3-sys, a C library), so this
// re-export is gated to native targets. The `SyncEntity` derive uses it.
#[cfg(not(target_arch = "wasm32"))]
pub use sea_orm;

// Re-export serde_json so the `SyncEntity` derive macro can reference it
// at `wavesyncdb::serde_json::*` without forcing every consuming crate to
// declare serde_json as a direct dependency.
pub use serde_json;

#[cfg(all(feature = "derive", not(target_arch = "wasm32")))]
pub use wavesyncdb_derive::SyncEntity;

#[cfg(feature = "dioxus")]
pub mod dioxus;
