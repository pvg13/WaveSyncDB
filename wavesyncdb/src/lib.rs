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
pub(crate) mod mailbox_seal;
pub mod messages;
pub mod network_status;
pub mod protocol;
pub mod reconcile;
pub mod registry;
pub(crate) mod rejection;
pub mod synced_model;
pub mod synced_table;

// Wire-level libp2p codecs (snapshot / push). Target-independent so the
// native engine and the browser engine share ONE definition of each
// protocol id and framing — see wire/mod.rs for why this must not live
// under the wasm-gated engine/ tree.
pub mod wire;

// Target-independent core of the browser sync engine: conflict application,
// delete semantics, and digest enumeration over a small store trait. Compiled
// on native under `--features web` so its semantics are testable (and proven
// against the native engine) in plain `cargo test`; wasm32 always gets it.
#[cfg(any(feature = "web", target_arch = "wasm32"))]
pub mod web_sync_core;

// Native-only modules: anything that touches sea-orm (SQLite), libp2p
// transports, tokio I/O, the local filesystem, or platform FFI. The
// browser/wasm32 build skips all of these — a future browser sync path
// (WebSocket/WebRTC/WebTransport + sqlite-wasm) will live behind its own
// feature.
#[cfg(not(target_arch = "wasm32"))]
pub mod background_sync;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod capture;
#[cfg(not(target_arch = "wasm32"))]
pub mod connection;
// Uses libp2p::PeerId (per-peer health store) and is only ever read from
// WaveSyncDb::diagnostics, itself native-only — no browser build consumes it.
#[cfg(not(target_arch = "wasm32"))]
pub mod diagnostics;
#[cfg(not(target_arch = "wasm32"))]
pub mod engine;
#[cfg(all(not(target_arch = "wasm32"), feature = "mobile-ffi"))]
mod ffi;
// On-disk cache for derived group keys. Only ever called from
// `connection::group_key_for_dir` on iOS — every other platform's process
// budget affords the KDF directly (see module docs). Also compiled under
// `cfg(test)` on every native target so the load-only contract is
// host-testable; a plain non-iOS build never links it in.
#[cfg(all(not(target_arch = "wasm32"), any(target_os = "ios", test)))]
pub(crate) mod key_cache;
// Lets an app resolve the iOS App Group container directory it shares with
// its Notification Service Extension, so both point their `WaveSyncDbBuilder`
// at the same directory. See `ffi::wavesync_app_group_container`'s docs.
#[cfg(all(not(target_arch = "wasm32"), feature = "mobile-ffi", target_os = "ios"))]
pub use ffi::wavesync_app_group_container;
// Process-global registry of live sync nodes, keyed by canonical DB path.
// Lets a push wake reuse the in-process engine instead of building a
// duplicate-identity second one. See the module docs.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod node_registry;
#[cfg(not(target_arch = "wasm32"))]
pub mod notify;
// Native notification display for the headless background-sync path (the
// foreground Dioxus hook can't run in the FCM service process).
#[cfg(all(not(target_arch = "wasm32"), feature = "push-sync"))]
pub(crate) mod notify_display;
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
    LoopbackEnd, LoopbackLink, LoopbackPair, WebGroupHandle, WebSyncClient, WebSyncError,
    WebSyncStatus,
};
#[cfg(target_arch = "wasm32")]
pub use web_entity::BrowserEntity;
#[cfg(target_arch = "wasm32")]
pub use web_store::{BrowserStore, ResolvedRow, ShadowRow, StoreError};

pub use auth::GroupKey;
#[cfg(not(target_arch = "wasm32"))]
pub use connection::{SchemaBuilder, SyncConfig, WaveSyncDb, WaveSyncDbBuilder, WaveSyncNode};
#[cfg(not(target_arch = "wasm32"))]
pub use engine::EngineCommand;
pub use messages::{
    AppId, ChangeNotification, ChangeSource, ColumnChange, ColumnName, DeletePolicy, HmacTag,
    NodeId, PrimaryKey, SyncChangeset, TableName, TopicString, WriteKind,
};
pub use network_status::{NatStatus, NetworkEvent, NetworkStatus, PeerId, PeerInfo, RelayStatus};
#[cfg(not(target_arch = "wasm32"))]
pub use notify::{Notification, NotifyEntityInfo, SyncEvent, SyncNotify};
pub use registry::EntityScope;
#[cfg(not(target_arch = "wasm32"))]
pub use registry::SyncEntityInfo;
pub use registry::{SyncEntityDescriptor, TableMeta, TableRegistry};
pub use synced_model::{SyncedModel, lenient_from_value};
pub use synced_table::SyncedTableEntity;

/// Returns a recommended `EnvFilter`/`RUST_LOG` directive string for silencing
/// noisy dependencies.
///
/// The string is a comma-separated list of `target=level` directives (the
/// same syntax `RUST_LOG` and [`tracing_subscriber::EnvFilter`] both parse)
/// and is meant to be layered under an application-chosen default level.
///
/// Usage with `tracing-subscriber`:
/// ```rust,no_run
/// let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
///     tracing_subscriber::EnvFilter::new(format!(
///         "info,{}",
///         wavesyncdb::recommended_log_filters()
///     ))
/// });
/// tracing_subscriber::fmt().with_env_filter(filter).init();
/// ```
///
/// `env_logger`-based setups can use the same string directly:
/// ```rust,ignore
/// let mut builder = env_logger::Builder::from_env(
///     env_logger::Env::default().default_filter_or(format!(
///         "info,{}",
///         wavesyncdb::recommended_log_filters()
///     )),
/// );
/// builder.init();
/// ```
pub fn recommended_log_filters() -> &'static str {
    concat!(
        "hickory_resolver=warn,",
        "hickory_proto=warn,",
        "libp2p_autonat=warn,",
        "libp2p_dcutr=info,",
        "libp2p_mdns=warn,",
        "libp2p_swarm=warn,",
        "libp2p_dns=warn,",
        "libp2p_tcp=warn,",
        // libp2p_core emits long type-name debug stack traces ("Failed to
        // listen/dial using libp2p_core::transport::map::Map<...>"). The full
        // generic type name fills tens of lines per failed dial attempt and
        // adds nothing actionable. Drop to warn.
        "libp2p_core=warn,",
        "libp2p_noise=warn,",
        "libp2p_quic=warn,",
        "libp2p_relay=warn,",
        "libp2p_identify=warn,",
        "libp2p_yamux=warn,",
        "libp2p_ping=warn,",
        "libp2p_request_response=warn,",
        "multistream_select=warn,",
        "netlink_proto=warn,",
        // sqlx logs every query at INFO by default. We also set
        // SeaORM's `ConnectOptions::sqlx_logging_level(Debug)` in
        // `connection.rs` so the events themselves are emitted at debug —
        // this filter is the second line of defence in case anything routes
        // through the `log` crate at info regardless. Set
        // RUST_LOG=sqlx::query=info to re-enable when debugging slow queries.
        "sqlx::query=warn,",
        "sqlx_core::logger=warn"
    )
}

/// The crate's semver version string (from `CARGO_PKG_VERSION`).
///
/// Available on every target — including wasm32 — so consumer crates (e.g.
/// the documentation website) can render a single source of truth for the
/// shipped version without re-declaring it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// Re-export for use by the #[derive(SyncEntity)] macro
pub use inventory::submit as register_sync_entity;

// Sibling re-export for the target-independent `SyncEntityDescriptor`
// submission (see `registry.rs`). Same shape as `register_sync_entity`
// above — both are aliases of `inventory::submit!` — kept as a separate
// name so derive-generated code reads as "submitting a descriptor" at the
// call site, ungated so it works on wasm32 too.
pub use inventory::submit as register_sync_entity_descriptor;

// Re-export sea-orm for users of the library. sea-orm is not available on
// wasm32 (sqlx-sqlite pulls in libsqlite3-sys, a C library), so this
// re-export is gated to native targets. The `SyncEntity` derive uses it.
#[cfg(not(target_arch = "wasm32"))]
pub use sea_orm;

// Re-export serde_json so the `SyncEntity` derive macro can reference it
// at `wavesyncdb::serde_json::*` without forcing every consuming crate to
// declare serde_json as a direct dependency.
pub use serde_json;

#[cfg(feature = "derive")]
pub use wavesyncdb_derive::SyncEntity;
#[cfg(all(feature = "derive", not(target_arch = "wasm32")))]
pub use wavesyncdb_derive::SyncNotify;

#[cfg(feature = "dioxus")]
pub mod dioxus;
