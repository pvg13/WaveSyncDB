//! # WaveSyncDB
//!
//! Transparent peer-to-peer sync for SeaORM applications.
//!
//! WaveSyncDB wraps a SeaORM [`DatabaseConnection`](sea_orm::DatabaseConnection) via
//! [`WaveSyncDb`], intercepting write operations (INSERT, UPDATE, DELETE) and replicating
//! them to peers over libp2p gossipsub. Conflicts are resolved automatically using
//! Last-Write-Wins (LWW) with hybrid logical clocks.
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
//! - [`SyncOperation`] — a serialized write operation sent over the network
//! - [`ChangeNotification`] — lightweight event emitted after every write

pub mod connection;
pub mod conflict;
pub mod engine;
pub mod messages;
pub mod protocol;
pub mod registry;
pub mod sync_log;

pub use connection::{SchemaBuilder, WaveSyncDb, WaveSyncDbBuilder};
pub use messages::{ChangeNotification, NodeId, SyncOperation, WriteKind};
pub use registry::{SyncEntityInfo, TableMeta, TableRegistry};

// Re-export for use by the #[derive(SyncEntity)] macro
pub use inventory::submit as register_sync_entity;

// Re-export sea-orm for users of the library
pub use sea_orm;
