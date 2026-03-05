//! Dioxus integration for WaveSyncDB.
//!
//! Provides a turnkey setup for Dioxus desktop apps with peer-to-peer sync:
//!
//! - [`launch()`] — creates a tokio runtime, builds [`WaveSyncDb`](wavesyncdb::WaveSyncDb),
//!   runs your setup closure, and launches Dioxus desktop with the DB available via context.
//! - [`launch_dynamic()`] — launches Dioxus without a pre-built DB; the DB can be initialized
//!   later from within a component via [`use_init_db()`].
//! - [`use_db()`] — retrieves the `&'static WaveSyncDb` from Dioxus context.
//! - [`use_db_opt()`] — returns `Signal<Option<&'static WaveSyncDb>>`, useful in dynamic mode.
//! - [`use_init_db()`] — returns an [`InitDb`] handle to build and inject the DB at runtime.
//! - [`use_synced_table`] — reactive signal of all rows in a table, auto-refreshes on writes.
//! - [`use_synced_row`] — reactive signal for a single row by primary key.
//!
//! ## Example
//!
//! ```ignore
//! fn main() {
//!     wavesyncdb_dioxus::launch("sqlite:./app.db?mode=rwc", "my-topic", |db| async move {
//!         db.get_schema_registry(module_path!().split("::").next().unwrap())
//!             .sync().await?;
//!         Ok(())
//!     }, App);
//! }
//!
//! fn App() -> Element {
//!     let db = wavesyncdb_dioxus::use_db();
//!     let tasks = wavesyncdb_dioxus::use_synced_table::<task::Entity>(db);
//!     // ... render tasks
//!     todo!()
//! }
//! ```

pub mod hooks;
pub mod launch;

pub use hooks::*;
pub use launch::{InitDb, launch, launch_dynamic, use_db, use_db_opt, use_init_db};
