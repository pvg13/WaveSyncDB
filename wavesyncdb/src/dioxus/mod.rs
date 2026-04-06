//! Dioxus integration for WaveSyncDB.
//!
//! Provides reactive hooks and context helpers for Dioxus apps with peer-to-peer sync:
//!
//! - [`use_wavesync_provider()`] — provide a pre-built DB to the component tree.
//! - [`use_wavesync_provider_lazy()`] — provide a lazy DB signal (initialized later).
//! - [`use_wavesync()`] — retrieve the `&'static WaveSyncDb` from context.
//! - [`use_wavesync_opt()`] — returns `Signal<Option<&'static WaveSyncDb>>`.
//! - [`use_wavesync_init()`] — returns an [`InitDb`] handle to build the DB at runtime.
//! - [`use_synced_table`] — reactive signal of all rows in a table, auto-refreshes on writes.
//! - [`use_synced_row`] — reactive signal for a single row by primary key.
//!
//! ## Example
//!
//! ```ignore
//! fn main() {
//!     let rt = tokio::runtime::Builder::new_multi_thread()
//!         .enable_all()
//!         .build()
//!         .unwrap();
//!     let _guard = rt.enter();
//!
//!     let db: &'static WaveSyncDb = rt.block_on(async {
//!         let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
//!             .build().await.unwrap();
//!         let db = Box::leak(Box::new(db));
//!         db.get_schema_registry(module_path!().split("::").next().unwrap())
//!             .sync().await.unwrap();
//!         db
//!     });
//!
//!     dioxus::launch(move || {
//!         wavesyncdb::dioxus::use_wavesync_provider(db);
//!         let tasks = wavesyncdb::dioxus::use_synced_table::<task::Entity>(
//!             wavesyncdb::dioxus::use_wavesync()
//!         );
//!         // ... render tasks
//!         todo!()
//!     });
//! }
//! ```

mod lifecycle;
pub mod hooks;

pub use hooks::*;
