//! Dioxus integration for WaveSyncDB.
//!
//! Provides reactive hooks and context helpers for Dioxus apps with peer-to-peer sync:
//!
//! - [`use_wavesync_provider()`] — provide a pre-built DB to the component tree.
//! - [`use_wavesync_provider_lazy()`] — provide a lazy DB signal (initialized later).
//! - [`use_wavesync()`] — retrieve the `&'static WaveSyncDb` from context.
//! - [`use_wavesync_opt()`] — returns `Signal<Option<&'static WaveSyncDb>>`.
//! - [`use_wavesync_init()`] — returns an [`InitDb`] handle to build the DB at runtime.
//! - [`use_synced_table`] — cross-target reactive signal of all rows
//!   in a table. Takes a [`SyncHandle`] so the same UI component
//!   compiles on native and wasm32 without cfg gating at the call site.
//! - [`use_synced_table_db`] / [`use_synced_table_client`] — backend-
//!   specific escape hatches when a [`SyncHandle`] isn't available.
//! - [`use_synced_row`] — reactive signal for a single row by primary key.
//! - [`SyncHandle`] — opaque transport wrapper (`WaveSyncDb` on native,
//!   `Signal<Option<WebSyncClient>>` on wasm32) with a unified
//!   [`SyncHandle::submit`] for writes.
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
//!     let db: WaveSyncDb = rt.block_on(async {
//!         let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
//!             .build().await.unwrap();
//!         db.get_schema_registry(module_path!().split("::").next().unwrap())
//!             .sync().await.unwrap();
//!         db
//!     });
//!
//!     dioxus::launch(move || {
//!         let handle = wavesyncdb::dioxus::SyncHandle::new(db.clone());
//!         let tasks = wavesyncdb::dioxus::use_synced_table::<task::Model>(handle);
//!         // ... render tasks
//!         todo!()
//!     });
//! }
//! ```

// Native (non-wasm32) hooks built on top of SeaORM. These pull in
// `sea_orm` types and a tokio multi-thread runtime, so they cannot
// compile to wasm32. Browser apps get a parallel `web_hooks` module
// below that exposes a similarly-shaped reactive API over
// [`crate::WebSyncClient`] and [`crate::BrowserStore`].
#[cfg(not(target_arch = "wasm32"))]
pub mod hooks;
#[cfg(not(target_arch = "wasm32"))]
mod lifecycle;
#[cfg(not(target_arch = "wasm32"))]
pub use hooks::*;

#[cfg(target_arch = "wasm32")]
pub mod web_hooks;
#[cfg(target_arch = "wasm32")]
pub use web_hooks::*;

// Cross-target transport facade. Exposes [`SyncHandle`], the
// target-agnostic [`use_synced_table`] hook, and [`SyncSubmitError`].
// Lives outside `hooks` / `web_hooks` so the same definitions are
// available on both targets.
pub mod sync_handle;
pub use sync_handle::{SyncHandle, SyncSubmitError, use_synced_table};
