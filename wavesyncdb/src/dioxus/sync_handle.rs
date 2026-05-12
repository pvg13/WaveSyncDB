//! Cross-target transport handle for shared Dioxus UI components.
//!
//! The native and web sync backends have fundamentally different runtime
//! shapes — [`WaveSyncDb`] (SeaORM connection pool) on native vs
//! [`WebSyncClient`] (libp2p over WebSocket) on wasm32 — so the hooks
//! that read and write to them can't share a transport parameter
//! verbatim. `SyncHandle` is the thin facade that hides this: a
//! target-specific opaque wrapper with a constructor, a clone-friendly
//! shape, and an async [`submit`](Self::submit) method that dispatches
//! to whichever backend is compiled in.
//!
//! UI components written against `SyncHandle` compile unchanged on both
//! targets — `use_synced_table::<E>(handle)` reads uniformly,
//! `handle.submit(&entity).await` writes uniformly. Apps construct the
//! handle at the entry point (one cfg-gated arm per target) and pass
//! it down as a component prop or context value.
//!
//! [`WaveSyncDb`]: crate::WaveSyncDb
//! [`WebSyncClient`]: crate::WebSyncClient

use dioxus::prelude::*;

use crate::synced_table::SyncedTableEntity;

#[cfg(not(target_arch = "wasm32"))]
use crate::WaveSyncDb;
#[cfg(target_arch = "wasm32")]
use crate::WebSyncClient;

/// Target-specific transport wrapper. Construct with `SyncHandle::new`
/// passing whichever backend is compiled in.
///
/// The handle is always cheap to clone — native stores a `WaveSyncDb`
/// (itself an `Arc`-shaped clone), web stores a Dioxus `Signal` (a
/// `Copy`-shaped reactive handle). On wasm, `SyncHandle` is `Copy` as
/// well so it can be freely captured in Dioxus event-handler closures
/// without per-call cloning. On native it's `Clone` only because
/// `WaveSyncDb` carries non-`Copy` state; shared UI components that
/// target both should still call `handle.clone()` defensively to keep
/// the source identical across targets.
#[derive(Clone)]
#[cfg_attr(target_arch = "wasm32", derive(Copy))]
pub struct SyncHandle {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) db: WaveSyncDb,
    #[cfg(target_arch = "wasm32")]
    pub(crate) client: Signal<Option<WebSyncClient>>,
}

/// Always equal. Dioxus's `#[component]` macro derives a prop-equality
/// check to decide whether to skip re-renders on parent updates;
/// `SyncHandle` is conceptually a singleton for a component's lifetime
/// (the same backend connection from mount to unmount) so comparing
/// two instances meaningfully would require dereferencing the
/// underlying `Arc` / `Signal`, and the answer would be "the same one
/// you saw before" anyway. Internal reactivity (the hook's own
/// subscription to `change_rx` / `subscribe_resolved`) drives renders
/// on data changes, not prop equality.
impl PartialEq for SyncHandle {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl SyncHandle {
    /// Wrap a native [`WaveSyncDb`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(db: WaveSyncDb) -> Self {
        Self { db }
    }

    /// Wrap a web [`WebSyncClient`] signal. The signal pattern matches
    /// the typical browser setup where the client is constructed async
    /// during a `use_hook` and lives in a `Signal<Option<WebSyncClient>>`
    /// until the initial connect resolves.
    #[cfg(target_arch = "wasm32")]
    pub fn new(client: Signal<Option<WebSyncClient>>) -> Self {
        Self { client }
    }

    /// Escape hatch: borrow the underlying native database for SeaORM
    /// operations the unified API doesn't cover (raw SQL, custom queries,
    /// transactions, etc.). Code that uses this is non-portable to wasm32.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn db(&self) -> &WaveSyncDb {
        &self.db
    }

    /// Escape hatch: borrow the underlying web client signal for
    /// browser-only operations (peer status, presence pings, etc.).
    /// Code that uses this is non-portable to native targets.
    #[cfg(target_arch = "wasm32")]
    pub fn client(&self) -> Signal<Option<WebSyncClient>> {
        self.client
    }

    /// Persist a row.
    ///
    /// On native, converts the entity to its `ActiveModel` and calls
    /// `save()` — SeaORM's upsert path. The write goes through the
    /// `WaveSyncDb` interception layer and broadcasts a
    /// `ChangeNotification` to subscribers.
    ///
    /// On wasm32, dispatches to [`WebSyncClient::submit`] with the
    /// table name resolved from `E::table_name()`. The browser engine
    /// persists to the IndexedDB shadow table and broadcasts a
    /// `ColumnChange` to subscribers.
    ///
    /// Either path is no-op-safe to spawn from a Dioxus event handler;
    /// `submit` is async and intended to be called inside a `spawn`.
    pub async fn submit<E>(&self, entity: &E) -> Result<(), SyncSubmitError>
    where
        E: SyncedTableEntity,
    {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use ::sea_orm::ActiveModelTrait;
            // `IntoActiveModel<E::ActiveModel>` is a supertrait of
            // `SyncedTableEntity` on native, so `into_active_model`
            // is in scope without an explicit `use`.
            let am: E::ActiveModel = entity.clone().into_active_model();
            am.save(&self.db).await?;
            Ok(())
        }
        #[cfg(target_arch = "wasm32")]
        {
            let Some(client) = self.client.read().clone() else {
                return Err(SyncSubmitError::NotConnected);
            };
            client.submit::<E>(E::table_name(), entity).await?;
            Ok(())
        }
    }
}

/// Error returned by [`SyncHandle::submit`]. The variants are
/// target-gated so each target sees only the failure modes its
/// backend can produce.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub enum SyncSubmitError {
    /// SeaORM returned an error from `save()` — connection issue,
    /// constraint violation, etc.
    Db(::sea_orm::DbErr),
}

#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub enum SyncSubmitError {
    /// Browser sync engine returned an error — usually IndexedDB
    /// persistence or shadow-table write failure.
    Web(crate::WebSyncError),
    /// The web client signal is still `None` — connect hasn't resolved
    /// yet. UI code can retry once the connect future completes.
    NotConnected,
}

impl ::std::fmt::Display for SyncSubmitError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            SyncSubmitError::Db(e) => write!(f, "db error: {e}"),
            #[cfg(target_arch = "wasm32")]
            SyncSubmitError::Web(e) => write!(f, "web sync error: {e}"),
            #[cfg(target_arch = "wasm32")]
            SyncSubmitError::NotConnected => write!(f, "web client not connected"),
        }
    }
}

impl ::std::error::Error for SyncSubmitError {}

#[cfg(not(target_arch = "wasm32"))]
impl From<::sea_orm::DbErr> for SyncSubmitError {
    fn from(e: ::sea_orm::DbErr) -> Self {
        SyncSubmitError::Db(e)
    }
}

#[cfg(target_arch = "wasm32")]
impl From<crate::WebSyncError> for SyncSubmitError {
    fn from(e: crate::WebSyncError) -> Self {
        SyncSubmitError::Web(e)
    }
}

/// Reactive `Vec<E>` materialized from a synced table, target-agnostic.
///
/// Dispatches to the backend-specific implementation based on which
/// transport `handle` wraps:
///
/// - Native (`cfg(not(target_arch = "wasm32"))`): delegates to
///   [`super::use_synced_table_db`] with `E::Entity` — SeaORM does the
///   initial load and the per-column patching uses `SyncedModel`.
/// - Web (`cfg(target_arch = "wasm32")`): delegates to
///   [`super::use_synced_table_client`] with `E::table_name()` — reads
///   from IndexedDB and folds resolved `ColumnChange` events.
///
/// Both paths return `Signal<Vec<E>>` (note: on native, the Entity's
/// `Model` is the user's struct `E` — the unified type means no
/// `E::Model` indirection at the UI call site).
///
/// The two function definitions below have intentionally different
/// where-clauses because each target needs different downstream
/// bounds, and `#[cfg(...)]` on individual where predicates is still
/// unstable — keeping the cfg at the function level dodges that.
#[cfg(not(target_arch = "wasm32"))]
pub fn use_synced_table<E>(handle: SyncHandle) -> Signal<Vec<E>>
where
    E: SyncedTableEntity,
    <<E::Entity as ::sea_orm::EntityTrait>::PrimaryKey as ::sea_orm::PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<::sea_orm::Value> + From<String>,
{
    super::use_synced_table_db::<E::Entity>(handle.db)
}

#[cfg(target_arch = "wasm32")]
pub fn use_synced_table<E: SyncedTableEntity>(handle: SyncHandle) -> Signal<Vec<E>> {
    super::use_synced_table_client::<E>(handle.client, E::table_name())
}
