//! Reactive Dioxus hooks for synced database tables.
//!
//! These hooks provide Dioxus signals that automatically refresh when the underlying
//! data changes — whether from local writes or remote sync operations. They subscribe
//! to [`ChangeNotification`](crate::ChangeNotification) events via
//! [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx) and apply the carried
//! `column_values` payload in place via [`SyncedModel`](crate::SyncedModel) — no
//! per-notification SeaORM round trip on the receive path.

use std::collections::HashMap;
use std::future::Future;

use dioxus::prelude::*;
use sea_orm::{DbErr, EntityTrait, FromQueryResult, PrimaryKeyTrait};
use tokio::time::{Duration, Instant};

use crate::{NetworkStatus, SyncedModel, WaveSyncDb, WaveSyncDbBuilder, WriteKind};

const BATCH_WINDOW: Duration = Duration::from_millis(16);
const LAGGED_DEBOUNCE: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// Context providers
// ---------------------------------------------------------------------------

/// Provide a pre-built `WaveSyncDb` to the component tree.
///
/// Since `WaveSyncDb` is cheap to clone (internally Arc-based), no wrapping is needed.
pub fn use_wavesync_provider(db: WaveSyncDb) {
    use_context_provider(|| Signal::new(Some(db)));
}

/// Provide a lazy `WaveSyncDb` signal to the component tree.
///
/// Call this in your root component when the DB will be initialized later
/// (e.g., after user picks a file). The signal starts as `None` and becomes
/// `Some` after [`use_wavesync_init()`] completes.
pub fn use_wavesync_provider_lazy() {
    use_context_provider::<Signal<Option<WaveSyncDb>>>(|| Signal::new(None));
}

// ---------------------------------------------------------------------------
// Context consumers
// ---------------------------------------------------------------------------

/// Retrieve the `WaveSyncDb` from Dioxus context.
///
/// In lazy mode this **panics** if the DB has not been initialized yet —
/// use [`use_wavesync_opt()`] instead.
pub fn use_wavesync() -> WaveSyncDb {
    let sig = use_context::<Signal<Option<WaveSyncDb>>>();
    sig.read()
        .clone()
        .expect("use_wavesync() called before DB was initialized — use use_wavesync_opt() instead")
}

/// Returns a reactive signal that is `None` until the DB is initialized.
///
/// In **lazy mode** (via [`use_wavesync_provider_lazy`]), the signal starts as `None`
/// and becomes `Some` after [`use_wavesync_init()`] completes.
pub fn use_wavesync_opt() -> Signal<Option<WaveSyncDb>> {
    use_context::<Signal<Option<WaveSyncDb>>>()
}

/// Returns a handle to initialize the database at runtime.
///
/// Use this in lazy mode to build and inject the DB into context.
pub fn use_wavesync_init() -> InitDb {
    let sig = use_context::<Signal<Option<WaveSyncDb>>>();
    InitDb {
        sig,
        generation: use_context::<Signal<u64>>(),
    }
}

/// Provide the generation counter context. Called once in the root component
/// alongside [`use_wavesync_provider_lazy()`].
pub fn use_wavesync_generation() {
    use_context_provider::<Signal<u64>>(|| Signal::new(0));
}

/// Handle returned by [`use_wavesync_init()`].
#[derive(Clone, Copy)]
pub struct InitDb {
    sig: Signal<Option<WaveSyncDb>>,
    generation: Signal<u64>,
}

impl InitDb {
    /// Get the current generation counter. Useful for detecting stale async tasks.
    pub fn generation(&self) -> u64 {
        *self.generation.read()
    }

    /// Returns `true` if the database has already been initialized.
    pub fn is_initialized(&self) -> bool {
        self.sig.read().is_some()
    }

    /// Clear the current database, shutting down the old engine.
    pub fn reset(&self) {
        // Increment generation first so in-flight tasks see the new value
        let mut generation = self.generation;
        generation.set(generation() + 1);

        let old = { self.sig.read().clone() };
        let mut sig = self.sig;
        sig.set(None);

        if let Some(db) = old {
            spawn(async move {
                db.shutdown().await;
            });
        }
    }

    /// Build the database, run the setup closure, and inject it into context.
    ///
    /// Uses default builder settings. For custom builder configuration
    /// (e.g., passphrase, sync interval, relay server), use [`call_with`](Self::call_with).
    pub async fn call<F, Fut>(&self, url: &str, topic: &str, setup: F) -> Result<(), DbErr>
    where
        F: FnOnce(WaveSyncDb) -> Fut,
        Fut: Future<Output = Result<(), DbErr>>,
    {
        self.call_with(url, topic, |b| b, setup).await
    }

    /// Build the database with custom builder configuration, run the setup
    /// closure, and inject it into context.
    ///
    /// The `configure` closure receives a [`WaveSyncDbBuilder`] and should
    /// return it after applying any desired settings:
    ///
    /// ```ignore
    /// init.call_with(
    ///     &db_url,
    ///     "my-app",
    ///     |b| b.with_passphrase("secret").with_sync_interval(Duration::from_secs(5)),
    ///     |db| async move {
    ///         db.get_schema_registry("my_app").sync().await?;
    ///         Ok(())
    ///     },
    /// ).await?;
    /// ```
    ///
    /// # Race Safety
    ///
    /// A generation counter guards against concurrent [`reset()`](Self::reset)
    /// calls. The generation is sampled before building, then re-checked after
    /// the builder completes and again after the setup closure returns. If a
    /// `reset()` occurred in the meantime the newly built DB is shut down and
    /// discarded, preventing a stale instance from being injected into context.
    pub async fn call_with<C, F, Fut>(
        &self,
        url: &str,
        topic: &str,
        configure: C,
        setup: F,
    ) -> Result<(), DbErr>
    where
        C: FnOnce(WaveSyncDbBuilder) -> WaveSyncDbBuilder,
        F: FnOnce(WaveSyncDb) -> Fut,
        Fut: Future<Output = Result<(), DbErr>>,
    {
        if self.sig.read().is_some() {
            log::warn!("use_wavesync_init: DB already initialized, ignoring");
            return Ok(());
        }

        let current_gen = self.generation();

        let builder = WaveSyncDbBuilder::new(url, topic);
        let db = configure(builder).build().await?;

        // Check if a reset happened while we were building the DB
        if self.generation() != current_gen {
            log::warn!("use_wavesync_init: generation changed during build, discarding new DB");
            db.shutdown().await;
            return Ok(());
        }

        setup(db.clone()).await?;

        // Double-check generation after setup
        if self.generation() != current_gen {
            log::warn!("use_wavesync_init: generation changed during setup, discarding new DB");
            db.shutdown().await;
            return Ok(());
        }

        let mut sig = self.sig;
        sig.set(Some(db));

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Network status hook
// ---------------------------------------------------------------------------

/// Reactive signal containing the current [`NetworkStatus`].
///
/// Subscribes to the event channel eagerly, then reads the initial snapshot,
/// ensuring no [`NetworkEvent::EngineStarted`](crate::NetworkEvent) is missed
/// (important on mobile where the engine task may not have run yet at first render).
/// Refreshes whenever any [`NetworkEvent`](crate::NetworkEvent) is received.
pub fn use_network_status(db: WaveSyncDb) -> Signal<NetworkStatus> {
    // Subscribe BEFORE reading the snapshot so we never miss EngineStarted.
    // Use a Signal<bool> just to trigger the initial re-read inside the effect.
    let mut signal = use_signal(|| db.network_status());

    use_effect(move || {
        // Create the subscription inside the effect but immediately re-read
        // the snapshot to catch any events that fired before this point.
        let mut rx = db.network_event_rx();
        let db = db.clone();
        signal.set(db.network_status());
        spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(_) => {
                        signal.set(db.network_status());
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Snapshot is always fresh
                        signal.set(db.network_status());
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    });

    signal
}

/// Reactive signal containing connected peers grouped by application-level identity.
///
/// Refreshes whenever a [`NetworkEvent::PeerIdentityReceived`](crate::NetworkEvent),
/// [`NetworkEvent::PeerConnected`](crate::NetworkEvent), or
/// [`NetworkEvent::PeerDisconnected`](crate::NetworkEvent) event is received.
/// Only includes peers that have announced an identity.
pub fn use_peer_identities(
    db: WaveSyncDb,
) -> Signal<std::collections::HashMap<String, Vec<crate::network_status::PeerInfo>>> {
    let mut signal = use_signal(|| db.peers_by_identity());

    use_effect(move || {
        let mut rx = db.network_event_rx();
        let db = db.clone();
        signal.set(db.peers_by_identity());
        spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if matches!(
                            event,
                            crate::network_status::NetworkEvent::PeerIdentityReceived { .. }
                                | crate::network_status::NetworkEvent::PeerConnected(_)
                                | crate::network_status::NetworkEvent::PeerDisconnected(_)
                        ) {
                            signal.set(db.peers_by_identity());
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        signal.set(db.peers_by_identity());
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    });

    signal
}

// ---------------------------------------------------------------------------
// Reactive table/row hooks
// ---------------------------------------------------------------------------

/// Reactive signal containing all rows in a table, keyed by a SeaORM
/// `Entity` type and operating directly on a [`WaveSyncDb`].
///
/// **Most apps should use [`super::use_synced_table`] instead** — the
/// cross-target hook that takes a [`SyncHandle`](super::SyncHandle) and
/// works identically on native and wasm32. This `_db` variant is the
/// native-only escape hatch for code that holds a `WaveSyncDb` directly
/// (e.g., legacy callers, advanced query setups).
///
/// Performs an initial `E::find().all(db)` query, then keeps the in-memory
/// `Vec<E::Model>` in sync with subsequent writes by **applying the
/// `column_values` payload from each [`ChangeNotification`] in place** via
/// [`SyncedModel`](crate::SyncedModel). No per-notification SeaORM round
/// trip is issued unless the payload is missing (older callers, raw SQL),
/// in which case the hook falls back to a single-row `find_by_id` query.
///
/// Falls back to a full `find().all()` reload only when the broadcast
/// channel reports `Lagged` — i.e. the subscriber missed notifications and
/// can't reconstruct the delta.
pub fn use_synced_table_db<E>(db: WaveSyncDb) -> Signal<Vec<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + From<String>,
{
    let mut signal: Signal<Vec<E::Model>> = use_signal(Vec::new);
    let target_table = E::default().table_name().to_string();
    let pk_column = pk_column_name::<E>();

    use_effect(move || {
        let mut rx = db.change_rx();
        let target_table = target_table.clone();
        let pk_column = pk_column.clone();
        let db = db.clone();
        spawn(async move {
            // Check in-memory cache first — instant on page re-navigation.
            let mut rows: Vec<E::Model> =
                if let Some(cached) = db.get_table_cache::<Vec<E::Model>>() {
                    cached
                } else {
                    match E::find().all(&db).await {
                        Ok(r) => r,
                        Err(e) => {
                            log::error!("Failed initial table load: {}", e);
                            Vec::new()
                        }
                    }
                };
            let mut pk_index: HashMap<String, usize> = rows
                .iter()
                .enumerate()
                .map(|(i, r)| (SyncedModel::wavesync_pk_string(r), i))
                .collect();
            db.set_table_cache(rows.clone());
            signal.set(rows.clone());

            let mut last_full_reload = Instant::now();
            let mut refresh_rx = db.refresh_rx();

            loop {
                // Wait for either a change notification or a resume/refresh tick.
                // A tick means the DB may have been written by a background-sync
                // process (a separate process — no in-process notification), so
                // reload the whole table to surface those writes.
                let first = tokio::select! {
                    biased;
                    r = refresh_rx.recv() => {
                        match r {
                            Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                if let Ok(r) = E::find().all(&db).await {
                                    rows = r;
                                    rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                                    db.set_table_cache(rows.clone());
                                    signal.set(rows.clone());
                                    last_full_reload = Instant::now();
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                    n = rx.recv() => n,
                };
                let first = match first {
                    Ok(n) => n,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Missed {} change notifications for {}", n, target_table);
                        if last_full_reload.elapsed() >= LAGGED_DEBOUNCE
                            && let Ok(r) = E::find().all(&db).await
                        {
                            rows = r;
                            rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                            db.set_table_cache(rows.clone());
                            signal.set(rows.clone());
                            last_full_reload = Instant::now();
                        }
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };

                // Drain additional notifications within the batch window.
                let mut batch = vec![first];
                let deadline = Instant::now() + BATCH_WINDOW;
                loop {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Ok(n)) => batch.push(n),
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                            log::warn!("Missed {} change notifications for {}", n, target_table);
                            batch.clear();
                            if last_full_reload.elapsed() >= LAGGED_DEBOUNCE
                                && let Ok(r) = E::find().all(&db).await
                            {
                                rows = r;
                                rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                                db.set_table_cache(rows.clone());
                                signal.set(rows.clone());
                                last_full_reload = Instant::now();
                            }
                            break;
                        }
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return,
                        Err(_timeout) => break,
                    }
                }

                if batch.is_empty() {
                    continue;
                }

                let mut needs_refetch: Vec<String> = Vec::new();
                let mut changed = false;

                for notif in batch {
                    if notif.table != target_table {
                        continue;
                    }
                    let pk_str = notif.primary_key.0.clone();
                    changed = true;

                    match notif.kind {
                        WriteKind::Delete => {
                            if let Some(idx) = pk_index.remove(&pk_str) {
                                rows.swap_remove(idx);
                                if idx < rows.len() {
                                    let moved_pk = SyncedModel::wavesync_pk_string(&rows[idx]);
                                    pk_index.insert(moved_pk, idx);
                                }
                            }
                        }
                        WriteKind::Update => {
                            if let Some(cols) = &notif.column_values {
                                if let Some(&idx) = pk_index.get(&pk_str) {
                                    for (col, val) in cols {
                                        SyncedModel::wavesync_apply_change(
                                            &mut rows[idx],
                                            &col.0,
                                            val,
                                        );
                                    }
                                } else {
                                    needs_refetch.push(pk_str);
                                }
                            } else {
                                needs_refetch.push(pk_str);
                            }
                        }
                        WriteKind::Insert => {
                            if let Some(cols) = &notif.column_values {
                                let pairs: Vec<(String, serde_json::Value)> =
                                    cols.iter().map(|(c, v)| (c.0.clone(), v.clone())).collect();
                                if let Some(model) =
                                    E::Model::wavesync_from_changes(&pk_column, &pk_str, &pairs)
                                {
                                    if let Some(&idx) = pk_index.get(&pk_str) {
                                        rows[idx] = model;
                                    } else {
                                        pk_index.insert(pk_str.clone(), rows.len());
                                        rows.push(model);
                                    }
                                } else {
                                    needs_refetch.push(pk_str);
                                }
                            } else {
                                needs_refetch.push(pk_str);
                            }
                        }
                    }
                }

                for pk_str in &needs_refetch {
                    let pk_typed: <E::PrimaryKey as PrimaryKeyTrait>::ValueType =
                        pk_str.to_string().into();
                    match E::find_by_id(pk_typed).one(&db).await {
                        Ok(Some(row)) => {
                            if let Some(&idx) = pk_index.get(pk_str) {
                                rows[idx] = row;
                            } else {
                                pk_index.insert(pk_str.clone(), rows.len());
                                rows.push(row);
                            }
                        }
                        Ok(None) => {
                            if let Some(idx) = pk_index.remove(pk_str) {
                                rows.swap_remove(idx);
                                if idx < rows.len() {
                                    let moved_pk = SyncedModel::wavesync_pk_string(&rows[idx]);
                                    pk_index.insert(moved_pk, idx);
                                }
                            }
                        }
                        Err(e) => log::error!("Failed to refresh row {}: {}", pk_str, e),
                    }
                }

                if changed || !needs_refetch.is_empty() {
                    db.set_table_cache(rows.clone());
                    signal.set(rows.clone());
                }
            }
        });
    });

    signal
}

/// Reactive signal for a single row, looked up by primary key.
///
/// Performs an initial `E::find_by_id(pk).one(db)` query, then watches for
/// [`ChangeNotification`]s **filtered by the same primary key** — unrelated
/// row changes never wake this hook. When a relevant notification arrives,
/// the hook applies its `column_values` payload via
/// [`SyncedModel`](crate::SyncedModel) without re-querying SeaORM.
///
/// Returns a `Signal<Option<E::Model>>` — `None` if the row doesn't exist.
pub fn use_synced_row<E>(
    db: WaveSyncDb,
    pk: <E::PrimaryKey as PrimaryKeyTrait>::ValueType,
) -> Signal<Option<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + std::fmt::Display,
{
    let mut signal: Signal<Option<E::Model>> = use_signal(|| None);
    let pk_string = format!("{}", pk);
    let target_table = E::default().table_name().to_string();
    let pk_column = pk_column_name::<E>();

    use_effect(move || {
        let mut rx = db.change_rx();
        let pk = pk.clone();
        let pk_string = pk_string.clone();
        let target_table = target_table.clone();
        let pk_column = pk_column.clone();
        let db = db.clone();
        spawn(async move {
            // Try the table cache first — avoids a DB round trip on
            // page re-navigation when use_synced_table already loaded.
            let mut current: Option<E::Model> = db
                .get_table_cache::<Vec<E::Model>>()
                .and_then(|rows| {
                    rows.into_iter()
                        .find(|r| SyncedModel::wavesync_pk_string(r) == pk_string)
                })
                .or_else(|| {
                    // Fallback: query by PK (blocking — runs on first load)
                    None
                });
            if current.is_none() {
                current = match E::find_by_id(pk.clone()).one(&db).await {
                    Ok(row) => row,
                    Err(e) => {
                        log::error!("Failed initial row load: {}", e);
                        None
                    }
                };
            }
            signal.set(current.clone());

            let mut last_full_reload = Instant::now();
            let mut refresh_rx = db.refresh_rx();

            loop {
                // Either a change notification or a resume/refresh tick. A tick
                // means a background-sync process may have written the DB with no
                // in-process notification, so re-query this row.
                let first = tokio::select! {
                    biased;
                    r = refresh_rx.recv() => {
                        match r {
                            Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                                    current = row;
                                    signal.set(current.clone());
                                    last_full_reload = Instant::now();
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                        continue;
                    }
                    n = rx.recv() => n,
                };
                let first = match first {
                    Ok(n) => n,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Missed {} change notifications for {}", n, target_table);
                        if last_full_reload.elapsed() >= LAGGED_DEBOUNCE
                            && let Ok(row) = E::find_by_id(pk.clone()).one(&db).await
                        {
                            current = row;
                            signal.set(current.clone());
                            last_full_reload = Instant::now();
                        }
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };

                let mut batch = vec![first];
                let deadline = Instant::now() + BATCH_WINDOW;
                loop {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Ok(n)) => batch.push(n),
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                            log::warn!("Missed {} change notifications for {}", n, target_table);
                            batch.clear();
                            if last_full_reload.elapsed() >= LAGGED_DEBOUNCE
                                && let Ok(row) = E::find_by_id(pk.clone()).one(&db).await
                            {
                                current = row;
                                signal.set(current.clone());
                                last_full_reload = Instant::now();
                            }
                            break;
                        }
                        Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return,
                        Err(_timeout) => break,
                    }
                }

                if batch.is_empty() {
                    continue;
                }

                let mut changed = false;
                let mut needs_refetch = false;

                for notif in batch {
                    if notif.table != target_table || notif.primary_key.0 != pk_string {
                        continue;
                    }
                    changed = true;

                    match notif.kind {
                        WriteKind::Delete => {
                            current = None;
                        }
                        WriteKind::Update => {
                            if let Some(cols) = &notif.column_values {
                                if let Some(ref mut model) = current {
                                    for (col, val) in cols {
                                        SyncedModel::wavesync_apply_change(model, &col.0, val);
                                    }
                                } else {
                                    needs_refetch = true;
                                }
                            } else {
                                needs_refetch = true;
                            }
                        }
                        WriteKind::Insert => {
                            if let Some(cols) = &notif.column_values {
                                let pairs: Vec<(String, serde_json::Value)> =
                                    cols.iter().map(|(c, v)| (c.0.clone(), v.clone())).collect();
                                if let Some(model) =
                                    E::Model::wavesync_from_changes(&pk_column, &pk_string, &pairs)
                                {
                                    current = Some(model);
                                } else {
                                    needs_refetch = true;
                                }
                            } else {
                                needs_refetch = true;
                            }
                        }
                    }
                }

                if needs_refetch && let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                    current = row;
                    changed = true;
                }

                if changed {
                    signal.set(current.clone());
                }
            }
        });
    });

    signal
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Look up the primary-key column name for an entity. Mirrors what the
/// `SyncEntity` derive emits at registration time.
fn pk_column_name<E: EntityTrait>() -> String {
    use sea_orm::{IdenStatic, Iterable, PrimaryKeyToColumn};
    <E::PrimaryKey as Iterable>::iter()
        .next()
        .map(|pk| IdenStatic::as_str(&pk.into_column()).to_string())
        .unwrap_or_default()
}

fn rebuild_pk_index<M: SyncedModel>(rows: &[M], index: &mut HashMap<String, usize>) {
    index.clear();
    for (i, row) in rows.iter().enumerate() {
        index.insert(SyncedModel::wavesync_pk_string(row), i);
    }
}

// ---------------------------------------------------------------------------
// App lifecycle hooks
// ---------------------------------------------------------------------------

/// Watches a foreground signal and calls [`WaveSyncDb::resume()`] when the app
/// transitions from background to foreground (`false` → `true`).
///
/// The app developer is responsible for setting the `foreground` signal from
/// platform-specific lifecycle callbacks:
///
/// - **Android**: `onResume` / `onPause` via JNI
/// - **iOS**: `applicationDidBecomeActive` / `applicationWillResignActive`
/// - **Desktop**: window focus events (optional)
///
/// ```ignore
/// let mut foreground = use_signal(|| true);
/// use_app_resume(db, foreground);
///
/// // In your platform callback:
/// foreground.set(true);  // app came to foreground
/// foreground.set(false); // app went to background
/// ```
pub fn use_app_resume(db: WaveSyncDb, foreground: Signal<bool>) {
    let mut was_foreground = use_signal(|| true);

    use_effect(move || {
        let is_fg = *foreground.read();
        let was_fg = *was_foreground.peek(); // peek() — don't subscribe

        if is_fg != was_fg {
            was_foreground.set(is_fg);
            if is_fg && !was_fg {
                db.resume();
            }
        }
    });
}

/// Automatically detects app lifecycle transitions and calls
/// [`WaveSyncDb::resume()`] when the app returns to foreground.
///
/// On **Android**, polls `hasWindowFocus()` via JNI to detect foreground state.
/// On **iOS**, observes `UIApplicationDidBecomeActiveNotification` /
/// `UIApplicationWillResignActiveNotification` via `NSNotificationCenter`.
/// On **desktop** and other platforms, this is a no-op — use
/// [`use_app_resume()`] with a manual signal if you need desktop lifecycle.
///
/// ```ignore
/// use_auto_lifecycle(db);  // One line — done.
/// ```
pub fn use_auto_lifecycle(db: WaveSyncDb) {
    let rx = use_hook(|| {
        let (tx, rx) = tokio::sync::watch::channel(true);
        std::thread::Builder::new()
            .name("wavesync-lifecycle".into())
            .spawn(move || {
                super::lifecycle::start_lifecycle_listener(tx);
            })
            .ok();
        rx
    });

    use_effect(move || {
        let mut rx = rx.clone();
        let db = db.clone();
        let mut was_foreground = true;
        spawn(async move {
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let is_foreground = *rx.borrow_and_update();
                if is_foreground && !was_foreground {
                    log::info!("Auto-lifecycle: app resumed, triggering sync");
                    db.resume();
                }
                was_foreground = is_foreground;
            }
        });
    });
}

/// Deprecated no-op retained for source compatibility.
///
/// Push-notification registration is now fully automatic on both mobile
/// platforms — the Swift Package's `+load` method installs the iOS APNs
/// delegate selectors at image load, and the Android `WaveSyncInitProvider`
/// ContentProvider handles the equivalent on Android via manifest merging.
/// `WaveSyncDbBuilder::build()` wires everything up.
///
/// Remove calls to this function at your convenience. Kept for one release
/// so the `examples/dioxus_fcm_sync` demo and other downstream consumers do
/// not break.
#[deprecated(note = "Push registration is now automatic; remove this call.")]
pub fn use_auto_push(_db: WaveSyncDb) {}
