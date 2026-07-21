//! Reactive Dioxus hooks for synced database tables.
//!
//! These hooks provide Dioxus signals that automatically refresh when the underlying
//! data changes — whether from local writes or remote sync operations. They subscribe
//! to [`ChangeNotification`](crate::ChangeNotification) events via
//! [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx) and apply the carried
//! `column_values` payload in place via [`SyncedModel`](crate::SyncedModel) — no
//! per-notification SeaORM round trip on the receive path.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;

use dioxus::dioxus_core::{Task, spawn_forever};
use dioxus::prelude::*;
use sea_orm::{DbErr, EntityTrait, FromQueryResult, PrimaryKeyTrait};
use tokio::time::{Duration, Instant};

use crate::{NetworkStatus, SyncedModel, WaveSyncDb, WaveSyncDbBuilder, WriteKind};

const BATCH_WINDOW: Duration = Duration::from_millis(16);
const LAGGED_DEBOUNCE: Duration = Duration::from_millis(500);

/// Process-global guard so the auto-resume lifecycle listener starts at most once.
#[cfg(not(target_arch = "wasm32"))]
static AUTO_RESUME_STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();

/// Start a single process-wide lifecycle listener that drives
/// [`WaveSyncDb::resume`] on every return to foreground, so synced hooks pick up
/// writes made by a push-triggered background sync (which runs in a *separate
/// process* and emits no in-process notification) — even when the app never
/// explicitly calls [`use_auto_lifecycle`]. Idempotent: only the first caller
/// starts the listener.
///
/// Runs on plain OS threads (not Dioxus tasks) so it survives component
/// unmounts, and holds only a [`WaveSyncDb::resume_trigger`] closure (channel
/// senders, no node `Arc`) so it never keeps the engine alive past teardown. On
/// desktop the platform listener is a no-op — its sender drops immediately, the
/// driver's `changed()` errors, and the driver thread exits at once, so nothing
/// lingers in tests.
#[cfg(not(target_arch = "wasm32"))]
fn ensure_auto_resume(db: &WaveSyncDb) {
    if AUTO_RESUME_STARTED.set(()).is_err() {
        return;
    }
    let trigger = db.resume_trigger();
    let (tx, mut rx) = tokio::sync::watch::channel(true);
    std::thread::Builder::new()
        .name("wavesync-lifecycle".into())
        .spawn(move || super::lifecycle::start_lifecycle_listener(tx))
        .ok();
    std::thread::Builder::new()
        .name("wavesync-auto-resume".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::error!("auto-resume: failed to build runtime: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                let mut was_foreground = true;
                // Wall clock (not Instant): CLOCK_MONOTONIC pauses across
                // device suspend on Android, which would under-report
                // exactly the pocket-doze gaps this measures (#111).
                let mut background_since: Option<std::time::SystemTime> = None;
                loop {
                    if rx.changed().await.is_err() {
                        break; // listener gone (e.g. desktop no-op) — stop.
                    }
                    let is_foreground = *rx.borrow_and_update();
                    if is_foreground && !was_foreground {
                        // A backwards wall jump (NTP) yields None → plain
                        // resume, the safe default.
                        let backgrounded = background_since
                            .take()
                            .and_then(|t| std::time::SystemTime::now().duration_since(t).ok());
                        tracing::info!(
                            backgrounded_secs = backgrounded.map(|d| d.as_secs()).unwrap_or(0),
                            "wavesync: app returned to foreground — resync + UI refresh"
                        );
                        trigger(backgrounded);
                    } else if !is_foreground && was_foreground {
                        background_since = Some(std::time::SystemTime::now());
                    }
                    was_foreground = is_foreground;
                }
            });
        })
        .ok();
}

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
            // Root-scope spawn: `reset()`'s canonical caller is a logout
            // handler that tears down the UI right after, and a scope-tied
            // `spawn` gets cancelled by that unmount BEFORE the shutdown
            // command is sent — the old engine (and its peer identity) keeps
            // running alongside the next login's engine (#105). The engine's
            // own tasks hold `WaveSyncDb` clones, so nothing is reclaimed by
            // Drop; only an explicit `shutdown()` stops it.
            spawn_forever(async move {
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
            tracing::warn!("use_wavesync_init: DB already initialized, ignoring");
            return Ok(());
        }

        let current_gen = self.generation();

        let builder = WaveSyncDbBuilder::new(url, topic);
        let db = configure(builder).build().await?;

        // Check if a reset happened while we were building the DB
        if self.generation() != current_gen {
            tracing::warn!("use_wavesync_init: generation changed during build, discarding new DB");
            // Same unmount-cancellation hazard as `reset()` (#105): a
            // generation bump means a reset/logout is in progress, so THIS
            // task's scope may be about to unmount — hand the discard to the
            // root scope instead of awaiting it here.
            spawn_forever(async move {
                db.shutdown().await;
            });
            return Ok(());
        }

        setup(db.clone()).await?;

        // Double-check generation after setup
        if self.generation() != current_gen {
            tracing::warn!("use_wavesync_init: generation changed during setup, discarding new DB");
            spawn_forever(async move {
                db.shutdown().await;
            });
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

/// Spawn a driver task from a `use_effect`, cancelling the previous one.
///
/// Reads the app's generation counter (provided by
/// [`use_wavesync_generation`]) INSIDE the effect so an `InitDb::reset()` +
/// re-init re-runs the effect with the freshly rendered `db` and swaps the
/// driver. Cancelling the old task drops its `WaveSyncDb` clone — without
/// this, a mounted hook pins the dead engine's SQLite file open forever
/// after a reset. Apps that pass a `db` without the context pattern get no
/// generation dependency: same behavior as before, re-attach on engine swap
/// requires the context pattern (documented on the hooks).
///
/// `start` must spawn via [`dioxus::prelude::spawn`] and nothing else — the
/// `LocalPublish` wrapper the hooks pass through it is only sound because
/// the driver future never migrates off the Dioxus thread that runs it.
fn use_driver_task(db: WaveSyncDb, start: impl Fn(WaveSyncDb) -> Task + 'static) {
    let generation = try_use_context::<Signal<u64>>();
    let slot: Rc<Cell<Option<Task>>> = use_hook(|| Rc::new(Cell::new(None)));

    use_effect(move || {
        if let Some(generation) = generation {
            // Reactive dependency: a reset/re-init bumps this and re-runs
            // the effect with the new db captured by the latest render.
            let _ = generation.read();
        }
        if let Some(prev) = slot.take() {
            prev.cancel();
        }
        slot.set(Some(start(db.clone())));
    });
}

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
    let signal: Signal<Vec<E::Model>> = use_signal(Vec::new);

    // Drive resume() on app foreground so a background-sync process's writes
    // surface in the UI even without an explicit `use_auto_lifecycle` call.
    #[cfg(not(target_arch = "wasm32"))]
    ensure_auto_resume(&db);

    use_driver_task(db, move |db| {
        // `run_table_driver` needs its callback bound `Send` so plain-tokio
        // tests can drive it via `tokio::spawn` without a Dioxus runtime.
        // The `Signal` this hook publishes to is intentionally `!Send`
        // (its storage is thread-local) — wrap it so the bound is satisfied.
        // Sound because `dioxus::spawn` below runs the whole future on a
        // single-threaded local task and never migrates it across threads,
        // so the wrapped `Signal` is never touched from another thread.
        let mut publish = LocalPublish(signal);
        spawn(async move {
            run_table_driver::<E>(db, move |rows| publish.publish(rows)).await;
        })
    });

    signal
}

/// Like [`use_synced_table_db`] but distinguishes "still loading" from
/// "loaded and empty": `None` until the initial query resolves, then
/// `Some(rows)` forever — including when deletes empty the table. On a
/// failed initial query the driver logs and publishes the empty snapshot
/// (see the comment on that fallback in [`run_table_driver`]), so
/// consumers never block on `None` indefinitely. Use this for one-shot
/// hydration latches that must not fire on a mid-load empty read.
pub fn use_synced_table_loaded<E>(db: WaveSyncDb) -> Signal<Option<Vec<E::Model>>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + From<String>,
{
    let signal: Signal<Option<Vec<E::Model>>> = use_signal(|| None);

    #[cfg(not(target_arch = "wasm32"))]
    ensure_auto_resume(&db);

    use_driver_task(db, move |db| {
        // Same `!Send` bridging as `use_synced_table_db` — see that hook's
        // comment. `publish_some` wraps each snapshot in `Some(..)` so the
        // first publish is the observable loading -> loaded transition.
        let mut publish = LocalPublish(signal);
        spawn(async move {
            run_table_driver::<E>(db, move |rows| publish.publish_some(rows)).await;
        })
    });

    signal
}

/// Bridges a `!Send` closure capture (a Dioxus `Signal`) across the `Send`
/// bound that [`run_table_driver`] and [`run_row_driver`] require of their
/// `publish` callback.
///
/// `Signal`'s lack of `Send` reflects its thread-local storage — this
/// wrapper doesn't change that, it only asserts (per the safety note at its
/// use site) that the value is never actually accessed from a second
/// thread. The `publish` method (rather than field access) is what makes
/// the enclosing closure capture the whole wrapper instead of reaching
/// through to the inner `!Send` `Signal` directly.
struct LocalPublish<T>(Signal<T>);

impl<T: 'static> LocalPublish<T> {
    fn publish(&mut self, value: T) {
        self.0.set(value);
    }
}

impl<T: 'static> LocalPublish<Option<T>> {
    /// Wraps `value` in `Some` before publishing. Used by the `_loaded`
    /// hook variants, whose signal is `Signal<Option<Inner>>` starting at
    /// `None` (still loading) — the driver's callback only ever hands back
    /// the unwrapped `Inner`, so this is where the `Some(..)` distinguishing
    /// "loaded" from "loading" gets applied.
    fn publish_some(&mut self, value: T) {
        self.0.set(Some(value));
    }
}

// SAFETY: see the comment at each construction site (`use_synced_table_db`,
// `use_synced_row`) — the wrapped value is only ever touched from the single
// thread that runs the `dioxus::spawn`-driven task.
unsafe impl<T> Send for LocalPublish<T> {}

/// Drives one table subscription: initial load (cache fast path), then
/// change-notification batches applied in place, publishing a full snapshot
/// after the initial load and after every applied batch/reload. This is the
/// hook loop, extracted so it can be driven and asserted by plain tokio
/// tests — the Dioxus hooks are thin wrappers that feed `publish` into a
/// `Signal`. Hidden from docs: not a public API commitment.
#[doc(hidden)]
pub async fn run_table_driver<E>(
    db: WaveSyncDb,
    mut publish: impl FnMut(Vec<E::Model>) + Send + 'static,
) where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + From<String>,
{
    let target_table = E::default().table_name().to_string();
    let pk_column = pk_column_name::<E>();

    let mut rx = db.change_rx();

    // Check in-memory cache first — instant on page re-navigation.
    let mut rows: Vec<E::Model> = if let Some(cached) = db.get_table_cache::<Vec<E::Model>>() {
        cached
    } else {
        match E::find().all(&db).await {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed initial table load: {}", e);
                // Falls through to `publish(rows.clone())` below with the
                // empty Vec, same as a legitimately empty table. This is
                // load-bearing for `use_synced_table_loaded`: a failed
                // initial query still counts as "loaded" (Some([])), so a
                // `_loaded` consumer never blocks on `None` forever.
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
    publish(rows.clone());

    let mut last_full_reload = Instant::now();
    // A lagged full reload the debounce window pushed into the future. A
    // debounced `Lagged` must DEFER the reload, never drop it: the missed
    // notifications' rows have no other trigger, and later in-place applies
    // land on that stale base — without this timer the staleness persists
    // until the next unrelated change (#89 / H7).
    let mut lagged_reload_due: Option<Instant> = None;
    let mut refresh_rx = db.refresh_rx();
    // Closed means every sender is gone. While this driver holds `db` the
    // inner sender field cannot drop, so this is effectively unreachable —
    // but if it ever fires (future refactor weakens the hold), re-subscribe
    // from the handle and reload once; a second immediate Closed means the
    // handle is truly dead and the driver exits cleanly. A successful receive
    // on either the primary or refresh channel proves the driver is alive and
    // clears this flag, preventing two unrelated Closed events from triggering exit.
    let mut closed_retry = false;

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
                        closed_retry = false;
                        if let Ok(r) = E::find().all(&db).await {
                            rows = r;
                            rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                            db.set_table_cache(rows.clone());
                            publish(rows.clone());
                            last_full_reload = Instant::now();
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        if closed_retry {
                            return;
                        }
                        closed_retry = true;
                        rx = db.change_rx();
                        refresh_rx = db.refresh_rx();
                        if let Ok(r) = E::find().all(&db).await {
                            rows = r;
                            rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                            db.set_table_cache(rows.clone());
                            publish(rows.clone());
                        }
                        continue;
                    }
                }
                lagged_reload_due = None;
                continue;
            }
            _ = async {
                match lagged_reload_due {
                    Some(due) => tokio::time::sleep_until(due).await,
                    None => std::future::pending().await,
                }
            }, if lagged_reload_due.is_some() => {
                // The deferred lagged reload came due (see `lagged_reload_due`).
                lagged_reload_due = None;
                if let Ok(r) = E::find().all(&db).await {
                    rows = r;
                    rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                    db.set_table_cache(rows.clone());
                    publish(rows.clone());
                    last_full_reload = Instant::now();
                }
                continue;
            }
            n = rx.recv() => n,
        };
        let first = match first {
            Ok(n) => {
                closed_retry = false;
                n
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("Missed {} change notifications for {}", n, target_table);
                if last_full_reload.elapsed() >= LAGGED_DEBOUNCE {
                    if let Ok(r) = E::find().all(&db).await {
                        rows = r;
                        rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                        db.set_table_cache(rows.clone());
                        publish(rows.clone());
                        last_full_reload = Instant::now();
                        lagged_reload_due = None;
                    }
                } else {
                    // Defer, never drop (see `lagged_reload_due`).
                    lagged_reload_due.get_or_insert(last_full_reload + LAGGED_DEBOUNCE);
                }
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                if closed_retry {
                    return;
                }
                closed_retry = true;
                rx = db.change_rx();
                refresh_rx = db.refresh_rx();
                if let Ok(r) = E::find().all(&db).await {
                    rows = r;
                    rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                    db.set_table_cache(rows.clone());
                    publish(rows.clone());
                }
                continue;
            }
        };

        // Drain additional notifications within the batch window.
        let mut batch = vec![first];
        let deadline = Instant::now() + BATCH_WINDOW;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(n)) => batch.push(n),
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!("Missed {} change notifications for {}", n, target_table);
                    batch.clear();
                    if last_full_reload.elapsed() >= LAGGED_DEBOUNCE {
                        if let Ok(r) = E::find().all(&db).await {
                            rows = r;
                            rebuild_pk_index::<E::Model>(&rows, &mut pk_index);
                            db.set_table_cache(rows.clone());
                            publish(rows.clone());
                            last_full_reload = Instant::now();
                            lagged_reload_due = None;
                        }
                    } else {
                        // Defer, never drop (see `lagged_reload_due`).
                        lagged_reload_due.get_or_insert(last_full_reload + LAGGED_DEBOUNCE);
                    }
                    break;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
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
                                SyncedModel::wavesync_apply_change(&mut rows[idx], &col.0, val);
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
            let pk_typed: <E::PrimaryKey as PrimaryKeyTrait>::ValueType = pk_str.to_string().into();
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
                Err(e) => tracing::error!("Failed to refresh row {}: {}", pk_str, e),
            }
        }

        if changed || !needs_refetch.is_empty() {
            db.set_table_cache(rows.clone());
            publish(rows.clone());
        }
    }
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
    let signal: Signal<Option<E::Model>> = use_signal(|| None);

    // See `use_synced_table_db`: auto-drive resume() so background-sync writes
    // surface on foreground without an explicit `use_auto_lifecycle` call.
    #[cfg(not(target_arch = "wasm32"))]
    ensure_auto_resume(&db);

    use_driver_task(db, move |db| {
        let pk = pk.clone();
        // See `use_synced_table_db`: `run_row_driver` needs its callback
        // bound `Send`, but the `Signal` this hook publishes to is
        // intentionally `!Send` — wrap it so the bound is satisfied. Sound
        // for the same reason: `spawn` below runs the whole future on a
        // single-threaded local task and never migrates it across threads.
        let mut publish = LocalPublish(signal);
        spawn(async move {
            run_row_driver::<E>(db, pk, move |row| publish.publish(row)).await;
        })
    });

    signal
}

/// Like [`use_synced_row`] but distinguishes "still loading" from "row
/// absent": outer `None` = loading; `Some(None)` = loaded, row absent;
/// `Some(Some(model))` = loaded and present. On a failed initial query the
/// driver logs and publishes `Some(None)` (see the comment on that
/// fallback in [`run_row_driver`]), so consumers never block on the outer
/// `None` indefinitely.
pub fn use_synced_row_loaded<E>(
    db: WaveSyncDb,
    pk: <E::PrimaryKey as PrimaryKeyTrait>::ValueType,
) -> Signal<Option<Option<E::Model>>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + std::fmt::Display,
{
    let signal: Signal<Option<Option<E::Model>>> = use_signal(|| None);

    #[cfg(not(target_arch = "wasm32"))]
    ensure_auto_resume(&db);

    use_driver_task(db, move |db| {
        let pk = pk.clone();
        // Same `!Send` bridging as `use_synced_row` — see that hook's
        // comment. `publish_some` wraps each snapshot in `Some(..)` so the
        // first publish is the observable loading -> loaded transition.
        let mut publish = LocalPublish(signal);
        spawn(async move {
            run_row_driver::<E>(db, pk, move |row| publish.publish_some(row)).await;
        })
    });

    signal
}

/// Drives one row subscription: initial load (cache fast path, falling back
/// to `find_by_id`), then change-notification batches — filtered to this
/// row's primary key — applied in place, publishing a snapshot after the
/// initial load and after every applied batch/reload. Extracted from the
/// hook loop so it can be driven and asserted by plain tokio tests; the
/// Dioxus hook is a thin wrapper that feeds `publish` into a `Signal`.
/// Hidden from docs: not a public API commitment.
#[doc(hidden)]
pub async fn run_row_driver<E>(
    db: WaveSyncDb,
    pk: <E::PrimaryKey as PrimaryKeyTrait>::ValueType,
    publish: impl FnMut(Option<E::Model>) + Send + 'static,
) where
    E: EntityTrait,
    E::Model: FromQueryResult + SyncedModel + Clone + Send + Sync + 'static,
    <E::PrimaryKey as PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value> + std::fmt::Display,
{
    let mut publish = publish;
    let pk_string = format!("{}", pk);
    let target_table = E::default().table_name().to_string();
    let pk_column = pk_column_name::<E>();

    let mut rx = db.change_rx();

    // Try the table cache first — avoids a DB round trip on page
    // re-navigation when use_synced_table already loaded.
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
                tracing::error!("Failed initial row load: {}", e);
                // Falls through to `publish(current.clone())` below with
                // None, same as a legitimately absent row. Load-bearing for
                // `use_synced_row_loaded`: a failed initial query still
                // counts as "loaded" (Some(None)), so a `_loaded` consumer
                // never blocks on the outer `None` forever.
                None
            }
        };
    }
    publish(current.clone());

    let mut last_full_reload = Instant::now();
    // Deferred lagged re-query — same rationale as the table driver's
    // `lagged_reload_due` (#89 / H7): a debounced `Lagged` must defer the
    // re-query, never drop it, or this row can stay stale indefinitely.
    let mut lagged_reload_due: Option<Instant> = None;
    let mut refresh_rx = db.refresh_rx();
    // Closed means every sender is gone. While this driver holds `db` the
    // inner sender field cannot drop, so this is effectively unreachable —
    // but if it ever fires (future refactor weakens the hold), re-subscribe
    // from the handle and reload once; a second immediate Closed means the
    // handle is truly dead and the driver exits cleanly. A successful receive
    // on either the primary or refresh channel proves the driver is alive and
    // clears this flag, preventing two unrelated Closed events from triggering exit.
    let mut closed_retry = false;

    loop {
        // Either a change notification or a resume/refresh tick. A tick
        // means a background-sync process may have written the DB with no
        // in-process notification, so re-query this row.
        let first = tokio::select! {
            biased;
            r = refresh_rx.recv() => {
                match r {
                    Ok(()) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        closed_retry = false;
                        if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                            current = row;
                            publish(current.clone());
                            last_full_reload = Instant::now();
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        if closed_retry {
                            return;
                        }
                        closed_retry = true;
                        rx = db.change_rx();
                        refresh_rx = db.refresh_rx();
                        if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                            current = row;
                            publish(current.clone());
                        }
                        continue;
                    }
                }
                lagged_reload_due = None;
                continue;
            }
            _ = async {
                match lagged_reload_due {
                    Some(due) => tokio::time::sleep_until(due).await,
                    None => std::future::pending().await,
                }
            }, if lagged_reload_due.is_some() => {
                // The deferred lagged re-query came due (see `lagged_reload_due`).
                lagged_reload_due = None;
                if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                    current = row;
                    publish(current.clone());
                    last_full_reload = Instant::now();
                }
                continue;
            }
            n = rx.recv() => n,
        };
        let first = match first {
            Ok(n) => {
                closed_retry = false;
                n
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("Missed {} change notifications for {}", n, target_table);
                if last_full_reload.elapsed() >= LAGGED_DEBOUNCE {
                    if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                        current = row;
                        publish(current.clone());
                        last_full_reload = Instant::now();
                        lagged_reload_due = None;
                    }
                } else {
                    // Defer, never drop (see `lagged_reload_due`).
                    lagged_reload_due.get_or_insert(last_full_reload + LAGGED_DEBOUNCE);
                }
                continue;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                if closed_retry {
                    return;
                }
                closed_retry = true;
                rx = db.change_rx();
                refresh_rx = db.refresh_rx();
                if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                    current = row;
                    publish(current.clone());
                }
                continue;
            }
        };

        let mut batch = vec![first];
        let deadline = Instant::now() + BATCH_WINDOW;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Ok(n)) => batch.push(n),
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!("Missed {} change notifications for {}", n, target_table);
                    batch.clear();
                    if last_full_reload.elapsed() >= LAGGED_DEBOUNCE {
                        if let Ok(row) = E::find_by_id(pk.clone()).one(&db).await {
                            current = row;
                            publish(current.clone());
                            last_full_reload = Instant::now();
                            lagged_reload_due = None;
                        }
                    } else {
                        // Defer, never drop (see `lagged_reload_due`).
                        lagged_reload_due.get_or_insert(last_full_reload + LAGGED_DEBOUNCE);
                    }
                    break;
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => break,
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
            publish(current.clone());
        }
    }
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
                    tracing::info!("Auto-lifecycle: app resumed, triggering sync");
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
