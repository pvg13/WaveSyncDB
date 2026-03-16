//! Reactive Dioxus hooks for synced database tables.
//!
//! These hooks provide Dioxus signals that automatically refresh when the underlying
//! data changes — whether from local writes or remote sync operations. They subscribe
//! to [`ChangeNotification`](crate::ChangeNotification) events via
//! [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx).

use std::future::Future;

use dioxus::prelude::*;
use sea_orm::{DbErr, EntityTrait, FromQueryResult};

use crate::{NetworkStatus, WaveSyncDb, WaveSyncDbBuilder};

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

// ---------------------------------------------------------------------------
// Reactive table/row hooks
// ---------------------------------------------------------------------------

/// Reactive signal containing all rows in a table.
///
/// Performs an initial `E::find().all(db)` query, then re-queries whenever a
/// [`ChangeNotification`](crate::ChangeNotification) is received for this table.
///
/// Returns a `Signal<Vec<E::Model>>` that Dioxus components can `.read()` to
/// get the current rows and will automatically re-render when the data changes.
pub fn use_synced_table<E>(db: WaveSyncDb) -> Signal<Vec<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Clone + Send + Sync + 'static,
{
    let mut signal = use_signal(Vec::new);

    // Initial load
    let db2 = db.clone();
    use_effect(move || {
        let db = db2.clone();
        spawn(async move {
            match E::find().all(&db).await {
                Ok(rows) => signal.set(rows),
                Err(e) => log::error!("Failed initial table load: {}", e),
            }
        });
    });

    // Subscribe to changes (filtered by table name)
    use_effect(move || {
        let mut rx = db.change_rx();
        let target_table = E::default().table_name().to_string();
        let db = db.clone();
        spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(notification) => {
                        if notification.table != target_table {
                            continue;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Missed {} change notifications for {}", n, target_table);
                        // Fall through to re-query below
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
                match E::find().all(&db).await {
                    Ok(rows) => signal.set(rows),
                    Err(e) => log::error!("Failed to refresh table: {}", e),
                }
            }
        });
    });

    signal
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

/// Reactive signal for a single row, looked up by primary key.
///
/// Performs an initial `E::find_by_id(pk).one(db)` query, then re-queries whenever
/// a [`ChangeNotification`](crate::ChangeNotification) is received for this table.
///
/// Returns a `Signal<Option<E::Model>>` — `None` if the row doesn't exist.
pub fn use_synced_row<E>(
    db: WaveSyncDb,
    pk: <E::PrimaryKey as sea_orm::PrimaryKeyTrait>::ValueType,
) -> Signal<Option<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Clone + Send + Sync + 'static,
    <E::PrimaryKey as sea_orm::PrimaryKeyTrait>::ValueType:
        Clone + Send + Sync + 'static + Into<sea_orm::Value>,
{
    let mut signal: Signal<Option<E::Model>> = use_signal(|| None);
    let pk_clone = pk.clone();

    // Initial load
    let db2 = db.clone();
    use_effect(move || {
        let pk = pk_clone.clone();
        let db = db2.clone();
        spawn(async move {
            match E::find_by_id(pk).one(&db).await {
                Ok(row) => signal.set(row),
                Err(e) => log::error!("Failed initial row load: {}", e),
            }
        });
    });

    // Subscribe to changes (filtered by table name)
    use_effect(move || {
        let mut rx = db.change_rx();
        let pk = pk.clone();
        let target_table = E::default().table_name().to_string();
        let db = db.clone();
        spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(notification) => {
                        if notification.table != target_table {
                            continue;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        log::warn!("Missed {} change notifications for {}", n, target_table);
                        // Fall through to re-query below
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
                let pk = pk.clone();
                match E::find_by_id(pk).one(&db).await {
                    Ok(row) => signal.set(row),
                    Err(e) => log::error!("Failed to refresh row: {}", e),
                }
            }
        });
    });

    signal
}
