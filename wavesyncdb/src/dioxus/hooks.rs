//! Reactive Dioxus hooks for synced database tables.
//!
//! These hooks provide Dioxus signals that automatically refresh when the underlying
//! data changes — whether from local writes or remote sync operations. They subscribe
//! to [`ChangeNotification`](crate::ChangeNotification) events via
//! [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx).

use std::future::Future;

use dioxus::prelude::*;
use sea_orm::{DbErr, EntityTrait, FromQueryResult};

use crate::{WaveSyncDb, WaveSyncDbBuilder};

// ---------------------------------------------------------------------------
// Context providers
// ---------------------------------------------------------------------------

/// Provide a pre-built `WaveSyncDb` to the component tree.
///
/// Call this in your root component when you have a DB ready at launch time.
/// Child components can then use [`use_wavesync()`] to access it.
pub fn use_wavesync_provider(db: &'static WaveSyncDb) {
    use_context_provider(|| db);
    use_context_provider(|| Signal::new(Some(db)));
}

/// Provide a lazy `WaveSyncDb` signal to the component tree.
///
/// Call this in your root component when the DB will be initialized later
/// (e.g., after user picks a file). The signal starts as `None` and becomes
/// `Some` after [`use_wavesync_init()`] completes.
pub fn use_wavesync_provider_lazy() {
    use_context_provider::<Signal<Option<&'static WaveSyncDb>>>(|| Signal::new(None));
}

// ---------------------------------------------------------------------------
// Context consumers
// ---------------------------------------------------------------------------

/// Retrieve the `&'static WaveSyncDb` from Dioxus context.
///
/// Works in both eager ([`use_wavesync_provider`]) and lazy
/// ([`use_wavesync_provider_lazy`]) modes. In lazy mode this **panics** if
/// the DB has not been initialized yet — use [`use_wavesync_opt()`] instead.
pub fn use_wavesync() -> &'static WaveSyncDb {
    // Eager mode: direct &'static ref in context
    if let Some(db) = try_use_context::<&'static WaveSyncDb>() {
        return db;
    }
    // Lazy mode: read the signal
    let sig = use_context::<Signal<Option<&'static WaveSyncDb>>>();
    sig.read()
        .expect("use_wavesync() called before DB was initialized — use use_wavesync_opt() instead")
}

/// Returns a reactive signal that is `None` until the DB is initialized.
///
/// In **eager mode** (via [`use_wavesync_provider`]), the signal is immediately `Some`.
/// In **lazy mode** (via [`use_wavesync_provider_lazy`]), the signal starts as `None`
/// and becomes `Some` after [`use_wavesync_init()`] completes.
pub fn use_wavesync_opt() -> Signal<Option<&'static WaveSyncDb>> {
    use_context::<Signal<Option<&'static WaveSyncDb>>>()
}

/// Returns a handle to initialize the database at runtime.
///
/// Use this in lazy mode to build and inject the DB into context.
pub fn use_wavesync_init() -> InitDb {
    let sig = use_context::<Signal<Option<&'static WaveSyncDb>>>();
    InitDb { sig }
}

/// Handle returned by [`use_wavesync_init()`].
#[derive(Clone, Copy)]
pub struct InitDb {
    sig: Signal<Option<&'static WaveSyncDb>>,
}

impl InitDb {
    /// Clear the current database reference, allowing a new `call()`.
    pub fn reset(&self) {
        let mut sig = self.sig;
        sig.set(None);
    }

    /// Build the database, run the setup closure, and inject it into context.
    pub async fn call<F, Fut>(&self, url: &str, topic: &str, setup: F) -> Result<(), DbErr>
    where
        F: FnOnce(&'static WaveSyncDb) -> Fut,
        Fut: Future<Output = Result<(), DbErr>>,
    {
        if self.sig.read().is_some() {
            log::warn!("use_wavesync_init: DB already initialized, ignoring");
            return Ok(());
        }

        let db = WaveSyncDbBuilder::new(url, topic).build().await?;
        let db: &'static WaveSyncDb = Box::leak(Box::new(db));
        setup(db).await?;

        let mut sig = self.sig;
        sig.set(Some(db));

        Ok(())
    }
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
pub fn use_synced_table<E>(db: &'static WaveSyncDb) -> Signal<Vec<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Clone + Send + Sync + 'static,
{
    let mut signal = use_signal(Vec::new);

    // Initial load
    use_effect(move || {
        spawn(async move {
            match E::find().all(db).await {
                Ok(rows) => signal.set(rows),
                Err(e) => log::error!("Failed initial table load: {}", e),
            }
        });
    });

    // Subscribe to changes (filtered by table name)
    use_effect(move || {
        let mut rx = db.change_rx();
        let target_table = E::default().table_name().to_string();
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
                match E::find().all(db).await {
                    Ok(rows) => signal.set(rows),
                    Err(e) => log::error!("Failed to refresh table: {}", e),
                }
            }
        });
    });

    signal
}

/// Reactive signal for a single row, looked up by primary key.
///
/// Performs an initial `E::find_by_id(pk).one(db)` query, then re-queries whenever
/// a [`ChangeNotification`](crate::ChangeNotification) is received for this table.
///
/// Returns a `Signal<Option<E::Model>>` — `None` if the row doesn't exist.
pub fn use_synced_row<E>(
    db: &'static WaveSyncDb,
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
    use_effect(move || {
        let pk = pk_clone.clone();
        spawn(async move {
            match E::find_by_id(pk).one(db).await {
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
                match E::find_by_id(pk).one(db).await {
                    Ok(row) => signal.set(row),
                    Err(e) => log::error!("Failed to refresh row: {}", e),
                }
            }
        });
    });

    signal
}
