//! Reactive Dioxus hooks for synced database tables.
//!
//! These hooks provide Dioxus signals that automatically refresh when the underlying
//! data changes — whether from local writes or remote sync operations. They subscribe
//! to [`ChangeNotification`](wavesyncdb::ChangeNotification) events via
//! [`WaveSyncDb::change_rx()`](wavesyncdb::WaveSyncDb::change_rx).

use dioxus::prelude::*;
use sea_orm::{EntityTrait, FromQueryResult};
use wavesyncdb::WaveSyncDb;

/// Reactive signal containing all rows in a table.
///
/// Performs an initial `E::find().all(db)` query, then re-queries whenever a
/// [`ChangeNotification`](wavesyncdb::ChangeNotification) is received (from any
/// table — filtering by table name can be added in the future).
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

    // Subscribe to changes
    use_effect(move || {
        let mut rx = db.change_rx();
        spawn(async move {
            while let Ok(_notification) = rx.recv().await {
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
/// a [`ChangeNotification`](wavesyncdb::ChangeNotification) is received.
///
/// Returns a `Signal<Option<E::Model>>` — `None` if the row doesn't exist.
///
/// The primary key type must implement `Clone + Send + Sync + Into<sea_orm::Value>`.
pub fn use_synced_row<E>(
    db: &'static WaveSyncDb,
    pk: <E::PrimaryKey as sea_orm::PrimaryKeyTrait>::ValueType,
) -> Signal<Option<E::Model>>
where
    E: EntityTrait,
    E::Model: FromQueryResult + Clone + Send + Sync + 'static,
    <E::PrimaryKey as sea_orm::PrimaryKeyTrait>::ValueType: Clone + Send + Sync + 'static + Into<sea_orm::Value>,
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

    // Subscribe to changes
    use_effect(move || {
        let mut rx = db.change_rx();
        let pk = pk.clone();
        spawn(async move {
            while let Ok(_notification) = rx.recv().await {
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
