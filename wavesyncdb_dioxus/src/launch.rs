use std::future::Future;

use dioxus::prelude::*;
use sea_orm::DbErr;
use wavesyncdb::{WaveSyncDb, WaveSyncDbBuilder};

/// Launch a Dioxus desktop app with WaveSyncDB fully wired up.
///
/// This handles all the boilerplate:
/// - Creates a multi-threaded tokio runtime and enters it
/// - Builds `WaveSyncDb` via `WaveSyncDbBuilder`
/// - Calls your async `setup` closure (for table creation, `register_table`, etc.)
/// - Leaks the DB to get a `&'static` reference
/// - Provides it via Dioxus context so components can call `use_db()`
/// - Launches Dioxus desktop
///
/// # Example
///
/// ```no_run
/// fn main() {
///     wavesyncdb_dioxus::launch("sqlite::memory:", "my-topic", |db| async move {
///         // create tables, register_table, etc.
///         Ok(())
///     }, App);
/// }
///
/// fn App() -> dioxus::prelude::Element {
///     let db = wavesyncdb_dioxus::use_db();
///     // ... use db with SeaORM operations
///     todo!()
/// }
/// ```
pub fn launch<F, Fut>(url: &str, topic: &str, setup: F, app: fn() -> Element)
where
    F: FnOnce(&'static WaveSyncDb) -> Fut,
    Fut: Future<Output = Result<(), DbErr>>,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    let _guard = rt.enter();

    let db: &'static WaveSyncDb = rt.block_on(async {
        let db = WaveSyncDbBuilder::new(url, topic)
            .build()
            .await
            .expect("Failed to create WaveSyncDb");

        let db: &'static WaveSyncDb = Box::leak(Box::new(db));

        setup(db).await.expect("Setup failed");

        db
    });

    // Store the user's component and the db ref, then launch with our wrapper
    if DB_REF.set(db).is_err() {
        panic!("DB already initialized");
    }
    if USER_APP.set(app).is_err() {
        panic!("App already initialized");
    }

    dioxus::launch(RootWrapper);
}

/// Convenience hook — retrieves the `&'static WaveSyncDb` from Dioxus context.
///
/// Must be called inside a component rendered under `wavesyncdb_dioxus::launch`.
pub fn use_db() -> &'static WaveSyncDb {
    use_context::<&'static WaveSyncDb>()
}

// Internal statics used to pass data from `launch()` into the Dioxus component tree.
static DB_REF: std::sync::OnceLock<&'static WaveSyncDb> = std::sync::OnceLock::new();
static USER_APP: std::sync::OnceLock<fn() -> Element> = std::sync::OnceLock::new();

/// Internal root component that provides the DB context then renders the user's app.
#[component]
fn RootWrapper() -> Element {
    let db = *DB_REF.get().expect("DB not initialized");
    use_context_provider(|| db);

    let app = *USER_APP.get().expect("App not initialized");
    app()
}
