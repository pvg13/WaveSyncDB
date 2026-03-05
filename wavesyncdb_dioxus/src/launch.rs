use std::future::Future;
use std::sync::OnceLock;

use dioxus::prelude::*;
use sea_orm::DbErr;
use wavesyncdb::{WaveSyncDb, WaveSyncDbBuilder};

// ---------------------------------------------------------------------------
// Static launch (existing API)
// ---------------------------------------------------------------------------

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

// Internal statics used to pass data from `launch()` into the Dioxus component tree.
static DB_REF: OnceLock<&'static WaveSyncDb> = OnceLock::new();
static USER_APP: OnceLock<fn() -> Element> = OnceLock::new();

/// Internal root component that provides the DB context then renders the user's app.
#[component]
fn RootWrapper() -> Element {
    let db = *DB_REF.get().expect("DB not initialized");
    use_context_provider(|| db);
    // Also provide the dynamic signal so use_db_opt() works in static mode too
    use_context_provider(|| Signal::new(Some(db)));

    let app = *USER_APP.get().expect("App not initialized");
    app()
}

// ---------------------------------------------------------------------------
// Dynamic launch
// ---------------------------------------------------------------------------

static RT_HANDLE: OnceLock<tokio::runtime::Handle> = OnceLock::new();
static DYNAMIC_APP: OnceLock<fn() -> Element> = OnceLock::new();

/// Launch a Dioxus desktop app **without** a pre-built database.
///
/// The app starts immediately with no DB connection. Components should use
/// [`use_db_opt()`] to check whether the DB is ready, and call [`use_init_db()`]
/// to initialize it at runtime (e.g. after the user picks a file).
///
/// # Example
///
/// ```no_run
/// fn main() {
///     wavesyncdb_dioxus::launch_dynamic(App);
/// }
///
/// fn App() -> dioxus::prelude::Element {
///     let db = wavesyncdb_dioxus::use_db_opt();
///     match db() {
///         Some(_db) => todo!("render data UI"),
///         None => todo!("render DB picker UI"),
///     }
/// }
/// ```
pub fn launch_dynamic(app: fn() -> Element) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    RT_HANDLE
        .set(rt.handle().clone())
        .expect("Runtime handle already set");

    let _guard = rt.enter();

    if DYNAMIC_APP.set(app).is_err() {
        panic!("Dynamic app already initialized");
    }

    dioxus::launch(RootWrapperDynamic);
}

/// Internal root component for dynamic mode — provides an empty DB signal.
#[component]
fn RootWrapperDynamic() -> Element {
    use_context_provider::<Signal<Option<&'static WaveSyncDb>>>(|| Signal::new(None));

    let app = *DYNAMIC_APP.get().expect("Dynamic app not initialized");
    app()
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

/// Retrieves the `&'static WaveSyncDb` from Dioxus context.
///
/// Works in both static (`launch`) and dynamic (`launch_dynamic`) modes.
/// In dynamic mode this **panics** if the DB has not been initialized yet —
/// use [`use_db_opt()`] if you need to handle the uninitialized case.
pub fn use_db() -> &'static WaveSyncDb {
    // Static mode: direct &'static ref in context
    if let Some(db) = try_use_context::<&'static WaveSyncDb>() {
        return db;
    }
    // Dynamic mode: read the signal
    let sig = use_context::<Signal<Option<&'static WaveSyncDb>>>();
    sig.read()
        .expect("use_db() called before DB was initialized — use use_db_opt() instead")
}

/// Returns a reactive signal that is `None` until the DB is initialized.
///
/// In **static mode** (via `launch()`), the signal is immediately `Some`.
/// In **dynamic mode** (via `launch_dynamic()`), the signal starts as `None`
/// and becomes `Some` after [`use_init_db()`] completes.
pub fn use_db_opt() -> Signal<Option<&'static WaveSyncDb>> {
    use_context::<Signal<Option<&'static WaveSyncDb>>>()
}

/// Returns a callback that initializes the database at runtime.
///
/// The returned closure accepts a database URL, a libp2p topic, and an async
/// setup function (identical to the parameters of [`launch()`]). It builds the
/// DB, runs the setup closure, and injects the result into context — causing
/// all components that read [`use_db_opt()`] to re-render.
///
/// Calling the returned closure more than once logs a warning and no-ops.
///
/// # Example
///
/// ```ignore
/// let init_db = wavesyncdb_dioxus::use_init_db();
/// spawn(async move {
///     init_db("sqlite:./app.db?mode=rwc", "my-topic", |db| async move {
///         db.get_schema_registry("my_app").sync().await?;
///         Ok(())
///     }).await;
/// });
/// ```
pub fn use_init_db() -> InitDb {
    let sig = use_context::<Signal<Option<&'static WaveSyncDb>>>();
    InitDb { sig }
}

/// Handle returned by [`use_init_db()`].
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
            log::warn!("use_init_db: DB already initialized, ignoring");
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
