# Dioxus integration

WaveSyncDB ships first-class Dioxus hooks behind the `dioxus` feature flag. They give you reactive signals that auto-refresh on every local *and* remote write — no manual subscriptions, no wiring code.

## Setup

```toml
[dependencies]
wavesyncdb = { version = "0.6", features = ["derive", "dioxus"] }
dioxus = "0.7"
```

## Provide the database to the app

There are two ways to get a `WaveSyncDb` into the component tree.

**Eager** — build the DB at startup, then inject it (simplest):

```rust
use dioxus::prelude::*;
use wavesyncdb::WaveSyncDbBuilder;
use wavesyncdb::dioxus::{SyncHandle, use_wavesync, use_wavesync_provider, use_synced_table};

// `DB` is built once at startup and stashed (e.g. in a `OnceLock`):
//   let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic").build().await?;
//   db.get_schema_registry(module_path!().split("::").next().unwrap()).sync().await?;
// then `dioxus::launch(App)`.

fn App() -> Element {
    // Inject the pre-built DB into context (cheap — WaveSyncDb is Arc-based).
    use_wavesync_provider(DB.get().unwrap().clone());
    rsx! { TaskList {} }
}

#[component]
fn TaskList() -> Element {
    let db = use_wavesync();
    let tasks = use_synced_table::<task::Model>(SyncHandle::new(db));

    rsx! {
        for task in tasks.read().iter() {
            p { "{task.title}" }
        }
    }
}
```

**Lazy** — when the DB is created at runtime (e.g. after the user picks a file). Provide the
lazy context in the root, then build via the `InitDb` handle:

```rust
use wavesyncdb::dioxus::{
    use_wavesync_provider_lazy, use_wavesync_generation, use_wavesync_init, use_wavesync_opt,
};

fn App() -> Element {
    use_wavesync_provider_lazy();
    use_wavesync_generation();
    let db = use_wavesync_opt(); // Signal<Option<WaveSyncDb>>, None until init
    // ... render a picker when None, the app when Some ...
    rsx! {}
}

// inside the picker component, once you know the url/topic:
async fn open(init: wavesyncdb::dioxus::InitDb, url: String) -> Result<(), wavesyncdb::DbErr> {
    init.call(&url, "my-topic", |db| async move {
        db.get_schema_registry(module_path!().split("::").next().unwrap()).sync().await?;
        Ok(())
    }).await
}
```

## The hooks

| Hook | What it does |
|------|---|
| `use_wavesync_provider(db)` | Injects a pre-built `WaveSyncDb` into Dioxus context (eager mode). |
| `use_wavesync_provider_lazy()` + `use_wavesync_generation()` | Provide an empty (lazy) DB context in the root; fill it later via `use_wavesync_init`. |
| `use_wavesync_init() -> InitDb` | Returns a handle; call `init.call(url, topic, setup).await` (or `call_with` for a custom builder) to build and inject the DB. |
| `use_wavesync() -> WaveSyncDb` | Retrieves the DB from context (panics in lazy mode before init — use `use_wavesync_opt`). |
| `use_wavesync_opt() -> Signal<Option<WaveSyncDb>>` | Retrieves the DB reactively; `None` until init completes. |
| `use_synced_table::<E::Model>(SyncHandle::new(db))` | Reactive `Signal<Vec<E::Model>>`. Auto-refreshes on every local and remote write to that table. |
| `use_synced_row::<E::Model>(SyncHandle::new(db), pk)` | Reactive `Signal<Option<E::Model>>`. Useful for detail pages keyed by primary key. |

## What "reactive" means here

WaveSyncDB broadcasts a `ChangeNotification` after every committed write — local or remote. The notification carries the post-write column values, so the hook applies them in place via the auto-derived [`SyncedModel`](/docs/api-reference) impl — **no SeaORM round-trip per notification**. The component re-renders with the new data automatically.

That means a remote peer toggling `completed = true` will refresh the UI on every other peer, immediately, with no glue code on your side and no extra database read on the receiving side.

### Per-hook behaviour

- `use_synced_row(pk)` ignores notifications whose `primary_key` doesn't match. A list view rendering 100 row hooks does **not** wake all of them on every write — only the one whose row actually changed.
- `use_synced_table` walks its in-memory `Vec<E::Model>`, finds the row by primary key, and patches it in place (or appends on Insert / drops on Delete). The whole-table `find().all()` runs only once, on first mount, and again only as a fallback if the broadcast channel reports `Lagged` (the subscriber missed notifications).
- If a notification arrives without a payload — for example, after a raw `execute_unprepared(...)` that bypasses the SeaORM ActiveModel path — the hook falls back to a single-row `find_by_id(pk)` query. Even in that case it never re-fetches the whole table.

## Filtering and projections

The hooks return whole rows as a signal. If you need a filtered or projected view, derive it with a normal Dioxus memo:

```rust
let tasks = use_synced_table::<task::Model>(SyncHandle::new(db));
let pending = use_memo(move || {
    tasks.read().iter().filter(|t| !t.completed).cloned().collect::<Vec<_>>()
});
```

The memo recomputes whenever `tasks` changes, which happens whenever any peer in the mesh writes.

## Background sync on mobile

On mobile, the engine is suspended when the app is backgrounded. To keep data fresh while the app is closed, see [Mobile & push notifications](/docs/mobile-and-push).
