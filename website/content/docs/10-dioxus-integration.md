# Dioxus integration

WaveSyncDB ships first-class Dioxus hooks behind the `dioxus` feature flag. They give you reactive signals that auto-refresh on every local *and* remote write — no manual subscriptions, no wiring code.

## Setup

```toml
[dependencies]
wavesyncdb = { version = "0.5", features = ["derive", "dioxus"] }
dioxus = "0.7"
```

## Provide the database to the app

```rust
use dioxus::prelude::*;
use wavesyncdb::dioxus::{use_wavesync_init, use_wavesync_provider, use_synced_table};

fn main() {
    dioxus::launch(App);
}

fn App() -> Element {
    use_wavesync_init("sqlite:./app.db?mode=rwc", "my-topic", |db| async move {
        db.get_schema_registry(module_path!().split("::").next().unwrap())
            .sync()
            .await?;
        Ok(())
    });

    let db = use_wavesync_provider();
    let tasks = use_synced_table::<task::Entity>(&db);

    rsx! {
        for task in tasks.read().iter() {
            p { "{task.title}" }
        }
    }
}
```

## The hooks

| Hook | What it does |
|------|---|
| `use_wavesync_init(url, topic, setup)` | Builds the `WaveSyncDb`, runs your setup closure, and provides the DB through Dioxus context. Returns `Resource<WaveSyncDb>`. |
| `use_wavesync_provider()` | Retrieves the DB from context. Use it inside any descendant component. |
| `use_synced_table::<E>(db)` | Reactive `Signal<Vec<E::Model>>`. Auto-refreshes on every local and remote write to that table. |
| `use_synced_row::<E>(db, pk)` | Reactive `Signal<Option<E::Model>>`. Useful for detail pages keyed by primary key. |

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
let tasks = use_synced_table::<task::Entity>(&db);
let pending = use_memo(move || {
    tasks.read().iter().filter(|t| !t.completed).cloned().collect::<Vec<_>>()
});
```

The memo recomputes whenever `tasks` changes, which happens whenever any peer in the mesh writes.

## Background sync on mobile

On mobile, the engine is suspended when the app is backgrounded. To keep data fresh while the app is closed, see [Mobile & push notifications](/docs/mobile-and-push).
