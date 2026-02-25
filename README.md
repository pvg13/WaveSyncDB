# WaveSyncDB

**Transparent peer-to-peer sync for SeaORM applications.**

WaveSyncDB wraps your SeaORM `DatabaseConnection` and transparently intercepts write operations, replicating them across peers via libp2p gossipsub. Conflicts are resolved automatically using Last-Write-Wins (LWW) with hybrid logical clocks. Your application code stays the same — just swap in `WaveSyncDb` and every insert, update, and delete syncs in the background.

## Quick Start

Define a standard SeaORM entity and derive `SyncEntity` for auto-discovery:

```rust
use sea_orm::entity::prelude::*;
use wavesyncdb_derive::SyncEntity;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
#[sea_orm(table_name = "tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
```

Initialize WaveSyncDB and use it like a normal SeaORM connection:

```rust
use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

#[tokio::main]
async fn main() -> Result<(), DbErr> {
    // Build the connection — starts the P2P engine automatically
    let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
        .build()
        .await?;

    // Auto-discover entities annotated with #[derive(SyncEntity)]
    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await?;

    // Standard SeaORM usage — sync happens transparently
    let task = task::ActiveModel {
        id: Set("1".into()),
        title: Set("Buy milk".into()),
        completed: Set(false),
        ..Default::default()
    };
    task.insert(&db).await?;

    Ok(())
}
```

## Architecture

```
App writes via SeaORM
        │
        ▼
   WaveSyncDb (ConnectionTrait wrapper)
        │
        ├──▶ Execute SQL on local database
        │
        ├──▶ Classify SQL (INSERT/UPDATE/DELETE) + extract table name
        │
        ├──▶ Dispatch SyncOperation to P2P engine (mpsc channel)
        │         │
        │         ▼
        │    libp2p gossipsub ──▶ Remote peers
        │
        └──▶ Broadcast ChangeNotification (for reactive UI)
```

When a remote operation arrives via gossipsub, the engine:

1. Looks up the latest local operation for that row in `_wavesync_log`
2. Applies LWW conflict resolution: compares `(hlc_time, hlc_counter, node_id)`
3. If the remote op wins, executes the SQL and logs it
4. Broadcasts a `ChangeNotification` so reactive UIs update

## Crate Structure

| Crate | Description |
|-------|-------------|
| **wavesyncdb** | Core library: SeaORM connection wrapper, sync protocol, P2P engine, LWW conflict resolution |
| **wavesyncdb_derive** | Proc macro: `#[derive(SyncEntity)]` for auto-discovery of entities |
| **wavesyncdb_dioxus** | Dioxus integration: `launch()`, `use_synced_table`, `use_synced_row` reactive hooks |

## Dioxus Integration

The `wavesyncdb_dioxus` crate provides a turnkey setup for Dioxus desktop apps:

```rust
use dioxus::prelude::*;
use wavesyncdb_dioxus::{launch, use_db, use_synced_table};

fn main() {
    launch("sqlite:./app.db?mode=rwc", "my-topic", |db| async move {
        db.get_schema_registry(module_path!().split("::").next().unwrap())
            .sync()
            .await?;
        Ok(())
    }, App);
}

fn App() -> Element {
    let db = use_db();
    let tasks = use_synced_table::<task::Entity>(db);

    rsx! {
        for task in tasks.read().iter() {
            p { "{task.title}" }
        }
    }
}
```

- `launch()` — creates a tokio runtime, builds `WaveSyncDb`, runs setup, provides DB via Dioxus context
- `use_db()` — retrieves the `&'static WaveSyncDb` from context
- `use_synced_table::<E>(db)` — reactive signal of all rows, auto-refreshes on local and remote changes
- `use_synced_row::<E>(db, pk)` — reactive signal for a single row by primary key

## Key Types

| Type | Description |
|------|-------------|
| `WaveSyncDb` | SeaORM `ConnectionTrait` wrapper that intercepts writes and dispatches sync |
| `WaveSyncDbBuilder` | Builder to configure and create a `WaveSyncDb` instance |
| `SchemaBuilder` | Fluent API for registering entities (`.register()`, `.register_local()`, `.sync()`) |
| `TableMeta` | Metadata for a synced table (name, primary key, columns) |
| `SyncOperation` | A write operation with HLC timestamp, node ID, and serialized SQL |
| `ChangeNotification` | Lightweight notification emitted after every local or remote write |
| `WriteKind` | Enum: `Insert`, `Update`, `Delete` |

## Current Status

**Stable:**
- SeaORM connection wrapper with transparent write interception
- P2P sync via libp2p gossipsub with mDNS peer discovery
- LWW conflict resolution with hybrid logical clocks
- Persistent sync log (`_wavesync_log` table)
- `#[derive(SyncEntity)]` for auto-discovery
- Dioxus reactive hooks

**Work in Progress:**
- Full sync protocol (snapshot + incremental replay)
- NAT traversal (relay/dcutr/autonat behaviours integrated but untested)

## License

GPL-3.0-or-later
