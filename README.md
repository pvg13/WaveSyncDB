# WaveSyncDB

**Transparent peer-to-peer sync for SeaORM applications.**

WaveSyncDB wraps your SeaORM `DatabaseConnection` and transparently intercepts write operations, replicating them across peers via libp2p gossipsub. Conflicts are resolved automatically using per-column Lamport clocks (CRDTs) — concurrent edits to different columns on the same row both survive, and all nodes converge deterministically. Your application code stays the same — just swap in `WaveSyncDb` and every insert, update, and delete syncs in the background.

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
        ├──▶ Parse SQL → extract column-value pairs
        │
        ├──▶ Update shadow table (per-column Lamport clocks)
        │
        ├──▶ Dispatch SyncChangeset to P2P engine (mpsc channel)
        │         │
        │         ▼
        │    libp2p gossipsub ──▶ Remote peers
        │
        └──▶ Broadcast ChangeNotification (for reactive UI)
```

When a remote changeset arrives via gossipsub, the engine:

1. Groups column changes by (table, primary key)
2. For each column: applies per-column conflict resolution via `should_apply_column()` — higher `col_version` wins, then value bytes, then `site_id`
3. For deletes: checks `should_apply_delete()` with configurable `DeletePolicy` (`DeleteWins` or `AddWins`)
4. Applies winning columns to the local database and updates shadow tables
5. Broadcasts a `ChangeNotification` so reactive UIs update

## Crate Structure

| Crate | Description |
|-------|-------------|
| **wavesyncdb** | Core library: SeaORM connection wrapper, per-column CRDT sync engine, shadow tables, version vector catch-up |
| **wavesyncdb_derive** | Proc macro: `#[derive(SyncEntity)]` for auto-discovery of entities |
| **wavesync_relay** | Relay + rendezvous server for WAN peer discovery and NAT traversal |

## Dioxus Integration

The `wavesyncdb` crate provides Dioxus reactive hooks behind the `dioxus` feature flag:

```rust
use dioxus::prelude::*;
use wavesyncdb::dioxus::{use_wavesync_init, use_synced_table, use_wavesync_provider};

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

- `use_wavesync_init()` — creates the `WaveSyncDb`, runs setup, provides DB via Dioxus context
- `use_wavesync_provider()` — retrieves the `WaveSyncDb` from context
- `use_synced_table::<E>(db)` — reactive signal of all rows, auto-refreshes on local and remote changes
- `use_synced_row::<E>(db, pk)` — reactive signal for a single row by primary key

## Key Types

| Type | Description |
|------|-------------|
| `WaveSyncDb` | SeaORM `ConnectionTrait` wrapper that intercepts writes and dispatches sync |
| `WaveSyncDbBuilder` | Builder to configure and create a `WaveSyncDb` instance |
| `SchemaBuilder` | Fluent API for registering entities (`.register()`, `.register_local()`, `.sync()`) |
| `TableMeta` | Metadata for a synced table (name, primary key, columns, delete policy) |
| `SyncChangeset` | A set of column-level changes with per-column Lamport clocks and site IDs |
| `ColumnChange` | A single column change: table, pk, column, value, col_version, site_id |
| `ChangeNotification` | Lightweight notification emitted after every local or remote write |
| `DeletePolicy` | Per-table policy: `DeleteWins` (default) or `AddWins` |
| `WriteKind` | Enum: `Insert`, `Update`, `Delete` |

## Current Status

**Stable:**
- SeaORM connection wrapper with transparent write interception
- P2P sync via libp2p gossipsub with mDNS peer discovery
- Per-column CRDT conflict resolution with Lamport clocks
- Shadow tables (`_wavesync_{table}_clock`) for per-column metadata
- Version vector sync for catch-up on peer join/reconnect
- `#[derive(SyncEntity)]` for auto-discovery
- Dioxus reactive hooks (feature-gated)
- PSK-based peer group authentication (passphrase → BLAKE3 topic isolation + HMAC)

**Work in Progress:**
- WAN relay + rendezvous server (`wavesync_relay`)
- NAT traversal (relay/dcutr/autonat behaviours integrated)
- Push notifications for mobile wake-up

## License

GPL-3.0-or-later
