# WaveSyncDB

**Transparent peer-to-peer sync for SeaORM applications.**

[![CI](https://img.shields.io/github/actions/workflow/status/pvg13/WaveSyncDB/rust.yml?branch=main&label=CI&logo=github)](https://github.com/pvg13/WaveSyncDB/actions/workflows/rust.yml)
[![License](https://img.shields.io/badge/license-AGPL--3.0%20OR%20Commercial-blue)](LICENSE)
[![Issues](https://img.shields.io/github/issues/pvg13/WaveSyncDB?logo=github)](https://github.com/pvg13/WaveSyncDB/issues)

### Open issues by priority

[![critical](https://img.shields.io/github/issues/pvg13/WaveSyncDB/priority:critical?label=critical&color=b60205)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22priority%3Acritical%22)
[![high](https://img.shields.io/github/issues/pvg13/WaveSyncDB/priority:high?label=high&color=d93f0b)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22priority%3Ahigh%22)
[![medium](https://img.shields.io/github/issues/pvg13/WaveSyncDB/priority:medium?label=medium&color=fbca04)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22priority%3Amedium%22)
[![low](https://img.shields.io/github/issues/pvg13/WaveSyncDB/priority:low?label=low&color=0e8a16)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22priority%3Alow%22)
[![design](https://img.shields.io/github/issues/pvg13/WaveSyncDB/priority:design?label=design&color=5319e7)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22priority%3Adesign%22)

### Open issues by topic

[![sync-protocol](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:sync-protocol?label=sync-protocol&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Async-protocol%22)
[![sql-parsing](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:sql-parsing?label=sql-parsing&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Asql-parsing%22)
[![shadow-tables](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:shadow-tables?label=shadow-tables&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Ashadow-tables%22)
[![p2p-networking](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:p2p-networking?label=p2p-networking&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Ap2p-networking%22)
[![dioxus](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:dioxus?label=dioxus&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Adioxus%22)
[![relay](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:relay?label=relay&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Arelay%22)
[![security](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:security?label=security&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Asecurity%22)
[![performance](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:performance?label=performance&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Aperformance%22)
[![lifecycle](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:lifecycle?label=lifecycle&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Alifecycle%22)
[![portability](https://img.shields.io/github/issues/pvg13/WaveSyncDB/topic:portability?label=portability&color=1d76db)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22topic%3Aportability%22)
[![feature-request](https://img.shields.io/github/issues/pvg13/WaveSyncDB/feature-request?label=feature-request&color=a2eeef)](https://github.com/pvg13/WaveSyncDB/issues?q=is%3Aopen+is%3Aissue+label%3A%22feature-request%22)

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

WaveSyncDB is **dual-licensed**:

- **AGPL-3.0-or-later** — free to use in projects that are themselves AGPL-compatible open-source. AGPL extends copyleft to network use: if you run a modified WaveSyncDB (or its relay) as part of a service, you must offer remote users the source.
- **Commercial license** — for proprietary, closed-source, or SaaS use where AGPL's source-disclosure requirements are not acceptable.

Pick whichever fits your project. See [`LICENSE`](LICENSE) for the umbrella description, [`LICENSE-AGPL`](LICENSE-AGPL) for the open-source terms, and [`LICENSE-COMMERCIAL`](LICENSE-COMMERCIAL) for how to obtain a commercial license.

For commercial licensing inquiries: **pablo13vazquez@gmail.com**.
