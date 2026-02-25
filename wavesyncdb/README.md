# wavesyncdb

Core library for WaveSyncDB — transparent peer-to-peer sync for SeaORM applications.

## Features

- **Connection wrapper** — `WaveSyncDb` implements SeaORM's `ConnectionTrait`, intercepting writes transparently
- **P2P sync** — libp2p gossipsub with mDNS discovery, QUIC and TCP transports
- **LWW conflict resolution** — hybrid logical clocks with deterministic tiebreakers
- **Persistent sync log** — `_wavesync_log` table tracks all operations for incremental sync
- **Schema builder** — fluent API to register entities for sync or local-only use

## Usage

```rust
use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
    .build()
    .await?;

// Register entities for sync
db.get_schema_registry(module_path!().split("::").next().unwrap())
    .sync()
    .await?;

// Use standard SeaORM operations — sync happens automatically
let task = task::ActiveModel { /* ... */ };
task.insert(&db).await?;
```

See the [root README](../README.md) for full documentation, architecture overview, and Dioxus integration.

## License

GPL-3.0-or-later
