# wavesyncdb

Local-first SQLite that syncs itself. A drop-in SeaORM connection wrapper with per-column CRDTs and peer-to-peer sync over libp2p.

## What it does

Replace your `DatabaseConnection` with `WaveSyncDb` and every write replicates to peers automatically. Conflicts resolve deterministically using per-column Lamport clocks — concurrent edits to different columns both survive.

- **Drop-in SeaORM** — implements `ConnectionTrait`, no API changes needed
- **Per-column CRDTs** — concurrent edits to different columns merge; same-column conflicts resolve deterministically
- **P2P over libp2p** — mDNS for LAN, circuit relay + DCUtR for WAN
- **Offline-first** — writes commit locally before any network I/O
- **Mobile push** — silent FCM/APNs wake sleeping phones for background sync
- **Group auth** — shared passphrase derives topic + HMAC signs every message
- **Cross-platform** — desktop, Android, iOS, and browser (wasm32)

## Quick start

```rust
use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
    .with_passphrase("shared-secret")
    .build()
    .await?;

db.get_schema_registry(module_path!().split("::").next().unwrap())
    .sync()
    .await?;

// Standard SeaORM — sync happens transparently
let task = task::ActiveModel {
    id: Set(Uuid::new_v4().to_string()),
    title: Set("Buy milk".into()),
    completed: Set(false),
    ..Default::default()
};
task.insert(&db).await?;
```

## Features

| Feature | Description |
|---|---|
| `derive` | `#[derive(SyncEntity)]` proc macro for entity auto-discovery |
| `dioxus` | Reactive hooks: `use_synced_table`, `use_synced_row` |
| `web` | wasm32 engine with WebSocket transport + IndexedDB storage |
| `push-sync` | Mobile background sync via FCM/APNs |

## Documentation

Full docs, architecture guide, and live demo: [wavesyncdb.com](https://wavesyncdb.com)

## License

Dual-licensed: [AGPL-3.0-or-later](../LICENSE-AGPL) for open-source use, [commercial license](../LICENSE-COMMERCIAL) for proprietary use. Contact pablo13vazquez@gmail.com for commercial licensing.
