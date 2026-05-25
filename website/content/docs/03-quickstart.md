# Quickstart

Get two peers syncing in under five minutes. By the end of this page you'll have a Rust binary that writes to a local SQLite file and automatically replicates to any other instance on the same network.

## 1. Add the dependencies

```toml
[dependencies]
wavesyncdb = { version = "0.6", features = ["derive"] }
sea-orm = { version = "2.0.0-rc", features = ["sqlx-sqlite", "runtime-tokio", "macros"] }
tokio = { version = "1", features = ["full"] }
```

If you're using Dioxus, also enable the `dioxus` feature:

```toml
wavesyncdb = { version = "0.6", features = ["derive", "dioxus"] }
```

## 2. Define a SeaORM entity

Add `#[derive(SyncEntity)]` so WaveSyncDB can auto-discover it:

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

## 3. Build the connection

```rust
use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

#[tokio::main]
async fn main() -> Result<(), DbErr> {
    let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
        .build()
        .await?;

    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await?;

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

Run two instances of this binary on the same LAN (each pointing at a different SQLite file) and they'll discover each other via mDNS and stay in sync automatically.

## 4. Add a passphrase (recommended)

Without a passphrase, any peer on the same topic can read and write. For any real deployment, add one:

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
    .with_passphrase("super-secret-shared-string")
    .build()
    .await?;
```

The passphrase does two things: it's mixed into the topic hash (so peers without it can't even discover you) and it's used as the HMAC key on every message (so they can't inject data).

## 5. Add WAN sync (optional)

For sync across different networks (e.g., cellular to home Wi-Fi), point peers at a relay server:

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
    .with_passphrase("...")
    .with_relay_server("/ip4/203.0.113.10/udp/4001/quic-v1")
    .build()
    .await?;
```

The relay bridges peers behind NAT. It never sees your data in plaintext — end-to-end encryption is handled by libp2p's Noise transport. See [Relay deployment](/docs/relay-deployment) for how to host one.

## Try it

The fastest way to see sync in action without building anything:

1. Open the [live demo](/demo) — it runs two virtual devices in your browser using the same engine compiled to wasm32.
2. Add a task on one device and watch it appear on the other.
3. Toggle one device offline, make changes on both, then reconnect — conflicts resolve automatically.

## Where to go from here

- [Architecture](/docs/architecture) — understand the write path end to end.
- [Dioxus integration](/docs/dioxus-integration) — reactive hooks for UI apps.
- [Configuration](/docs/configuration) — every builder option explained.
- [Mobile & push notifications](/docs/mobile-and-push) — wake sleeping phones via FCM/APNs.
