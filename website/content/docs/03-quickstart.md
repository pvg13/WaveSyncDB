# Quickstart

Get a sync-aware SeaORM app running in under five minutes.

## 1. Add the dependencies

```toml
[dependencies]
wavesyncdb = { version = "0.5", features = ["derive"] }
sea-orm = { version = "2.0.0-rc", features = ["sqlx-sqlite", "runtime-tokio", "macros"] }
tokio = { version = "1", features = ["full"] }
```

If you're using Dioxus, also enable the `dioxus` feature:

```toml
wavesyncdb = { version = "0.5", features = ["derive", "dioxus"] }
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

That's it. Run two instances of this binary on the same LAN — each one writes to its own SQLite file, but both databases stay in sync via mDNS-discovered peers.

## 4. Add a passphrase (recommended)

By default, any peer on the same topic can read and write. To restrict your sync mesh to a specific group, add a passphrase:

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
    .with_passphrase("super-secret-shared-string")
    .build()
    .await?;
```

The passphrase is hashed into the topic (so peers without it can't even discover yours) and is used as the HMAC key on every message.

## 5. Add WAN sync (optional)

For sync across networks (cellular ↔ home Wi-Fi, etc.), point your peers at a relay server:

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
    .with_passphrase("...")
    .with_relay_server("/ip4/203.0.113.10/udp/4001/quic-v1")
    .build()
    .await?;
```

See [Relay deployment](/docs/relay-deployment) for how to host one.

## Where to go from here

- [Dioxus integration](/docs/dioxus-integration) — reactive hooks for UI apps.
- [Mobile & push notifications](/docs/mobile-and-push) — wake sleeping phones via FCM/APNs.
- [API reference](/docs/api-reference) — the public types.
