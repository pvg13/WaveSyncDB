# API reference

The public surface is small on purpose. Most apps need only the builder, the connection, and the schema registry — everything else is internal.

## Core types

| Type | Description |
|---|---|
| `WaveSyncDb` | SeaORM `ConnectionTrait` wrapper that intercepts writes and dispatches sync. |
| `WaveSyncDbBuilder` | Fluent builder for configuring and creating a `WaveSyncDb`. |
| `SchemaBuilder` | Returned by `db.get_schema_registry(crate_name)`. Use `.register::<E>()`, `.register_local::<E>()`, `.sync()`. |
| `TableMeta` | Metadata for a synced table (name, primary key, columns, delete policy). |
| `SyncChangeset` | A set of column-level changes with per-column Lamport clocks and site ids. |
| `ColumnChange` | A single column change: table, primary key, column, value, `col_version`, `site_id`. |
| `ChangeNotification` | Emitted after every committed local or remote write. |
| `DeletePolicy` | Per-table policy: `DeleteWins` (default) or `AddWins`. |
| `WriteKind` | `Insert`, `Update`, `Delete`. |
| `BackgroundSyncResult` | Result of a one-shot mobile background sync: `Synced { peers_synced }`, `TimedOut { peers_synced }`, `NoPeers`. |
| `SyncedModel` | Trait auto-derived by `#[derive(SyncEntity)]`. The Dioxus hooks call its `wavesync_apply_change` / `wavesync_from_changes` methods to update signal data from `ChangeNotification.column_values` without a DB round-trip. |

## Builder configuration

```rust
WaveSyncDbBuilder::new(database_url, topic)
    .with_passphrase("...")                       // BLAKE3-derives topic + HMAC key
    .with_relay_server("/ip4/.../udp/4001/quic-v1")
    .with_rendezvous_server("/ip4/.../udp/4001/quic-v1")
    .with_bootstrap_peer("/ip4/.../udp/4001/quic-v1/p2p/12D3...")
    .with_sync_interval(Duration::from_secs(30))
    .with_ipv6(true)
    .managed_relay(relay_addr, "api-key")        // sets up FCM/APNs push registration
    .build()
    .await?;
```

## Schema registration

```rust
db.get_schema_registry(module_path!().split("::").next().unwrap())
    .sync()                       // auto-discover everything #[derive(SyncEntity)]
    .await?;

// Or register manually:
db.get_schema_registry("my_crate")
    .register::<task::Entity>()
    .register::<note::Entity>()
    .register_local::<settings::Entity>()  // table exists but is NOT replicated
    .sync()
    .await?;
```

## Reactive subscriptions

```rust
let mut events = db.network_event_rx();
while let Ok(event) = events.recv().await {
    match event {
        NetworkEvent::PeerConnected(peer_id) => { /* ... */ }
        NetworkEvent::PeerSynced { peer_id, .. } => { /* ... */ }
        NetworkEvent::EngineStarted => { /* first event you'll see */ }
        _ => {}
    }
}
```

## Dioxus hooks (feature `dioxus`)

| Hook | Returns |
|---|---|
| `use_wavesync_init(url, topic, setup)` | `Resource<WaveSyncDb>` |
| `use_wavesync_provider()` | `WaveSyncDb` from Dioxus context |
| `use_synced_table::<E>(db)` | `Signal<Vec<E::Model>>` |
| `use_synced_row::<E>(db, pk)` | `Signal<Option<E::Model>>` |

## Background sync (feature `push-sync`)

```rust
use wavesyncdb::background_sync::{background_sync, background_sync_with_peers};

let result = background_sync("sqlite:///data/data/com.app/app.db?mode=rwc", Duration::from_secs(30)).await?;
```

`background_sync_with_peers` accepts a slice of peer multiaddrs (typically delivered in the FCM payload) so the engine dials directly instead of waiting for discovery.

## Errors

WaveSyncDB returns SeaORM's `DbErr` for normal database errors and a small set of crate-specific error types for sync setup failures. Standard `?` propagation works as expected.

## Crate features

| Feature | Effect |
|---|---|
| `derive` | Enables `#[derive(SyncEntity)]` from `wavesyncdb_derive`. |
| `dioxus` | Enables the reactive hooks listed above. |
| `push-sync` | Enables `background_sync` + the FFI surface used by mobile push handlers. |
