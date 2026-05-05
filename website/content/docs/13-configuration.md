# Configuration reference

This page lists every method on `WaveSyncDbBuilder` and what it controls. The builder follows the standard fluent pattern: every `with_...` returns the builder by value, so you can chain them.

```rust
WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
    .with_passphrase("...")
    .with_relay_server("/ip4/203.0.113.10/udp/4001/quic-v1")
    .with_sync_interval(Duration::from_secs(30))
    .with_ipv6(true)
    .build()
    .await?
```

## Required

| Method | Description |
|---|---|
| `WaveSyncDbBuilder::new(database_url, topic)` | Database URL (a SeaORM-compatible SQLite URL) and an application topic string. The topic is hashed with the passphrase if one is set; otherwise it's used directly. |

## Authentication

| Method | Default | Notes |
|---|---|---|
| `with_passphrase(s: &str)` | none | Enables HMAC on every message and mixes the passphrase into the topic hash. Required for any real-world deployment on a shared network. See [Authentication & security](/docs/authentication). |

## Identity

| Method | Default | Notes |
|---|---|---|
| `with_node_id(NodeId)` | random 16-byte UUID, persisted to `_wavesync_meta` | Override the node's site id. Almost never needed — the auto-generated id is stable across restarts. |

## WAN: relay & rendezvous

| Method | Default | Notes |
|---|---|---|
| `with_relay_server(addr: &str)` | none | libp2p multiaddr of the circuit-relay server. Required for WAN sync between peers behind NAT. Typical: `/ip4/203.0.113.10/udp/4001/quic-v1/p2p/12D3...` |
| `managed_relay(addr: &str, api_key: &str)` | none | Same as `with_relay_server` plus push-token registration on the relay. Use for mobile apps that should wake on FCM/APNs. |
| `with_rendezvous_server(addr: &str)` | none | libp2p multiaddr of the rendezvous server (often the same machine as the relay). Peers register their topic and discover each other through it. |
| `with_rendezvous_discover_interval(Duration)` | 60 s | How often to query the rendezvous server for new peers. |
| `with_rendezvous_ttl(seconds: u64)` | 7200 | How long to keep a registration alive on the server. The peer auto-renews before expiry. |
| `with_bootstrap_peer(addr: &str)` | none (multi-call) | Hard-coded peer multiaddr to dial at startup. Useful for tests; also surfaced in FCM payloads so a phone can dial directly on wake. |

## Network discovery

| Method | Default | Notes |
|---|---|---|
| `with_ipv6(enabled: bool)` | `false` | Listen on IPv6 in addition to IPv4. |
| `with_mdns_query_interval(Duration)` | 5 s | mDNS broadcast frequency. Lower = faster LAN discovery but more multicast traffic. |
| `with_mdns_ttl(Duration)` | 120 s | How long mDNS-discovered peer addresses stay valid. |
| `with_keep_alive_interval(Duration)` | 30 s | libp2p `ping` interval — keeps idle connections alive through stateful firewalls. |

## Sync timing

| Method | Default | Notes |
|---|---|---|
| `with_sync_interval(Duration)` | 30 s | Periodic catch-up sync interval. Lower = faster catch-up after partition, more network chatter. |
| `with_circuit_max_duration(Duration)` | 60 min | How long to keep a single circuit-relay connection open before forcing a fresh reservation. |

## Push notifications (mobile)

| Method | Default | Notes |
|---|---|---|
| `with_push_token(platform: &str, token: &str)` | none | Pre-set the push token. Usually you don't call this directly — `managed_relay` + the platform-side push handler register the token at runtime. `platform` is `"fcm"` or `"apns"`. |
| `with_google_services(google_services_json: &str)` | none | Inline contents of `google-services.json` for FCM token retrieval on Android. |
| `with_fcm(project_id, app_id, api_key)` | none | Lower-level alternative to `with_google_services` if you want to set the three values explicitly. |

## Database directory

The builder writes a small config file (`wavesync.json`) next to the SQLite database. This config records the parameters needed for `background_sync` to reconstruct the connection on a future invocation — see [Mobile & push notifications](/docs/mobile-and-push). The config path is derived from the `database_url`, so as long as your URL is stable across launches, no extra setup is needed.

## Example: typical mobile app

```rust
let db = WaveSyncDbBuilder::new(
        "sqlite:./app.db?mode=rwc",
        "com.example.myapp",
    )
    .with_passphrase(&user_group_secret)
    .managed_relay(
        "/ip4/203.0.113.10/udp/4001/quic-v1/p2p/12D3KooWRelayPeerIdHere",
        &relay_api_key,
    )
    .with_rendezvous_server(
        "/ip4/203.0.113.10/udp/4001/quic-v1/p2p/12D3KooWRelayPeerIdHere",
    )
    .with_sync_interval(Duration::from_secs(30))
    .with_ipv6(true)
    .build()
    .await?;
```

## Example: pure LAN, no relay

```rust
let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "lan-only-topic")
    .with_passphrase("shared-on-the-fridge-magnet")
    .with_mdns_query_interval(Duration::from_secs(3))
    .build()
    .await?;
```

## Example: headless service (no UI)

```rust
let db = WaveSyncDbBuilder::new("sqlite:/var/lib/myapp/state.db?mode=rwc", "service")
    .with_passphrase(&env::var("WAVESYNC_PASSPHRASE")?)
    .with_relay_server(&env::var("WAVESYNC_RELAY")?)
    .with_keep_alive_interval(Duration::from_secs(60))
    .build()
    .await?;
```

## Inspecting state at runtime

After the builder produces a `WaveSyncDb`, useful inspection methods include:

| Method | Returns |
|---|---|
| `db.node_id()` | this peer's stable site id |
| `db.network_status()` | snapshot of currently-connected peers, NAT state, relay status |
| `db.network_event_rx()` | broadcast receiver of `NetworkEvent`s for reactive UIs |
| `db.is_engine_alive()` | health check |
| `db.request_full_sync()` | force an immediate catch-up round |
| `db.set_peer_identity(app_id)` | label this peer with a human-readable identity (multi-device per-user setups) |

See [API reference](/docs/api-reference) for the full method signatures.
