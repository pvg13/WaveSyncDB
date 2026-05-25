# Installation

WaveSyncDB targets stable Rust 1.85+ (edition 2024) and works on Linux, macOS, Windows, Android, iOS, and browser (wasm32).

## Pick your features

The library is split into [Cargo features](https://doc.rust-lang.org/cargo/reference/features.html) so you only pay for what you use.

| Feature | Pulls in | When to enable |
|---|---|---|
| _(default)_ | core sync engine, libp2p, SeaORM connection wrapper | always (native targets) |
| `derive` | the `#[derive(SyncEntity)]` proc macro | almost always — required for entity auto-discovery |
| `dioxus` | reactive hooks: `use_synced_table`, `use_synced_row`, `use_wavesync_init` | UI apps using Dioxus |
| `web` | wasm32 engine with WebSocket transport + IndexedDB storage | browser apps via Dioxus web |
| `push-sync` | mobile FFI + the `background_sync` entry point | Android/iOS apps that wake on FCM/APNs push |
| `mobile-ffi` | C ABI bindings used by native push handlers | implied by `push-sync`; rarely set directly |

Pick the smallest set that matches your application:

```toml
# A typical desktop app with reactive UI
[dependencies]
wavesyncdb = { version = "0.6", features = ["derive", "dioxus"] }

# A cross-platform desktop + mobile app
[dependencies]
wavesyncdb = { version = "0.6", features = ["derive", "dioxus", "push-sync"] }

# A browser app (wasm32 target)
[dependencies]
wavesyncdb = { version = "0.6", features = ["web", "dioxus"] }

# Headless service (no UI)
[dependencies]
wavesyncdb = { version = "0.6", features = ["derive"] }
```

## Companion crates

| Crate | Purpose | Required? |
|---|---|---|
| [`sea-orm`](https://crates.io/crates/sea-orm) | the ORM you write your entities against | yes |
| [`tokio`](https://crates.io/crates/tokio) | async runtime | yes |
| [`wavesyncdb_derive`](https://crates.io/crates/wavesyncdb_derive) | proc macro (re-exported via `derive` feature) | recommended |
| [`wavesync_relay`](https://crates.io/crates/wavesync_relay) | relay server for WAN sync | only when you need WAN |
| [`uuid`](https://crates.io/crates/uuid) | string primary keys | recommended |

```toml
[dependencies]
sea-orm = { version = "2.0.0-rc", features = ["sqlx-sqlite", "runtime-tokio", "macros"] }
tokio = { version = "1", features = ["full"] }
uuid = { version = "1", features = ["v4"] }
```

## Platform-specific notes

### Linux / macOS / Windows desktop

No system dependencies beyond a C compiler and OpenSSL development headers (which `sqlx` uses transitively). On most distros these are pre-installed; otherwise:

```bash
# Debian / Ubuntu
sudo apt install build-essential libssl-dev pkg-config

# Fedora / RHEL
sudo dnf install gcc openssl-devel pkgconfig

# macOS — Xcode Command Line Tools cover everything
xcode-select --install
```

### Android

Use `dx` (the Dioxus CLI) for cross-compilation. The `examples/dioxus_fcm_sync` sample shows the full setup including FCM service registration:

```bash
cargo install dioxus-cli --locked
cd your-project
dx serve --platform android --device
```

You'll need:

- **Android SDK + NDK** (typically via Android Studio).
- **`google-services.json`** in your project root if you want push notifications.
- API level **24+** (the relay-client behaviours require relatively modern TLS APIs).

### iOS

```bash
dx serve --platform ios --device
```

You'll need:

- **Xcode 15+** with the iOS 16 SDK.
- An **Apple Developer account** (free tier works for sideloading; paid is required for App Store + APNs in production).
- Push entitlements configured in `Dioxus.toml`. See [Mobile & push notifications](/docs/mobile-and-push).

### Browser (wasm32)

Browser WASM is supported via the `web` feature. The engine uses libp2p's WebSocket-websys transport (connecting through a relay server) and IndexedDB for local persistence instead of SQLite.

```toml
[dependencies]
wavesyncdb = { version = "0.6", features = ["web", "dioxus"] }
```

The `web` feature replaces the native libp2p transports (QUIC/UDP) with WebSocket-over-websys and swaps SeaORM/SQLite for an IndexedDB-backed store. The sync protocol is the same — browser peers participate in the same mesh as native peers, connecting through the relay's WebSocket listener.

Limitations compared to native:
- **No mDNS** — browsers can't send multicast. Discovery requires a relay or rendezvous server.
- **No background sync** — when the tab is closed, the engine stops. Use a service worker for keep-alive if needed.
- **IndexedDB storage** — performance characteristics differ from SQLite; fine for typical app workloads.

## Verify

After adding the dependency and building, run the tiniest possible smoke test:

```rust
use wavesyncdb::WaveSyncDbBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = env_logger::try_init();
    let _db = WaveSyncDbBuilder::new("sqlite::memory:", "smoketest")
        .build()
        .await?;
    println!("WaveSyncDB engine started.");
    Ok(())
}
```

You should see libp2p log lines indicating the engine has started and is listening for peers. If you instead see compile errors complaining about missing features, double-check the `features = [...]` list above.

Next: [Quickstart](/docs/quickstart).
