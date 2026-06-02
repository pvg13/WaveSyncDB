# Changelog

## Unreleased

## v0.7.0 — 03/06/2026

Crate versions: `wavesyncdb` 0.7.0, `wavesyncdb_derive` 0.3.0,
`wavesync_relay` 0.2.1.

### iOS cold-sync + unified `push-sync` feature

iOS APNs integration now matches Android's zero-setup experience. A Swift
Package (`WaveSyncPush`) bundled via `manganis` installs its AppDelegate
hooks at image-load time through an ObjC `+load` method, so consumer apps
need no AppDelegate wiring. Cold-start pushes (app launched by silent
APNs) are handled by inspecting `UIApplicationLaunchOptionsRemoteNotificationKey`
and re-dispatching into the same handler path as foreground pushes.

#### Added
- `wavesyncdb/src/ios/Sources/WaveSyncPushObjC/` — ObjC target with
  `WaveSyncAppDelegateProxy` (`+load` entry point, selector installation,
  launch-options scan) and `WaveSyncCompletionWrapper` (safe ObjC→Swift
  completion-block bridging).
- `wavesyncdb/src/ios/Sources/WaveSyncPush/WaveSyncTokenStore.swift`,
  `WaveSyncPushBridge.swift`, `WaveSyncPushHandler.swift` — Swift helpers
  that own the APNs token path, parse push payloads, and call Rust's
  `wavesync_background_sync_with_peers` C FFI.
- `wavesyncdb/src/push.rs::notify_ios_token_dir` — Rust-side bridge that
  hands Swift the database directory so the token file lands next to
  the SQLite DB.
- `findDatabaseUrl()` now searches `Application Support/` (Dioxus apps)
  before `Documents/` (legacy / custom path users).
- APNs token file written with `NSFileProtectionCompleteUntilFirstUserAuthentication`
  so background-launched pushes can read it before first unlock after reboot.
- `extract_db_path` tests covering iOS Application-Support URLs, Android
  app-data URLs, relative paths, and malformed inputs.

#### Changed
- **Feature flags:** `android-fcm` and `ios-push` collapse into a single
  `push-sync` feature. `push-sync = ["mobile-ffi", "dep:manganis"]`.
  Platform-specific code remains gated by `#[cfg(target_os = "...")]`.
  Update `wavesyncdb = { features = [..., "push-sync"] }` and drop the
  two old flags.
- `use_auto_push(db)` is now a deprecated no-op — push registration is
  automatic on both platforms. Remove the call at your leisure; it will
  be dropped in the release after next.
- `wavesyncdb/src/dioxus/push/` deleted — its ObjC-runtime injection
  logic lived in a layer that the Swift Package now replaces cleanly.

#### Removed
- `wavesyncdb/src/dioxus/push/ios.rs` — Rust-side ObjC `class_addMethod`
  manipulation.
- `wavesyncdb/src/ios/Sources/WaveSyncPush/WaveSyncTokenWriter.swift` —
  hex-encoding logic absorbed into `WaveSyncPushHandler.writeDeviceToken`.
- `wavesyncdb/src/ios/Integration/` — stray directory outside the Swift
  Package tree.
- `templates/ios/WaveSyncNotificationHandler.swift` — the automatic path
  is the only supported one now; manual wire-up is available via a future
  opt-in feature if users need it.
- Accidentally-committed `.wavesync_config.json` at repo root; added to
  `.gitignore`.

### Sync architecture rework (column-level CRDTs)

Replaced the row-level LWW + HLC + `_wavesync_log` + Merkle-tree + snapshot
architecture (see v0.3.0) with per-column CRDTs, shadow tables, and
version-vector sync.

#### Added
- Per-column Lamport clocks (`col_version`) with deterministic conflict
  resolution (`col_version → value_bytes → site_id`; no wall-clock).
- Shadow tables (`_wavesync_{table}_clock`, `(pk, cid)` PK, `INSERT OR REPLACE`).
- `db_version` monotonic counter + per-peer `_wavesync_peer_versions`; one-round
  version-vector catch-up (`/wavesync/snapshot/3.0.0`) plus real-time push
  fan-out (`/wavesync/push/1.0.0`).
- Cached working peer addresses (`_wavesync_peer_addrs`) pre-dialed at startup.

#### Removed
- HLC (`uhlc`), `_wavesync_log`, Merkle tree, snapshot protocol, and compaction.

### Multi-group sync + entity scope

#### Added
- `WaveSyncNode::join_group(user_topic, passphrase, kind)` / `leave_group` —
  one node serves N groups over one swarm, each backed by its own database.
- `#[wavesync(scope = private | all | groups(...))]` to control which groups an
  entity replicates to.
- Per-(group, peer) topic/HMAC rejection (`rejected_peers` on `GroupState`).

### Sync notifications

#### Added
- `#[derive(SyncNotify)]` + per-table `on_sync` policy and the
  `use_sync_notifications` Dioxus hook; `ChangeSource` distinguishes local vs
  remote writes (notifications fire only for incoming remote changes).

### Browser / WASM target

#### Added
- `web` feature: a wasm32 sync engine (WebSocket transport) with IndexedDB
  persistence, interoperable with native peers over the shared snapshot protocol.

### Background sync (FCM / APNs)

#### Added
- Targeted, multi-group background wake: the push payload's topic selects which
  group(s) to rejoin and catch up; incremental version-vector sync on wake.

### Sync reliability hardening

#### Added
- **Convergence verification + RBSR (#82).** Recursive range-based set
  reconciliation over the shadow tables: peers exchange a value-inclusive
  digest that *proves* whether they hold identical data (the version-vector
  catch-up only compares height, not equality), then reconcile the symmetric
  difference by recursively splitting mismatching key ranges and transferring
  only the differing cells. Additive on the wire — older / web peers that
  can't decode it fall back to the version-vector catch-up. The periodic
  version-vector resend is skipped for peers proven converged.
- **Fast-path push redelivery (#81).** Un-acked real-time pushes are retried
  on a short cadence so a dropped push reaches a still-connected peer in
  seconds instead of waiting for the next reconcile pass. In-memory and
  idempotent; durability is already guaranteed by the shadow tables + RBSR.
  New `pending_pushes_redelivered` diagnostics counter.
- **Relay-cost telemetry (#84).** Connections are classified direct vs
  relayed (`PeerInfo.via_relay`, `relayed_connections_established` /
  `direct_connections_established` counters) toward demoting paid relay paths.

#### Fixed
- **#80** — writes queued at `shutdown()` are now drained and flushed to peers
  before the engine stops, instead of waiting for a peer's next catch-up.
- **#83** — shadow-table write failures fail closed (roll back rather than
  advance `db_version` on a half-written change), and `ChangeNotification`
  fires only after the shadow transaction commits.
- **#85** — incompatible-protocol peers are surfaced
  (`NetworkEvent::PeerProtocolMismatch`) instead of silently never syncing.
- **#86** — per-(group, peer) topic/HMAC rejection is time-boxed with
  exponential backoff (30s→1h) and cleared on a later successful verify,
  instead of being permanent (which needed an engine restart to recover).

### Fixed
- **#72** — iOS QUIC listener bound to loopback made all WAN dials fail with
  `EADDRNOTAVAIL`; now binds to concrete routable interface addresses (via
  `if-addrs`) and re-listens on network change.
- **#71** — multi-group rendezvous discovery reused the default group's
  pagination cookie for every namespace, so secondary-group peers were never
  discovered over WAN; each namespace is now discovered with a fresh cookie.

## v0.3.0 — 25/02/2026

### Architecture rewrite: SeaORM connection wrapper

Replaced the custom `CrudModel`/`SyncedModel` trait system (sqlx-based) with a
transparent SeaORM connection wrapper. Applications now use standard SeaORM
entities and operations — sync happens automatically under the hood.

#### Added
- `WaveSyncDb` — implements SeaORM `ConnectionTrait`, intercepts writes and dispatches sync operations
- `WaveSyncDbBuilder` — one-call setup: `WaveSyncDbBuilder::new(url, topic).build().await`
- `SchemaBuilder` — fluent API for registering entities (`.register()`, `.register_local()`, `.sync()`)
- `#[derive(SyncEntity)]` — proc macro for auto-discovery via `db.get_schema_registry()`
- `TableRegistry` — tracks which tables participate in sync
- LWW conflict resolution with hybrid logical clocks (`uhlc` crate)
- Persistent `_wavesync_log` table for operation history and incremental sync
- Full sync protocol types (`SyncRequest`, `SyncResponse`, `TableSnapshot`) — WIP
- NAT traversal behaviours (relay, dcutr, autonat) integrated into the P2P engine
- Dioxus hooks: `use_synced_table`, `use_synced_row`, `launch()`, `use_db()`
- Comprehensive documentation: crate-level docs, module docs, doc comments on all public types

#### Changed
- Replaced `sqlx` with `sea-orm 2.0.0-rc` (features: `sqlx-sqlite`, `runtime-tokio`, `macros`)
- Replaced custom `CrudModel`/`SyncedModel` traits with standard `DeriveEntityModel`
- P2P engine now receives operations via mpsc channel instead of static TX
- Conflict resolution changed from single-writer-per-object (SWPO) to Last-Write-Wins (LWW)

#### Removed
- `wavesyncdb/src/crud.rs` — old `CrudModel` trait
- `wavesyncdb/src/sync.rs` — old `SyncedModel` trait
- `wavesyncdb/src/instrument.rs` — Diesel instrumentation layer
- `wavesyncdb/src/error.rs` — custom error types (now uses `sea_orm::DbErr`)
- `wavesyncdb_derive/tests/` — old derive macro tests (replaced by integration tests)
- `examples/p2p/src/model.rs` — old model with `CrudModel` derive

---

### 12/02/2026
- Added dioxus signals

### 10/02/2026
- Fixed some derive erros
- Tested p2p example: The data does not correctly sync :(

### 09/02/2026
- Changed the format from instrumenting an existing database connection to implementing basic Crud operations to simplify development and add custom features
- Added the `Crud` trait to implement basic CRUD operations
- Added the `Synced` traid to implement the syncronization features on top of crud
- Changed the SyncEngine to use an static TX channel and the new Crud trait to handle syncronization
- Replaced `diesel` for `sqlx` as the main sql database connection
- Added the `Operation` enum as the main synced message to sync and execute the network operations safetly (I hope to protect better against sql injections)
- Started to sketch the derive trait to easily implement the `Crud` and `Synced` traits
