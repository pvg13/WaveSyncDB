# Changelog

## Unreleased

### Changed
- **Logging emits `tracing` natively (#69).** `wavesyncdb` and `wavesync_relay` now emit
  `tracing` events directly instead of going through the `log` facade (~500 call sites migrated).
  The `log` bridge is preserved (`tracing = { features = ["log"] }`), so existing `env_logger` /
  `android_logger` consumers keep working with zero changes, and `RUST_LOG` filtering semantics
  are unchanged. Structured spans are new visibility for `tracing` subscribers only — plain `log`
  consumers see the same messages as before, without span context.
  **Breaking:** `recommended_log_filters()` returns a `&'static str` `EnvFilter`/`RUST_LOG`
  directive string instead of `Vec<(&str, log::LevelFilter)>` tuples. Migration: pass it to
  `EnvFilter::new(format!("info,{}", wavesyncdb::recommended_log_filters()))` or
  `env_logger::Env::default().default_filter_or(...)`. See `REQUESTS.md` Breaking Changes.

### Added
- **Structured spans on sync entry points.** `#[tracing::instrument]` spans cover the local
  write path (`handle_local_changeset`), catch-up (`handle_version_vector_request`,
  reconcile handlers), real-time push (`handle_push_request`, `apply_remote_changeset`), and
  `initiate_sync_for_peer` / `drain_and_dispatch`, carrying `peer`, `topic`, `db_version` /
  `my_db_version` / `your_last_db_version`, `n_changes`, and `committed` fields as applicable;
  HMAC-reject events carry a `reason` field. All spans `skip_all` — changesets and DB handles
  are never `Debug`-formatted into span fields. Relay-side spans cover rendezvous, circuit, and
  push-registration paths.
- **Relay Prometheus `/metrics` endpoint.** The relay exposes an OpenMetrics HTTP endpoint
  on `METRICS_ADDR` (default loopback `127.0.0.1:9464`, configurable to `0.0.0.0:9464` for
  remote scrape). Metrics include: circuit reservation time per topic (billing meter for
  relay-mediated sync sessions; per-circuit bytes unavailable from libp2p — global
  per-transport bytes exposed instead), reservation / rendezvous / push notification counters,
  and libp2p bandwidth families (aggregate + per-protocol/direction).
- **Tombstone retention (#41).** Deleted rows' tombstones are now aged out
  after a retention window (default 7 days;
  `WaveSyncDbBuilder::with_tombstone_retention` / `without_tombstone_gc`),
  bounding shadow-table growth, new-peer onboarding payloads, and digest
  work by *current data* instead of all-time deletes. The deleter's
  wall-clock stamp travels on the wire (`ColumnChange::deleted_ts`), so
  every replica ages a tombstone from the same instant: expired tombstones
  vanish from catch-up, digests, RBSR, and conflict resolution
  simultaneously on all peers, keeping reconcile digests equal no matter
  when each peer physically garbage-collects. The setting is persisted in
  the database so background-sync processes follow the same rule; the web
  engine mirrors it (`WebSyncConfig::tombstone_retention_secs`).
  **Documented tradeoff:** a peer offline longer than the retention window
  can resurrect rows deleted in its absence — tune the window to the data.
- **Relay push-wake coalescing (#76).** The relay now suppresses redundant
  per-device wake pushes within a configurable window of a device's last
  wake: a burst of writes to the same topic costs one push, not one per
  write. Platform-aware — `--apns-coalesce-secs`/`APNS_COALESCE_SECS`
  (default `900`, 15 min: APNs allows only a handful of silent pushes per
  device per day) and `--fcm-coalesce-secs`/`FCM_COALESCE_SECS` (default
  `0`, disabled: FCM has no comparable daily cap and the existing
  topic-keyed send debounce already smooths bursts). Composes with, doesn't
  replace, the existing debounce. A suppressed send is not a delivery
  failure — push is a best-effort wake hint and the next catch-up sync
  delivers the data regardless; suppressed sends are logged and counted
  under a new `relay_pushes_sent_total{outcome="coalesced"}` label. Retried
  sends stay exempt (already-committed).
- **iOS QUIC bind A/B toggle (experimental, #73).**
  `WaveSyncDbBuilder::with_ios_unspecified_quic_bind(bool)` (default
  `false`) plus a runtime env override, `WAVESYNC_IOS_UNSPECIFIED_QUIC=1`
  (settable from an Xcode scheme without a rebuild), switch iOS from its
  concrete-interface QUIC bind to the unspecified-address bind every other
  platform already uses, and disable the associated interface-watch tick.
  Exists purely to produce an on-device verdict between the two bind
  strategies — see `docs/ios-device-protocol-2026-07.md`. No-op on
  non-iOS platforms; zero behavior change at the default.
- **`WaveSyncPushHandler.backgroundSyncTimeoutSecs` (iOS, #79).** The
  background-sync deadline passed to the Rust FFI on a push wake is now a
  host-app-tunable `public static var` (default `25`, unchanged) instead of
  a buried literal. Rust's internal `background_sync` timers
  (`fallback_after`, `completion_grace`) now scale to whatever timeout is
  passed in, so a shorter grant still leaves room for the fallback/linger
  windows to complete before the hard deadline — identical values to before
  at the default 25s.
- **iOS deployment templates and docs (#75).**
  `wavesyncdb/src/ios/{Entitlements,Info}.template.plist` — a relay-only
  entitlements/Info.plist starting point (APNs background wake only, no
  local-network/Bonjour/multicast keys, each omission commented with its
  rationale) — plus `website/content/docs/17-ios-deployment.md` (entitlements
  matrix, `.p8` APNs setup against the relay's `APNS_KEY_ID`/`APNS_TEAM_ID`/
  `APNS_BUNDLE_ID`/`APNS_SANDBOX` config, and the coalescing/timeout knobs
  above) and `docs/ios-device-protocol-2026-07.md` (the on-device
  measurement checklist for #73/#74/#77/#79).
- **Alert-class relay pushes + Notification Service Extension support (#78).**
  A changeset that touches a `SyncNotify`-visible table now wakes iOS peers
  with an unbudgeted APNs ALERT-class push instead of the budgeted silent
  one — real-time delivery for user-relevant changes, not throttled by the
  daily silent-push cap. **Zero user data crosses the relay for this:** the
  only new wire field is `NotifyTopic.visible: bool`, a metadata flag
  computed sender-side from the changeset's table names — no row content,
  no app-supplied text. The alert's title comes exclusively from the relay
  operator's `APNS_ALERT_TITLE` env var (default `"Nueva actividad"`), the
  same placeholder for every alert on that deployment; the real
  per-notification text is composed entirely on-device, after the normal
  end-to-end-encrypted sync completes, and never touches the relay. Per-device
  anti-spam guard is `ALERT_COALESCE_SECS` (default 30s, independent of the
  silent class's `APNS_COALESCE_SECS`), and `apns-collapse-id` keeps a burst
  of alerts for one topic to a single Notification Center entry.
  Complementary iOS library support (`wavesyncdb`, iOS targets only):
  an on-disk group-key cache (`.wavesync_group_keys.json`,
  `WaveSyncDbBuilder::with_group_key_cache`, default on) lets a Notification
  Service Extension load an already-derived 32-byte group key instead of
  running the Argon2id KDF, whose ~19 MiB footprint the NSE's ~24 MB memory
  cap can't afford. **This is a structural guarantee, not a convention:**
  when the NSE's sync path sets `key_cache_load_only`, the code branch that
  would call `GroupKey::from_passphrase` is unreachable — a cache miss
  returns `GroupKeyLoadOnlyMiss` instead of falling back to deriving, so the
  KDF cannot run inside the extension under any code path. A new FFI entry
  point, `wavesync_nse_handle_push`, runs a budget-scoped one-shot sync and
  returns the latest captured `SyncNotify` notification's title/body (or
  `None` on timeout / cache miss / nothing notify-worthy), and a
  `WaveSyncNotificationService.swift` template + `wavesync_app_group_container`
  helper let an app share its database with the extension via an iOS App
  Group. The NSE's failure mode is always safe: if it can't finish (killed,
  cold key cache, no App Group), the operator's placeholder banner is shown
  as-is and the data still syncs on the next catch-up — the extension is
  purely a content upgrade, never a sync dependency.

### Changed
- **Sync protocol 4.0.0.** The `deleted_ts` field is covered by message
  HMACs, making 3.0.0 peers incompatible (pre-1.0 hard bump; all peers of
  a group upgrade together, older peers log a protocol mismatch at
  identify time).

### Security
- **Argon2id passphrase derivation (breaking).** The group key is now
  derived exclusively with Argon2id (19 MiB, 2 passes, salted with the user
  topic) instead of a single fast BLAKE3 call. Each offline guess costs
  ~100 ms + 19 MiB instead of nanoseconds, closing the dictionary-attack
  vector against observers of the cleartext derived topic — including the
  relay operator. The salt makes precomputed tables useless across
  deployments. Breaking: `GroupKey::from_passphrase(passphrase, user_topic)`
  gains the salt parameter, and upgraded passphrase groups derive new
  `wavesync2-` topics — all peers of a group must run the same version
  (pre-1.0, no deployed users; older peers are silently ignored, never
  rejected). One-time derivation cost: ~100 ms native, a few seconds in
  the browser.

### Changed
- **Trigger-driven change capture.** Write capture no longer parses SQL text.
  Every registered table gets `AFTER INSERT/UPDATE/DELETE` triggers recording
  row changes into a permanent `_wavesync_changes` table, drained after each
  intercepted statement into the existing shadow bookkeeping (one
  transaction, drained rows purged in the same transaction). What this fixes
  by construction: expression UPDATEs (`SET count = count + 1`) sync the
  computed value instead of the literal expression text; `REPLACE INTO`,
  CTEs, and multi-statement scripts are captured correctly; BLOB columns
  sync (as lowercase hex strings — receivers store TEXT, a documented
  limitation); writes made via `db.inner()` or by a separate process sharing
  the database file (iOS background sync) are captured and drained on the
  next write or startup; a failed bookkeeping pass is retryable instead of
  dropped. Remote applies suppress capture inside the apply transaction so
  applied changesets are never re-broadcast.
- **Unified value encoding.** Push, catch-up, and conflict-tiebreak reads now
  all produce SQLite `json_object()` spelling (booleans as `0`/`1`), closing
  a documented sender/receiver byte asymmetry in tie-breaking. Dioxus model
  reconstruction decodes leniently (`0`/`1` ↔ `bool`), so in-place UI
  updates keep working across the spelling change. Mixed-version meshes
  interoperate; no protocol identifier bump.
- **`register_table` is now `async` and returns `Result`** — it installs the
  capture triggers itself, so manually registered tables (custom delete
  policies) sync correctly. Callers add `.await?`.
- The catch-up path no longer fails outright on tables containing BLOB
  columns ("JSON cannot hold BLOB values").

### Added
- **`use_synced_table_loaded` / `use_synced_row_loaded`.** Loading-aware
  Dioxus hooks that distinguish "still loading" from "loaded and empty/absent"
  — outer `None` only until the initial query resolves, `Some(..)` from then
  on (including on a failed initial query, which still counts as loaded).
  For one-shot hydration latches that must not fire on a mid-load empty read.
- **`WaveSyncDbBuilder::with_change_channel_capacity`.** Configures the
  per-group `ChangeNotification` broadcast channel capacity (default 1024,
  floor 16) for bursty writers whose subscribers need to keep per-row deltas
  instead of falling back to a debounced full-table reload.
- **Per-peer sync health on `PeerInfo`.** Each connected peer's entry gains
  `bytes_in`/`bytes_out` (relay + direct combined), `last_synced_at_ms`,
  `last_converged_at_ms`, and `sync_rtt_ms`. Timestamps are Unix epoch
  milliseconds sampled at the point a peer's data was applied or its
  reconcile digest matched ours; RTT is the last catch-up (version-vector)
  round trip. Never used as input to conflict resolution — display/health
  only. All fields are `#[serde(default)]`, so older serialized snapshots
  keep deserializing.
- **Client relay-traffic gauge (#84, client half).** `diagnostics::Snapshot`
  gains `relay_bytes_in`/`relay_bytes_out` and
  `direct_bytes_in`/`direct_bytes_out` totals, plus
  `Snapshot::relay_traffic_ratio()` (`None` until any traffic has moved,
  otherwise relay-bytes / total-bytes). Also adds a 14-bucket sync-RTT
  histogram (`sync_rtt_histogram`; bucket upper bounds
  `[1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]` ms plus an
  implicit `+Inf` bucket). Byte counting rides the wire bytes already
  computed at the existing HMAC sign/verify sites — no new serialization,
  no new wire traffic.
- `wavesyncdb::diagnostics` is now `#[cfg(not(target_arch = "wasm32"))]`.
  It had no wasm consumer and was compiled there as dead code; wasm builds
  no longer pull it in.

### Fixed
- **Peer version cursor no longer advances on an uncommitted apply.** A
  remote changeset that rolled back mid-apply (e.g. a transaction error)
  used to still persist the sender's `db_version` cursor, which told future
  catch-ups the range had already been applied — silently losing it forever.
  The cursor is now only recorded once the apply transaction actually
  commits; an uncommitted apply leaves it untouched so the peer re-sends the
  same range next time.
- **Engine-swap zombie/leak in Dioxus hooks.** After `InitDb::reset()` +
  re-init, a mounted `use_synced_table`/`use_synced_row` used to keep the old
  engine's `WaveSyncDb` clone alive in its driver task, pinning the dead
  engine's SQLite file open and silently going stale — no crash, no error,
  just a signal that stopped updating. Effects are now generation-reactive
  (driven by `use_wavesync_generation()`) so a reset cancels the stale driver
  task and respawns it against the new instance. As belt-and-braces, the
  drivers' `RecvError::Closed` arms also re-subscribe once before giving up,
  in case a future refactor weakens the task's hold on `db`.

## v0.8.0 — 03/07/2026

Crate versions: `wavesyncdb` 0.8.0, `wavesyncdb_derive` 0.3.0,
`wavesync_relay` 0.3.0.

Consolidation release: merges the July hardening wave — security audit fixes,
SQL-parsing correctness, fail-closed remote apply, edit-vs-delete convergence,
network hardening, iOS cold-sync groundwork, and protocol-version negotiation.

### Security
- **Constant-time MAC verification.** Message authentication now compares
  digests in constant time; the previous comparison was an early-exit `==`.
- **Relay abuse limits.** `NotifyTopic` now requires the sender to be
  registered on the topic it wakes; push tokens get a per-token daily wake
  budget and a per-peer registration cap; inbound connections are capped.
- **Argon2id key derivation (foundation).** `GroupKey::from_passphrase_v2`
  derives keys via Argon2id salted with the user topic, with a dual v1/v2
  `GroupKeySet` for a future transition window. Not yet wired into the engine
  topic derivation — the wire format is unchanged in this release.
- The on-disk config file is written with mode 600 on Unix, and secret
  environment values are no longer partially logged.
- Trust-model limits (replay tolerance via CRDT idempotence, self-asserted
  site_id, relay membership visibility) are now documented in `auth.rs`.

### Fixed
- **Quoted / range-operator WHERE parsing.** Primary-key extraction now
  handles quoted values containing spaces and rejects range operators
  (`>=`, `<=`, `<>`, `!=`) instead of misparsing them as equality — both were
  silent-divergence vectors for UPDATE/DELETE.
- **Unicode UPDATE parsing.** Column extraction uses byte-length-preserving
  ASCII uppercasing; non-ASCII SQL no longer risks misaligned slices.
- **Escaped-quote value splitting.** Values ending in SQL-escaped quotes
  (`'it'''`) no longer desynchronize the value splitter.
- **Remote apply is fail-closed.** A shadow-write failure while applying a
  remote changeset rolls back the whole chunk and leaves the peer cursor
  unmoved, so the change is re-requested instead of silently skipped.
  Oversized payload lengths are rejected explicitly (`u32::try_from` guard),
  and a poisoned network-status lock recovers instead of propagating.
- **Edit-vs-delete convergence (N8).** After a concurrent edit-vs-delete
  conflict, both engines now clear a provably-lost tombstone (and native drops
  residue clock entries on resurrection), local re-inserts outrank stale
  tombstones under DeleteWins, and a one-time startup sweep
  (`heal_lost_tombstones`) repairs rows stuck in the pre-fix state. The
  reconcile digest for such pairs now converges instead of churning forever.
- **Stale sync-request recovery.** In-flight catch-up requests are re-issued
  after 10s instead of hanging until reconnect; on connect, catch-up runs for
  all shared groups; pushes only fan out to currently-connected peers (#38).

### Added
- **Sync-protocol version ladder (#85).** The snapshot protocol is advertised
  as a newest-first ladder with per-stream negotiation; peers sharing no rung
  log an explicit protocol-mismatch warning at identify time instead of
  failing silently. See REQUESTS.md for the breaking-change process.
- **iOS background-sync groundwork.** Explicit `UIBackgroundModes` in the
  example Info.plist, malformed-JSON logging in the FFI layer, and a
  production entitlements checklist (`docs/ios-background-sync-checklist.md`).

### Internal
- Clippy/rustfmt alignment with current stable; `resume_trigger` is gated
  behind the `dioxus` feature.
- `test_n4` rewritten for the single-transaction bookkeeping architecture:
  asserts stale-meta version recovery via the shadow MAX and fail-closed
  writes when shadow bookkeeping cannot commit.

## v0.7.1 — 03/06/2026

Crate versions: `wavesyncdb` 0.7.1, `wavesyncdb_derive` 0.3.0,
`wavesync_relay` 0.2.2.

Connectivity hardening and bug fixes. Headline: fixes a circuit-relay storm in
which a client could open a fresh relay circuit to the same peer every ~300 ms
until the relay's per-peer circuit cap was exhausted (`ResourceLimitExceeded`),
stalling sync even for two foreground devices on the same Wi-Fi.

### Fixed
- **Circuit-relay storm.** When a direct path to a peer already exists, the
  relay's repeated re-introduction of that peer's circuit address is no longer
  re-dialed. The DERP-style demotion (#84) closed a redundant relay connection,
  but the re-dial guard only checked `is_connected` — which flickers false the
  instant a connection closes — so each re-introduction re-opened a circuit that
  was immediately closed again, piling up accepted-but-unreleased circuits on
  the relay. The decision is now keyed on a stable "direct path preferred"
  marker that survives the flicker, and circuit addresses are filtered out of
  introduced address sets for directly-reached peers.
- **Out-of-group push redelivery loop (multi-group).** A peer that receives a
  push for a group it isn't a member of now answers with `PushAck` instead of
  dropping the response channel. Dropping it made the sender treat the push as
  un-acked and redeliver it every few seconds forever — a permanent
  battery/bandwidth drain for any two peers with asymmetric group membership.
- **`SyncNotify` dropped notifications for rows with bool columns**; the full
  row is reconstructed so RBSR-delivered changes notify correctly.
- **iOS:** bind QUIC to the device's default-route interface rather than every
  interface, fixing WAN sync from a loopback-bound listener.

### Changed
- **Per-peer dial backoff** (go-libp2p style: `5s + n²`, capped at 5 min, reset
  on any successful connection) on the relay-introduced and `OutboundFailure`
  redial paths; a **deterministic single-closer** for relay demotion; and an
  **anti-thrash dwell** before a relay connection is closed once a direct path
  appears, so a flaky DCUtR upgrade doesn't force an immediate re-reservation.
- **Prefer LAN:** peers discovered on the local network via mDNS suppress
  relay-circuit dials entirely (the LAN path is closer and more reliable).
- App resume / network transition now clears stale dial backoff and LAN-
  preference markers so reconnects aren't throttled or mis-classified.
- **QUIC idle timeout raised 10 s → 30 s** (client + relay) to tolerate
  transient mobile/Wi-Fi blips without tearing the connection down.
- **Android:** hold a foreground multicast lock so mDNS finds LAN peers.
- **Relay:** per-peer caps tuned to generous-but-sane headroom now that the
  client-side churn is fixed — `max_circuits_per_peer` default 32,
  `max_reservations_per_peer` default 16. Effective relay limits are logged at
  startup. **Redeploy the relay with these lower caps only after clients
  carrying the storm fix are rolled out** (an un-patched client hits the cap
  sooner).

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
- **Relay-cost telemetry + DERP demotion (#84).** Connections are classified
  direct vs relayed (`PeerInfo.via_relay`, `relayed_connections_established` /
  `direct_connections_established` counters). Once a direct path to a peer
  comes up (DCUtR hole-punch, or a naturally-formed direct connection), the
  relay-carried connection to that peer is closed so steady-state data leaves
  the paid relay — it reverts to wake/fallback only. New
  `relay_connections_demoted` counter; in-flight requests on a closed relay
  connection self-heal via push redelivery (#81) + reconcile.

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
