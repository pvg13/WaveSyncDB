# iOS Background / Cold Sync — Verification Checklist & Remaining Work

*Companion to `docs/research/ios-sync-state-of-the-art.md`. Written 2026-07 after a full
review of the wake path. The Rust/relay plumbing (concrete-interface QUIC bind #72,
targeted rejoin, ack-gated token registration, self-wake exclusion) was verified sound in
code; the most likely causes of "background cold sync doesn't work" are configuration and
Apple-side throttling, not sync logic.*

## What this round changed (verifiable in code)

- **Explicit `UIBackgroundModes: [remote-notification]`** in
  `examples/dioxus_fcm_sync/info.plist`. Previously this was requested *only* via
  `Dioxus.toml`'s `[background] remote-notifications = true` and depended entirely on `dx`
  emitting the key. If it isn't emitted, iOS silently never delivers the wake push to a
  backgrounded app — no error, no log. This is the #1 root-cause suspect. dx merges the
  file, so declaring it explicitly guarantees it.
- **Relay APNs budget** (already landed on `fix/security`): a per-token daily silent-push
  cap (`push_budget` ledger) plus a requirement that a `NotifyTopic` sender be a registered
  token holder. Prevents a busy or hostile group from exhausting the ~few/day APNs
  background budget, after which Apple drops wakes for the whole group.
- **Wake-path instrumentation**: the FFI now logs malformed / non-UTF-8 `peer_addrs`
  payloads instead of silently degrading to discovery-only.

## On-device verification (ranked by likelihood — do in order)

1. **Confirm the background mode actually shipped.** In the built `.app`:
   `/usr/libexec/PlistBuddy -c 'Print :UIBackgroundModes' <App>.app/Info.plist`
   → must list `remote-notification`. If absent, silent pushes are never delivered to a
   backgrounded app. (This is the single most likely cause.)
2. **Confirm the APNs budget isn't exhausted.** Instrument the relay's per-device push
   count (the `push_budget` table now records it). If a device hit its daily cap, wakes stop
   silently until the next UTC day — the classic "worked, then stopped" symptom. Coalesce
   harder on the relay if the app writes frequently.
3. **Confirm the app is backgrounded, not terminated.** A silent `content-available` push
   does **not** relaunch a force-quit / terminated app. If the product needs to sync a
   terminated app, it requires a user-visible alert push (tap-to-launch) or a Notification
   Service Extension (#78) — neither is wired today.
4. **Confirm the token was registered before the write that should wake it** (#65 is fixed:
   registration is ack-gated with a 5s reconcile resend — verify the reconcile actually
   runs on a real late-join).
5. **Capture the wake in Console.app** filtered to the app: you should see the swizzled
   `didReceiveRemoteNotification` fire → `wavesync_background_sync_targeted` → a
   `PeerConnected`/`PeerSynced` within the 25s budget. A missing first line means the push
   never arrived (steps 1–3); a missing last line means the sync didn't complete in time
   (step: cellular/DCUtR, or raise coalescing).

## Remaining work (needs on-device / Swift build — not doable in this environment)

These are specified for a device-enabled follow-up; they involve Swift/Network.framework or
`dx`-built targets that `cargo` cannot compile or exercise here, so shipping them blind would
risk uncompilable code.

- **#74 — NWPathMonitor re-listen.** Replace the 3s interface poll with an
  `NWPathMonitor` on a serial queue that feeds address changes over an mpsc channel into the
  engine loop (never touch the swarm from the callback — Rule 2.10). The Rust side already
  exposes `WaveSyncDb::network_transition()`; the missing piece is the Swift binding (mirror
  the structure of `dioxus/lifecycle/ios.rs`). The 3s poll remains the fallback.
- **#79 — configurable background-sync timeout.** The 25s is hardcoded in
  `WaveSyncPushHandler.swift` and passed to the FFI (which already accepts it as a param).
  Make Swift read it from the persisted config, and keep it conservative vs iOS 26's tighter
  background grants; ensure a partial sync still returns a useful `UIBackgroundFetchResult`.
- **#78 — Notification Service Extension.** Only if the product needs visible/mutated
  notifications or a wake path for terminated apps. Share the token file via an App Group;
  respect `serviceExtensionTimeWillExpire()`.
- **Production entitlement flip.** `examples/dioxus_fcm_sync/entitlements.plist` sets
  `aps-environment = development`; production builds must use `production`, and the relay's
  APNs endpoint must match (sandbox vs prod).
- **#77 — DCUtR on cellular.** Validate that symmetric/CGNAT sessions stay relay-carried and
  still sync; confirm relay-only sessions complete within the wake budget.
