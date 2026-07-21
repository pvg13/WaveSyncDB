# iOS mobile connectivity day — investigation (#109, M1/P1)

> Status: PREPARED + SCRIPTED (2026-07-22), awaiting the hardware sitting.
> **One iPhone + one Mac now covers every phase except the
> phone↔phone-cellular cell** (the Android day proved the one-phone +
> host-writer topology answers the carrier questions). This file is the
> protocol + findings container; base device mechanics live in
> docs/ios-device-protocol-2026-07.md (Sections C–E).

## Session runner (connect the phone once, run one script)

`tests-e2e/ios/run_device_session.sh` (run on the Mac) drives S1→S6 plus
the #92 NSE check through a single USB connection:

- **Env injection without Xcode:** `xcrun devicectl device process launch
  --console --environment-variables …` both toggles the #73 A/B arm
  (`WAVESYNC_IOS_UNSPECIFIED_QUIC`) and streams the app's stdio into
  stamped capture files — no scheme editing, no Console.app.
- **Engine-side `m1-diag` beacon** (new, env-gated `WAVESYNC_M1_DIAG=1`):
  the classification line the Android app logged app-side is now emitted
  by the engine itself every ~30s when the env is set, so ANY consuming
  app (Mediterranea) carries it after a plain rebuild against this rev.
  Field-identical to the Android line — same parsers read both.
- **Host writer** (`test-peer` built on the Mac) is the phone's peer, same
  contract as the Android carrier probe; `RELAY_ADDR` points at the test
  relay.
- The only human actions are the ones iOS physically requires (Control
  Center WiFi flips, HOME, foreground, reading the lock-screen banner) —
  the script prompts and timestamps each one, and computes the latency
  laps from the stamped captures.
- Output: `.session-<ts>/summary.md` pre-filled per issue (#73 A/B
  verdict inputs, #74 trial table, S1 classification beacons, S5 bg_sync
  stage table, #92 banner observation) plus raw per-phase logs.
- `--selftest` (any OS) exercises the log parsers against fixtures.

Prerequisites for the sitting: dev-signed Mediterranea build against a
wavesyncdb rev ≥ the beacon commit (the same rebuild picks up the #92 NSE
template fix); relay with APNs creds for S5/N1 (both phases skippable via
`SKIP_S5=1` / `SKIP_NSE=1` if not ready).

### Mediterranea-official arrangement (the default)

Run the session with the real app through its own build pipeline
(`scripts/ios_sign_install.sh`, `WITH_NSE=1`) — with two isolation rules
that make it safe even on a phone that isn't the tester's:

1. **Group isolation, not app isolation.** The host writer joins whatever
   `TOPIC`/`PASSPHRASE` the runner is given — NEVER the real household's.
   Create a fresh test account/household in the app on the phone for the
   session and hand the runner *that* group. Real code, signing, APNs and
   NSE pipeline; throwaway data plane. The real account stays untouched
   and is switched back to afterwards.
2. **Test relay, not production.** S5 needs `APNS_COALESCE_SECS=0` and
   budget resets — relay-side settings you don't touch on prod. The dev
   build must point at the test relay (a relay-override knob in the dev
   build, Android-demo-style, if it doesn't have one yet); the
   Mediterranea APNs key is per-bundle-id and works from the test relay.

Additionally on a borrowed/household phone: skip the cold-cache
*reinstall* variant of N1 on-device (the Simulator covers that row —
NSEs run there and `xcrun simctl push` drives them); a dev build over an
existing install is an upgrade-install (data kept), and the way back is
a TestFlight/App Store reinstall. End-of-session restore: switch the app
back to the real account, Developer Mode off, un-trust the Mac.

## Objective

One hardware day resolving the open iOS connectivity cluster with device
evidence: #77 (cellular DCUtR / relay-only sessions), #74 (NWPathMonitor
vs 3 s polling), #73 (if_watch hang premise / QUIC bind simplification),
plus M1's carrier-NAT classification and the push-budget reality. Feeds
#92 scheduling if time allows.

## Session plan (ordered so early failures don't block later items)

### S1 — Carrier NAT classification (M1; mirrors Android's protocol)

Primary run (scripted): iPhone WiFi-baseline then cellular-only against
the host writer behind the home NAT — the same phone↔home-machine
topology the Android day proved decisive. Optional second run if a second
iPhone is available: same-carrier cellular↔cellular (the one cell the
script can't cover). Classification signals are the engine-side `m1-diag`
beacon lines (env-gated — the runner injects `WAVESYNC_M1_DIAG=1` at
launch; field-identical to Android's).

Record: carrier, radio (5G/LTE), per-phone verdict.

### S2 — #77: relay-only sessions + cellular DCUtR

With the phones in whatever state S1 found: if circuits persist
(symmetric-like), confirm sync completes relay-only (writes both ways,
convergence). Note any `dcutr` terminal outcomes in logs — remembering
the #110 finding: `attempted` counts only terminal outcomes; zero over a
short window proves nothing about engagement.

### S3 — #73: bind premise A/B

`WAVESYNC_IOS_UNSPECIFIED_QUIC` toggle (both values), per
docs/ios-device-protocol-2026-07.md Section C. Verdict decides #74's
scope and lets the losing bind path be deleted.

### S4 — #74 input: interface-change latency

On the winning bind: WiFi↔cellular flips while foregrounded; measure
re-listen/reconnect latency with the current 3 s polling. This is the
baseline NWPathMonitor would have to beat.

### S5 — Push-budget reality (M1)

Over the day's natural use: count silent pushes sent (relay logs) vs
delivered (device logs), foreground/background/killed. Bounds how much
wake-dependence can be engineered away.

### S6 — Suspension vs live P2P (M1)

Foreground sync session → home button → measure how long the direct QUIC
session survives suspension (peer-side observation: when does the
connection die) and whether resume re-establishes direct or degrades to
circuits.

## Findings

_(per section, pending the hardware day)_

## Verdicts

_(to fill: #73/#74/#77 outcomes; iOS rows in the M1 KPI table; NSE wake
implications for #92)_
