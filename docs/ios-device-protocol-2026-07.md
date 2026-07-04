# iOS on-device test protocol — 2026-07

A checklist to run at a kitchen table with two iPhones + a Mac, against
`feat/ios-prep`. Follow the sections in order — later sections assume the
build from Step 0. Fill in the record-sheet tables as you go; don't summarize
from memory afterward.

Gear: 2× iPhone (different iOS versions if you have them — not required),
1× Mac, relay running somewhere reachable from both phones' networks (the
Mac itself is fine for a same-LAN session; a public host is needed to test a
cellular↔cellular pair). Xcode with device run enabled on both phones.

---

## Step 0 — build gates

Before anything else, this is the FIRST compile of tonight's Swift edit
(`WaveSyncPushHandler.backgroundSyncTimeoutSecs`) — it has only been checked
with `cargo build`/`cargo check` on the Rust side so far.

1. Point the consuming app's `Cargo.toml`/`Package.swift` dependency at this
   branch (`feat/ios-prep`).
2. `dx build --platform ios` (or open the Xcode project directly) for the
   consuming app.
3. Archive/run to BOTH physical devices (not Simulator — Simulator doesn't
   reproduce interface binding, the multicast entitlement gate, or
   background-execution grants; see `docs/research/ios-sync-state-of-the-art.md`).
4. Confirm the app launches and the Swift package resolves
   `WaveSyncPushHandler.backgroundSyncTimeoutSecs` with no build errors.

| Check | Device A | Device B |
|---|---|---|
| dx/Xcode build succeeds | ☐ | ☐ |
| App installs and launches | ☐ | ☐ |
| Push permission prompt accepted | ☐ | ☐ |

**Pass:** both devices build, install, and launch cleanly. **If this fails,
stop — nothing below can run.**

---

## A — #73 QUIC bind A/B (baseline vs. `WAVESYNC_IOS_UNSPECIFIED_QUIC`)

Tests whether the concrete-interface QUIC bind (current default,
`engine/mod.rs`, #72's fix) or the unspecified-address bind (the experimental
toggle) behaves better on real hardware.

### A1 — baseline (toggle OFF, the shipped default)

1. Run the app on Device A with no env override.
2. In Xcode's device console (or `Console.app` filtered to your app), grep for:
   ```
   DIAG calling listen_on(QUIC)=
   DIAG listen_on(QUIC) returned in
   ```
3. Confirm a WAN sync completes: write on Device A, confirm the change shows
   up on Device B (or a desktop peer) within ~10s.
4. Confirm relay reconnect: background the app, wait 30s, foreground it,
   confirm `RelayStatusChanged` transitions back to `Listening` (grep
   `"Attempting immediate relay reconnection"` and watch for a follow-up
   `Reservation accepted`-style relay log on the relay side, or just confirm
   sync resumes).

### A2 — toggle ON

1. In Xcode: Product → Scheme → Edit Scheme → Run → Arguments → Environment
   Variables → add `WAVESYNC_IOS_UNSPECIFIED_QUIC` = `1`. No rebuild needed —
   read at engine start.
2. Re-run on Device A. Grep for the same two `DIAG` lines, PLUS confirm the
   experimental-path marker:
   ```
   iOS QUIC bind: unspecified-listen override ACTIVE (experimental #73 toggle; source=env WAVESYNC_IOS_UNSPECIFIED_QUIC)
   ```
3. Repeat the WAN sync + relay reconnect checks from A1.
4. Additionally test Wi-Fi↔cellular handoff (see Section B) under this toggle.

### Record sheet

| Check | A1 baseline | A2 toggle ON |
|---|---|---|
| `DIAG calling listen_on(QUIC)=` seen | ☐ | ☐ |
| `DIAG listen_on(QUIC) returned in` seen, elapsed ms | ___ ms | ___ ms |
| WAN sync completes (≤10s) | ☐ | ☐ |
| Relay reconnect after background/foreground | ☐ | ☐ |
| Wi-Fi↔cellular handoff still syncs (A2 only, see §B) | — | ☐ |
| Notes / anomalies | | |

### Verdict tree

- **Unspecified (A2) works including handoff** → next round deletes the
  concrete-bind machinery (`routable_listen_ips`, the interface-watch tick,
  the `#[cfg(target_os = "ios")]` concrete-bind arm); closes **#73 and #74**.
- **Unspecified hangs or breaks WAN sync** → keep the concrete bind as the
  permanent default; build **#74** (NWPathMonitor-driven re-listen) properly.
- **Unspecified works but handoff is broken/slow** → hybrid discussion
  needed (unspecified bind + some other handoff signal); do not decide
  unilaterally, bring both A and B data back.

---

## B — handoff latency data point (feeds #74)

Run this under section A1's **default** (concrete bind, toggle OFF) —
its result governs whether #74 (NWPathMonitor) is worth building at all
against the *shipped* configuration, independent of A's verdict. Repeat 3 times.

1. Get Device A actively synced (confirm a recent sync in the last 10s).
2. Turn Wi-Fi OFF on Device A (Control Center, not Airplane Mode — you want
   the radio to fail over to cellular, not go fully dark).
3. Start a stopwatch the instant you toggle Wi-Fi off.
4. Watch the console for the first re-listen after the radio change:
   ```
   Network interface <ip> departed; removing QUIC listener
   Network interface <ip> appeared; binding QUIC listener
   ```
5. Stop the first lap at the `appeared; binding QUIC listener` line —
   this is "radio-change → first re-listen".
6. Trigger a write on the peer device, keep watching, stop the second lap at
   the next successful sync (`bg_sync stage=first_peer_synced` if this
   happens to route through background sync, otherwise just observe the
   change arrive in the app's data).

### Record sheet

| Trial | Radio-change → first re-listen | First re-listen → first successful sync | Total |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |

**Pass:** total ≤3–4s and acceptable for your use case → **#74 may close
wontfix-with-rationale** (the existing 3s interface-watch poll already bounds
recovery well enough; NWPathMonitor's event-driven immediacy isn't worth the
platform FFI). **Fail:** total consistently >4s or handoff sometimes never
recovers → #74 stays open, scoped as "replace the 3s poll with
NWPathMonitor".

---

## C — #77 DCUtR matrix (hole-punch behavior, relay fallback)

Tests direct-connection-upgrade (DCUtR) success across NAT topologies, and
confirms relay-only fallback still converges when it fails. Run each pairing
below; a "pair" means Device A and Device B (or Device A and a desktop peer)
connected via the SAME relay, on the network combination named.

For each pairing, after ~45-60s of connectivity (the first reconcile digest exchange runs on
the 30s sync interval — measuring at exactly 30s races it; allow time for the exchange to
complete), read both diagnostics fields on EACH device.
There's no built-in log line for these — add a temporary print in the
consuming app (remove after tonight):

```rust
let snap = db.diagnostics();
let status = db.network_status();
tracing::info!(
    "MEASURE dcutr_attempted={} dcutr_succeeded={} relay_ratio={:?} peers={:?}",
    snap.dcutr_upgrades_attempted,
    snap.dcutr_upgrades_succeeded,
    snap.relay_traffic_ratio(),
    status.connected_peers.iter().map(|p| (p.peer_id.0.clone(), p.via_relay, p.last_converged_at_ms)).collect::<Vec<_>>(),
);
```

Trigger it from a debug button or a one-shot timer — a `println!`/`NSLog` is
fine too if that's faster to wire up tonight.

On the relay's Mac, scrape circuit metrics before/after each pairing:

```
curl -s 127.0.0.1:9464/metrics | grep -E 'relay_circuit_seconds_total|relay_active_circuits|relay_circuits_opened_total|relay_circuits_closed_total'
```

(Family names above are the on-wire OpenMetrics names — the encoder appends
`_total` to every counter automatically; use these exact strings for `grep`.)

### Pairings

| Pairing | dcutr_upgrades_attempted (A→B) | dcutr_upgrades_succeeded | `via_relay` before | `via_relay` after | `relay_traffic_ratio()` | `last_converged_at_ms` both sides set? | relay `relay_circuit_seconds_total` delta |
|---|---|---|---|---|---|---|---|
| Wi-Fi ↔ Wi-Fi (different NATs — e.g. home Wi-Fi vs. phone hotspot) | | | | | | | |
| Cellular ↔ Wi-Fi | | | | | | | |
| Cellular ↔ Cellular | | | | | | | |

### Success criteria

- **Upgrade rate recorded** for every pairing (attempted vs. succeeded,
  whatever the number — a low cellular rate is expected, not a failure; see
  the doc comment on `dcutr_upgrades_succeeded` for typical ranges: ~70% on
  mixed home/office NATs, ~10–30% on cellular).
- **Relay-only sessions MUST converge**: whenever DCUtR does NOT succeed
  (`via_relay` stays `true` on both ends), the pair must still show
  `last_converged_at_ms` set on both devices before the session ends — i.e.
  data flows correctly over the relay even when the direct upgrade never
  happens. A pairing that never converges over relay-only is a **blocking
  failure**, independent of DCUtR's own success rate.

---

## D — #79 grant measurement (informs `backgroundSyncTimeoutSecs` default)

Measures how long iOS actually gives the app to run after a silent push,
so the `backgroundSyncTimeoutSecs` default (currently `25`) can be adjusted
with real data instead of the ~30s assumption in its doc comment.

> **⚠️ Relay config for this section — do this FIRST or trials 2–5 are silently
> eaten by the relay itself:**
>
> - Restart the session relay with **`APNS_COALESCE_SECS=0`** — otherwise the
>   default 15-minute per-device coalescing window suppresses every push after
>   the first one, and step 4 below would misread the silence as "the OS killed
>   the process".
> - Reset Device A's daily wake budget before starting: point the relay at a
>   fresh `--push-db` file (or `DELETE FROM push_budget; DELETE FROM push_wakes;`
>   in the existing one). The hard cap is **5 pushes per device per day** and
>   sections A–C may already have spent some of it.
> - Even with coalescing off, **5 trials equals the entire daily budget** — don't
>   burn trials on setup mistakes.
> - Whenever a trial shows nothing on-device, check the relay before recording
>   an OS kill: the log (or `/metrics` scrape) will show
>   `outcome="coalesced"` or `outcome="budget_denied"` if the relay never sent
>   the push at all. Only a push that was actually **sent** counts as a trial.

1. Fully background the app on Device A (not force-quit — background
   suspend, the state a silent push actually wakes from).
2. From Device B (or any peer), make a write that will trigger
   `notify_relay_topic` → a silent push to Device A.
3. Watch Device A's console for the `bg_sync` stage markers (typical happy path):
   ```
   bg_sync stage=config_loaded elapsed_ms=N
   bg_sync stage=engine_built elapsed_ms=N
   bg_sync stage=registry_ready elapsed_ms=N
   bg_sync stage=groups_rejoined elapsed_ms=N   (only if extra groups are configured)
   bg_sync stage=relay_listening elapsed_ms=N   (relay-only sessions; first time only)
   bg_sync stage=first_peer elapsed_ms=N
   bg_sync stage=first_peer_synced elapsed_ms=N
   bg_sync stage=shutdown_started elapsed_ms=N
   bg_sync stage=done elapsed_ms=N result=…
   ```
   Note: `timeout` and `full_sync_fallback` may appear as alternates if the first-peer
   connection is slow or fails.
4. Record whether `done` is reached before the process is actually killed by
   the OS (if the process dies mid-run, note the LAST stage line seen — that
   is your lower bound on the actual grant).
5. Repeat 3–5 times. Vary conditions if you can (screen off vs. just
   backgrounded; a few minutes since last foreground vs. immediately).

### Record sheet

| Trial | Last stage reached | `elapsed_ms` at last stage | Reached `done`? | Notes (screen state, time since backgrounding) |
|---|---|---|---|---|
| 1 | | | ☐ | |
| 2 | | | ☐ | |
| 3 | | | ☐ | |
| 4 | | | ☐ | |
| 5 | | | ☐ | |

**Decide:** if `done` is reliably reached with elapsed_ms comfortably under
25000 across trials, the current default is fine — no change needed. If the
process is being killed before `done` at, say, ~18–20s consistently, lower
`backgroundSyncTimeoutSecs` to leave more shutdown headroom (the internal
`fallback_after`/`completion_grace` timers scale automatically — see
`background_sync.rs`). If trials show a MUCH longer grant available, raising
the default gives more peers time to land before teardown — weigh against
battery/OS-goodwill cost of running longer.

---

## E — #78 alert-push verification (unbudgeted alert class + NSE)

Tests the WhatsApp-model realtime push: a changeset touching a
`SyncNotify`-visible table now wakes iOS peers with an unbudgeted APNs
ALERT-class push instead of the throttled silent one. E1 needs only a
rebuild against the redeployed relay (no app changes at all — it's the
`content-available` + placeholder-banner path every app already has). E2
needs Mediterranea's App-Group + Notification Service Extension build from
Task 3; run E1 to completion first so a real content-rewrite in E2 has a
known-good placeholder baseline to compare against.

### E0 — relay redeploy + env recap

This is the FIRST deploy of `feat/alert-push` — Sections A–D above ran
against `feat/ios-prep`, which predates the alert class entirely.

1. Redeploy `wavesync_relay` from `feat/alert-push`.
2. Confirm these two new env vars/flags before starting (no relay log line
   announces them at startup — check your compose file / process env
   directly, not a log grep):

   | Env var | CLI flag | Default | Purpose |
   |---|---|---|---|
   | `APNS_ALERT_TITLE` | `--apns-alert-title` | `Nueva actividad` | The ONLY user-facing text on an alert push — relay-operator branding. |
   | `ALERT_COALESCE_SECS` | `--alert-coalesce-secs` | `30` | Per-device wake-coalescing window for the alert class, independent of `APNS_COALESCE_SECS`. |

3. `APNS_COALESCE_SECS`/`FCM_COALESCE_SECS`/`PUSH_DEBOUNCE_SECS` (Section D's
   flags) are unaffected — alert-class sends never consult them. No reset
   needed there.

| Check | Done |
|---|---|
| Relay running from `feat/alert-push` | ☐ |
| `APNS_ALERT_TITLE` confirmed (default or overridden — note which) | ☐ |
| `ALERT_COALESCE_SECS` confirmed (default or overridden — note which) | ☐ |

### E1 — Task-1-only: placeholder banner + background sync (no app changes on iPhone)

Run this against the SAME iPhone build as Sections A–D (just pointed at the
redeployed relay) — before installing anything from Task 3. This is the
entire value Task 1 ships standalone: a rebuild against the new relay,
nothing else.

1. Fully background iPhone (Device A) — same background-not-force-quit state
   as Section D.
2. From an Android peer sharing the group, write to a table with a
   registered `SyncNotify` policy (e.g. add a grocery item) — this is what
   flips `visible: true` sender-side (`handle_local_changeset`, no app code
   change needed to trigger it).
3. Time from the write to the banner appearing on Device A's lock screen.
   Expect **≤ ~3s** on a healthy connection — this rides `apns-priority: 10`
   / `apns-push-type: alert`, not the throttled background class. Record the
   actual time.
4. Confirm the banner text is the operator's placeholder
   (`APNS_ALERT_TITLE`) — Task 1 ships no on-device composition, so it MUST
   be the placeholder, never real content, at this stage.
5. Watch Device A's console for the SAME `bg_sync` stage lines as Section D
   (`config_loaded` → … → `done`) — `content-available: 1` still rides
   alongside the alert payload, so background sync runs on the same push
   regardless of the banner.
6. Grep the relay's log for the new field on this send:
   ```
   kind="alert"
   ```
   alongside the existing `topic=… outcome="ok"` push-sent line — confirms
   this send took the alert branch, not silent.
7. Confirm the daily silent-push budget is untouched:
   `sqlite3 <push-db> "SELECT * FROM push_budget WHERE token LIKE '<device-a-token-prefix>%';"`
   before and after step 2 — alert sends skip `charge_daily_budget` (and its
   refund path) entirely, so the row must be byte-identical.
8. Burst test: make 5 writes in quick succession (well inside the 30s
   `ALERT_COALESCE_SECS` window). Confirm Device A shows exactly **ONE**
   Notification Center entry for the group, not five stacked banners — the
   relay's `check_and_stamp_wake` suppresses repeats within the window, and
   `apns-collapse-id` (topic-derived) makes anything that does land replace
   in place rather than stack.

### Record sheet

| Check | Result |
|---|---|
| Write → banner latency | ___ s |
| Banner text is the placeholder (`APNS_ALERT_TITLE`) | ☐ |
| `bg_sync` stages seen on the same push | ☐ |
| Relay log shows `kind="alert"` for this send | ☐ |
| `push_budget` row unchanged before/after | ☐ |
| Burst of 5 → exactly ONE Notification Center entry | ☐ |
| Notes / anomalies | |

**Pass:** all six rows check out. **If banner latency is high or budget
changed, stop and diagnose before moving to E2** — E2 depends on E1's
placeholder path working correctly as its fallback.

### E2 — after Mediterranea's NSE build (`WITH_NSE=1` + `NSE_PROFILE`)

Precondition: install Task 3's App-Group + NSE build —
`WITH_NSE=1 NSE_PROFILE=/path/to/nse.mobileprovision ./scripts/ios_sign_install.sh`
in Mediterranea — on Device A. Repeat E1's trigger (steps 1–2) against this
build.

1. Background Device A, trigger the same kind of write as E1 step 2.
2. Confirm the banner text is now the REAL `SyncNotify`-composed string
   (e.g. "Ana añadió leche"), not the placeholder — this is the entire point
   of the extension. Record time-to-rewrite: from the write to the banner
   showing the REAL text (this includes the NSE's own sync + compose time on
   top of APNs delivery, budget-capped by
   `WaveSyncNotificationService.budgetSecs`, 20s default — expect longer
   than E1's raw delivery number).
3. **If the placeholder shows instead of real content, the NSE died before
   finishing** — this is the safe fallback (data still syncs via
   `content-available` either way), but triage which failure it was:
   - Device console (Console.app filtered to the device, or Xcode's) —
     search for `jetsam`; a Notification Service Extension terminated for
     out-of-memory confirms the ~24 MB cap was exceeded (most likely
     failure: if the group key wasn't cached yet, `wavesync_nse_handle_push`
     returns `synced: false` cleanly instead of running the KDF, so a cold
     cache does NOT show as a memory kill — check for that first, it's the
     more common case on a fresh install).
   - Search for `[WaveSyncNSE]` lines (`NSLog` in
     `WaveSyncNotificationService.swift`) — "No App Group container" means
     the entitlement/profile setup is wrong and this failed instantly, not
     after a timeout.
4. Migration check — **first launch of the App-Group build only**, not every
   E2 run. Before installing the App-Group build, note a few known values
   already in the app (e.g. an existing item). After first launch of the new
   build:
   - Confirm those values are still visible in the app (data intact — this
     is the fail-safe migration from Task 3; a partial failure rolls itself
     back rather than losing anything).
   - Confirm `.wavesync_migration_done` is present in the group container
     (via debug logging or a temporary debug button reading
     `FileManager.default.containerURL(forSecurityApplicationGroupIdentifier:)`
     — there's no external filesystem access to check this directly).
   - Confirm the legacy (pre-App-Group) app-private directory is now EMPTY
     of the migrated entries — not just "app still works". A `Failed`
     migration outcome deliberately leaves data in the legacy dir and rolls
     back anything that did move, so a non-empty legacy dir with the app
     working normally means migration deferred to next launch (expected,
     not a failure — but note which state you're actually in, and don't
     conflate "app works" with "migration completed").

### Record sheet

| Check | Result |
|---|---|
| Banner shows REAL `SyncNotify` text, not placeholder | ☐ |
| Time-to-rewrite (write → real banner text) | ___ s |
| If placeholder shown: jetsam/OOM kill seen in console? | ☐ / n/a |
| If placeholder shown: cold key cache (no memory kill)? | ☐ / n/a |
| If placeholder shown: "No App Group container" logged? | ☐ / n/a |
| Migration: known pre-existing data intact after first launch | ☐ |
| Migration: `.wavesync_migration_done` present in group container | ☐ |
| Migration: legacy dir empty of migrated entries | ☐ |
| Notes / anomalies | |

**Pass:** real content shown, OR (if placeholder shown) a clearly identified
safe-fallback reason from the triage list above — data still synced either
way. Migration's three rows must all pass on the first App-Group launch;
"legacy dir not yet empty + app still works" is an acceptable *deferred*
state (retries next launch), not a pass for that row.

---

## What to send back

For each section, paste:

- All record-sheet tables above (A–E).
- Any full console log excerpt around an anomaly (not the whole session —
  just ±10 lines around anything unexpected).
- A one-line verdict per section: A → which arm of the tree; B → pass/fail
  vs. the 3-4s bar; C → per-pairing convergence pass/fail; D → the decided
  `backgroundSyncTimeoutSecs` value (keep 25, or a new number); E1 → pass/fail
  on the six-row checklist; E2 → real content vs. placeholder-with-reason,
  plus the three migration rows.
- Anything that broke Step 0 (build/install) even if you worked around it.

---

## Draft issue comments (post after tonight, adjust based on actual verdicts)

### #74 (defer)

> Deferred pending the #73 on-device A/B verdict (see
> `docs/ios-device-protocol-2026-07.md`). If the unspecified-bind arm proves
> out on-device including Wi-Fi↔cellular handoff, this issue closes outright
> — NWPathMonitor becomes moot once there's no concrete bind to re-point. If
> the concrete bind stays, the handoff-latency measurement in Section B of
> the protocol showed radio-change → first-sync in roughly N seconds against
> the existing 3s interface-watch poll; if that's within an acceptable bound
> for the app's use case, closing wontfix-with-rationale is reasonable — the
> poll already bounds recovery well enough that swapping in NWPathMonitor's
> event-driven immediacy isn't worth the added platform FFI surface.

### #78 (superseded — see Section E)

> No longer deferred: a consumer (Mediterranea) asked for exactly this —
> realtime delivery for user-visible changes instead of the throttled silent
> class. Alert-class relay pushes, the on-disk group-key cache, the
> `wavesync_nse_handle_push` FFI, and the `WaveSyncNotificationService.swift`
> template all landed; Section E above is the on-device verification
> protocol for it (E1 needs only a rebuild, E2 needs a consuming app's App
> Group + NSE build). Close once Section E's checklists pass.
