# Android cellular/5G P2P reality — investigation (#108, M1/P1)

> Status: COMPLETE (2026-07-21) for one device/carrier (Pixel 10 Pro,
> DIGI ES). All four questions answered; the single open cell is
> carrier↔carrier phone↔phone (needs a second device — expected-fail,
> see Verdicts). Emulator half runnable from the repo.

## Question

How much of Android's sync traffic can avoid the relay on real mobile
networks — and what structurally prevents it? Concretely: do carrier NATs
behave **cone-like** (WaveSyncDB's introduction cross-dials punch them →
relay payload ratio →0) or **CGNAT/symmetric-like** (circuits forever →
ratio →1, making the #107 mailbox dial the cost lever)?

Context from the #110 investigation (docker, wire-verified): introductions
already hole-punch port-restricted cones without DCUtR; symmetric NAT is
1.000-by-physics. The carrier question decides which world mobile fleets
live in.

## Instrumentation

The `dioxus_fcm_sync` example app now logs an **`m1-diag` beacon** every
30 s (logcat-greppable):

```
m1-diag relayed_est=N direct_est=N demoted=N dcutr=S/A relay_bytes=N direct_bytes=N ratio=Some(r) peers=N peers_via_relay=N
```

Interpretation:

| Signal | cone-like carrier | symmetric-like carrier |
|---|---|---|
| `direct_est` for the peer | ≥1 shortly after both apps run | stays 0 (only the relay conn counts if any) |
| `peers_via_relay` | 0 once settled | equals peer count |
| `ratio` | → 0.000 | → 1.000 |
| `dcutr=S/A` | usually 0/0 (introductions punched first) | 0/0 or 0/A (punches can't land) |

## Device protocol (carrier half — needs two phones)

1. Build + install the example app on both phones (`dx build` as usual —
   the beacon is compiled in).
2. Phone A on **cellular only** (WiFi off). Phone B: first same-carrier
   cellular, then home WiFi (two runs — carrier↔carrier and
   carrier↔home-NAT are different questions).
3. Open the app on both, write a few tasks from each side, leave both
   foregrounded ≥2 min.
4. Collect: `adb logcat -d | grep m1-diag | tail -5` per phone (or
   Android Studio logcat for the non-USB phone).
5. Record per run: carrier name, network type (5G/4G, from status bar),
   the last beacon line of each phone.
6. Repeat with the app backgrounded 5 min then foregrounded — does the
   classification change after resume?

Name the carriers in the findings; do not generalize beyond them.

## Emulator half (runnable from repo)

`tests-e2e/android/run_wan_scenarios.sh` — A1–A3 (cold start, WiFi→cell
migration, airplane blip) from the N14 round, plus the new **A4 Doze**
scenario (#108): background → `deviceidle force-idle` → write-during-Doze
observation → recovery TTFS → post-recovery `m1-diag` snapshot.

Note the emulator's NAT is the host's (cone-like, usually punchable) — it
answers the Doze/lifecycle questions, never the carrier-NAT question.

## Findings

### Emulator: Doze / App Standby (A4) — run 2026-07-21, emulator-5556 (SyncDemo AVD)

Full suite green; N14-era scenarios hold (A1 cold start 891 ms, A2
wifi→cellular resume 2 351 ms, A3 airplane resume 224 ms). The new results:

1. **Doze freezes sync completely.** A row written while the app sat in
   forced Doze (`deviceidle force-idle`, 60 s) never arrived during the
   freeze — the expected cached-app network freeze, confirming that Doze'd
   devices are unreachable to live P2P and only a (high-priority) push or
   the user's own foreground can end the gap.
2. **Doze *recovery* is slow: 22 245 ms TTFS** on unforce + foreground —
   an order of magnitude above every other resume scenario (224–2 351 ms).
   Mechanism (matches the N14/bg-engine-reuse analysis): Doze freezes the
   sockets without an interface change, so the plain foreground resume
   keeps the "same network ⇒ connections still valid" anti-churn behavior
   and waits out the reactive 2-strike sync-timeout eviction (~20 s)
   before re-establishing. The killed-app push path already solves exactly
   this with the wall-clock suspension-gap-gated relay reset
   (`EngineCommand::PushWake`); the foreground resume path has no such
   gate. **Filed as #111.**
3. **The m1-diag beacon works end-to-end.** Post-recovery line:
   `relayed_est=1 direct_est=4 demoted=0 dcutr=0/1 relay_bytes=4528
   direct_bytes=31017 ratio=0.127 peers=1 peers_via_relay=0` — mostly
   direct (host NAT is cone-like, as expected on an emulator), with the
   relay blip and one terminal DCUtR outcome from the recovery window.
   The classification signals a real-carrier run needs are all present.

### #111 fix round (2026-07-21, same emulator)

Three-part fix landed (see the #111 commit): Resume consults the
suspension-gap detector; the lifecycle layer converts a >=60s-backgrounded
resume into a NetworkTransition (the gap detector is provably blind to the
frozen-network Doze variant — the engine loop keeps running, log-verified);
and the post-resume retry now re-attempts the relay reconnect (bounded
re-arms), closing the measured ~45s window where the first post-Doze relay
dial died against a still-waking network and nothing retried until the 30s
periodic tick.

Mechanism evidence (all log-verified): the lifecycle gate fires at
foreground after forced Doze; the relay reservation returns ~7s after a
cold forced teardown; best-case doze recovery measured **249 ms**
(vs 22.2s pre-fix). A clean numeric acceptance (the <3s bar) is deferred
to a stable device: across the day the emulator environment degraded until
run-to-run variance dwarfed the signal — consecutive suite runs produced
22.2s / 7.2s / 45s / 249ms / all-timeouts for the same scenario. The
user's #108 device day should capture one A4 run on a real phone as the
acceptance number.

Methodology notes (hard-earned):
- Clear app state (`pm clear`) before measuring: the demo app accumulates
  rows across runs and re-seeds each fresh writer, inflating every TTFS.
- The headless emulators wedge adb entirely after ~1 day of uptime (kill +
  relaunch fixes it; `adb kill-server` alone does not), and a dying qemu
  can linger holding the AVD lock — verify the process is gone before
  relaunching, or the new instance dies with "multiple emulators with the
  same AVD".
- Emulator serial<->AVD mapping is not stable across relaunches (a
  relaunched AVD grabs the lowest free port): verify identity via
  `pm list packages`, never assume 5554/5556.
- The Pixel 9 Pro AVD hosts the Mediterranea test install and sits at 93%
  storage (installs fail with INSTALL_FAILED_INSUFFICIENT_STORAGE) — run
  scenarios on the SyncDemo AVD.

### Carrier NAT classification — CLEAN RUN (VPN off)

> The first run's results (kept below for methodology) were fully
> VPN-confounded: a phone VPN tunnels ALL traffic — including same-LAN
> WiFi — through the VPN exit, inserting an unknown NAT in front of every
> measurement. Always confirm no VPN before classifying.

| date | device | carrier / radio | topology | verdict | beacon |
|---|---|---|---|---|---|
| 2026-07-21 | Pixel 10 Pro | (WiFi baseline, same LAN, no VPN) | phone-WiFi ↔ host | **DIRECT** — ratio 0.000, `direct_est=1 demoted=1` (circuit demoted once direct formed); host ufw not a blocker in practice | `relay_bytes=0 direct_bytes=13778 ratio=0.0 peers_via_relay=0` |
| 2026-07-21 | Pixel 10 Pro | **DIGI ES / LTE**, no VPN | phone-cellular ↔ host behind home NAT | **RELAY-CARRIED** — new circuit for the cellular session, `peers_via_relay=1`, `dcutr 0/0`: the introduction cross-dials do NOT punch DIGI's carrier NAT (CGNAT/symmetric-like world) | `relay_bytes=+11764 ratio(cellular phase)≈1.0` |

**Verdicts:** (1) On this carrier, mobile payload rides the relay — the
#107 mailbox/cost work is the lever, UX is already fine over circuits.
(2) LAN stays direct as designed. (3) NEW finding: a **foregrounded**
WiFi→cellular flip took **36 s** to recover (P1 TTFS) — no trigger fires
when the interface changes while the app is foregrounded (the A2
scenario's fast number rides the background→resume path). Android needs a
ConnectivityManager-driven NetworkTransition — the Android analogue of
iOS's #74 NWPathMonitor item.

### Doze on real hardware — #111 acceptance NOT met (corrected)

**Correction (same day):** the 2 774 ms reading was an artifact — the
user had woken the screen during the Doze window, ending forced idle
early, so recovery pre-warmed before the measured foreground. A
confirmation cycle with the phone untouched and Doze **verified held**
(`dumpsys deviceidle: mState=IDLE` at the end of the window) measured
recovery at **52 526 ms**. The earlier 47 s device number, previously
dismissed as VPN noise, was evidently real.

**Final (third cycle, held Doze verified + user unlocks at wake — the
real user flow): recovery 2 907 ms. ACCEPTANCE MET.** The engine
timeline post-unlock: foreground event (backgrounded_secs=351) →
NetworkTransition fires → relay reconnected +53 ms → reservation
+157 ms → writer introduced and connected +283 ms — ~0.3 s of engine
time; the rest of the 2.9 s is the wake/unlock itself.

The 52.5 s no-unlock cycle stands as a separate, semantically distinct
case: behind a secure keyguard the activity never resumes, so no
lifecycle edge fires and recovery is the reactive path — but no user is
watching a locked phone; sync lands the moment they actually unlock,
which the 2.9 s run measures. Methodology: verify `mState=IDLE` at
window end AND ensure the unlock happens for the foreground
measurement; a screen-on mid-window voids the run, a missing unlock
measures the keyguard case instead.

### Superseded first run (VPN-confounded — kept for methodology)

#### Carrier NAT classification (first run)

| date | device | carrier / radio | topology | verdict | beacon |
|---|---|---|---|---|---|
| 2026-07-21 | Pixel 10 Pro | DIGI ES / LTE (+IWLAN listed) | phone-cellular ↔ host behind home NAT + **active ufw** | **relay-carried** (ratio 1.000, peers_via_relay=1, dcutr 0/1) | `relayed_est=2 direct_est=0 relay_bytes=23893 ratio=1.0` |
| 2026-07-21 | Pixel 10 Pro | (same, WiFi baseline) | phone-WiFi ↔ host **same LAN** | **relay-carried** (ratio 1.000) — a same-LAN direct dial cannot fail at the NAT, so this points at the host's ufw dropping inbound UDP | `relayed_est=2 direct_est=0 ratio=1.0` |

**Interpretation — honest about the confound:** the same-LAN result proves
the host side was undialable (local firewall), so this run cannot isolate
the carrier NAT: direct would have failed on the host side regardless of
what DIGI's NAT allows. What the run DOES establish, as a true field data
point: the common real topology "phone on LTE ↔ typical home machine
(NAT + default-deny firewall)" runs entirely over the relay — and the UX
is nonetheless excellent (**1 310 ms TTFS on LTE over a relay circuit**;
WiFi baseline 1 344 ms). The relay cost question is therefore about server
bytes, not user experience. A clean carrier-NAT isolation needs either the
host's inbound UDP opened for the writer or a second phone
(carrier↔carrier).

### Doze on real hardware (#111 acceptance attempt)

Pixel 10 Pro, forced Doze 90 s on cellular: the freeze held (row did not
land during Doze) and **recovery took 47 069 ms** — despite the #111 fix
demonstrably running: the post-recovery beacon shows `relayed_est` rising
2→4 (the forced teardown + fresh circuits happened) yet `peers=0` even
35 s after recovery. The reset half of #111 works; the residual is the
**peer reintroduction after the forced reconnect** (the N14
edge-triggered introduction one-shots racing the relay reconnect), with
the periodic tick as the eventual rescue. #111's remaining scope is
therefore reintroduction-after-reset, not socket detection.

### FCM delivery classes under throttling (#108 Q3) — MEASURED 2026-07-21

Single-phone sweep, Pixel 10 Pro on home WiFi (the FCM socket rides the
default network; LTE also attached), app backgrounded, battery reported
unplugged (`dumpsys battery unplug` — buckets don't throttle while
charging), screen off. Sender: FCM HTTP v1 API driven from the host with
the relay's service account, so `android.priority` could be set per
message (the relay itself hardcodes `high` — see push_sender.rs). Each
send carries a unique marker echoed by `onMessageReceived` to logcat;
latency is FCM-accept (HTTP 200) → receipt line, 200 ms poll resolution.
Two sends per cell.

**Phase A — App Standby buckets in isolation** (`deviceidle disable`, so
Doze can't interfere):

| bucket | HIGH (ms) | NORMAL (ms) |
|---|---|---|
| active | 203, 1 012 | 405, 1 013 |
| working_set | 1 214, 608 | 1 012, 204 |
| frequent | 1 013, 406 | 608, 1 013 |
| rare | 607, 608 | 811, 1 013 |
| restricted | 1 012, 1 821 | 608, 810 |

**Verdict: buckets alone throttle nothing on this device** — 20/20
delivered sub-2 s, both priorities, even `restricted`. (Caveat: a
~40-push hour where every push triggers visible work is not the regime
Google's high-priority *quota* deprioritization targets — apps whose
high-pri pushes don't lead to engagement may still be demoted over
days; unobservable in a one-day sweep.)

**Phase B — deep Doze** (`deviceidle force-idle deep` before each send,
`rare` bucket):

| priority | result |
|---|---|
| HIGH | 1 215 ms, 1 012 ms — **delivers through deep Doze** (doze-exempt, as documented) |
| NORMAL | both **held >240 s**; both flushed **~2 s after screen-wake ended Doze** — total deferral 10.3 min and 6.3 min from send |

An earlier (doze-confounded) sweep pass reproduced the same flush
mechanic: a deferred NORMAL push landed the instant `deviceidle unforce`
ran, 5.5 min after send. One NORMAL send from that pass was never
observed at all (a ~60 s monitoring gap between passes means "arrived
unobserved" can't be excluded — but "dropped entirely" can't either).

**Bonus finding — natural quick-Doze onset:** with screen off and the
battery reporting discharge, the Pixel entered **deep Doze in under
10 minutes** of stillness (no force-idle involved) — it's what
confounded the first sweep pass. Field translation: a screen-off phone
on battery is effectively *in the Doze column within minutes*, so the
bucket table above is the exception and Phase B is the rule for a
pocketed phone.

Deferred-flush detail worth knowing: consecutive flushed messages
arrived exactly ~25 s apart — serialized behind the app's per-message
25 s background-sync window (`backgroundSyncTargeted` timeout), not by
FCM. A doze-exit burst of N queued normal-priority pushes takes N×25 s
to drain; another reason the relay's coalescing (one wake signal, data
via sync) is the right shape.

**Verdicts for #108 Q3:** (1) the relay's hardcoded
`android.priority: "high"` is load-bearing — it is the only delivery
class that reaches a dozed device, and Doze-within-minutes is the
default state of a phone in a pocket; (2) normal priority defers
minutes-to-unbounded (until wake/maintenance window) and should never
carry the wake signal; (3) the FCM leg itself is fast and reliable
(~0.2–1.8 s across every bucket) — wake latency is dominated by the
engine's post-wake recovery (#111: ~2.9 s), not by push delivery; (4)
bucket-based throttling contributed zero deferral on this device and can
be dropped as a modeled concern for the wake path, with the multi-day
quota caveat above.

### Foregrounded network-flip (#112) — acceptance met

Definitive run (Pixel 10 Pro, DIGI ES, VPN off, screen held awake via
`svc power stayon usb` — a mid-run screen timeout unfocuses the app and
the foreground gate correctly suppresses the transition, which silently
voided one measurement): **foregrounded WiFi→cellular recovery
2 894 ms** (pre-fix: 36 s). The beacon shows fresh circuits immediately
after the flip (`relayed_est 0→3`) — detection → NetworkTransition →
reconnect, as designed. Same run reconfirmed: P0 LAN direct (754 ms,
ratio 0.0), DIGI LTE relay-carried, and the ~48 s keyguard-case doze
recovery (consistent with #111's documented semantics; the unlocked
flow stands at 2.9 s).

### IPv6 availability on the tested carrier (2026-07-21)

No v6 escape hatch on DIGI ES: the handset's APN profile requests
dual-stack (`IPv4/IPv6`, user-verified in settings), but the core grants
the internet APN **IPv4-only behind CGNAT** (`rmnet1`: `10.44.221.82/32`,
no global v6) — while the IMS APN comes up IPv6, proving the core itself
speaks v6 and the restriction is a deliberate provisioning choice on the
consumer data APN. Consequence: on this carrier, v6-to-v6 direct paths
are structurally unavailable regardless of peer support; the relay path
(and #107's cost work) is the whole story. Worth re-checking per carrier
in future runs — a beacon extension logging global-v6 presence on the
data interface would answer it fleet-wide.

### Wake efficiency (#108 Q4) — closed by composition

Whether a push wake ends on a direct path or on circuits is decided
entirely by the topology column above, not by anything wake-specific:
same-LAN wakes demote to direct once formed (P0 baseline, ratio 0.000);
LTE wakes on this carrier stay relay-carried (CGNAT verdict, `dcutr
0/0`, IPv4-only APN — no v6 escape). The wake pipeline itself is fast at
every stage: FCM HIGH delivery 0.2–1.8 s in every standby bucket and
through deep Doze, engine recovery ~2.9 s unlocked (#111) / 2.9 s on a
foregrounded flip (#112), and 1.3 s TTFS over a relay circuit on LTE.
Nothing in the wake path forces circuits where a direct path is
possible — the carrier NAT does. No wake-specific reducer exists to
build; the cost lever for wakes is the same as for steady state (#107).

## Verdicts for M1

1. **Which reducers matter on Android: #107 (mailbox/relay-byte cost),
   and nothing else measured.** On DIGI ES the payload rides the relay
   regardless (CGNAT, IPv4-only APN, introductions don't punch, DCUtR
   never engages) and the UX over circuits is already excellent (1.3 s
   TTFS) — so the mobile relay question is server cost, not user
   experience. AutoRelay (#31) and gossip re-dissemination (#87) address
   costs this data says are second-order.
2. **Phone↔phone cellular direct: expected-fail, one cell unverified.**
   Same-carrier and cross-carrier DCUtR (the carrier↔carrier column)
   needs a second phone; given both endpoints on this carrier sit behind
   IPv4-only CGNAT that defeated every punch attempt against a home
   cone, direct phone↔phone on DIGI is structurally implausible. Recheck
   per carrier when a second device is available (~30 min with
   `run_device_carrier_probe.sh`).
3. **Expected fleet relay-bytes profile:** LAN/cone fleets ~0 payload
   (already measured 0.000) + full mailbox appends; mobile-on-CGNAT
   fleets ~100% payload + mailbox. In both, the mailbox append stream is
   the dominant steady-state byte source — exactly what the #107
   ack-threshold dial removes for overlapping fleets. Post-#107 fleet
   profile: relay bytes ≈ introductions + the genuinely-offline residue.
4. **Push priority is load-bearing** (Q3): deep Doze is the default
   state of a pocketed phone (<10 min onset) and only HIGH-priority FCM
   reaches it; buckets contributed zero deferral. The relay's hardcoded
   high priority stays.
