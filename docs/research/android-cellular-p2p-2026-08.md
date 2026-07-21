# Android cellular/5G P2P reality — investigation (#108, M1/P1)

> Status: IN PROGRESS. Emulator half runnable from the repo; carrier half
> needs real devices on real cellular. Findings land in this file.

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

### Carrier NAT classification

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

### FCM delivery classes under throttling

_(pending — bounded by the standby-bucket experiments; see issue #108 Q3)_

## Verdicts for M1

_(to fill: which reducers matter on Android; is phone↔phone cellular
direct achievable at all; expected fleet relay bytes profile)_
