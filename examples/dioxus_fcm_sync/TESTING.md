# FCM cold-wake test — `examples/dioxus_fcm_sync`

End-to-end test that proves the headline contract of the push-sync
feature: **when the Android app is killed (swiped from recents),
a write made by another peer reaches the killed phone via FCM
wake-up**, the engine wakes briefly in the background, syncs, and
shuts back down.

If this test passes locally, you have evidence that:

1. The relay correctly maps `NotifyTopic` → FCM token list →
   `messaging.send()`.
2. The Android app's `FirebaseMessagingService` survives the kill
   and is invoked when FCM arrives.
3. The Rust engine's background-sync entry point reconnects to
   the relay, runs version-vector catch-up, and writes the row
   to local SQLite — all while the UI is dead.

## Files

| File | Purpose |
|---|---|
| `test.sh` | Orchestrator: relay + writer peer + APK build/install + maestro phases |
| `test.maestro.phase-a.yaml` | Setup: launchApp clear, add sentinel, killApp |
| `test.maestro.phase-b.yaml` | Assertion: relaunch (no clear), assert remote row visible at first paint |

## Prerequisites

### Hardware / OS

- A Linux or macOS workstation with `adb` on PATH.
- A connected Android device **OR** an emulator running a Google
  Play Services system image. **`default` images don't work** —
  Google Play Services isn't installed on them, so FCM never
  delivers and `test.sh` will stall at the wait step.

  Recommended AVD setup:

  ```bash
  sdkmanager 'system-images;android-34;google_apis_playstore;x86_64'
  avdmanager create avd \
      -n fcm-test \
      -k 'system-images;android-34;google_apis_playstore;x86_64' \
      -d pixel_7
  emulator -avd fcm-test -no-snapshot-load &
  ```

  Quick check that GMS is on the device:

  ```bash
  adb shell pm list packages | grep com.google.android.gms
  # → package:com.google.android.gms
  ```

### Tools

- `dx` (Dioxus CLI): `cargo install dioxus-cli`
- Maestro: <https://maestro.mobile.dev> (tested against v1.39+)
- Rust toolchain (already required by this repo)

### Firebase project

Two halves must come from the **same Firebase project**:

- `examples/dioxus_fcm_sync/google-services.json` — the client
  config bundled into the APK. Already present in the repo for
  the `wavesync` Firebase project; replace it with your own if
  testing under a different project.
- An Admin SDK service-account JSON for the relay, exported via
  `FCM_CREDENTIALS`. Generate via Firebase Console →
  Project Settings → Service Accounts → "Generate new private
  key". Save outside the repo and never commit it.

If the two halves don't match, the relay's FCM `send()` succeeds
but the message goes to a different project's tokens (or fails
silently), and the test stalls.

## Running

```bash
export FCM_CREDENTIALS=/abs/path/to/firebase-adminsdk.json
./test.sh
```

Optional knobs:

| Var | Default | What |
|---|---|---|
| `ANDROID_SERIAL` | first online adb device | which device to drive |
| `WAKE_WAIT` | `90` (seconds) | how long to wait for FCM delivery + background sync |
| `WRITER_HTTP_PORT` | `8489` | local port for the writer-peer HTTP API |
| `RELAY_QUIC_PORT` | `4001` | UDP port the local relay listens on |
| `SKIP_INSTALL` | (unset) | reuse already-installed APK; speeds iteration |

## Pass / fail interpretation

**Pass** — Phase B's `extendedWaitUntil` for the writer's row
succeeds within 5 s of relaunch. The row was written to SQLite
*before* the UI started, which means FCM woke the engine in the
background and let it complete the sync.

**Fail at Phase B's assertion** — three sub-cases:

1. **No row at all after 5 s** — FCM didn't wake the app. Check:
   - Is the AVD/device on a Play Store image?
     `adb shell pm list packages | grep gms`
   - Are `FCM_CREDENTIALS` and `google-services.json` from the
     same Firebase project?
   - Did the relay's startup log show `FCM credentials loaded`
     (look in `.test-logs/relay.log`)?
   - Was the FCM token registered with the relay during phase A?
     Look for `RegisterToken` in the relay log.
2. **Row appears within ~10 s of relaunch but not at first paint**
   — FCM wake didn't happen, but foreground-resume sync caught
   up. Less catastrophic than (1) but still a failure of the
   cold-wake contract.
3. **Sentinel `from-phone-A` is missing in Phase B** — local DB
   was wiped, probably by an OS-level "uninstall on app crash"
   or by a stale `clearState` somewhere. Re-run.

**Fail at the sentinel-check step (after Phase A)** — the
app→relay path itself is broken, before FCM is even relevant.
Most common cause: the relay's bridge address isn't reachable
from the emulator (check `LAN_IP` detection in `test.sh` and
that the emulator can ping it).

## Known limitations

- Single-test-at-a-time: the topic is hardcoded, so two parallel
  runs would cross-talk via the relay.
- The "writer" peer is the test-peer binary from `tests-e2e/`,
  which has `iproute2`/`iptables` baggage it doesn't need here
  but builds the same way. No functional impact.
- Doze mode delays FCM delivery by minutes when the device has
  been idle a long time. Real CI machines aren't a concern; if
  you're testing on a personal phone left overnight, run a
  short workload first to keep it out of Doze.
- iOS is not yet covered. APNs has a similar wake mechanism but
  Maestro's `killApp` interacts differently with iOS task death,
  and APNs sandbox tokens have shorter TTLs. Filed as a future
  follow-up.

## Why we use `killApp` and not `am force-stop`

`killApp` (Maestro) maps to `adb shell am stop-app` (Android 14+)
or a process kill — the app exits but stays FCM-deliverable, the
shape of "user swiped from recents".

`am force-stop` puts the package in **stopped state**, an
Android security flag that disables FCM delivery until the user
manually relaunches. It's the right tool for testing the
"reinstall / first launch" path but the wrong tool for testing
"app was killed by the OS or user kill action" — exactly the
case our push-sync feature is designed to handle. Don't switch
to `force-stop` even if `killApp` flakes; the test would silently
become useless.
