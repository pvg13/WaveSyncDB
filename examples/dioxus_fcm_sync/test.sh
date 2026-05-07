#!/usr/bin/env bash
# End-to-end FCM cold-wake test for the dioxus_fcm_sync example.
#
# Proves that when the Android app is killed (swiped from recents),
# a write made by another peer reaches the killed phone via FCM
# wake-up — the actual contract of the push-sync feature on mobile.
#
# # Sequence
#
#   1. Start a local wavesync_relay configured with FCM credentials.
#   2. Start a writer peer (the tests-e2e test-peer binary) connected
#      to that relay with the same topic + passphrase the example
#      app uses.
#   3. Build & install the example APK on the connected emulator
#      (or device). The build sets WAVESYNC_RELAY_OVERRIDE so the
#      installed app dials our local relay rather than the bundled
#      production address.
#   4. Maestro phase-a: launch app, add sentinel task, killApp.
#   5. Verify the writer peer received the sentinel — the relay path
#      is alive.
#   6. Writer peer adds a task with a unique title. Relay sees the
#      writer's notify, FCM-pushes the killed phone.
#   7. Sleep 90 s so the OS delivers FCM and wakes the engine in
#      background. (Real Android delivery latency on a fresh build
#      is typically <30 s.)
#   8. Maestro phase-b: relaunch app (no clearState), assert the
#      writer's row is in the local DB at first paint.
#
# Pass = phase-b finds the row at first paint.
# Fail = the row isn't there OR appears only after foreground sync
#        kicks in (→ FCM cold-wake isn't working).
#
# # Prerequisites
#
#   - Connected Android device or emulator running a Google Play
#     Services system image:
#         system-images;android-34;google_apis_playstore;x86_64
#     `default` images don't ship Google Play Services and FCM
#     never gets delivered. `adb shell pm list packages | grep gms`
#     should return at least `com.google.android.gms` if your
#     device is suitable.
#
#   - Maestro CLI installed (https://maestro.mobile.dev). Tested
#     against v1.39+.
#
#   - dx (Dioxus CLI) on PATH.
#
#   - FCM credentials JSON for a Firebase project that matches the
#     google-services.json bundled in this example. Set its path
#     via FCM_CREDENTIALS env var, e.g.:
#         export FCM_CREDENTIALS=/path/to/firebase-adminsdk.json
#
# # Usage
#
#     ./test.sh                # full FCM cold-wake test
#     ./test.sh --stop         # stop everything started by a prior run
#
# Override knobs:
#     FCM_CREDENTIALS=/abs/path  # required, see above
#     ANDROID_SERIAL=emulator-5554  # adb -s target if multiple devices
#     SKIP_INSTALL=1               # reuse already-installed APK

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
LOGDIR="$HERE/.test-logs"
PIDDIR="$HERE/.test-pids"
mkdir -p "$LOGDIR" "$PIDDIR"

PACKAGE="com.wavesync.mobile_demo"
TOPIC="mobile-tasks-demo"
PASSPHRASE="demo-shared-secret"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-8489}"

# Identity-keypair pinned so the relay's PeerId is stable across runs.
# The actual PeerId derived from this keypair is read from the relay's
# startup log ("Relay server PeerId: …") and substituted into
# WAVESYNC_RELAY_OVERRIDE for the APK build — that way we don't have
# to hand-maintain a peer-id constant that has to match the keypair.
RELAY_KEY='CAESQGlCc264ZKF3D4l/5VXTLjnGdDKxg0cyX2UosIkZmNAbxV5oeISRfEDIrc/+hdQuqepe9CCCc3M5G3DJBs6N6lE='
RELAY_QUIC_PORT="${RELAY_QUIC_PORT:-4001}"
RELAY_TCP_PORT="${RELAY_TCP_PORT:-4002}"

stop_all() {
    for pidfile in "$PIDDIR"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        local name pid
        name="$(basename "$pidfile" .pid)"
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
            echo "stopped $name (pid=$pid)"
        fi
        rm -f "$pidfile"
    done
}
trap stop_all EXIT INT TERM

if [[ "${1:-}" == "--stop" ]]; then
    stop_all
    trap - EXIT
    exit 0
fi

# Idempotent — kill leftovers from a previous run before starting.
stop_all

# ── Prereq checks ──────────────────────────────────────────────────

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "ERROR: $1 not found on PATH. $2" >&2
        exit 2
    }
}
require adb     "Install Android SDK platform-tools."
require dx      "Install Dioxus CLI: cargo install dioxus-cli."
require maestro "Install Maestro: https://maestro.mobile.dev"
require cargo   "Install Rust toolchain."

if [[ -z "${FCM_CREDENTIALS:-}" ]]; then
    echo "ERROR: FCM_CREDENTIALS env var not set." >&2
    echo "       Point it at the Firebase Admin SDK JSON for the project that" >&2
    echo "       matches google-services.json in this directory." >&2
    exit 2
fi
if [[ ! -f "$FCM_CREDENTIALS" ]]; then
    echo "ERROR: FCM_CREDENTIALS=$FCM_CREDENTIALS — file does not exist." >&2
    exit 2
fi

# Detect a connected emulator/device. If ANDROID_SERIAL is set,
# trust it; otherwise pick the first online device.
if [[ -z "${ANDROID_SERIAL:-}" ]]; then
    ANDROID_SERIAL="$(adb devices | awk 'NR>1 && $2=="device" {print $1; exit}')"
fi
if [[ -z "$ANDROID_SERIAL" ]]; then
    echo "ERROR: no online adb devices. Boot an emulator (Play Store image!) or plug a phone." >&2
    exit 2
fi
export ANDROID_SERIAL
echo "Using device: $ANDROID_SERIAL"

# Verify it's a Play-Store image — the test will hang on FCM
# delivery without Google Play Services.
if ! adb -s "$ANDROID_SERIAL" shell pm list packages 2>/dev/null | grep -q '^package:com.google.android.gms$'; then
    echo "WARNING: $ANDROID_SERIAL does not have com.google.android.gms installed." >&2
    echo "         FCM delivery will not work. Recreate the AVD with a" >&2
    echo "         google_apis_playstore system image." >&2
fi

# ── 1. Start the relay ─────────────────────────────────────────────

LAN_IP="$(ip -4 -o addr show scope global 2>/dev/null \
    | awk '{print $4}' | cut -d/ -f1 \
    | grep -E '^192\.168\.' | head -1 || true)"
[[ -z "$LAN_IP" ]] && LAN_IP="$(ip -4 -o addr show scope global 2>/dev/null \
    | awk '{print $4}' | cut -d/ -f1 | head -1 || echo 127.0.0.1)"
echo "LAN IP: $LAN_IP"

# EXTERNAL_ADDRESS bootstrap: the relay needs to know its own peer-id
# to print a multiaddr we can dial. We pass it the identity keypair
# via env, but we don't yet know what peer-id that derives. Start the
# relay with a placeholder external address pointing at the LAN IP —
# the relay logs "Relay server PeerId: …" in the first 1-2s of startup,
# we read it back, and then we have the real RELAY_ADDR to embed in
# the APK build.
# Pre-build the relay and writer-peer release binaries up front so
# the runtime startup timeouts below aren't dominated by `cargo run`
# compile time on a cold target dir (5+ minutes from scratch).
echo "==> Pre-building relay + writer (cold compile can take several minutes)"
(cd "$ROOT" && cargo build --release --quiet -p wavesync_relay)
(cd "$ROOT" && cargo build --release --quiet -p wavesyncdb-e2e --bin test-peer)

echo "==> Starting relay (peer-id will be read from log)"
(
    cd "$ROOT"
    setsid env RUST_LOG=info cargo run --release --quiet -p wavesync_relay -- \
        --identity-keypair="$RELAY_KEY" \
        --listen-addr "/ip4/0.0.0.0/tcp/$RELAY_TCP_PORT" \
        --external-address "/ip4/$LAN_IP/tcp/$RELAY_TCP_PORT" \
        --external-address "/ip4/$LAN_IP/udp/$RELAY_QUIC_PORT/quic-v1" \
        --max-reservations-per-peer 256 \
        --fcm-credentials "$FCM_CREDENTIALS" \
        > "$LOGDIR/relay.log" 2>&1 &
    echo $! > "$PIDDIR/relay.pid"
)

# Wait for the PeerId to appear in the log (or the relay to die).
RELAY_PEER_ID=""
for i in {1..120}; do
    if ! kill -0 "$(cat "$PIDDIR/relay.pid")" 2>/dev/null; then
        echo "ERROR: relay died at startup. Tail of log:" >&2
        tail -50 "$LOGDIR/relay.log" >&2
        exit 1
    fi
    RELAY_PEER_ID="$(grep -oP 'Relay server PeerId: \K\S+' "$LOGDIR/relay.log" | head -1 || true)"
    if [[ -n "$RELAY_PEER_ID" ]]; then
        break
    fi
    sleep 1
done
if [[ -z "$RELAY_PEER_ID" ]]; then
    echo "ERROR: relay did not print 'Relay server PeerId: …' within 120s." >&2
    tail -50 "$LOGDIR/relay.log" >&2
    exit 1
fi
RELAY_ADDR_HOST="/ip4/$LAN_IP/udp/$RELAY_QUIC_PORT/quic-v1/p2p/$RELAY_PEER_ID"
# Android emulators (`emulator-NNNN`) use a NAT'd network where the
# host machine appears as `10.0.2.2` — the LAN IP of the host is NOT
# routable from inside the emulator. For physical devices on the same
# WiFi as the host, the LAN IP is reachable directly.
if [[ "$ANDROID_SERIAL" == emulator-* ]]; then
    APK_RELAY_HOST="10.0.2.2"
    echo "Detected emulator — APK will dial relay at 10.0.2.2 (Android NAT to host)"
else
    APK_RELAY_HOST="$LAN_IP"
    echo "Detected physical device — APK will dial relay at LAN IP $LAN_IP"
fi
APK_RELAY_ADDR="/ip4/$APK_RELAY_HOST/udp/$RELAY_QUIC_PORT/quic-v1/p2p/$RELAY_PEER_ID"
RELAY_ADDR="$RELAY_ADDR_HOST"  # writer peer on the host uses LAN-IP form
echo "relay pid=$(cat "$PIDDIR/relay.pid")  peer-id=$RELAY_PEER_ID"
echo "relay addr (host/writer) = $RELAY_ADDR"
echo "relay addr (apk/emulator) = $APK_RELAY_ADDR"

# ── 2. Start the writer peer (test-peer binary) ────────────────────

echo "==> Starting writer peer on http://127.0.0.1:$WRITER_HTTP_PORT"
WRITER_DB="$(mktemp -d)/writer.db"
(
    cd "$ROOT"
    setsid env BIND_ADDR="0.0.0.0:$WRITER_HTTP_PORT" \
        DB_URL="sqlite:$WRITER_DB?mode=rwc" \
        TOPIC="$TOPIC" \
        PASSPHRASE="$PASSPHRASE" \
        RELAY_ADDR="$RELAY_ADDR" \
        RUST_LOG=info,libp2p_swarm=warn \
        cargo run --release -p wavesyncdb-e2e --bin test-peer \
            > "$LOGDIR/writer.log" 2>&1 &
    echo $! > "$PIDDIR/writer.pid"
)
# Wait for HTTP up. Writer was pre-built above so this is just
# binary startup — but we still allow 60s of slack for slow CI
# machines / cold caches.
for i in {1..60}; do
    if curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/health" >/dev/null 2>&1; then
        echo "writer up after ${i}s"
        break
    fi
    sleep 1
done
if ! curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/health" >/dev/null; then
    echo "ERROR: writer-peer never came up. Tail of log:" >&2
    tail -30 "$LOGDIR/writer.log" >&2
    exit 1
fi

# ── 3. Build & install the example APK ─────────────────────────────

if [[ -z "${SKIP_INSTALL:-}" ]]; then
    echo "==> Building & installing APK with WAVESYNC_RELAY_OVERRIDE=$APK_RELAY_ADDR"
    (
        cd "$HERE"
        WAVESYNC_RELAY_OVERRIDE="$APK_RELAY_ADDR" \
            dx build --platform android --release 2>&1 \
            | tee "$LOGDIR/dx-build.log"
    )
    # dx writes the APK under the workspace target dir (CARGO_TARGET_DIR
    # respected — see ~/.cargo/shared-target). The path is announced at
    # the end of the build as "path=…/example-dioxus-fcm-sync/release/
    # android/app"; the actual APK is the Gradle output a few directories
    # deeper. Even with --release, dx's Android Gradle build emits to the
    # `debug/` output by default.
    DX_OUT_BASE="${CARGO_TARGET_DIR:-$HOME/.cargo/shared-target}/dx/example-dioxus-fcm-sync/release/android/app"
    APK="$(find "$DX_OUT_BASE" -name 'app-debug.apk' -o -name 'app-release*.apk' 2>/dev/null \
        | head -1)"
    if [[ -z "$APK" ]]; then
        echo "ERROR: no APK produced under $DX_OUT_BASE; see $LOGDIR/dx-build.log" >&2
        exit 1
    fi
    echo "APK: $APK"
    adb -s "$ANDROID_SERIAL" install -r -g "$APK" >/dev/null
fi

# ── 4. Maestro phase A — launch, add sentinel, killApp ─────────────

# Per-run unique titles. UUID-suffixing makes the assertions
# idempotent: residual rows from prior runs (the SQLite DB inside
# the app's data dir survives `clearState` in some Maestro/Dioxus
# combos) can't make the test trivially pass with an old row.
RUN_TAG="$(date +%s)-$$"
PHONE_SENTINEL="from-phone-$RUN_TAG"
REMOTE_TITLE="from-cli-$RUN_TAG"

echo "==> Maestro phase A (launch + sentinel + killApp)"
echo "    phone sentinel:  $PHONE_SENTINEL"
maestro --device "$ANDROID_SERIAL" test \
    --env "WAVESYNC_FCM_PHONE_SENTINEL=$PHONE_SENTINEL" \
    "$HERE/test.maestro.phase-a.yaml" \
    | tee "$LOGDIR/maestro-a.log"

# ── 5. Verify the writer received the sentinel (app→relay path alive) ─

echo "==> Waiting for writer to see the sentinel '$PHONE_SENTINEL'..."
# The app generates a UUID for `id` on Add, so we can't lookup by
# the title-as-id. Query the full tasks list and grep by title.
SENTINEL_OK=0
for i in {1..60}; do
    if curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/tasks" 2>/dev/null \
        | grep -q "\"title\":\"$PHONE_SENTINEL\""; then
        SENTINEL_OK=1
        echo "sentinel reached writer after ${i}s"
        break
    fi
    sleep 1
done
if [[ $SENTINEL_OK -eq 0 ]]; then
    echo "ERROR: writer never saw the sentinel — app→relay path is broken," >&2
    echo "       cold-wake test invalid. Tail of writer log:" >&2
    tail -30 "$LOGDIR/writer.log" >&2
    exit 1
fi

# ── 6. Writer adds the under-test task while phone is killed ───────

echo "==> Writer adds task '$REMOTE_TITLE' while phone is killed"
curl -fsS -X POST "http://127.0.0.1:$WRITER_HTTP_PORT/tasks" \
    -H 'content-type: application/json' \
    -d "$(printf '{"id":"%s","title":"%s","completed":false}' "$REMOTE_TITLE" "$REMOTE_TITLE")" \
    > /dev/null

# ── 7. Wait for FCM to wake the killed app and sync ────────────────

WAKE_WAIT="${WAKE_WAIT:-90}"
echo "==> Sleeping ${WAKE_WAIT}s for FCM to deliver and wake the engine in background..."
sleep "$WAKE_WAIT"

# ── 8. Maestro phase B — relaunch and assert the row is present ────

echo "==> Maestro phase B (relaunch + assert)"
maestro --device "$ANDROID_SERIAL" test \
    --env "WAVESYNC_FCM_REMOTE_TASK=$REMOTE_TITLE" \
    --env "WAVESYNC_FCM_PHONE_SENTINEL=$PHONE_SENTINEL" \
    "$HERE/test.maestro.phase-b.yaml" \
    | tee "$LOGDIR/maestro-b.log"

echo
echo "============================================================"
echo "  PASS — FCM cold-wake delivered '$REMOTE_TITLE' to the"
echo "  killed app while the UI was dead."
echo "============================================================"
