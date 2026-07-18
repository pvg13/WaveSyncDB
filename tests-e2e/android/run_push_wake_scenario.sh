#!/usr/bin/env bash
# Not-killed push-wake scenario — the on-device counterpart of
# tests-e2e/tests/wan_latency.rs::s5_not_killed_push_wake, and the direct
# device verification that an FCM wake of a backgrounded-but-NOT-killed
# app REUSES the live in-process engine instead of building a second,
# duplicate-identity one.
#
# Sequence:
#   1. relay (with FCM credentials) + writer peer on the host.
#   2. App foregrounded; baseline row proves the app↔relay path and the
#      device's FCM token registration.
#   3. HOME (background, process alive), 75s — past the QUIC idle
#      timeout AND the engine's 60s suspension-gap threshold.
#   4. Writer inserts a row → relay NotifyTopic → FCM data push →
#      WaveSyncService.backgroundSyncTargeted runs IN THE APP PROCESS.
#   5. Assert from logcat: `bg_sync stage=live_engine_found` (the wake
#      found and reused the live engine), NO `stage=engine_built` (the
#      cold path never ran → no duplicate identity), sync completes;
#      the app PID never changes (proves not-killed throughout).
#   6. Foreground the app: the row must already be visible.
#
# Prerequisites: the dioxus_fcm_sync APK must already be installed and
# pointed at this relay (same pinned RELAY_KEY + port 4001 as
# examples/dioxus_fcm_sync/test.sh — run that first, or any build with
# WAVESYNC_RELAY_OVERRIDE=/ip4/10.0.2.2/udp/4001/quic-v1/p2p/<pinned>).
# Device must run a Google Play Services image (FCM delivery).
#
# Usage:
#     FCM_CREDENTIALS=/abs/path ./run_push_wake_scenario.sh
#     ./run_push_wake_scenario.sh --stop

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/common.sh"

ROOT="$(repo_root)"
LOGDIR="$HERE/.test-logs"
PIDDIR="$HERE/.test-pids"
mkdir -p "$LOGDIR" "$PIDDIR"

PACKAGE="com.wavesync.mobile_demo"
TOPIC="mobile-tasks-demo"
PASSPHRASE="demo-shared-secret"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-8489}"
# Must match the APK build from examples/dioxus_fcm_sync/test.sh: same
# pinned key (stable PeerId) and same port, TCP == QUIC (the relay
# derives its QUIC listener from the TCP listen port).
RELAY_QUIC_PORT="${RELAY_QUIC_PORT:-4001}"
RELAY_TCP_PORT="${RELAY_TCP_PORT:-4001}"
RELAY_KEY='CAESQGlCc264ZKF3D4l/5VXTLjnGdDKxg0cyX2UosIkZmNAbxV5oeISRfEDIrc/+hdQuqepe9CCCc3M5G3DJBs6N6lE='

cleanup() { stop_all; }
trap cleanup EXIT INT TERM
if [[ "${1:-}" == "--stop" ]]; then
    cleanup
    trap - EXIT
    exit 0
fi
stop_all 2>/dev/null || true

require adb   "Install Android SDK platform-tools."
require cargo "Install Rust toolchain."
if [[ -z "${FCM_CREDENTIALS:-}" || ! -f "${FCM_CREDENTIALS:-}" ]]; then
    echo "ERROR: FCM_CREDENTIALS must point at the Firebase Admin SDK JSON." >&2
    exit 2
fi
detect_device
echo "Using device: $ANDROID_SERIAL"

adb -s "$ANDROID_SERIAL" shell pm list packages 2>/dev/null \
    | grep -q "^package:$PACKAGE$" || {
    echo "ERROR: $PACKAGE not installed — run examples/dioxus_fcm_sync/test.sh first." >&2
    exit 2
}

# ── relay with FCM credentials (common.sh's start_relay has none) ──
start_relay_fcm() {
    local root lan tgt
    root="$(repo_root)"
    lan="$(lan_ip)"
    tgt="$(cargo_target_dir)"
    echo "==> Pre-building relay + writer"
    (cd "$root" && cargo build --release --quiet -p wavesync_relay)
    (cd "$root" && cargo build --release --quiet -p wavesyncdb-e2e --bin test-peer)
    echo "==> Starting relay (with FCM)"
    # --push-db is what ENABLES the push subsystem — without it the relay
    # answers every RegisterToken/NotifyTopic with "not configured". Fresh
    # file per run so budget/coalescing state doesn't leak across runs.
    rm -f "$LOGDIR/push.db"
    (
        cd "$root"
        setsid env RUST_LOG=info \
            "$tgt/release/wavesync-relay" \
            --identity-keypair="$RELAY_KEY" \
            --listen-addr "/ip4/0.0.0.0/tcp/$RELAY_TCP_PORT" \
            --external-address "/ip4/$lan/tcp/$RELAY_TCP_PORT" \
            --external-address "/ip4/$lan/udp/$RELAY_QUIC_PORT/quic-v1" \
            --max-reservations-per-peer 256 \
            --push-db "$LOGDIR/push.db" \
            --fcm-credentials "$FCM_CREDENTIALS" \
            > "$LOGDIR/relay.log" 2>&1 &
        echo $! > "$PIDDIR/relay.pid"
    )
    RELAY_PEER_ID=""
    local i
    for i in {1..60}; do
        kill -0 "$(cat "$PIDDIR/relay.pid")" 2>/dev/null || {
            echo "ERROR: relay died at startup:" >&2
            tail -30 "$LOGDIR/relay.log" >&2
            exit 1
        }
        RELAY_PEER_ID="$(grep -oP 'Relay server PeerId: \K\S+' "$LOGDIR/relay.log" | head -1 || true)"
        [[ -n "$RELAY_PEER_ID" ]] && break
        sleep 1
    done
    [[ -n "$RELAY_PEER_ID" ]] || {
        echo "ERROR: relay never printed its PeerId" >&2
        exit 1
    }
    RELAY_ADDR="/ip4/$lan/udp/$RELAY_QUIC_PORT/quic-v1/p2p/$RELAY_PEER_ID"
    echo "relay peer-id=$RELAY_PEER_ID"
}

start_relay_fcm
# The relay only accepts NotifyTopic from a peer that has itself registered
# a token for the topic (anti-wake-spam). The writer is a host process with
# no real FCM token, so it registers a dummy one; the fan-out excludes the
# sender's own token, so nothing is ever actually sent to it.
export PUSH_TOKEN="host-writer-dummy-token"
start_writer

ACTIVITY="$(adb -s "$ANDROID_SERIAL" shell cmd package resolve-activity --brief "$PACKAGE" 2>/dev/null | tail -1 | tr -d '\r')"
[[ "$ACTIVITY" == */* ]] || { echo "ERROR: cannot resolve activity for $PACKAGE" >&2; exit 1; }

app_pid() { adb -s "$ANDROID_SERIAL" shell pidof "$PACKAGE" 2>/dev/null | tr -d '\r' || true; }

RUN_TAG="$(date +%s)-$$"
FAILED=0
report() { echo "[ttfs] scenario=$1 ms=$2"; }

# ── Baseline: prove app↔writer wiring + token registration ─────────
echo "==> Baseline: cold launch + first sync"
adb -s "$ANDROID_SERIAL" shell am force-stop "$PACKAGE"
sleep 1
BASE="pw-base-$RUN_TAG"
writer_insert "$BASE"
logcat_clear
adb -s "$ANDROID_SERIAL" shell am start -n "$ACTIVITY" >/dev/null
if ms=$(logcat_wait_for "tasks_visible.*$BASE" 120); then
    report baseline_first_launch "$ms"
else
    echo "ERROR: baseline sync never happened — wiring broken; aborting." >&2
    exit 1
fi
# Wait until the relay has BOTH token registrations (writer's dummy +
# the phone's) — without the phone's, the wake push has no recipient
# and the scenario fails for harness reasons, not product ones.
REG_OK=0
for i in {1..60}; do
    if (( $(grep -c "Registered Fcm push token" "$LOGDIR/relay.log" || true) >= 2 )); then
        REG_OK=1
        echo "phone token registered after ${i}s"
        break
    fi
    sleep 1
done
if [[ $REG_OK -eq 0 ]]; then
    echo "ERROR: phone never registered its FCM token with the relay:" >&2
    grep -iE "token|register" "$LOGDIR/relay.log" | tail -10 >&2
    exit 1
fi
PID_BEFORE="$(app_pid)"
[[ -n "$PID_BEFORE" ]] || { echo "ERROR: app not running after baseline" >&2; exit 1; }
echo "app pid=$PID_BEFORE"

# ── Background (NOT killed), let connections die ───────────────────
echo "==> HOME (background, process alive); waiting 75s (past QUIC idle + suspension gap)"
adb -s "$ANDROID_SERIAL" shell input keyevent 3
sleep 75
PID_MID="$(app_pid)"
if [[ "$PID_MID" != "$PID_BEFORE" ]]; then
    echo "WARNING: app process changed/died while backgrounded (pid '$PID_BEFORE' → '$PID_MID')." >&2
    echo "         This run degenerates to the cold path — not the scenario under test." >&2
fi

# ── The push wake ──────────────────────────────────────────────────
NK="pw-nk-$RUN_TAG"
echo "==> Writer inserts '$NK' → FCM data push should wake the backgrounded app"
logcat_clear
writer_insert "$NK"

# The wake must REUSE the live engine: the reuse stage marker must
# appear; the cold path's engine_built must NOT.
if ms=$(logcat_wait_for "bg_sync stage=live_engine_found" 120); then
    report wake_reused_live_engine "$ms"
else
    echo "FAIL: never saw 'bg_sync stage=live_engine_found' — wake didn't reuse the live engine." >&2
    adb -s "$ANDROID_SERIAL" logcat -d 2>/dev/null | grep -iE "wavesync|WaveSyncService|bg_sync" | tail -25 >&2
    FAILED=1
fi
if ms=$(logcat_wait_for "bg_sync stage=done" 60); then
    report wake_done "$ms"
else
    echo "FAIL: background sync never reported stage=done." >&2
    FAILED=1
fi
if adb -s "$ANDROID_SERIAL" logcat -d 2>/dev/null | grep -q "bg_sync stage=engine_built"; then
    echo "FAIL: cold path ran (stage=engine_built) — duplicate engine was built next to the live one." >&2
    FAILED=1
fi
PID_AFTER="$(app_pid)"
if [[ "$PID_AFTER" == "$PID_BEFORE" && -n "$PID_BEFORE" ]]; then
    echo "app pid unchanged through the wake ($PID_AFTER) — not-killed scenario held"
else
    echo "WARNING: app pid changed across the wake ('$PID_BEFORE' → '$PID_AFTER')." >&2
fi

# ── Foreground: the row must already be there ──────────────────────
echo "==> Foregrounding app; row must already be applied"
logcat_clear
adb -s "$ANDROID_SERIAL" shell am start -n "$ACTIVITY" >/dev/null
if ms=$(logcat_wait_for "tasks_visible.*$NK" 30); then
    report row_visible_on_foreground "$ms"
else
    echo "FAIL: pushed row not visible after foregrounding." >&2
    FAILED=1
fi

echo
if [[ "$FAILED" -eq 0 ]]; then
    echo "============================================================"
    echo "  PASS — not-killed push wake reused the live engine and"
    echo "  synced. Grep '[ttfs]' above for timings."
    echo "============================================================"
else
    echo "SOME ASSERTIONS FAILED — see logs in $LOGDIR" >&2
    exit 1
fi
