#!/usr/bin/env bash
# Android-emulator WAN sync scenarios — the on-device counterpart of
# tests-e2e/tests/wan_latency.rs. Measures time-to-first-sync (TTFS)
# for the conditions only a real Android stack exhibits:
#
#   A1 cold-start      app force-stopped, relaunched; host row written
#                      while dead. Device analogue of e2e S1.
#   A2 path-change     app backgrounded, WiFi→cellular interface
#      resume          migration (wlan0 loses its address), 60s past
#                      the QUIC idle timeout, then foregrounded. The
#                      one N14-family condition Docker cannot model.
#   A3 airplane blip   app backgrounded through a 30s airplane-mode
#                      blackout (elevator/tunnel), then foregrounded.
#
# Measurement: the example app logs `tasks_visible titles=[…]` on every
# task-list update; the host writes a UUID-titled row at the reference
# instant and polls logcat for the title. TTFS resolution ≈ 0.2s.
#
# Uses the dioxus_fcm_sync example app (its compile-time topic and
# passphrase; the relay address is injected at build time). adb-only —
# no Maestro needed for these scenarios.
#
# Usage:
#     ./run_wan_scenarios.sh            # all scenarios
#     ./run_wan_scenarios.sh --stop     # tear down relay/writer
#     SKIP_INSTALL=1 ./run_wan_scenarios.sh   # reuse installed APK

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/common.sh"

ROOT="$(repo_root)"
APP_DIR="$ROOT/examples/dioxus_fcm_sync"
LOGDIR="$HERE/.test-logs"
PIDDIR="$HERE/.test-pids"
mkdir -p "$LOGDIR" "$PIDDIR"

PACKAGE="com.wavesync.mobile_demo"
# The app's topic/passphrase are compile-time constants — adopt them.
# Per-run UUID titles keep concurrent/stale rows from false-passing.
TOPIC="mobile-tasks-demo"
PASSPHRASE="demo-shared-secret"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-8499}"
# The relay derives its QUIC listener from the TCP listen port
# (wavesync_relay main.rs `extract_tcp_port`), so TCP and QUIC must be
# the SAME port or the advertised QUIC address points at nothing.
RELAY_QUIC_PORT="${RELAY_QUIC_PORT:-4011}"
RELAY_TCP_PORT="${RELAY_TCP_PORT:-4011}"
# Same pinned key the fcm test uses — stable PeerId across runs.
RELAY_KEY='CAESQGlCc264ZKF3D4l/5VXTLjnGdDKxg0cyX2UosIkZmNAbxV5oeISRfEDIrc/+hdQuqepe9CCCc3M5G3DJBs6N6lE='

cleanup() {
    "$HERE/netctl.sh" restore >/dev/null 2>&1 || true
    stop_all
}
trap cleanup EXIT INT TERM

if [[ "${1:-}" == "--stop" ]]; then
    cleanup
    trap - EXIT
    exit 0
fi
stop_all 2>/dev/null || true

require adb   "Install Android SDK platform-tools."
require dx    "Install Dioxus CLI: cargo install dioxus-cli."
require cargo "Install Rust toolchain."
detect_device
echo "Using device: $ANDROID_SERIAL"

start_relay
start_writer

# ── Build & install the APK against our relay ──────────────────────

if [[ -z "${SKIP_INSTALL:-}" ]]; then
    echo "==> Building APK with WAVESYNC_RELAY_OVERRIDE=$APK_RELAY_ADDR"
    (
        cd "$APP_DIR"
        WAVESYNC_RELAY_OVERRIDE="$APK_RELAY_ADDR" \
            dx build --platform android --release 2>&1 | tee "$LOGDIR/dx-build.log" | tail -3
    )
    DX_OUT_BASE="${CARGO_TARGET_DIR:-$HOME/.cargo/shared-target}/dx/example-dioxus-fcm-sync/release/android/app"
    APK="$(find "$DX_OUT_BASE" -name 'app-debug.apk' -o -name 'app-release*.apk' 2>/dev/null | head -1)"
    [[ -n "$APK" ]] || { echo "ERROR: no APK under $DX_OUT_BASE" >&2; exit 1; }
    echo "APK: $APK"
    adb -s "$ANDROID_SERIAL" install -r -g "$APK" >/dev/null
fi

ACTIVITY="$(adb -s "$ANDROID_SERIAL" shell cmd package resolve-activity --brief "$PACKAGE" 2>/dev/null | tail -1 | tr -d '\r')"
[[ "$ACTIVITY" == */* ]] || { echo "ERROR: could not resolve launcher activity for $PACKAGE (got '$ACTIVITY')" >&2; exit 1; }
echo "activity: $ACTIVITY"

app_start()  { adb -s "$ANDROID_SERIAL" shell am start -n "$ACTIVITY" >/dev/null; }
app_stop()   { adb -s "$ANDROID_SERIAL" shell am force-stop "$PACKAGE"; }
app_home()   { adb -s "$ANDROID_SERIAL" shell input keyevent 3; }

RUN_TAG="$(date +%s)-$$"
report() { echo "[ttfs] scenario=$1 ms=$2"; }
fail_scenario() {
    echo "[ttfs] scenario=$1 ms=TIMEOUT" >&2
    echo "  last app logcat lines:" >&2
    adb -s "$ANDROID_SERIAL" logcat -d 2>/dev/null | grep -iE "wavesync|RustStdoutStderr|tasks_visible" | tail -20 >&2
    FAILED=1
}
FAILED=0

# ── Baseline: prove the app↔writer path before measuring anything ──

echo "==> Baseline: app cold launch + first sync"
app_stop
BASE="wan-base-$RUN_TAG"
writer_insert "$BASE"
logcat_clear
app_start
if ms=$(logcat_wait_for "tasks_visible.*$BASE" 120); then
    report baseline_first_launch "$ms"
else
    echo "ERROR: baseline sync never happened — relay/app wiring broken; aborting." >&2
    fail_scenario baseline_first_launch
    exit 1
fi

# ── A1: cold-start with warm cache ─────────────────────────────────

echo "==> A1: cold start (force-stop → relaunch)"
app_stop
sleep 2
T1="wan-a1-$RUN_TAG"
writer_insert "$T1"
logcat_clear
app_start
if ms=$(logcat_wait_for "tasks_visible.*$T1" 120); then
    report a1_cold_start "$ms"
else
    fail_scenario a1_cold_start
fi

# ── A2: WiFi→cellular path change while backgrounded ───────────────

echo "==> A2: backgrounded path change (wifi→cellular), 60s, foreground"
app_home
sleep 2
"$HERE/netctl.sh" flip-to-cellular
echo "    flipped to cellular; waiting 60s (past QUIC idle timeout)"
sleep 60
T2="wan-a2-$RUN_TAG"
writer_insert "$T2"
logcat_clear
app_start
if ms=$(logcat_wait_for "tasks_visible.*$T2" 120); then
    report a2_pathchange_resume "$ms"
else
    fail_scenario a2_pathchange_resume
fi
"$HERE/netctl.sh" restore
sleep 8   # let WiFi revalidate before the next scenario

# ── A3: airplane-mode blackout while backgrounded ──────────────────

echo "==> A3: backgrounded airplane blip (30s), foreground after restore"
app_home
sleep 2
"$HERE/netctl.sh" airplane on
sleep 30
T3="wan-a3-$RUN_TAG"
writer_insert "$T3"
"$HERE/netctl.sh" airplane off
sleep 3   # radio re-attach
logcat_clear
app_start
if ms=$(logcat_wait_for "tasks_visible.*$T3" 120); then
    report a3_airplane_resume "$ms"
else
    fail_scenario a3_airplane_resume
fi

echo
if [[ "$FAILED" -eq 0 ]]; then
    echo "============================================================"
    echo "  All WAN emulator scenarios completed. Grep '[ttfs]' above."
    echo "============================================================"
else
    echo "SOME SCENARIOS FAILED — see [ttfs] lines and logs in $LOGDIR" >&2
    exit 1
fi
