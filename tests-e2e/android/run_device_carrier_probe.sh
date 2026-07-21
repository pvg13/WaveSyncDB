#!/usr/bin/env bash
# Real-device carrier probe (#108 carrier half + #111 acceptance).
#
# One Android phone on USB + an internet-reachable TEST relay (never the
# Mediterranea production relay). The host runs the second peer (the
# writer), so the measured topology is phone-on-carrier <-> home-NAT —
# the common Mediterranea case. The carrier<->carrier column still needs
# a second phone.
#
# Phases:
#   P0 baseline   WiFi, proves app<->writer<->relay wiring end to end.
#   P1 cellular   WiFi off; TTFS + the m1-diag beacon = the carrier-NAT
#                 classification (direct vs relay; see the research doc's
#                 interpretation table).
#   P2 doze       screen off + deviceidle force-idle 90s + write during
#                 the freeze, then wake + foreground: recovery TTFS is
#                 the #111 acceptance number on real hardware.
#   P3 restore    WiFi back on.
#
# Requirements:
#   - RELAY_ADDR env: full multiaddr of the TEST relay, e.g.
#       /dns4/test-relay.example.com/udp/4011/quic-v1/p2p/<PeerId>
#     (the relay derives its QUIC listener from its TCP port — expose the
#     same port for both; pin IDENTITY_KEYPAIR so the PeerId is stable).
#   - Phone: USB debugging on, active cellular data, and FOR P2 set the
#     screen lock to None/Swipe for the duration (adb cannot enter a PIN;
#     with a PIN set you must unlock manually when prompted).
#   - The phone may have WiFi toggled and be dozed during the run.
#
# Usage:
#   RELAY_ADDR=/dns4/.../p2p/... ./run_device_carrier_probe.sh
#   SKIP_INSTALL=1 ...           # reuse the installed APK

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/common.sh"

: "${RELAY_ADDR:?set RELAY_ADDR to the test relay multiaddr (/dns4|/ip4/.../udp/PORT/quic-v1/p2p/PeerId)}"
export RELAY_ADDR   # consumed by common.sh's start_writer

ROOT="$(repo_root)"
APP_DIR="$ROOT/examples/dioxus_fcm_sync"
LOGDIR="$HERE/.test-logs"
PIDDIR="$HERE/.test-pids"
mkdir -p "$LOGDIR" "$PIDDIR"
PACKAGE="com.wavesync.mobile_demo"
TOPIC="mobile-tasks-demo"
PASSPHRASE="demo-shared-secret"
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-8499}"

require adb   "Install Android SDK platform-tools."
require dx    "Install Dioxus CLI: cargo install dioxus-cli."
require cargo "Install Rust toolchain."
detect_device
echo "Using device: $ANDROID_SERIAL"

CARRIER="$(adb -s "$ANDROID_SERIAL" shell getprop gsm.operator.alpha | tr -d '\r')"
NETTYPE="$(adb -s "$ANDROID_SERIAL" shell getprop gsm.network.type | tr -d '\r')"
echo "Carrier: ${CARRIER:-unknown}   network type(s): ${NETTYPE:-unknown}"

cleanup() {
    adb -s "$ANDROID_SERIAL" shell svc power stayon false >/dev/null 2>&1 || true
    adb -s "$ANDROID_SERIAL" shell svc wifi enable >/dev/null 2>&1 || true
    adb -s "$ANDROID_SERIAL" shell dumpsys deviceidle unforce >/dev/null 2>&1 || true
    stop_all 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ── Writer peer on the host, joined via the SAME test relay ────────
echo "==> Pre-building writer"
(cd "$ROOT" && cargo build --release --quiet -p wavesyncdb-e2e --bin test-peer)
echo "==> Starting writer peer (host) against $RELAY_ADDR"
start_writer

# ── Build & install the APK against the test relay ─────────────────
if [[ -z "${SKIP_INSTALL:-}" ]]; then
    echo "==> Building APK with WAVESYNC_RELAY_OVERRIDE=$RELAY_ADDR"
    (
        cd "$APP_DIR"
        WAVESYNC_RELAY_OVERRIDE="$RELAY_ADDR" \
            dx build --platform android --release 2>&1 | tee "$LOGDIR/dx-build.log" | tail -3
    )
    DX_OUT_BASE="${CARGO_TARGET_DIR:-$HOME/.cargo/shared-target}/dx/example-dioxus-fcm-sync/release/android/app"
    APK="$(find "$DX_OUT_BASE" -name 'app-debug.apk' -o -name 'app-release*.apk' 2>/dev/null | head -1)"
    [[ -n "$APK" ]] || { echo "ERROR: no APK under $DX_OUT_BASE" >&2; exit 1; }
    adb -s "$ANDROID_SERIAL" install -r -g "$APK" >/dev/null
    echo "installed: $APK"
fi
# Clean state so numbers aren't inflated by prior-run rows.
adb -s "$ANDROID_SERIAL" shell pm clear "$PACKAGE" >/dev/null
# Keep the screen awake while USB-connected: a screen timeout mid-run
# unfocuses the app, and the foreground gate (#112) then correctly
# suppresses transitions — silently voiding the P1 measurement (this is
# not hypothetical; it ate a run).
adb -s "$ANDROID_SERIAL" shell svc power stayon usb
adb -s "$ANDROID_SERIAL" shell input keyevent 224

ACTIVITY="$(adb -s "$ANDROID_SERIAL" shell cmd package resolve-activity --brief "$PACKAGE" 2>/dev/null | tail -1 | tr -d '\r')"
[[ "$ACTIVITY" == */* ]] || { echo "ERROR: could not resolve activity for $PACKAGE" >&2; exit 1; }

app_start()  { adb -s "$ANDROID_SERIAL" shell am start -n "$ACTIVITY" >/dev/null; }
app_home()   { adb -s "$ANDROID_SERIAL" shell input keyevent 3; }
screen_off() { adb -s "$ANDROID_SERIAL" shell input keyevent 26; }
screen_on()  { adb -s "$ANDROID_SERIAL" shell input keyevent 224; sleep 1; adb -s "$ANDROID_SERIAL" shell input keyevent 82; }

RUN_TAG="dev-$(date +%s)"
report() { echo "[carrier-probe] $1=$2"; }
beacon() {
    echo "    waiting for the next m1-diag beacon (30s cadence)..."
    sleep 35
    adb -s "$ANDROID_SERIAL" logcat -d 2>/dev/null | grep "m1-diag" | tail -1 | sed 's/^/[m1-diag] /' || true
}

# ── P0: baseline on WiFi ───────────────────────────────────────────
echo "==> P0 baseline (WiFi): cold launch + first sync"
T="p0-$RUN_TAG"; writer_insert "$T"; logcat_clear; app_start
if ms=$(logcat_wait_for "tasks_visible.*$T" 120); then report p0_wifi_baseline_ms "$ms"; else
    echo "ERROR: baseline sync failed — check RELAY_ADDR reachability and writer logs in $LOGDIR" >&2; exit 1; fi
beacon

# ── P1: cellular only ──────────────────────────────────────────────
echo "==> P1 cellular: disabling WiFi"
adb -s "$ANDROID_SERIAL" shell svc wifi disable
echo "    waiting for cellular validation (up to 30s)"
for i in $(seq 1 30); do
    V="$(adb -s "$ANDROID_SERIAL" shell dumpsys connectivity 2>/dev/null | grep -c 'CELLULAR.*VALIDATED' || true)"
    [[ "$V" -ge 1 ]] && break; sleep 1
done
[[ "${V:-0}" -ge 1 ]] || echo "WARNING: cellular not showing VALIDATED — results may be void; check mobile data."
T="p1-$RUN_TAG"; writer_insert "$T"; logcat_clear
if ms=$(logcat_wait_for "tasks_visible.*$T" 120); then report p1_cellular_ttfs_ms "$ms"; else report p1_cellular_ttfs_ms TIMEOUT; fi
beacon   # <-- THE carrier<->home classification line

# ── P2: doze on cellular (#111 acceptance) ────────────────────────
echo "==> P2 doze: background + screen off + force-idle 90s"
app_home; sleep 2; screen_off; sleep 2
adb -s "$ANDROID_SERIAL" shell dumpsys deviceidle force-idle >/dev/null || echo "WARNING: force-idle refused (screen state?); doze phase may be void"
T="p2-$RUN_TAG"; writer_insert "$T"; logcat_clear
echo "    dozing 90s (write already queued at the writer)"
if ms=$(logcat_wait_for "tasks_visible.*$T" 90); then
    echo "[doze] row landed DURING doze after ${ms}ms (exemption/foreground-service in play)"
else
    echo "[doze] row did not land during 90s doze (expected freeze)"
fi
adb -s "$ANDROID_SERIAL" shell dumpsys deviceidle unforce >/dev/null
screen_on; sleep 1; logcat_clear; app_start
if ms=$(logcat_wait_for "tasks_visible.*$T" 120); then report p2_doze_recovery_ms "$ms"; else report p2_doze_recovery_ms TIMEOUT; fi
beacon

# ── P3: restore ────────────────────────────────────────────────────
adb -s "$ANDROID_SERIAL" shell svc wifi enable
echo
echo "============================================================"
echo " Summary — paste into docs/research/android-cellular-p2p-2026-08.md"
echo "   carrier: ${CARRIER:-unknown}   network: ${NETTYPE:-unknown}"
echo "   grep '[carrier-probe]' and '[m1-diag]' lines above."
echo "   Interpretation table: research doc §Instrumentation."
echo "============================================================"
