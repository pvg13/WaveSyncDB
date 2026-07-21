#!/usr/bin/env bash
# iOS device session runner (#109) — one phone, one USB connection, one
# sitting. Drives every phase of docs/research/ios-cellular-p2p-2026-08.md
# that a single iPhone + this Mac can answer, captures the evidence, and
# writes a pre-filled summary keyed to the issues it closes:
#
#   S1  carrier-NAT classification (WiFi baseline + cellular)   → #109/M1
#   S3  #73 QUIC bind A/B (concrete vs unspecified)             → #73, #74
#   S4  #74 handoff-latency trials (3×, default arm)            → #74
#   S6  suspension vs live P2P                                  → #109/M1
#   S5  push-budget reality (needs relay APNs creds)            → #109/M1
#   N1  #92 NSE fallback verification (needs the NSE build)     → #92
#
# The phone↔phone cellular column still needs a second device — everything
# else lands here. Run ON A MAC (Xcode 15+, `xcrun devicectl`); the app
# must be a DEV-SIGNED build of a consumer app (Mediterranea) linked
# against a wavesyncdb rev that has the engine-side m1-diag beacon and the
# #92 NSE template.
#
# Requirements:
#   - RELAY_ADDR env: full multiaddr of the TEST relay (never production),
#     e.g. /dns4/relay.wavesyncdb.com/udp/4001/quic-v1/p2p/<PeerId>
#   - BUNDLE_ID env: the app's bundle id (dev-signed install on the phone).
#   - TOPIC + PASSPHRASE env: the group the app is joined to (the host
#     writer joins the same group).
#   - cargo (builds the host writer), curl.
#   - Phone on USB, developer mode on, unlocked at phase starts.
#   - S5 additionally: the relay must run with APNs credentials and the
#     phone's token registered (else exported SKIP_S5=1 skips it).
#   - N1 additionally: the NSE build installed (else SKIP_NSE=1 skips it).
#
# Usage:
#   RELAY_ADDR=... BUNDLE_ID=com.example.app TOPIC=... PASSPHRASE=... \
#       ./run_device_session.sh
#   ./run_device_session.sh --selftest      # parser self-test, any OS
#
# Every phase appends to $SESSION_DIR; nothing on the phone is deleted.

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

# ── log parsers (pure; used by phases and by --selftest) ─────────────────

# Last m1-diag beacon line → "ratio=<r> peers=<n> peers_via_relay=<n> dcutr=<S/A>"
parse_last_beacon() { # <logfile>
    grep "m1-diag " "$1" | tail -1 | sed -E \
        's/.*dcutr=([0-9]+\/[0-9]+).*ratio=([^ ]+) peers=([0-9]+) peers_via_relay=([0-9]+).*/ratio=\2 peers=\3 peers_via_relay=\4 dcutr=\1/'
}

# DIAG QUIC listen elapsed ms (S3): last "listen_on(QUIC) returned in Nms"-style line.
parse_listen_elapsed() { # <logfile>
    grep "DIAG listen_on(QUIC) returned in" "$1" | tail -1 | grep -oE '[0-9]+ ?ms' | tail -1
}

# Handoff lap 1 (S4): epoch ms of the first "appeared; binding QUIC listener"
# line AFTER the given epoch-ms mark, using the capture file's own stamps
# (each captured line is prefixed "EPOCHMS |" by the capture pipeline).
first_line_after() { # <logfile> <mark_ms> <regex>
    awk -F'|' -v mark="$2" -v re="$3" \
        '$1+0 > mark+0 && $0 ~ re { print $1+0; exit }' "$1"
}

# bg_sync stage table (S5): "stage elapsed_ms" pairs in order.
parse_bg_stages() { # <logfile>
    grep -oE 'bg_sync stage=[a-z_]+( elapsed_ms=[0-9]+)?' "$1" \
        | sed -E 's/bg_sync stage=([a-z_]+)( elapsed_ms=([0-9]+))?/\1 \3/'
}

selftest() {
    local t; t="$(mktemp)"
    cat > "$t" <<'EOF'
1000 |noise
2000 |m1-diag relayed_est=2 direct_est=0 demoted=0 dcutr=0/1 relay_bytes=100 direct_bytes=0 ratio=Some(1.0) peers=1 peers_via_relay=1
3000 |DIAG listen_on(QUIC) returned in 42ms
4000 |Network interface 10.0.0.5 departed; removing QUIC listener
5500 |Network interface 100.64.1.2 appeared; binding QUIC listener
6000 |bg_sync stage=config_loaded elapsed_ms=120
7000 |bg_sync stage=done elapsed_ms=9000
8000 |m1-diag relayed_est=2 direct_est=1 demoted=1 dcutr=1/1 relay_bytes=100 direct_bytes=900 ratio=Some(0.1) peers=1 peers_via_relay=0
EOF
    local fails=0
    local b; b="$(parse_last_beacon "$t")"
    [[ "$b" == "ratio=Some(0.1) peers=1 peers_via_relay=0 dcutr=1/1" ]] \
        || { echo "FAIL parse_last_beacon: '$b'"; fails=1; }
    local l; l="$(parse_listen_elapsed "$t")"
    [[ "$l" == "42ms" || "$l" == "42 ms" ]] \
        || { echo "FAIL parse_listen_elapsed: '$l'"; fails=1; }
    local f; f="$(first_line_after "$t" 4100 'appeared; binding QUIC listener')"
    [[ "$f" == "5500" ]] || { echo "FAIL first_line_after: '$f'"; fails=1; }
    local s; s="$(parse_bg_stages "$t" | tr '\n' ';')"
    [[ "$s" == "config_loaded 120;done 9000;" ]] \
        || { echo "FAIL parse_bg_stages: '$s'"; fails=1; }
    rm -f "$t"
    ((fails == 0)) && echo "selftest OK" || exit 1
}
if [[ "${1:-}" == "--selftest" ]]; then selftest; exit 0; fi

# ── session setup ────────────────────────────────────────────────────────

: "${RELAY_ADDR:?set RELAY_ADDR to the TEST relay multiaddr}"
: "${BUNDLE_ID:?set BUNDLE_ID to the dev-signed app's bundle id}"
: "${TOPIC:?set TOPIC to the app's group topic}"
: "${PASSPHRASE:?set PASSPHRASE to the group passphrase}"

command -v xcrun >/dev/null || { echo "ERROR: xcrun not found — run on a Mac."; exit 2; }
command -v cargo >/dev/null || { echo "ERROR: cargo not found."; exit 2; }

SESSION_DIR="$HERE/.session-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$SESSION_DIR"
SUMMARY="$SESSION_DIR/summary.md"
echo "session dir: $SESSION_DIR"

UDID="${UDID:-$(xcrun devicectl list devices 2>/dev/null \
    | awk '/connected/ {print $NF; exit}')}"
[[ -n "$UDID" ]] || { echo "ERROR: no device via devicectl; plug the phone in, trust the Mac."; exit 2; }
echo "device: $UDID"

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
mark()   { echo "$(now_ms) [mark] $*" | tee -a "$SESSION_DIR/marks.log"; }
say()    { echo; echo "=== $*"; }
pause_for() { read -r -p ">>> $* — press Enter WHEN DONE: "; }

# Launch the app with env, streaming its stdio into a stamped capture file.
# devicectl --console blocks while attached; we background it and kill the
# attachment (NOT the app) to end a capture. Each captured line is prefixed
# "EPOCHMS |" so latency laps use one clock (this Mac's).
CONSOLE_PID=""
launch_app() { # <capture-file> <env-json>
    local cap="$1" envjson="$2"
    end_capture
    ( xcrun devicectl device process launch --console --terminate-existing \
          --environment-variables "$envjson" \
          --device "$UDID" "$BUNDLE_ID" 2>&1 \
      | while IFS= read -r line; do printf '%s |%s\n' "$(now_ms)" "$line"; done \
      > "$cap" ) &
    CONSOLE_PID=$!
    sleep 8
    kill -0 "$CONSOLE_PID" 2>/dev/null \
        || { echo "ERROR: devicectl launch died — check dev signing / unlocked phone:"; tail -5 "$cap"; exit 1; }
}
end_capture() {
    [[ -n "$CONSOLE_PID" ]] && { kill "$CONSOLE_PID" 2>/dev/null; wait "$CONSOLE_PID" 2>/dev/null; CONSOLE_PID=""; }
}
trap 'end_capture; stop_writer 2>/dev/null' EXIT INT TERM

ENV_BASE='{"WAVESYNC_M1_DIAG":"1"}'
ENV_UNSPEC='{"WAVESYNC_M1_DIAG":"1","WAVESYNC_IOS_UNSPECIFIED_QUIC":"1"}'

# Wait until a fresh beacon (≥1 line newer than <count>) lands, ≤ timeout.
wait_beacons() { # <capture> <min-count> <timeout-s>
    local cap="$1" want="$2" tmo="$3" i
    for ((i = 0; i < tmo; i++)); do
        (( $(grep -c "m1-diag " "$cap" 2>/dev/null || echo 0) >= want )) && return 0
        sleep 1
    done
    return 1
}

# ── host writer (the phone's peer), same contract as the Android probe ──
WRITER_HTTP_PORT="${WRITER_HTTP_PORT:-8499}"
WRITER_PID=""
start_writer() {
    say "building + starting host writer (test-peer)"
    local root tgt db
    root="$(cd "$HERE/../.." && pwd)"
    (cd "$root" && cargo build --release --quiet -p wavesyncdb-e2e --bin test-peer)
    tgt="$(cd "$root" && cargo metadata --format-version=1 --no-deps 2>/dev/null \
        | grep -oE '"target_directory":"[^"]+"' | cut -d'"' -f4)"
    db="$(mktemp -d)/writer.db"
    env BIND_ADDR="0.0.0.0:$WRITER_HTTP_PORT" DB_URL="sqlite:$db?mode=rwc" \
        TOPIC="$TOPIC" PASSPHRASE="$PASSPHRASE" RELAY_ADDR="$RELAY_ADDR" \
        MDNS_ENABLED=false RUST_LOG=info,libp2p_swarm=warn \
        "$tgt/release/test-peer" > "$SESSION_DIR/writer.log" 2>&1 &
    WRITER_PID=$!
    local i
    for i in {1..60}; do
        curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/health" >/dev/null 2>&1 && return 0
        sleep 1
    done
    echo "ERROR: writer never came up"; tail -20 "$SESSION_DIR/writer.log"; exit 1
}
stop_writer() { [[ -n "$WRITER_PID" ]] && kill "$WRITER_PID" 2>/dev/null; WRITER_PID=""; }
writer_insert() { # <id>
    curl -fsS -X POST "http://127.0.0.1:$WRITER_HTTP_PORT/tasks" \
        -H 'content-type: application/json' \
        -d "$(printf '{"id":"%s","title":"%s","completed":false}' "$1" "$1")" >/dev/null
}

# ── phases ───────────────────────────────────────────────────────────────

{
    echo "# iOS device session — $(date +%F)"
    echo
    echo "Device: $UDID · App: $BUNDLE_ID · Relay: $RELAY_ADDR"
    echo
} > "$SUMMARY"

start_writer

# S1a — WiFi baseline classification
say "S1a — WiFi baseline (phone on home WiFi, app foregrounded)"
pause_for "phone ON WiFi, screen on"
CAP="$SESSION_DIR/s1a-wifi.log"
launch_app "$CAP" "$ENV_BASE"
mark "s1a launch"
writer_insert "s1a-$(date +%s)"
wait_beacons "$CAP" 2 90 || echo "WARN: <2 beacons in 90s — check $CAP"
B_WIFI="$(parse_last_beacon "$CAP")"
echo "S1a beacon: $B_WIFI"
{ echo "## S1 — carrier-NAT classification"; echo; echo "- WiFi baseline: \`$B_WIFI\`"; } >> "$SUMMARY"

# S1b — cellular classification (same launch keeps running)
say "S1b — cellular (WiFi OFF via Control Center, app stays foregrounded)"
M_CELL="$(now_ms)"; mark "s1b wifi-off prompt"
pause_for "turn WiFi OFF now"
wait_beacons "$CAP" 4 150 || echo "WARN: beacons stalled post-flip — check $CAP"
B_CELL="$(parse_last_beacon "$CAP")"
echo "S1b beacon: $B_CELL"
# S4 free data point: interface departed→appeared under the default arm.
T_APPEAR="$(first_line_after "$CAP" "$M_CELL" 'appeared; binding QUIC listener')"
{ echo "- Cellular: \`$B_CELL\`"; echo "- (flip re-listen at +$(( ${T_APPEAR:-0} > 0 ? T_APPEAR - M_CELL : -1 )) ms — S4 trial 0)"; } >> "$SUMMARY"

# S3 — #73 bind A/B
say "S3 — #73 A/B: relaunching with the UNSPECIFIED bind arm"
pause_for "turn WiFi back ON"
CAP_B="$SESSION_DIR/s3-unspecified.log"
launch_app "$CAP_B" "$ENV_UNSPEC"
sleep 5
grep -q "unspecified-listen override ACTIVE" "$CAP_B" \
    && echo "toggle ACTIVE confirmed" || echo "WARN: toggle marker missing — env not delivered?"
writer_insert "s3-$(date +%s)"
wait_beacons "$CAP_B" 2 90 || echo "WARN: no beacons on unspecified arm (hang? — that IS the #73 answer)"
B_UNSPEC="$(parse_last_beacon "$CAP_B")"
L_UNSPEC="$(parse_listen_elapsed "$CAP_B")"
say "S3 — handoff under the unspecified arm"
M_S3="$(now_ms)"; mark "s3 wifi-off prompt"
pause_for "turn WiFi OFF now"
wait_beacons "$CAP_B" 4 150 || echo "WARN: unspecified arm lost sync after flip"
B_UNSPEC_CELL="$(parse_last_beacon "$CAP_B")"
{
    echo; echo "## S3 — #73 bind A/B"
    echo; echo "- unspecified arm: listen elapsed \`${L_UNSPEC:-n/a}\`, WiFi beacon \`$B_UNSPEC\`, post-flip beacon \`$B_UNSPEC_CELL\`"
    echo "- toggle marker seen: $(grep -q 'unspecified-listen override ACTIVE' "$CAP_B" && echo yes || echo NO)"
} >> "$SUMMARY"

# S4 — handoff trials, default (concrete) arm
say "S4 — #74 handoff trials (default arm), 3 trials"
pause_for "turn WiFi back ON"
CAP_C="$SESSION_DIR/s4-handoff.log"
launch_app "$CAP_C" "$ENV_BASE"
wait_beacons "$CAP_C" 1 60 || echo "WARN: no first beacon"
{ echo; echo "## S4 — #74 handoff trials (concrete arm, 3s poll)"; echo; } >> "$SUMMARY"
for trial in 1 2 3; do
    say "S4 trial $trial: WiFi OFF on prompt"
    M_T="$(now_ms)"; mark "s4 trial $trial wifi-off"
    pause_for "turn WiFi OFF now"
    sleep 12
    T_A="$(first_line_after "$CAP_C" "$M_T" 'appeared; binding QUIC listener')"
    writer_insert "s4-t$trial-$(date +%s)"
    sleep 10
    T_S="$(first_line_after "$CAP_C" "$M_T" 'm1-diag ')"
    echo "- trial $trial: re-listen +$(( ${T_A:-0} > 0 ? T_A - M_T : -1 )) ms (next beacon +$(( ${T_S:-0} > 0 ? T_S - M_T : -1 )) ms)" >> "$SUMMARY"
    pause_for "turn WiFi back ON, wait for it to connect"
    sleep 8
done

# S6 — suspension vs live P2P
say "S6 — suspension: HOME the app on prompt; writer keeps watching"
M_S6="$(now_ms)"; mark "s6 home prompt"
pause_for "press HOME now (do not lock the screen)"
sleep 90
writer_insert "s6-during-$(date +%s)"
sleep 5
say "S6 — foreground the app"
M_S6B="$(now_ms)"; mark "s6 foreground prompt"
pause_for "foreground the app now"
sleep 20
B_S6="$(parse_last_beacon "$CAP_C")"
{
    echo; echo "## S6 — suspension vs live P2P"
    echo; echo "- backgrounded at mark $M_S6, foregrounded at $M_S6B; post-resume beacon \`$B_S6\`"
    echo "- writer-side connection death: grep 'Connection.*closed\\|disconnect' writer.log around $M_S6"
} >> "$SUMMARY"

# S5 — push budget (skippable)
if [[ "${SKIP_S5:-0}" != "1" ]]; then
    say "S5 — push budget: background the app; writer write triggers a silent push"
    pause_for "background the app (HOME, not force-quit)"
    M_S5="$(now_ms)"; mark "s5 push write"
    writer_insert "s5-$(date +%s)"
    sleep 45
    {
        echo; echo "## S5 — push-budget reality"
        echo; echo '```'; parse_bg_stages "$CAP_C"; echo '```'
        echo "- relay-side: check the relay log/metrics for outcome=... on this send (coalesced/budget_denied = not a trial)"
    } >> "$SUMMARY"
else
    echo "S5 skipped (SKIP_S5=1)" >> "$SUMMARY"
fi

# N1 — #92 NSE fallback (skippable; needs NSE build + a SyncNotify-visible write)
if [[ "${SKIP_NSE:-0}" != "1" ]]; then
    say "N1 — #92: NSE fallback. Requires the NSE build + relay APNs."
    pause_for "app backgrounded; if verifying COLD CACHE, reinstall the app first"
    mark "n1 visible write"
    writer_insert "n1-$(date +%s)"
    echo ">>> Watch the lock screen: localized fallback text (cold cache) or composed content (warm)."
    read -r -p ">>> Banner text seen (type it): " BANNER
    {
        echo; echo "## N1 — #92 NSE fallback"
        echo; echo "- observed banner: \"$BANNER\""
        echo "- NSE process logs aren't in the app's stdio: run 'sudo log collect --device-udid $UDID --last 5m'"
        echo "  and grep the archive for '[WaveSyncNSE]' + 'fallback' to confirm which tier fired."
    } >> "$SUMMARY"
else
    echo "N1 (#92) skipped (SKIP_NSE=1)" >> "$SUMMARY"
fi

end_capture
stop_writer
say "DONE — summary at $SUMMARY"
echo "Attach the summary + per-phase logs to the issue comments; the"
echo "cellular↔cellular column still needs a second phone (expected-fail)."
