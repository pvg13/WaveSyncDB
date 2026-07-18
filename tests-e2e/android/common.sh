#!/usr/bin/env bash
# Shared environment + helpers for the Android-emulator WAN scenarios.
# Source this from provision.sh / netctl.sh / run_wan_scenarios.sh.
#
# Design mirrors examples/dioxus_fcm_sync/test.sh (the proven Android
# orchestration in this repo): pinned relay identity read back from the
# startup log, test-peer HTTP writer as the counterpart, .test-logs/
# and .test-pids/ per-script state, --stop support via stop_all.

set -euo pipefail

ANDROID_HOME="${ANDROID_HOME:-$HOME/Android/Sdk}"
export ANDROID_HOME
# avdmanager/sdkmanager live in cmdline-tools; maestro installs to
# ~/.maestro/bin. Neither is customarily on PATH.
export PATH="$ANDROID_HOME/platform-tools:$ANDROID_HOME/emulator:$ANDROID_HOME/cmdline-tools/latest/bin:$HOME/.maestro/bin:$PATH"

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "ERROR: $1 not found on PATH. $2" >&2
        exit 2
    }
}

# Pick the adb target: honor ANDROID_SERIAL, else first online device.
# Sets and exports ANDROID_SERIAL; fails if nothing is connected.
detect_device() {
    if [[ -z "${ANDROID_SERIAL:-}" ]]; then
        ANDROID_SERIAL="$(adb devices | awk 'NR>1 && $2=="device" {print $1; exit}')"
    fi
    if [[ -z "${ANDROID_SERIAL:-}" ]]; then
        echo "ERROR: no online adb devices. Run provision.sh first." >&2
        exit 2
    fi
    export ANDROID_SERIAL
}

# ── logcat measurement helpers ─────────────────────────────────────
#
# TTFS measurement contract: logcat_clear at the reference instant,
# then logcat_wait_for '<regex>' <timeout_s>. Prints the elapsed
# milliseconds from the call to the first matching line and returns 0,
# or returns 1 on timeout. Resolution is bounded by the 0.2s poll.

logcat_clear() {
    adb -s "$ANDROID_SERIAL" logcat -c
}

logcat_wait_for() {
    local regex="$1" timeout_s="$2"
    local start_ms now_ms deadline_ms
    start_ms=$(date +%s%3N)
    deadline_ms=$((start_ms + timeout_s * 1000))
    while :; do
        if adb -s "$ANDROID_SERIAL" logcat -d 2>/dev/null | grep -qE "$regex"; then
            now_ms=$(date +%s%3N)
            echo $((now_ms - start_ms))
            return 0
        fi
        now_ms=$(date +%s%3N)
        if ((now_ms >= deadline_ms)); then
            return 1
        fi
        sleep 0.2
    done
}

# ── host-side relay + writer (test-peer) orchestration ─────────────
#
# Callers set: LOGDIR PIDDIR RELAY_KEY RELAY_QUIC_PORT RELAY_TCP_PORT
# WRITER_HTTP_PORT TOPIC PASSPHRASE. start_relay sets RELAY_PEER_ID,
# RELAY_ADDR (host form) and APK_RELAY_ADDR (10.0.2.2 form for
# emulators, LAN-IP form for physical devices).

repo_root() {
    cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

# The workspace may redirect build output (CARGO_TARGET_DIR / cargo
# config — this repo uses a shared target dir), so never assume ./target.
cargo_target_dir() {
    (cd "$(repo_root)" && cargo metadata --format-version=1 --no-deps 2>/dev/null \
        | grep -oP '"target_directory":"\K[^"]+')
}

stop_all() {
    local pidfile name pid
    for pidfile in "${PIDDIR:?}"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        name="$(basename "$pidfile" .pid)"
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
            echo "stopped $name (pid=$pid)"
        fi
        rm -f "$pidfile"
    done
}

lan_ip() {
    local ip
    ip="$(ip -4 -o addr show scope global 2>/dev/null \
        | awk '{print $4}' | cut -d/ -f1 \
        | grep -E '^192\.168\.' | head -1 || true)"
    [[ -z "$ip" ]] && ip="$(ip -4 -o addr show scope global 2>/dev/null \
        | awk '{print $4}' | cut -d/ -f1 | head -1 || echo 127.0.0.1)"
    echo "$ip"
}

start_relay() {
    local root lan tgt
    root="$(repo_root)"
    lan="$(lan_ip)"
    tgt="$(cargo_target_dir)"
    echo "==> Pre-building relay + writer"
    (cd "$root" && cargo build --release --quiet -p wavesync_relay)
    (cd "$root" && cargo build --release --quiet -p wavesyncdb-e2e --bin test-peer)

    echo "==> Starting relay"
    (
        cd "$root"
        setsid env RUST_LOG=info \
            "$tgt/release/wavesync-relay" \
            --identity-keypair="$RELAY_KEY" \
            --listen-addr "/ip4/0.0.0.0/tcp/$RELAY_TCP_PORT" \
            --external-address "/ip4/$lan/tcp/$RELAY_TCP_PORT" \
            --external-address "/ip4/$lan/udp/$RELAY_QUIC_PORT/quic-v1" \
            --max-reservations-per-peer 256 \
            > "$LOGDIR/relay.log" 2>&1 &
        echo $! > "$PIDDIR/relay.pid"
    )
    RELAY_PEER_ID=""
    local i
    for i in {1..60}; do
        if ! kill -0 "$(cat "$PIDDIR/relay.pid")" 2>/dev/null; then
            echo "ERROR: relay died at startup:" >&2
            tail -30 "$LOGDIR/relay.log" >&2
            exit 1
        fi
        RELAY_PEER_ID="$(grep -oP 'Relay server PeerId: \K\S+' "$LOGDIR/relay.log" | head -1 || true)"
        [[ -n "$RELAY_PEER_ID" ]] && break
        sleep 1
    done
    if [[ -z "$RELAY_PEER_ID" ]]; then
        echo "ERROR: relay never printed its PeerId:" >&2
        tail -30 "$LOGDIR/relay.log" >&2
        exit 1
    fi
    RELAY_ADDR="/ip4/$lan/udp/$RELAY_QUIC_PORT/quic-v1/p2p/$RELAY_PEER_ID"
    # Emulators reach the host at 10.0.2.2, never at the LAN IP.
    if [[ "$ANDROID_SERIAL" == emulator-* ]]; then
        APK_RELAY_ADDR="/ip4/10.0.2.2/udp/$RELAY_QUIC_PORT/quic-v1/p2p/$RELAY_PEER_ID"
    else
        APK_RELAY_ADDR="$RELAY_ADDR"
    fi
    echo "relay peer-id=$RELAY_PEER_ID"
    echo "relay addr (host)=$RELAY_ADDR"
    echo "relay addr (apk) =$APK_RELAY_ADDR"
}

start_writer() {
    local root db tgt
    root="$(repo_root)"
    db="$(mktemp -d)/writer.db"
    tgt="$(cargo_target_dir)"
    echo "==> Starting writer peer on http://127.0.0.1:$WRITER_HTTP_PORT"
    (
        cd "$root"
        setsid env BIND_ADDR="0.0.0.0:$WRITER_HTTP_PORT" \
            DB_URL="sqlite:$db?mode=rwc" \
            TOPIC="$TOPIC" \
            PASSPHRASE="$PASSPHRASE" \
            RELAY_ADDR="$RELAY_ADDR" \
            PUSH_TOKEN="${PUSH_TOKEN:-}" \
            MDNS_ENABLED=false \
            RUST_LOG=info,libp2p_swarm=warn \
            "$tgt/release/test-peer" \
            > "$LOGDIR/writer.log" 2>&1 &
        echo $! > "$PIDDIR/writer.pid"
    )
    local i
    for i in {1..60}; do
        if curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/health" >/dev/null 2>&1; then
            echo "writer up after ${i}s"
            return 0
        fi
        sleep 1
    done
    echo "ERROR: writer never came up:" >&2
    tail -30 "$LOGDIR/writer.log" >&2
    exit 1
}

writer_insert() {
    # writer_insert <id-and-title> — the app UI shows titles; ids just
    # need uniqueness.
    curl -fsS -X POST "http://127.0.0.1:$WRITER_HTTP_PORT/tasks" \
        -H 'content-type: application/json' \
        -d "$(printf '{"id":"%s","title":"%s","completed":false}' "$1" "$1")" \
        > /dev/null
}

writer_has_title() {
    curl -fs "http://127.0.0.1:$WRITER_HTTP_PORT/tasks" 2>/dev/null \
        | grep -q "\"title\":\"$1\""
}
