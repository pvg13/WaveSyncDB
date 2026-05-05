#!/usr/bin/env bash
# Quick end-to-end test harness for the qr-pairing example.
#
# Spins up:
#   1. wavesync_relay (TCP/QUIC/WS listeners + 3 external addrs + bumped
#      reservation cap so renewal storms don't trip ResourceLimitExceeded).
#   2. dx serve --platform web — wasm build of the example, served at :8080.
#   3. (optional, --android) dx serve --platform android — installs the APK
#      onto the connected `adb` device.
#
# Live-tails `WebSyncClient:` lines from the web log so the diagnostic
# patterns added in `wavesyncdb/src/web_engine.rs` (connected count,
# pushing → N peers, send_request, OutboundFailure) show up as soon as
# they fire.
#
# Usage:
#   ./test.sh             # relay + web (manual driving)
#   ./test.sh --android   # also install the Android side
#   ./test.sh --full      # autonomous end-to-end: relay + web + Android +
#                         #   maestro (drives phone) + puppeteer (drives
#                         #   web). Pass-fail at the end.
#   ./test.sh --stop      # stop everything started by a previous run
#
# In --full mode:
#   1. Relay + web build come up the same as the default mode.
#   2. Android APK is installed on the connected `adb` device.
#   3. maestro pairs the phone, waits for the Tasks panel.
#   4. As soon as maestro confirms Tasks is up, the web driver
#      (web_driver.js) opens the page in headless Chrome and clicks Add
#      with text 'from-web'.
#   5. maestro waits up to 60s for 'from-web' to appear on the phone.
#   6. Pass = real-time web→phone push works. Fail = it doesn't, in
#      which case the diagnostic logs in $LOGDIR/web.log show whether
#      web's `connected` set was empty, the send_request fired, etc.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
LOGDIR="$HERE/.test-logs"
PIDDIR="$HERE/.test-pids"
mkdir -p "$LOGDIR" "$PIDDIR"

# Identity-keypair pinned so the relay's PeerId is stable across runs —
# matches DEFAULT_RELAY_PEER_ID in src/web_app.rs and RELAY_PEER_ID in
# src/native_app.rs.
RELAY_KEY='CAESQGlCc264ZKF3D4l/5VXTLjnGdDKxg0cyX2UosIkZmNAbxV5oeISRfEDIrc/+hdQuqepe9CCCc3M5G3DJBs6N6lE='

# Web server port. dx serve defaults to 8080 + 127.0.0.1 — both wrong for
# this test: 8080 is commonly held by another dev server (a leftover
# `dx serve --platform android` will sit on it for a session), and a
# 127.0.0.1 bind is unreachable from a phone on the LAN. We pin to a
# less-trafficked port and bind 0.0.0.0 so the phone can dial the dev
# machine over wifi. Override via WEB_PORT env var if 8087 collides.
WEB_PORT="${WEB_PORT:-8087}"

stop_all() {
    for pidfile in "$PIDDIR"/*.pid; do
        [[ -f "$pidfile" ]] || continue
        local name pid
        name="$(basename "$pidfile" .pid)"
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            # Kill the whole process group — `dx serve` and `cargo run`
            # spawn children that don't exit when the parent does.
            kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
            echo "stopped $name (pid=$pid)"
        fi
        rm -f "$pidfile"
    done
}

if [[ "${1:-}" == "--stop" ]]; then
    stop_all
    exit 0
fi

# Idempotent: always tear down before starting.
stop_all

# Detect LAN IP. Prefer 192.168.x.x (the network the user's phone is on
# for this setup), fall back to first non-loopback global address.
LAN_IP="$(ip -4 -o addr show scope global 2>/dev/null \
    | awk '{print $4}' | cut -d/ -f1 \
    | grep -E '^192\.168\.' | head -1 || true)"
if [[ -z "$LAN_IP" ]]; then
    LAN_IP="$(ip -4 -o addr show scope global 2>/dev/null \
        | awk '{print $4}' | cut -d/ -f1 | head -1 || true)"
fi
if [[ -z "$LAN_IP" ]]; then
    echo "WARNING: no LAN IP detected — using 127.0.0.1 (a real phone won't reach this)"
    LAN_IP="127.0.0.1"
fi

# Sync .env so build.rs bakes the correct host into the next native build.
# This is what `option_env!("WAVESYNC_RELAY_HOST")` reads in
# src/native_app.rs::RELAY_HOST.
echo "WAVESYNC_RELAY_HOST=$LAN_IP" > "$HERE/.env"
echo "LAN IP: $LAN_IP  (written to $HERE/.env)"

# ── Start relay ──────────────────────────────────────────────────────
echo "Starting relay…"
(
    cd "$ROOT"
    setsid env RUST_LOG=info cargo run --quiet -p wavesync_relay -- \
        --identity-keypair="$RELAY_KEY" \
        --listen-addr /ip4/0.0.0.0/tcp/4001 \
        --ws-listen-addr /ip4/0.0.0.0/tcp/4002/ws \
        --external-address "/ip4/$LAN_IP/tcp/4001" \
        --external-address "/ip4/$LAN_IP/udp/4001/quic-v1" \
        --external-address "/ip4/$LAN_IP/tcp/4002/ws" \
        --max-reservations-per-peer 256 \
        </dev/null >"$LOGDIR/relay.log" 2>&1 &
    echo $! > "$PIDDIR/relay.pid"
)

# Block until the relay prints its first listen line.
until grep -q "Also listening on WebSocket" "$LOGDIR/relay.log" 2>/dev/null; do
    if ! kill -0 "$(cat "$PIDDIR/relay.pid")" 2>/dev/null; then
        echo "Relay died early; tail of log:"
        tail -30 "$LOGDIR/relay.log"
        exit 1
    fi
    sleep 0.5
done
echo "  relay up (logs: $LOGDIR/relay.log)"

# ── Start web (dx serve --platform web) ──────────────────────────────
# Sanity-check the port before launching dx — dx silently falls back to
# a random ephemeral port when its requested port is busy, which would
# leave us claiming "served at $WEB_PORT" while it's actually elsewhere.
if ss -tlnH "sport = :$WEB_PORT" 2>/dev/null | grep -q .; then
    echo "ERROR: port $WEB_PORT already in use. Set WEB_PORT=<n> and retry, or stop:"
    ss -tlnpH "sport = :$WEB_PORT" 2>/dev/null
    stop_all
    exit 1
fi

echo "Starting web build on 0.0.0.0:$WEB_PORT (this takes 30–60s on a clean target)…"
(
    cd "$HERE"
    setsid env RUST_LOG=info,wavesyncdb=info dx serve --platform web \
        --addr 0.0.0.0 --port "$WEB_PORT" \
        </dev/null >"$LOGDIR/web.log" 2>&1 &
    echo $! > "$PIDDIR/web.pid"
)

until grep -qE "Build completed successfully|launching app" "$LOGDIR/web.log" 2>/dev/null; do
    if ! kill -0 "$(cat "$PIDDIR/web.pid")" 2>/dev/null; then
        echo "dx serve --platform web died; tail of log:"
        tail -30 "$LOGDIR/web.log"
        exit 1
    fi
    sleep 1
done
echo "  web served at http://$LAN_IP:$WEB_PORT (logs: $LOGDIR/web.log)"

# ── Optional Android ─────────────────────────────────────────────────
if [[ "${1:-}" == "--android" || "${1:-}" == "--full" ]]; then
    if ! adb get-state >/dev/null 2>&1; then
        echo "No adb device connected — skipping Android"
    elif [[ "${SKIP_ANDROID_BUILD:-}" == "1" ]] \
        && adb shell pm list packages 2>/dev/null | grep -q "package:com.wavesync.qr_pairing"; then
        echo "SKIP_ANDROID_BUILD=1 and app is already installed — using existing APK"
    else
        echo "Building + installing Android (this takes 1–3 min on a clean target)…"
        (
            cd "$HERE"
            setsid env RUST_LOG=info dx serve --platform android \
                </dev/null >"$LOGDIR/android.log" 2>&1 &
            echo $! > "$PIDDIR/android.pid"
        )
        until grep -qE "Build completed successfully|app launched|launching app" \
            "$LOGDIR/android.log" 2>/dev/null; do
            if ! kill -0 "$(cat "$PIDDIR/android.pid")" 2>/dev/null; then
                echo "dx serve --platform android died; tail of log:"
                tail -30 "$LOGDIR/android.log"
                exit 1
            fi
            sleep 2
        done
        echo "  Android installed (logs: $LOGDIR/android.log; logcat: adb logcat -s WaveSyncDB)"
    fi
fi

# ── --full: orchestrate end-to-end pass/fail ─────────────────────────
if [[ "${1:-}" == "--full" ]]; then
    MAESTRO_BIN="${MAESTRO_BIN:-$HOME/.maestro/bin/maestro}"
    if [[ ! -x "$MAESTRO_BIN" ]]; then
        echo "ERROR: maestro not found at $MAESTRO_BIN. Install via 'curl -Ls \"https://get.maestro.mobile.dev\" | bash' or set MAESTRO_BIN=<path>."
        stop_all
        exit 1
    fi
    if [[ ! -d "$HERE/node_modules/puppeteer-core" ]]; then
        echo "ERROR: puppeteer-core not installed. Run 'npm install --save-dev puppeteer-core' in $HERE."
        stop_all
        exit 1
    fi

    cat <<EOF

=== Bidirectional driving ===
  Phone (maestro):
    1. pair, 2. wait 'from-web' (web→phone proves), 3. type 'from-phone' + Add
  Web (web_driver.js):
    1. open page, 2. wait >=1 connected peer, 3. type 'from-web' + Add,
    4. wait 'from-phone' to land in DOM (phone→web proves)

EOF

    # Capture phone-side logs in parallel so we can correlate with the
    # web-side diagnostics. clear-then-tail avoids any stale chunks
    # from a previous run muddying the pass/fail post-mortem.
    adb logcat -c 2>/dev/null || true
    setsid adb logcat -v time WaveSyncDB:V '*:S' \
        </dev/null >"$LOGDIR/phone.log" 2>&1 &
    LOGCAT_PID=$!
    echo $LOGCAT_PID > "$PIDDIR/logcat.pid"

    # Drive the phone in background. The `from-web` wait at the end is
    # what the web side will satisfy once we trigger it below.
    setsid "$MAESTRO_BIN" test --no-ansi "$HERE/test.maestro.yaml" \
        </dev/null >"$LOGDIR/maestro.log" 2>&1 &
    MAESTRO_PID=$!
    echo $MAESTRO_PID > "$PIDDIR/maestro.pid"

    # Wait for the phone to reach the Tasks view (i.e. paired and on the
    # topic mesh). After this point the web's Push fan-out has someone
    # to deliver to. Poll the maestro log; bail if maestro dies first.
    echo "  waiting for phone to pair…"
    until grep -q '"Tasks" is visible... COMPLETED' "$LOGDIR/maestro.log" 2>/dev/null; do
        if ! kill -0 "$MAESTRO_PID" 2>/dev/null; then
            echo "  maestro died before phone paired — last lines:"
            tail -40 "$LOGDIR/maestro.log"
            stop_all
            exit 1
        fi
        sleep 1
    done
    echo "  phone paired and on the topic mesh"

    # Drive the web side. web_driver.js types 'from-web', then waits
    # for 'from-phone' to appear on the page so it asserts both
    # directions before exiting.
    echo "  adding 'from-web' on the web side, waiting for phone→web 'from-phone'"
    setsid env WEB_URL="http://$LAN_IP:$WEB_PORT" \
        TASK_TEXT=from-web EXPECT_TASK_TEXT=from-phone HEADLESS=1 \
        node "$HERE/web_driver.js" \
        </dev/null >"$LOGDIR/web_driver.log" 2>&1 &
    DRIVER_PID=$!
    echo $DRIVER_PID > "$PIDDIR/driver.pid"

    # Block on both sides. Maestro proves web→phone (it asserts
    # 'from-web' visible, then types 'from-phone'). web_driver proves
    # phone→web (it waits for 'from-phone' in its DOM after typing
    # 'from-web' itself). Both must exit 0 for a green run.
    MAESTRO_RC=0
    DRIVER_RC=0
    wait "$MAESTRO_PID" || MAESTRO_RC=$?
    wait "$DRIVER_PID"  || DRIVER_RC=$?

    EXIT=0
    echo ""
    if [[ $MAESTRO_RC -eq 0 ]]; then
        echo "  ✓ web→phone — phone saw 'from-web' (maestro)"
    else
        echo "  ✗ web→phone — phone never saw 'from-web' (maestro rc=$MAESTRO_RC)"
        EXIT=1
    fi
    if [[ $DRIVER_RC -eq 0 ]]; then
        echo "  ✓ phone→web — web saw 'from-phone' (web_driver)"
    else
        echo "  ✗ phone→web — web never saw 'from-phone' (web_driver rc=$DRIVER_RC)"
        EXIT=1
    fi

    if [[ $EXIT -eq 0 ]]; then
        echo ""
        echo "=== ✓ PASS — both directions sync in real time ==="
    else
        echo ""
        echo "=== ✗ FAIL ==="
        if [[ $MAESTRO_RC -ne 0 ]]; then
            echo ""
            echo "Maestro tail:"
            tail -25 "$LOGDIR/maestro.log"
        fi
        if [[ $DRIVER_RC -ne 0 ]]; then
            echo ""
            echo "web_driver tail:"
            tail -25 "$LOGDIR/web_driver.log" | grep -v "Multiplexed\|with_other_transport\|::start::"
        fi
        echo ""
        echo "Web wasm diagnostics (look for 'pushing changeset to N peers' and 'connected count'):"
        grep -E "WebSyncClient:" "$LOGDIR/web_driver.log" \
            | grep -vE "Multiplexed|with_other_transport|::start::" \
            | sed -E 's/.*WebSyncClient: //;s/;\s+color:.*//' | tail -20
    fi

    # Tear down logcat tail — leave the rest of the env up unless the
    # user explicitly stops it. They may want to inspect logs.
    kill -TERM -- "-$LOGCAT_PID" 2>/dev/null || kill -TERM "$LOGCAT_PID" 2>/dev/null || true
    rm -f "$PIDDIR/driver.pid" "$PIDDIR/maestro.pid" "$PIDDIR/logcat.pid"
    echo ""
    echo "Logs preserved in $LOGDIR/. Stop everything with '$0 --stop'."
    exit $EXIT
fi

cat <<EOF

=== Ready ===
  Web URL:    http://$LAN_IP:$WEB_PORT
  Phone:      $([[ "${1:-}" == "--android" ]] && echo "installed via dx (re-install with adb if needed)" || echo "(skipped — pass --android to build/install)")

  Logs:       $LOGDIR/{relay,web,android}.log
  Stop all:   $0 --stop

  Drive the phone: maestro test $HERE/test.maestro.yaml
  Drive the web:   open the URL, type 'from-web' in the Tasks panel, click Add
  Or full auto:    $0 --full

Live-tailing 'WebSyncClient:' from the web log (Ctrl+C stops the tail; the
processes keep running until you call '$0 --stop'):

EOF

# Foreground tail so the user sees the diagnostic lines as they fire.
# Filter to the high-signal patterns: connected/disconnected, push fan-out,
# send_request, outbound failures, panics.
exec tail -F "$LOGDIR/web.log" 2>/dev/null \
    | grep --line-buffered -E "WebSyncClient:|panicked|ERROR|connected to relay|connected to peer|disconnected from"
