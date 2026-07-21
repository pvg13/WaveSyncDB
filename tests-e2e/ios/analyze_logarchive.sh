#!/usr/bin/env bash
# iOS observational-session analyzer (#109) — the "real flow" companion.
#
# The phone is used naturally (Mediterranea dev build with the m1-diag
# beacon enabled); at the end of the day it connects to the Mac ONCE:
#
#   sudo log collect --device-udid <udid> --last 8h --output session.logarchive
#   log show --archive session.logarchive --info --debug \
#       --predicate 'eventMessage CONTAINS "m1-diag" OR eventMessage CONTAINS "DIAG " OR eventMessage CONTAINS "QUIC listener" OR eventMessage CONTAINS "bg_sync" OR eventMessage CONTAINS "WaveSyncNSE" OR eventMessage CONTAINS "unspecified-listen override"' \
#       --style syslog > session.flat.log
#   ./analyze_logarchive.sh session.flat.log
#
# Produces summary.md next to the input: carrier classification from the
# beacon timeline, #73 arm markers, #74 handoff laps (device-clock deltas
# between listener departed/appeared pairs), bg_sync stage tables per
# wake, and NSE tier evidence. Timestamps come from the device's own
# clock (log show syslog style: "YYYY-MM-DD HH:MM:SS.ffffff+ZZZZ").
#
#   ./analyze_logarchive.sh --selftest     # parser checks, any OS

set -uo pipefail

# syslog-style timestamp of a line → epoch milliseconds (device clock).
line_epoch_ms() { # <line>
    python3 - "$1" <<'EOF'
import sys, datetime, re
m = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)([+-]\d{4})', sys.argv[1])
if not m:
    print(-1); sys.exit()
dt = datetime.datetime.strptime(m.group(1)[:26], "%Y-%m-%d %H:%M:%S.%f")
tz = m.group(2)
off = int(tz[:3]) * 3600 + int(tz[0] + tz[3:]) * 60
print(int((dt - datetime.timedelta(seconds=off)).replace(tzinfo=datetime.timezone.utc).timestamp() * 1000))
EOF
}

# All beacon lines → "epoch_ms ratio peers peers_via_relay dcutr" rows.
beacon_timeline() { # <flatlog>
    grep "m1-diag " "$1" | while IFS= read -r line; do
        local ms fields
        ms="$(line_epoch_ms "$line")"
        fields="$(sed -E 's/.*dcutr=([0-9]+\/[0-9]+).*ratio=([^ ]+) peers=([0-9]+) peers_via_relay=([0-9]+).*/\2 \3 \4 \1/' <<<"$line")"
        echo "$ms $fields"
    done
}

# Handoff laps: pair each "departed" with the NEXT "appeared"; print the
# device-clock delta per pair.
handoff_laps() { # <flatlog>
    grep -E "QUIC listener" "$1" | while IFS= read -r line; do
        local ms kind
        ms="$(line_epoch_ms "$line")"
        case "$line" in
            *"departed; removing"*) kind=D ;;
            *"appeared; binding"*)  kind=A ;;
            *) continue ;;
        esac
        echo "$ms $kind"
    done | awk '$2=="D"{d=$1} $2=="A"&&d{print "lap_ms=" $1-d; d=0}'
}

# bg_sync wakes: group stage lines into per-wake tables (a new wake starts
# at each config_loaded).
bg_sync_wakes() { # <flatlog>
    grep -oE 'bg_sync stage=[a-z_]+( elapsed_ms=[0-9]+)?( result=[a-z_]+)?' "$1" \
        | awk '/stage=config_loaded/{print "--- wake"} {print}'
}

selftest() {
    local t d; t="$(mktemp)"
    cat > "$t" <<'EOF'
2026-07-22 10:00:00.000000+0200 app[1]: m1-diag relayed_est=1 direct_est=0 demoted=0 dcutr=0/0 relay_bytes=9 direct_bytes=0 ratio=Some(1.0) peers=1 peers_via_relay=1
2026-07-22 10:00:05.500000+0200 app[1]: Network interface 10.0.0.5 departed; removing QUIC listener
2026-07-22 10:00:08.250000+0200 app[1]: Network interface 100.64.0.9 appeared; binding QUIC listener
2026-07-22 10:00:30.000000+0200 app[1]: m1-diag relayed_est=1 direct_est=1 demoted=1 dcutr=1/1 relay_bytes=9 direct_bytes=90 ratio=Some(0.09) peers=1 peers_via_relay=0
2026-07-22 10:01:00.000000+0200 app[1]: bg_sync stage=config_loaded elapsed_ms=100
2026-07-22 10:01:09.000000+0200 app[1]: bg_sync stage=done elapsed_ms=9000 result=synced
EOF
    local fails=0
    local laps; laps="$(handoff_laps "$t")"
    [[ "$laps" == "lap_ms=2750" ]] || { echo "FAIL handoff_laps: '$laps'"; fails=1; }
    local rows; rows="$(beacon_timeline "$t" | wc -l | tr -d ' ')"
    [[ "$rows" == "2" ]] || { echo "FAIL beacon_timeline rows: $rows"; fails=1; }
    local last; last="$(beacon_timeline "$t" | tail -1 | cut -d' ' -f2-)"
    [[ "$last" == "Some(0.09) 1 0 1/1" ]] || { echo "FAIL beacon last: '$last'"; fails=1; }
    local wakes; wakes="$(bg_sync_wakes "$t" | grep -c '^--- wake')"
    [[ "$wakes" == "1" ]] || { echo "FAIL bg_sync wakes: $wakes"; fails=1; }
    rm -f "$t"
    ((fails == 0)) && echo "selftest OK" || exit 1
}
if [[ "${1:-}" == "--selftest" ]]; then selftest; exit 0; fi

FLAT="${1:?usage: analyze_logarchive.sh <session.flat.log> (see header for the log show command)}"
[[ -f "$FLAT" ]] || { echo "ERROR: $FLAT not found"; exit 2; }
OUT="$(dirname "$FLAT")/summary.md"

{
    echo "# iOS observational session — analysis of $(basename "$FLAT")"
    echo
    echo "## Beacon timeline (S1/#77 classification: epoch_ms ratio peers peers_via_relay dcutr)"
    echo '```'
    beacon_timeline "$FLAT"
    echo '```'
    echo
    echo "## #73 bind-arm markers"
    if grep -q "unspecified-listen override ACTIVE" "$FLAT"; then
        echo "- unspecified arm was ACTIVE during (portions of) this capture:"
        grep "unspecified-listen override ACTIVE" "$FLAT" | head -5 | sed 's/^/  - /'
    else
        echo "- concrete-bind arm only (no override marker in capture)"
    fi
    grep "DIAG listen_on(QUIC) returned in" "$FLAT" | tail -5 | sed 's/^/- /'
    echo
    echo "## #74 handoff laps (departed → next appeared, device clock)"
    echo '```'
    handoff_laps "$FLAT"
    echo '```'
    echo "(bar: ≤3–4s total to first sync ⇒ #74 can close wontfix-with-rationale)"
    echo
    echo "## S5 — bg_sync wakes"
    echo '```'
    bg_sync_wakes "$FLAT"
    echo '```'
    echo "(cross-check the relay log for sends this capture window: coalesced/budget_denied sends are not trials)"
    echo
    echo "## #92 — NSE evidence"
    if grep -q "WaveSyncNSE" "$FLAT"; then
        grep "WaveSyncNSE" "$FLAT" | sed 's/^/- /'
    else
        echo "- no [WaveSyncNSE] lines in capture (NSE logs only appear when alert pushes arrived in the window)"
    fi
} > "$OUT"

echo "summary written: $OUT"
