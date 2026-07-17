#!/usr/bin/env bash
# Network-path control for the Android WAN scenarios.
#
# Real WAN failures are connectivity EVENTS (WiFi↔cellular handoff,
# elevator/tunnel blackout), not steady-state shaping — these toggles
# produce genuine guest-side interface migrations, which is the one
# N14-family condition the Docker harness cannot model (cgroup pause
# preserves the network path; the phone's OS does not).
#
# Usage: ./netctl.sh <wifi on|wifi off|data on|data off|
#                     airplane on|airplane off|
#                     flip-to-cellular|restore|status>
#
# Honors ANDROID_SERIAL; defaults to the first online device.

set -euo pipefail
source "$(dirname "$0")/common.sh"

require adb "Install Android SDK platform-tools."
detect_device

sh() { adb -s "$ANDROID_SERIAL" shell "$@"; }

case "${1:-status} ${2:-}" in
    "wifi on")      sh svc wifi enable ;;
    "wifi off")     sh svc wifi disable ;;
    "data on")      sh svc data enable ;;
    "data off")     sh svc data disable ;;
    "airplane on")  sh cmd connectivity airplane-mode enable ;;
    "airplane off") sh cmd connectivity airplane-mode disable ;;
    "flip-to-cellular ")
        # The WiFi→cellular handoff: drop wlan0, keep the radio. The
        # guest's sockets on wlan0 die; anything bound unspecified must
        # migrate to rmnet — exactly a phone walking out of WiFi range.
        sh svc data enable
        sh svc wifi disable
        ;;
    "restore ")
        sh cmd connectivity airplane-mode disable
        sh svc wifi enable
        sh svc data enable
        ;;
    "status ")
        echo "— transports —"
        sh dumpsys connectivity | grep -E "Active default network|NetworkAgentInfo.*(WIFI|CELLULAR)" | head -5
        echo "— interfaces —"
        sh ip -4 -o addr show | awk '{print $2, $4}'
        ;;
    *)
        echo "usage: $0 <wifi on|wifi off|data on|data off|airplane on|airplane off|flip-to-cellular|restore|status>" >&2
        exit 2
        ;;
esac
