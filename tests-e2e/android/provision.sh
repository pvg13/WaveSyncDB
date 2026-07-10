#!/usr/bin/env bash
# Idempotent Android-emulator provisioning for the WAN scenarios.
#
# If any adb device is already online, use it (never disturb a running
# emulator — it may be the user's). Otherwise create a dedicated
# `wavesync-e2e` AVD from the Play-services image (FCM needs Google
# Play Services; `default` images silently break push delivery) and
# boot it headless.
#
# Usage:
#   ./provision.sh          # ensure a device is available, print serial
#   ./provision.sh --fresh  # force-create + boot the wavesync-e2e AVD
#                           # even if another device is online

set -euo pipefail
source "$(dirname "$0")/common.sh"

IMAGE="system-images;android-36.1;google_apis_playstore;x86_64"
AVD_NAME="wavesync-e2e"

require adb "Install Android SDK platform-tools."

if [[ "${1:-}" != "--fresh" ]]; then
    existing="$(adb devices | awk 'NR>1 && $2=="device" {print $1; exit}')"
    if [[ -n "$existing" ]]; then
        echo "device already online: $existing"
        exit 0
    fi
fi

require avdmanager "Install Android cmdline-tools (sdkmanager 'cmdline-tools;latest')."
require emulator   "Install the Android emulator (sdkmanager 'emulator')."

# Ensure the system image exists (sdkmanager is a no-op when installed).
if [[ ! -d "$ANDROID_HOME/system-images/android-36.1/google_apis_playstore" ]]; then
    require sdkmanager "Install Android cmdline-tools."
    echo "==> Installing $IMAGE"
    yes | sdkmanager "$IMAGE"
fi

if ! avdmanager list avd -c 2>/dev/null | grep -qx "$AVD_NAME"; then
    echo "==> Creating AVD $AVD_NAME"
    echo no | avdmanager create avd -n "$AVD_NAME" -k "$IMAGE" --device pixel_7
fi

echo "==> Booting $AVD_NAME headless"
setsid emulator -avd "$AVD_NAME" -no-window -no-audio -no-snapshot -no-boot-anim \
    > /tmp/wavesync-e2e-emulator.log 2>&1 &

# Wait for the device, then for full boot.
adb wait-for-device
for i in {1..300}; do
    if [[ "$(adb shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; then
        serial="$(adb devices | awk 'NR>1 && $2=="device" {print $1; exit}')"
        echo "booted: $serial (after ${i}s)"
        exit 0
    fi
    sleep 1
done
echo "ERROR: emulator did not finish booting within 300s" >&2
exit 1
