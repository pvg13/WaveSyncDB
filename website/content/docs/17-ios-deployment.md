# iOS deployment

iOS has three things a LAN-only (Android/desktop) deployment doesn't: an
entitlements file, an APNs push credential (separate from FCM), and a hard
OS-imposed background-execution budget. This page covers all three for the
**relay-only** deployment shape — WAN sync via circuit-relay + rendezvous,
background wake via silent APNs push, no local-network/mDNS permissions.

Templates for the two plist files below live at
`wavesyncdb/src/ios/{Entitlements,Info}.template.plist` in the repo.

## Entitlements matrix

| Capability | Key | Where | Why |
|---|---|---|---|
| App identity | `application-identifier` | Entitlements | Must match your provisioning profile's bundle id. |
| Team identity | `com.apple.developer.team-identifier` | Entitlements | Your Apple Developer team id. |
| APNs environment | `aps-environment` | Entitlements | `development` (sandbox gateway) or `production` (live gateway) — must match which APNs gateway the relay's `APNS_SANDBOX` flag targets, or tokens fail with `InvalidProviderToken`/`BadDeviceToken`. |
| Debugger attach | `get-task-allow` | Entitlements | Dev-only convenience; Xcode strips it from release archives. |
| Keychain sharing | `keychain-access-groups` | Entitlements | Only needed if you share credentials across app + extension targets. |
| Background wake on push | `UIBackgroundModes` = `["remote-notification"]` | Info.plist | Lets a silent APNs push (`content-available: 1`) wake the app in the background to run a catch-up sync. Without it, background sync silently never runs — no error, no log. |
| Local-network access | `NSLocalNetworkUsageDescription` | Info.plist | **Omitted in the relay-only template.** Only needed for same-Wi-Fi mDNS discovery. |
| Bonjour service browsing | `NSBonjourServices` | Info.plist | **Omitted in the relay-only template.** Only needed alongside the local-network key, for the same reason. |
| Multicast/broadcast | `com.apple.developer.networking.multicast` | Entitlements | **Omitted.** Required for libp2p's mDNS responder to work on a physical device (Simulator works without it — a trap). Apple grants this case-by-case; it is not self-service. See "When to add local-network keys" below. |

## When you WOULD add the local-network keys

Add `NSLocalNetworkUsageDescription` + `NSBonjourServices` back, and request
the multicast entitlement from Apple, only if your deployment also does
same-Wi-Fi LAN discovery (mDNS) rather than relying purely on relay/WAN sync.
The relay-only template ships without them because:

1. WAN sync doesn't use mDNS at all — it goes through the circuit-relay and
   rendezvous server, neither of which touches the local-network APIs those
   keys gate.
2. The multicast entitlement is Apple-approval-gated and not guaranteed, so
   depending on mDNS as your only discovery path is a deployment risk on iOS
   specifically (it is not gated the same way on Android or desktop).
3. Requesting a permission you don't use costs a permission prompt on first
   launch for no benefit.

If you do add them, use `examples/dioxus_fcm_sync/{entitlements,info}.plist`
in the repo as the LAN-enabled reference — it declares libp2p's canonical
`_p2p._udp` mDNS service type in `NSBonjourServices`.

## APNs `.p8` setup on the relay

The relay (`wavesync_relay`) needs four pieces of APNs configuration, read
from CLI flags or the matching environment variables:

| Env var | CLI flag | Required | Notes |
|---|---|---|---|
| `APNS_KEY_FILE` | `--apns-key-file` | yes (for APNs) | The `.p8` signing key — either inline PEM (starting with `-----BEGIN`) or a filesystem path. Auto-discovered from `/run/secrets/apns.p8` if unset. |
| `APNS_KEY_ID` | `--apns-key-id` | yes | The key id shown next to the `.p8` key in App Store Connect. |
| `APNS_TEAM_ID` | `--apns-team-id` | yes | Your Apple Developer team id — the same value as the entitlements' `com.apple.developer.team-identifier`. |
| `APNS_BUNDLE_ID` | `--apns-bundle-id` | yes | Your app's bundle id, e.g. `com.example.myapp`. |
| `APNS_SANDBOX` | `--apns-sandbox` | no (bool flag) | Set to target APNs' sandbox gateway (development builds). Unset for production. Must agree with the entitlements' `aps-environment`. |

Steps:

1. In App Store Connect → Certificates, Identifiers & Profiles → Keys, create
   a new key with the **Apple Push Notifications service (APNs)** capability
   enabled. Download the `.p8` file once — Apple does not let you re-download it.
2. Note the **Key ID** (shown on the key's detail page) and your **Team ID**
   (top-right of the Developer account page).
3. Place the `.p8` file where the relay can read it — under Docker Compose,
   drop it at `secrets/apns.p8` (mounted to `/run/secrets/apns.p8`, the
   `APNS_KEY_FILE` default). See [Relay deployment](/docs/relay-deployment).
4. Set `APNS_TEAM_ID`, `APNS_BUNDLE_ID`, and (for non-production builds)
   `APNS_SANDBOX=1` alongside it.
5. Set `aps-environment` in the client's Entitlements.plist to match:
   `development` while `APNS_SANDBOX` is set, `production` once you drop it
   for a release build.

The push itself is a **silent, high-priority background notification**:
`apns-push-type: background`, `apns-priority: 5`, `content-available: 1`, and
no user-visible alert text — it exists only to wake the app, never to display
anything. Delivery is best-effort like all silent pushes; the normal periodic
catch-up sync is the backstop if one is ever dropped.

## Background modes rationale

`UIBackgroundModes = ["remote-notification"]` is the only background mode
this library needs, and it is declared explicitly in the Info.plist template
rather than left to a build tool's implicit background-modes mapping — a
build that silently ships without this key produces exactly the failure mode
that's hardest to diagnose (background sync stops working with zero errors
and zero log lines, because iOS never delivers the wake in the first place).

`processing` / `fetch` background modes are deliberately **not** requested:
iOS does not guarantee opportunistic background execution windows for either,
so this library's design relies entirely on the silent-push wake rather than
`BGTaskScheduler`. Add one of those modes yourself only if you introduce a
best-effort periodic top-up sync on your own.

## The `backgroundSyncTimeoutSecs` knob

`WaveSyncPushHandler.backgroundSyncTimeoutSecs` (Swift, `public static var`,
default `25`) is the budget passed to the Rust background-sync FFI call when
a silent push wakes the app. iOS grants roughly 30 seconds of background
execution time after a background notification; the 25s default leaves ~5s
of headroom for the sync engine to shut down cleanly and for the completion
handler round trip back to UIKit.

This is a host-app-tunable **static var**, not a compile-time constant —
override it once at app startup if on-device measurement (see
`docs/ios-device-protocol-2026-07.md` in the repo) shows your actual grant is
meaningfully different from ~30s:

```swift
WaveSyncPushHandler.backgroundSyncTimeoutSecs = 20 // after measuring a tighter grant
```

On the Rust side, `background_sync`'s internal timers scale to whatever
timeout value is passed in — a shorter grant doesn't just reduce the top-level
deadline, it also shrinks the fallback and completion-grace windows so they
still leave room to run before the hard cutoff. At the default 25s value both
timers keep their historical fixed values — this scaling is a zero-behavior-
change no-op at defaults, only mattering if you tune the knob down.

## APNs budget and coalescing behavior

APNs throttles silent background pushes to a small number per app per day —
in practice a handful, not dozens. A burst of writes on one peer must not
translate into a burst of wake pushes to every other device, or you exhaust
that daily allowance in minutes.

The relay coalesces per-**device** wake pushes within a configurable window:
a burst of writes to the same topic, within the coalescing window of a given
device's last wake, costs that device **one** push, not one per write.

| Env var | CLI flag | Default | Platform |
|---|---|---|---|
| `APNS_COALESCE_SECS` | `--apns-coalesce-secs` | `900` (15 min) | APNs |
| `FCM_COALESCE_SECS` | `--fcm-coalesce-secs` | `0` (disabled) | FCM |

APNs defaults to a 15-minute coalescing window — against a ~5-pushes/day
budget, that's roughly one wake per 3 waking hours, which comfortably absorbs
a burst without starving the device of wakes entirely. FCM has no comparable
hard daily cap, and the existing topic-keyed send debounce (`PUSH_DEBOUNCE_SECS`,
default 1s, smooths bursts across *all* devices) is already enough, so
FCM's per-device window defaults to disabled.

A suppressed (coalesced) send is not a failure: push is only a best-effort
wake hint, and the normal periodic catch-up sync delivers the data on the
device's next wake or foreground open regardless. Coalesced sends are logged
and counted separately (`relay_pushes_sent_total{outcome="coalesced"}` on the
relay's `/metrics` endpoint) so you can see how often the window is doing its
job without it looking like a delivery failure.

**The daily budget above governs the *silent* class only.** A changeset that
touches a `SyncNotify`-visible table wakes iOS peers with an unbudgeted
ALERT-class push instead — it skips `APNS_COALESCE_SECS` and the daily cap
entirely, on the reasoning that a user-relevant change should never wait
behind a throttle built for non-visible background wakes. Alerts get their
own, much shorter, anti-spam window instead: see [Notification Service
Extension](#notification-service-extension-nse) below.

## Notification Service Extension (NSE)

A `visible: true` push (any changeset touching a `SyncNotify`-visible table)
sends `mutable-content: 1` alongside the usual `content-available: 1` — so an
app with no NSE at all keeps working exactly as described above (background
sync runs, the user sees the relay operator's placeholder banner), while an
app that ships a Notification Service Extension gets a chance to rewrite that
banner with real, on-device-composed content before it's ever shown.

**What the NSE does, precisely:** iOS launches it in place of ordinary
delivery whenever `mutable-content: 1` is present. The extension calls into
`wavesyncdb`'s `wavesync_nse_handle_push(config_dir, payload_json,
budget_secs)`, which runs a short one-shot sync scoped to just the group named
in the push, and — if a `SyncNotify` policy fired for whatever landed — hands
back that notification's title/body as JSON. The Swift template
(`wavesyncdb/src/ios/Sources/WaveSyncPush/WaveSyncNotificationService.swift`)
rewrites the banner with it; on timeout, a cold key cache, or nothing
notify-worthy, it leaves the operator's placeholder untouched. Either way the
user sees a notification — this is a content upgrade, never a dependency the
sync itself relies on.

**App Group setup.** The NSE is its own process and must open the exact same
SQLite database as the app, so both need to agree on a data directory shared
through an [App
Group](https://developer.apple.com/documentation/xcode/configuring-app-groups)
container:

1. Add the **App Groups** capability to the *app's* App ID, with a group id
   like `group.com.example.myapp`, and add the matching entitlement to the
   app target.
2. The extension is its own App ID too (e.g. `com.example.myapp.nse`,
   distinct from the app's) — give it its own App Groups capability, for the
   SAME group id, and its own entitlement. A profile is scoped to one App ID:
   the app's provisioning profile cannot sign the extension, and vice versa —
   plan on requesting/regenerating a separate profile for the NSE's App ID.
3. At runtime, both binaries resolve the shared directory the same way: call
   `wavesync_app_group_container(group_id)` (the Rust wrapper in `ffi.rs`,
   backed by a `wavesync_app_group_container` `@_cdecl` in
   `WaveSyncPushBridge.swift`) and point `WaveSyncDbBuilder`'s directory at
   the result instead of the app's private container. If your app predates
   this and already has data in its private container, that's a one-time
   migration your app owns (move the old directory into the group container
   on first launch of the App-Group-enabled build) — `wavesyncdb` has no
   opinion on how you do that migration, only on where the two binaries end
   up pointing afterward.

**Appex assembly.** `dx`/`xcodebuild` cannot generate a Notification Service
Extension target for you — it's its own `.appex` executable, not something
either build tool creates automatically (see `wavesyncdb/src/ios/README.md`).
Ship it via your own build script, run before your normal sign/install step:
build a static library for `aarch64-apple-ios` that links the crates
registering your `SyncNotify` policies (so the inventory the NSE reads at
runtime is actually populated), compile
`WaveSyncNotificationService.swift` (copied out of the template and
subclassed with your `appGroupId`) against it, assemble an `Info.plist` with
`NSExtensionPointIdentifier = com.apple.usernotifications.service`, and copy
the result into `YourApp.app/PlugIns/` before your existing signing pass —
your signing script needs to also sign the appex bundle, and separately embed
the NSE's own provisioning profile into it
(`PlugIns/*.appex/embedded.mobileprovision`), since it has its own App ID and
can't reuse the app's profile. A build-flag pattern like
`WITH_NSE=1 NSE_PROFILE=/path/to/nse.mobileprovision ./your_sign_script.sh`
(opt-in, off by default) keeps a broken or not-yet-provisioned NSE build from
ever blocking a plain app install — the app works fully without it, per the
budget-section note above.

**Key-cache tradeoff.** The NSE's ~24 MB memory ceiling can't afford the
group key's Argon2id derivation (~19 MiB by design — see
[Authentication](/docs/authentication)), so it never runs the KDF at all: it can only
load a key the foreground app already derived and cached to disk at
`build()`/`join_group()` time
(`WaveSyncDbBuilder::with_group_key_cache`, default `true` on iOS, no-op
elsewhere). That means a copy of each group's raw 32-byte key sits on disk
(data-protected, `NSFileProtectionCompleteUntilFirstUserAuthentication`) for
as long as the app remains joined to that group — the same tradeoff any
end-to-end-encrypted app with a notification extension makes. If your threat
model forbids caching key material to disk, call
`with_group_key_cache(false)`; the NSE then always falls straight through to
the placeholder banner (safe, just less rich), and nothing else changes.

**Relay config for alert-class pushes:**

| Env var | CLI flag | Default | Notes |
|---|---|---|---|
| `APNS_ALERT_TITLE` | `--apns-alert-title` | `Nueva actividad` | The ONLY user-facing text on an alert push — relay-operator branding, never client-supplied. Real content is composed on-device by the NSE (or stays this placeholder without one). |
| `ALERT_COALESCE_SECS` | `--alert-coalesce-secs` | `30` | Per-device wake-coalescing window for the unbudgeted alert class — independent of `APNS_COALESCE_SECS`, and deliberately much shorter (a real-time banner should still feel real-time). |

## Further reading

- [Mobile & push notifications](/docs/mobile-and-push) — the general FCM/APNs
  architecture and the background-sync stage log lines.
- [Relay deployment](/docs/relay-deployment) — Docker Compose, secrets
  mounts, and the full relay environment-variable reference.
- `docs/ios-device-protocol-2026-07.md` in the repo — the on-device
  measurement protocol used to validate the QUIC bind strategy, DCUtR
  hole-punch behavior, and the actual background-execution grant on real
  hardware.
