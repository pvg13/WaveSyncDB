# iOS deployment templates

`Entitlements.template.plist` and `Info.template.plist` are the relay-only
starting point for shipping a WaveSyncDB-backed iOS app: WAN sync via the
circuit-relay + rendezvous server, background wake via silent APNs push, no
local-network/mDNS permissions.

Copy both into your app target, rename (drop `.template`), fill in the
`TEAMID` / bundle-id placeholders, and point `Dioxus.toml` at them
(`ios_entitlements = "Entitlements.plist"`, `ios_info_plist = "Info.plist"`)
or wire them into your own Xcode target if you're not using `dx`.

Each omission (local-network usage string, Bonjour services, the multicast
entitlement) is explained inline via an XML comment — read them before
deciding you need those keys back. The full walkthrough, including the .p8
APNs setup on the relay side and the `backgroundSyncTimeoutSecs` knob, is at
[`/docs/ios-deployment`](../../../website/content/docs/17-ios-deployment.md)
on the website.

For a LAN-discovery (mDNS) variant of these files — which additionally
requires Apple's case-by-case `com.apple.developer.networking.multicast`
entitlement — see `examples/dioxus_fcm_sync/{entitlements,info}.plist`.

## Notification Service Extension (alert-class pushes)

`Sources/WaveSyncPush/WaveSyncNotificationService.swift` is a **template**
for a `UNNotificationServiceExtension` that rewrites an alert-class push's
placeholder title/body with real content before it's shown — see the file's
header comment for the Xcode setup (it's its own `.appex` target; this
package can't build that for you). It calls into `wavesyncdb`'s
`wavesync_nse_handle_push`, which never runs the passphrase KDF (the
extension's memory cap can't afford it) — see `key_cache` module docs in the
Rust crate for the on-disk group-key cache that makes that possible, and
`WaveSyncDbBuilder::with_group_key_cache` for the opt-out.

Both the app and the extension need to agree on where the database lives —
share it via an [App
Group](https://developer.apple.com/documentation/xcode/configuring-app-groups)
container. `wavesync_app_group_container(groupId)` (Rust) /
`wavesync_app_group_container` (the `@_cdecl` it calls into, in
`WaveSyncPushBridge.swift`) resolves that container's path so both binaries
can point their `WaveSyncDbBuilder` at the same directory.

**If your app keeps the database somewhere other than the container root**
(e.g. a per-account subdirectory), the NSE has no way to know that on its
own — it's only ever handed the container root, never told the app's layout.
Bridge the gap with a pointer file: on every launch/login (whenever the
active account's directory is decided), write
`<container root>/.wavesync_config_dir` containing a single line — the
active config directory's path, RELATIVE to the container root (e.g.
`u/<user_id>` if that's where `.wavesync_config.json` lives). Overwrite it
every time, so switching accounts on the same device retargets the NSE
without reinstalling. `resolveConfigDir` in the Swift template reads this
pointer first and falls back to the container root if it's absent or
doesn't resolve to a directory that actually holds
`.wavesync_config.json` — so an app that keeps its database directly at the
root needs no pointer file at all.
