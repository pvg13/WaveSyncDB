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
