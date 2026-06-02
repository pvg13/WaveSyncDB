# Wi-Fi Aware — research notes

Status: **investigation only**. Not implemented. Bookmarked for a future
workstream once the lower-effort wins from this branch are validated in
production.

## What it is

Wi-Fi Aware (formerly Neighbor Awareness Networking / NAN) is a Wi-Fi
Alliance standard for infrastructure-free peer-to-peer Wi-Fi. Two devices
discover and exchange data **without joining a Wi-Fi network at all** — no
AP, no router, no internet. The radio negotiates a small "discovery"
window plus on-demand data paths.

Throughput is real Wi-Fi (tens to hundreds of Mbps), range is roughly the
same as the Wi-Fi radio (~50 m line of sight), and the protocol does its
own peer-to-peer encryption.

Use cases this would unlock for WaveSyncDB:

- Two phones in a meeting room with no Wi-Fi at all sync edits.
- Two devices in a moving vehicle (no LTE) keep notes in sync.
- Field workers in a no-signal area collaborate on a shared dataset.
- Privacy-sensitive contexts where users explicitly want no
  infrastructure between their devices.

Today these scenarios silently fall back to "no sync until you find Wi-Fi
or cellular." mDNS (commit A2) handles same-Wi-Fi; Wi-Fi Aware handles
no-Wi-Fi.

## Platform status

### iOS

Apple shipped Wi-Fi Aware as a first-class framework at **WWDC 2025**
(iOS 26+). API: `WiFiAware` framework with `WAPairingHandler`,
`WAPairingSession`, etc. Requires:

- A new `com.apple.developer.wifi-aware` entitlement
  (provisioning profile must include it; gated per-app via the
  Developer Portal)
- A `WiFiAwareServices` key in Info.plist declaring the published/subscribed services
- `NSWiFiAwareUsageDescription` in Info.plist
- User permission prompt on first use
- iOS 26.0 deployment floor for the new APIs

Notes from the WWDC25 session:
- Service-publishing model similar to MultipeerConnectivity but at
  the radio layer (no Bluetooth fallback dependence)
- Pairing via QR code, NFC, or device-to-device negotiation
- Energy management is automatic (discovery is low-duty-cycle until
  data is needed)

### Android

`WifiAwareManager` since **API 26 (Android 8.0)**, in
`android.net.wifi.aware`. Mature for ~8 years. Permissions:

- `ACCESS_FINE_LOCATION` (Wi-Fi Aware reveals proximity, so location)
- `CHANGE_WIFI_STATE` for managing publish/subscribe sessions
- `NEARBY_WIFI_DEVICES` (Android 13+) — replaces location for this
  specific use case

The `attach()` → `publishService()` / `subscribeService()` →
`requestNetwork()` flow is well-documented but verbose. Real apps
(Google Nearby Share, WiFi Direct file transfer) layer over it.

### Cross-platform

No first-class library covers both. Each platform needs its own native
bridge with Rust glue, similar to how WaveSyncDB currently bridges
APNs/FCM. UniFFI could share the Rust orchestration code if we wanted
that route, but the platform-specific surface is unavoidable.

## Effort estimate

This is **not a small feature**. Rough breakdown:

| Piece | Effort | Notes |
|---|---|---|
| iOS native bridge (Swift) | ~1 week | New `WAPairingSession` integration, Swift Package similar to `WaveSyncPush`, entitlement provisioning |
| Android native bridge (Kotlin) | ~1 week | `WifiAwareManager` + service publishing/subscribing |
| Rust orchestration | ~3-5 days | Replace the libp2p socket transport with a Wi-Fi-Aware socket adapter; reuse the existing sync engine on top |
| Discovery protocol design | ~3 days | What service name? How are topics keyed? How do peers discover each other vs. the rest of the world? |
| Testing infrastructure | ~1 week | Requires two physical devices; CI is impossible (radios) |
| Documentation | 2-3 days | Permissions, entitlements, deployment story |
| **Total** | **3-4 weeks** | One-person-month, assuming no platform surprises |

The platform surprises are real: Apple's framework just shipped, the API
surface may still be evolving, and Android's older API has historically
had OEM-specific bugs (Samsung's Wi-Fi stack diverges from AOSP in
several places).

## Why defer

1. **Track A's lower-effort wins (IPv6, iOS Info.plist, multi-relay,
   DCUtR retry, UPnP) get most of the value of "minimize central
   infrastructure" without this scope of work.** Wi-Fi Aware is the
   *last* mile, not the first.
2. **No production telemetry on the use case yet.** We don't know what
   fraction of users actually hit "no Wi-Fi, no cellular." Without
   metrics, this could be 0.1% — not worth 4 weeks.
3. **iOS API is brand-new (iOS 26).** Six months of soak time before
   committing to it would be wise. Many WWDC 25 frameworks have had
   API breaks in 26.1.
4. **Bluetooth LE is the cheaper fallback** for the same scenarios at
   lower throughput. If we *do* want offline same-room sync, BLE
   should be measured first.

## Decision criteria for revisiting

Re-prioritise Wi-Fi Aware when **any** of these is true:

- Metrics show >5% of sync attempts happen with both peers offline
  (logged in `peer_addrs::record_failure` over a representative
  sample).
- A specific customer / use case requires no-infrastructure sync as a
  hard requirement (field-ops, healthcare in remote settings, etc.).
- iOS API stabilises through 26.x release cycle without breaking
  changes (track the WiFiAware framework changelog).
- A simpler alternative emerges (e.g., a libp2p WiFiAware transport in
  rust-libp2p — would collapse the effort estimate to ~1 week of glue).

## References

- [Apple: WiFiAware framework (iOS 26+)](https://developer.apple.com/documentation/wifiaware)
- [WWDC 2025: Meet WiFi Aware](https://developer.apple.com/videos/play/wwdc2025) (session 10000-series; check actual session number)
- [Android: WifiAwareManager](https://developer.android.com/reference/android/net/wifi/aware/WifiAwareManager)
- [Wi-Fi Alliance: NAN technical specification](https://www.wi-fi.org/discover-wi-fi/wi-fi-aware)
- [libp2p transport landscape (no Wi-Fi Aware transport as of 0.56)](https://github.com/libp2p/rust-libp2p)
