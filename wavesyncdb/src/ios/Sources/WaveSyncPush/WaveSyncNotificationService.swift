import Foundation
import UserNotifications

// ============================================================================
// TEMPLATE — NOT compiled into the WaveSyncPush framework.
//
// This file lives in the WaveSyncPush package for discoverability, but is
// excluded from the "WaveSyncPush" SwiftPM target (see `Package.swift`'s
// `exclude:` list). A Notification Service Extension (NSE) is its own
// `.appex` executable target, distinct from the app's main framework —
// `dx`/`xcodebuild` can't create that target for you (see
// docs/ios-deployment's "riskiest blind part" note), so this ships as a
// starting point instead of a linked library.
//
// To use it: create an NSE target in Xcode (File → New → Target →
// Notification Service Extension), copy this file into it, subclass
// `WaveSyncNotificationService` overriding `appGroupId` (and optionally
// `budgetSecs`), link the NSE target against the same WaveSyncPush framework
// the app uses, and give the NSE target's Info.plist an
// `NSExtensionPrincipalClass` pointing at your subclass. The NSE target also
// needs its own App Groups entitlement for the SAME group id as the app.
// ============================================================================

// Declared by wavesyncdb's C FFI (features = ["mobile-ffi", "push-sync"],
// target_os = "ios"). Resolved at runtime by dyld against the main
// executable's exports, same as `WaveSyncPushHandler`'s `@_silgen_name`
// declarations — the Swift compiler and the Rust linker stay independent.
@_silgen_name("wavesync_nse_handle_push")
private func wavesync_nse_handle_push(
    _ configDir: UnsafePointer<CChar>,
    _ payloadJson: UnsafePointer<CChar>,
    _ budgetSecs: UInt32
) -> UnsafeMutablePointer<CChar>?

@_silgen_name("wavesync_string_free")
private func wavesync_string_free(_ s: UnsafeMutablePointer<CChar>?)

/// Template `UNNotificationServiceExtension` for WaveSyncDB's alert-class
/// pushes (`NotifyTopic.visible` — see the relay's `push_sender.rs`).
///
/// Runs whenever APNs delivers `mutable-content: 1` (an alert-class push).
/// Loads the SAME database as the app, through the shared App Group
/// container, and runs a short one-shot sync scoped to just the group named
/// by the push. If a `SyncNotify` policy fires for whatever synced, this
/// replaces the operator-branded placeholder title/body with the real
/// content (e.g. "Ana added milk"). Either way the user sees SOME
/// notification — the placeholder is the safe fallback if the sync times
/// out, the NSE is killed before it finishes, or nothing notify-worthy
/// happened.
///
/// The Rust side never runs the KDF here (see `wavesyncdb`'s `key_cache`
/// module docs) — it can only sync a group whose key the app already
/// cached to disk on a previous foreground launch. A cold cache (fresh
/// install, cache cleared, or `with_group_key_cache(false)`) means this
/// falls straight through to the placeholder, same as a timeout.
open class WaveSyncNotificationService: UNNotificationServiceExtension {

    /// App Group identifier shared with the main app (e.g.
    /// `"group.com.example.myapp"`). MUST match the entitlement on both the
    /// app's target and this extension's target, and the group id the app
    /// passed to `wavesync_app_group_container` when it chose its database
    /// directory. Override in your subclass — there is no safe default.
    open class var appGroupId: String { "" }

    /// Upper bound on how long the sync is allowed to run, in seconds.
    /// iOS grants an NSE roughly 30s of wall-clock time total before force-
    /// killing it; keep this comfortably under that so there's room left for
    /// the extension's own startup and the final `contentHandler` call.
    /// Deliberately lower than the app's own background-sync budget
    /// (`WaveSyncPushHandler.backgroundSyncTimeoutSecs`, 25s default) — the
    /// NSE's total budget is tighter and shared with the OS's own
    /// bookkeeping around the extension process.
    open class var budgetSecs: UInt32 { 20 }

    private var contentHandler: ((UNNotificationContent) -> Void)?
    private var bestAttemptContent: UNMutableNotificationContent?

    /// Serializes `deliver`'s read-nil-call of `contentHandler` — see that
    /// method's doc comment for the race it closes.
    private let deliverQueue = DispatchQueue(label: "com.wavesyncdb.nse.deliver")

    override open func didReceive(
        _ request: UNNotificationRequest,
        withContentHandler contentHandler: @escaping (UNNotificationContent) -> Void
    ) {
        self.contentHandler = contentHandler
        let best = (request.content.mutableCopy() as? UNMutableNotificationContent)
            ?? UNMutableNotificationContent()
        bestAttemptContent = best

        guard let configDir = Self.resolveConfigDir(groupId: Self.appGroupId) else {
            NSLog("[WaveSyncNSE] No App Group container for '%@' — delivering placeholder",
                  Self.appGroupId)
            deliver(best)
            return
        }
        guard let payloadJson = Self.encodeUserInfo(request.content.userInfo) else {
            NSLog("[WaveSyncNSE] Could not encode push payload — delivering placeholder")
            deliver(best)
            return
        }

        // The FFI call blocks on a fresh tokio runtime inside Rust for up to
        // `budgetSecs` — run it off the extension's main thread.
        DispatchQueue.global(qos: .utility).async { [weak self] in
            let resultJson: String? = configDir.withCString { dirPtr in
                payloadJson.withCString { payloadPtr in
                    guard let raw = wavesync_nse_handle_push(dirPtr, payloadPtr, Self.budgetSecs)
                    else {
                        return nil
                    }
                    defer { wavesync_string_free(raw) }
                    return String(cString: raw)
                }
            }
            self?.applyResult(resultJson, to: best)
        }
    }

    /// iOS is about to kill the extension — deliver whatever we have. If the
    /// sync already finished, `best` carries the real title/body (written by
    /// `applyResult` on the same object); if not, it's still the original
    /// placeholder content untouched.
    override open func serviceExtensionTimeWillExpire() {
        if let best = bestAttemptContent {
            deliver(best)
        }
    }

    private func applyResult(_ json: String?, to best: UNMutableNotificationContent) {
        guard let json = json,
              let data = json.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            deliver(best)
            return
        }
        if let title = obj["title"] as? String, !title.isEmpty {
            best.title = title
        }
        if let body = obj["body"] as? String, !body.isEmpty {
            best.body = body
        }
        deliver(best)
    }

    /// Calls the content handler exactly once. Both `didReceive`'s early-exit
    /// paths and `applyResult`'s completion path funnel through here, and
    /// `serviceExtensionTimeWillExpire` may race either of them — one runs on
    /// the utility queue `didReceive` dispatched onto, the other on whatever
    /// queue iOS calls the expiry callback on, and both can land at once.
    /// `deliverQueue` serializes the read-nil-call sequence so only one of
    /// them ever wins the `guard`, closing the race that would otherwise let
    /// both call the handler — logged by UNNotificationServiceExtension as a
    /// programmer error.
    private func deliver(_ content: UNNotificationContent) {
        deliverQueue.sync {
            guard let handler = contentHandler else { return }
            contentHandler = nil
            handler(content)
        }
    }

    /// App Group container path as a plain filesystem path string (not a
    /// `URL`) — matches the `config_dir` contract `wavesync_nse_handle_push`
    /// expects (the directory containing `.wavesync_config.json`).
    ///
    /// The container ROOT is not necessarily where `.wavesync_config.json`
    /// lives — an app with a per-account (or otherwise nested) data layout
    /// keeps it in a subdirectory instead. Such an app is expected to write
    /// a pointer file, `.wavesync_config_dir`, at the container root on
    /// every launch/login: a single line holding the path of the currently
    /// active config directory, RELATIVE to the container root (so the
    /// pointer keeps working if the container itself is ever relocated by
    /// iOS between launches). If the pointer exists and names a directory
    /// that actually holds `.wavesync_config.json`, use it; otherwise fall
    /// back to the root itself, which is correct for an app that keeps its
    /// database directly there and never writes a pointer at all.
    private static func resolveConfigDir(groupId: String) -> String? {
        guard !groupId.isEmpty else { return nil }
        guard let root = FileManager.default
            .containerURL(forSecurityApplicationGroupIdentifier: groupId)?
            .path
        else {
            return nil
        }

        let pointerPath = (root as NSString).appendingPathComponent(".wavesync_config_dir")
        if let pointerContents = try? String(contentsOfFile: pointerPath, encoding: .utf8) {
            let relative = pointerContents.trimmingCharacters(in: .whitespacesAndNewlines)
            if !relative.isEmpty {
                let candidate = (root as NSString).appendingPathComponent(relative)
                let configPath = (candidate as NSString).appendingPathComponent(".wavesync_config.json")
                if FileManager.default.fileExists(atPath: configPath) {
                    return candidate
                }
            }
        }

        return root
    }

    /// APNs delivers `userInfo` as an `[AnyHashable: Any]` dictionary;
    /// `wavesync_nse_handle_push` wants a JSON string with (at minimum) the
    /// same `"topic"` / `"peer_addrs"` keys `WaveSyncPushHandler` reads on
    /// the app side — see `parse_push_payload` in `wavesyncdb/src/ffi.rs`.
    private static func encodeUserInfo(_ userInfo: [AnyHashable: Any]) -> String? {
        var plain: [String: Any] = [:]
        for (key, value) in userInfo {
            guard let key = key as? String else { continue }
            plain[key] = value
        }
        guard JSONSerialization.isValidJSONObject(plain),
              let data = try? JSONSerialization.data(withJSONObject: plain)
        else {
            return nil
        }
        return String(data: data, encoding: .utf8)
    }
}
