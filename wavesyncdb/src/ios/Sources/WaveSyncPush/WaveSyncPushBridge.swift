import Foundation
import UIKit
import UserNotifications
import WaveSyncPushObjC

/// C-ABI entry points that the ObjC AppDelegate proxy calls into.
///
/// The ObjC proxy performs zero policy: it only captures the three APNs
/// delegate callbacks and forwards them here. All policy (file-system paths,
/// protection classes, JSON parsing, Rust FFI) lives in Swift.

@_cdecl("wavesync_push_bridge_did_register")
public func wavesync_push_bridge_did_register(_ tokenPtr: UnsafeRawPointer?) {
    guard let tokenPtr = tokenPtr else { return }
    let data = Unmanaged<NSData>.fromOpaque(tokenPtr).takeUnretainedValue() as Data
    WaveSyncPushHandler.writeDeviceToken(data)
}

@_cdecl("wavesync_push_bridge_did_fail")
public func wavesync_push_bridge_did_fail(_ errorPtr: UnsafeRawPointer?) {
    guard let errorPtr = errorPtr else { return }
    let error = Unmanaged<NSError>.fromOpaque(errorPtr).takeUnretainedValue()
    NSLog("[WaveSync] Failed to register for remote notifications: %@",
          error.localizedDescription)
}

/// Minimal `UNUserNotificationCenterDelegate` that opts foreground
/// notifications into banner + sound presentation.
///
/// iOS suppresses notification banners while the app is in the foreground
/// unless the notification center has a delegate whose `willPresent` callback
/// returns presentation options. Without this, locally-posted sync
/// notifications (e.g. "Ana added milk") are silently queued and never shown
/// while the user has the app open.
private final class WaveSyncNotificationDelegate: NSObject, UNUserNotificationCenterDelegate {
    static let shared = WaveSyncNotificationDelegate()

    func userNotificationCenter(
        _ center: UNUserNotificationCenter,
        willPresent notification: UNNotification,
        withCompletionHandler completionHandler: @escaping (UNNotificationPresentationOptions) -> Void
    ) {
        // Suppress the relay's generic alert placeholder while the app is in
        // the FOREGROUND (willPresent only runs then): the live engine is
        // receiving this change over its open connections right now, and the
        // SyncNotify policy posts the specific local notification — or
        // nothing, if it deems the change not notify-worthy. Either way the
        // generic banner is redundant noise on top of an app the user is
        // already looking at. WaveSync remote pushes are identified by the
        // "topic" payload key + remote trigger; everything else (including
        // our own specific local notifications) presents normally.
        if notification.request.trigger is UNPushNotificationTrigger,
           notification.request.content.userInfo["topic"] != nil {
            completionHandler([])
            return
        }
        completionHandler([.banner, .sound])
    }
}

/// Install the foreground-presentation delegate on the shared notification
/// center, unless the host app has already set its own.
///
/// Called from the ObjC AppDelegate proxy at `UIApplicationDidFinishLaunching`
/// (already on the main thread) so the delegate is set before the first sync
/// notification can arrive. Setting it this early means even the very first
/// foreground notification presents as a banner.
///
/// The `delegate == nil` guard makes this a no-op when the host app installs
/// its own `UNUserNotificationCenterDelegate` — we never clobber it.
@_cdecl("wavesync_install_notification_delegate")
public func wavesync_install_notification_delegate() {
    let install = {
        let center = UNUserNotificationCenter.current()
        if center.delegate == nil {
            center.delegate = WaveSyncNotificationDelegate.shared
        }
    }
    if Thread.isMainThread {
        install()
    } else {
        DispatchQueue.main.async(execute: install)
    }
}

/// Request user authorization for notifications at launch.
///
/// iOS heavily throttles silent (`content-available`) pushes to an app the user
/// has never authorized for notifications — especially when the app is killed,
/// which is exactly the background-sync wake path. Requesting authorization
/// early (idempotently — iOS caches the choice and only prompts once) keeps that
/// wake reliable, and also lets the locally-posted sync notifications display.
///
/// Called from the ObjC AppDelegate proxy at `UIApplicationDidFinishLaunching`,
/// just before `registerForRemoteNotifications`. The request itself is async and
/// does not block registration; silent pushes work regardless of the prompt's
/// outcome, but an authorized app is not throttled.
@_cdecl("wavesync_request_push_authorization")
public func wavesync_request_push_authorization() {
    UNUserNotificationCenter.current()
        .requestAuthorization(options: [.alert, .sound, .badge]) { _, error in
            if let error = error {
                NSLog("[WaveSync] Push authorization request error: %@",
                      error.localizedDescription)
            }
        }
}

/// Show a local user notification for an incoming synced change.
///
/// Called from Rust (the `use_sync_notifications` Dioxus hook) via `dlsym`. The
/// `group` becomes the notification `threadIdentifier` and request identifier so
/// repeats for the same conversation collapse/replace instead of stacking.
///
/// Authorization is requested idempotently — iOS caches the user's choice, so
/// this only prompts once; subsequent calls just deliver (or no-op if denied).
///
/// Before delivering, any already-delivered WaveSync *remote* notification —
/// the relay's generic alert-class placeholder ("Nueva actividad") that iOS
/// displayed the moment the push arrived — is removed, so the end state is a
/// single, specific notification rather than the placeholder stacked under
/// it. WaveSync placeholders are identified by the top-level `"topic"` key
/// the relay puts in every push payload plus the remote-push trigger; the
/// app's own local notifications never carry `"topic"`, and other apps'
/// notifications are invisible to this process. Once a Notification Service
/// Extension exists, this same sweep harmlessly replaces the NSE-rewritten
/// banner with identical content. When the `SyncNotify` policy declines to
/// notify (returns `None`), this function is never called and the
/// placeholder stays — the intended fallback so the user still gets SOME
/// signal for the change.
@_cdecl("wavesync_show_notification")
public func wavesync_show_notification(
    _ titlePtr: UnsafePointer<CChar>?,
    _ bodyPtr: UnsafePointer<CChar>?,
    _ groupPtr: UnsafePointer<CChar>?
) {
    guard let titlePtr = titlePtr, let bodyPtr = bodyPtr else { return }
    let title = String(cString: titlePtr)
    let body = String(cString: bodyPtr)
    let group = groupPtr.map { String(cString: $0) }

    let content = UNMutableNotificationContent()
    content.title = title
    content.body = body
    content.sound = .default
    if let group = group, !group.isEmpty {
        content.threadIdentifier = group
    }

    let center = UNUserNotificationCenter.current()
    center.requestAuthorization(options: [.alert, .sound, .badge]) { granted, error in
        if let error = error {
            NSLog("[WaveSync] Notification authorization error: %@", error.localizedDescription)
        }
        guard granted else {
            NSLog("[WaveSync] Notification permission not granted; skipping display")
            return
        }
        // Evict delivered relay placeholders BEFORE adding, inside the same
        // completion, so remove→add ordering is deterministic and the
        // specific notification can never be swept by its own cleanup.
        center.getDeliveredNotifications { delivered in
            let placeholders = delivered
                .filter {
                    $0.request.trigger is UNPushNotificationTrigger
                        && $0.request.content.userInfo["topic"] != nil
                }
                .map { $0.request.identifier }
            if !placeholders.isEmpty {
                center.removeDeliveredNotifications(withIdentifiers: placeholders)
            }

            // Stable identifier per group → replace-in-place; immediate delivery.
            let identifier = (group?.isEmpty == false ? group! : UUID().uuidString)
            let request = UNNotificationRequest(
                identifier: identifier, content: content, trigger: nil)
            center.add(request) { addError in
                if let addError = addError {
                    NSLog("[WaveSync] Failed to add notification: %@",
                          addError.localizedDescription)
                }
            }
        }
    }
}

/// Mark a file `NSFileProtectionCompleteUntilFirstUserAuthentication`.
///
/// Called from Rust (`key_cache::save_group_key`) via `dlsym` after writing
/// the on-disk group-key cache — same dyld-lazy-resolution idiom as
/// `wavesync_show_notification` above. This protection class matches the one
/// `WaveSyncPushHandler.writeDeviceToken` already uses for the APNs token
/// file: background-launchable (the Notification Service Extension can read
/// it before first unlock) while the file stays encrypted at rest whenever
/// the device is locked. Best-effort — a failure is logged, never fatal.
@_cdecl("wavesync_protect_file")
public func wavesync_protect_file(_ pathPtr: UnsafePointer<CChar>?) {
    guard let pathPtr = pathPtr else { return }
    let path = String(cString: pathPtr)
    do {
        try FileManager.default.setAttributes(
            [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
            ofItemAtPath: path)
    } catch {
        NSLog("[WaveSync] Failed to protect file %@: %@", path, error.localizedDescription)
    }
}

@_cdecl("wavesync_push_bridge_did_receive")
public func wavesync_push_bridge_did_receive(
    _ userInfoPtr: UnsafeRawPointer?,
    _ wrapperPtr: UnsafeRawPointer?
) {
    guard let userInfoPtr = userInfoPtr,
          let wrapperPtr = wrapperPtr else { return }

    let userInfo = Unmanaged<NSDictionary>.fromOpaque(userInfoPtr)
        .takeUnretainedValue() as? [AnyHashable: Any] ?? [:]
    // ObjC passed the wrapper via `__bridge_retained` — Swift assumes ownership.
    let wrapper = Unmanaged<WaveSyncCompletionWrapper>.fromOpaque(wrapperPtr)
        .takeRetainedValue()

    WaveSyncPushHandler.handleRemoteNotification(userInfo: userInfo) { result in
        // `UIBackgroundFetchResult.rawValue` is `UInt`; the ObjC wrapper's
        // selector takes `NSInteger` which bridges to `Int`.
        wrapper.invoke(withResult: Int(result.rawValue))
    }
}

/// Resolve an iOS App Group container directory by group id.
///
/// Exposed as a C symbol so wavesyncdb's Rust side (which has no access to
/// Foundation) can point both the app's `WaveSyncDbBuilder` and its
/// Notification Service Extension at the same shared directory, instead of
/// duplicating `containerURL(forSecurityApplicationGroupIdentifier:)` logic
/// in Rust. Resolved at runtime via `dlsym` from the Rust side (see
/// `wavesyncdb::wavesync_app_group_container`) — same dyld-lazy-resolution
/// idiom as `wavesync_show_notification` above.
///
/// Returns a `strdup`'d C string the caller must free with the C library
/// `free()` — **not** `wavesync_string_free`, which frees a *Rust*-allocated
/// `CString` from a different allocator. Returns `NULL` if the app has no
/// entitlement for `groupId` or the container doesn't exist.
@_cdecl("wavesync_app_group_container")
public func wavesync_app_group_container(
    _ groupIdPtr: UnsafePointer<CChar>?
) -> UnsafeMutablePointer<CChar>? {
    guard let groupIdPtr = groupIdPtr else { return nil }
    let groupId = String(cString: groupIdPtr)
    guard let url = FileManager.default
        .containerURL(forSecurityApplicationGroupIdentifier: groupId)
    else {
        return nil
    }
    return strdup(url.path)
}
