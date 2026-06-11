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

/// Show a local user notification for an incoming synced change.
///
/// Called from Rust (the `use_sync_notifications` Dioxus hook) via `dlsym`. The
/// `group` becomes the notification `threadIdentifier` and request identifier so
/// repeats for the same conversation collapse/replace instead of stacking.
///
/// Authorization is requested idempotently — iOS caches the user's choice, so
/// this only prompts once; subsequent calls just deliver (or no-op if denied).
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
        // Stable identifier per group → replace-in-place; immediate delivery.
        let identifier = (group?.isEmpty == false ? group! : UUID().uuidString)
        let request = UNNotificationRequest(identifier: identifier, content: content, trigger: nil)
        center.add(request) { addError in
            if let addError = addError {
                NSLog("[WaveSync] Failed to add notification: %@", addError.localizedDescription)
            }
        }
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
