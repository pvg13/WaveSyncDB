import Foundation
import UIKit
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
