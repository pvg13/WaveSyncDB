import Foundation
import WaveSyncPushObjC

/// Process-wide state owned by the Swift side of the iOS push integration.
///
/// Rust tells Swift where the database lives by calling
/// `wavesync_set_ios_token_dir(path)` from `WaveSyncDbBuilder::build()`
/// (on iOS, under the `push-sync` feature). Swift stores that path here;
/// the APNs callbacks in `WaveSyncPushBridge` use it to locate the token
/// file next to the database.
public enum WaveSyncTokenStore {

    /// Directory where push-related artefacts (token file, config) are stored.
    /// Set once by Rust after the database has been opened; nil before then.
    public static var tokenDir: URL? {
        get { queue.sync { _tokenDir } }
        set { queue.sync { _tokenDir = newValue } }
    }

    /// Filename produced by the APNs token writer. Must match
    /// `APNS_TOKEN_FILENAME` in `wavesyncdb/src/push.rs`.
    public static let tokenFilename = "wavesync_apns_token"

    /// Path to the token file, if the token directory has been set.
    public static var tokenFileURL: URL? {
        tokenDir?.appendingPathComponent(tokenFilename)
    }

    /// Reference the ObjC proxy class so the linker does not dead-strip it
    /// from the static archive. Accessed once at startup; the value is unused.
    @inline(never)
    public static func keepProxyAlive() {
        _ = WaveSyncAppDelegateProxy.self
    }

    // MARK: - Private

    private static let queue = DispatchQueue(label: "dev.wavesync.token-store")
    nonisolated(unsafe) private static var _tokenDir: URL?
}

/// C entry point called by Rust (`wavesyncdb/src/connection.rs`) on iOS.
///
/// Idempotent: the first call wins; subsequent calls with a different path
/// log a warning and are ignored. Passing a null pointer is a no-op.
@_cdecl("wavesync_set_ios_token_dir")
public func wavesync_set_ios_token_dir(_ path: UnsafePointer<CChar>?) {
    guard let path = path else { return }
    let url = URL(fileURLWithPath: String(cString: path), isDirectory: true)

    if let existing = WaveSyncTokenStore.tokenDir {
        if existing.path != url.path {
            NSLog("[WaveSync] wavesync_set_ios_token_dir called again with a "
                  + "different path (\(existing.path) → \(url.path)); ignoring")
        }
        return
    }

    WaveSyncTokenStore.tokenDir = url
    WaveSyncTokenStore.keepProxyAlive()
    NSLog("[WaveSync] Token directory set to %@", url.path)
}
