import Foundation

// Declare the C FFI function from wavesyncdb (built with features = ["mobile-ffi"])
@_silgen_name("wavesync_set_push_token")
private func wavesync_set_push_token(
    _ databaseUrl: UnsafePointer<CChar>,
    _ platform: UnsafePointer<CChar>,
    _ token: UnsafePointer<CChar>
) -> Int32

/// Persists the APNs device token into the WaveSyncDB config via Rust FFI.
///
/// This is the iOS equivalent of Android's `WaveSyncInitProvider` — it bridges
/// the native push token to Rust by writing it into `SyncConfig`, where both
/// the running engine and cold sync can read it.
///
/// ## Usage
///
/// In your `AppDelegate`:
///
/// ```swift
/// func application(
///     _ application: UIApplication,
///     didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
/// ) {
///     WaveSyncTokenWriter.writeToken(deviceToken, databaseUrl: "sqlite:///path/to/db.db?mode=rwc")
/// }
/// ```
public final class WaveSyncTokenWriter {

    /// Persist the APNs device token to the WaveSyncDB config.
    ///
    /// The token `Data` is hex-encoded before persisting. The Rust engine reads
    /// this from `SyncConfig` and registers the token with the relay server
    /// for push notification delivery.
    ///
    /// - Parameters:
    ///   - tokenData: The raw device token from
    ///     `didRegisterForRemoteNotificationsWithDeviceToken`.
    ///   - databaseUrl: The SQLite URL used when building `WaveSyncDb`
    ///     (e.g., `"sqlite:///path/to/app.db?mode=rwc"`).
    public static func writeToken(_ tokenData: Data, databaseUrl: String) {
        let hexToken = tokenData.map { String(format: "%02x", $0) }.joined()
        writeTokenString(hexToken, databaseUrl: databaseUrl)
    }

    /// Persist a pre-formatted hex token string.
    ///
    /// Use `writeToken(_:databaseUrl:)` when you have the raw `Data` from UIKit.
    /// Use this method only if you already have the hex-encoded string.
    public static func writeTokenString(_ hexToken: String, databaseUrl: String) {
        let result = databaseUrl.withCString { urlPtr in
            "Apns".withCString { platPtr in
                hexToken.withCString { tokPtr in
                    wavesync_set_push_token(urlPtr, platPtr, tokPtr)
                }
            }
        }

        switch result {
        case 0:
            let preview = String(hexToken.prefix(10))
            NSLog("[WaveSync] APNs token persisted to config: %@...", preview)
        case -1:
            NSLog("[WaveSync] Failed to persist APNs token: config not found (app never started?)")
        case -2:
            NSLog("[WaveSync] Failed to persist APNs token: could not save config")
        default:
            NSLog("[WaveSync] Failed to persist APNs token: error code %d", result)
        }
    }
}
