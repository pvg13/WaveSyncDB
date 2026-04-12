import Foundation

/// Writes the APNs device token to a file that the Rust WaveSyncDB engine reads.
///
/// This is the iOS equivalent of Android's `WaveSyncInitProvider` — it bridges
/// the native push token to Rust via the filesystem, avoiding complex FFI for
/// token retrieval.
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
///     WaveSyncTokenWriter.writeToken(deviceToken)
/// }
/// ```
public final class WaveSyncTokenWriter {

    /// The filename used for the APNs token file.
    /// Must match `APNS_TOKEN_FILENAME` in `wavesyncdb/src/push.rs`.
    public static let tokenFilename = "wavesync_apns_token"

    /// Write the APNs device token to the app's Documents directory.
    ///
    /// The token `Data` is hex-encoded before writing. The Rust engine reads
    /// this file during `WaveSyncDbBuilder::build()` and registers the token
    /// with the relay server for push notification delivery.
    ///
    /// - Parameter tokenData: The raw device token from
    ///   `didRegisterForRemoteNotificationsWithDeviceToken`.
    public static func writeToken(_ tokenData: Data) {
        let hexToken = tokenData.map { String(format: "%02x", $0) }.joined()

        guard let path = tokenFilePath() else {
            NSLog("[WaveSync] Cannot determine documents directory for token file")
            return
        }

        do {
            try hexToken.write(to: path, atomically: true, encoding: .utf8)
            let preview = String(hexToken.prefix(10))
            NSLog("[WaveSync] APNs token written to %@: %@...", path.path, preview)
        } catch {
            NSLog("[WaveSync] Failed to write APNs token file: %@", error.localizedDescription)
        }
    }

    /// Write a pre-formatted token string (e.g. from a token refresh callback).
    ///
    /// Use `writeToken(_:Data)` when you have the raw `Data` from UIKit.
    /// Use this method only if you already have the hex-encoded string.
    public static func writeTokenString(_ hexToken: String) {
        guard let path = tokenFilePath() else {
            NSLog("[WaveSync] Cannot determine documents directory for token file")
            return
        }

        do {
            try hexToken.write(to: path, atomically: true, encoding: .utf8)
            let preview = String(hexToken.prefix(10))
            NSLog("[WaveSync] APNs token written to %@: %@...", path.path, preview)
        } catch {
            NSLog("[WaveSync] Failed to write APNs token file: %@", error.localizedDescription)
        }
    }

    /// Returns the URL of the token file in the app's Documents directory.
    public static func tokenFilePath() -> URL? {
        return FileManager.default
            .urls(for: .documentDirectory, in: .userDomainMask)
            .first?
            .appendingPathComponent(tokenFilename)
    }
}
