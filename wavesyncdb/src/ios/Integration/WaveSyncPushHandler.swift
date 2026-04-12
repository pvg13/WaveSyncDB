import Foundation
import UIKit

// Declare the C FFI functions from wavesyncdb (built with features = ["mobile-ffi"])
//
// Returns:
//   0  = synced successfully
//   1  = no peers found
//   2  = timed out
//   <0 = error (see wavesyncdb::ffi docs)
@_silgen_name("wavesync_background_sync")
private func wavesync_background_sync(
    _ databaseUrl: UnsafePointer<CChar>,
    _ timeoutSecs: UInt32
) -> Int32

@_silgen_name("wavesync_background_sync_with_peers")
private func wavesync_background_sync_with_peers(
    _ databaseUrl: UnsafePointer<CChar>,
    _ timeoutSecs: UInt32,
    _ peerAddrsJson: UnsafePointer<CChar>?
) -> Int32

/// Handles incoming APNs silent push notifications for WaveSyncDB background sync.
///
/// This is the iOS equivalent of Android's `WaveSyncService` — it receives silent
/// push notifications from the relay server, finds the local database, and triggers
/// a one-shot background sync via the Rust C FFI.
///
/// ## Usage
///
/// In your `AppDelegate`:
///
/// ```swift
/// func application(
///     _ application: UIApplication,
///     didReceiveRemoteNotification userInfo: [AnyHashable: Any],
///     fetchCompletionHandler completionHandler: @escaping (UIBackgroundFetchResult) -> Void
/// ) {
///     WaveSyncPushHandler.handleRemoteNotification(
///         userInfo: userInfo,
///         completionHandler: completionHandler
///     )
/// }
/// ```
public final class WaveSyncPushHandler {

    /// Handle a remote notification by triggering background sync.
    ///
    /// Extracts the sync topic and optional peer addresses from the push payload,
    /// locates the WaveSyncDB database, and calls the Rust background sync engine.
    /// iOS provides approximately 30 seconds for background execution; this method
    /// uses a 25-second timeout to leave headroom.
    ///
    /// - Parameters:
    ///   - userInfo: The notification payload from APNs.
    ///   - completionHandler: The UIKit completion handler to call when sync finishes.
    public static func handleRemoteNotification(
        userInfo: [AnyHashable: Any],
        completionHandler: @escaping (UIBackgroundFetchResult) -> Void
    ) {
        // Only handle WaveSync sync notifications
        guard userInfo["topic"] is String else {
            completionHandler(.noData)
            return
        }

        NSLog("[WaveSync] Received sync push, starting background sync")

        // Extract peer addresses from payload (sent by relay for direct dialing)
        let peerAddrsJson = userInfo["peer_addrs"] as? String

        // Find the database URL from saved config
        guard let dbUrl = findDatabaseUrl() else {
            NSLog("[WaveSync] No WaveSyncDB database found - has the app been launched?")
            completionHandler(.failed)
            return
        }

        // Run sync on a background queue to avoid blocking the main thread
        DispatchQueue.global(qos: .utility).async {
            let result: Int32

            if let peerAddrs = peerAddrsJson {
                result = dbUrl.withCString { urlPtr in
                    peerAddrs.withCString { addrsPtr in
                        wavesync_background_sync_with_peers(urlPtr, 25, addrsPtr)
                    }
                }
            } else {
                result = dbUrl.withCString { urlPtr in
                    wavesync_background_sync(urlPtr, 25)
                }
            }

            DispatchQueue.main.async {
                switch result {
                case 0:
                    NSLog("[WaveSync] Background sync completed successfully")
                    completionHandler(.newData)
                case 1:
                    NSLog("[WaveSync] Background sync: no peers found")
                    completionHandler(.noData)
                case 2:
                    NSLog("[WaveSync] Background sync: timed out")
                    completionHandler(.noData)
                default:
                    NSLog("[WaveSync] Background sync failed with error code: %d", result)
                    completionHandler(.failed)
                }
            }
        }
    }

    // MARK: - Database URL Discovery

    /// Find the WaveSyncDB database URL from the saved config file.
    ///
    /// Searches the app's Documents directory for `.wavesync_config.json`,
    /// which is written by `WaveSyncDbBuilder::build()` on first launch.
    /// This mirrors Android's `WaveSyncService.findDatabaseUrl()`.
    private static func findDatabaseUrl() -> String? {
        let documentsDir = FileManager.default
            .urls(for: .documentDirectory, in: .userDomainMask)
            .first

        guard let documentsDir = documentsDir else { return nil }

        // Check for config in Documents root
        let configInDocs = documentsDir.appendingPathComponent(".wavesync_config.json")
        if FileManager.default.fileExists(atPath: configInDocs.path) {
            return extractDatabaseUrl(from: configInDocs)
        }

        // Search subdirectories (some apps store databases in subdirs)
        if let contents = try? FileManager.default.contentsOfDirectory(
            at: documentsDir,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: .skipsHiddenFiles
        ) {
            for item in contents {
                var isDir: ObjCBool = false
                if FileManager.default.fileExists(atPath: item.path, isDirectory: &isDir),
                   isDir.boolValue {
                    let config = item.appendingPathComponent(".wavesync_config.json")
                    if FileManager.default.fileExists(atPath: config.path) {
                        return extractDatabaseUrl(from: config)
                    }
                }
            }
        }

        // Fallback: find a .db file in Documents
        if let contents = try? FileManager.default.contentsOfDirectory(
            at: documentsDir,
            includingPropertiesForKeys: nil,
            options: .skipsHiddenFiles
        ) {
            if let dbFile = contents.first(where: { $0.pathExtension == "db" }) {
                return "sqlite:\(dbFile.path)?mode=rwc"
            }
        }

        return nil
    }

    /// Extract the database_url field from a WaveSync config JSON file.
    private static func extractDatabaseUrl(from configFile: URL) -> String? {
        guard let data = try? Data(contentsOf: configFile),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let url = json["database_url"] as? String else {
            return nil
        }
        return url
    }
}
