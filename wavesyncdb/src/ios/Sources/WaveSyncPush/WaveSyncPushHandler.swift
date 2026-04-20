import Foundation
import UIKit

// Declared by wavesyncdb's C FFI (features = ["mobile-ffi"]).
//
// Returns:
//   0  — sync completed with at least one peer
//   1  — no peers found within timeout
//   2  — timed out (some peers may have synced)
//   <0 — error (see `wavesyncdb::ffi` docs)
@_silgen_name("wavesync_background_sync_with_peers")
private func wavesync_background_sync_with_peers(
    _ databaseUrl: UnsafePointer<CChar>,
    _ timeoutSecs: UInt32,
    _ peerAddrsJson: UnsafePointer<CChar>?
) -> Int32

/// Implements the APNs side of WaveSyncDB's iOS cold-sync integration.
///
/// Called by `WaveSyncPushBridge` in response to the three APNs delegate
/// callbacks (token register / fail / push received), which are installed
/// by `WaveSyncAppDelegateProxy` at `+load` time.
public enum WaveSyncPushHandler {

    // MARK: - Device token

    /// Hex-encode the APNs device token and persist it next to the database
    /// so the Rust engine's next `WaveSyncDbBuilder::build()` call picks it up
    /// (or the already-running engine sees it via the file watcher — future work).
    ///
    /// The file is written with `.completeUntilFirstUserAuthentication` data
    /// protection so a background-launched app can still read it after a
    /// device reboot, before the user unlocks.
    public static func writeDeviceToken(_ data: Data) {
        let hex = data.map { String(format: "%02x", $0) }.joined()

        guard let dir = WaveSyncTokenStore.tokenDir else {
            NSLog("[WaveSync] Token received but tokenDir not yet set — "
                  + "discarding. Rust must call wavesync_set_ios_token_dir() "
                  + "before APNs responds.")
            return
        }

        let fileURL = dir.appendingPathComponent(WaveSyncTokenStore.tokenFilename)
        do {
            try hex.write(to: fileURL, atomically: true, encoding: .utf8)
            try FileManager.default.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: fileURL.path)
            let preview = String(hex.prefix(10))
            NSLog("[WaveSync] APNs token written to %@: %@...", fileURL.path, preview)
        } catch {
            NSLog("[WaveSync] Failed to write APNs token to %@: %@",
                  fileURL.path, error.localizedDescription)
        }
    }

    // MARK: - Remote notification dispatch

    /// Parse the APNs payload, locate the database, and run background sync.
    /// iOS grants roughly 30 s of background execution; we use a 25 s FFI
    /// timeout to leave headroom for tokio shutdown and the UIKit handshake.
    public static func handleRemoteNotification(
        userInfo: [AnyHashable: Any],
        completionHandler: @escaping (UIBackgroundFetchResult) -> Void
    ) {
        guard userInfo["topic"] is String else {
            completionHandler(.noData)
            return
        }

        NSLog("[WaveSync] Received sync push, starting background sync")
        let peerAddrsJson = userInfo["peer_addrs"] as? String

        guard let dbUrl = findDatabaseUrl() else {
            NSLog("[WaveSync] No WaveSyncDB database found — has the app been launched?")
            completionHandler(.failed)
            return
        }

        DispatchQueue.global(qos: .utility).async {
            let rc: Int32 = dbUrl.withCString { urlPtr in
                if let peers = peerAddrsJson {
                    return peers.withCString { peersPtr in
                        wavesync_background_sync_with_peers(urlPtr, 25, peersPtr)
                    }
                }
                return wavesync_background_sync_with_peers(urlPtr, 25, nil)
            }

            let result: UIBackgroundFetchResult
            switch rc {
            case 0:
                NSLog("[WaveSync] Background sync completed successfully")
                result = .newData
            case 1:
                NSLog("[WaveSync] Background sync: no peers found")
                result = .noData
            case 2:
                NSLog("[WaveSync] Background sync: timed out")
                result = .noData
            default:
                NSLog("[WaveSync] Background sync failed with code %d", rc)
                result = .failed
            }

            DispatchQueue.main.async {
                completionHandler(result)
            }
        }
    }

    // MARK: - Database discovery

    /// Locate the WaveSyncDB database by searching for `.wavesync_config.json`
    /// in the app's standard writable directories. The search order matches
    /// where Rust's `WaveSyncDbBuilder::build()` most plausibly wrote it:
    ///
    ///   1. `Application Support/…` — used by `dioxus-sdk-storage::data_directory()`
    ///      on iOS and therefore the default for Dioxus apps.
    ///   2. `Documents/…` — older Swift handler layout; also where many apps
    ///      that manage their own paths put their SQLite files.
    ///
    /// Each directory is searched directly and one subdirectory level deep.
    /// As a last resort, the first `.db` file in either directory is returned
    /// as a `sqlite:` URL.
    static func findDatabaseUrl() -> String? {
        let fm = FileManager.default
        let searchRoots: [URL] = [
            fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first,
            fm.urls(for: .documentDirectory, in: .userDomainMask).first,
        ].compactMap { $0 }

        for root in searchRoots {
            if let url = findConfigUrl(under: root, fileManager: fm) {
                return url
            }
        }
        for root in searchRoots {
            if let url = findAnyDatabase(under: root, fileManager: fm) {
                return url
            }
        }
        return nil
    }

    private static func findConfigUrl(under root: URL, fileManager fm: FileManager) -> String? {
        let configName = ".wavesync_config.json"
        let rootConfig = root.appendingPathComponent(configName)
        if fm.fileExists(atPath: rootConfig.path) {
            if let url = extractDatabaseUrl(from: rootConfig) { return url }
        }
        let contents = (try? fm.contentsOfDirectory(
            at: root,
            includingPropertiesForKeys: [.isDirectoryKey],
            options: .skipsHiddenFiles)) ?? []
        for item in contents where isDirectory(item, fileManager: fm) {
            let nested = item.appendingPathComponent(configName)
            if fm.fileExists(atPath: nested.path),
               let url = extractDatabaseUrl(from: nested) {
                return url
            }
        }
        return nil
    }

    private static func findAnyDatabase(under root: URL, fileManager fm: FileManager) -> String? {
        let contents = (try? fm.contentsOfDirectory(
            at: root,
            includingPropertiesForKeys: nil,
            options: .skipsHiddenFiles)) ?? []
        if let db = contents.first(where: { $0.pathExtension == "db" }) {
            return "sqlite:\(db.path)?mode=rwc"
        }
        for item in contents where isDirectory(item, fileManager: fm) {
            let nested = (try? fm.contentsOfDirectory(
                at: item, includingPropertiesForKeys: nil,
                options: .skipsHiddenFiles)) ?? []
            if let db = nested.first(where: { $0.pathExtension == "db" }) {
                return "sqlite:\(db.path)?mode=rwc"
            }
        }
        return nil
    }

    private static func isDirectory(_ url: URL, fileManager fm: FileManager) -> Bool {
        var isDir: ObjCBool = false
        return fm.fileExists(atPath: url.path, isDirectory: &isDir) && isDir.boolValue
    }

    private static func extractDatabaseUrl(from configFile: URL) -> String? {
        guard let data = try? Data(contentsOf: configFile),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let url = json["database_url"] as? String else {
            return nil
        }
        return url
    }
}
