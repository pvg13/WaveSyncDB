import Foundation
import UIKit

// Declared by wavesyncdb's C FFI (features = ["mobile-ffi"]).
//
// Returns:
//   0  — sync completed with at least one peer
//   1  — no peers found within timeout
//   2  — timed out (some peers may have synced)
//   <0 — error (see `wavesyncdb::ffi` docs)
//
// Resolved at runtime by dyld against the main executable's exports; the
// Swift package deliberately does not depend on any Rust symbols at build
// time so the Swift compiler and the Rust linker remain independent.
@_silgen_name("wavesync_background_sync_with_peers")
private func wavesync_background_sync_with_peers(
    _ databaseUrl: UnsafePointer<CChar>,
    _ timeoutSecs: UInt32,
    _ peerAddrsJson: UnsafePointer<CChar>?
) -> Int32

/// Implements the APNs side of WaveSyncDB's iOS cold-sync integration.
///
/// Called by `WaveSyncPushBridge` in response to the three APNs delegate
/// callbacks installed by `WaveSyncAppDelegateProxy+load`. All file-system
/// paths are discovered by searching for the `.wavesync_config.json` file
/// that `WaveSyncDbBuilder::build()` writes next to the SQLite database —
/// the Swift side intentionally does not accept runtime configuration from
/// Rust, to keep the build-time link graph one-directional (Swift → Rust
/// only, resolved lazily by dyld).
public enum WaveSyncPushHandler {

    /// Filename produced by `writeDeviceToken` alongside the SQLite DB.
    /// Must match `APNS_TOKEN_FILENAME` in `wavesyncdb/src/push.rs`.
    public static let tokenFilename = "wavesync_apns_token"

    /// Name of the sync config file Rust writes at `WaveSyncDbBuilder::build()`.
    public static let configFilename = ".wavesync_config.json"

    // MARK: - Device token

    /// Hex-encode the APNs device token and persist it next to the database.
    ///
    /// The file is written with `.completeUntilFirstUserAuthentication` data
    /// protection so a background-launched app can still read it after a
    /// device reboot, before the user unlocks. Rust picks the token up on
    /// the next `WaveSyncDbBuilder::build()` call via the retry loop in
    /// `wavesyncdb/src/connection.rs`.
    public static func writeDeviceToken(_ data: Data) {
        let hex = data.map { String(format: "%02x", $0) }.joined()

        guard let dir = findTokenDirectory() else {
            NSLog("[WaveSync] APNs token received but no .wavesync_config.json "
                  + "found yet — nothing to pair the token with. Will be picked "
                  + "up on next app launch once the config file exists.")
            return
        }

        let fileURL = dir.appendingPathComponent(tokenFilename)
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
    /// iOS grants roughly 30 s of background execution; the FFI uses a 25 s
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

    // MARK: - Discovery helpers

    /// Return the URL of the `.wavesync_config.json` file that
    /// `WaveSyncDbBuilder::build()` wrote alongside the SQLite database.
    ///
    /// Search order matches where Rust most plausibly put it:
    ///   1. `Application Support/…` — used by `dioxus-sdk-storage::data_directory()`
    ///      on iOS and therefore the default for Dioxus apps.
    ///   2. `Documents/…` — non-Dioxus apps that manage their own paths.
    ///
    /// Each directory is checked directly and one subdirectory level deep.
    static func findConfigFile() -> URL? {
        let fm = FileManager.default
        let roots: [URL] = [
            fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first,
            fm.urls(for: .documentDirectory, in: .userDomainMask).first,
        ].compactMap { $0 }

        for root in roots {
            let rootConfig = root.appendingPathComponent(configFilename)
            if fm.fileExists(atPath: rootConfig.path) {
                return rootConfig
            }
            let contents = (try? fm.contentsOfDirectory(
                at: root,
                includingPropertiesForKeys: [.isDirectoryKey],
                options: .skipsHiddenFiles)) ?? []
            for item in contents where isDirectory(item, fileManager: fm) {
                let nested = item.appendingPathComponent(configFilename)
                if fm.fileExists(atPath: nested.path) {
                    return nested
                }
            }
        }
        return nil
    }

    /// Directory where the APNs token file should be written — same parent
    /// directory as `.wavesync_config.json` (and therefore the SQLite DB).
    static func findTokenDirectory() -> URL? {
        findConfigFile()?.deletingLastPathComponent()
    }

    /// Locate the SQLite database URL. Reads the config file's
    /// `database_url` field; falls back to any `.db` file in the search
    /// roots if the config can't be parsed.
    static func findDatabaseUrl() -> String? {
        if let config = findConfigFile(),
           let url = extractDatabaseUrl(from: config) {
            return url
        }
        return findAnyDatabaseFallback()
    }

    private static func findAnyDatabaseFallback() -> String? {
        let fm = FileManager.default
        let roots: [URL] = [
            fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first,
            fm.urls(for: .documentDirectory, in: .userDomainMask).first,
        ].compactMap { $0 }

        for root in roots {
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
