import Foundation
import UIKit

// Declared by wavesyncdb's C FFI (features = ["mobile-ffi"]).
//
// Returns:
//   0  — sync completed with at least one peer
//   1  — no peers found within timeout
//   2  — timed out (some peers may have synced)
//   3  — skipped: the app's own live engine handles the sync itself
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

@_silgen_name("wavesync_background_sync_targeted")
private func wavesync_background_sync_targeted(
    _ databaseUrl: UnsafePointer<CChar>,
    _ timeoutSecs: UInt32,
    _ peerAddrsJson: UnsafePointer<CChar>?,
    _ topic: UnsafePointer<CChar>?
) -> Int32

/// Run `body` with a C string for `s`, or `nil` when `s` is nil — lets us pass
/// an optional Swift `String?` to a `*const c_char` FFI parameter.
private func withOptionalCString<R>(
    _ s: String?,
    _ body: (UnsafePointer<CChar>?) -> R
) -> R {
    if let s = s {
        return s.withCString { body($0) }
    }
    return body(nil)
}

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

    /// Timeout (seconds) passed to the Rust background-sync FFI call in
    /// `handleRemoteNotification`. Host apps may tune this after measuring
    /// their actual granted background-execution window (varies by device,
    /// OS version, and background-modes entitlement — see the on-device A/B
    /// verdict process in docs/ios-device-protocol). Default of 25s assumes
    /// iOS's typical ~30s grant, leaving headroom for tokio shutdown and the
    /// UIKit completion handshake; Rust's internal timers
    /// (`background_sync`'s fallback/completion-grace windows) scale to
    /// whatever value is passed here.
    public static var backgroundSyncTimeoutSecs: UInt64 = 25

    // MARK: - Device token

    /// Hex-encode the APNs device token and persist it next to the database.
    ///
    /// The file is written with `.completeUntilFirstUserAuthentication` data
    /// protection so a background-launched app can still read it after a
    /// device reboot, before the user unlocks. Rust picks the token up on
    /// the next `WaveSyncDbBuilder::build()` call via the retry loop in
    /// `wavesyncdb/src/connection.rs`.
    public static func writeDeviceToken(_ data: Data) {
        writeDeviceToken(data, attempt: 0)
    }

    /// On a fresh install the APNs token typically arrives BEFORE the app's
    /// first `build()` finishes writing `.wavesync_config.json`, so the
    /// target directory does not exist yet. Retry every 2s (up to 2 minutes)
    /// until the config appears instead of deferring to the next launch —
    /// push must work from the very first install.
    private static func writeDeviceToken(_ data: Data, attempt: Int) {
        let hex = data.map { String(format: "%02x", $0) }.joined()

        guard let dir = findTokenDirectory() else {
            if attempt == 0 {
                NSLog("[WaveSync] APNs token received before .wavesync_config.json "
                      + "exists (fresh install) — waiting for the first build() to "
                      + "write it, retrying every 2s…")
            }
            if attempt < 60 {
                DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + 2) {
                    writeDeviceToken(data, attempt: attempt + 1)
                }
            } else {
                NSLog("[WaveSync] Gave up waiting for .wavesync_config.json after "
                      + "2 minutes — token will be written on next app launch.")
            }
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
    /// iOS grants roughly 30 s of background execution; the FFI call below
    /// uses `backgroundSyncTimeoutSecs` (25s default) to leave headroom for
    /// tokio shutdown and the UIKit handshake. The grant varies in practice
    /// (device, OS version, background-modes entitlement) — tune the static
    /// var after measuring on real hardware; see docs/ios-device-protocol.
    public static func handleRemoteNotification(
        userInfo: [AnyHashable: Any],
        completionHandler: @escaping (UIBackgroundFetchResult) -> Void
    ) {
        guard let topic = userInfo["topic"] as? String else {
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
            // Sync only the group named by the push (`topic`); the Rust side
            // falls back to all groups if it is nil.
            let rc: Int32 = dbUrl.withCString { urlPtr in
                withOptionalCString(peerAddrsJson) { peersPtr in
                    withOptionalCString(topic) { topicPtr in
                        wavesync_background_sync_targeted(
                            urlPtr, UInt32(backgroundSyncTimeoutSecs), peersPtr, topicPtr)
                    }
                }
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
                // Timed out — but some peers may have synced before the
                // deadline. Report .newData rather than .noData: telling iOS
                // "no data" on wakes that actually delivered teaches it to
                // deprioritize this app's future background pushes.
                NSLog("[WaveSync] Background sync: timed out")
                result = .newData
            case 3:
                // The app's own engine is live in this process and receives
                // the change through its open connections; nothing for this
                // wake to do.
                NSLog("[WaveSync] Background sync skipped: live engine handles it")
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

    /// Name of the pointer file Rust's `SyncConfig::save` writes at each
    /// search root: a single line holding the config directory's path
    /// relative to that root ("." when the config lives at the root itself).
    /// Must match `CONFIG_DIR_POINTER_FILE_NAME` in
    /// `wavesyncdb/src/connection.rs` (and the NSE's `resolveConfigDir`,
    /// which reads the same pointer at the App Group container root).
    public static let configDirPointerFilename = ".wavesync_config_dir"

    /// Return the URL of the `.wavesync_config.json` file that
    /// `WaveSyncDbBuilder::build()` wrote alongside the SQLite database.
    ///
    /// Search order, per root (`Application Support` — the
    /// `dioxus-sdk-storage::data_directory()` default — then `Documents`):
    ///   1. The `.wavesync_config_dir` pointer Rust writes on every config
    ///      save. This is the only path that works for configs nested more
    ///      than one level deep (e.g. a per-account `u/<user_id>/` layout).
    ///   2. The root itself, then one subdirectory level — the pre-pointer
    ///      fallback, kept for apps whose config predates the pointer file.
    static func findConfigFile() -> URL? {
        let fm = FileManager.default
        let roots: [URL] = [
            fm.urls(for: .applicationSupportDirectory, in: .userDomainMask).first,
            fm.urls(for: .documentDirectory, in: .userDomainMask).first,
        ].compactMap { $0 }

        for root in roots {
            if let pointed = configViaPointer(root: root, fileManager: fm) {
                return pointed
            }
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

    /// Resolve the config via `root`'s `.wavesync_config_dir` pointer, or nil
    /// when the pointer is absent, empty, or names a directory that doesn't
    /// actually hold a config (stale pointer → fall through to the search).
    private static func configViaPointer(root: URL, fileManager fm: FileManager) -> URL? {
        let pointer = root.appendingPathComponent(configDirPointerFilename)
        guard let contents = try? String(contentsOf: pointer, encoding: .utf8) else {
            return nil
        }
        let relative = contents.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !relative.isEmpty else { return nil }
        let config = root
            .appendingPathComponent(relative)
            .appendingPathComponent(configFilename)
        return fm.fileExists(atPath: config.path) ? config.standardizedFileURL : nil
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
