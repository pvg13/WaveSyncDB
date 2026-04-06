// WaveSyncDB — iOS Background Notification Handler for background sync.
//
// Add this to your AppDelegate to handle silent push notifications that
// wake the app for sync when it has been closed.
//
// Prerequisites:
// 1. Enable "Background Modes" capability in Xcode → target → Signing & Capabilities
// 2. Check "Remote notifications" under Background Modes
// 3. Add to Info.plist:
//      <key>UIBackgroundModes</key>
//      <array>
//          <string>remote-notification</string>
//      </array>
// 4. Link the Rust static library containing wavesync_background_sync
//    (built with features = ["mobile-ffi"])

import UIKit

// Declare the C FFI function from wavesyncdb
//
// Returns:
//   0  = synced successfully
//   1  = no peers found
//   2  = timed out
//   <0 = error (see wavesyncdb::ffi docs)
@_silgen_name("wavesync_background_sync")
func wavesync_background_sync(_ databaseUrl: UnsafePointer<CChar>, _ timeoutSecs: UInt32) -> Int32

// MARK: - Add to your AppDelegate

extension AppDelegate {

    func application(
        _ application: UIApplication,
        didReceiveRemoteNotification userInfo: [AnyHashable: Any],
        fetchCompletionHandler completionHandler: @escaping (UIBackgroundFetchResult) -> Void
    ) {
        // Only handle WaveSync sync notifications
        guard let type = userInfo["topic"] as? String else {
            completionHandler(.noData)
            return
        }

        NSLog("[WaveSync] Received sync push for topic: %@, starting background sync", type)

        // Build the SQLite URL pointing to the app's database.
        // TODO: Replace "app.db" with your actual database filename.
        let documentsDir = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
        let dbPath = documentsDir.appendingPathComponent("app.db").path
        let databaseUrl = "sqlite://\(dbPath)?mode=rwc"

        let result = databaseUrl.withCString { urlPtr in
            wavesync_background_sync(urlPtr, 30)
        }

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
