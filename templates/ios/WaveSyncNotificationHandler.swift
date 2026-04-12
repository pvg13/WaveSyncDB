// WaveSyncDB — iOS Push Notification Integration Template
//
// This template shows how to wire APNs push notifications into your
// AppDelegate for WaveSyncDB background sync.
//
// With `ios-push` feature enabled, the Swift `WaveSyncPush` package is
// auto-bundled into your app — you just need to add these AppDelegate methods.
//
// Prerequisites:
// 1. Enable "Background Modes" capability in Xcode -> target -> Signing & Capabilities
// 2. Check "Remote notifications" under Background Modes
// 3. Enable "Push Notifications" capability
// 4. Add to Info.plist:
//      <key>UIBackgroundModes</key>
//      <array>
//          <string>remote-notification</string>
//      </array>
// 5. Build wavesyncdb with features = ["ios-push"] (or ["dioxus", "ios-push"])
//
// For Dioxus.toml, add:
//
//   [ios]
//   identifier = "com.example.myapp"
//
// And configure your Apple Developer account with push notification
// entitlements for your bundle ID.

import UIKit
import WaveSyncPush

// MARK: - Add to your AppDelegate

extension AppDelegate {

    // Call this from application(_:didFinishLaunchingWithOptions:)
    func setupWaveSyncPushNotifications(_ application: UIApplication) {
        // Request permission to receive push notifications.
        // For silent/background pushes (content-available), no user permission
        // dialog is required — only registerForRemoteNotifications() is needed.
        application.registerForRemoteNotifications()
    }

    // MARK: - APNs Token Registration

    func application(
        _ application: UIApplication,
        didRegisterForRemoteNotificationsWithDeviceToken deviceToken: Data
    ) {
        // Write the APNs token to a file for the Rust engine to read.
        // The engine picks this up during WaveSyncDbBuilder::build() and
        // registers it with the relay server for push delivery.
        WaveSyncTokenWriter.writeToken(deviceToken)
    }

    func application(
        _ application: UIApplication,
        didFailToRegisterForRemoteNotificationsWithError error: Error
    ) {
        NSLog("[WaveSync] Failed to register for remote notifications: %@", error.localizedDescription)
    }

    // MARK: - Background Push Handling

    func application(
        _ application: UIApplication,
        didReceiveRemoteNotification userInfo: [AnyHashable: Any],
        fetchCompletionHandler completionHandler: @escaping (UIBackgroundFetchResult) -> Void
    ) {
        // Delegate to WaveSyncPushHandler which:
        // 1. Extracts the sync topic and peer addresses from the payload
        // 2. Finds the WaveSyncDB database from the saved config
        // 3. Calls the Rust background sync engine via C FFI
        // 4. Returns the appropriate UIBackgroundFetchResult
        WaveSyncPushHandler.handleRemoteNotification(
            userInfo: userInfo,
            completionHandler: completionHandler
        )
    }
}
