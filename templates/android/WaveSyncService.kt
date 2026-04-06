// WaveSyncDB — Firebase Cloud Messaging Service for background sync.
//
// Copy this file into your Android project and update the package name.
// Register it in AndroidManifest.xml (see below).
//
// AndroidManifest.xml:
//   <service android:name=".WaveSyncService" android:exported="false">
//       <intent-filter>
//           <action android:name="com.google.firebase.MESSAGING_EVENT" />
//       </intent-filter>
//   </service>
//
// Also ensure your app's build.gradle includes the Firebase Messaging dependency:
//   implementation("com.google.firebase:firebase-messaging:24.1.0")

package com.example.app // TODO: Replace with your package name

import android.util.Log
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage

class WaveSyncService : FirebaseMessagingService() {

    companion object {
        private const val TAG = "WaveSyncService"

        init {
            // Load the native library containing wavesync_background_sync.
            // This should match the library name in your Cargo.toml [lib] section
            // or the app's main native library.
            System.loadLibrary("your_app_name") // TODO: Replace with your library name
        }

        // Native function exported by wavesyncdb with feature = "mobile-ffi"
        //
        // Returns:
        //   0  = synced successfully
        //   1  = no peers found
        //   2  = timed out
        //   <0 = error (see wavesyncdb::ffi docs)
        @JvmStatic
        private external fun wavesync_background_sync(databaseUrl: String, timeoutSecs: Int): Int
    }

    override fun onMessageReceived(message: RemoteMessage) {
        // Only handle WaveSync sync notifications
        if (message.data["type"] != "sync_available") {
            return
        }

        Log.d(TAG, "Received sync_available push, starting background sync")

        // Build the SQLite URL pointing to the app's database.
        // TODO: Replace "app.db" with your actual database filename.
        val dbPath = getDatabasePath("app.db").absolutePath
        val databaseUrl = "sqlite://$dbPath?mode=rwc"

        val result = wavesync_background_sync(databaseUrl, 30)

        when (result) {
            0 -> Log.i(TAG, "Background sync completed successfully")
            1 -> Log.w(TAG, "Background sync: no peers found")
            2 -> Log.w(TAG, "Background sync: timed out")
            else -> Log.e(TAG, "Background sync failed with error code: $result")
        }
    }

    override fun onNewToken(token: String) {
        Log.d(TAG, "FCM token refreshed")

        // Persist the new token so the app can register it on next launch.
        // Call db.register_push_token("Fcm", token) when the app starts.
        getSharedPreferences("wavesync", MODE_PRIVATE)
            .edit()
            .putString("fcm_token", token)
            .apply()
    }
}
