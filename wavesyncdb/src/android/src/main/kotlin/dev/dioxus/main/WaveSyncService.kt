package dev.dioxus.main

import android.content.Context
import android.util.Log
import com.google.firebase.messaging.FirebaseMessaging
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import java.io.File

/**
 * Firebase Cloud Messaging service for WaveSyncDB background sync.
 *
 * Firebase auto-initializes from google-services.json (via Google Services plugin).
 * When a "sync_available" push arrives (even if the app is killed), this service:
 * 1. Calls goAsync() to extend execution time to ~30s
 * 2. Loads the native Rust library
 * 3. Reads the database path from the WaveSyncDB config
 * 4. Calls native backgroundSync() to pull changes from peers
 */
class WaveSyncService : FirebaseMessagingService() {

    companion object {
        private const val TAG = "WaveSyncService"
        private const val TOKEN_FILENAME = "wavesync_fcm_token"

        private var nativeLoaded = false

        private fun ensureNativeLoaded() {
            if (!nativeLoaded) {
                try {
                    System.loadLibrary("main")
                    nativeLoaded = true
                } catch (e: UnsatisfiedLinkError) {
                    Log.e(TAG, "Failed to load native library: ${e.message}")
                }
            }
        }

        /**
         * Initialize Firebase from credentials saved in .wavesync_config.json.
         * Called on cold start when the Google Services plugin isn't available.
         */
        private fun ensureFirebaseFromConfig(context: Context): Boolean {
            if (com.google.firebase.FirebaseApp.getApps(context).isNotEmpty()) return true

            // Find and parse .wavesync_config.json
            val config = findConfigFile(context) ?: return false
            try {
                val json = config.readText()
                val projectId = Regex(""""fcm_project_id"\s*:\s*"([^"]+)"""").find(json)?.groupValues?.get(1) ?: return false
                val appId = Regex(""""fcm_app_id"\s*:\s*"([^"]+)"""").find(json)?.groupValues?.get(1) ?: return false
                val apiKey = Regex(""""fcm_api_key"\s*:\s*"([^"]+)"""").find(json)?.groupValues?.get(1) ?: return false

                val options = com.google.firebase.FirebaseOptions.Builder()
                    .setProjectId(projectId)
                    .setApplicationId(appId)
                    .setApiKey(apiKey)
                    .build()
                com.google.firebase.FirebaseApp.initializeApp(context, options)
                Log.i(TAG, "Firebase initialized from wavesync config")
                return true
            } catch (e: Exception) {
                Log.e(TAG, "Failed to init Firebase from config: ${e.message}")
                return false
            }
        }

        private fun findConfigFile(context: Context): File? {
            val direct = File(context.filesDir, ".wavesync_config.json")
            if (direct.exists()) return direct

            context.filesDir.listFiles()?.forEach { dir ->
                if (dir.isDirectory) {
                    val config = File(dir, ".wavesync_config.json")
                    if (config.exists()) return config
                }
            }
            return null
        }

        /**
         * Get the current FCM token and write it to a file for Rust to read.
         * Call this from the main thread during app startup.
         */
        @JvmStatic
        fun ensureTokenFile(context: Context) {
            // Init Firebase if not already done (cold start path)
            ensureFirebaseFromConfig(context)

            try {
                val token = com.google.android.gms.tasks.Tasks.await(
                    FirebaseMessaging.getInstance().token
                )
                if (token != null) {
                    File(context.filesDir, TOKEN_FILENAME).writeText(token)
                    Log.i(TAG, "FCM token written: ${token.take(10)}...")
                }
            } catch (e: Exception) {
                Log.w(TAG, "Could not get FCM token yet: ${e.message}")
            }
        }

        @JvmStatic
        private external fun backgroundSync(databaseUrl: String, timeoutSecs: Int, peerAddrsJson: String?): Int
    }

    override fun onCreate() {
        super.onCreate()
        ensureFirebaseFromConfig(applicationContext)
    }

    override fun onMessageReceived(message: RemoteMessage) {
        if (message.data["type"] != "sync_available") return

        Log.i(TAG, "Received sync_available push, starting background sync")

        // Extract peer addresses from FCM payload (sent by relay)
        val peerAddrsJson = message.data["peer_addrs"]
        if (peerAddrsJson != null) {
            Log.i(TAG, "FCM includes peer addresses: ${peerAddrsJson.take(100)}...")
        }

        // onMessageReceived runs on a background thread with ~20s budget.
        val dbUrl = findDatabaseUrl()
        if (dbUrl == null) {
            Log.e(TAG, "No WaveSyncDB database found — has the app been launched?")
            return
        }

        ensureNativeLoaded()
        if (!nativeLoaded) return

        try {
            val result = backgroundSync(dbUrl, 25, peerAddrsJson)
            when (result) {
                0 -> Log.i(TAG, "Background sync completed successfully")
                1 -> Log.w(TAG, "Background sync: no peers found")
                2 -> Log.w(TAG, "Background sync: timed out")
                else -> Log.e(TAG, "Background sync failed with error code: $result")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Background sync exception: ${e.message}")
        }
    }

    override fun onNewToken(token: String) {
        Log.i(TAG, "FCM token refreshed: ${token.take(10)}...")
        getSharedPreferences("wavesync", MODE_PRIVATE)
            .edit()
            .putString("fcm_token", token)
            .apply()

        // Write to file so Rust can read it without JNI
        try {
            File(applicationContext.filesDir, TOKEN_FILENAME).writeText(token)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to write token file: ${e.message}")
        }
    }

    private fun findDatabaseUrl(): String? {
        val configInFiles = File(applicationContext.filesDir, ".wavesync_config.json")
        if (configInFiles.exists()) return extractDatabaseUrl(configInFiles)

        val dbDir = applicationContext.getDatabasePath("dummy").parentFile
        if (dbDir != null) {
            val configInDb = File(dbDir, ".wavesync_config.json")
            if (configInDb.exists()) return extractDatabaseUrl(configInDb)
        }

        applicationContext.filesDir.listFiles()?.forEach { dir ->
            if (dir.isDirectory) {
                val config = File(dir, ".wavesync_config.json")
                if (config.exists()) return extractDatabaseUrl(config)
            }
        }

        val dbFile = applicationContext.filesDir.listFiles()
            ?.firstOrNull { it.extension == "db" }
        return dbFile?.let { "sqlite:${it.absolutePath}?mode=rwc" }
    }

    private fun extractDatabaseUrl(configFile: File): String? {
        return try {
            val json = configFile.readText()
            Regex(""""database_url"\s*:\s*"([^"]+)"""").find(json)?.groupValues?.get(1)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to read config: ${e.message}")
            null
        }
    }
}
