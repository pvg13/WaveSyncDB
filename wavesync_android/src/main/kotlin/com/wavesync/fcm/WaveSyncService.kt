package com.wavesync.fcm

import android.content.Context
import android.util.Log
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.messaging.FirebaseMessaging
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import java.io.File

/**
 * JNI bridge to the Rust native library.
 *
 * All native calls go through this object. The JNI symbol names are derived
 * from `com.wavesync.fcm.WaveSyncNative`, so the Rust side must export:
 *   - `Java_com_wavesync_fcm_WaveSyncNative_backgroundSync`
 */
object WaveSyncNative {
    private var loaded = false

    fun ensureLoaded() {
        if (!loaded) {
            try {
                System.loadLibrary("main")
                loaded = true
            } catch (e: UnsatisfiedLinkError) {
                Log.e("WaveSyncNative", "Failed to load native library: ${e.message}")
            }
        }
    }

    /**
     * Run a one-shot background sync. Connects to peers via the relay,
     * pulls changes, and returns.
     *
     * @param databaseUrl SQLite URL, e.g. "sqlite:/data/data/com.app/files/app.db?mode=rwc"
     * @param timeoutSecs Maximum seconds to wait for sync
     * @return 0=synced, 1=no peers, 2=timed out, negative=error
     */
    @JvmStatic
    external fun backgroundSync(databaseUrl: String, timeoutSecs: Int): Int
}

/**
 * Firebase Cloud Messaging service for WaveSyncDB background sync.
 *
 * Auto-registered via manifest merging — no setup required in the host app.
 * Firebase is initialized manually from config stored in SharedPreferences,
 * so no google-services.json or Gradle plugin is needed.
 *
 * When a "sync_available" push arrives (even if the app is killed), this service:
 * 1. Calls goAsync() to extend execution time to ~30s
 * 2. Initializes Firebase (if needed, from saved SharedPreferences)
 * 3. Loads the native Rust library
 * 4. Reads the database path from the WaveSyncDB config
 * 5. Calls `WaveSyncNative.backgroundSync()` to pull changes from peers
 */
class WaveSyncService : FirebaseMessagingService() {

    companion object {
        private const val TAG = "WaveSyncService"
        const val PREFS_NAME = "wavesync"
        const val KEY_FCM_TOKEN = "fcm_token"
        private const val KEY_FIREBASE_PROJECT_ID = "firebase_project_id"
        private const val KEY_FIREBASE_APP_ID = "firebase_app_id"
        private const val KEY_FIREBASE_API_KEY = "firebase_api_key"

        /**
         * Initialize Firebase from config values.
         * Called from Rust via JNI during app startup.
         * No google-services.json or Gradle plugin needed.
         */
        @JvmStatic
        fun initFirebase(context: Context, projectId: String, appId: String, apiKey: String) {
            if (FirebaseApp.getApps(context).isNotEmpty()) {
                Log.d(TAG, "Firebase already initialized")
                return
            }

            // Save config for use when service is cold-started by FCM
            context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
                .edit()
                .putString(KEY_FIREBASE_PROJECT_ID, projectId)
                .putString(KEY_FIREBASE_APP_ID, appId)
                .putString(KEY_FIREBASE_API_KEY, apiKey)
                .apply()

            val options = FirebaseOptions.Builder()
                .setProjectId(projectId)
                .setApplicationId(appId)
                .setApiKey(apiKey)
                .build()

            FirebaseApp.initializeApp(context, options)
            Log.i(TAG, "Firebase initialized for project: $projectId")
        }

        /**
         * Initialize Firebase from saved SharedPreferences (cold start path).
         */
        private fun ensureFirebaseInitialized(context: Context): Boolean {
            if (FirebaseApp.getApps(context).isNotEmpty()) return true

            val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            val projectId = prefs.getString(KEY_FIREBASE_PROJECT_ID, null) ?: return false
            val appId = prefs.getString(KEY_FIREBASE_APP_ID, null) ?: return false
            val apiKey = prefs.getString(KEY_FIREBASE_API_KEY, null) ?: return false

            initFirebase(context, projectId, appId, apiKey)
            return true
        }

        /**
         * Get the current FCM token. Called from Rust via JNI.
         */
        @JvmStatic
        fun getToken(context: Context): String? {
            if (!ensureFirebaseInitialized(context)) return null
            return try {
                com.google.android.gms.tasks.Tasks.await(
                    FirebaseMessaging.getInstance().token
                )
            } catch (e: Exception) {
                Log.e(TAG, "Failed to get FCM token: ${e.message}")
                null
            }
        }
    }

    override fun onCreate() {
        super.onCreate()
        ensureFirebaseInitialized(applicationContext)
    }

    override fun onMessageReceived(message: RemoteMessage) {
        if (message.data["type"] != "sync_available") return

        Log.i(TAG, "Received sync_available push, starting background sync")

        // onMessageReceived runs on a background thread with ~20s budget.
        val dbUrl = findDatabaseUrl()
        if (dbUrl == null) {
            Log.e(TAG, "No WaveSyncDB database found — has the app been launched?")
            return
        }

        WaveSyncNative.ensureLoaded()

        try {
            val result = WaveSyncNative.backgroundSync(dbUrl, 15)
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
        getSharedPreferences(PREFS_NAME, MODE_PRIVATE)
            .edit()
            .putString(KEY_FCM_TOKEN, token)
            .apply()

        // Write token to a file so Rust can read it without JNI.
        // The file is placed in filesDir where the database also lives.
        writeTokenFile(token)
    }

    /**
     * Write the FCM token to a plain file that Rust can read.
     * This avoids JNI classloader issues on native threads.
     */
    private fun writeTokenFile(token: String) {
        try {
            // Write to filesDir (standard Dioxus data location)
            File(applicationContext.filesDir, "wavesync_fcm_token").writeText(token)
            Log.d(TAG, "FCM token written to wavesync_fcm_token")
        } catch (e: Exception) {
            Log.e(TAG, "Failed to write FCM token file: ${e.message}")
        }
    }

    /**
     * Find the WaveSyncDB database URL from the config file.
     * Checks standard app data directories for .wavesync_config.json.
     */
    private fun findDatabaseUrl(): String? {
        // Check filesDir first (standard Dioxus data location)
        val configInFiles = File(applicationContext.filesDir, ".wavesync_config.json")
        if (configInFiles.exists()) {
            return extractDatabaseUrl(configInFiles)
        }

        // Check databases dir (standard Android DB location)
        val dbDir = applicationContext.getDatabasePath("dummy").parentFile
        if (dbDir != null) {
            val configInDb = File(dbDir, ".wavesync_config.json")
            if (configInDb.exists()) {
                return extractDatabaseUrl(configInDb)
            }
        }

        // Fallback: walk filesDir one level deep for config in subdirectories
        applicationContext.filesDir.listFiles()?.forEach { dir ->
            if (dir.isDirectory) {
                val config = File(dir, ".wavesync_config.json")
                if (config.exists()) {
                    return extractDatabaseUrl(config)
                }
            }
        }

        // Last resort: find any .db file
        val dbFile = applicationContext.filesDir.listFiles()
            ?.firstOrNull { it.extension == "db" }
        return dbFile?.let { "sqlite:${it.absolutePath}?mode=rwc" }
    }

    private fun extractDatabaseUrl(configFile: File): String? {
        return try {
            val json = configFile.readText()
            val match = Regex(""""database_url"\s*:\s*"([^"]+)"""").find(json)
            match?.groupValues?.get(1)
        } catch (e: Exception) {
            Log.e(TAG, "Failed to read config: ${e.message}")
            null
        }
    }
}
