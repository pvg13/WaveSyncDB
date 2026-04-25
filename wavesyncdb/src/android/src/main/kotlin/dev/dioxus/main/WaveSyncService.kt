package dev.dioxus.main

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.util.Log
import androidx.core.app.NotificationCompat
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

    /// Held while the foreground sync is running so that mDNS multicast
    /// sends actually go out on Wi-Fi. Without it, Android silently drops
    /// outgoing multicast and `libp2p_mdns` logs `Operation not permitted`.
    /// Released in `stopSyncForeground()`.
    private var multicastLock: WifiManager.MulticastLock? = null

    companion object {
        private const val TAG = "WaveSyncService"
        private const val TOKEN_FILENAME = "wavesync_fcm_token"
        private const val FG_NOTIFICATION_CHANNEL_ID = "wavesync_sync"
        private const val FG_NOTIFICATION_ID = 0xC0DEC0DE.toInt()
        private const val MULTICAST_LOCK_TAG = "wavesync.mdns"

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

        // Promote ourselves to a foreground service BEFORE doing anything else.
        // Without this, Android's background-app restrictions deny socket I/O
        // (DNS, TCP, mDNS multicast) with EPERM / ENETUNREACH and the engine
        // can't reach the relay or peers. FCM-triggered foreground starts are
        // explicitly exempted from the Android 12+ background-start ban; see
        // developer.android.com/about/versions/12/foreground-services
        // #background-start-exceptions.
        try {
            startSyncForeground()
        } catch (e: Exception) {
            Log.e(TAG, "Failed to enter foreground: ${e.message}")
            // Continue anyway — on older OS versions or unusual states the
            // service may still have network. Better to attempt sync than skip.
        }

        // Extract peer addresses from FCM payload (sent by relay)
        val peerAddrsJson = message.data["peer_addrs"]
        if (peerAddrsJson != null) {
            Log.i(TAG, "FCM includes peer addresses: ${peerAddrsJson.take(100)}...")
        }

        val dbUrl = findDatabaseUrl()
        if (dbUrl == null) {
            Log.e(TAG, "No WaveSyncDB database found — has the app been launched?")
            stopSyncForeground()
            return
        }

        ensureNativeLoaded()
        if (!nativeLoaded) {
            stopSyncForeground()
            return
        }

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
        } finally {
            stopSyncForeground()
        }
    }

    /**
     * Promote this service to the foreground. Required for FCM-triggered
     * sync because regular background services on modern Android are
     * heavily restricted from network I/O. The notification is intentionally
     * minimal — Android requires *some* notification while a service runs
     * in the foreground, but most users won't see it for the brief sync
     * window (≤25s) and dismissing it has no effect on the sync.
     */
    private fun startSyncForeground() {
        ensureNotificationChannel()
        val notification: Notification = NotificationCompat.Builder(this, FG_NOTIFICATION_CHANNEL_ID)
            .setContentTitle("Syncing")
            .setContentText("Pulling latest changes…")
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .setOngoing(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .build()

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            // Android 14+ requires the service-type argument and matching
            // permission `FOREGROUND_SERVICE_DATA_SYNC`.
            startForeground(
                FG_NOTIFICATION_ID,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC,
            )
        } else {
            startForeground(FG_NOTIFICATION_ID, notification)
        }

        acquireMulticastLock()
    }

    private fun stopSyncForeground() {
        releaseMulticastLock()
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                stopForeground(STOP_FOREGROUND_REMOVE)
            } else {
                @Suppress("DEPRECATION")
                stopForeground(true)
            }
        } catch (e: Exception) {
            Log.w(TAG, "stopForeground failed: ${e.message}")
        }
    }

    /**
     * Acquire a [WifiManager.MulticastLock] so that libp2p mDNS can actually
     * send packets while the service runs. Android silently filters
     * outgoing multicast on Wi-Fi unless a multicast lock is held; the
     * symptom in `wavesync` logs is `libp2p_mdns: error sending packet on
     * iface ... Operation not permitted` repeated every few seconds.
     *
     * Held only for the duration of one sync (paired with the foreground-
     * service notification). Idempotent — safe to call multiple times.
     */
    private fun acquireMulticastLock() {
        if (multicastLock?.isHeld == true) return
        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm == null) {
                Log.w(TAG, "WifiManager unavailable; mDNS multicast may fail")
                return
            }
            val lock = wm.createMulticastLock(MULTICAST_LOCK_TAG).apply {
                setReferenceCounted(false)
                acquire()
            }
            multicastLock = lock
        } catch (e: Exception) {
            // Acquisition is best-effort — if the device denies it (no Wi-Fi,
            // or some OEM lockdown), the engine still functions, it just
            // won't have working mDNS.
            Log.w(TAG, "Could not acquire MulticastLock: ${e.message}")
        }
    }

    private fun releaseMulticastLock() {
        try {
            multicastLock?.takeIf { it.isHeld }?.release()
        } catch (e: Exception) {
            Log.w(TAG, "MulticastLock.release() failed: ${e.message}")
        } finally {
            multicastLock = null
        }
    }

    private fun ensureNotificationChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val nm = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        if (nm.getNotificationChannel(FG_NOTIFICATION_CHANNEL_ID) != null) return
        val channel = NotificationChannel(
            FG_NOTIFICATION_CHANNEL_ID,
            "Sync",
            NotificationManager.IMPORTANCE_LOW,
        ).apply {
            description = "Brief notification shown while WaveSyncDB pulls changes from peers"
            setShowBadge(false)
        }
        nm.createNotificationChannel(channel)
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
