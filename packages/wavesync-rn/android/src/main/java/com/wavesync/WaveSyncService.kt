package com.wavesync

import android.util.Log
import com.google.firebase.messaging.FirebaseMessagingService
import com.google.firebase.messaging.RemoteMessage
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import uniffi.wavesyncdb_ffi.FfiBackgroundSyncResult
import uniffi.wavesyncdb_ffi.backgroundSync
import java.io.File

/**
 * Firebase Cloud Messaging service for WaveSyncDB background sync.
 *
 * When a "sync_available" push arrives (even if the app is killed), this service:
 * 1. Finds the database URL from .wavesync_config.json
 * 2. Calls the UniFFI background_sync() to pull changes from peers
 * 3. Shuts down the engine cleanly
 *
 * Register in your AndroidManifest.xml:
 * ```xml
 * <service
 *     android:name="com.wavesync.WaveSyncService"
 *     android:exported="false">
 *     <intent-filter>
 *         <action android:name="com.google.firebase.MESSAGING_EVENT" />
 *     </intent-filter>
 * </service>
 * ```
 */
class WaveSyncService : FirebaseMessagingService() {

    companion object {
        private const val TAG = "WaveSyncService"
    }

    override fun onMessageReceived(message: RemoteMessage) {
        if (message.data["type"] != "sync_available") return

        Log.i(TAG, "Received sync_available push, starting background sync")

        val peerAddrsJson = message.data["peer_addrs"]
        if (peerAddrsJson != null) {
            Log.i(TAG, "FCM includes peer addresses: ${peerAddrsJson.take(100)}...")
        }

        val dbUrl = findDatabaseUrl()
        if (dbUrl == null) {
            Log.e(TAG, "No WaveSyncDB database found — has the app been launched?")
            return
        }

        // onMessageReceived runs on a background thread with ~20s budget.
        // Use runBlocking since we're already off the main thread.
        try {
            val result = runBlocking {
                withTimeoutOrNull(25_000) {
                    backgroundSync(dbUrl, 20u, peerAddrsJson)
                }
            }
            when (result) {
                is FfiBackgroundSyncResult.Synced ->
                    Log.i(TAG, "Background sync completed: ${result.peersSynced} peers")
                is FfiBackgroundSyncResult.NoPeers ->
                    Log.w(TAG, "Background sync: no peers found")
                is FfiBackgroundSyncResult.TimedOut ->
                    Log.w(TAG, "Background sync: timed out (${result.peersSynced} peers synced)")
                null ->
                    Log.w(TAG, "Background sync: Kotlin timeout reached")
            }
        } catch (e: Exception) {
            Log.e(TAG, "Background sync exception: ${e.message}")
        }
    }

    override fun onNewToken(token: String) {
        Log.i(TAG, "FCM token refreshed: ${token.take(10)}...")

        // Persist token to SyncConfig so cold sync registers it with the relay.
        val dbUrl = findDatabaseUrl() ?: return
        try {
            val config = loadConfig(dbUrl) ?: return
            config.remove("push_platform")
            config.remove("push_token")
            val org = org.json.JSONObject()
            for (key in config.keys()) {
                org.put(key, config.get(key))
            }
            org.put("push_platform", "Fcm")
            org.put("push_token", token)
            val configPath = configPathFor(dbUrl) ?: return
            configPath.writeText(org.toString(2))
            Log.i(TAG, "FCM token persisted to config")
        } catch (e: Exception) {
            Log.w(TAG, "Failed to persist FCM token: ${e.message}")
        }
    }

    private fun findDatabaseUrl(): String? {
        // Check for config in files dir
        val configInFiles = File(applicationContext.filesDir, ".wavesync_config.json")
        if (configInFiles.exists()) return extractDatabaseUrl(configInFiles)

        // Search subdirectories
        applicationContext.filesDir.listFiles()?.forEach { dir ->
            if (dir.isDirectory) {
                val config = File(dir, ".wavesync_config.json")
                if (config.exists()) return extractDatabaseUrl(config)
            }
        }

        // Fallback: look for a .db file and construct URL
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

    private fun configPathFor(dbUrl: String): File? {
        val path = dbUrl
            .removePrefix("sqlite://")
            .removePrefix("sqlite:")
            .substringBefore("?")
        val parent = File(path).parentFile ?: return null
        return File(parent, ".wavesync_config.json")
    }

    private fun loadConfig(dbUrl: String): org.json.JSONObject? {
        val configFile = configPathFor(dbUrl) ?: return null
        return try {
            org.json.JSONObject(configFile.readText())
        } catch (e: Exception) {
            null
        }
    }
}
