package dev.dioxus.main

import android.app.Application
import android.content.ContentProvider
import android.content.ContentValues
import android.content.Context
import android.database.Cursor
import android.net.Uri
import android.net.wifi.WifiManager
import android.util.Log

/**
 * Auto-initializes Firebase and writes the FCM token on app startup.
 *
 * ContentProviders run before Application.onCreate() and before any Activity,
 * ensuring the token file exists by the time Rust code reads it during
 * WaveSyncDbBuilder::build().
 *
 * Registered via manifest merging from the Android module — no app-side setup needed.
 */
class WaveSyncInitProvider : ContentProvider() {

    companion object {
        private const val TAG = "WaveSyncInitProvider"

        /// Held for the process lifetime so libp2p mDNS works in the FOREGROUND
        /// app, not just during background sync. Android silently filters
        /// incoming/outgoing Wi-Fi multicast unless a `MulticastLock` is held —
        /// the only one the library took was the per-sync lock in
        /// [WaveSyncService], so a foregrounded app could neither answer nor
        /// hear `_p2p._udp` mDNS and never discovered LAN peers, forcing every
        /// same-Wi-Fi sync (e.g. desktop ↔ phone) onto the relay. Static so it
        /// outlives `onCreate` and isn't garbage-collected.
        @Volatile
        private var multicastLock: WifiManager.MulticastLock? = null

        private fun acquireMulticastLock(context: Context) {
            if (multicastLock?.isHeld == true) return
            try {
                val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
                if (wm == null) {
                    Log.w(TAG, "WifiManager unavailable; foreground mDNS may not work")
                    return
                }
                multicastLock = wm.createMulticastLock("wavesync.mdns.foreground").apply {
                    setReferenceCounted(false)
                    acquire()
                }
                Log.i(TAG, "Acquired foreground mDNS multicast lock")
            } catch (e: Exception) {
                // Best-effort: without it mDNS won't work, but the relay/
                // rendezvous path still functions.
                Log.w(TAG, "Could not acquire foreground multicast lock: ${e.message}")
            }
        }
    }

    override fun onCreate(): Boolean {
        val ctx = context ?: return false
        Log.i("WaveSyncInitProvider", "Initializing Firebase and writing FCM token")

        // Stash the application context so the Rust background-sync notification
        // pump (which runs in this FCM service process, with no Activity) can
        // post notifications via NotificationHelper.showFromNative. Runs in every
        // process because ContentProviders initialize per-process at startup.
        NotificationHelper.appContext = ctx.applicationContext

        // Hold a multicast lock for the foreground app so libp2p mDNS can
        // discover LAN peers directly (and stay off the relay on a shared
        // Wi-Fi). Without it Android filters multicast outside the brief
        // background-sync windows that WaveSyncService covers.
        acquireMulticastLock(ctx)

        // Run token fetch on a background thread to avoid blocking app startup,
        // but it will still complete before most Rust code runs.
        Thread {
            WaveSyncService.ensureTokenFile(ctx)
        }.start()

        // Prompt for the Android 13+ notification permission on first foreground
        // (a ContentProvider has no Activity, so we defer to the first one that
        // resumes). Without this the manifest's POST_NOTIFICATIONS is never
        // requested and both the foreground-sync notification and SyncNotify
        // notifications are silently dropped on API 33+.
        (ctx.applicationContext as? Application)?.let {
            NotificationPermission.installAutoRequest(it)
        } ?: Log.w(
            "WaveSyncInitProvider",
            "App context is not an Application; notification permission must be " +
                "requested manually via NotificationPermission.requestIfNeeded(activity)"
        )

        return true
    }

    // ContentProvider contract — not used, just needed for the auto-init hook
    override fun query(uri: Uri, proj: Array<String>?, sel: String?, args: Array<String>?, sort: String?): Cursor? = null
    override fun getType(uri: Uri): String? = null
    override fun insert(uri: Uri, values: ContentValues?): Uri? = null
    override fun delete(uri: Uri, sel: String?, args: Array<String>?): Int = 0
    override fun update(uri: Uri, values: ContentValues?, sel: String?, args: Array<String>?): Int = 0
}
