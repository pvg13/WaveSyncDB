package dev.dioxus.main

import android.app.NotificationChannel
import android.app.NotificationManager
import android.content.Context
import android.os.Build
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat

/**
 * Posts the user-facing notifications produced by WaveSyncDB's per-table
 * sync-notification policies (`#[derive(SyncNotify)]`).
 *
 * Called from Rust (the `use_sync_notifications` Dioxus hook) via JNI:
 * `NotificationHelper.show(context, title, body, group)`. Kept separate from
 * [WaveSyncService]'s low-importance foreground-sync channel — these are
 * user-visible "you got new data" notifications and use a default-importance
 * channel so they can make a sound / heads-up.
 */
object NotificationHelper {
    private const val TAG = "WaveSyncNotification"
    private const val CHANNEL_ID = "wavesync_messages"

    /**
     * Show a notification. [group] is the coalescing key: notifications sharing
     * a group replace each other (a stable notification id), so a burst for the
     * same conversation collapses to one entry instead of stacking.
     */
    @JvmStatic
    fun show(context: Context, title: String, body: String, group: String) {
        ensureChannel(context)

        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle(title)
            .setContentText(body)
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setAutoCancel(true)
        if (group.isNotEmpty()) {
            builder.setGroup(group)
        }

        // Stable id per group → a newer notification for the same group replaces
        // the previous one. Falls back to a time-based id when ungrouped.
        val id = if (group.isNotEmpty()) group.hashCode() else System.nanoTime().toInt()

        try {
            NotificationManagerCompat.from(context).notify(id, builder.build())
        } catch (e: SecurityException) {
            // POST_NOTIFICATIONS not granted at runtime (Android 13+). The
            // permission is declared in the manifest, but the user can deny it.
            Log.w(TAG, "Notification not posted (permission denied): ${e.message}")
        }
    }

    private fun ensureChannel(context: Context) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return
        val nm = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        if (nm.getNotificationChannel(CHANNEL_ID) != null) return
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Sync updates",
            NotificationManager.IMPORTANCE_DEFAULT,
        ).apply {
            description = "Notifications for new data synced from your peers"
        }
        nm.createNotificationChannel(channel)
    }
}
