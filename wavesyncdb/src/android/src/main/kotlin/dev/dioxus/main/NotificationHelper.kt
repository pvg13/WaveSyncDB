package dev.dioxus.main

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat

/**
 * Posts the user-facing notifications produced by WaveSyncDB's per-table
 * sync-notification policies (`#[derive(SyncNotify)]`).
 *
 * Called from Rust (the `use_sync_notifications` Dioxus hook) via JNI:
 * `NotificationHelper.show(context, title, body, group, deeplink)`. Kept separate from
 * [WaveSyncService]'s low-importance foreground-sync channel — these are
 * user-visible "you got new data" notifications and use a default-importance
 * channel so they can make a sound / heads-up.
 */
object NotificationHelper {
    private const val TAG = "WaveSyncNotification"
    private const val CHANNEL_ID = "wavesync_messages"

    /**
     * Application context stashed by [WaveSyncInitProvider] (which runs once per
     * process, including the FCM service process). Lets the Rust background-sync
     * path post notifications via [showFromNative] without a Context of its own.
     * Holding the *application* context is leak-safe — it lives for the process.
     */
    @Volatile
    @JvmStatic
    var appContext: Context? = null

    /**
     * Context-less entry point for the Rust background-sync notification pump,
     * which has no Activity/Context. Uses the stashed [appContext]; no-ops with
     * a log if it hasn't been set yet.
     */
    @JvmStatic
    fun showFromNative(title: String, body: String, group: String, deeplink: String) {
        val ctx = appContext
        if (ctx == null) {
            Log.w(TAG, "showFromNative: no application context stored; notification dropped")
            return
        }
        show(ctx, title, body, group, deeplink)
    }

    /**
     * Show a notification. [group] is the coalescing key: notifications sharing
     * a group replace each other (a stable notification id), so a burst for the
     * same conversation collapses to one entry instead of stacking.
     *
     * [deeplink] (empty = none) is an opaque URL: tapping the notification fires
     * an explicit ACTION_VIEW intent carrying it at the app's launcher activity,
     * which routes it itself — no manifest intent filter involved. When empty,
     * the tap just opens the app.
     */
    @JvmStatic
    fun show(context: Context, title: String, body: String, group: String, deeplink: String) {
        ensureChannel(context)

        // Stable id per group → a newer notification for the same group replaces
        // the previous one. Falls back to a time-based id when ungrouped. Also
        // used as the PendingIntent requestCode so distinct groups don't share
        // one cached PendingIntent (String.hashCode collisions aside — at the
        // handful of groups a real app has, negligible).
        val id = if (group.isNotEmpty()) group.hashCode() else System.nanoTime().toInt()

        // Small icon, in preference order: the app's declared FCM notification
        // icon (the standard monochrome resource, correct silhouette); the app
        // icon on P+ only — 8.0/8.1 crash the posting app trying to render an
        // adaptive launcher icon in the status bar ("Bad notification posted",
        // the bug FCM's own fallback special-cases); else the system glyph.
        val icon = fcmDefaultIcon(context)
            ?: context.applicationInfo.icon
                .takeIf { it != 0 && Build.VERSION.SDK_INT >= Build.VERSION_CODES.P }
            ?: android.R.drawable.stat_notify_sync

        val builder = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle(title)
            .setContentText(body)
            .setSmallIcon(icon)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setAutoCancel(true)
        if (group.isNotEmpty()) {
            builder.setGroup(group)
        }

        // Tap-to-open: an explicit launch intent, so no manifest filter is
        // needed and no app-chooser appears. A non-empty deeplink rides on the
        // same explicit intent as ACTION_VIEW data for the activity to route
        // (delivery is onNewIntent / onCreate depending on its launchMode).
        // No launcher activity (headless install) → tap does nothing, but the
        // notification still posts.
        val launch = context.packageManager.getLaunchIntentForPackage(context.packageName)
        if (launch != null) {
            if (deeplink.isNotEmpty()) {
                launch.action = Intent.ACTION_VIEW
                launch.data = Uri.parse(deeplink)
                // The LAUNCHER category is meaningless on a VIEW intent and can
                // confuse app-side routing that inspects categories.
                launch.removeCategory(Intent.CATEGORY_LAUNCHER)
                // Deliver to the running activity via onNewIntent even with the
                // default launchMode: without SINGLE_TOP|CLEAR_TOP a warm tap
                // stacks a second activity instance (a second webview + engine
                // for a Dioxus app) instead of routing in the existing one.
                launch.addFlags(
                    Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP,
                )
            }
            try {
                val pending = PendingIntent.getActivity(
                    context, id, launch,
                    PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT,
                )
                builder.setContentIntent(pending)
            } catch (e: Exception) {
                // e.g. the Android 12+ per-uid PendingIntent cap
                // (SecurityException). A tap-less notification beats none.
                Log.w(TAG, "content intent not attached: ${e.message}")
            }
        }

        try {
            NotificationManagerCompat.from(context).notify(id, builder.build())
        } catch (e: SecurityException) {
            // POST_NOTIFICATIONS not granted at runtime (Android 13+). The
            // permission is declared in the manifest, but the user can deny it.
            Log.w(TAG, "Notification not posted (permission denied): ${e.message}")
        }
    }

    /**
     * The app's `com.google.firebase.messaging.default_notification_icon`
     * manifest metadata, if declared — the standard place FCM apps register a
     * proper monochrome notification icon. Null when absent or unreadable.
     */
    private fun fcmDefaultIcon(context: Context): Int? = try {
        context.packageManager
            .getApplicationInfo(context.packageName, PackageManager.GET_META_DATA)
            .metaData
            ?.getInt("com.google.firebase.messaging.default_notification_icon", 0)
            ?.takeIf { it != 0 }
    } catch (e: Exception) {
        null
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
