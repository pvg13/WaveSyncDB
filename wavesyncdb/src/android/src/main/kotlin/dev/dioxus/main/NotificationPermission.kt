package dev.dioxus.main

import android.app.Activity
import android.app.Application
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.util.Log

/**
 * Requests the Android 13+ (API 33) `POST_NOTIFICATIONS` runtime permission so
 * the foreground-sync notification ([WaveSyncService]) and the user-visible
 * sync notifications ([NotificationHelper], driven by `#[derive(SyncNotify)]`)
 * can actually appear.
 *
 * The permission is declared in the merged manifest, but on API 33+ it is a
 * *runtime* permission: without an explicit request the system never shows the
 * popup, and `NotificationManagerCompat.notify()` throws `SecurityException`
 * (which [NotificationHelper] swallows). This mirrors the iOS side, which
 * prompts lazily via `UNUserNotificationCenter.requestAuthorization` the first
 * time a notification is shown.
 *
 * [installAutoRequest] wires a one-shot prompt on the first Activity that
 * reaches the foreground — no app-side code needed, consistent with
 * [WaveSyncInitProvider]'s auto-init. Apps that prefer to control timing (e.g.
 * show an in-app rationale first) can call [requestIfNeeded] from their own
 * Activity instead.
 */
object NotificationPermission {
    private const val TAG = "WaveSyncNotification"
    private const val PERMISSION = "android.permission.POST_NOTIFICATIONS"

    /** Request code reported to `Activity.onRequestPermissionsResult`. Kept
     *  small (< 256) to stay within every Activity base class's constraints.
     *  The result is not consumed here — granting alone is what matters. */
    const val REQUEST_CODE = 0x53

    /**
     * Request `POST_NOTIFICATIONS` from [activity] when needed. Returns `true`
     * if the permission is already held (or not required below API 33), `false`
     * if a request was issued (or could not be).
     */
    @JvmStatic
    fun requestIfNeeded(activity: Activity): Boolean {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) {
            return true // implicitly granted before Android 13
        }
        if (activity.checkSelfPermission(PERMISSION) == PackageManager.PERMISSION_GRANTED) {
            return true
        }
        return try {
            activity.requestPermissions(arrayOf(PERMISSION), REQUEST_CODE)
            false
        } catch (e: Exception) {
            Log.w(TAG, "Could not request POST_NOTIFICATIONS: ${e.message}")
            false
        }
    }

    /**
     * Register a one-shot listener that requests the permission the first time
     * any Activity resumes, then unregisters itself. Safe to call from a
     * ContentProvider at startup. No-op below API 33.
     */
    fun installAutoRequest(app: Application) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
        app.registerActivityLifecycleCallbacks(object : Application.ActivityLifecycleCallbacks {
            private var done = false
            override fun onActivityResumed(activity: Activity) {
                if (done) return
                done = true
                // Unregister before prompting so a permission-driven
                // pause/resume can't re-enter this callback.
                app.unregisterActivityLifecycleCallbacks(this)
                requestIfNeeded(activity)
            }

            override fun onActivityCreated(activity: Activity, savedInstanceState: Bundle?) {}
            override fun onActivityStarted(activity: Activity) {}
            override fun onActivityPaused(activity: Activity) {}
            override fun onActivityStopped(activity: Activity) {}
            override fun onActivitySaveInstanceState(activity: Activity, outState: Bundle) {}
            override fun onActivityDestroyed(activity: Activity) {}
        })
    }
}
