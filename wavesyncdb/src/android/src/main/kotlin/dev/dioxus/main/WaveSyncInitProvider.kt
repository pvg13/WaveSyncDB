package dev.dioxus.main

import android.content.ContentProvider
import android.content.ContentValues
import android.database.Cursor
import android.net.Uri
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

    override fun onCreate(): Boolean {
        val ctx = context ?: return false
        Log.i("WaveSyncInitProvider", "Initializing Firebase and writing FCM token")

        // Run token fetch on a background thread to avoid blocking app startup,
        // but it will still complete before most Rust code runs.
        Thread {
            WaveSyncService.ensureTokenFile(ctx)
        }.start()

        return true
    }

    // ContentProvider contract — not used, just needed for the auto-init hook
    override fun query(uri: Uri, proj: Array<String>?, sel: String?, args: Array<String>?, sort: String?): Cursor? = null
    override fun getType(uri: Uri): String? = null
    override fun insert(uri: Uri, values: ContentValues?): Uri? = null
    override fun delete(uri: Uri, sel: String?, args: Array<String>?): Int = 0
    override fun update(uri: Uri, values: ContentValues?, sel: String?, args: Array<String>?): Int = 0
}
