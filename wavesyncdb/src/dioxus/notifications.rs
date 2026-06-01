//! Reactive hook that turns per-table `#[derive(SyncNotify)]` policies into
//! user-visible notifications.
//!
//! [`use_sync_notifications`] subscribes to
//! [`WaveSyncDb::notification_rx`](crate::WaveSyncDb::notification_rx), shows each
//! [`Notification`] via the platform's native mechanism, and returns a
//! `Signal<Vec<Notification>>` of recent items for optional in-app rendering
//! (toasts). Use [`use_sync_notifications_with`] to supply a custom display
//! function instead of (or in addition to) the built-in OS display.
//!
//! ```ignore
//! fn App() -> Element {
//!     let db = use_wavesync();
//!     let _recent = use_sync_notifications(db);   // fire OS notifications
//!     rsx! { /* ... */ }
//! }
//! ```
//!
//! Platform display: desktop (Linux/macOS/Windows) uses `notify-rust`. On
//! Android/iOS native display is a follow-up — the notification is logged and
//! still delivered to the returned signal, so apps can render it themselves or
//! pass a custom callback to [`use_sync_notifications_with`].

use std::rc::Rc;

use dioxus::prelude::*;

use crate::Notification;
use crate::WaveSyncDb;

/// Max recent notifications retained in the returned signal (for in-app toasts).
const RECENT_CAP: usize = 32;

/// Subscribe to sync notifications, show each via the OS, and expose recent ones.
///
/// The returned signal holds up to the last [`RECENT_CAP`] notifications, newest
/// last — handy for rendering in-app toasts when the app is foregrounded.
pub fn use_sync_notifications(db: WaveSyncDb) -> Signal<Vec<Notification>> {
    use_sync_notifications_with(db, show_os_notification)
}

/// Like [`use_sync_notifications`] but invokes `display` for each notification
/// instead of the built-in OS display — use it to render a custom in-app toast,
/// or to reach a platform the defaults don't cover.
pub fn use_sync_notifications_with(
    db: WaveSyncDb,
    display: impl Fn(&Notification) + 'static,
) -> Signal<Vec<Notification>> {
    // Store the display fn once so the effect can clone a handle each run.
    let display = use_hook(|| Rc::new(display));
    let recent = use_signal(Vec::<Notification>::new);

    use_effect(move || {
        let mut rx = db.notification_rx();
        let display = display.clone();
        let mut recent = recent;
        spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(n) => {
                        display(&n);
                        let mut list = recent.write();
                        list.push(n);
                        if list.len() > RECENT_CAP {
                            list.remove(0);
                        }
                    }
                    // A dropped notification is acceptable for a UX channel.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    });

    recent
}

/// Show a notification using the platform's native mechanism.
fn show_os_notification(n: &Notification) {
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
    {
        if let Err(e) = notify_rust::Notification::new()
            .summary(&n.title)
            .body(&n.body)
            .show()
        {
            log::warn!("Failed to show desktop notification: {e}");
        }
    }

    // Native mobile display needs the bundled Kotlin/Swift code, which only the
    // `push-sync` feature ships. Without it (dioxus-only mobile builds) we fall
    // back to logging; the notification still reaches the returned signal for
    // in-app toasts, and apps can pass a custom callback to
    // `use_sync_notifications_with`.
    #[cfg(all(target_os = "android", feature = "push-sync"))]
    show_android_notification(&n.title, &n.body, &notification_group(n));

    #[cfg(all(target_os = "ios", feature = "push-sync"))]
    show_ios_notification(&n.title, &n.body, &notification_group(n));

    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        all(target_os = "android", feature = "push-sync"),
        all(target_os = "ios", feature = "push-sync"),
    )))]
    {
        log::info!(
            "sync notification (no native display on this target — enable `push-sync` on mobile): {} — {}",
            n.title,
            n.body
        );
    }
}

/// Display/coalescing group key for native notifications: the policy's
/// `coalesce_key`, else `table:primary_key`. Used as the notification
/// thread/group id so repeats for the same conversation replace in place.
#[cfg(any(
    all(target_os = "android", feature = "push-sync"),
    all(target_os = "ios", feature = "push-sync"),
))]
fn notification_group(n: &Notification) -> String {
    n.coalesce_key
        .clone()
        .unwrap_or_else(|| format!("{}:{}", n.table, n.primary_key))
}

/// Post an Android notification by calling the bundled Kotlin
/// `NotificationHelper.show(context, title, body, group)` over JNI.
///
/// Runs on a plain Rust thread (not a JNI callback), so it bootstraps the
/// `JavaVM` + `Context` via `ndk_context` — the same pattern as the lifecycle
/// listener. Failures are logged, never fatal.
#[cfg(all(target_os = "android", feature = "push-sync"))]
fn show_android_notification(title: &str, body: &str, group: &str) {
    use jni::JavaVM;
    use jni::objects::{JObject, JValue};

    let ctx = ndk_context::android_context();
    let vm = match unsafe { JavaVM::from_raw(ctx.vm().cast()) } {
        Ok(vm) => vm,
        Err(e) => {
            log::warn!("notification: JavaVM unavailable: {e}");
            return;
        }
    };
    let mut env = match vm.attach_current_thread() {
        Ok(env) => env,
        Err(e) => {
            log::warn!("notification: JNI attach failed: {e}");
            return;
        }
    };
    log::info!("notification: posting Android notification via NotificationHelper.show (group={group})");
    let context = unsafe { JObject::from_raw(ctx.context().cast()) };
    let (Ok(title_j), Ok(body_j), Ok(group_j)) = (
        env.new_string(title),
        env.new_string(body),
        env.new_string(group),
    ) else {
        log::warn!("notification: failed to build JNI strings");
        return;
    };
    match env.call_static_method(
        "dev/dioxus/main/NotificationHelper",
        "show",
        "(Landroid/content/Context;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&context),
            JValue::Object(&title_j),
            JValue::Object(&body_j),
            JValue::Object(&group_j),
        ],
    ) {
        Ok(_) => log::debug!("notification: NotificationHelper.show returned ok"),
        Err(e) => {
            log::warn!("notification: NotificationHelper.show failed: {e}");
            // Clear any pending Java exception so it can't leak into later JNI calls.
            let _ = env.exception_clear();
        }
    }
}

/// Post an iOS notification by calling the bundled Swift
/// `wavesync_show_notification` C-ABI helper.
///
/// The WaveSyncPush framework is `dlopen`'d at startup
/// (`push::load_ios_push_framework`), so the symbol is resolvable via
/// `RTLD_DEFAULT`. Failures are logged, never fatal.
#[cfg(all(target_os = "ios", feature = "push-sync"))]
fn show_ios_notification(title: &str, body: &str, group: &str) {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_void};

    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    }
    // Darwin: searches all globally-loaded images, incl. the dlopen'd framework.
    const RTLD_DEFAULT: *mut c_void = (-2isize) as *mut c_void;
    type ShowFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char);

    let (Ok(title_c), Ok(body_c), Ok(group_c)) =
        (CString::new(title), CString::new(body), CString::new(group))
    else {
        return;
    };

    unsafe {
        let sym = dlsym(RTLD_DEFAULT, c"wavesync_show_notification".as_ptr());
        if sym.is_null() {
            log::warn!(
                "notification: wavesync_show_notification not found \
                 (WaveSyncPush framework not loaded?)"
            );
            return;
        }
        let show: ShowFn = std::mem::transmute(sym);
        show(title_c.as_ptr(), body_c.as_ptr(), group_c.as_ptr());
    }
}
