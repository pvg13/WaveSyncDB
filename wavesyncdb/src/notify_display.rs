//! Native OS notification display for the **background-sync** path.
//!
//! The foreground Dioxus hook ([`crate::dioxus::notifications`]) shows
//! notifications through `ndk_context`, which is only initialized in the UI
//! Activity's process. A push-triggered background sync runs in the
//! `FirebaseMessagingService` process, where `ndk_context` is absent — so this
//! module instead uses:
//!
//! * **Android:** the `JavaVM` captured from the background-sync JNI entry
//!   ([`store_java_vm`], called from `crate::ffi`), plus the application
//!   `Context` that `WaveSyncInitProvider` stashes on `NotificationHelper`
//!   (the provider runs once per process, including the FCM service process).
//!   Rust calls the Context-less `NotificationHelper.showFromNative`.
//! * **iOS:** the same `wavesync_show_notification` C-ABI the foreground path
//!   uses (resolved via `dlsym`; the WaveSyncPush framework is already loaded).
//!
//! Display is best-effort: every failure is logged and swallowed so a missing
//! notification never aborts the background sync.

use crate::notify::Notification;

/// Coalescing / group key for a notification: the explicit key, else
/// `table:primary_key` (mirrors the foreground hook).
fn group_of(n: &Notification) -> String {
    n.coalesce_key
        .clone()
        .unwrap_or_else(|| format!("{}:{}", n.table, n.primary_key))
}

#[cfg(target_os = "android")]
mod imp {
    use std::sync::OnceLock;

    /// `JavaVM` captured from the background-sync JNI entry. The FCM service
    /// process has no `ndk_context`, but the VM is process-global and lets us
    /// attach a tokio worker thread to call back into Kotlin.
    static JAVA_VM: OnceLock<jni::JavaVM> = OnceLock::new();

    pub(super) fn store_java_vm(env: &jni::JNIEnv) {
        if JAVA_VM.get().is_some() {
            return;
        }
        match env.get_java_vm() {
            Ok(vm) => {
                let _ = JAVA_VM.set(vm);
            }
            Err(e) => log::warn!("notify_display: could not capture JavaVM: {e}"),
        }
    }

    pub(super) fn show(title: &str, body: &str, group: &str) {
        use jni::objects::JValue;

        let Some(vm) = JAVA_VM.get() else {
            log::warn!(
                "notify_display: no JavaVM captured — background notification skipped \
                 (background sync not entered via JNI?)"
            );
            return;
        };
        let mut env = match vm.attach_current_thread() {
            Ok(env) => env,
            Err(e) => {
                log::warn!("notify_display: JNI attach failed: {e}");
                return;
            }
        };
        let (Ok(t), Ok(b), Ok(g)) = (
            env.new_string(title),
            env.new_string(body),
            env.new_string(group),
        ) else {
            log::warn!("notify_display: failed to build JNI strings");
            return;
        };
        match env.call_static_method(
            "dev/dioxus/main/NotificationHelper",
            "showFromNative",
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
            &[JValue::Object(&t), JValue::Object(&b), JValue::Object(&g)],
        ) {
            Ok(_) => log::debug!("notify_display: NotificationHelper.showFromNative ok"),
            Err(e) => {
                log::warn!("notify_display: showFromNative failed: {e}");
                let _ = env.exception_clear();
            }
        }
    }
}

#[cfg(target_os = "ios")]
mod imp {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_void};

    pub(super) fn show(title: &str, body: &str, group: &str) {
        unsafe extern "C" {
            fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
        }
        const RTLD_DEFAULT: *mut c_void = (-2isize) as *mut c_void;
        type ShowFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char);

        let (Ok(t), Ok(b), Ok(g)) = (
            CString::new(title),
            CString::new(body),
            CString::new(group),
        ) else {
            return;
        };
        unsafe {
            let sym = dlsym(RTLD_DEFAULT, c"wavesync_show_notification".as_ptr());
            if sym.is_null() {
                log::warn!("notify_display: wavesync_show_notification not found");
                return;
            }
            let show: ShowFn = std::mem::transmute(sym);
            show(t.as_ptr(), b.as_ptr(), g.as_ptr());
        }
    }
}

// Desktop / other targets that happen to enable `push-sync`: no native channel
// from a headless context, so this is a no-op (the foreground hook covers
// desktop via notify-rust).
#[cfg(not(any(target_os = "android", target_os = "ios")))]
mod imp {
    pub(super) fn show(_title: &str, _body: &str, _group: &str) {}
}

/// Display `n` as a native OS notification from a headless (background-sync)
/// context. Best-effort; never panics.
pub(crate) fn show_background(n: &Notification) {
    imp::show(&n.title, &n.body, &group_of(n));
}

/// Capture the `JavaVM` from the background-sync JNI entry so [`show_background`]
/// can call back into Kotlin without `ndk_context`. Android-only; called from
/// `crate::ffi`'s background-sync entry points.
#[cfg(target_os = "android")]
pub(crate) fn store_java_vm(env: &jni::JNIEnv) {
    imp::store_java_vm(env);
}
