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

    /// Global ref to `dev.dioxus.main.NotificationHelper`, resolved on the JNI
    /// entry thread. `show()` runs on a runtime-spawned worker thread, where
    /// `FindClass` resolves against the system classloader — which cannot see
    /// app classes, so a name lookup there fails with ClassNotFoundException
    /// ("Java exception was thrown") and the notification is silently dropped.
    /// The JNI entry thread is JVM-created and carries the app classloader, so
    /// we resolve the class there and hold a global ref for the worker to use.
    static HELPER_CLASS: OnceLock<jni::objects::GlobalRef> = OnceLock::new();

    pub(super) fn store_java_vm(env: &mut jni::JNIEnv) {
        if JAVA_VM.get().is_none() {
            match env.get_java_vm() {
                Ok(vm) => {
                    let _ = JAVA_VM.set(vm);
                }
                Err(e) => tracing::warn!("notify_display: could not capture JavaVM: {e}"),
            }
        }
        // Cache the class from this (JVM-created, app-classloader) thread.
        if HELPER_CLASS.get().is_none() {
            match env
                .find_class("dev/dioxus/main/NotificationHelper")
                .and_then(|c| env.new_global_ref(c))
            {
                Ok(g) => {
                    let _ = HELPER_CLASS.set(g);
                }
                Err(e) => {
                    tracing::warn!("notify_display: could not cache NotificationHelper class: {e}");
                    let _ = env.exception_clear();
                }
            }
        }
    }

    pub(super) fn show(title: &str, body: &str, group: &str, deeplink: &str) {
        use jni::objects::{JClass, JValue};

        let Some(vm) = JAVA_VM.get() else {
            tracing::warn!(
                "notify_display: no JavaVM captured — background notification skipped \
                 (background sync not entered via JNI?)"
            );
            return;
        };
        let mut env = match vm.attach_current_thread() {
            Ok(env) => env,
            Err(e) => {
                tracing::warn!("notify_display: JNI attach failed: {e}");
                return;
            }
        };
        let (Ok(t), Ok(b), Ok(g), Ok(d)) = (
            env.new_string(title),
            env.new_string(body),
            env.new_string(group),
            env.new_string(deeplink),
        ) else {
            tracing::warn!("notify_display: failed to build JNI strings");
            return;
        };
        let args = [
            JValue::Object(&t),
            JValue::Object(&b),
            JValue::Object(&g),
            JValue::Object(&d),
        ];
        const SIG: &str =
            "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V";

        // Use the class cached on the JNI entry thread (correct classloader).
        // Fall back to a name lookup only if caching failed — that path works
        // solely on JVM-created threads, but it's better than nothing.
        let result = match HELPER_CLASS.get() {
            Some(global) => {
                // SAFETY: `global` is a live global ref to the class for the
                // process lifetime; we only borrow its handle for this call and
                // never free it here.
                let class = unsafe { JClass::from_raw(global.as_raw()) };
                env.call_static_method(&class, "showFromNative", SIG, &args)
            }
            None => env.call_static_method(
                "dev/dioxus/main/NotificationHelper",
                "showFromNative",
                SIG,
                &args,
            ),
        };
        match result {
            Ok(_) => tracing::debug!("notify_display: NotificationHelper.showFromNative ok"),
            Err(e) => {
                tracing::warn!("notify_display: showFromNative failed: {e}");
                let _ = env.exception_clear();
            }
        }
    }
}

#[cfg(target_os = "ios")]
mod imp {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_void};

    // iOS display plumbing for the deeplink is a follow-up; the field is
    // carried on the struct only.
    pub(super) fn show(title: &str, body: &str, group: &str, _deeplink: &str) {
        unsafe extern "C" {
            fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
        }
        const RTLD_DEFAULT: *mut c_void = (-2isize) as *mut c_void;
        type ShowFn = unsafe extern "C" fn(*const c_char, *const c_char, *const c_char);

        let (Ok(t), Ok(b), Ok(g)) = (CString::new(title), CString::new(body), CString::new(group))
        else {
            return;
        };
        unsafe {
            let sym = dlsym(RTLD_DEFAULT, c"wavesync_show_notification".as_ptr());
            if sym.is_null() {
                tracing::warn!("notify_display: wavesync_show_notification not found");
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
    pub(super) fn show(_title: &str, _body: &str, _group: &str, _deeplink: &str) {}
}

/// Display `n` as a native OS notification from a headless (background-sync)
/// context. Best-effort; never panics.
pub(crate) fn show_background(n: &Notification) {
    imp::show(
        &n.title,
        &n.body,
        &group_of(n),
        n.deeplink.as_deref().unwrap_or(""),
    );
}

/// Capture the `JavaVM` from the background-sync JNI entry so [`show_background`]
/// can call back into Kotlin without `ndk_context`. Android-only; called from
/// `crate::ffi`'s background-sync entry points.
#[cfg(target_os = "android")]
pub(crate) fn store_java_vm(env: &mut jni::JNIEnv) {
    imp::store_java_vm(env);
}
