use std::ptr::NonNull;
use std::sync::OnceLock;

use objc2::runtime::AnyObject;

/// Cached APNs token, set by the injected ObjC delegate callback.
/// Available for synchronous reads (e.g. during `build()`).
static APNS_TOKEN: OnceLock<String> = OnceLock::new();

/// Callback invoked when the APNs token arrives.
/// Captures a `WaveSyncDb` clone to call `register_push_token()` directly.
static TOKEN_CALLBACK: OnceLock<Box<dyn Fn(String) + Send + Sync>> = OnceLock::new();

/// Returns the cached APNs token if the ObjC callback has already fired.
///
/// Used by `WaveSyncDbBuilder::build()` to pull the token without file I/O.
pub fn cached_apns_token() -> Option<String> {
    APNS_TOKEN.get().cloned()
}

/// Set up automatic APNs token retrieval by injecting delegate methods
/// into tao's existing `UIApplicationDelegate` at runtime.
///
/// When iOS delivers the token, the injected method stores it in a Rust
/// static and invokes `callback` to register it with the running engine.
///
/// # Arguments
///
/// * `callback` — called with the hex-encoded APNs token when it arrives.
///   Typically calls `db.register_push_token("Apns", &token)`.
pub fn setup_push_token(callback: Box<dyn Fn(String) + Send + Sync>) {
    TOKEN_CALLBACK.set(callback).ok();
    unsafe { inject_and_register() };
}

unsafe fn inject_and_register() {
    use objc2::ffi;
    use objc2::rc::Retained;
    use objc2::runtime::{AnyClass, Imp};
    use objc2::sel;
    use objc2_ui_kit::UIApplication;

    // Must be on main thread for UIKit APIs.
    let Some(mtm) = objc2::MainThreadMarker::new() else {
        log::error!("setup_push_token must be called from the main thread");
        return;
    };

    let app = UIApplication::sharedApplication(mtm);

    // Get tao's app delegate and its class so we can add methods to it.
    let Some(delegate) = (unsafe { app.delegate() }) else {
        log::error!("No UIApplicationDelegate found — cannot inject push methods");
        return;
    };

    let delegate_obj: &AnyObject = Retained::as_ref(&delegate);
    let cls = delegate_obj.class() as *const AnyClass as *mut AnyClass;

    // Inject application:didRegisterForRemoteNotificationsWithDeviceToken:
    // Type encoding: v@:@@ (void return, self, _cmd, UIApplication, NSData)
    let sel_register = sel!(application:didRegisterForRemoteNotificationsWithDeviceToken:);
    let imp_register: Imp =
        unsafe { std::mem::transmute(did_register_for_remote_notifications as usize) };
    let added = ffi::class_addMethod(cls.cast(), sel_register, imp_register, c"v@:@@".as_ptr());
    if added.as_bool() {
        log::info!("Injected didRegisterForRemoteNotificationsWithDeviceToken: into app delegate");
    } else {
        log::warn!(
            "App delegate already implements didRegisterForRemoteNotificationsWithDeviceToken: — \
             skipping injection. Ensure the existing implementation writes the APNs token to \
             the WaveSyncDB engine."
        );
    }

    // Inject application:didFailToRegisterForRemoteNotificationsWithError:
    let sel_fail = sel!(application:didFailToRegisterForRemoteNotificationsWithError:);
    let imp_fail: Imp = unsafe { std::mem::transmute(did_fail_to_register as usize) };
    ffi::class_addMethod(cls.cast(), sel_fail, imp_fail, c"v@:@@".as_ptr());

    // Trigger the registration flow. iOS will call back on the delegate.
    app.registerForRemoteNotifications();
    log::info!("Called registerForRemoteNotifications()");
}

/// ObjC callback: `application:didRegisterForRemoteNotificationsWithDeviceToken:`
///
/// Hex-encodes the device token, caches it in a static, and invokes the
/// registered callback to deliver it to the running engine.
unsafe extern "C" fn did_register_for_remote_notifications(
    _this: &AnyObject,
    _cmd: objc2::runtime::Sel,
    _application: NonNull<AnyObject>,
    device_token: NonNull<objc2_foundation::NSData>,
) {
    let data = unsafe { device_token.as_ref() };
    let bytes = unsafe { data.as_bytes_unchecked() };
    let hex_token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    let preview = &hex_token[..hex_token.len().min(10)];
    log::info!("APNs device token received: {preview}...");

    // Cache for synchronous access (e.g. build() pulling the token).
    APNS_TOKEN.set(hex_token.clone()).ok();

    // Notify the running engine via the registered callback.
    if let Some(cb) = TOKEN_CALLBACK.get() {
        cb(hex_token);
    }
}

/// ObjC callback: `application:didFailToRegisterForRemoteNotificationsWithError:`
///
/// Logs the error. On simulator this is expected (no APNs support).
unsafe extern "C" fn did_fail_to_register(
    _this: &AnyObject,
    _cmd: objc2::runtime::Sel,
    _application: NonNull<AnyObject>,
    error: NonNull<AnyObject>,
) {
    // Cast to NSError to get the description.
    let error: &objc2_foundation::NSError =
        unsafe { &*(error.as_ptr() as *const objc2_foundation::NSError) };
    let desc = error.localizedDescription();
    log::warn!("Failed to register for remote notifications: {desc}");
}
