use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::OnceLock;

use objc2::runtime::AnyObject;

/// Directory where the APNs token file will be written.
/// Set once before calling `registerForRemoteNotifications()`.
static TOKEN_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Set up automatic APNs token writing by injecting delegate methods
/// into tao's existing `UIApplicationDelegate` at runtime.
///
/// This is the iOS equivalent of Android's `WaveSyncInitProvider`:
/// - Injects `application:didRegisterForRemoteNotificationsWithDeviceToken:` into the app delegate
/// - Calls `UIApplication.registerForRemoteNotifications()`
/// - When iOS delivers the token, the injected method writes it to
///   `{token_dir}/wavesync_apns_token` — the same file that
///   [`WaveSyncDbBuilder::build()`] reads with its retry loop.
///
/// # Arguments
///
/// * `token_dir` — directory where the token file should be written
///   (same directory as the database file).
pub fn setup_push_token_writer(token_dir: PathBuf) {
    TOKEN_DIR.set(token_dir).ok();
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
        log::error!("setup_push_token_writer must be called from the main thread");
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
             the wavesync_apns_token file."
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
/// Hex-encodes the device token and writes it to `{TOKEN_DIR}/wavesync_apns_token`.
unsafe extern "C" fn did_register_for_remote_notifications(
    _this: &AnyObject,
    _cmd: objc2::runtime::Sel,
    _application: NonNull<AnyObject>,
    device_token: NonNull<objc2_foundation::NSData>,
) {
    let data = unsafe { device_token.as_ref() };
    let bytes = unsafe { data.as_bytes_unchecked() };
    let hex_token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();

    let Some(dir) = TOKEN_DIR.get() else {
        log::error!("TOKEN_DIR not set — cannot write APNs token file");
        return;
    };

    let path = dir.join(crate::push::APNS_TOKEN_FILENAME);
    match std::fs::write(&path, &hex_token) {
        Ok(()) => {
            let preview = &hex_token[..hex_token.len().min(10)];
            log::info!("APNs token written to {}: {preview}...", path.display());
        }
        Err(e) => {
            log::error!("Failed to write APNs token file {}: {e}", path.display());
        }
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
