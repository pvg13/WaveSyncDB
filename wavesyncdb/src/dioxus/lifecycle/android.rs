use jni::objects::JObject;
use jni::JavaVM;
use tokio::sync::watch;

/// Starts listening for Android activity lifecycle transitions.
///
/// Polls the activity's `hasWindowFocus()` every second to detect
/// foreground/background transitions. This approach is simple, reliable
/// across Android versions, and works regardless of how Dioxus manages
/// the activity internally.
pub fn start_lifecycle_listener(tx: watch::Sender<bool>) {
    let ctx = ndk_context::android_context();
    let vm = match unsafe { JavaVM::from_raw(ctx.vm().cast()) } {
        Ok(vm) => vm,
        Err(e) => {
            log::error!("Failed to get JavaVM for lifecycle detection: {e}");
            return;
        }
    };
    let activity = unsafe { JObject::from_raw(ctx.context().cast()) };

    let mut env = match vm.attach_current_thread() {
        Ok(env) => env,
        Err(e) => {
            log::error!("Failed to attach JNI thread: {e}");
            return;
        }
    };

    let activity_global = match env.new_global_ref(&activity) {
        Ok(g) => g,
        Err(e) => {
            log::error!("Failed to create global ref for activity: {e}");
            return;
        }
    };

    drop(env);

    log::info!("Android lifecycle polling started");

    let mut was_foreground = true;

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));

        let Ok(mut env) = vm.attach_current_thread() else {
            continue;
        };

        let focused = env
            .call_method(activity_global.as_obj(), "hasWindowFocus", "()Z", &[])
            .and_then(|v| v.z())
            .unwrap_or(was_foreground);

        if focused != was_foreground {
            log::debug!("Android lifecycle: foreground={focused}");
            let _ = tx.send(focused);
            was_foreground = focused;
        }
    }
}
