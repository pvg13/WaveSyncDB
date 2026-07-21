use jni::JavaVM;
use jni::objects::JObject;
use tokio::sync::watch;

/// Starts listening for Android activity lifecycle transitions.
///
/// Polls the activity's `hasWindowFocus()` every second to detect
/// foreground/background transitions. This approach is simple, reliable
/// across Android versions, and works regardless of how Dioxus manages
/// the activity internally.
///
/// The same poll also watches the DEFAULT NETWORK's identity (#112):
/// `ConnectivityManager.getActiveNetwork().getNetworkHandle()`. A changed
/// handle while foregrounded means the device migrated networks (e.g.
/// WiFi → cellular) under a live app — the one case no lifecycle edge
/// covers, which used to leave sync on the reactive dead-socket path for
/// ~36 s (measured on-device). Each detected change bumps the counter on
/// `net_tx`; the auto-resume driver turns it into a `NetworkTransition`.
/// Same-network WiFi roaming keeps its `Network` handle, so this is
/// debounced against AP hops by construction; a loss (`Some → None`) is
/// not fired on its own — the fire happens when a *different* network
/// takes over (`None → Some(new)` or `Some(a) → Some(b)`), which is the
/// moment a reconnect can actually succeed.
pub fn start_lifecycle_listener(tx: watch::Sender<bool>, net_tx: watch::Sender<u64>) {
    let ctx = ndk_context::android_context();
    let vm = match unsafe { JavaVM::from_raw(ctx.vm().cast()) } {
        Ok(vm) => vm,
        Err(e) => {
            tracing::error!("Failed to get JavaVM for lifecycle detection: {e}");
            return;
        }
    };
    let activity = unsafe { JObject::from_raw(ctx.context().cast()) };

    let env = match vm.attach_current_thread() {
        Ok(env) => env,
        Err(e) => {
            tracing::error!("Failed to attach JNI thread: {e}");
            return;
        }
    };

    let activity_global = match env.new_global_ref(&activity) {
        Ok(g) => g,
        Err(e) => {
            tracing::error!("Failed to create global ref for activity: {e}");
            return;
        }
    };

    // ConnectivityManager, resolved once and held as a global ref. A
    // failure here (headless contexts) disables only the network watch —
    // lifecycle polling continues.
    let connectivity_global = vm.attach_current_thread().ok().and_then(|mut env| {
        let service_name = env.new_string("connectivity").ok()?;
        let cm = env
            .call_method(
                activity_global.as_obj(),
                "getSystemService",
                "(Ljava/lang/String;)Ljava/lang/Object;",
                &[jni::objects::JValue::Object(&service_name)],
            )
            .ok()?
            .l()
            .ok()?;
        if cm.is_null() {
            return None;
        }
        env.new_global_ref(&cm).ok()
    });
    if connectivity_global.is_none() {
        tracing::warn!("ConnectivityManager unavailable — foreground network-change watch off");
    }

    tracing::info!("Android lifecycle polling started");

    let mut was_foreground = true;
    // The default network's handle at the last poll. `None` = no network.
    let mut last_net: Option<i64> = None;
    let mut net_initialized = false;
    let mut net_changes: u64 = 0;

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
            tracing::debug!("Android lifecycle: foreground={focused}");
            let _ = tx.send(focused);
            was_foreground = focused;
        }

        // Default-network identity watch (#112).
        if let Some(ref cm) = connectivity_global {
            let net: Option<i64> = env
                .call_method(
                    cm.as_obj(),
                    "getActiveNetwork",
                    "()Landroid/net/Network;",
                    &[],
                )
                .ok()
                .and_then(|v| v.l().ok())
                .filter(|n| !n.is_null())
                .and_then(|n| {
                    env.call_method(&n, "getNetworkHandle", "()J", &[])
                        .and_then(|v| v.j())
                        .ok()
                });
            if !net_initialized {
                // First successful read is the baseline, never a transition.
                last_net = net;
                net_initialized = true;
            } else if let Some(new) = net {
                if last_net != Some(new) {
                    if focused {
                        net_changes += 1;
                        tracing::info!(
                            old = ?last_net,
                            new,
                            "Android default network changed while foregrounded — signalling transition"
                        );
                        let _ = net_tx.send(net_changes);
                    } else {
                        tracing::debug!(old = ?last_net, new, "default network changed while backgrounded (resume path will handle)");
                    }
                    last_net = Some(new);
                }
            } else if net.is_none() && last_net.is_some() {
                // Lost connectivity: remember, but don't fire — a reconnect
                // can't succeed with no network; the fire happens when a new
                // default network appears.
                tracing::debug!(old = ?last_net, "default network lost");
                last_net = None;
            }
        }
    }
}
