use tokio::sync::watch;

/// Starts listening for iOS application lifecycle notifications.
///
/// Observes `UIApplicationDidBecomeActiveNotification` and
/// `UIApplicationWillResignActiveNotification` via `NSNotificationCenter`.
/// Observer tokens are intentionally leaked (app-lifetime scope).
pub fn start_lifecycle_listener(tx: watch::Sender<bool>) {
    unsafe { register_observers(tx) };
}

unsafe fn register_observers(tx: watch::Sender<bool>) {
    use objc2::rc::Retained;
    use objc2::runtime::ProtocolObject;
    use objc2_foundation::{NSNotificationCenter, NSNotificationName, NSObjectProtocol};

    let center = NSNotificationCenter::defaultCenter();

    // UIApplicationDidBecomeActiveNotification
    let active_name: &NSNotificationName = objc2_ui_kit::UIApplicationDidBecomeActiveNotification;
    let tx_resume = tx.clone();
    let resume_observer: Retained<ProtocolObject<dyn NSObjectProtocol>> = center
        .addObserverForName_object_queue_usingBlock(
            Some(active_name),
            None,
            None,
            &move |_notification| {
                let _ = tx_resume.send(true);
            },
        );

    // UIApplicationWillResignActiveNotification
    let resign_name: &NSNotificationName = objc2_ui_kit::UIApplicationWillResignActiveNotification;
    let tx_pause = tx;
    let pause_observer: Retained<ProtocolObject<dyn NSObjectProtocol>> = center
        .addObserverForName_object_queue_usingBlock(
            Some(resign_name),
            None,
            None,
            &move |_notification| {
                let _ = tx_pause.send(false);
            },
        );

    // Leak observer tokens — they must live for the app's entire lifetime.
    // The app lifecycle is the process lifecycle, so this is intentional.
    std::mem::forget(resume_observer);
    std::mem::forget(pause_observer);

    log::info!("iOS lifecycle observers registered");
}
