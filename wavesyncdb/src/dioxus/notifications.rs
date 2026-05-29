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
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        // Android/iOS native display (JNI NotificationManager /
        // UNUserNotificationCenter) is a follow-up. Log so the flow is
        // observable; the notification is still in the returned signal, and
        // apps can render it or pass their own callback to
        // `use_sync_notifications_with`.
        log::info!(
            "sync notification (native display TODO on this platform): {} — {}",
            n.title,
            n.body
        );
    }
}
