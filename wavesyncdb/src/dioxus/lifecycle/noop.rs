use tokio::sync::watch;

/// No-op lifecycle listener for platforms without automatic detection —
/// and for iOS builds without the `dioxus-ios-lifecycle` feature (#113:
/// the UIKit observers are illegal in app extensions, so they compile
/// only under that opt-in).
///
/// The watch channel stays at `true` (foreground) forever.
/// Use [`super::super::hooks::use_app_resume`] with a manual signal for
/// desktop lifecycle control.
pub fn start_lifecycle_listener(_tx: watch::Sender<bool>, _net_tx: watch::Sender<u64>) {
    #[cfg(target_os = "ios")]
    tracing::warn!(
        "iOS UIApplication lifecycle observers NOT compiled (feature \
         `dioxus-ios-lifecycle` off — correct for app extensions). Auto \
         pause/resume will not fire; an APP target should enable the feature."
    );
    #[cfg(not(target_os = "ios"))]
    tracing::debug!("Auto lifecycle detection not available on this platform");
}
