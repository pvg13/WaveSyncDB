use tokio::sync::watch;

/// No-op lifecycle listener for platforms without automatic detection.
///
/// The watch channel stays at `true` (foreground) forever.
/// Use [`super::super::hooks::use_app_resume`] with a manual signal for
/// desktop lifecycle control.
pub fn start_lifecycle_listener(_tx: watch::Sender<bool>) {
    log::debug!("Auto lifecycle detection not available on this platform");
}
