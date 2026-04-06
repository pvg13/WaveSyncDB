//! Platform-specific app lifecycle detection.
//!
//! Provides a [`start_lifecycle_listener`] function that monitors
//! foreground/background transitions and communicates them via
//! a [`tokio::sync::watch`] channel.

#[cfg(target_os = "android")]
mod android;
#[cfg(target_os = "ios")]
mod ios;
#[cfg(not(any(target_os = "android", target_os = "ios")))]
mod noop;

#[cfg(target_os = "android")]
pub use android::start_lifecycle_listener;
#[cfg(target_os = "ios")]
pub use ios::start_lifecycle_listener;
#[cfg(not(any(target_os = "android", target_os = "ios")))]
pub use noop::start_lifecycle_listener;
