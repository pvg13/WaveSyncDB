//! Platform-specific app lifecycle detection.
//!
//! Provides a [`start_lifecycle_listener`] function that monitors
//! foreground/background transitions and communicates them via
//! a [`tokio::sync::watch`] channel.

#[cfg(target_os = "android")]
mod android;
// The iOS listener references UIApplication, which is illegal in app
// extensions — it (and the whole objc2 dep family) compiles only under
// the opt-in `dioxus-ios-lifecycle` feature so an NSE build sharing the
// `dioxus` feature through unification never emits UIKit symbols (#113).
#[cfg(all(target_os = "ios", feature = "dioxus-ios-lifecycle"))]
mod ios;
#[cfg(not(any(
    target_os = "android",
    all(target_os = "ios", feature = "dioxus-ios-lifecycle")
)))]
mod noop;

#[cfg(target_os = "android")]
pub use android::start_lifecycle_listener;
#[cfg(all(target_os = "ios", feature = "dioxus-ios-lifecycle"))]
pub use ios::start_lifecycle_listener;
#[cfg(not(any(
    target_os = "android",
    all(target_os = "ios", feature = "dioxus-ios-lifecycle")
)))]
pub use noop::start_lifecycle_listener;
