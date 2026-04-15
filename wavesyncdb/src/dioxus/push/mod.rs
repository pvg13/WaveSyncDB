//! Automatic push notification registration for mobile platforms.
//!
//! On iOS, injects the push notification delegate methods into tao's
//! existing AppDelegate at runtime and calls `registerForRemoteNotifications()`.
//! The device token is cached in a Rust static and delivered to the engine
//! via a callback.

#[cfg(target_os = "ios")]
mod ios;
#[cfg(not(target_os = "ios"))]
mod noop;

#[cfg(target_os = "ios")]
pub use ios::{cached_apns_token, setup_push_token};
#[cfg(not(target_os = "ios"))]
pub use noop::setup_push_token;
