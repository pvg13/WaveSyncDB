//! Automatic push notification registration for mobile platforms.
//!
//! On iOS, injects the push notification delegate methods into tao's
//! existing AppDelegate at runtime and calls `registerForRemoteNotifications()`.
//! The device token is written to a file next to the database, matching the
//! pattern used by Android's `WaveSyncInitProvider`.

#[cfg(target_os = "ios")]
mod ios;
#[cfg(not(target_os = "ios"))]
mod noop;

#[cfg(target_os = "ios")]
pub use ios::setup_push_token_writer;
#[cfg(not(target_os = "ios"))]
pub use noop::setup_push_token_writer;
