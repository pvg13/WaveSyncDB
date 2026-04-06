//! C FFI exports for native mobile push notification services.
//!
//! These functions are called from Android Kotlin (via JNI) or iOS Swift (via FFI)
//! when a silent push notification wakes the app to sync.
//!
//! Enable with `features = ["mobile-ffi"]` in Cargo.toml.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::Duration;

use crate::background_sync::{self, BackgroundSyncResult};

/// Perform a one-shot background sync. Called from native Android/iOS push services.
///
/// # Arguments
///
/// * `database_url` — Null-terminated UTF-8 C string, e.g.
///   `"sqlite:///data/data/com.example.app/databases/app.db?mode=rwc"`
/// * `timeout_secs` — Maximum seconds to wait for sync completion
///
/// # Returns
///
/// * `0` — Successfully synced with at least one peer
/// * `1` — No peers found within timeout
/// * `2` — Timed out (some peers may have synced)
/// * `-1` — Config not found (app was never started)
/// * `-2` — Invalid config
/// * `-3` — Database error
/// * `-4` — Registry error
/// * `-5` — Invalid arguments (null pointer or bad UTF-8)
/// * `-6` — Runtime creation failed
///
/// # Safety
///
/// `database_url` must be a valid, null-terminated UTF-8 string pointer.
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_background_sync(database_url: *const c_char, timeout_secs: u32) -> i32 {
    if database_url.is_null() {
        return -5;
    }

    let url = match unsafe { CStr::from_ptr(database_url) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return -5,
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return -6,
    };

    let timeout = Duration::from_secs(timeout_secs.into());

    rt.block_on(async {
        match background_sync::background_sync(&url, timeout).await {
            Ok(BackgroundSyncResult::Synced { .. }) => 0,
            Ok(BackgroundSyncResult::NoPeers) => 1,
            Ok(BackgroundSyncResult::TimedOut { .. }) => 2,
            Err(background_sync::BackgroundSyncError::ConfigNotFound(_)) => -1,
            Err(background_sync::BackgroundSyncError::ConfigInvalid(_)) => -2,
            Err(background_sync::BackgroundSyncError::DatabaseError(_)) => -3,
            Err(background_sync::BackgroundSyncError::RegistryError(_)) => -4,
        }
    })
}
