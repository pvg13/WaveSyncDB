//! FFI exports for native mobile push notification services.
//!
//! Two interfaces are provided:
//!
//! - **C FFI** (`wavesync_background_sync`) — called from iOS Swift via `@_silgen_name`.
//!   Enable with `features = ["mobile-ffi"]`.
//!
//! - **JNI** (`Java_dev_dioxus_main_WaveSyncService_backgroundSync`) — called from
//!   Android Kotlin via `WaveSyncService` in `dev.dioxus.main`.
//!   Enable with `features = ["android-fcm"]`.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::Duration;

use crate::background_sync::{self, BackgroundSyncResult};

/// Shared sync logic used by both C FFI and JNI entry points.
fn run_background_sync(database_url: &str, timeout_secs: u32, peer_addrs: &[String]) -> i32 {
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return -6,
    };

    let timeout = Duration::from_secs(timeout_secs.into());

    rt.block_on(async {
        match background_sync::background_sync_with_peers(database_url, timeout, peer_addrs).await {
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

/// C FFI entry point for background sync. Called from iOS Swift via `@_silgen_name`.
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
        Ok(s) => s,
        Err(_) => return -5,
    };

    run_background_sync(url, timeout_secs, &[])
}

/// C FFI entry point for background sync with peer addresses.
/// Called from iOS Swift via `@_silgen_name`.
///
/// `peer_addrs_json` is a JSON array of multiaddr strings from the APNs payload,
/// e.g. `["/ip4/192.168.1.150/tcp/36189/p2p/12D3Koo..."]`. These are dialed
/// directly as bootstrap peers, bypassing slow mDNS/relay discovery.
/// Pass `null` to skip (equivalent to `wavesync_background_sync`).
///
/// Same return codes as `wavesync_background_sync`.
///
/// # Safety
///
/// `database_url` must be a valid, null-terminated UTF-8 string pointer.
/// `peer_addrs_json`, if non-null, must be a valid, null-terminated UTF-8 string pointer.
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_background_sync_with_peers(
    database_url: *const c_char,
    timeout_secs: u32,
    peer_addrs_json: *const c_char,
) -> i32 {
    if database_url.is_null() {
        return -5;
    }

    let url = match unsafe { CStr::from_ptr(database_url) }.to_str() {
        Ok(s) => s,
        Err(_) => return -5,
    };

    let peer_addrs: Vec<String> = if peer_addrs_json.is_null() {
        Vec::new()
    } else {
        match unsafe { CStr::from_ptr(peer_addrs_json) }.to_str() {
            Ok(json) => serde_json::from_str(json).unwrap_or_default(),
            Err(_) => Vec::new(),
        }
    };

    run_background_sync(url, timeout_secs, &peer_addrs)
}

/// C FFI entry point for persisting a push notification token.
///
/// Loads the `SyncConfig` from the database directory, updates the push token
/// fields, and saves it back. This allows cold sync (`background_sync`) to
/// read the token and register it with the relay server.
///
/// Called from iOS Swift (via `WaveSyncTokenWriter`) or directly by consuming apps.
///
/// # Returns
///
/// * `0` — Success
/// * `-1` — Config not found (app was never started)
/// * `-2` — Failed to save config
/// * `-5` — Invalid arguments (null pointer or bad UTF-8)
///
/// # Safety
///
/// All pointer arguments must be valid, null-terminated UTF-8 string pointers.
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_set_push_token(
    database_url: *const c_char,
    platform: *const c_char,
    token: *const c_char,
) -> i32 {
    if database_url.is_null() || platform.is_null() || token.is_null() {
        return -5;
    }

    let url = match unsafe { CStr::from_ptr(database_url) }.to_str() {
        Ok(s) => s,
        Err(_) => return -5,
    };
    let platform_str = match unsafe { CStr::from_ptr(platform) }.to_str() {
        Ok(s) => s,
        Err(_) => return -5,
    };
    let token_str = match unsafe { CStr::from_ptr(token) }.to_str() {
        Ok(s) => s,
        Err(_) => return -5,
    };

    let mut config = match crate::connection::SyncConfig::load(url) {
        Ok(c) => c,
        Err(_) => return -1,
    };

    config.push_platform = Some(platform_str.to_string());
    config.push_token = Some(token_str.to_string());

    match config.save() {
        Ok(()) => 0,
        Err(_) => -2,
    }
}

/// JNI entry point for background sync. Called from Dioxus-generated
/// `WaveSyncService.backgroundSync()` in `dev.dioxus.main`.
///
/// `peer_addrs_json` is a JSON array of multiaddr strings from the FCM payload,
/// e.g. `["/ip4/192.168.1.150/tcp/36189/p2p/12D3Koo..."]`. These are dialed
/// directly as bootstrap peers, bypassing slow mDNS/relay discovery.
///
/// Same return codes as `wavesync_background_sync`.
#[cfg(all(target_os = "android", feature = "android-fcm"))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_dioxus_main_WaveSyncService_backgroundSync(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    database_url: jni::objects::JString,
    timeout_secs: jni::sys::jint,
    peer_addrs_json: jni::objects::JString,
) -> jni::sys::jint {
    let url: String = match env.get_string(&database_url) {
        Ok(s) => s.into(),
        Err(_) => return -5,
    };

    let peer_addrs: Vec<String> = if peer_addrs_json.is_null() {
        Vec::new()
    } else {
        match env.get_string(&peer_addrs_json) {
            Ok(s) => {
                let json: String = s.into();
                serde_json::from_str(&json).unwrap_or_default()
            }
            Err(_) => Vec::new(),
        }
    };

    run_background_sync(&url, timeout_secs as u32, &peer_addrs)
}

/// JNI entry point for persisting a push notification token.
///
/// Loads the `SyncConfig`, updates the push token fields, and saves it back.
/// Called from Kotlin `WaveSyncService.setPushToken()`.
///
/// Same return codes as `wavesync_set_push_token`.
#[cfg(all(target_os = "android", feature = "android-fcm"))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_dioxus_main_WaveSyncService_setPushToken(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    database_url: jni::objects::JString,
    token: jni::objects::JString,
) -> jni::sys::jint {
    let url: String = match env.get_string(&database_url) {
        Ok(s) => s.into(),
        Err(_) => return -5,
    };
    let token_str: String = match env.get_string(&token) {
        Ok(s) => s.into(),
        Err(_) => return -5,
    };

    let mut config = match crate::connection::SyncConfig::load(&url) {
        Ok(c) => c,
        Err(_) => return -1,
    };

    config.push_platform = Some("Fcm".to_string());
    config.push_token = Some(token_str);

    match config.save() {
        Ok(()) => 0,
        Err(_) => -2,
    }
}
