//! FFI exports for native mobile push notification services.
//!
//! Three interfaces are provided:
//!
//! - **C FFI** (`wavesync_background_sync`) — called from iOS Swift via `@_silgen_name`.
//!   Enable with `features = ["mobile-ffi"]`.
//!
//! - **JNI** (`Java_dev_dioxus_main_WaveSyncService_backgroundSync`) — called from
//!   Android Kotlin via `WaveSyncService` in `dev.dioxus.main`.
//!   Enable with `features = ["push-sync"]`.
//!
//! - **NSE C FFI** (`wavesync_nse_handle_push`) — called from the iOS
//!   Notification Service Extension template (`WaveSyncNotificationService`).
//!   `#[cfg(all(target_os = "ios", feature = "push-sync"))]` — see that
//!   function's docs for why it takes a config *directory* rather than a
//!   database URL, and never runs the KDF.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::Duration;

use crate::background_sync::{self, BackgroundSyncResult};

/// On Android, route engine output (tracing events, bridged via `tracing`'s
/// `log` feature) through the system logger so background-sync messages show
/// up in `adb logcat` (filterable by tag `wavesync`). Without this, the
/// FirebaseMessagingService JNI thread has no log destination — a plain
/// stderr-based setup is discarded by Android from native code — so "no
/// peers found" failures are uninvestigable.
///
/// `android_logger`'s `Config` is a `log`-crate API (it predates `tracing`
/// and has no tracing-native equivalent), so this function still reaches for
/// `log::LevelFilter` directly — the crate keeps a narrow `log` dependency
/// scoped to native targets for exactly this and SeaORM's equivalent
/// `sqlx_logging_level` API (see `Cargo.toml`).
///
/// Idempotent: `android_logger::init_once` is documented to be a no-op on
/// subsequent calls. Safe to call from every JNI entry point.
#[cfg(all(target_os = "android", feature = "push-sync"))]
fn ensure_android_logger() {
    use android_logger::{Config, FilterBuilder};
    let mut filter = FilterBuilder::new();
    filter.parse(&format!("info,{}", crate::recommended_log_filters()));
    android_logger::init_once(
        Config::default()
            .with_tag("wavesync")
            .with_max_level(log::LevelFilter::Info)
            .with_filter(filter.build()),
    );
}

/// Shared sync logic used by both C FFI and JNI entry points.
///
/// `target_topic`, when `Some`, is the effective (PSK-derived) topic from the
/// push payload — only that group is brought up for this wake. `None` rejoins
/// every configured group (the conservative wake).
///
/// Panic-safe: the whole body runs under `catch_unwind` so a panic anywhere
/// in the setup or sync path returns `-6` instead of unwinding across the
/// `extern "C"` boundary and aborting the host app — on iOS, repeated
/// background crashes teach the OS to throttle the app's background pushes.
fn run_background_sync(
    database_url: &str,
    timeout_secs: u32,
    peer_addrs: &[String],
    target_topic: Option<&str>,
) -> i32 {
    // A push delivered while the app's own engine is live (foreground
    // delivery, or backgrounded-but-not-suspended) must not build a second
    // engine: it would load the same persisted libp2p keypair and register
    // the same PeerId at the rendezvous/relay with its own short-lived
    // addresses, clobbering the live engine's state. The live engine
    // receives the change through its open connections anyway.
    if crate::engine::any_engine_live() {
        tracing::info!(
            "background sync skipped: an engine is already live in this \
             process and will handle the sync itself"
        );
        return 3;
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let rt = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(_) => return -6,
        };

        let timeout = Duration::from_secs(timeout_secs.into());

        rt.block_on(async {
            match background_sync::background_sync_with_peers_for_topic(
                database_url,
                timeout,
                peer_addrs,
                target_topic,
            )
            .await
            {
                Ok(BackgroundSyncResult::Synced { .. }) => 0,
                Ok(BackgroundSyncResult::NoPeers) => 1,
                Ok(BackgroundSyncResult::TimedOut { .. }) => 2,
                Err(background_sync::BackgroundSyncError::ConfigNotFound(_)) => -1,
                Err(background_sync::BackgroundSyncError::ConfigInvalid(_)) => -2,
                Err(background_sync::BackgroundSyncError::DatabaseError(_)) => -3,
                Err(background_sync::BackgroundSyncError::RegistryError(_)) => -4,
            }
        })
    }));
    match result {
        Ok(rc) => rc,
        Err(_) => {
            tracing::error!("background sync panicked; returning -6");
            -6
        }
    }
}

/// C FFI entry point for background sync. Called from iOS Swift via `@_silgen_name`.
///
/// # Returns
///
/// * `0` — Successfully synced with at least one peer
/// * `1` — No peers found within timeout
/// * `2` — Timed out (some peers may have synced)
/// * `3` — Skipped: the app's own engine is live in this process and handles
///   the sync itself (treat as "nothing for this call to do", not an error)
/// * `-1` — Config not found (app was never started)
/// * `-2` — Invalid config
/// * `-3` — Database error
/// * `-4` — Registry error
/// * `-5` — Invalid arguments (null pointer or bad UTF-8)
/// * `-6` — Runtime creation failed, or an internal panic (caught; never
///   unwinds into the caller)
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

    run_background_sync(url, timeout_secs, &[], None)
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
            Ok(json) => serde_json::from_str(json).unwrap_or_else(|e| {
                // Malformed payload peer hints: log rather than silently degrade
                // to discovery-only, which slows the cold-start wake on cellular.
                tracing::warn!("background sync: ignoring malformed peer_addrs JSON: {e}");
                Vec::new()
            }),
            Err(e) => {
                tracing::warn!("background sync: peer_addrs_json is not valid UTF-8: {e}");
                Vec::new()
            }
        }
    };

    run_background_sync(url, timeout_secs, &peer_addrs, None)
}

/// C FFI entry point for **targeted** background sync. Called from iOS Swift via
/// `@_silgen_name`. Like `wavesync_background_sync_with_peers`, but `topic` is the
/// effective (PSK-derived) topic from the APNs payload — only that group is synced
/// for this wake. A null/empty `topic` falls back to syncing all groups.
///
/// Same return codes as `wavesync_background_sync`.
///
/// # Safety
///
/// `database_url` must be a valid, null-terminated UTF-8 string pointer.
/// `peer_addrs_json` and `topic`, if non-null, must be valid null-terminated
/// UTF-8 string pointers.
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_background_sync_targeted(
    database_url: *const c_char,
    timeout_secs: u32,
    peer_addrs_json: *const c_char,
    topic: *const c_char,
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
            Ok(json) => serde_json::from_str(json).unwrap_or_else(|e| {
                // Malformed payload peer hints: log rather than silently degrade
                // to discovery-only, which slows the cold-start wake on cellular.
                tracing::warn!("background sync: ignoring malformed peer_addrs JSON: {e}");
                Vec::new()
            }),
            Err(e) => {
                tracing::warn!("background sync: peer_addrs_json is not valid UTF-8: {e}");
                Vec::new()
            }
        }
    };

    let target: Option<String> = if topic.is_null() {
        None
    } else {
        unsafe { CStr::from_ptr(topic) }
            .to_str()
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };

    run_background_sync(url, timeout_secs, &peer_addrs, target.as_deref())
}

/// JNI entry point for background sync. Called from Dioxus-generated
/// `WaveSyncService.backgroundSync()` in `dev.dioxus.main`.
///
/// `peer_addrs_json` is a JSON array of multiaddr strings from the FCM payload,
/// e.g. `["/ip4/192.168.1.150/tcp/36189/p2p/12D3Koo..."]`. These are dialed
/// directly as bootstrap peers, bypassing slow mDNS/relay discovery.
///
/// Same return codes as `wavesync_background_sync`.
#[cfg(all(target_os = "android", feature = "push-sync"))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_dioxus_main_WaveSyncService_backgroundSync(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    database_url: jni::objects::JString,
    timeout_secs: jni::sys::jint,
    peer_addrs_json: jni::objects::JString,
) -> jni::sys::jint {
    ensure_android_logger();
    // Capture the JavaVM so the background-sync notification pump can call back
    // into Kotlin (this process has no ndk_context).
    crate::notify_display::store_java_vm(&mut env);
    tracing::info!(
        "JNI backgroundSync invoked (timeout={}s, peer_addrs payload present={})",
        timeout_secs,
        !peer_addrs_json.is_null()
    );

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

    tracing::info!(
        "background_sync starting: db={url}, {} bootstrap peer(s) from FCM payload",
        peer_addrs.len()
    );
    // `jint` is signed — clamp instead of `as u32`, which would turn a
    // negative caller value into a ~136-year timeout.
    run_background_sync(&url, timeout_secs.max(0) as u32, &peer_addrs, None)
}

/// JNI entry point for **targeted** background sync. Called from Dioxus-generated
/// `WaveSyncService.backgroundSyncTargeted()` in `dev.dioxus.main`. Like
/// `backgroundSync`, but `topic` is the effective (PSK-derived) topic from the
/// FCM payload — only that group is synced for this wake. A null/empty `topic`
/// falls back to syncing all groups.
///
/// Same return codes as `wavesync_background_sync`.
#[cfg(all(target_os = "android", feature = "push-sync"))]
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_dioxus_main_WaveSyncService_backgroundSyncTargeted(
    mut env: jni::JNIEnv,
    _class: jni::objects::JClass,
    database_url: jni::objects::JString,
    timeout_secs: jni::sys::jint,
    peer_addrs_json: jni::objects::JString,
    topic: jni::objects::JString,
) -> jni::sys::jint {
    ensure_android_logger();
    // Capture the JavaVM for the background-sync notification pump (no
    // ndk_context in the FCM service process).
    crate::notify_display::store_java_vm(&mut env);

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

    let target: Option<String> = if topic.is_null() {
        None
    } else {
        match env.get_string(&topic) {
            Ok(s) => {
                let t: String = s.into();
                if t.is_empty() { None } else { Some(t) }
            }
            Err(_) => None,
        }
    };

    tracing::info!(
        "JNI backgroundSyncTargeted invoked (timeout={}s, peer_addrs present={}, topic present={})",
        timeout_secs,
        !peer_addrs_json.is_null(),
        target.is_some()
    );
    // `jint` is signed — clamp instead of `as u32` (see `backgroundSync`).
    run_background_sync(
        &url,
        timeout_secs.max(0) as u32,
        &peer_addrs,
        target.as_deref(),
    )
}

// ---------------------------------------------------------------------
// Notification Service Extension (NSE) support
// ---------------------------------------------------------------------

/// Parse the NSE's push payload JSON for the effective topic and any direct
/// peer-address hints.
///
/// Mirrors the exact keys `WaveSyncPushHandler` reads from the APNs
/// `userInfo` dictionary on the app side (`WaveSyncPushHandler.swift`): a
/// top-level `"topic"` string (the effective/PSK-derived topic the relay
/// stamped on the alert push) and an optional `"peer_addrs"` string, itself
/// JSON-encoding an array of multiaddr strings.
///
/// Returns `None` only when `topic` is missing or not a string — a push with
/// no topic can't be targeted at any group. A missing or malformed
/// `peer_addrs` degrades to an empty `Vec` rather than failing the whole
/// parse: peer hints are a cold-start latency optimization (direct dial
/// instead of waiting on discovery), not a requirement for sync to succeed.
///
/// Compiled whenever the real (iOS + `push-sync`) call site is, or under
/// `cfg(test)` so this pure logic is host-testable on every platform without
/// pulling in the target-gated caller.
#[cfg(any(all(target_os = "ios", feature = "push-sync"), test))]
fn parse_push_payload(json: &str) -> Option<(String, Vec<String>)> {
    let value: serde_json::Value = serde_json::from_str(json).ok()?;
    let topic = value.get("topic")?.as_str()?.to_string();
    let peer_addrs = value
        .get("peer_addrs")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
        .unwrap_or_default();
    Some((topic, peer_addrs))
}

/// Re-anchor a config-recorded `database_url` against the directory the
/// config was actually found in.
///
/// The config file itself is located relocation-safely (App Group container
/// lookup + `.wavesync_config_dir` pointer), but the `database_url` recorded
/// INSIDE it is an absolute path written by the app on some earlier launch —
/// and iOS is free to relocate containers between launches (restore,
/// migration, `.appex` vs app path differences). Trusting the stale absolute
/// path made the NSE fail to open the DB on every wake after a relocation,
/// silently, until the app next foregrounded and rewrote the config.
///
/// Resolution: if the recorded path still exists, keep it; otherwise, if a
/// file with the same name exists in `config_dir` (the DB always lives
/// beside its config), rebuild the URL against that. If neither exists the
/// original URL is returned and the open fails downstream with its normal
/// error.
#[cfg(any(all(target_os = "ios", feature = "push-sync"), test))]
fn resolve_database_url_for_dir(config_dir: &std::path::Path, database_url: &str) -> String {
    let (scheme, rest) = match database_url.split_once(':') {
        Some((s, r)) if s.starts_with("sqlite") => (format!("{s}:"), r),
        _ => (String::new(), database_url),
    };
    let (path_str, query) = match rest.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (rest, None),
    };
    let path_str = path_str.strip_prefix("//").unwrap_or(path_str);

    let recorded = std::path::Path::new(path_str);
    if recorded.exists() {
        return database_url.to_string();
    }
    let Some(file_name) = recorded.file_name() else {
        return database_url.to_string();
    };
    let candidate = config_dir.join(file_name);
    if !candidate.exists() {
        return database_url.to_string();
    }
    tracing::info!(
        "NSE: recorded database path {} is stale; using co-located {}",
        recorded.display(),
        candidate.display()
    );
    match query {
        Some(q) => format!("{scheme}{}?{q}", candidate.display()),
        None => format!("{scheme}{}", candidate.display()),
    }
}

/// JSON shape returned by [`wavesync_nse_handle_push`]. See that function's
/// docs for what `title`/`body` mean when absent.
#[cfg(any(all(target_os = "ios", feature = "push-sync"), test))]
#[derive(serde::Serialize)]
struct NsePushResult {
    synced: bool,
    title: Option<String>,
    body: Option<String>,
}

#[cfg(any(all(target_os = "ios", feature = "push-sync"), test))]
fn nse_result_json(synced: bool, title: Option<String>, body: Option<String>) -> String {
    serde_json::to_string(&NsePushResult {
        synced,
        title,
        body,
    })
    // `NsePushResult` has no field that can fail to serialize (plain
    // bool/String/Option<String>) — this is unreachable in practice, but a
    // literal fallback beats unwrapping into a panic across an FFI boundary.
    .unwrap_or_else(|_| "{\"synced\":false,\"title\":null,\"body\":null}".to_string())
}

/// C FFI entry point for the iOS Notification Service Extension (NSE).
///
/// Unlike every other entry point in this module, the NSE is NOT handed a
/// database URL — it's handed `config_dir`, the shared App Group container
/// directory (see [`wavesync_app_group_container`] and the
/// `WaveSyncNotificationService` Swift template). `.wavesync_config.json`'s
/// `database_url` field is read directly from that directory
/// (`SyncConfig::load_from_dir`), so the NSE never needs to be told the
/// database URL separately — one less thing that can drift out of sync
/// between the app and its extension.
///
/// `payload_json` is the APNs `userInfo` dictionary, JSON-encoded by the
/// Swift template (see [`parse_push_payload`] for the keys read).
/// `budget_secs` bounds the sync the same way `timeout_secs` does for the
/// other background entry points — the NSE's OS-enforced wall-clock budget
/// is much tighter (~30s total, shared with the extension's own startup),
/// so callers should pass something well under that, leaving room for
/// process teardown.
///
/// The KDF (Argon2id, ~19 MiB) can **never** run here, by construction: this
/// function calls `background_sync_with_capture` with `key_cache_load_only =
/// true`, which threads through to every group-key derivation the sync makes
/// (`connection::group_key_for_dir`) and turns a cache miss into an error
/// instead of a fallback derivation — the branch that would call
/// `GroupKey::from_passphrase` is unreachable in load-only mode (see
/// `GroupKeyLoadOnlyMiss`). A group whose key isn't already cached there
/// simply can't be synced. In that case (or on timeout) the caller sees
/// `synced: false` with no title/body and falls back to the operator's
/// placeholder alert content — a safe fallback by construction, not a
/// special case this function has to detect.
///
/// Returns a JSON string `{"synced": bool, "title": string|null, "body":
/// string|null}` — `title`/`body` are the last captured `SyncNotify`
/// notification's, or `null` if none fired (timeout, no matching policy, or
/// the change wasn't notify-worthy). The returned pointer is heap-allocated
/// by Rust (`CString::into_raw`) and **must** be freed with
/// [`wavesync_string_free`] — never Swift's `free()` (different allocator;
/// contrast with [`wavesync_app_group_container`]'s Swift-`strdup`'d return
/// value, which is freed the other way).
///
/// # Safety
///
/// `config_dir` and `payload_json` must be valid, null-terminated UTF-8
/// string pointers.
#[cfg(all(target_os = "ios", feature = "push-sync"))]
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_nse_handle_push(
    config_dir: *const c_char,
    payload_json: *const c_char,
    budget_secs: u32,
) -> *mut c_char {
    fn to_cstring(json: String) -> *mut c_char {
        match std::ffi::CString::new(json) {
            Ok(c) => c.into_raw(),
            // A NUL byte inside our own serde_json output can't happen for
            // this shape, but never hand back a dangling pointer if it did.
            Err(_) => std::ptr::null_mut(),
        }
    }
    let fallback = || to_cstring(nse_result_json(false, None, None));
    let cstr_to_str = |ptr: *const c_char| -> Option<&str> {
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    };

    let Some(config_dir) = cstr_to_str(config_dir) else {
        return fallback();
    };
    let Some(payload_json) = cstr_to_str(payload_json) else {
        return fallback();
    };
    let Some((topic, peer_addrs)) = parse_push_payload(payload_json) else {
        return fallback();
    };

    let config =
        match crate::connection::SyncConfig::load_from_dir(std::path::Path::new(config_dir)) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "wavesync_nse_handle_push: could not load config from {config_dir}: {e}"
                );
                return fallback();
            }
        };
    // The recorded absolute path can be stale after a container relocation —
    // re-anchor it against where the config was actually found.
    let database_url =
        resolve_database_url_for_dir(std::path::Path::new(config_dir), &config.database_url);

    // A single worker thread — unlike `run_background_sync`'s runtime above,
    // which serves the app process (Android's background service, or iOS's
    // own foreground-triggered wake) with no comparable constraint. Every
    // extra worker thread costs its own OS stack (default 2 MiB, reducible
    // but still not free) against the NSE's ~24 MB total ceiling (see this
    // function's doc comment and the key-cache tradeoff it links) — a
    // multi-thread pool sized to available cores has nothing to parallelize
    // here anyway (one push, one group, one sync) and would only spend that
    // budget on idle threads.
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return fallback(),
    };

    let budget = Duration::from_secs(budget_secs.into());
    // catch_unwind: a panic must never unwind across this `extern "C"`
    // boundary — it would abort the NSE outright, and repeated extension
    // crashes make iOS stop invoking it. The uniform `synced:false`
    // fallback (placeholder banner) is exactly the right degradation.
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async {
            match background_sync::background_sync_with_capture(
                &database_url,
                budget,
                &peer_addrs,
                Some(&topic),
                // Never let the NSE's sync fall back to running the KDF — see
                // this function's doc comment.
                true,
            )
            .await
            {
                Ok((result, notifications)) => {
                    let synced = matches!(result, BackgroundSyncResult::Synced { .. });
                    match notifications.into_iter().last() {
                        Some(n) => (synced, Some(n.title), Some(n.body)),
                        None => (synced, None, None),
                    }
                }
                Err(e) => {
                    tracing::warn!("wavesync_nse_handle_push: background sync failed: {e}");
                    (false, None, None)
                }
            }
        })
    }));
    let (synced, title, body) = match outcome {
        Ok(t) => t,
        Err(_) => {
            tracing::error!("wavesync_nse_handle_push: sync panicked; returning fallback");
            (false, None, None)
        }
    };

    to_cstring(nse_result_json(synced, title, body))
}

/// Frees a string returned by [`wavesync_nse_handle_push`].
///
/// Every other FFI entry point in this module returns a plain integer, so
/// this is currently the only pointer this crate hands to Swift that Swift
/// must give back — a `CString::into_raw` allocation, which only Rust may
/// reclaim (`CString::from_raw`, then drop). Do not pass a Swift-`strdup`'d
/// pointer here (e.g. [`wavesync_app_group_container`]'s return value on
/// the Swift side) — that is a different allocator, and freeing it this way
/// corrupts the heap; free those with the C library `free()` instead.
///
/// # Safety
///
/// `s` must be exactly a pointer previously returned by
/// `wavesync_nse_handle_push`, and must not be freed more than once.
#[cfg(all(target_os = "ios", feature = "push-sync"))]
#[unsafe(no_mangle)]
pub extern "C" fn wavesync_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    drop(unsafe { std::ffi::CString::from_raw(s) });
}

/// Resolve an iOS App Group container directory by group id, via the Swift
/// `wavesync_app_group_container` `@_cdecl` helper
/// (`WaveSyncPushBridge.swift`).
///
/// Lets an app point its [`WaveSyncDbBuilder`](crate::WaveSyncDbBuilder) and
/// its Notification Service Extension at the same shared directory without
/// duplicating `FileManager.containerURL(forSecurityApplicationGroupIdentifier:)`
/// logic in Rust, which has no access to Foundation. Resolved via `dlsym`,
/// mirroring `notify_display`'s pattern for calling into the already-loaded
/// WaveSyncPush framework.
///
/// Returns `None` if the symbol can't be resolved (host app hasn't linked
/// WaveSyncPush, or linked a version predating this helper), the group id
/// isn't valid UTF-8 with no interior NUL, or the app has no entitlement for
/// `group_id` / the container doesn't exist.
#[cfg(target_os = "ios")]
pub fn wavesync_app_group_container(group_id: &str) -> Option<String> {
    use std::os::raw::c_void;

    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
        fn free(ptr: *mut c_void);
    }
    const RTLD_DEFAULT: *mut c_void = (-2isize) as *mut c_void;
    type ContainerFn = unsafe extern "C" fn(*const c_char) -> *mut c_char;

    let group_id_c = std::ffi::CString::new(group_id).ok()?;
    unsafe {
        let sym = dlsym(RTLD_DEFAULT, c"wavesync_app_group_container".as_ptr());
        if sym.is_null() {
            tracing::warn!(
                "wavesync_app_group_container: symbol not found (WaveSyncPush not linked?)"
            );
            return None;
        }
        let f: ContainerFn = std::mem::transmute(sym);
        let ptr = f(group_id_c.as_ptr());
        if ptr.is_null() {
            return None;
        }
        // Swift `strdup`'d this — free with the C allocator, never
        // `CString::from_raw` (that assumes a Rust allocation).
        let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        free(ptr as *mut c_void);
        Some(s)
    }
}

#[cfg(test)]
mod nse_tests {
    use super::*;

    #[test]
    fn parses_topic_and_peer_addrs() {
        let json = r#"{"topic":"wavesync2-abc","peer_addrs":"[\"/ip4/1.2.3.4/tcp/1\"]"}"#;
        let (topic, addrs) = parse_push_payload(json).expect("should parse");
        assert_eq!(topic, "wavesync2-abc");
        assert_eq!(addrs, vec!["/ip4/1.2.3.4/tcp/1".to_string()]);
    }

    #[test]
    fn missing_peer_addrs_yields_empty_vec_not_none() {
        let json = r#"{"topic":"wavesync2-abc"}"#;
        let (topic, addrs) = parse_push_payload(json).expect("topic alone should parse");
        assert_eq!(topic, "wavesync2-abc");
        assert!(addrs.is_empty());
    }

    #[test]
    fn malformed_peer_addrs_degrades_to_empty_vec() {
        let json = r#"{"topic":"wavesync2-abc","peer_addrs":"not json"}"#;
        let (topic, addrs) = parse_push_payload(json).expect("topic alone should parse");
        assert_eq!(topic, "wavesync2-abc");
        assert!(addrs.is_empty());
    }

    #[test]
    fn missing_topic_returns_none() {
        let json = r#"{"peer_addrs":"[]"}"#;
        assert!(parse_push_payload(json).is_none());
    }

    #[test]
    fn topic_not_a_string_returns_none() {
        let json = r#"{"topic":42}"#;
        assert!(parse_push_payload(json).is_none());
    }

    #[test]
    fn invalid_json_returns_none_not_panic() {
        assert!(parse_push_payload("not json {{{").is_none());
    }

    #[test]
    fn empty_object_returns_none() {
        assert!(parse_push_payload("{}").is_none());
    }

    #[test]
    fn result_json_shape_synced_with_notification() {
        let json = nse_result_json(
            true,
            Some("Ana added milk".to_string()),
            Some("groceries".to_string()),
        );
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["synced"], true);
        assert_eq!(value["title"], "Ana added milk");
        assert_eq!(value["body"], "groceries");
    }

    #[test]
    fn result_json_shape_no_notification() {
        let json = nse_result_json(false, None, None);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["synced"], false);
        assert!(value["title"].is_null());
        assert!(value["body"].is_null());
    }

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("wavesync_nse_url_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn existing_recorded_path_is_kept_verbatim() {
        let dir = temp_dir();
        let db = dir.join("app.db");
        std::fs::write(&db, b"x").unwrap();
        let url = format!("sqlite:{}?mode=rwc", db.display());
        // Even with a different config_dir, an existing path wins.
        assert_eq!(
            resolve_database_url_for_dir(std::path::Path::new("/nonexistent"), &url),
            url
        );
    }

    #[test]
    fn stale_recorded_path_reanchors_to_config_dir() {
        let dir = temp_dir();
        std::fs::write(dir.join("app.db"), b"x").unwrap();
        let stale = "sqlite:/private/var/mobile/OLD-UUID/app.db?mode=rwc";
        let resolved = resolve_database_url_for_dir(&dir, stale);
        assert_eq!(
            resolved,
            format!("sqlite:{}?mode=rwc", dir.join("app.db").display())
        );
    }

    #[test]
    fn stale_path_with_no_colocated_file_returns_original() {
        let dir = temp_dir(); // empty — no app.db here either
        let stale = "sqlite:/private/var/mobile/OLD-UUID/app.db?mode=rwc";
        assert_eq!(resolve_database_url_for_dir(&dir, stale), stale);
    }

    #[test]
    fn sqlite_double_slash_prefix_is_handled() {
        let dir = temp_dir();
        std::fs::write(dir.join("app.db"), b"x").unwrap();
        let stale = "sqlite:///private/var/mobile/OLD-UUID/app.db";
        let resolved = resolve_database_url_for_dir(&dir, stale);
        assert_eq!(
            resolved,
            format!("sqlite:{}", dir.join("app.db").display())
        );
    }
}
