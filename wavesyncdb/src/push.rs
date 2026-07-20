//! Push notification integration for mobile platforms.
//!
//! Uses a file-based approach for token exchange between native services and Rust:
//!
//! - **Android (FCM):** The Kotlin `WaveSyncService` writes the FCM token to a file;
//!   Rust reads it during [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build).
//! - **iOS (APNs):** The Swift `WaveSyncTokenWriter` writes the APNs device token to a file;
//!   Rust reads it during [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build).
//!
//! This avoids JNI classloader issues on Android and keeps the iOS integration
//! consistent with the same pattern.

// ── Android: bundle Kotlin sources ──────────────────────────────────────────

// Bundle the Android Kotlin sources (WaveSyncService, WaveSyncInitProvider) as a
// Gradle submodule. dx build picks this up from the compiled binary's symbol table,
// so apps that depend on wavesyncdb with `push-sync` feature get the service
// automatically — no manual copying of Kotlin files needed.
#[cfg(all(target_os = "android", feature = "push-sync"))]
#[manganis::ffi("src/android")]
extern "Kotlin" {}

// ── iOS: bundle Swift package ───────────────────────────────────────────────

// Bundle the Swift WaveSyncPush package as a Swift Package Manager library.
// `dx build` compiles it via `xcrun swift build` and links the static library
// into the app binary. The ObjC `+load` method in `WaveSyncAppDelegateProxy`
// runs at image load and installs the APNs AppDelegate selectors automatically.
//
// The `pub type WaveSyncPush` anchor references the package's product name
// so the linker cannot dead-strip the static archive.
#[cfg(all(target_os = "ios", feature = "push-sync"))]
#[manganis::ffi("src/ios")]
extern "Swift" {
    pub type WaveSyncPush;
}

// ── Shared: file-based token reading ────────────────────────────────────────

#[cfg(any(target_os = "android", target_os = "ios"))]
use std::path::Path;

/// The filename where the Android Kotlin service writes the FCM token.
#[cfg(target_os = "android")]
pub const FCM_TOKEN_FILENAME: &str = "wavesync_fcm_token";

/// The filename where the iOS Swift handler writes the APNs device token.
#[cfg(target_os = "ios")]
pub const APNS_TOKEN_FILENAME: &str = "wavesync_apns_token";

/// Extract the filesystem path from a SQLite URL.
///
/// Handles formats like:
/// - `sqlite:///data/data/com.app/files/db.db?mode=rwc`
/// - `sqlite:/data/data/com.app/files/db.db?mode=rwc`
/// - `sqlite:db.db?mode=rwc`
pub(crate) fn extract_db_path(url: &str) -> Option<String> {
    let without_scheme = url.strip_prefix("sqlite:")?;
    let without_params = without_scheme.split('?').next()?;
    // Strip leading slashes for URI format (sqlite:///path → /path)
    let path = without_params.trim_start_matches('/');
    if path.is_empty() {
        return None;
    }
    // Re-add leading slash for absolute paths
    if without_params.starts_with('/') {
        Some(format!("/{path}"))
    } else {
        Some(path.to_string())
    }
}

/// Read a push token from a file next to the database.
///
/// Returns `None` if the file doesn't exist yet (first launch before the
/// native service delivers the token).
#[cfg(any(target_os = "android", target_os = "ios"))]
fn read_token_from_file(database_url: &str, filename: &str, platform_name: &str) -> Option<String> {
    let db_path = extract_db_path(database_url)?;
    let dir = Path::new(&db_path).parent()?;
    let token_path = dir.join(filename);

    match std::fs::read_to_string(&token_path) {
        Ok(token) => {
            let token = token.trim().to_string();
            if token.is_empty() {
                None
            } else {
                tracing::info!(
                    "{platform_name} token read from {}: {}...",
                    token_path.display(),
                    &token[..token.len().min(10)]
                );
                Some(token)
            }
        }
        Err(_) => {
            tracing::debug!(
                "No {platform_name} token file at {} (expected on first launch)",
                token_path.display()
            );
            None
        }
    }
}

// ── Android-specific ────────────────────────────────────────────────────────

/// Read the FCM token from the file written by `WaveSyncService`.
///
/// The token file is located next to the database file (in the same directory).
/// Returns `None` if the file doesn't exist yet (first launch before Firebase
/// delivers the token).
#[cfg(target_os = "android")]
pub(crate) fn read_token_file(database_url: &str) -> Option<String> {
    read_token_from_file(database_url, FCM_TOKEN_FILENAME, "FCM")
}

/// Fire-and-forget request for the Kotlin push bootstrap to run again now
/// that `.wavesync_config.json` exists on disk.
///
/// On a fresh install's first process lifetime, `WaveSyncInitProvider` runs
/// at process start — BEFORE `build()` has written the config — so its
/// Firebase init finds nothing and Kotlin never retries: the token file is
/// never written, push registration never happens, and the entire first
/// session (exactly the onboarding session) can neither wake peers nor be
/// woken (#106). Rust already re-reads the token file mid-run every 5s;
/// only the Kotlin retry was missing. Called from `build()` right after
/// `SyncConfig::save`.
///
/// Spawns a named thread because `WaveSyncService.ensureTokenFile` blocks on
/// `Tasks.await` (an FCM round-trip), which must run on neither the caller
/// nor the Android main thread. In the background-sync (FCM service) process
/// `ndk_context` was never initialized and panics — contained by
/// `catch_unwind`; that process doesn't need this call (its InitProvider
/// already ran with the config present, else no push could have woken it).
#[cfg(all(target_os = "android", feature = "push-sync"))]
pub(crate) fn request_android_token_file() {
    let spawned = std::thread::Builder::new()
        .name("wavesync-fcm-token".into())
        .spawn(|| {
            if std::panic::catch_unwind(ensure_token_file_via_jni).is_err() {
                tracing::debug!(
                    "push: token-file request skipped (no ndk context in this process)"
                );
            }
        });
    if let Err(e) = spawned {
        tracing::warn!("push: could not spawn token-file request thread: {e}");
    }
}

/// JNI body of [`request_android_token_file`]: bootstrap `JavaVM` + `Context`
/// via `ndk_context` (same pattern as the foreground notification display)
/// and invoke `@JvmStatic WaveSyncService.ensureTokenFile(Context)`.
#[cfg(all(target_os = "android", feature = "push-sync"))]
fn ensure_token_file_via_jni() {
    use jni::JavaVM;
    use jni::objects::{JObject, JValue};

    let ctx = ndk_context::android_context();
    let vm = match unsafe { JavaVM::from_raw(ctx.vm().cast()) } {
        Ok(vm) => vm,
        Err(e) => {
            tracing::warn!("push: JavaVM unavailable for token-file request: {e}");
            return;
        }
    };
    let mut env = match vm.attach_current_thread() {
        Ok(env) => env,
        Err(e) => {
            tracing::warn!("push: JNI attach failed for token-file request: {e}");
            return;
        }
    };
    let context = unsafe { JObject::from_raw(ctx.context().cast()) };

    // App classloader, not FindClass — this is a Rust-spawned thread (see
    // `resolve_app_class`).
    let service = match resolve_app_class(&mut env, &context, "dev.dioxus.main.WaveSyncService") {
        Some(c) => c,
        None => {
            tracing::warn!("push: could not resolve WaveSyncService via app classloader");
            describe_and_clear(&mut env);
            return;
        }
    };
    match env.call_static_method(
        &service,
        "ensureTokenFile",
        "(Landroid/content/Context;)V",
        &[JValue::Object(&context)],
    ) {
        Ok(_) => tracing::info!(
            "push: requested FCM token file now that the sync config is on disk"
        ),
        Err(e) => {
            tracing::warn!("push: WaveSyncService.ensureTokenFile failed: {e}");
            describe_and_clear(&mut env);
        }
    }
}

/// Load an application class by its binary name via the Context's classloader,
/// rather than `FindClass` (which uses the system loader on non-JVM threads and
/// cannot see app classes). Returns `None` on failure, leaving any pending
/// exception for the caller to describe/clear.
#[cfg(all(target_os = "android", feature = "push-sync"))]
pub(crate) fn resolve_app_class<'a>(
    env: &mut jni::JNIEnv<'a>,
    context: &jni::objects::JObject,
    binary_name: &str,
) -> Option<jni::objects::JClass<'a>> {
    use jni::objects::{JClass, JValue};

    let loader = env
        .call_method(context, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])
        .ok()?
        .l()
        .ok()?;
    let name = env.new_string(binary_name).ok()?;
    let class = env
        .call_method(
            &loader,
            "loadClass",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&name)],
        )
        .ok()?
        .l()
        .ok()?;
    Some(JClass::from(class))
}

/// Print a pending Java exception's stack trace to logcat, then clear it.
#[cfg(all(target_os = "android", feature = "push-sync"))]
pub(crate) fn describe_and_clear(env: &mut jni::JNIEnv) {
    if env.exception_check().unwrap_or(false) {
        let _ = env.exception_describe();
        let _ = env.exception_clear();
    }
}

/// Firebase credentials parsed from `google-services.json`.
///
/// Used for validation at build time and persisted in the sync config
/// so the background service can re-initialize Firebase if needed.
#[cfg(feature = "push-sync")]
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct FcmCredentials {
    pub project_id: String,
    pub app_id: String,
    pub api_key: String,
}

#[cfg(feature = "push-sync")]
impl FcmCredentials {
    /// Parse a `google-services.json` file to extract Firebase credentials.
    pub(crate) fn from_google_services_json(json: &str) -> Result<Self, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(json).map_err(|e| format!("Invalid google-services.json: {e}"))?;

        let project_id = parsed["project_info"]["project_id"]
            .as_str()
            .ok_or("Missing project_info.project_id in google-services.json")?
            .to_string();

        let clients = parsed["client"]
            .as_array()
            .ok_or("Missing client array in google-services.json")?;

        let client = clients
            .first()
            .ok_or("Empty client array in google-services.json")?;

        let app_id = client["client_info"]["mobilesdk_app_id"]
            .as_str()
            .ok_or("Missing client_info.mobilesdk_app_id in google-services.json")?
            .to_string();

        let api_key = client["api_key"]
            .as_array()
            .and_then(|keys| keys.first())
            .and_then(|k| k["current_key"].as_str())
            .ok_or("Missing api_key[0].current_key in google-services.json")?
            .to_string();

        Ok(Self {
            project_id,
            app_id,
            api_key,
        })
    }
}

// ── iOS-specific ────────────────────────────────────────────────────────────

/// Read the APNs device token from the file written by `WaveSyncPushHandler`.
///
/// The token file is located next to the database file (in the same directory).
/// Returns `None` if the file doesn't exist yet (first launch before the app
/// registers for remote notifications).
///
/// Swift discovers this same directory by reading `.wavesync_config.json`
/// (written by `SyncConfig::save` in `connection.rs`) — Rust does not push
/// the path across to Swift, keeping the build-time link graph strictly
/// Swift → Rust.
#[cfg(target_os = "ios")]
pub(crate) fn read_apns_token_file(database_url: &str) -> Option<String> {
    read_token_from_file(database_url, APNS_TOKEN_FILENAME, "APNs")
}

/// Force-load the Swift `WaveSyncPush` framework so its ObjC `+load` method
/// runs and installs the APNs AppDelegate selectors.
///
/// `dx build` compiles the Swift Package into a dynamic framework
/// (`DioxusSwiftPlugins.framework`) and embeds it in `.app/Frameworks/`, but
/// it never adds an `LC_LOAD_DYLIB` entry on the Rust-produced main
/// executable, because Rust's static link phase runs *before* the Swift
/// framework is built. Without that link command, dyld has no reason to
/// load the framework at launch, so `+load` never fires and the swizzle
/// never happens.
///
/// We work around the ordering by calling `dlopen` from Rust at DB-build
/// time — long before the UI event loop starts, so the `+load` handler
/// can finish its `UIApplicationDidFinishLaunchingNotification` observer
/// registration in time to catch the cold-start launch. Uses the standard
/// `@executable_path` rpath semantics so the same path works in both the
/// simulator and a signed on-device bundle.
///
/// Idempotent: `dlopen` reference-counts, so repeated calls are harmless.
/// Force-preserve the C-ABI FFI symbols the Swift framework will need at
/// runtime. The Rust linker aggressively dead-strips `#[unsafe(no_mangle)]`
/// functions that no Rust code references, which is our situation here:
/// `wavesync_background_sync_with_peers` is only called from Swift, via
/// dyld's flat-namespace lookup into the main executable. Referencing the
/// function address from reachable Rust code (this function is called from
/// `WaveSyncDbBuilder::build`) teaches the linker to keep the symbol.
#[cfg(all(feature = "push-sync", feature = "mobile-ffi", target_os = "ios"))]
#[inline(never)]
fn anchor_ios_ffi_exports() {
    // Cast through a volatile write so even aggressive LTO cannot elide.
    let slots: [*const (); 3] = [
        crate::ffi::wavesync_background_sync as *const (),
        crate::ffi::wavesync_background_sync_with_peers as *const (),
        crate::ffi::wavesync_background_sync_targeted as *const (),
    ];
    unsafe {
        std::ptr::read_volatile(&slots as *const [*const (); 3]);
    }
}

#[cfg(all(feature = "push-sync", target_os = "ios"))]
pub(crate) fn load_ios_push_framework() {
    use std::ffi::CStr;
    use std::os::raw::{c_char, c_int, c_void};

    // Keep the C FFI exports alive against linker dead-strip.
    #[cfg(feature = "mobile-ffi")]
    anchor_ios_ffi_exports();

    unsafe extern "C" {
        fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
        fn dlerror() -> *const c_char;
    }
    const RTLD_NOW: c_int = 0x2;

    // dx currently bundles every Swift Package referenced via
    // `#[manganis::ffi]` into a single combined framework named
    // `DioxusSwiftPlugins`. If dx's bundling convention ever changes, this
    // constant is the only place that needs updating.
    let framework_path =
        c"@executable_path/Frameworks/DioxusSwiftPlugins.framework/DioxusSwiftPlugins";

    unsafe {
        let handle = dlopen(framework_path.as_ptr(), RTLD_NOW);
        if handle.is_null() {
            let err_ptr = dlerror();
            let err = if err_ptr.is_null() {
                "unknown dlopen error".into()
            } else {
                CStr::from_ptr(err_ptr).to_string_lossy()
            };
            tracing::warn!(
                "Failed to load WaveSyncPush framework: {err}. \
                 APNs integration will be unavailable this launch."
            );
        } else {
            tracing::info!("Loaded WaveSyncPush framework (dlopen); +load handlers active");
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "push-sync")]
    #[test]
    fn test_parse_google_services_json() {
        let json = r#"{
            "project_info": {
                "project_number": "829552879299",
                "project_id": "wavesync-f36b5",
                "storage_bucket": "wavesync-f36b5.firebasestorage.app"
            },
            "client": [{
                "client_info": {
                    "mobilesdk_app_id": "1:829552879299:android:f87f1b101d5b3f2b476476",
                    "android_client_info": { "package_name": "com.pvazquez.example" }
                },
                "api_key": [{ "current_key": "AIzaSyC7yttzT4g7R83Vx7vQw9WPH_CqXoCXhpc" }]
            }]
        }"#;

        let creds = FcmCredentials::from_google_services_json(json).unwrap();
        assert_eq!(creds.project_id, "wavesync-f36b5");
        assert_eq!(
            creds.app_id,
            "1:829552879299:android:f87f1b101d5b3f2b476476"
        );
        assert_eq!(creds.api_key, "AIzaSyC7yttzT4g7R83Vx7vQw9WPH_CqXoCXhpc");
    }

    #[cfg(feature = "push-sync")]
    #[test]
    fn test_parse_google_services_json_invalid() {
        assert!(FcmCredentials::from_google_services_json("not json").is_err());
        assert!(FcmCredentials::from_google_services_json("{}").is_err());
        assert!(
            FcmCredentials::from_google_services_json(r#"{"project_info":{},"client":[]}"#)
                .is_err()
        );
    }

    #[test]
    fn test_extract_db_path_ios_application_support() {
        // iOS path shape produced by `dioxus_sdk_storage::data_directory()`.
        let url = "sqlite:///var/mobile/Containers/Data/Application/\
                   12345678-1234-1234-1234-123456789abc/Library/Application Support/\
                   com.example.myapp/mobile_tasks.db?mode=rwc";
        let path = extract_db_path(url).expect("iOS app-support path should parse");
        assert!(path.starts_with("/var/mobile/Containers/"));
        assert!(path.ends_with("/mobile_tasks.db"));
        assert!(!path.contains('?'));
    }

    #[test]
    fn test_extract_db_path_android() {
        let url = "sqlite:///data/data/com.example.myapp/files/app.db?mode=rwc";
        assert_eq!(
            extract_db_path(url).as_deref(),
            Some("/data/data/com.example.myapp/files/app.db")
        );
    }

    #[test]
    fn test_extract_db_path_relative() {
        assert_eq!(
            extract_db_path("sqlite:app.db?mode=rwc").as_deref(),
            Some("app.db")
        );
    }

    #[test]
    fn test_extract_db_path_empty_returns_none() {
        assert!(extract_db_path("sqlite:").is_none());
        assert!(extract_db_path("sqlite:///").is_none());
    }
}
