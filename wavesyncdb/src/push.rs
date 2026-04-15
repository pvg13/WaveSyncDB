//! Push notification integration for mobile platforms.
//!
//! Native services persist push tokens to `SyncConfig` via Rust FFI:
//!
//! - **Android (FCM):** The Kotlin `WaveSyncService` calls `setPushToken()` JNI
//!   to persist the FCM token in `SyncConfig`.
//! - **iOS (APNs):** The Swift `WaveSyncTokenWriter` calls `wavesync_set_push_token()`
//!   C FFI to persist the APNs device token in `SyncConfig`.
//! - **Dioxus iOS:** The injected ObjC callback stores the token in a Rust static
//!   and calls `register_push_token()` on the running engine directly.
//!
//! During cold sync (`background_sync`), the token is read from `SyncConfig`
//! and registered with the relay server.

// ── Android: bundle Kotlin sources ──────────────────────────────────────────

// Bundle the Android Kotlin sources (WaveSyncService, WaveSyncInitProvider) as a
// Gradle submodule. dx build picks this up from the compiled binary's symbol table,
// so apps that depend on wavesyncdb with `android-fcm` feature get the service
// automatically — no manual copying of Kotlin files needed.
#[cfg(all(target_os = "android", feature = "android-fcm"))]
#[manganis::ffi("src/android")]
extern "Kotlin" {}

// ── iOS: bundle Swift package ───────────────────────────────────────────────

// Bundle the Swift WaveSyncPush package (WaveSyncTokenWriter, WaveSyncPushHandler)
// as a Swift Package Manager library. dx build compiles it via `xcrun swift build`
// and links the static library into the app binary.
#[cfg(all(target_os = "ios", feature = "ios-push"))]
#[manganis::ffi("src/ios")]
extern "Swift" {
    pub type WaveSyncPush;
}

// ── Shared utilities ────────────────────────────────────────────────────────

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

// ── Android-specific ────────────────────────────────────────────────────────

/// Firebase credentials parsed from `google-services.json`.
///
/// Used for validation at build time and persisted in the sync config
/// so the background service can re-initialize Firebase if needed.
#[cfg(feature = "android-fcm")]
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct FcmCredentials {
    pub project_id: String,
    pub app_id: String,
    pub api_key: String,
}

#[cfg(feature = "android-fcm")]
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

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "android-fcm")]
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

    #[cfg(feature = "android-fcm")]
    #[test]
    fn test_parse_google_services_json_invalid() {
        assert!(FcmCredentials::from_google_services_json("not json").is_err());
        assert!(FcmCredentials::from_google_services_json("{}").is_err());
        assert!(
            FcmCredentials::from_google_services_json(r#"{"project_info":{},"client":[]}"#)
                .is_err()
        );
    }
}
