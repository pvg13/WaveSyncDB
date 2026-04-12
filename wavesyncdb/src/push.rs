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

// ── Shared: file-based token reading ────────────────────────────────────────

#[cfg(any(target_os = "android", target_os = "ios"))]
use std::path::Path;

/// The filename where the Android Kotlin service writes the FCM token.
#[cfg(target_os = "android")]
pub const FCM_TOKEN_FILENAME: &str = "wavesync_fcm_token";

/// The filename where the iOS Swift handler writes the APNs device token.
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
                log::info!(
                    "{platform_name} token read from {}: {}...",
                    token_path.display(),
                    &token[..token.len().min(10)]
                );
                Some(token)
            }
        }
        Err(_) => {
            log::debug!(
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

// ── iOS-specific ────────────────────────────────────────────────────────────

/// Read the APNs device token from the file written by `WaveSyncTokenWriter`.
///
/// The token file is located next to the database file (in the same directory).
/// Returns `None` if the file doesn't exist yet (first launch before the app
/// registers for remote notifications).
#[cfg(target_os = "ios")]
pub(crate) fn read_apns_token_file(database_url: &str) -> Option<String> {
    read_token_from_file(database_url, APNS_TOKEN_FILENAME, "APNs")
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

    // extract_db_path and read_token_file are only compiled on mobile targets
    // so their tests are also mobile-only.
}
