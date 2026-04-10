//! Firebase Cloud Messaging integration for Android.
//!
//! Instead of calling Firebase via JNI from Rust (which has classloader issues on
//! native threads), we use a file-based approach:
//!
//! 1. The Kotlin `WaveSyncService` handles Firebase init and token retrieval
//! 2. It writes the FCM token to a file next to the database
//! 3. Rust reads the token file during [`WaveSyncDbBuilder::build()`](crate::WaveSyncDbBuilder::build)
//!
//! This avoids all JNI calls from Rust for FCM setup, eliminating classloader
//! issues that occur when `FindClass` is called from native threads.

// Bundle the Android Kotlin sources (WaveSyncService, WaveSyncInitProvider) as a
// Gradle submodule. dx build picks this up from the compiled binary's symbol table,
// so apps that depend on wavesyncdb with `android-fcm` feature get the service
// automatically — no manual copying of Kotlin files needed.
#[cfg(target_os = "android")]
#[manganis::ffi("src/android")]
extern "Kotlin" {}

#[cfg(target_os = "android")]
use std::path::Path;

/// The filename where the Kotlin service writes the FCM token.
#[cfg(target_os = "android")]
pub const TOKEN_FILENAME: &str = "wavesync_fcm_token";

/// Firebase credentials parsed from `google-services.json`.
///
/// Used for validation at build time and persisted in the sync config
/// so the background service can re-initialize Firebase if needed.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct FcmCredentials {
    pub project_id: String,
    pub app_id: String,
    pub api_key: String,
}

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

#[cfg(target_os = "android")]
/// Read the FCM token from the file written by `WaveSyncService`.
///
/// The token file is located next to the database file (in the same directory).
/// Returns `None` if the file doesn't exist yet (first launch before Firebase
/// delivers the token).
pub(crate) fn read_token_file(database_url: &str) -> Option<String> {
    let db_path = extract_db_path(database_url)?;
    let dir = Path::new(&db_path).parent()?;
    let token_path = dir.join(TOKEN_FILENAME);

    match std::fs::read_to_string(&token_path) {
        Ok(token) => {
            let token = token.trim().to_string();
            if token.is_empty() {
                None
            } else {
                log::info!(
                    "FCM token read from {}: {}...",
                    token_path.display(),
                    &token[..token.len().min(10)]
                );
                Some(token)
            }
        }
        Err(_) => {
            log::debug!(
                "No FCM token file at {} (expected on first launch)",
                token_path.display()
            );
            None
        }
    }
}

#[cfg(target_os = "android")]
/// Extract the filesystem path from a SQLite URL.
///
/// Handles formats like:
/// - `sqlite:///data/data/com.app/files/db.db?mode=rwc`
/// - `sqlite:/data/data/com.app/files/db.db?mode=rwc`
/// - `sqlite:db.db?mode=rwc`
fn extract_db_path(url: &str) -> Option<String> {
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_parse_google_services_json_invalid() {
        assert!(FcmCredentials::from_google_services_json("not json").is_err());
        assert!(FcmCredentials::from_google_services_json("{}").is_err());
        assert!(
            FcmCredentials::from_google_services_json(r#"{"project_info":{},"client":[]}"#)
                .is_err()
        );
    }

    // extract_db_path and read_token_file are only compiled on Android
    // so their tests are also Android-only.
}
