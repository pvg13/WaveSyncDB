//! On-disk cache for derived group keys.
//!
//! `GroupKey::from_passphrase` runs Argon2id at ~19 MiB of memory (see
//! `auth.rs`) — deliberately heavy, to price out offline dictionary attacks
//! against the relay-visible derived topic. That cost is fine for a normal
//! app process, but it does not fit the iOS Notification Service Extension
//! (NSE), which iOS caps at roughly 24 MB total: the KDF alone would consume
//! most of the extension's entire budget before the sync engine even starts.
//!
//! The fix is to never run the KDF inside the NSE at all. Instead, the main
//! app derives the key once (at `build()` / `join_group()`, its normal
//! foreground cost) and persists the raw 32 bytes to a small on-disk cache
//! file, `.wavesync_group_keys.json`, keyed by the plaintext user topic. The
//! NSE reads that file — a 32-byte load, not a KDF — and gets a ready-made
//! key. If the cache is missing (fresh install, cache cleared, or the app
//! never got a chance to warm it), the NSE has no way to derive the key
//! itself; the sync for that wake simply doesn't happen and the OS shows the
//! operator's placeholder alert content instead (safe fallback by
//! construction — see `ffi::wavesync_nse_handle_push`).
//!
//! This module holds only the pure load/save logic and is compiled on every
//! native target so it can be unit-tested on the host. Whether it is ever
//! *called* is gated at the call sites (`connection::group_key_for_dir`) to
//! `#[cfg(target_os = "ios")]` — every other platform's process budget can
//! afford the KDF outright and gains nothing from caching key material to
//! disk (and every unnecessary copy of a symmetric secret at rest is a cost,
//! not a feature).
//!
//! **At-rest protection**: this module only writes bytes; it does not set
//! any iOS data-protection class. The app is expected to mark the cache file
//! `NSFileProtectionCompleteUntilFirstUserAuthentication` from Swift (the
//! same class `WaveSyncPushHandler` already uses for the APNs token file) —
//! background-launchable (the NSE can run before first unlock) while still
//! encrypted at rest. See the `WaveSyncNotificationService` template.

use std::collections::HashMap;
use std::path::Path;

/// Filename for the cache, written beside `.wavesync_config.json` (i.e. in
/// the same directory as the SQLite database).
const CACHE_FILE_NAME: &str = ".wavesync_group_keys.json";

fn cache_path(dir: &Path) -> std::path::PathBuf {
    dir.join(CACHE_FILE_NAME)
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn decode_hex(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Load the on-disk `{topic_name: hex_key}` map. Every failure mode (file
/// missing, unreadable, not valid JSON, wrong shape) yields an empty map
/// rather than propagating an error — a cold cache is exactly as valid a
/// starting state as a corrupt one; both just mean "derive fresh".
fn load_map(dir: &Path) -> HashMap<String, String> {
    let path = cache_path(dir);
    match std::fs::read_to_string(&path) {
        Ok(json) => serde_json::from_str(&json).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

/// Persist a derived group key for `topic_name` (the plaintext user topic,
/// not the PSK-derived effective topic — the key is what's needed to derive
/// the latter, so it can't be the map key). Upserts into the existing cache
/// file, preserving any other topics' entries.
///
/// Best-effort: a write failure (read-only filesystem, out of space, no
/// parent directory) is logged and swallowed. Losing the cache costs one
/// extra Argon2id derivation on the next foreground launch — never data.
///
/// Atomic: writes to a sibling temp file and renames over the real path, so
/// a process kill mid-write (very much the normal case for anything sharing
/// a codepath with the NSE) never leaves a truncated or half-written cache
/// for the next reader to choke on.
pub fn save_group_key(dir: &Path, topic_name: &str, key: &[u8; 32]) {
    let mut map = load_map(dir);
    map.insert(topic_name.to_string(), encode_hex(key));

    let json = match serde_json::to_string_pretty(&map) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("key_cache: failed to serialize group key cache: {e}");
            return;
        }
    };

    let path = cache_path(dir);
    let tmp_path = dir.join(format!("{CACHE_FILE_NAME}.tmp"));
    if let Err(e) = std::fs::write(&tmp_path, &json) {
        tracing::warn!(
            "key_cache: failed to write temp cache file {}: {e}",
            tmp_path.display()
        );
        return;
    }
    if let Err(e) = std::fs::rename(&tmp_path, &path) {
        tracing::warn!(
            "key_cache: failed to atomically install cache file {}: {e}",
            path.display()
        );
        let _ = std::fs::remove_file(&tmp_path);
        return;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)) {
            tracing::warn!(
                "key_cache: could not restrict permissions on {}: {e}",
                path.display()
            );
        }
    }
}

/// Load a previously cached group key for `topic_name`. Returns `None` for
/// every failure mode — missing file, unreadable, corrupt JSON, wrong shape,
/// malformed hex, or no entry for this topic — so a caller can always treat
/// `None` uniformly as "derive it yourself"; this function never panics.
pub fn load_group_key(dir: &Path, topic_name: &str) -> Option<[u8; 32]> {
    let map = load_map(dir);
    map.get(topic_name).and_then(|hex| decode_hex(hex))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("wavesync_key_cache_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn roundtrip() {
        let dir = temp_dir();
        let key = [7u8; 32];
        save_group_key(&dir, "roommates", &key);
        assert_eq!(load_group_key(&dir, "roommates"), Some(key));
    }

    #[test]
    fn missing_file_returns_none() {
        let dir = temp_dir();
        assert_eq!(load_group_key(&dir, "anything"), None);
    }

    #[test]
    fn missing_dir_returns_none_not_panic() {
        let dir = std::env::temp_dir().join(format!(
            "wavesync_key_cache_missing_{}",
            uuid::Uuid::new_v4()
        ));
        assert_eq!(load_group_key(&dir, "anything"), None);
    }

    #[test]
    fn corrupt_json_returns_none_not_panic() {
        let dir = temp_dir();
        std::fs::write(dir.join(".wavesync_group_keys.json"), b"not json {{{").unwrap();
        assert_eq!(load_group_key(&dir, "roommates"), None);
    }

    #[test]
    fn wrong_shape_json_returns_none_not_panic() {
        let dir = temp_dir();
        // Valid JSON, but not an object of string->string.
        std::fs::write(dir.join(".wavesync_group_keys.json"), b"[1, 2, 3]").unwrap();
        assert_eq!(load_group_key(&dir, "roommates"), None);
    }

    #[test]
    fn malformed_hex_value_returns_none_not_panic() {
        let dir = temp_dir();
        std::fs::write(
            dir.join(".wavesync_group_keys.json"),
            br#"{"roommates": "not-hex-and-wrong-length"}"#,
        )
        .unwrap();
        assert_eq!(load_group_key(&dir, "roommates"), None);
    }

    #[test]
    fn multi_topic_entries_are_independent() {
        let dir = temp_dir();
        save_group_key(&dir, "alpha", &[1u8; 32]);
        save_group_key(&dir, "beta", &[2u8; 32]);
        assert_eq!(load_group_key(&dir, "alpha"), Some([1u8; 32]));
        assert_eq!(load_group_key(&dir, "beta"), Some([2u8; 32]));
        assert_eq!(load_group_key(&dir, "gamma"), None);
    }

    #[test]
    fn atomic_overwrite_updates_in_place() {
        let dir = temp_dir();
        save_group_key(&dir, "roommates", &[1u8; 32]);
        save_group_key(&dir, "roommates", &[2u8; 32]);
        assert_eq!(load_group_key(&dir, "roommates"), Some([2u8; 32]));
        assert!(!dir.join(".wavesync_group_keys.json.tmp").exists());
    }

    #[test]
    fn hex_roundtrip_covers_every_byte_value() {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = (i * 8 + 3) as u8;
        }
        let dir = temp_dir();
        save_group_key(&dir, "x", &key);
        assert_eq!(load_group_key(&dir, "x"), Some(key));
    }
}
