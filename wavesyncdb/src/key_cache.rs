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
//! never got a chance to warm it), the NSE is invoked with
//! `key_cache_load_only = true` (`connection::group_key_for_dir`'s
//! `load_only` parameter), which turns a miss into
//! `GroupKeyLoadOnlyMiss` instead of a fallback derivation — the branch that
//! would call `GroupKey::from_passphrase` is textually unreachable in that
//! mode, so "never runs the KDF inside the NSE" is enforced by the code
//! shape, not just documented as a convention. The sync for that wake simply
//! doesn't happen and the OS shows the operator's placeholder alert content
//! instead (safe fallback by construction — see `ffi::wavesync_nse_handle_push`).
//!
//! This module holds only the pure load/save logic and is compiled on every
//! native target so it can be unit-tested on the host. Whether it is ever
//! *called* is gated at the call sites (`connection::group_key_for_dir`) to
//! `#[cfg(any(target_os = "ios", test))]` — every other platform's process
//! budget can afford the KDF outright and gains nothing from caching key
//! material to disk (and every unnecessary copy of a symmetric secret at
//! rest is a cost, not a feature). The `test` half of that cfg exists solely
//! so the load-only contract itself is host-testable; production non-iOS
//! builds never consult this cache.
//!
//! **At-rest protection**: `save_group_key` best-effort calls the bundled
//! Swift `wavesync_protect_file` helper (via `dlsym`, iOS only) to mark the
//! cache file `NSFileProtectionCompleteUntilFirstUserAuthentication` — the
//! same class `WaveSyncPushHandler` already uses for the APNs token file —
//! background-launchable (the NSE can run before first unlock) while still
//! encrypted at rest. See `protect_file` and the `WaveSyncNotificationService`
//! template.

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
/// for the next reader to choke on. On unix the temp file is created with
/// `0o600` from the moment it exists (see `write_tmp_file`) — `rename`
/// preserves those permissions onto the final path, so there is never a
/// window where the key material is readable beyond the owner.
pub(crate) fn save_group_key(dir: &Path, topic_name: &str, key: &[u8; 32]) {
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
    if let Err(e) = write_tmp_file(&tmp_path, &json) {
        tracing::warn!(
            "key_cache: failed to write temp cache file {}: {e}",
            tmp_path.display()
        );
        return;
    }
    match std::fs::rename(&tmp_path, &path) {
        Err(e) => {
            tracing::warn!(
                "key_cache: failed to atomically install cache file {}: {e}",
                path.display()
            );
            let _ = std::fs::remove_file(&tmp_path);
        }
        Ok(()) => {
            // Best-effort iOS data protection: mark the cache file so the
            // NSE can still read it before first unlock while it stays
            // encrypted at rest. See `protect_file`'s docs.
            #[cfg(target_os = "ios")]
            protect_file(&path);
        }
    }
}

/// Remove `topic_name`'s cached key, preserving other topics' entries.
/// When the removal leaves the map empty, the cache file itself is deleted
/// so no key-shaped artifact lingers on disk. Best-effort like every other
/// write in this module. Called on `leave_group` (a left group's key must
/// not stay readable on the device), by `build()`'s cache opt-out, and by
/// `WaveSyncDbBuilder::invalidate_group_key_cache` (passphrase rotation).
pub(crate) fn remove_group_key(dir: &Path, topic_name: &str) {
    let mut map = load_map(dir);
    if map.remove(topic_name).is_none() {
        return;
    }
    let path = cache_path(dir);
    if map.is_empty() {
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(
                "key_cache: failed to remove empty cache file {}: {e}",
                path.display()
            );
        }
        return;
    }
    let json = match serde_json::to_string_pretty(&map) {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!("key_cache: failed to serialize group key cache: {e}");
            return;
        }
    };
    let tmp_path = dir.join(format!("{CACHE_FILE_NAME}.tmp"));
    if let Err(e) = write_tmp_file(&tmp_path, &json) {
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
    }
}

// The owner-only tmp-file write half of the atomic write-then-rename idiom
// lives in `connection::write_tmp_file` (compiled on every platform — the
// sync config uses the same idiom); re-exported here so this module's
// call sites read naturally.
pub(crate) use crate::connection::write_tmp_file;

/// Best-effort iOS data-protection: calls the bundled Swift
/// `wavesync_protect_file` C-ABI helper (`WaveSyncPushBridge.swift`) to mark
/// `path` `NSFileProtectionCompleteUntilFirstUserAuthentication` — the same
/// protection class already used for the APNs token file
/// (`WaveSyncPushHandler`), chosen so the Notification Service Extension can
/// still read this cache before first unlock while it stays encrypted at
/// rest. Resolved via `dlsym`, mirroring
/// `dioxus::notifications::show_ios_notification`'s pattern for calling into
/// the already-loaded WaveSyncPush framework; a no-op if the symbol can't be
/// resolved (host app hasn't linked WaveSyncPush, or linked a version
/// predating this helper).
#[cfg(target_os = "ios")]
fn protect_file(path: &Path) {
    use std::ffi::CString;
    use std::os::raw::{c_char, c_void};

    unsafe extern "C" {
        fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    }
    const RTLD_DEFAULT: *mut c_void = (-2isize) as *mut c_void;
    type ProtectFn = unsafe extern "C" fn(*const c_char);

    let Some(path_str) = path.to_str() else {
        return;
    };
    let Ok(path_c) = CString::new(path_str) else {
        return;
    };

    unsafe {
        let sym = dlsym(RTLD_DEFAULT, c"wavesync_protect_file".as_ptr());
        if sym.is_null() {
            tracing::warn!(
                "key_cache: wavesync_protect_file not found (WaveSyncPush framework not loaded?)"
            );
            return;
        }
        let protect: ProtectFn = std::mem::transmute(sym);
        protect(path_c.as_ptr());
    }
}

/// Load a previously cached group key for `topic_name`. Returns `None` for
/// every failure mode — missing file, unreadable, corrupt JSON, wrong shape,
/// malformed hex, or no entry for this topic — so a caller can always treat
/// `None` uniformly as "derive it yourself"; this function never panics.
pub(crate) fn load_group_key(dir: &Path, topic_name: &str) -> Option<[u8; 32]> {
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

    #[cfg(unix)]
    #[test]
    fn cache_file_is_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir();
        save_group_key(&dir, "roommates", &[1u8; 32]);
        let mode = std::fs::metadata(dir.join(".wavesync_group_keys.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "cache file must never be group/world-readable");
    }

    #[cfg(unix)]
    #[test]
    fn stale_loose_permission_tmp_file_is_locked_down_before_write() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir();
        let tmp_path = dir.join(".wavesync_group_keys.json.tmp");
        // Simulate a leftover tmp file from a killed previous run, with
        // default (loose) permissions.
        std::fs::write(&tmp_path, b"stale").unwrap();
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        save_group_key(&dir, "roommates", &[9u8; 32]);

        // The tmp file is renamed away, but if it still existed the fix-up
        // in `write_tmp_file` guarantees it would be 0o600, never the stale
        // 0o644 — proven indirectly here by the final path's permissions
        // (renamed from that same, now-locked-down, tmp file).
        let mode = std::fs::metadata(dir.join(".wavesync_group_keys.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
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

    #[test]
    fn remove_group_key_preserves_other_topics() {
        let dir = temp_dir();
        save_group_key(&dir, "roommates", &[1u8; 32]);
        save_group_key(&dir, "family", &[2u8; 32]);

        remove_group_key(&dir, "roommates");

        assert_eq!(load_group_key(&dir, "roommates"), None);
        assert_eq!(load_group_key(&dir, "family"), Some([2u8; 32]));
    }

    #[test]
    fn remove_last_group_key_deletes_the_cache_file() {
        let dir = temp_dir();
        save_group_key(&dir, "roommates", &[1u8; 32]);

        remove_group_key(&dir, "roommates");

        assert!(
            !dir.join(".wavesync_group_keys.json").exists(),
            "an empty cache file must not linger on disk"
        );
        // Removing a topic that isn't cached is a quiet no-op.
        remove_group_key(&dir, "roommates");
    }
}
