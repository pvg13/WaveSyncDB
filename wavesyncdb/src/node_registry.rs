//! Process-global registry of live sync nodes, keyed by canonical DB path.
//!
//! A push wake (FCM service / APNs handler) runs in the **same OS process**
//! as the app's engine whenever the app is backgrounded-but-not-killed. The
//! libp2p keypair and site_id are persisted in the database, so building a
//! second engine on the same file produces a **duplicate identity** in the
//! swarm — two live engines with one PeerId contend for the relay
//! reservation and confuse remote peers' per-PeerId sync state. This
//! registry lets the background-sync entry points find and reuse the live
//! engine instead: [`crate::WaveSyncDbBuilder::build`] registers every node
//! here, and `background_sync` looks the path up before falling back to the
//! cold (build-a-fresh-engine) path.
//!
//! Entries are [`Weak`], mirroring the `groups` map on the node itself: the
//! registry must never keep an engine alive past the last external handle
//! (that resurrects the zombie-swarm / "database is locked" problems the
//! `Weak` group map exists to prevent). Dead entries are pruned lazily on
//! lookup, so no `Drop` hook is needed.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, Weak};

use crate::connection::WaveSyncNodeInner;

/// Generic path-keyed weak registry. Factored out of the statics below so
/// the register/lookup/prune lifecycle is unit-testable without constructing
/// a real [`WaveSyncNodeInner`] (whose fields are private to `connection`).
pub(crate) struct PathRegistry<T> {
    map: Mutex<HashMap<PathBuf, Weak<T>>>,
}

impl<T> PathRegistry<T> {
    pub(crate) fn new() -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
        }
    }

    /// Insert or overwrite the entry for `key`. Overwriting is correct: a
    /// re-`build()` on the same file replaces the (dead or dying) previous
    /// node.
    fn register(&self, key: PathBuf, value: &Arc<T>) {
        self.map.lock().unwrap().insert(key, Arc::downgrade(value));
    }

    /// Upgrade the entry for `key`, pruning it if the value has died.
    fn live(&self, key: &PathBuf) -> Option<Arc<T>> {
        let mut map = self.map.lock().unwrap();
        match map.get(key).and_then(Weak::upgrade) {
            Some(v) => Some(v),
            None => {
                map.remove(key);
                None
            }
        }
    }
}

static NODES: LazyLock<PathRegistry<WaveSyncNodeInner>> = LazyLock::new(PathRegistry::new);

/// Per-DB async locks serializing background-sync wakes. iOS dispatches push
/// callbacks on a concurrent queue, so two pushes can overlap; without this,
/// each would take the cold path and build its own engine — the same
/// duplicate-identity hazard the node registry exists to prevent, just
/// between two cold engines. `tokio::sync::Mutex` is runtime-agnostic, so
/// the lock works across the per-wake runtimes the FFI layer creates.
static WAKE_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The SQLite file path named by a `sqlite:`/`sqlite://` URL, lexically:
/// prefix and query parameters stripped, no filesystem access. Shared with
/// `SyncConfig::config_path` so the registry and the config file always
/// agree on which file a URL means.
pub(crate) fn sqlite_lexical_path(database_url: &str) -> Option<PathBuf> {
    let path_str = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite:"))
        .unwrap_or(database_url);
    let path_str = path_str.split('?').next().unwrap_or(path_str);
    if path_str.is_empty() {
        return None;
    }
    Some(PathBuf::from(path_str))
}

/// Registry key for a database URL: the lexical path, canonicalized when the
/// file exists so two spellings of one file (relative vs absolute, `sqlite:`
/// vs `sqlite://`, symlinks) share an entry. Falls back to the lexical path
/// when canonicalization fails (file not created yet) — both the register
/// and lookup sites run after the file exists in practice, so the fallback
/// only has to be *consistent*, not perfect.
fn registry_key(database_url: &str) -> Option<PathBuf> {
    let lexical = sqlite_lexical_path(database_url)?;
    Some(std::fs::canonicalize(&lexical).unwrap_or(lexical))
}

/// Record a freshly built node under its database URL. Called by
/// `WaveSyncDbBuilder::build()`; entries self-clear via [`Weak`].
pub(crate) fn register(database_url: &str, node: &Arc<WaveSyncNodeInner>) {
    if let Some(key) = registry_key(database_url) {
        NODES.register(key, node);
    }
}

/// Look up the live node for a database URL, if one exists in this process.
pub(crate) fn live_node(database_url: &str) -> Option<Arc<WaveSyncNodeInner>> {
    NODES.live(&registry_key(database_url)?)
}

/// The per-DB wake serialization lock for a database URL. Always returns a
/// lock (URLs that don't name a file share one bucket keyed by the empty
/// path — such URLs never reach the background-sync path in practice).
pub(crate) fn wake_lock(database_url: &str) -> Arc<tokio::sync::Mutex<()>> {
    let key = registry_key(database_url).unwrap_or_default();
    WAKE_LOCKS.lock().unwrap().entry(key).or_default().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_path_strips_prefixes_and_query() {
        for url in [
            "sqlite:/data/app.db?mode=rwc",
            "sqlite:///data/app.db?mode=rwc&cache=private",
            "sqlite:/data/app.db",
            "/data/app.db",
        ] {
            assert_eq!(
                sqlite_lexical_path(url),
                Some(PathBuf::from("/data/app.db")),
                "url: {url}"
            );
        }
    }

    #[test]
    fn lexical_path_rejects_empty() {
        assert_eq!(sqlite_lexical_path("sqlite:"), None);
        assert_eq!(sqlite_lexical_path(""), None);
        assert_eq!(sqlite_lexical_path("sqlite:?mode=memory"), None);
    }

    #[test]
    fn registry_register_live_and_prune() {
        let reg: PathRegistry<u8> = PathRegistry::new();
        let key = PathBuf::from("/tmp/reg-test.db");
        let value = Arc::new(7u8);

        assert!(reg.live(&key).is_none(), "empty registry misses");
        reg.register(key.clone(), &value);
        let live = reg.live(&key).expect("registered value is live");
        assert!(Arc::ptr_eq(&live, &value), "lookup returns the same Arc");

        drop(live);
        drop(value);
        assert!(reg.live(&key).is_none(), "dropped value is gone");
        assert!(
            reg.map.lock().unwrap().is_empty(),
            "dead entry is pruned on lookup"
        );
    }

    #[test]
    fn registry_overwrites_on_reregister() {
        let reg: PathRegistry<u8> = PathRegistry::new();
        let key = PathBuf::from("/tmp/reg-overwrite.db");
        let first = Arc::new(1u8);
        let second = Arc::new(2u8);
        reg.register(key.clone(), &first);
        reg.register(key.clone(), &second);
        let live = reg.live(&key).unwrap();
        assert!(Arc::ptr_eq(&live, &second));
    }

    #[test]
    fn registry_key_unifies_url_spellings_of_one_file() {
        let dir = std::env::temp_dir().join(format!(
            "wavesync_node_registry_{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("app.db");
        std::fs::write(&file, b"").unwrap();

        let a = registry_key(&format!("sqlite:{}?mode=rwc", file.display())).unwrap();
        let b = registry_key(&format!("sqlite://{}", file.display())).unwrap();
        // A dot-segment spelling of the same file.
        let dotted = dir.join(".").join("app.db");
        let c = registry_key(&format!("sqlite:{}", dotted.display())).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, c, "canonicalization unifies dot segments");
    }

    #[test]
    fn wake_lock_is_shared_per_path_and_distinct_across_paths() {
        let l1 = wake_lock("sqlite:/tmp/wake-a.db?mode=rwc");
        let l2 = wake_lock("sqlite:/tmp/wake-a.db");
        let l3 = wake_lock("sqlite:/tmp/wake-b.db");
        assert!(Arc::ptr_eq(&l1, &l2), "same path shares one lock");
        assert!(!Arc::ptr_eq(&l1, &l3), "different paths get distinct locks");
    }
}
