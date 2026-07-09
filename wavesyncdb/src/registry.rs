//! Table registry for tracking which tables participate in sync.
//!
//! The [`TableRegistry`] is the central lookup used by the P2P engine to determine
//! whether a write should be replicated. Tables are registered either manually via
//! [`WaveSyncDb::register_table()`](crate::WaveSyncDb::register_table) or
//! automatically via [`SchemaBuilder::sync()`](crate::SchemaBuilder::sync).

use std::collections::HashMap;
use std::sync::RwLock;

#[cfg(not(target_arch = "wasm32"))]
use sea_orm::DatabaseBackend;

use crate::messages::DeletePolicy;

/// Metadata about a synced table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// The SQL table name (e.g., `"tasks"`).
    pub table_name: String,
    /// Name of the primary key column (e.g., `"id"`).
    pub primary_key_column: String,
    /// All column names in the table.
    pub columns: Vec<String>,
    /// How to resolve delete vs. non-delete conflicts for this table.
    pub delete_policy: DeletePolicy,
}

/// Metadata submitted by `#[derive(SyncEntity)]` at link time.
///
/// Each entity annotated with `SyncEntity` contributes one of these to the
/// global [`inventory`] collection. [`WaveSyncDb::get_schema_registry`](crate::WaveSyncDb::get_schema_registry)
/// iterates them to auto-discover entities whose `module_path` matches the
/// given prefix.
///
/// Native-only: the `schema_fn` signature is typed against `sea_orm::DatabaseBackend`,
/// and `sea-orm` is not available on wasm32 builds. Browser apps register
/// table metadata directly via [`TableRegistry::register`]. See
/// [`SyncEntityDescriptor`] for the target-independent sibling that both
/// engines can read.
#[cfg(not(target_arch = "wasm32"))]
pub struct SyncEntityInfo {
    /// The `module_path!()` of the entity, used for prefix matching.
    pub module_path: &'static str,
    /// Function that generates the CREATE TABLE SQL and [`TableMeta`] for a given backend.
    pub schema_fn: fn(DatabaseBackend) -> (String, TableMeta),
    /// Which sync groups this entity replicates in. Declared on the type via
    /// `#[wavesync(scope = ...)]`; defaults to [`EntityScope::Private`].
    pub scope: EntityScope,
}

/// The set of sync groups a `#[derive(SyncEntity)]` model replicates in.
///
/// Declared on the type via `#[wavesync(scope = ...)]`. Scope governs *which
/// groups' databases the entity's table is created in and registered for sync* —
/// it is a registration-time policy, not a per-message filter. Because each
/// group is a separate SQLite database with its own [`TableRegistry`], a
/// `Private` entity simply never gets a table in another group's DB, so it
/// cannot sync there.
///
/// All variants are `const`-constructible so the derive macro can emit them in
/// the static [`SyncEntityInfo`] inventory record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityScope {
    /// Default. Only the node's *default* group (the user's own-devices replica,
    /// created by [`WaveSyncDbBuilder::build`](crate::WaveSyncDbBuilder::build)).
    /// Personal data stays private unless explicitly opted into sharing.
    Private,
    /// Every joined group, including the default one.
    All,
    /// Only groups joined with a *kind* label in this list (see
    /// [`WaveSyncNode::join_group`](crate::WaveSyncNode::join_group)'s `kind`).
    /// The kind is a stable label decoupled from the (often dynamic) topic id.
    Groups(&'static [&'static str]),
}

impl EntityScope {
    /// Whether an entity with this scope should be created+registered for a group.
    ///
    /// * `is_default` — is this the node's default group?
    /// * `kind` — the group's kind label (`None` for the default group or any
    ///   group joined without one).
    pub fn matches(&self, is_default: bool, kind: Option<&str>) -> bool {
        match self {
            EntityScope::Private => is_default,
            EntityScope::All => true,
            EntityScope::Groups(kinds) => kind.is_some_and(|k| kinds.contains(&k)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
inventory::collect!(SyncEntityInfo);

/// Target-independent sibling of [`SyncEntityInfo`].
///
/// `SyncEntityInfo` carries a `schema_fn` typed against `sea_orm::DatabaseBackend`,
/// which doesn't exist on wasm32 (no `sea-orm` there). A browser entity still needs
/// to announce its table name, primary key, delete policy, and scope so *both*
/// engines (native P2P and the browser's `web_engine`) can discover it the same
/// way — via `inventory` at link time, no manual registration call required. This
/// struct is that shared, schema-generation-free metadata: everything an entity
/// can describe about itself without depending on a SeaORM connection.
#[derive(Debug, Clone)]
pub struct SyncEntityDescriptor {
    /// The SQL table name (e.g., `"tasks"`).
    pub table_name: &'static str,
    /// Name of the primary key column (e.g., `"id"`).
    pub pk_column: &'static str,
    /// How to resolve delete vs. non-delete conflicts for this table.
    pub delete_policy: DeletePolicy,
    /// Which sync groups this entity replicates in.
    pub scope: EntityScope,
    /// The `module_path!()` of the entity, used for prefix matching.
    pub module_path: &'static str,
}

inventory::collect!(SyncEntityDescriptor);

/// Registry of tables that participate in sync.
///
/// Thread-safe via interior `RwLock`. Shared between the connection wrapper
/// (which checks registration before dispatching sync) and the P2P engine.
#[derive(Debug, Default)]
pub struct TableRegistry {
    tables: RwLock<HashMap<String, TableMeta>>,
}

impl TableRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Register a table for sync. Replaces any existing entry with the same name.
    pub fn register(&self, meta: TableMeta) {
        let name = meta.table_name.clone();
        self.tables.write().unwrap().insert(name, meta);
    }

    /// Look up metadata for a table by name.
    pub fn get(&self, table_name: &str) -> Option<TableMeta> {
        self.tables.read().unwrap().get(table_name).cloned()
    }

    /// Return metadata for all registered tables.
    pub fn all_tables(&self) -> Vec<TableMeta> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    /// Check whether a table is registered for sync.
    pub fn is_registered(&self, table_name: &str) -> bool {
        self.tables.read().unwrap().contains_key(table_name)
    }
}

/// Registry of per-table user-notification policies, plus the anti-spam gate.
///
/// Each `#[derive(SyncNotify)]` entity contributes a type-erased dispatch
/// closure (keyed by table name) that reconstructs the typed row and calls the
/// entity's `on_sync` policy. Stored separately from [`TableRegistry`] because
/// the closures are not `Clone`/`Debug`. Native-only — the dispatch runs in the
/// engine's remote-apply path.
#[cfg(not(target_arch = "wasm32"))]
pub struct NotificationRegistry {
    dispatch: RwLock<HashMap<String, crate::notify::NotifyDispatch>>,
    gate: NotificationGate,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NotificationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.dispatch.read().map(|d| d.len()).unwrap_or(0);
        f.debug_struct("NotificationRegistry")
            .field("policies", &n)
            .finish()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Default for NotificationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NotificationRegistry {
    /// Create an empty registry with the default coalescing interval (~2s).
    pub fn new() -> Self {
        Self {
            dispatch: RwLock::new(HashMap::new()),
            gate: NotificationGate::new(std::time::Duration::from_secs(2)),
        }
    }

    /// Register a per-table dispatch closure. Replaces any existing entry.
    pub fn register(&self, table_name: String, dispatch: crate::notify::NotifyDispatch) {
        self.dispatch.write().unwrap().insert(table_name, dispatch);
    }

    /// Whether any notification policy is registered (lets the engine skip work).
    pub fn is_empty(&self) -> bool {
        self.dispatch.read().unwrap().is_empty()
    }

    /// Whether a notification policy is registered for `table`. Lets the apply
    /// path skip the extra full-row read for tables that never notify.
    pub fn has(&self, table: &str) -> bool {
        self.dispatch.read().unwrap().contains_key(table)
    }

    /// Run the policy for this change (if the table has one), then apply the
    /// anti-spam gate. Returns the notification to surface, or `None` if the
    /// policy declined or the change was coalesced away.
    pub fn dispatch(
        &self,
        change: &crate::messages::ChangeNotification,
    ) -> Option<crate::notify::Notification> {
        let notif = {
            let map = self.dispatch.read().unwrap();
            let policy = map.get(&change.table.0)?;
            policy(change)?
        };
        let key = notif
            .coalesce_key
            .clone()
            .unwrap_or_else(|| format!("{}:{}", notif.table, notif.primary_key));
        if self.gate.allow(&key) {
            Some(notif)
        } else {
            None
        }
    }
}

/// Coalescing gate: drops a notification whose coalescing key was emitted within
/// `min_interval`. This collapses bursts — a catch-up sync that applies many rows
/// to the same conversation produces one notification, not dozens. Exact
/// re-delivery of an already-applied change is separately suppressed upstream:
/// the engine only emits a `ChangeNotification` when a change actually altered
/// local state (CRDT conflict resolution), so identical re-applies never reach
/// here.
#[cfg(not(target_arch = "wasm32"))]
struct NotificationGate {
    last_emit: std::sync::Mutex<HashMap<String, std::time::Instant>>,
    min_interval: std::time::Duration,
    /// Bound on retained keys; oldest are pruned when exceeded.
    max_entries: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl NotificationGate {
    fn new(min_interval: std::time::Duration) -> Self {
        Self {
            last_emit: std::sync::Mutex::new(HashMap::new()),
            min_interval,
            max_entries: 4096,
        }
    }

    /// Returns `true` if a notification for `key` may be emitted now, recording
    /// the emission. Returns `false` if one was emitted within `min_interval`.
    fn allow(&self, key: &str) -> bool {
        let now = std::time::Instant::now();
        let mut map = self.last_emit.lock().unwrap();
        if let Some(&last) = map.get(key)
            && now.duration_since(last) < self.min_interval
        {
            return false;
        }
        if map.len() >= self.max_entries {
            // Cheap bound: drop entries older than the interval; if that frees
            // nothing (all recent), clear to avoid unbounded growth.
            let cutoff = self.min_interval;
            map.retain(|_, &mut t| now.duration_since(t) < cutoff);
            if map.len() >= self.max_entries {
                map.clear();
            }
        }
        map.insert(key.to_string(), now);
        true
    }
}

// `SyncEntityInfo`/`NotificationGate` (and the notification-gate test below)
// are native-only (see the `not(wasm32)` gates above) — `wasm-pack test`
// always runs `cargo build --tests`, which builds this crate's own lib unit
// tests alongside whatever integration test was named, so this module must
// not compile under a wasm32 `cfg(test)` build or it fails to resolve the
// gated-out types.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::messages::DeletePolicy;

    inventory::submit! {
        crate::SyncEntityDescriptor {
            table_name: "reg_test_desc",
            pk_column: "id",
            delete_policy: crate::DeletePolicy::DeleteWins,
            scope: crate::EntityScope::All,
            module_path: module_path!(),
        }
    }

    #[test]
    fn descriptor_inventory_collects() {
        let found = inventory::iter::<crate::SyncEntityDescriptor>()
            .any(|d| d.table_name == "reg_test_desc");
        assert!(found);
    }

    #[test]
    fn entity_scope_private_matches_only_default() {
        assert!(EntityScope::Private.matches(true, None));
        assert!(EntityScope::Private.matches(true, Some("household")));
        assert!(!EntityScope::Private.matches(false, None));
        assert!(!EntityScope::Private.matches(false, Some("household")));
    }

    #[test]
    fn entity_scope_all_matches_everything() {
        assert!(EntityScope::All.matches(true, None));
        assert!(EntityScope::All.matches(false, None));
        assert!(EntityScope::All.matches(false, Some("household")));
    }

    #[test]
    fn entity_scope_groups_matches_by_kind() {
        let s = EntityScope::Groups(&["household", "team"]);
        assert!(s.matches(false, Some("household")));
        assert!(s.matches(false, Some("team")));
        // wrong kind, no kind, and default-without-kind all fail
        assert!(!s.matches(false, Some("other")));
        assert!(!s.matches(false, None));
        assert!(!s.matches(true, None));
    }

    fn make_meta(name: &str, pk: &str, cols: &[&str]) -> TableMeta {
        TableMeta {
            table_name: name.to_string(),
            primary_key_column: pk.to_string(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
            delete_policy: DeletePolicy::default(),
        }
    }

    #[test]
    fn test_new_creates_empty() {
        let registry = TableRegistry::new();
        assert!(registry.all_tables().is_empty());
    }

    #[test]
    fn test_register_and_get() {
        let registry = TableRegistry::new();
        registry.register(make_meta("tasks", "id", &["id", "title", "done"]));

        let meta = registry.get("tasks").expect("should find registered table");
        assert_eq!(meta.table_name, "tasks");
        assert_eq!(meta.primary_key_column, "id");
        assert_eq!(meta.columns, vec!["id", "title", "done"]);
    }

    #[test]
    fn test_get_missing_returns_none() {
        let registry = TableRegistry::new();
        assert!(registry.get("nonexistent").is_none());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn test_notification_gate_coalesces_within_interval() {
        // Long interval so timing isn't flaky: the second emit for the same key
        // is always within the window.
        let gate = NotificationGate::new(std::time::Duration::from_secs(3600));
        assert!(gate.allow("chat:1"), "first emit for a key is allowed");
        assert!(!gate.allow("chat:1"), "repeat within interval is coalesced");
        assert!(gate.allow("chat:2"), "a different key is independent");
        assert!(!gate.allow("chat:2"), "and is then coalesced too");
    }

    #[test]
    fn test_is_registered() {
        let registry = TableRegistry::new();
        registry.register(make_meta("users", "id", &["id", "name"]));

        assert!(registry.is_registered("users"));
        assert!(!registry.is_registered("missing"));
    }

    #[test]
    fn test_all_tables() {
        let registry = TableRegistry::new();
        registry.register(make_meta("tasks", "id", &["id", "title"]));
        registry.register(make_meta("users", "user_id", &["user_id", "name"]));
        registry.register(make_meta("notes", "note_id", &["note_id", "body"]));

        let all = registry.all_tables();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_register_replaces() {
        let registry = TableRegistry::new();
        registry.register(make_meta("tasks", "id", &["id", "title"]));
        registry.register(make_meta("tasks", "task_id", &["task_id", "title", "done"]));

        let meta = registry.get("tasks").expect("should find table");
        assert_eq!(meta.primary_key_column, "task_id");
        assert_eq!(meta.columns, vec!["task_id", "title", "done"]);
    }
}
