//! Table registry for tracking which tables participate in sync.
//!
//! The [`TableRegistry`] is the central lookup used by the P2P engine to determine
//! whether a write should be replicated. Tables are registered either manually via
//! [`WaveSyncDb::register_table()`](crate::WaveSyncDb::register_table) or
//! automatically via [`SchemaBuilder::sync()`](crate::SchemaBuilder::sync).

use std::collections::HashMap;
use std::sync::RwLock;

use sea_orm::DatabaseBackend;

/// Metadata about a synced table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// The SQL table name (e.g., `"tasks"`).
    pub table_name: String,
    /// Name of the primary key column (e.g., `"id"`).
    pub primary_key_column: String,
    /// All column names in the table.
    pub columns: Vec<String>,
}

/// Metadata submitted by `#[derive(SyncEntity)]` at link time.
///
/// Each entity annotated with `SyncEntity` contributes one of these to the
/// global [`inventory`] collection. [`WaveSyncDb::get_schema_registry`](crate::WaveSyncDb::get_schema_registry)
/// iterates them to auto-discover entities whose `module_path` matches the
/// given prefix.
pub struct SyncEntityInfo {
    /// The `module_path!()` of the entity, used for prefix matching.
    pub module_path: &'static str,
    /// Function that generates the CREATE TABLE SQL and [`TableMeta`] for a given backend.
    pub schema_fn: fn(DatabaseBackend) -> (String, TableMeta),
}

inventory::collect!(SyncEntityInfo);

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
