//! Target-independent core of the browser sync engine.
//!
//! The browser engine (`web_engine.rs`) is `wasm32`-only — it depends on
//! IndexedDB, `wasm_bindgen_futures`, and the WebSocket transport. The
//! convergence-critical logic, however, must behave *identically* to the
//! native engine or a browser replica silently diverges from its native
//! peers. That logic lives here, written against the small [`ShadowStore`]
//! trait, so that:
//!
//! - on wasm32 it runs over [`crate::web_store::BrowserStore`] (IndexedDB),
//! - in native `cargo test` it runs over an in-memory store, where the
//!   delete-policy matrix and digest parity with the native engine are
//!   asserted directly (`tests/web_core.rs`, `tests/web_native_convergence.rs`).
//!
//! ## Invariants mirrored from the native engine
//!
//! - **One atomic batch per logical write.** A local write or an incoming
//!   changeset produces exactly one [`WriteBatch`] — `db_version`, every
//!   winning shadow row, row deletions, and the peer cursor commit together
//!   or not at all. A store error is **fail-closed**: nothing is persisted,
//!   nothing is broadcast, the peer cursor does not advance, so the data is
//!   re-requested on the next catch-up instead of silently lost.
//! - **Notifications only after commit**: the apply functions *return* the
//!   applied changes; the caller broadcasts them only on `Ok` (the shadow
//!   state subscribers re-query is already durable).
//! - **Per-column conflict resolution** via [`conflict::should_apply_column`]
//!   — identical inputs to the native `sync_handler` path.
//!
//! ## Value-byte parity caveat
//!
//! Reconciliation fingerprints feed `serde_json::to_vec(&val)`. Native
//! values round-trip through SQLite (`json_object()`), so a REAL `1.0`
//! fingerprints as `1.0`; a browser app that wrote `json!(1)` for the same
//! logical value fingerprints as `1`. Identical *logical* numbers with
//! different JSON spellings show up as a (self-repairing) digest mismatch.
//! Stick to strings / integers / booleans for synced columns, or normalize
//! numbers on the app side.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::conflict;
use crate::messages::{
    ColumnChange, ColumnName, DeletePolicy, NodeId, PrimaryKey, SyncChangeset, TableName,
};

/// Sentinel column id marking a row tombstone — same constant as native.
pub const DELETED_COLUMN: &str = "__deleted";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("indexeddb error: {0}")]
    Idb(String),
    #[error("serde error: {0}")]
    Serde(String),
}

/// One persisted shadow-table row — the per-(table, pk, cid) Lamport state.
///
/// The shape mirrors `shadow::ShadowRow` on native, except `val` is JSON
/// (browser doesn't have SQLite blob types) and field types use plain
/// integers for IndexedDB-friendly serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowRow {
    pub val: Option<serde_json::Value>,
    pub site_id: [u8; 16],
    pub col_version: u64,
    pub cl: u64,
    pub seq: u32,
    pub db_version: u64,
}

/// Per-table sync configuration for the browser engine.
///
/// The native engine reads this from the `TableRegistry` built by
/// `#[derive(SyncEntity)]`; the browser engine has no registry, so consuming
/// apps configure it explicitly at connect time. Tables without an entry get
/// the defaults (`DeletePolicy::DeleteWins`, no PK column known).
#[derive(Debug, Clone, Default)]
pub struct WebTableConfig {
    /// Resolves delete vs. concurrent-edit conflicts. Must match the policy
    /// the native peers registered for this table, or the two sides resolve
    /// the same conflict differently and diverge.
    pub delete_policy: DeletePolicy,
    /// The primary-key column name. Used to exclude the PK column's clock
    /// cell from reconciliation digests, matching the native registry filter
    /// (the PK is already implicit in every cell's key).
    pub primary_key_column: Option<String>,
}

/// Engine-wide configuration: per-table policies keyed by table name.
#[derive(Debug, Clone, Default)]
pub struct WebSyncConfig {
    pub tables: HashMap<String, WebTableConfig>,
}

impl WebSyncConfig {
    /// Builder-style table registration.
    pub fn with_table(mut self, table: impl Into<String>, cfg: WebTableConfig) -> Self {
        self.tables.insert(table.into(), cfg);
        self
    }

    /// Delete policy for `table` (default `DeleteWins`, matching native).
    pub fn delete_policy_for(&self, table: &str) -> DeletePolicy {
        self.tables
            .get(table)
            .map(|t| t.delete_policy.clone())
            .unwrap_or_default()
    }

    /// PK column for `table`, if configured.
    pub fn pk_column_of(&self, table: &str) -> Option<String> {
        self.tables
            .get(table)
            .and_then(|t| t.primary_key_column.clone())
    }
}

/// One logical write's worth of store mutations. Implementors MUST apply the
/// whole batch atomically — all of it commits or none of it does. This is
/// what closes the "db_version advanced past missing shadow rows" window.
#[derive(Debug, Default)]
pub struct WriteBatch {
    /// New engine `db_version` to persist (meta store).
    pub db_version: Option<u64>,
    /// Shadow upserts: `(table, pk, cid, row)`.
    pub shadow_puts: Vec<(String, String, String, ShadowRow)>,
    /// Delete EVERY shadow entry (all cids) of `(table, pk)` — a winning
    /// remote delete clears the row's clock entries before the tombstone in
    /// `shadow_puts` lands (native: `delete_clock_entries` + `insert_tombstone`).
    pub row_deletes: Vec<(String, String)>,
    /// Delete only the `(table, pk, "__deleted")` entry — a local
    /// insert/update on a tombstoned pk resurrects the row (native:
    /// `clear_tombstone`).
    pub tombstone_clears: Vec<(String, String)>,
    /// Persist the per-peer cursor `(peer_key, last_db_version)`.
    pub peer_version: Option<(String, u64)>,
}

impl WriteBatch {
    pub fn is_empty(&self) -> bool {
        self.db_version.is_none()
            && self.shadow_puts.is_empty()
            && self.row_deletes.is_empty()
            && self.tombstone_clears.is_empty()
            && self.peer_version.is_none()
    }
}

/// The store surface the sync core needs. `BrowserStore` (IndexedDB)
/// implements this on wasm32; tests implement it in memory on native.
///
/// Futures are deliberately not `Send` — wasm futures never are, and the
/// whole engine runs on the single-threaded browser event loop.
#[allow(async_fn_in_trait)]
pub trait ShadowStore {
    /// Current shadow entry for `(table, pk, cid)`, if any.
    async fn get_shadow(
        &self,
        table: &str,
        pk: &str,
        cid: &str,
    ) -> Result<Option<ShadowRow>, StoreError>;

    /// Every shadow entry for `(table, pk)` as `(cid, row)` pairs — the
    /// native `get_clock_entries_for_row` equivalent, needed to compute the
    /// row's max `col_version` for delete resolution.
    async fn get_row_entries(
        &self,
        table: &str,
        pk: &str,
    ) -> Result<Vec<(String, ShadowRow)>, StoreError>;

    /// Every shadow entry with `db_version > since`, sorted by
    /// `(db_version, seq)` — drives catch-up responses and digest
    /// enumeration.
    async fn get_changes_since(&self, since: u64) -> Result<Vec<ColumnChange>, StoreError>;

    /// Apply `batch` atomically (see [`WriteBatch`]).
    async fn apply_batch(&self, batch: WriteBatch) -> Result<(), StoreError>;
}

/// No-op store for **ephemeral** clients (no IndexedDB). Conflict
/// resolution sees an empty local state, so every incoming change applies
/// and is surfaced to subscribers; nothing survives a reload — exactly the
/// pre-existing ephemeral semantics.
pub struct EphemeralStore;

impl ShadowStore for EphemeralStore {
    async fn get_shadow(
        &self,
        _table: &str,
        _pk: &str,
        _cid: &str,
    ) -> Result<Option<ShadowRow>, StoreError> {
        Ok(None)
    }

    async fn get_row_entries(
        &self,
        _table: &str,
        _pk: &str,
    ) -> Result<Vec<(String, ShadowRow)>, StoreError> {
        Ok(Vec::new())
    }

    async fn get_changes_since(&self, _since: u64) -> Result<Vec<ColumnChange>, StoreError> {
        Ok(Vec::new())
    }

    async fn apply_batch(&self, _batch: WriteBatch) -> Result<(), StoreError> {
        Ok(())
    }
}

/// Apply an incoming changeset with native-parity conflict resolution.
///
/// `next_db_version` is the already-incremented local Lamport value; the
/// caller holds the counter and must roll it back if this returns `Err`
/// (nothing was persisted). `peer_key`, when present, advances that peer's
/// catch-up cursor to `changeset.db_version` *in the same atomic batch* as
/// the data — never before the data is durable.
///
/// Returns the changes that actually applied (winners), for the caller to
/// broadcast — strictly after the batch committed.
pub async fn apply_remote_changeset_core<S: ShadowStore>(
    store: &S,
    _cfg: &WebSyncConfig,
    changeset: &SyncChangeset,
    next_db_version: u64,
    peer_key: Option<&str>,
) -> Result<Vec<ColumnChange>, StoreError> {
    let mut batch = WriteBatch {
        db_version: Some(next_db_version),
        peer_version: peer_key.map(|p| (p.to_string(), changeset.db_version)),
        ..Default::default()
    };
    let mut applied: Vec<ColumnChange> = Vec::new();

    // Group changes by (table, pk), preserving first-seen row order, so a
    // row's delete (if any) can be resolved against the row as a whole.
    let mut row_order: Vec<(String, String)> = Vec::new();
    let mut rows: HashMap<(String, String), Vec<&ColumnChange>> = HashMap::new();
    for change in &changeset.changes {
        let key = (change.table.0.clone(), change.pk.0.clone());
        rows.entry(key.clone())
            .or_insert_with(|| {
                row_order.push(key.clone());
                Vec::new()
            })
            .push(change);
    }

    for key in &row_order {
        let (table, pk) = (&key.0, &key.1);
        let row_changes = &rows[key];

        // Read the row's current clock entries once.
        let local_entries: HashMap<String, ShadowRow> = store
            .get_row_entries(table, pk)
            .await?
            .into_iter()
            .collect();

        for change in row_changes {
            let remote_val = match &change.val {
                Some(v) => serde_json::to_vec(v).unwrap_or_default(),
                None => Vec::new(),
            };
            let local = local_entries.get(&change.cid.0);
            let (local_cv, local_val_bytes, local_site) = match local {
                Some(r) => {
                    let bytes = r
                        .val
                        .as_ref()
                        .map(|v| serde_json::to_vec(v).unwrap_or_default())
                        .unwrap_or_default();
                    (r.col_version, bytes, NodeId(r.site_id))
                }
                None => (0, Vec::new(), NodeId([0u8; 16])),
            };

            if !conflict::should_apply_column(
                change.col_version,
                &remote_val,
                &change.site_id,
                local_cv,
                &local_val_bytes,
                &local_site,
            ) {
                continue;
            }

            batch.shadow_puts.push((
                table.clone(),
                pk.clone(),
                change.cid.0.clone(),
                ShadowRow {
                    val: change.val.clone(),
                    site_id: change.site_id.0,
                    col_version: change.col_version,
                    cl: change.cl,
                    seq: change.seq,
                    db_version: next_db_version,
                },
            ));
            applied.push((*change).clone());
        }
    }

    // Single atomic commit — fail-closed on error.
    store.apply_batch(batch).await?;
    Ok(applied)
}

/// Record a local insert/update batch with native-parity clock semantics.
///
/// Each column's `col_version` is the previous entry's + 1 (or 1), `cl`
/// carries the row's causal length forward. All columns plus `db_version`
/// commit in one atomic batch. Returns the `ColumnChange`s to fan out —
/// only after the batch committed.
pub async fn submit_local_write_core<S: ShadowStore>(
    store: &S,
    site_id: &NodeId,
    table: &str,
    pk: &str,
    columns: Vec<(String, serde_json::Value)>,
    next_db_version: u64,
) -> Result<Vec<ColumnChange>, StoreError> {
    let local_entries: HashMap<String, ShadowRow> = store
        .get_row_entries(table, pk)
        .await?
        .into_iter()
        .collect();

    let mut batch = WriteBatch {
        db_version: Some(next_db_version),
        ..Default::default()
    };
    let mut changes: Vec<ColumnChange> = Vec::with_capacity(columns.len());

    for (seq, (cid, val)) in columns.into_iter().enumerate() {
        let prev = local_entries.get(&cid);
        let next_cv = prev.map(|r| r.col_version + 1).unwrap_or(1);
        let next_cl = prev.map(|r| r.cl.max(next_cv)).unwrap_or(next_cv);

        batch.shadow_puts.push((
            table.to_string(),
            pk.to_string(),
            cid.clone(),
            ShadowRow {
                val: Some(val.clone()),
                site_id: site_id.0,
                col_version: next_cv,
                cl: next_cl,
                seq: seq as u32,
                db_version: next_db_version,
            },
        ));
        changes.push(ColumnChange {
            table: TableName(table.to_string()),
            pk: PrimaryKey(pk.to_string()),
            cid: ColumnName(cid),
            val: Some(val),
            site_id: *site_id,
            col_version: next_cv,
            cl: next_cl,
            seq: seq as u32,
            db_version: next_db_version,
        });
    }

    store.apply_batch(batch).await?;
    Ok(changes)
}
