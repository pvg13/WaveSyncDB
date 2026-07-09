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
//! values are `json_object()`-spelled on EVERY path — the capture triggers,
//! the catch-up JOIN, and the tiebreak read all share one blob-safe
//! expression — so a native boolean fingerprints as `0`/`1` and a REAL
//! `1.0` as `1.0`, identically on every native peer. The only remaining
//! mismatch source is a browser app hand-writing a different spelling for
//! the same logical value (`json!(true)` instead of `json!(1)`, `json!(1)`
//! for a REAL `1.0`). That shows up as a (self-repairing) digest mismatch.
//! Match SQLite's spelling on the browser side: booleans as `0`/`1`
//! numbers, no JSON nulls for absent values, and keep REAL columns REAL.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::conflict;
use crate::messages::{
    ColumnChange, ColumnName, DeletePolicy, NodeId, PrimaryKey, SyncChangeset, TableName,
};
use crate::protocol::RangeEntry;
use crate::reconcile;

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
    /// Deleter's wall-clock stamp (unix seconds) — `Some` only on
    /// `__deleted` rows. Same wire-carried value as native, so both
    /// engines age a given tombstone from the same instant. serde default
    /// keeps pre-retention IndexedDB rows loadable (they read as `None`
    /// and are backfilled at client init).
    #[serde(default)]
    pub deleted_ts: Option<u64>,
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
    /// Tombstone retention in seconds. `None` = the 7-day default
    /// (mirroring native's absent-meta-key semantics); `Some(0)` disables
    /// GC entirely. Must match the native peers' setting or the two sides
    /// age tombstones differently and digests drift.
    pub tombstone_retention_secs: Option<u64>,
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

    /// Disable tombstone garbage collection AND aging entirely: tombstones
    /// are kept (and synced) forever. Storage grows with all-time deletes.
    /// Mirrors the native builder's equivalent option; must match the
    /// native peers' setting or digests drift.
    pub fn without_tombstone_gc(mut self) -> Self {
        self.tombstone_retention_secs = Some(0);
        self
    }

    /// The retention exclusion cutoff for a given `now`: tombstones with
    /// `deleted_ts < cutoff` are treated as nonexistent on EVERY surface,
    /// identical to native's rule.
    pub fn tombstone_cutoff(&self, now_secs: u64) -> Option<u64> {
        match self.tombstone_retention_secs {
            Some(0) => None,
            Some(secs) => Some(now_secs.saturating_sub(secs)),
            None => {
                Some(now_secs.saturating_sub(crate::messages::DEFAULT_TOMBSTONE_RETENTION_SECS))
            }
        }
    }
}

/// True when a tombstone row is still live under `cutoff` (aged = absent).
fn tombstone_live(row_deleted_ts: Option<u64>, cutoff: Option<u64>) -> bool {
    match (row_deleted_ts, cutoff) {
        (Some(ts), Some(c)) => ts >= c,
        // No stamp (defensive) or GC disabled: never ages out.
        _ => true,
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

    /// Physically delete every `(table, pk, "__deleted")` entry whose
    /// `deleted_ts` is present and `< cutoff`, returning how many were
    /// reaped. Exclusion already hides these rows from every sync /
    /// reconcile / conflict surface, so this only reclaims storage.
    /// Entries with no `deleted_ts` stamp are kept (they never age).
    async fn gc_aged_tombstones(&self, cutoff: u64) -> Result<u64, StoreError>;
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

    async fn gc_aged_tombstones(&self, _cutoff: u64) -> Result<u64, StoreError> {
        Ok(0)
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
    cfg: &WebSyncConfig,
    changeset: &SyncChangeset,
    next_db_version: u64,
    peer_key: Option<&str>,
    now_secs: u64,
) -> Result<Vec<ColumnChange>, StoreError> {
    let cutoff = cfg.tombstone_cutoff(now_secs);
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

        // A row-level delete is resolved against the row as a whole and —
        // when present — runs INSTEAD of the row's column changes, exactly
        // like the native apply path (sync_handler.rs:1332-1347): a winning
        // delete supersedes sibling column changes in the same changeset, a
        // losing delete leaves the row untouched.
        if let Some(del) = row_changes.iter().find(|c| c.cid.0 == DELETED_COLUMN) {
            // An incoming tombstone already past the retention cutoff is
            // semantically nonexistent — same skip as native's
            // apply_remote_delete.
            if !tombstone_live(del.deleted_ts, cutoff) {
                continue;
            }
            let local_max_cv = local_entries
                .values()
                .map(|r| r.col_version)
                .max()
                .unwrap_or(0);
            if conflict::should_apply_delete(del.cl, local_max_cv, &cfg.delete_policy_for(table)) {
                // Clear every clock entry for the row, then write the
                // tombstone — mirrors native delete_clock_entries +
                // insert_tombstone (shadow.rs:613-673).
                batch.row_deletes.push((table.clone(), pk.clone()));
                batch.shadow_puts.push((
                    table.clone(),
                    pk.clone(),
                    DELETED_COLUMN.to_string(),
                    ShadowRow {
                        val: None,
                        site_id: del.site_id.0,
                        col_version: del.col_version,
                        cl: del.cl,
                        seq: 0,
                        db_version: next_db_version,
                        // The DELETER's stamp, stored verbatim so this
                        // replica ages the tombstone from the same instant
                        // as everyone else.
                        deleted_ts: Some(del.deleted_ts.unwrap_or(now_secs)),
                    },
                ));
                applied.push((*del).clone());
            }
            continue;
        }

        // N8: if this replica deleted the row, adjudicate the incoming column
        // edits against the local tombstone before applying them — mirrors native
        // apply_remote_column_changes. A delete that still dominates skips the row
        // (no resurrection); a delete that provably lost is cleared so the pair
        // reconverges. The web store keeps values, so a cleared tombstone makes
        // the surviving cells visible again with no residue drop needed.
        if let Some(tomb) = local_entries
            .get(DELETED_COLUMN)
            .filter(|t| tombstone_live(t.deleted_ts, cutoff))
        {
            let incoming_max = row_changes
                .iter()
                .filter(|c| c.cid.0 != DELETED_COLUMN)
                .map(|c| c.col_version)
                .max()
                .unwrap_or(0);
            if conflict::should_apply_delete(tomb.cl, incoming_max, &cfg.delete_policy_for(table)) {
                continue;
            }
            batch.tombstone_clears.push((table.clone(), pk.clone()));
        }

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
                    deleted_ts: change.deleted_ts,
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
// Mirrors the native drain's parameter surface; splitting a context
// struct for one function would obscure the native/web symmetry.
#[allow(clippy::too_many_arguments)]
pub async fn submit_local_write_core<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    site_id: &NodeId,
    table: &str,
    pk: &str,
    columns: Vec<(String, serde_json::Value)>,
    next_db_version: u64,
    now_secs: u64,
) -> Result<Vec<ColumnChange>, StoreError> {
    let cutoff = cfg.tombstone_cutoff(now_secs);
    let local_entries: HashMap<String, ShadowRow> = store
        .get_row_entries(table, pk)
        .await?
        .into_iter()
        .collect();

    let mut batch = WriteBatch {
        db_version: Some(next_db_version),
        ..Default::default()
    };
    // An insert/update on a tombstoned pk resurrects the row: clear the
    // tombstone but PRESERVE the per-column clock entries so col_versions
    // continue from their previous values — mirrors native clear_tombstone
    // (connection.rs:1039-1054).
    // Resurrection floor: if this row is reviving after a local delete, its cells
    // must outrank the tombstone (col_version >= cl + 1) or a DeleteWins peer that
    // still holds the tombstone would let its equal-cl delete win the tie and the
    // row would diverge. Mirrors the native floor in connection.rs. 0 = no
    // tombstone = normal write.
    // Aged tombstones are absent everywhere: they neither raise the
    // resurrection floor nor need clearing (physical GC will reap them).
    let live_tomb = local_entries
        .get(DELETED_COLUMN)
        .filter(|t| tombstone_live(t.deleted_ts, cutoff));
    let floor = live_tomb.map(|t| t.cl + 1).unwrap_or(0);
    if live_tomb.is_some() {
        batch
            .tombstone_clears
            .push((table.to_string(), pk.to_string()));
    }
    let mut changes: Vec<ColumnChange> = Vec::with_capacity(columns.len());

    for (seq, (cid, val)) in columns.into_iter().enumerate() {
        let prev = local_entries.get(&cid);
        let next_cv = prev.map(|r| r.col_version + 1).unwrap_or(1).max(floor);
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
                deleted_ts: None,
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
            deleted_ts: None,
        });
    }

    store.apply_batch(batch).await?;
    Ok(changes)
}

/// Record a local row deletion with native-parity tombstone semantics
/// (connection.rs:983-1037): the tombstone's `col_version` (== its causal
/// length `cl`) is one above the row's max `col_version`, so it beats every
/// prior column write. Per-column clock entries are left in place — only a
/// *receiving* peer clears them when the delete wins there.
///
/// Returns the single `__deleted` `ColumnChange` to fan out — only after
/// the batch committed.
pub async fn submit_local_delete_core<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    site_id: &NodeId,
    table: &str,
    pk: &str,
    next_db_version: u64,
    now_secs: u64,
) -> Result<Vec<ColumnChange>, StoreError> {
    let cutoff = cfg.tombstone_cutoff(now_secs);
    // An aged prior tombstone must not raise this delete's clock — a peer
    // that already collected it physically would compute a different
    // tombstone_cv for the same operation.
    let max_cv = store
        .get_row_entries(table, pk)
        .await?
        .into_iter()
        .filter(|(cid, r)| cid != DELETED_COLUMN || tombstone_live(r.deleted_ts, cutoff))
        .map(|(_, r)| r.col_version)
        .max()
        .unwrap_or(0);
    let tombstone_cv = max_cv + 1;

    let batch = WriteBatch {
        db_version: Some(next_db_version),
        shadow_puts: vec![(
            table.to_string(),
            pk.to_string(),
            DELETED_COLUMN.to_string(),
            ShadowRow {
                val: None,
                site_id: site_id.0,
                col_version: tombstone_cv,
                cl: tombstone_cv,
                seq: 0,
                db_version: next_db_version,
                deleted_ts: Some(now_secs),
            },
        )],
        ..Default::default()
    };
    store.apply_batch(batch).await?;

    Ok(vec![ColumnChange {
        table: TableName(table.to_string()),
        pk: PrimaryKey(pk.to_string()),
        cid: ColumnName(DELETED_COLUMN.to_string()),
        val: None,
        site_id: *site_id,
        col_version: tombstone_cv,
        cl: tombstone_cv,
        seq: 0,
        db_version: next_db_version,
        deleted_ts: Some(now_secs),
    }])
}

/// The sync-visible changes of this store since `since` — the browser
/// counterpart of native `shadow::get_changes_since`.
///
/// Native LEFT JOINs the user table, so a deleted row's stale per-column
/// clock entries produce no value and are skipped (shadow.rs); only the
/// tombstone flows. The web store has no user table, so the equivalent
/// rule is applied here: a pk with a `__deleted` entry exposes ONLY its
/// tombstone. The stale cells stay in the store (clock continuity for
/// resurrection — same reason native keeps them) but never reach catch-up
/// responses or digests. Without this filter, web digests and changesets
/// would include cells no native peer ever produces.
pub async fn changes_since_core<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    since: u64,
    now_secs: u64,
) -> Result<Vec<ColumnChange>, StoreError> {
    let cutoff = cfg.tombstone_cutoff(now_secs);
    // Full scan, then window-filter: the tombstone that hides a cell may
    // live outside the `since` window, so visibility must be computed over
    // the whole store. (The underlying store scan is full-table anyway.)
    let all = store.get_changes_since(0).await?;
    // Only LIVE tombstones hide their row's cells or travel themselves;
    // aged ones are absent from every surface (native parity — the same
    // predicate guards shadow::get_changes_since).
    let tombstoned: std::collections::HashSet<(&str, &str)> = all
        .iter()
        .filter(|c| c.cid.0 == DELETED_COLUMN && tombstone_live(c.deleted_ts, cutoff))
        .map(|c| (c.table.0.as_str(), c.pk.0.as_str()))
        .collect();
    Ok(all
        .iter()
        .filter(|c| {
            if c.db_version <= since {
                return false;
            }
            if c.cid.0 == DELETED_COLUMN {
                return tombstone_live(c.deleted_ts, cutoff);
            }
            !tombstoned.contains(&(c.table.0.as_str(), c.pk.0.as_str()))
        })
        .cloned()
        .collect())
}

/// Reap aged tombstones per `config`'s retention window. `Ok(0)` and no
/// scan when GC is disabled (`without_tombstone_gc`). Best-effort caller
/// contract: failures must never block startup or sync.
pub async fn gc_aged_tombstones_core<S: ShadowStore>(
    config: &WebSyncConfig,
    store: &S,
    now_secs: u64,
) -> Result<u64, StoreError> {
    match config.tombstone_cutoff(now_secs) {
        None => Ok(0),
        Some(cutoff) => store.gc_aged_tombstones(cutoff).await,
    }
}

// ── reconciliation (#82) ───────────────────────────────────────────────────

/// Enumerate this store's reconciliation cells, sorted by key, with each
/// table's PK column excluded per [`WebSyncConfig::pk_column_of`] —
/// the browser counterpart of the native `enumerate_sorted_cells`
/// (`engine/reconcile.rs`), built on the SAME shared fingerprint code, so
/// equal logical state produces an equal digest across targets.
pub async fn enumerate_store_cells<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    now_secs: u64,
) -> Result<Vec<reconcile::LocalCell>, StoreError> {
    let changes = changes_since_core(store, cfg, 0, now_secs).await?;
    Ok(reconcile::sorted_cells_from_changes(changes, |t| {
        cfg.pk_column_of(t)
    }))
}

/// The value-inclusive convergence digest of this store (XOR of every cell
/// fingerprint). Equal to a native peer's `compute_group_digest` iff the
/// two replicas hold identical data — THE convergence proof for the
/// web↔native path.
pub async fn compute_store_digest<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    now_secs: u64,
) -> Result<[u8; 32], StoreError> {
    Ok(reconcile::range_fp(
        &enumerate_store_cells(store, cfg, now_secs).await?,
    ))
}

/// Process one incoming `ReconcileRange` message against this store's
/// cells via the shared [`reconcile::reconcile_step`].
///
/// Returns `(reply_entries, changes_to_apply)`. The caller applies
/// `changes_to_apply` through the normal conflict-resolving apply path
/// (do NOT advance any peer catch-up cursor for them — reconcile
/// transfers carry the originator's clocks, not the peer's `db_version`).
pub async fn reconcile_range_step<S: ShadowStore>(
    store: &S,
    cfg: &WebSyncConfig,
    entries: &[RangeEntry],
    now_secs: u64,
) -> Result<(Vec<RangeEntry>, Vec<ColumnChange>), StoreError> {
    let cells = enumerate_store_cells(store, cfg, now_secs).await?;
    Ok(reconcile::reconcile_step(&cells, entries))
}
