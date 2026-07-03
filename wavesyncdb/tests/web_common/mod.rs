//! In-memory `ShadowStore` for testing the browser sync core on native.
//!
//! Mirrors the `BrowserStore` (IndexedDB) semantics: shadow entries keyed by
//! `(table, pk, cid)`, a `db_version` meta singleton, per-peer cursors — and,
//! critically, `apply_batch` is genuinely atomic (all-or-nothing), with an
//! injectable one-shot failure to assert the engine's fail-closed behavior.
#![cfg(feature = "web")]

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use wavesyncdb::messages::{ColumnChange, ColumnName, NodeId, PrimaryKey, TableName};
use wavesyncdb::web_sync_core::{ShadowRow, ShadowStore, StoreError, WriteBatch};

#[derive(Default)]
struct Inner {
    db_version: u64,
    // BTreeMap so scans are deterministic.
    shadow: BTreeMap<(String, String, String), ShadowRow>,
    peer_versions: HashMap<String, u64>,
    fail_next_batch: bool,
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

// Each test binary compiles this module independently, so helpers used by
// one suite look dead in another.
#[allow(dead_code)]
impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make the next `apply_batch` fail WITHOUT applying anything — models
    /// an IndexedDB transaction abort (quota, I/O error).
    pub fn fail_next_batch(&self) {
        self.inner.lock().unwrap().fail_next_batch = true;
    }

    pub fn db_version(&self) -> u64 {
        self.inner.lock().unwrap().db_version
    }

    pub fn peer_version(&self, peer: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .peer_versions
            .get(peer)
            .copied()
            .unwrap_or(0)
    }

    /// Number of shadow entries for a row — handy for delete assertions.
    pub fn row_entry_count(&self, table: &str, pk: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .shadow
            .iter()
            .filter(|((t, p, _), _)| t == table && p == pk)
            .count()
    }
}

impl ShadowStore for MemoryStore {
    async fn get_shadow(
        &self,
        table: &str,
        pk: &str,
        cid: &str,
    ) -> Result<Option<ShadowRow>, StoreError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .shadow
            .get(&(table.to_string(), pk.to_string(), cid.to_string()))
            .cloned())
    }

    async fn get_row_entries(
        &self,
        table: &str,
        pk: &str,
    ) -> Result<Vec<(String, ShadowRow)>, StoreError> {
        Ok(self
            .inner
            .lock()
            .unwrap()
            .shadow
            .iter()
            .filter(|((t, p, _), _)| t == table && p == pk)
            .map(|((_, _, cid), row)| (cid.clone(), row.clone()))
            .collect())
    }

    async fn get_changes_since(&self, since: u64) -> Result<Vec<ColumnChange>, StoreError> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<ColumnChange> = inner
            .shadow
            .iter()
            .filter(|(_, row)| row.db_version > since)
            .map(|((table, pk, cid), row)| ColumnChange {
                table: TableName(table.clone()),
                pk: PrimaryKey(pk.clone()),
                cid: ColumnName(cid.clone()),
                val: row.val.clone(),
                site_id: NodeId(row.site_id),
                col_version: row.col_version,
                cl: row.cl,
                seq: row.seq,
                db_version: row.db_version,
                // Carry the tombstone stamp through — the trait is the wire.
                deleted_ts: row.deleted_ts,
            })
            .collect();
        out.sort_by_key(|c| (c.db_version, c.seq));
        Ok(out)
    }

    async fn apply_batch(&self, batch: WriteBatch) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        if inner.fail_next_batch {
            inner.fail_next_batch = false;
            return Err(StoreError::Idb("injected batch failure".into()));
        }
        // Same mutation order as BrowserStore::apply_batch: clears and row
        // deletions first, then upserts, then the meta/cursor singletons.
        for (table, pk) in &batch.row_deletes {
            inner.shadow.retain(|(t, p, _), _| !(t == table && p == pk));
        }
        for (table, pk) in &batch.tombstone_clears {
            inner
                .shadow
                .remove(&(table.clone(), pk.clone(), "__deleted".to_string()));
        }
        for (table, pk, cid, row) in batch.shadow_puts {
            inner.shadow.insert((table, pk, cid), row);
        }
        if let Some(v) = batch.db_version {
            inner.db_version = v;
        }
        if let Some((peer, v)) = batch.peer_version {
            inner.peer_versions.insert(peer, v);
        }
        Ok(())
    }
}

impl MemoryStore {
    /// Test-only: rewrite a tombstone's deleted_ts to simulate aging
    /// without sleeping. Mirrors the raw-SQL UPDATE used on the native
    /// side of the convergence suite.
    pub fn age_tombstone(&self, table: &str, pk: &str, new_ts: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(row) =
            inner
                .shadow
                .get_mut(&(table.to_string(), pk.to_string(), "__deleted".to_string()))
        {
            row.deleted_ts = Some(new_ts);
        }
    }
}

/// Real wall clock for retention params in tests: native peers stamp real
/// times, so the web side must age against the same clock. Nothing ages
/// out under the 7-day default within a test run.
pub fn test_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
