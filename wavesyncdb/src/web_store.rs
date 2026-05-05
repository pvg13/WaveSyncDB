//! Browser-side persistent storage for `web_engine`.
//!
//! Backed by IndexedDB via the `idb` crate. Three object stores:
//!
//! - **`meta`** — singletons keyed by string: `"site_id"` (16-byte array
//!   serialized as JSON), `"db_version"` (u64), `"keypair"` (libp2p
//!   protobuf-encoded keypair bytes). Restoring the keypair on connect is
//!   what gives a reloaded tab the same `PeerId` it had before — without
//!   it, the network treats every reload as a brand-new peer.
//! - **`shadow`** — per-column Lamport state, keyed by
//!   `"<table>|<pk>|<cid>"`. Mirrors the native `_wavesync_<table>_clocks`
//!   tables: `(val, site_id, col_version, cl, seq, db_version)`. This is
//!   what lets conflict resolution work across reloads — without it,
//!   every restart forgets what version it last saw and remote changes
//!   that should have lost would re-apply on top of newer local data.
//! - **`peer_versions`** — per-peer `last_db_version` (u64), keyed by
//!   peer-id string. Reserved for a future version-vector catch-up; not
//!   read on this branch but written on every successful incoming Push so
//!   the data is ready when catch-up lands.
//!
//! Key encoding choices follow the same conventions as the native shadow
//! tables: composite keys are joined with `'|'` (table/pk/cid components
//! that contain `'|'` would collide, but native uses the same separator
//! and has not had problems in practice — see `shadow.rs`).

use std::sync::Arc;

use idb::{
    Database, DatabaseEvent, Error as IdbError, Factory, KeyRange, ObjectStoreParams, Query,
    TransactionMode,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;

use crate::messages::NodeId;

const STORE_META: &str = "meta";
const STORE_SHADOW: &str = "shadow";
const STORE_PEER_VERSIONS: &str = "peer_versions";

const META_SITE_ID: &str = "site_id";
const META_DB_VERSION: &str = "db_version";
const META_KEYPAIR: &str = "keypair";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("indexeddb error: {0}")]
    Idb(String),
    #[error("serde error: {0}")]
    Serde(String),
}

impl From<IdbError> for StoreError {
    fn from(e: IdbError) -> Self {
        Self::Idb(e.to_string())
    }
}

/// One materialized application row — pk plus the latest persisted value
/// for every column. Returned by [`BrowserStore::list_table_rows`].
#[derive(Debug, Clone)]
pub struct ResolvedRow {
    pub pk: String,
    pub columns: std::collections::HashMap<String, serde_json::Value>,
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

/// Async-friendly handle to the IndexedDB-backed store.
///
/// Cheap to clone — internally an `Arc<Database>`. `Database` itself is
/// `!Send + !Sync`, but the whole web engine runs on the single-threaded
/// browser event loop via `wasm_bindgen_futures::spawn_local`, so there's
/// no real cross-thread sharing happening.
#[derive(Clone)]
pub struct BrowserStore {
    db: Arc<Database>,
}

impl BrowserStore {
    /// Open or create the IndexedDB database for `store_name`.
    ///
    /// The database is named `wavesync-<store_name>`. If the database does
    /// not exist or its version is older than the current schema, the
    /// missing object stores are created in `on_upgrade_needed`.
    pub async fn open(store_name: &str) -> Result<Self, StoreError> {
        let db_name = format!("wavesync-{store_name}");
        let factory = Factory::new()?;
        let mut request = factory.open(&db_name, Some(1))?;

        request.on_upgrade_needed(|event| {
            let database = event.database().expect("database");
            let existing = database.store_names();
            for name in [STORE_META, STORE_SHADOW, STORE_PEER_VERSIONS] {
                if !existing.contains(&name.to_string()) {
                    let params = ObjectStoreParams::new();
                    database
                        .create_object_store(name, params)
                        .expect("create object store");
                }
            }
        });

        let db = request.await?;
        Ok(Self { db: Arc::new(db) })
    }

    // ── meta singletons ──────────────────────────────────────────────────

    /// Read the persisted libp2p keypair bytes (protobuf-encoded), if any.
    pub async fn get_keypair(&self) -> Result<Option<Vec<u8>>, StoreError> {
        self.meta_get_bytes(META_KEYPAIR).await
    }

    /// Persist a libp2p keypair as protobuf-encoded bytes.
    ///
    /// Called once when a fresh client generates a new identity.
    /// Subsequent `connect_persistent` calls on the same store name will
    /// restore that identity and present the same `PeerId` to the network.
    pub async fn put_keypair(&self, bytes: &[u8]) -> Result<(), StoreError> {
        self.meta_put_bytes(META_KEYPAIR, bytes).await
    }

    /// Read the persisted local `site_id` (used for CRDT conflict tiebreaks).
    pub async fn get_site_id(&self) -> Result<Option<NodeId>, StoreError> {
        let bytes = self.meta_get_bytes(META_SITE_ID).await?;
        Ok(bytes.and_then(|b| {
            if b.len() == 16 {
                let mut a = [0u8; 16];
                a.copy_from_slice(&b);
                Some(NodeId(a))
            } else {
                None
            }
        }))
    }

    pub async fn put_site_id(&self, id: &NodeId) -> Result<(), StoreError> {
        self.meta_put_bytes(META_SITE_ID, &id.0).await
    }

    pub async fn get_db_version(&self) -> Result<u64, StoreError> {
        let bytes = self.meta_get_bytes(META_DB_VERSION).await?;
        Ok(bytes
            .map(|b| {
                let mut buf = [0u8; 8];
                buf[..b.len().min(8)].copy_from_slice(&b[..b.len().min(8)]);
                u64::from_le_bytes(buf)
            })
            .unwrap_or(0))
    }

    pub async fn put_db_version(&self, v: u64) -> Result<(), StoreError> {
        self.meta_put_bytes(META_DB_VERSION, &v.to_le_bytes()).await
    }

    async fn meta_get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_META], TransactionMode::ReadOnly)?;
        let store = tx.object_store(STORE_META)?;
        let result = store.get(JsValue::from_str(key))?.await?;
        tx.commit()?.await?;
        match result {
            Some(v) if !v.is_null() && !v.is_undefined() => {
                let arr: Vec<u8> = serde_wasm_bindgen::from_value(v)
                    .map_err(|e| StoreError::Serde(e.to_string()))?;
                Ok(Some(arr))
            }
            _ => Ok(None),
        }
    }

    async fn meta_put_bytes(&self, key: &str, value: &[u8]) -> Result<(), StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_META], TransactionMode::ReadWrite)?;
        let store = tx.object_store(STORE_META)?;
        let js_val =
            serde_wasm_bindgen::to_value(value).map_err(|e| StoreError::Serde(e.to_string()))?;
        store.put(&js_val, Some(&JsValue::from_str(key)))?.await?;
        tx.commit()?.await?;
        Ok(())
    }

    // ── shadow rows ──────────────────────────────────────────────────────

    /// Look up the current shadow entry for `(table, pk, cid)`.
    ///
    /// `None` means no entry yet — first remote write wins by definition.
    pub async fn get_shadow(
        &self,
        table: &str,
        pk: &str,
        cid: &str,
    ) -> Result<Option<ShadowRow>, StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_SHADOW], TransactionMode::ReadOnly)?;
        let store = tx.object_store(STORE_SHADOW)?;
        let key = shadow_key(table, pk, cid);
        let result = store.get(JsValue::from_str(&key))?.await?;
        tx.commit()?.await?;
        match result {
            Some(v) if !v.is_null() && !v.is_undefined() => {
                let row: ShadowRow = serde_wasm_bindgen::from_value(v)
                    .map_err(|e| StoreError::Serde(e.to_string()))?;
                Ok(Some(row))
            }
            _ => Ok(None),
        }
    }

    /// Upsert the shadow entry for `(table, pk, cid)`.
    ///
    /// Mirrors native's `INSERT OR REPLACE` semantics — IndexedDB's `put`
    /// is upsert-by-default when a key is supplied.
    pub async fn put_shadow(
        &self,
        table: &str,
        pk: &str,
        cid: &str,
        row: &ShadowRow,
    ) -> Result<(), StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_SHADOW], TransactionMode::ReadWrite)?;
        let store = tx.object_store(STORE_SHADOW)?;
        let key = shadow_key(table, pk, cid);
        let js_val =
            serde_wasm_bindgen::to_value(row).map_err(|e| StoreError::Serde(e.to_string()))?;
        store.put(&js_val, Some(&JsValue::from_str(&key)))?.await?;
        tx.commit()?.await?;
        Ok(())
    }

    // ── shadow scans ─────────────────────────────────────────────────────

    /// Materialize all rows in `table` from the shadow store.
    ///
    /// Scans every shadow entry whose key starts with `"<table>|"`, groups
    /// them by primary key, and returns one [`ResolvedRow`] per pk with
    /// each column's most recently persisted value. Rows that have a
    /// `__deleted` shadow entry are excluded.
    ///
    /// This is what lets a UI on first mount say "show me everything in
    /// `tasks`" without the application keeping its own table store —
    /// the shadow table *is* the materialized state, since per-(pk, cid)
    /// upsert-in-place gives one entry per column.
    ///
    /// Cost is O(rows × cols) — fine for tens of thousands of entries,
    /// reconsider if shadow size grows beyond that.
    pub async fn list_table_rows(&self, table: &str) -> Result<Vec<ResolvedRow>, StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_SHADOW], TransactionMode::ReadOnly)?;
        let store = tx.object_store(STORE_SHADOW)?;

        // Range "<table>|" .. "<table>|\u{ffff}" — `\u{ffff}` is the
        // largest BMP codepoint, so this catches every key prefixed by
        // `"<table>|"` regardless of the pk/cid contents. Open ranges
        // would need exclusive-end handling, so use closed bounds and a
        // sentinel.
        let lower = JsValue::from_str(&format!("{table}|"));
        let upper = JsValue::from_str(&format!("{table}|\u{ffff}"));
        let range = KeyRange::bound(&lower, &upper, None, None)?;
        let q = Query::from(range);

        let keys: Vec<JsValue> = store.get_all_keys(Some(q.clone()), None)?.await?;
        let values: Vec<JsValue> = store.get_all(Some(q), None)?.await?;
        tx.commit()?.await?;

        if keys.len() != values.len() {
            return Err(StoreError::Idb(format!(
                "shadow scan returned {} keys but {} values",
                keys.len(),
                values.len()
            )));
        }

        // Group by pk → column map. The key format is `<table>|<pk>|<cid>`.
        let mut rows: std::collections::BTreeMap<String, ResolvedRow> = Default::default();
        let mut tombstoned: std::collections::HashSet<String> = Default::default();
        for (k_js, v_js) in keys.into_iter().zip(values.into_iter()) {
            let key = match k_js.as_string() {
                Some(s) => s,
                None => continue,
            };
            let parts: Vec<&str> = key.splitn(3, '|').collect();
            if parts.len() != 3 {
                continue;
            }
            let pk = parts[1].to_string();
            let cid = parts[2].to_string();

            let row: ShadowRow = serde_wasm_bindgen::from_value(v_js)
                .map_err(|e| StoreError::Serde(e.to_string()))?;

            if cid == "__deleted" {
                tombstoned.insert(pk.clone());
                continue;
            }

            let entry = rows.entry(pk.clone()).or_insert_with(|| ResolvedRow {
                pk,
                columns: std::collections::HashMap::new(),
            });
            if let Some(v) = row.val {
                entry.columns.insert(cid, v);
            }
        }

        Ok(rows
            .into_iter()
            .filter(|(pk, _)| !tombstoned.contains(pk))
            .map(|(_, r)| r)
            .collect())
    }

    // ── catch-up scans ───────────────────────────────────────────────────

    /// Return every shadow entry whose `db_version` is strictly greater
    /// than `since`, sorted by `(db_version, seq)` for deterministic
    /// replay order.
    ///
    /// Drives version-vector catch-up: a peer that asks "send me changes
    /// since N" gets exactly the entries written after N, mapped back into
    /// `ColumnChange`s ready to be wrapped in a `SyncChangeset` and
    /// pushed.
    ///
    /// Implementation is a full scan + filter — IndexedDB lacks an index
    /// on the `db_version` payload field, and adding one would require a
    /// schema migration. Fine for the sizes the demo handles; production
    /// browser apps with large shadow tables would want to add an index
    /// and a migration that uses it.
    pub async fn get_changes_since(
        &self,
        since: u64,
    ) -> Result<Vec<crate::messages::ColumnChange>, StoreError> {
        use crate::messages::{ColumnChange, ColumnName, NodeId, PrimaryKey, TableName};

        let tx = self
            .db
            .transaction(&[STORE_SHADOW], TransactionMode::ReadOnly)?;
        let store = tx.object_store(STORE_SHADOW)?;
        let keys: Vec<JsValue> = store.get_all_keys(None, None)?.await?;
        let values: Vec<JsValue> = store.get_all(None, None)?.await?;
        tx.commit()?.await?;

        if keys.len() != values.len() {
            return Err(StoreError::Idb(format!(
                "shadow scan returned {} keys but {} values",
                keys.len(),
                values.len()
            )));
        }

        let mut out: Vec<ColumnChange> = Vec::new();
        for (k_js, v_js) in keys.into_iter().zip(values.into_iter()) {
            let key = match k_js.as_string() {
                Some(s) => s,
                None => continue,
            };
            let parts: Vec<&str> = key.splitn(3, '|').collect();
            if parts.len() != 3 {
                continue;
            }
            let row: ShadowRow = serde_wasm_bindgen::from_value(v_js)
                .map_err(|e| StoreError::Serde(e.to_string()))?;
            if row.db_version <= since {
                continue;
            }
            out.push(ColumnChange {
                table: TableName(parts[0].to_string()),
                pk: PrimaryKey(parts[1].to_string()),
                cid: ColumnName(parts[2].to_string()),
                val: row.val,
                site_id: NodeId(row.site_id),
                col_version: row.col_version,
                cl: row.cl,
                seq: row.seq,
                db_version: row.db_version,
            });
        }

        // Deterministic replay order: by db_version (the batch the write
        // belonged to on the originator) then by seq (position within
        // that batch). This matches the native engine's get_changes_since
        // ordering so a peer migrating between transports won't see a
        // different replay shape.
        out.sort_by_key(|c| (c.db_version, c.seq));
        Ok(out)
    }

    // ── peer versions ────────────────────────────────────────────────────

    /// Record the highest `db_version` seen from `peer_id`.
    ///
    /// Written on every successful incoming Push and on every catch-up
    /// response, so the next [`Self::get_peer_version`] returns an
    /// accurate "I last saw you at N" for the catch-up request.
    pub async fn set_peer_version(&self, peer_id: &str, version: u64) -> Result<(), StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_PEER_VERSIONS], TransactionMode::ReadWrite)?;
        let store = tx.object_store(STORE_PEER_VERSIONS)?;
        let js_val =
            serde_wasm_bindgen::to_value(&version).map_err(|e| StoreError::Serde(e.to_string()))?;
        store
            .put(&js_val, Some(&JsValue::from_str(peer_id)))?
            .await?;
        tx.commit()?.await?;
        Ok(())
    }

    /// Read the persisted `last seen db_version` for `peer_id`. Returns
    /// `0` for unknown peers — same convention as the native protocol,
    /// where `your_last_db_version=0` means "send me everything you have."
    pub async fn get_peer_version(&self, peer_id: &str) -> Result<u64, StoreError> {
        let tx = self
            .db
            .transaction(&[STORE_PEER_VERSIONS], TransactionMode::ReadOnly)?;
        let store = tx.object_store(STORE_PEER_VERSIONS)?;
        let result = store.get(JsValue::from_str(peer_id))?.await?;
        tx.commit()?.await?;
        match result {
            Some(v) if !v.is_null() && !v.is_undefined() => {
                let n: u64 = serde_wasm_bindgen::from_value(v)
                    .map_err(|e| StoreError::Serde(e.to_string()))?;
                Ok(n)
            }
            _ => Ok(0),
        }
    }
}

fn shadow_key(table: &str, pk: &str, cid: &str) -> String {
    format!("{table}|{pk}|{cid}")
}
