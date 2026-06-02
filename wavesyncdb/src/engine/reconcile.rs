//! Convergence verification via a value-inclusive shadow digest (#82).
//!
//! Two peers hold identical group data iff every shadow-table cell agrees on
//! both its CRDT version *and* its value. We summarise that as a single 32-byte
//! digest: the XOR of a per-cell BLAKE3 hash over
//! `(table, pk, cid, col_version, site_id, canonical value bytes)`. XOR makes
//! the per-cell contributions order-independent, so the scan order is
//! irrelevant and two peers compute the *same* digest iff their data matches.
//!
//! The value bytes come from [`sync_handler::get_local_value_bytes`] — the same
//! serialization conflict resolution uses for its `value_bytes` tiebreak — so
//! two converged peers are guaranteed to produce byte-identical input (no false
//! divergence from value encoding).
//!
//! This is the foundation cut of #82: it lets a peer *prove* convergence, a
//! capability the version-vector catch-up lacks (matching `db_version` only
//! means "same height", not "same data"). On a digest mismatch the engine
//! still relies on the existing version-vector catch-up to transfer the diff;
//! range-based reconciliation that ships only the diff is a later additive step.

use super::*;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

/// Shadow `cid` sentinel for a row tombstone (deleted row). Tombstone cells
/// have no user-table value, so their value bytes are empty.
const TOMBSTONE_CID: &str = "__deleted";

/// Compute the value-inclusive convergence digest for one group's database.
///
/// Scans every synced table's `_wavesync_<table>_clock` shadow table and folds
/// each cell into an XOR accumulator. A missing shadow table (entity registered
/// but never written) contributes nothing. Returns the all-zero digest for an
/// empty group, which correctly matches another empty peer.
pub(super) async fn compute_group_digest(
    db: &DatabaseConnection,
    registry: &TableRegistry,
) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for meta in registry.all_tables() {
        let table = &meta.table_name;
        let shadow = format!("_wavesync_{}_clock", table.replace('"', "\"\""));
        let rows = match db
            .query_all_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                format!("SELECT pk, cid, col_version, site_id FROM \"{shadow}\""),
            ))
            .await
        {
            Ok(rows) => rows,
            // Shadow table absent (no writes yet) — nothing to fold in.
            Err(_) => continue,
        };
        for row in rows {
            let pk: String = row.try_get("", "pk").unwrap_or_default();
            let cid: String = row.try_get("", "cid").unwrap_or_default();
            let cv: i64 = row.try_get("", "col_version").unwrap_or(0);
            let site: Vec<u8> = row.try_get("", "site_id").unwrap_or_default();

            // Skip the primary-key column's clock cell. The *writer* records a
            // clock entry for the PK column in `dispatch_sync`, but the receiver
            // rejects PK-column changes in `apply_remote_changeset` (the PK is
            // immutable row identity, not a mutable value), so the receiver
            // never has this cell — an asymmetry that would make two otherwise-
            // converged peers' digests differ forever. The PK value is already
            // captured implicitly: it is the `pk` component of every other cell.
            if cid == meta.primary_key_column {
                continue;
            }
            // Tombstones carry no user-table value; live cells use the same
            // canonical serialization the CRDT tiebreak uses.
            let value_bytes = if cid == TOMBSTONE_CID {
                Vec::new()
            } else {
                sync_handler::get_local_value_bytes(db, table, &meta.primary_key_column, &pk, &cid)
                    .await
            };

            let mut h = blake3::Hasher::new();
            h.update(table.as_bytes());
            h.update(&[0]);
            h.update(pk.as_bytes());
            h.update(&[0]);
            h.update(cid.as_bytes());
            h.update(&[0]);
            h.update(&cv.to_le_bytes());
            h.update(&site);
            h.update(&value_bytes);
            let cell = h.finalize();
            for (a, b) in acc.iter_mut().zip(cell.as_bytes()) {
                *a ^= b;
            }
        }
    }
    acc
}
