//! Native-engine side of convergence verification + RBSR (#82).
//!
//! The pure algorithm (fingerprints, digests, range exchange) lives in the
//! target-independent [`crate::reconcile`] module — shared with the browser
//! engine so both targets fingerprint cells identically. This module adds the
//! SQLite-bound enumeration: value bytes come from `get_changes_since` (which
//! reads through SQLite), so two peers that hold the same data produce
//! identical fingerprints. The primary-key column's clock cell is excluded
//! (the writer records one but the receiver rejects PK-column changes, so it's
//! asymmetric; the PK value is already implicit in every cell's key).

use super::*;
use sea_orm::DatabaseConnection;

pub use crate::reconcile::*;

/// Enumerate this group's synced cells (PK column excluded), sorted by key.
pub(super) async fn enumerate_sorted_cells(
    db: &DatabaseConnection,
    registry: &TableRegistry,
) -> Vec<LocalCell> {
    let changes = shadow::get_changes_since(db, registry, 0)
        .await
        .unwrap_or_default();
    crate::reconcile::sorted_cells_from_changes(changes, |table| {
        registry.get(table).map(|m| m.primary_key_column.clone())
    })
}

/// The single value-inclusive convergence digest for one group (XOR of every
/// cell fingerprint). Equal across two peers iff their data is byte-identical.
pub(super) async fn compute_group_digest(
    db: &DatabaseConnection,
    registry: &TableRegistry,
) -> [u8; 32] {
    range_fp(&enumerate_sorted_cells(db, registry).await)
}
