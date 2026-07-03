//! Pure core of convergence verification + recursive range-based set
//! reconciliation (#82). Target-independent: shared by the native engine
//! (`engine/reconcile.rs`, which adds the SQLite-bound enumeration) and the
//! browser engine (which enumerates cells from its own store).
//!
//! Two peers hold identical group data iff every shadow cell agrees on its
//! CRDT version *and* value. A cell is summarised by [`cell_fp`] — a BLAKE3
//! hash over `(table, pk, cid, col_version, site_id, value bytes)` — and cells
//! are combined with XOR (order-independent).
//!
//! - **Convergence proof:** XOR every cell's fingerprint into one digest
//!   ([`digest_cells`]). Equal digests across two peers ⇒ proven identical
//!   (the version-vector catch-up can't prove this).
//! - **Recursive reconciliation (RBSR):** cells are sorted by key
//!   (`table\0pk\0cid`). A round exchanges a list of [`RangeEntry`]s covering
//!   the keyspace; [`reconcile_step`] drops converged ranges, **splits** large
//!   mismatching ones into sub-ranges (recursing), and **itemizes** small ones
//!   (`IdList` → the peer replies `Transfer` with the cells we lack). This
//!   terminates in O(log n) rounds and transfers ~the symmetric difference,
//!   not the whole range — so a long-offline / partition-merge peer reconciles
//!   over the relay cheaply.
//!
//! Because BOTH targets call these same functions, digest parity between a
//! browser replica and a native replica holds by construction: equal logical
//! cells produce equal fingerprints.

use crate::messages::ColumnChange;
use crate::protocol::{RangeEntry, RangeItem, RangePayload};

/// Fan-out / leaf threshold for the recursion: a mismatching range with at most
/// this many cells is itemized (`IdList`) and transferred; a larger one is split
/// into this many sub-ranges. 16 keeps each round's payload modest while giving
/// ~`log16(n)` rounds.
pub const RBSR_BUCKET: usize = 16;

/// A local cell as seen by reconciliation: sort key, content fingerprint, and
/// the change that reproduces it on a peer that lacks it.
#[derive(Clone, Debug)]
pub struct LocalCell {
    pub key: Vec<u8>,
    pub fp: [u8; 32],
    pub change: ColumnChange,
}

/// Sort key for a cell: `table\0 pk\0 cid` (lexicographic, stable across peers).
pub fn sort_key(table: &str, pk: &str, cid: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(table.len() + pk.len() + cid.len() + 2);
    k.extend_from_slice(table.as_bytes());
    k.push(0);
    k.extend_from_slice(pk.as_bytes());
    k.push(0);
    k.extend_from_slice(cid.as_bytes());
    k
}

/// Content fingerprint of a cell. Covers identity + CRDT version + value, so any
/// difference flips it. Both the digest and RBSR use this, keeping them
/// consistent (a digest mismatch ⇒ RBSR finds the differing cell).
pub fn cell_fp(c: &ColumnChange) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(c.table.0.as_bytes());
    h.update(&[0]);
    h.update(c.pk.0.as_bytes());
    h.update(&[0]);
    h.update(c.cid.0.as_bytes());
    h.update(&[0]);
    h.update(&c.col_version.to_le_bytes());
    h.update(&c.site_id.0);
    h.update(&serde_json::to_vec(&c.val).unwrap_or_default());
    *h.finalize().as_bytes()
}

fn xor_into(acc: &mut [u8; 32], v: &[u8; 32]) {
    for (a, b) in acc.iter_mut().zip(v) {
        *a ^= b;
    }
}

/// XOR the fingerprints of a slice of cells (a range fingerprint).
pub fn range_fp(cells: &[LocalCell]) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for c in cells {
        xor_into(&mut acc, &c.fp);
    }
    acc
}

/// The single value-inclusive convergence digest over a set of changes (XOR of
/// every cell fingerprint, order-independent). Equal across two peers iff their
/// data is byte-identical.
pub fn digest_cells(changes: &[ColumnChange]) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for c in changes {
        xor_into(&mut acc, &cell_fp(c));
    }
    acc
}

/// Turn a flat change list into reconciliation cells, sorted by key.
///
/// `pk_column_of` names the primary-key column for a table (if known); the PK
/// column's clock cell is excluded — the writer records one but the receiver
/// rejects PK-column changes, so it's asymmetric, and the PK value is already
/// implicit in every cell's key. An unknown table keeps its cells (mirrors the
/// native registry behaviour).
pub fn sorted_cells_from_changes(
    changes: Vec<ColumnChange>,
    pk_column_of: impl Fn(&str) -> Option<String>,
) -> Vec<LocalCell> {
    let mut cells: Vec<LocalCell> = changes
        .into_iter()
        .filter(|c| pk_column_of(&c.table.0).is_none_or(|pk_col| pk_col != c.cid.0))
        .map(|c| LocalCell {
            key: sort_key(&c.table.0, &c.pk.0, &c.cid.0),
            fp: cell_fp(&c),
            change: c,
        })
        .collect();
    cells.sort_by(|a, b| a.key.cmp(&b.key));
    cells
}

/// The first reconciliation message: split the whole keyspace into up to
/// `RBSR_BUCKET` sub-ranges, each with its fingerprint. (One empty range when
/// there are no cells, so an empty peer still drives the exchange.)
pub fn initial_entries(local: &[LocalCell]) -> Vec<RangeEntry> {
    if local.is_empty() {
        return vec![RangeEntry {
            lower: None,
            upper: None,
            payload: RangePayload::Fingerprint([0u8; 32]),
        }];
    }
    let mut out = Vec::new();
    split_into_fingerprints(&mut out, local, None, None);
    out
}

/// Split `slice` (cells within the range `[lo, hi)`) into up to `RBSR_BUCKET`
/// consecutive sub-ranges, each emitted as a `Fingerprint` carrying its own
/// explicit `[lower, upper)` bounds.
fn split_into_fingerprints(
    out: &mut Vec<RangeEntry>,
    slice: &[LocalCell],
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) {
    let n = slice.len();
    let group = n.div_ceil(RBSR_BUCKET).max(1);
    let mut i = 0;
    while i < n {
        let end = (i + group).min(n);
        // Lower bound: the parent's lower bound for the first group, else this
        // group's first key. Upper bound: the next group's first key, or the
        // parent's upper bound for the final group.
        let lower = if i == 0 {
            lo.map(|b| b.to_vec())
        } else {
            Some(slice[i].key.clone())
        };
        let upper = if end < n {
            Some(slice[end].key.clone())
        } else {
            hi.map(|b| b.to_vec())
        };
        out.push(RangeEntry {
            lower,
            upper,
            payload: RangePayload::Fingerprint(range_fp(&slice[i..end])),
        });
        i = end;
    }
}

/// Indices `[start, end)` of `local` (sorted) covered by `[lo, hi)`.
fn range_bounds(local: &[LocalCell], lo: Option<&[u8]>, hi: Option<&[u8]>) -> (usize, usize) {
    let start = lo.map_or(0, |b| local.partition_point(|c| c.key.as_slice() < b));
    let end = hi.map_or(local.len(), |b| {
        local.partition_point(|c| c.key.as_slice() < b)
    });
    (start, end)
}

fn to_range_item(c: &LocalCell) -> RangeItem {
    RangeItem {
        key: c.key.clone(),
        fp: c.fp,
    }
}

/// Process one incoming reconciliation message against our local cells.
///
/// Returns `(reply_entries, changes_to_apply)`. The reply continues the
/// recursion; `changes_to_apply` are cells the peer had that we lacked/differed
/// on (applied through the normal conflict-resolving path by the caller). An
/// empty reply for the whole message means this side is converged.
pub fn reconcile_step(
    local: &[LocalCell],
    incoming: &[RangeEntry],
) -> (Vec<RangeEntry>, Vec<ColumnChange>) {
    use std::collections::HashMap;
    let mut out = Vec::new();
    let mut to_apply = Vec::new();

    for entry in incoming {
        // Bounds are carried explicitly per entry, so each maps independently
        // of message order and of any converged ranges dropped by the peer.
        let lo = entry.lower.as_deref();
        let hi = entry.upper.as_deref();
        let (start, end) = range_bounds(local, lo, hi);
        let slice = &local[start..end];

        match &entry.payload {
            RangePayload::Fingerprint(remote_fp) => {
                if &range_fp(slice) != remote_fp {
                    if slice.len() <= RBSR_BUCKET {
                        // Small mismatching range — send our cell identities so
                        // the peer can compute the exact diff.
                        out.push(RangeEntry {
                            lower: entry.lower.clone(),
                            upper: entry.upper.clone(),
                            payload: RangePayload::IdList(
                                slice.iter().map(to_range_item).collect(),
                            ),
                        });
                    } else {
                        // Large — recurse by splitting into sub-range fingerprints.
                        split_into_fingerprints(&mut out, slice, lo, hi);
                    }
                }
                // Equal → converged range, dropped from the reply.
            }
            RangePayload::IdList(remote_items) => {
                let remote_by_key: HashMap<&[u8], [u8; 32]> = remote_items
                    .iter()
                    .map(|it| (it.key.as_slice(), it.fp))
                    .collect();
                // Cells WE have that the peer lacks/differs on → send them.
                let transfer: Vec<ColumnChange> = slice
                    .iter()
                    .filter(|c| remote_by_key.get(c.key.as_slice()) != Some(&c.fp))
                    .map(|c| c.change.clone())
                    .collect();
                // Cells the PEER has that we lack/differ on → request by key.
                let local_by_key: HashMap<&[u8], [u8; 32]> =
                    slice.iter().map(|c| (c.key.as_slice(), c.fp)).collect();
                let request: Vec<Vec<u8>> = remote_items
                    .iter()
                    .filter(|it| local_by_key.get(it.key.as_slice()) != Some(&it.fp))
                    .map(|it| it.key.clone())
                    .collect();
                if !transfer.is_empty() || !request.is_empty() {
                    out.push(RangeEntry {
                        lower: entry.lower.clone(),
                        upper: entry.upper.clone(),
                        payload: RangePayload::Resolve { transfer, request },
                    });
                }
                // (IdList carries no values, so nothing to apply here.)
            }
            RangePayload::Resolve { transfer, request } => {
                // Apply the cells the peer sent us...
                to_apply.extend(transfer.iter().cloned());
                // ...and reply with the cells they requested.
                let local_by_key: HashMap<&[u8], &LocalCell> =
                    slice.iter().map(|c| (c.key.as_slice(), c)).collect();
                let reqd: Vec<ColumnChange> = request
                    .iter()
                    .filter_map(|k| local_by_key.get(k.as_slice()).map(|c| c.change.clone()))
                    .collect();
                if !reqd.is_empty() {
                    out.push(RangeEntry {
                        lower: entry.lower.clone(),
                        upper: entry.upper.clone(),
                        payload: RangePayload::Transfer(reqd),
                    });
                }
            }
            RangePayload::Transfer(changes) => {
                // Terminal: the peer sent the cells we requested. Apply, no reply.
                to_apply.extend(changes.iter().cloned());
            }
        }
    }
    (out, to_apply)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ColumnName, NodeId, PrimaryKey, TableName};
    use std::collections::HashMap;

    // Build a synthetic cell for key `i` with content tag `v` (so the same i
    // with different v models a differing value).
    fn cell(i: u32, v: u32) -> LocalCell {
        let change = ColumnChange {
            table: TableName("t".into()),
            pk: PrimaryKey(format!("{i:08}")),
            cid: ColumnName("c".into()),
            val: Some(serde_json::json!(v)),
            site_id: NodeId([0u8; 16]),
            col_version: v as u64 + 1,
            cl: v as u64 + 1,
            seq: 0,
            db_version: 0,
            deleted_ts: None,
        };
        LocalCell {
            key: sort_key(&change.table.0, &change.pk.0, &change.cid.0),
            fp: cell_fp(&change),
            change,
        }
    }

    fn make_set(items: impl IntoIterator<Item = (u32, u32)>) -> Vec<LocalCell> {
        let mut v: Vec<LocalCell> = items.into_iter().map(|(i, val)| cell(i, val)).collect();
        v.sort_by(|a, b| a.key.cmp(&b.key));
        v
    }

    fn apply(set: &mut Vec<LocalCell>, changes: &[ColumnChange]) {
        let mut by_key: HashMap<Vec<u8>, LocalCell> =
            set.drain(..).map(|c| (c.key.clone(), c)).collect();
        for ch in changes {
            let key = sort_key(&ch.table.0, &ch.pk.0, &ch.cid.0);
            // Conflict-resolve by col_version (higher wins) — mirrors the engine.
            let take = by_key
                .get(&key)
                .is_none_or(|cur| ch.col_version >= cur.change.col_version);
            if take {
                by_key.insert(
                    key.clone(),
                    LocalCell {
                        key,
                        fp: cell_fp(ch),
                        change: ch.clone(),
                    },
                );
            }
        }
        *set = by_key.into_values().collect();
        set.sort_by(|a, b| a.key.cmp(&b.key));
    }

    fn fps(set: &[LocalCell]) -> HashMap<Vec<u8>, [u8; 32]> {
        set.iter().map(|c| (c.key.clone(), c.fp)).collect()
    }

    /// Drive the full multi-round exchange between A and B to completion and
    /// return the total number of cells transferred (both directions).
    fn run_exchange(a: &mut Vec<LocalCell>, b: &mut Vec<LocalCell>) -> usize {
        let mut transferred = 0usize;
        // Count only actual cell transfers (values moved); IdList/Fingerprint/
        // request carry just identities and are cheap.
        let count = |entries: &[RangeEntry]| -> usize {
            entries
                .iter()
                .map(|e| match &e.payload {
                    RangePayload::Resolve { transfer, .. } => transfer.len(),
                    RangePayload::Transfer(v) => v.len(),
                    RangePayload::Fingerprint(_) | RangePayload::IdList(_) => 0,
                })
                .sum()
        };

        // A initiates.
        let mut msg = initial_entries(a);
        for _ in 0..64 {
            // B processes A's message.
            let (b_reply, b_apply) = reconcile_step(b, &msg);
            transferred += count(&b_reply);
            apply(b, &b_apply);
            if b_reply.is_empty() {
                break;
            }
            // A processes B's reply.
            let (a_reply, a_apply) = reconcile_step(a, &b_reply);
            transferred += count(&a_reply);
            apply(a, &a_apply);
            if a_reply.is_empty() {
                break;
            }
            msg = a_reply;
        }
        transferred
    }

    #[test]
    fn reconciles_disjoint_and_overlapping_ranges_to_union() {
        // A = {0..100}, B = {50..150} (same value where they overlap).
        let mut a = make_set((0..100).map(|i| (i, 1)));
        let mut b = make_set((50..150).map(|i| (i, 1)));

        let transferred = run_exchange(&mut a, &mut b);

        // Both converge to the union {0..150}.
        assert_eq!(a.len(), 150);
        assert_eq!(b.len(), 150);
        assert_eq!(fps(&a), fps(&b), "A and B must hold identical cells");

        // Symmetric difference is exactly 100 cells (A lacked 50, B lacked 50);
        // RBSR must move ~that, not the whole 150+150. ID-then-fetch transfers
        // only the missing cells, so this is ~100, not the naive 200.
        assert!(
            transferred <= 120,
            "RBSR transferred {transferred} cells; should be ~the diff (100)"
        );
    }

    #[test]
    fn already_converged_transfers_nothing() {
        let mut a = make_set((0..200).map(|i| (i, 7)));
        let mut b = make_set((0..200).map(|i| (i, 7)));
        let transferred = run_exchange(&mut a, &mut b);
        assert_eq!(transferred, 0, "converged peers must transfer nothing");
        assert_eq!(fps(&a), fps(&b));
    }

    #[test]
    fn reconciles_single_differing_value() {
        // Same keys, one cell has a different value on each side.
        let mut a = make_set((0..100).map(|i| (i, if i == 42 { 1 } else { 0 })));
        let mut b = make_set((0..100).map(|i| (i, if i == 42 { 2 } else { 0 })));
        let transferred = run_exchange(&mut a, &mut b);
        assert_eq!(fps(&a), fps(&b), "must converge on the conflicting cell");
        // ID-then-fetch moves only the one differing cell (each way) — not the
        // whole leaf, and far less than the naive 200.
        assert!(
            transferred <= 4,
            "transferred {transferred}, expected ~1 cell each way"
        );
    }

    #[test]
    fn empty_vs_populated() {
        let mut a: Vec<LocalCell> = Vec::new();
        let mut b = make_set((0..50).map(|i| (i, 3)));
        run_exchange(&mut a, &mut b);
        assert_eq!(a.len(), 50);
        assert_eq!(fps(&a), fps(&b));
    }

    #[test]
    fn digest_cells_is_order_independent_and_matches_range_fp() {
        let set = make_set((0..20).map(|i| (i, i % 3)));
        let changes: Vec<ColumnChange> = set.iter().map(|c| c.change.clone()).collect();
        let mut reversed = changes.clone();
        reversed.reverse();
        assert_eq!(digest_cells(&changes), digest_cells(&reversed));
        assert_eq!(digest_cells(&changes), range_fp(&set));
    }

    #[test]
    fn sorted_cells_excludes_pk_column_and_keeps_unknown_tables() {
        let mk = |table: &str, cid: &str| ColumnChange {
            table: TableName(table.into()),
            pk: PrimaryKey("p1".into()),
            cid: ColumnName(cid.into()),
            val: Some(serde_json::json!("x")),
            site_id: NodeId([0u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
            deleted_ts: None,
        };
        let changes = vec![mk("tasks", "id"), mk("tasks", "title"), mk("other", "id")];
        let cells =
            sorted_cells_from_changes(changes, |t| (t == "tasks").then(|| "id".to_string()));
        // tasks.id excluded (PK); other.id kept (unknown table).
        let cids: Vec<String> = cells
            .iter()
            .map(|c| format!("{}.{}", c.change.table.0, c.change.cid.0))
            .collect();
        assert_eq!(
            cids,
            vec!["other.id".to_string(), "tasks.title".to_string()]
        );
    }
}
