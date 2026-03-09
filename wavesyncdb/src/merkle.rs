//! Per-table binary Merkle trees for efficient sync diff detection.
//!
//! Each table gets a Merkle tree where leaves are sorted by primary key.
//! Leaf hashes are computed from `(pk, hlc_time, hlc_counter, node_id)` of
//! the latest operation for that row. The tree is stored as an implicit
//! binary heap array (index 0 = root, children of i at 2i+1 and 2i+2),
//! padded to the next power-of-two with zero-hash leaves.

use serde::{Deserialize, Serialize};

use crate::messages::NodeId;

/// A 32-byte blake3 hash.
pub type MerkleHash = [u8; 32];

/// The zero hash used for padding leaves.
pub const ZERO_HASH: MerkleHash = [0u8; 32];

/// Summary of a table's Merkle tree — just the root hash and row count.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleRoot {
    pub table_name: String,
    pub root_hash: MerkleHash,
    pub row_count: usize,
}

/// A full per-table binary Merkle tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MerkleTree {
    pub table_name: String,
    /// Number of real (non-padding) leaves.
    pub leaf_count: usize,
    /// Implicit binary heap: index 0 = root, children of i at 2i+1 and 2i+2.
    pub nodes: Vec<MerkleHash>,
    /// Sorted primary keys corresponding to the real leaves.
    pub keys: Vec<String>,
}

/// Hash a single row's state into a leaf hash.
pub fn hash_row(pk: &str, hlc_time: u64, hlc_counter: u32, node_id: &NodeId) -> MerkleHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(pk.as_bytes());
    hasher.update(&hlc_time.to_le_bytes());
    hasher.update(&hlc_counter.to_le_bytes());
    hasher.update(node_id);
    *hasher.finalize().as_bytes()
}

/// Hash two child nodes into a parent node.
pub fn hash_pair(left: &MerkleHash, right: &MerkleHash) -> MerkleHash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(left);
    hasher.update(right);
    *hasher.finalize().as_bytes()
}

/// Row info for building a tree — one entry per (table, pk).
#[derive(Clone)]
pub struct RowInfo {
    pub primary_key: String,
    pub hlc_time: u64,
    pub hlc_counter: u32,
    pub node_id: NodeId,
}

impl MerkleTree {
    /// Build a Merkle tree from sorted row info.
    ///
    /// `rows` must be sorted by `primary_key`. Duplicate keys are not expected.
    pub fn build(table_name: String, rows: Vec<RowInfo>) -> Self {
        let leaf_count = rows.len();
        if leaf_count == 0 {
            return Self {
                table_name,
                leaf_count: 0,
                nodes: vec![ZERO_HASH],
                keys: Vec::new(),
            };
        }

        let padded = leaf_count.next_power_of_two();
        let total_nodes = 2 * padded - 1;
        let leaf_start = padded - 1;

        let mut nodes = vec![ZERO_HASH; total_nodes];
        let mut keys = Vec::with_capacity(leaf_count);

        // Fill leaves
        for (i, row) in rows.iter().enumerate() {
            nodes[leaf_start + i] =
                hash_row(&row.primary_key, row.hlc_time, row.hlc_counter, &row.node_id);
            keys.push(row.primary_key.clone());
        }
        // Padding leaves are already ZERO_HASH

        // Build internal nodes bottom-up
        for i in (0..leaf_start).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            nodes[i] = hash_pair(&nodes[left], &nodes[right]);
        }

        Self {
            table_name,
            leaf_count,
            nodes,
            keys,
        }
    }

    /// Get the root hash.
    pub fn root_hash(&self) -> MerkleHash {
        self.nodes[0]
    }

    /// Get a `MerkleRoot` summary.
    pub fn root(&self) -> MerkleRoot {
        MerkleRoot {
            table_name: self.table_name.clone(),
            root_hash: self.root_hash(),
            row_count: self.leaf_count,
        }
    }

    /// Index of the first leaf in the implicit heap.
    fn leaf_start(&self) -> usize {
        if self.leaf_count == 0 {
            return 0;
        }
        self.leaf_count.next_power_of_two() - 1
    }

    /// Find differing primary keys between two trees for the same table.
    ///
    /// Walks both trees top-down, pruning subtrees where hashes match.
    /// Handles asymmetric key sets by comparing sorted key ranges.
    pub fn diff(local: &MerkleTree, remote: &MerkleTree) -> Vec<String> {
        // If roots match, trees are identical
        if local.root_hash() == remote.root_hash() {
            return Vec::new();
        }

        // If either is empty, all keys from the other are different
        if local.leaf_count == 0 {
            return remote.keys.clone();
        }
        if remote.leaf_count == 0 {
            return local.keys.clone();
        }

        // If trees have different sizes, we can't do a node-by-node walk.
        // Fall back to leaf-level comparison.
        if local.nodes.len() != remote.nodes.len() {
            return Self::diff_by_leaves(local, remote);
        }

        // Same-size trees: walk top-down
        let mut differing = Vec::new();
        let leaf_start = local.leaf_start();
        let padded = local.leaf_count.next_power_of_two();
        let mut stack = vec![0usize]; // start at root

        while let Some(idx) = stack.pop() {
            if local.nodes[idx] == remote.nodes[idx] {
                continue; // subtree matches
            }

            if idx >= leaf_start {
                // Leaf node — determine which key this is
                let leaf_idx = idx - leaf_start;
                if leaf_idx < local.leaf_count {
                    differing.push(local.keys[leaf_idx].clone());
                } else if leaf_idx < remote.leaf_count {
                    differing.push(remote.keys[leaf_idx].clone());
                }
                // padding leaf differs — ignore
            } else {
                // Internal node — push children
                let left = 2 * idx + 1;
                let right = 2 * idx + 2;
                if left < 2 * padded - 1 {
                    stack.push(left);
                }
                if right < 2 * padded - 1 {
                    stack.push(right);
                }
            }
        }

        differing
    }

    /// Fallback diff for trees with different numbers of leaves.
    /// Compares leaf hashes by matching sorted keys.
    fn diff_by_leaves(local: &MerkleTree, remote: &MerkleTree) -> Vec<String> {
        let mut differing = Vec::new();
        let local_leaf_start = local.leaf_start();
        let remote_leaf_start = remote.leaf_start();

        let mut li = 0;
        let mut ri = 0;
        while li < local.leaf_count && ri < remote.leaf_count {
            match local.keys[li].cmp(&remote.keys[ri]) {
                std::cmp::Ordering::Equal => {
                    let lh = local.nodes[local_leaf_start + li];
                    let rh = remote.nodes[remote_leaf_start + ri];
                    if lh != rh {
                        differing.push(local.keys[li].clone());
                    }
                    li += 1;
                    ri += 1;
                }
                std::cmp::Ordering::Less => {
                    // local has a key remote doesn't
                    differing.push(local.keys[li].clone());
                    li += 1;
                }
                std::cmp::Ordering::Greater => {
                    // remote has a key local doesn't
                    differing.push(remote.keys[ri].clone());
                    ri += 1;
                }
            }
        }
        while li < local.leaf_count {
            differing.push(local.keys[li].clone());
            li += 1;
        }
        while ri < remote.leaf_count {
            differing.push(remote.keys[ri].clone());
            ri += 1;
        }

        differing
    }

    /// Update an existing leaf's hash (row was updated). O(log n).
    ///
    /// If the key doesn't exist, inserts it (which requires a rebuild).
    pub fn update_leaf(
        &mut self,
        pk: &str,
        hlc_time: u64,
        hlc_counter: u32,
        node_id: &NodeId,
    ) {
        let new_hash = hash_row(pk, hlc_time, hlc_counter, node_id);

        if let Ok(pos) = self.keys.binary_search_by(|k| k.as_str().cmp(pk)) {
            // Existing key — update in place
            let leaf_start = self.leaf_start();
            let idx = leaf_start + pos;
            self.nodes[idx] = new_hash;
            self.recompute_path(idx);
        } else {
            // New key — insert and rebuild
            self.insert_leaf(pk, hlc_time, hlc_counter, node_id);
        }
    }

    /// Insert a new leaf and rebuild the tree. O(n).
    pub fn insert_leaf(
        &mut self,
        pk: &str,
        hlc_time: u64,
        hlc_counter: u32,
        node_id: &NodeId,
    ) {
        let pos = self.keys.binary_search_by(|k| k.as_str().cmp(pk)).unwrap_or_else(|p| p);

        // Build row info from existing keys + new key
        let mut rows: Vec<RowInfo> = Vec::with_capacity(self.leaf_count + 1);
        let leaf_start = self.leaf_start();

        for (i, key) in self.keys.iter().enumerate() {
            if i == pos {
                rows.push(RowInfo {
                    primary_key: pk.to_string(),
                    hlc_time,
                    hlc_counter,
                    node_id: *node_id,
                });
            }
            // For existing leaves we need to recover their row info.
            // We can't recover the original hlc/node_id from the hash, so we
            // store the hash directly. Instead of rebuilding from RowInfo,
            // we'll just rebuild the tree from hashes.
            // Actually, we can just rebuild using the leaf hashes we already have.
            let _ = key; // suppress unused
            let _ = leaf_start; // suppress unused
            let _ = i;
        }

        // Simpler approach: insert key, rebuild from leaf hashes
        let new_hash = hash_row(pk, hlc_time, hlc_counter, node_id);

        self.keys.insert(pos, pk.to_string());
        self.leaf_count += 1;

        let padded = self.leaf_count.next_power_of_two();
        let total_nodes = 2 * padded - 1;
        let new_leaf_start = padded - 1;

        // Collect old leaf hashes (excluding the new one)
        let old_leaf_start = if self.leaf_count <= 1 {
            0
        } else {
            (self.leaf_count - 1).next_power_of_two() - 1
        };
        let mut old_hashes: Vec<MerkleHash> = Vec::with_capacity(self.leaf_count - 1);
        for i in 0..(self.leaf_count - 1) {
            if old_leaf_start + i < self.nodes.len() {
                old_hashes.push(self.nodes[old_leaf_start + i]);
            } else {
                old_hashes.push(ZERO_HASH);
            }
        }

        // Build new nodes array
        let mut nodes = vec![ZERO_HASH; total_nodes];
        // Place leaf hashes in order, inserting the new one at `pos`
        let mut old_idx = 0;
        for i in 0..self.leaf_count {
            if i == pos {
                nodes[new_leaf_start + i] = new_hash;
            } else {
                nodes[new_leaf_start + i] = old_hashes[old_idx];
                old_idx += 1;
            }
        }

        self.nodes = nodes;
        // Rebuild internal nodes
        for i in (0..new_leaf_start).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            self.nodes[i] = hash_pair(&self.nodes[left], &self.nodes[right]);
        }
    }

    /// Remove a leaf by primary key and rebuild the tree. O(n).
    pub fn remove_leaf(&mut self, pk: &str) {
        let pos = match self.keys.binary_search_by(|k| k.as_str().cmp(pk)) {
            Ok(p) => p,
            Err(_) => return, // key not found
        };

        let old_leaf_start = self.leaf_start();

        self.keys.remove(pos);
        self.leaf_count -= 1;

        if self.leaf_count == 0 {
            self.nodes = vec![ZERO_HASH];
            return;
        }

        let padded = self.leaf_count.next_power_of_two();
        let total_nodes = 2 * padded - 1;
        let new_leaf_start = padded - 1;

        // Collect old leaf hashes, skipping the removed one
        let mut leaf_hashes: Vec<MerkleHash> = Vec::with_capacity(self.leaf_count);
        for i in 0..(self.leaf_count + 1) {
            if i != pos
                && i < (self.leaf_count + 1)
                && old_leaf_start + i < self.nodes.len()
            {
                leaf_hashes.push(self.nodes[old_leaf_start + i]);
            }
        }

        let mut nodes = vec![ZERO_HASH; total_nodes];
        for (i, h) in leaf_hashes.iter().enumerate() {
            if new_leaf_start + i < nodes.len() {
                nodes[new_leaf_start + i] = *h;
            }
        }

        self.nodes = nodes;
        for i in (0..new_leaf_start).rev() {
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            self.nodes[i] = hash_pair(&self.nodes[left], &self.nodes[right]);
        }
    }

    /// Recompute hashes from a leaf up to the root.
    fn recompute_path(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            let left = 2 * parent + 1;
            let right = 2 * parent + 2;
            self.nodes[parent] = hash_pair(&self.nodes[left], &self.nodes[right]);
            idx = parent;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rows(pks: &[(&str, u64, u32)]) -> Vec<RowInfo> {
        pks.iter()
            .map(|(pk, hlc, cnt)| RowInfo {
                primary_key: pk.to_string(),
                hlc_time: *hlc,
                hlc_counter: *cnt,
                node_id: [1u8; 16],
            })
            .collect()
    }

    #[test]
    fn test_build_empty() {
        let tree = MerkleTree::build("t".into(), vec![]);
        assert_eq!(tree.leaf_count, 0);
        assert_eq!(tree.root_hash(), ZERO_HASH);
        assert!(tree.keys.is_empty());
    }

    #[test]
    fn test_build_single_leaf() {
        let rows = make_rows(&[("pk1", 100, 0)]);
        let tree = MerkleTree::build("t".into(), rows);
        assert_eq!(tree.leaf_count, 1);
        assert_ne!(tree.root_hash(), ZERO_HASH);
        assert_eq!(tree.keys, vec!["pk1"]);
    }

    #[test]
    fn test_build_multiple_leaves() {
        let rows = make_rows(&[("a", 100, 0), ("b", 200, 0), ("c", 300, 0)]);
        let tree = MerkleTree::build("t".into(), rows);
        assert_eq!(tree.leaf_count, 3);
        // Padded to 4 leaves -> 7 nodes
        assert_eq!(tree.nodes.len(), 7);
        assert_ne!(tree.root_hash(), ZERO_HASH);
    }

    #[test]
    fn test_root_hash_deterministic() {
        let rows1 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let rows2 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let t1 = MerkleTree::build("t".into(), rows1);
        let t2 = MerkleTree::build("t".into(), rows2);
        assert_eq!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn test_root_hash_changes_with_data() {
        let rows1 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let rows2 = make_rows(&[("a", 100, 0), ("b", 300, 0)]);
        let t1 = MerkleTree::build("t".into(), rows1);
        let t2 = MerkleTree::build("t".into(), rows2);
        assert_ne!(t1.root_hash(), t2.root_hash());
    }

    #[test]
    fn test_diff_same_trees() {
        let rows = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let t1 = MerkleTree::build("t".into(), rows.clone());
        let t2 = MerkleTree::build("t".into(), rows);
        let diff = MerkleTree::diff(&t1, &t2);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_one_leaf_different() {
        let rows1 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let rows2 = make_rows(&[("a", 100, 0), ("b", 999, 0)]);
        let t1 = MerkleTree::build("t".into(), rows1);
        let t2 = MerkleTree::build("t".into(), rows2);
        let diff = MerkleTree::diff(&t1, &t2);
        assert_eq!(diff, vec!["b"]);
    }

    #[test]
    fn test_diff_asymmetric_local_has_more() {
        let rows1 = make_rows(&[("a", 100, 0), ("b", 200, 0), ("c", 300, 0)]);
        let rows2 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let t1 = MerkleTree::build("t".into(), rows1);
        let t2 = MerkleTree::build("t".into(), rows2);
        let diff = MerkleTree::diff(&t1, &t2);
        assert!(diff.contains(&"c".to_string()));
    }

    #[test]
    fn test_diff_asymmetric_remote_has_more() {
        let rows1 = make_rows(&[("a", 100, 0)]);
        let rows2 = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let t1 = MerkleTree::build("t".into(), rows1);
        let t2 = MerkleTree::build("t".into(), rows2);
        let diff = MerkleTree::diff(&t1, &t2);
        assert!(diff.contains(&"b".to_string()));
    }

    #[test]
    fn test_diff_empty_vs_nonempty() {
        let t1 = MerkleTree::build("t".into(), vec![]);
        let t2 = MerkleTree::build("t".into(), make_rows(&[("a", 100, 0)]));
        let diff = MerkleTree::diff(&t1, &t2);
        assert_eq!(diff, vec!["a"]);
    }

    #[test]
    fn test_diff_nonempty_vs_empty() {
        let t1 = MerkleTree::build("t".into(), make_rows(&[("a", 100, 0)]));
        let t2 = MerkleTree::build("t".into(), vec![]);
        let diff = MerkleTree::diff(&t1, &t2);
        assert_eq!(diff, vec!["a"]);
    }

    #[test]
    fn test_update_leaf_existing() {
        let rows = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);
        let old_root = tree.root_hash();

        tree.update_leaf("b", 999, 0, &[1u8; 16]);

        assert_ne!(tree.root_hash(), old_root);
        assert_eq!(tree.leaf_count, 2);
        assert_eq!(tree.keys, vec!["a", "b"]);
    }

    #[test]
    fn test_update_leaf_new_key_inserts() {
        let rows = make_rows(&[("a", 100, 0), ("c", 300, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);

        tree.update_leaf("b", 200, 0, &[1u8; 16]);

        assert_eq!(tree.leaf_count, 3);
        assert_eq!(tree.keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_insert_leaf() {
        let rows = make_rows(&[("a", 100, 0), ("c", 300, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);

        tree.insert_leaf("b", 200, 0, &[1u8; 16]);

        assert_eq!(tree.leaf_count, 3);
        assert_eq!(tree.keys, vec!["a", "b", "c"]);

        // Verify the tree is consistent by rebuilding and comparing
        let rebuilt = MerkleTree::build(
            "t".into(),
            make_rows(&[("a", 100, 0), ("b", 200, 0), ("c", 300, 0)]),
        );
        assert_eq!(tree.root_hash(), rebuilt.root_hash());
    }

    #[test]
    fn test_insert_leaf_into_empty() {
        let mut tree = MerkleTree::build("t".into(), vec![]);
        tree.insert_leaf("a", 100, 0, &[1u8; 16]);

        assert_eq!(tree.leaf_count, 1);
        assert_eq!(tree.keys, vec!["a"]);

        let single = MerkleTree::build("t".into(), make_rows(&[("a", 100, 0)]));
        assert_eq!(tree.root_hash(), single.root_hash());
    }

    #[test]
    fn test_remove_leaf() {
        let rows = make_rows(&[("a", 100, 0), ("b", 200, 0), ("c", 300, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);

        tree.remove_leaf("b");

        assert_eq!(tree.leaf_count, 2);
        assert_eq!(tree.keys, vec!["a", "c"]);

        let rebuilt = MerkleTree::build("t".into(), make_rows(&[("a", 100, 0), ("c", 300, 0)]));
        assert_eq!(tree.root_hash(), rebuilt.root_hash());
    }

    #[test]
    fn test_remove_leaf_to_empty() {
        let rows = make_rows(&[("a", 100, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);

        tree.remove_leaf("a");

        assert_eq!(tree.leaf_count, 0);
        assert_eq!(tree.root_hash(), ZERO_HASH);
    }

    #[test]
    fn test_remove_leaf_nonexistent() {
        let rows = make_rows(&[("a", 100, 0)]);
        let mut tree = MerkleTree::build("t".into(), rows);
        let old_root = tree.root_hash();

        tree.remove_leaf("zzz");

        assert_eq!(tree.root_hash(), old_root);
        assert_eq!(tree.leaf_count, 1);
    }

    #[test]
    fn test_root_summary() {
        let rows = make_rows(&[("a", 100, 0), ("b", 200, 0)]);
        let tree = MerkleTree::build("t".into(), rows);
        let root = tree.root();
        assert_eq!(root.table_name, "t");
        assert_eq!(root.row_count, 2);
        assert_eq!(root.root_hash, tree.root_hash());
    }
}
