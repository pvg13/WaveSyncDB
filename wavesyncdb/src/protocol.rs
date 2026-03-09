//! Full sync protocol types (work in progress).
//!
//! These types define the request/response messages for bulk synchronization
//! between peers. Currently gossipsub handles real-time replication; these
//! types will be used for initial sync when a new node joins the network
//! or when a returning node needs to catch up.

use serde::{Deserialize, Serialize};

use crate::merkle::{MerkleRoot, MerkleTree};
use crate::messages::SyncOperation;

/// A request for the latest op(s) for a specific table row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowOpRequest {
    pub table_name: String,
    pub primary_key: String,
}

/// A sync request sent by a node that needs to catch up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    /// New node requesting a full snapshot of all tables.
    FullSync,
    /// Request root hashes for all tables (Merkle sync round 1).
    MerkleRoots,
    /// Request full Merkle trees for specific tables (round 2).
    MerkleTrees { table_names: Vec<String> },
    /// Request latest ops for specific rows (round 3).
    /// `initiator_ops` carries the initiator's own ops for the differing rows,
    /// enabling bidirectional sync in a single handshake.
    MerkleRowOps {
        requests: Vec<RowOpRequest>,
        initiator_ops: Vec<SyncOperation>,
    },
}

/// A serialized snapshot of a single table's contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshot {
    /// The table name.
    pub table_name: String,
    /// Column names in order.
    pub columns: Vec<String>,
    /// Each entry is a row of column values (in the same order as `columns`).
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// The response to a [`SyncRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncResponse {
    /// Full snapshot: all table data plus recent operations for replay.
    FullSnapshot {
        /// Snapshot of every registered table.
        tables: Vec<TableSnapshot>,
        /// Recent operations to replay after loading the snapshot.
        recent_ops: Vec<SyncOperation>,
        /// The responder's current HLC time (use as baseline for future incremental syncs).
        current_hlc: u64,
    },
    /// Response with root hashes for all tables (Merkle sync round 1).
    MerkleRootsResponse {
        roots: Vec<MerkleRoot>,
        current_hlc: u64,
    },
    /// Response with full Merkle trees for requested tables (round 2).
    MerkleTreesResponse { trees: Vec<MerkleTree> },
    /// Response with latest ops for requested rows (round 3).
    MerkleRowOpsResponse { ops: Vec<SyncOperation> },
}
