//! Full sync protocol types (work in progress).
//!
//! These types define the request/response messages for bulk synchronization
//! between peers. Currently gossipsub handles real-time replication; these
//! types will be used for initial sync when a new node joins the network
//! or when a returning node needs to catch up.

use serde::{Deserialize, Serialize};

use crate::messages::SyncOperation;

/// A sync request sent by a node that needs to catch up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    /// New node requesting a full snapshot of all tables.
    FullSync,
    /// Returning node requesting operations since a known HLC timestamp.
    IncrementalSync {
        /// The last HLC timestamp this node has seen.
        since_hlc: u64,
    },
}

/// A serialized snapshot of a single table's contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSnapshot {
    /// The table name.
    pub table_name: String,
    /// Column names in order.
    pub columns: Vec<String>,
    /// Each entry is a serialized row (column values in the same order as `columns`).
    pub rows: Vec<Vec<u8>>,
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
    /// Incremental: only the operations since the requested HLC timestamp.
    IncrementalOps {
        /// Operations to replay.
        ops: Vec<SyncOperation>,
        /// The responder's current HLC time.
        current_hlc: u64,
    },
}
