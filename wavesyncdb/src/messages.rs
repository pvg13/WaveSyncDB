//! Core sync message types exchanged between nodes.
//!
//! These types are serialized with [`postcard`] for gossipsub transport and stored
//! in the persistent `_wavesync_log` table for conflict resolution and replay.

use serde::{Deserialize, Serialize};

/// Unique identifier for a node in the sync network.
///
/// A 16-byte array, typically derived from process ID + timestamp at startup.
/// Used as the final tiebreaker in LWW conflict resolution when HLC time and
/// counter are equal.
pub type NodeId = [u8; 16];

/// A single write operation to be replicated across peers.
///
/// Created by [`WaveSyncDb`](crate::WaveSyncDb) when it intercepts a write,
/// then serialized and published to gossipsub. Remote nodes deserialize it,
/// check LWW conflict resolution, and apply if it wins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncOperation {
    /// Unique operation identifier (random, used for gossipsub deduplication).
    pub op_id: u128,
    /// HLC physical time component (nanoseconds since epoch).
    pub hlc_time: u64,
    /// HLC logical counter — incremented when wall-clock time hasn't advanced.
    pub hlc_counter: u32,
    /// ID of the node that originated this operation.
    pub node_id: NodeId,
    /// Name of the table being written to.
    pub table: String,
    /// Type of write operation.
    pub kind: WriteKind,
    /// Primary key of the affected row (as a string).
    pub primary_key: String,
    /// Serialized SQL statement with inlined parameter values.
    /// Remote nodes execute this directly against their local database.
    pub data: Option<Vec<u8>>,
    /// Column names involved in the operation.
    pub columns: Option<Vec<String>>,
}

/// The type of write operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WriteKind {
    /// A new row was inserted.
    Insert,
    /// An existing row was updated.
    Update,
    /// A row was deleted.
    Delete,
}

/// Lightweight notification emitted after every local or remote write.
///
/// Subscribe via [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx) to receive
/// these. Used by the Dioxus hooks to trigger reactive UI updates.
#[derive(Debug, Clone)]
pub struct ChangeNotification {
    /// Name of the table that was modified.
    pub table: String,
    /// Type of write that occurred.
    pub kind: WriteKind,
    /// Primary key of the affected row.
    pub primary_key: String,
}
