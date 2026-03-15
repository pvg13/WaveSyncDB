//! Core sync message types exchanged between nodes.
//!
//! These types are serialized with [`serde_json`] for gossipsub transport and stored
//! in shadow tables for conflict resolution.

use serde::{Deserialize, Serialize};

/// Unique identifier for a node in the sync network.
///
/// A 16-byte array, typically derived from process ID + timestamp at startup,
/// or persisted in `_wavesync_meta` for stable identity across restarts.
/// Used as the final tiebreaker in column-level conflict resolution.
pub type NodeId = [u8; 16];

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

/// Policy for resolving delete vs. non-delete conflicts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum DeletePolicy {
    /// A delete operation wins over a concurrent non-delete (default).
    #[default]
    DeleteWins,
    /// A non-delete operation wins over a concurrent delete.
    AddWins,
}

/// A single column-level change in the CRDT model.
///
/// Each column in a row gets its own Lamport clock (`col_version`).
/// When concurrent edits happen to different columns on the same row,
/// both changes survive because they have independent version histories.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnChange {
    /// Table name this change applies to.
    pub table: String,
    /// Primary key of the affected row (as a string).
    pub pk: String,
    /// Column name, or `"__deleted"` for tombstones.
    pub cid: String,
    /// New value for the column. `None` for deletes.
    pub val: Option<serde_json::Value>,
    /// Originating node's site_id for conflict resolution tiebreaking.
    pub site_id: NodeId,
    /// Per-column Lamport clock version.
    pub col_version: u64,
    /// Causal length — used for delete tracking.
    pub cl: u64,
    /// Ordering within a single `db_version` batch.
    pub seq: u32,
}

/// A batch of column-level changes from a single write operation.
///
/// Produced by `WaveSyncDb` when it intercepts a write, then published
/// to gossipsub and used for version vector sync.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncChangeset {
    /// The originating node's ID.
    pub site_id: NodeId,
    /// The db_version at which this changeset was created.
    pub db_version: u64,
    /// Individual column changes in this batch.
    pub changes: Vec<ColumnChange>,
}

/// Tagged envelope for gossipsub messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncMessage {
    /// A changeset containing column-level CRDT changes.
    Changeset(SyncChangeset),
}

/// Authenticated wrapper for gossipsub messages.
///
/// When a `GroupKey` is configured, the `hmac` field carries a BLAKE3-keyed MAC
/// over the serialized `inner` message. Peers without the matching key cannot
/// forge or verify messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticatedMessage {
    /// The wrapped sync message.
    pub inner: SyncMessage,
    /// HMAC tag (present when group authentication is enabled).
    #[serde(default)]
    pub hmac: Option<[u8; 32]>,
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
    /// Which columns were changed (if known).
    pub changed_columns: Option<Vec<String>>,
}
