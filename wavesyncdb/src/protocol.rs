//! Sync protocol types for version vector based synchronization.
//!
//! Peers exchange version vectors to determine what changes they need.
//! A single round trip suffices: "I have db_version X, last I heard you were at Y"
//! → peer responds with all changes since Y.

use serde::{Deserialize, Serialize};

use crate::messages::{ColumnChange, NodeId};

/// A sync request sent by a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncRequest {
    /// Version vector sync: "I have db_version X, last I heard you were at Y."
    VersionVector {
        /// The requesting peer's current db_version.
        my_db_version: u64,
        /// The last db_version this peer received from the responder (0 = first sync).
        your_last_db_version: u64,
        /// The requesting peer's site_id.
        site_id: NodeId,
        /// The gossipsub topic name — requests with a mismatched topic are rejected.
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
}

/// The response to a [`SyncRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncResponse {
    /// Response with all changes since the requested db_version.
    ChangesetResponse {
        /// Column-level changes since `your_last_db_version`.
        changes: Vec<ColumnChange>,
        /// The responder's current db_version.
        my_db_version: u64,
        /// Echo back the requester's db_version so they can update our peer version.
        your_last_db_version: u64,
        /// The responder's site_id.
        site_id: NodeId,
        /// The gossipsub topic name — responses with a mismatched topic are ignored.
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
}
