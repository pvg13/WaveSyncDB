//! Sync protocol types for version vector based synchronization.
//!
//! Peers exchange version vectors to determine what changes they need.
//! A single round trip suffices: "I have db_version X, last I heard you were at Y"
//! → peer responds with all changes since Y.

use serde::{Deserialize, Serialize};

use crate::messages::{ColumnChange, NodeId, SyncChangeset};

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
        /// The sync topic name — requests with a mismatched topic are rejected.
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// Push a changeset directly to a peer (fan-out from local writes).
    Push {
        /// The changeset to apply.
        changeset: SyncChangeset,
        /// The sync topic name — requests with a mismatched topic are rejected.
        topic: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// Announce application-level identity to a verified peer.
    IdentityAnnounce {
        /// Opaque application-defined identity string.
        app_id: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// Convergence-verification digest (#82). Carries a value-inclusive digest
    /// of the sender's group state so the responder can *prove* whether the two
    /// peers hold identical data. Additive: an older peer that can't decode this
    /// variant fails the request cleanly and the sender falls back to the
    /// version-vector catch-up.
    ReconcileDigest {
        /// The sender's value-inclusive group digest (see `engine::reconcile`).
        digest: [u8; 32],
        /// The sync topic — selects the group + key (Rule 2.8).
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (Rule 2.7).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// One round of recursive range-based set reconciliation (#82). Carries a
    /// list of range entries covering the keyspace; the responder compares,
    /// then drops converged ranges, splits large mismatching ones, and
    /// itemizes / transfers small ones (see [`crate::engine::reconcile`]). Sent
    /// after a `ReconcileDigest` shows global divergence; the exchange recurses
    /// over `ReconcileRangeResult` until empty. Additive: an older/web peer
    /// that can't decode it fails cleanly and the sender falls back to the
    /// version-vector catch-up.
    ReconcileRange {
        /// Range entries covering the keyspace (sorted by upper bound).
        entries: Vec<RangeEntry>,
        /// The sender's site_id (used as `peer_site` when the responder applies
        /// changes carried in `IdList`/`Transfer` entries).
        site_id: NodeId,
        /// The sync topic — selects the group + key (Rule 2.8).
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (Rule 2.7).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
}

/// One entry in a reconciliation message: covers the half-open key range
/// `[lower, upper)`. Bounds are carried explicitly (not threaded positionally)
/// so each entry maps independently — converged ranges can be dropped from a
/// reply without breaking the bounds of the ranges that remain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeEntry {
    /// Inclusive lower bound of this range as a sort key; `None` = −∞.
    #[serde(default)]
    pub lower: Option<Vec<u8>>,
    /// Exclusive upper bound of this range as a sort key; `None` = +∞.
    pub upper: Option<Vec<u8>>,
    /// What the sender knows about this range.
    pub payload: RangePayload,
}

/// The sender's knowledge of a reconciliation range. The recursion exchanges
/// these to converge while transferring only the symmetric difference:
/// `Fingerprint` (compare) → `IdList` (identities of a small range) →
/// `Resolve` (cells you lack + keys I want) → `Transfer` (the requested cells).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RangePayload {
    /// XOR of the sender's cell fingerprints in the range. The receiver
    /// compares to its own: equal ⇒ converged; unequal ⇒ split or itemize.
    Fingerprint([u8; 32]),
    /// Identities (key + fingerprint, no values) of every cell the sender has
    /// in this (small) range, so the receiver can compute the exact diff.
    IdList(Vec<RangeItem>),
    /// Reply to an `IdList`: the cells the `IdList` sender was missing
    /// (`transfer`, applied directly) plus the keys the replier is missing
    /// (`request`, which the `IdList` sender returns as a `Transfer`).
    Resolve {
        transfer: Vec<ColumnChange>,
        request: Vec<Vec<u8>>,
    },
    /// Terminal: the cells the peer requested (reply to `Resolve`'s `request`).
    /// Applied directly; no further reply.
    Transfer(Vec<ColumnChange>),
}

/// A single cell identity in an [`RangePayload::IdList`]: sort key + content
/// fingerprint (no value — values move only in `Resolve`/`Transfer`, and only
/// for cells the peer actually lacks).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeItem {
    /// Sort key (`table\0pk\0cid`).
    pub key: Vec<u8>,
    /// Content fingerprint (includes the value).
    pub fp: [u8; 32],
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
        /// The sync topic name — responses with a mismatched topic are ignored.
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (present when a passphrase is configured).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// Acknowledgement for a [`SyncRequest::Push`].
    PushAck,
    /// Acknowledgement for a [`SyncRequest::IdentityAnnounce`].
    IdentityAck,
    /// Response to [`SyncRequest::ReconcileDigest`] (#82): whether the
    /// responder's digest matched the requester's (peers are proven converged),
    /// plus the responder's own digest for logging.
    ReconcileResult {
        /// True iff the responder's group digest equals the requester's.
        converged: bool,
        /// The responder's value-inclusive group digest.
        digest: [u8; 32],
        /// The sync topic — selects the group + key (Rule 2.8).
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (Rule 2.7).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
    /// Response to [`SyncRequest::ReconcileRange`] (#82): the responder's reply
    /// range entries (converged ranges dropped, large ones split, small ones
    /// itemized/transferred). The requester processes these, applies any
    /// transferred changes, and — if the reply is non-empty — sends another
    /// `ReconcileRange` round until the exchange is empty.
    ReconcileRangeResult {
        /// Reply range entries (empty ⇒ fully converged, exchange done).
        entries: Vec<RangeEntry>,
        /// The responder's site_id (used as `peer_site` when applying transfers).
        site_id: NodeId,
        /// The sync topic — selects the group + key (Rule 2.8).
        #[serde(default)]
        topic: String,
        /// HMAC tag for group authentication (Rule 2.7).
        #[serde(default)]
        hmac: Option<[u8; 32]>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::NodeId;

    #[test]
    fn test_sync_request_version_vector_roundtrip() {
        let req = SyncRequest::VersionVector {
            my_db_version: 10,
            your_last_db_version: 5,
            site_id: NodeId([1u8; 16]),
            topic: "my-topic".to_string(),
            hmac: Some([0xAB; 32]),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: SyncRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncRequest::VersionVector {
                my_db_version,
                your_last_db_version,
                topic,
                hmac,
                ..
            } => {
                assert_eq!(my_db_version, 10);
                assert_eq!(your_last_db_version, 5);
                assert_eq!(topic, "my-topic");
                assert_eq!(hmac, Some([0xAB; 32]));
            }
            _ => panic!("Expected VersionVector"),
        }
    }

    #[test]
    fn test_sync_response_changeset_response_roundtrip() {
        let resp = SyncResponse::ChangesetResponse {
            changes: vec![ColumnChange {
                table: "tasks".into(),
                pk: "pk-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Hello")),
                site_id: NodeId([2u8; 16]),
                col_version: 3,
                cl: 3,
                seq: 0,
                db_version: 0,
            }],
            my_db_version: 20,
            your_last_db_version: 10,
            site_id: NodeId([2u8; 16]),
            topic: "test".to_string(),
            hmac: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: SyncResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncResponse::ChangesetResponse {
                changes,
                my_db_version,
                ..
            } => {
                assert_eq!(changes.len(), 1);
                assert_eq!(my_db_version, 20);
            }
            _ => panic!("Expected ChangesetResponse"),
        }
    }

    #[test]
    fn test_sync_request_missing_topic_defaults() {
        // Simulate old-format request without topic/hmac fields (N5)
        let json = r#"{"VersionVector":{"my_db_version":5,"your_last_db_version":0,"site_id":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]}}"#;
        let req: SyncRequest = serde_json::from_str(json).unwrap();
        match req {
            SyncRequest::VersionVector { topic, hmac, .. } => {
                assert_eq!(topic, "", "Missing topic should default to empty string");
                assert_eq!(hmac, None, "Missing hmac should default to None");
            }
            _ => panic!("Expected VersionVector"),
        }
    }

    #[test]
    fn test_sync_response_empty_changes() {
        let resp = SyncResponse::ChangesetResponse {
            changes: vec![],
            my_db_version: 0,
            your_last_db_version: 0,
            site_id: NodeId([0u8; 16]),
            topic: String::new(),
            hmac: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: SyncResponse = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncResponse::ChangesetResponse { changes, .. } => {
                assert!(changes.is_empty());
            }
            _ => panic!("Expected ChangesetResponse"),
        }
    }

    #[test]
    fn test_sync_request_push_roundtrip() {
        use crate::messages::SyncChangeset;
        let req = SyncRequest::Push {
            changeset: SyncChangeset {
                site_id: NodeId([5u8; 16]),
                db_version: 7,
                changes: vec![],
            },
            topic: "push-topic".to_string(),
            hmac: Some([0xCD; 32]),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deserialized: SyncRequest = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncRequest::Push {
                changeset,
                topic,
                hmac,
            } => {
                assert_eq!(changeset.db_version, 7);
                assert_eq!(topic, "push-topic");
                assert_eq!(hmac, Some([0xCD; 32]));
            }
            _ => panic!("Expected Push"),
        }
    }

    #[test]
    fn test_sync_response_push_ack_roundtrip() {
        let resp = SyncResponse::PushAck;
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: SyncResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, SyncResponse::PushAck));
    }
}
