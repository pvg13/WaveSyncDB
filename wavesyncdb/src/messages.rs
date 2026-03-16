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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_change_roundtrip_json() {
        let change = ColumnChange {
            table: "tasks".to_string(),
            pk: "pk-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("Hello")),
            site_id: [1u8; 16],
            col_version: 5,
            cl: 5,
            seq: 0,
        };
        let json = serde_json::to_string(&change).unwrap();
        let deserialized: ColumnChange = serde_json::from_str(&json).unwrap();
        assert_eq!(change, deserialized);
    }

    #[test]
    fn test_sync_changeset_roundtrip_json() {
        let changeset = SyncChangeset {
            site_id: [2u8; 16],
            db_version: 42,
            changes: vec![
                ColumnChange {
                    table: "tasks".to_string(),
                    pk: "pk-1".to_string(),
                    cid: "title".to_string(),
                    val: Some(serde_json::json!("First")),
                    site_id: [2u8; 16],
                    col_version: 1,
                    cl: 1,
                    seq: 0,
                },
                ColumnChange {
                    table: "tasks".to_string(),
                    pk: "pk-1".to_string(),
                    cid: "done".to_string(),
                    val: Some(serde_json::json!(false)),
                    site_id: [2u8; 16],
                    col_version: 1,
                    cl: 1,
                    seq: 1,
                },
            ],
        };
        let json = serde_json::to_string(&changeset).unwrap();
        let deserialized: SyncChangeset = serde_json::from_str(&json).unwrap();
        assert_eq!(changeset, deserialized);
    }

    #[test]
    fn test_authenticated_message_roundtrip_json() {
        let msg = AuthenticatedMessage {
            inner: SyncMessage::Changeset(SyncChangeset {
                site_id: [3u8; 16],
                db_version: 1,
                changes: vec![],
            }),
            hmac: Some([0xAB; 32]),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: AuthenticatedMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.hmac, Some([0xAB; 32]));
    }

    #[test]
    fn test_authenticated_message_hmac_none_default() {
        // Simulate old message without hmac field
        let json = r#"{"inner":{"Changeset":{"site_id":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"db_version":1,"changes":[]}}}"#;
        let msg: AuthenticatedMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.hmac, None);
    }

    #[test]
    fn test_write_kind_variants_serialize() {
        let insert_json = serde_json::to_string(&WriteKind::Insert).unwrap();
        let update_json = serde_json::to_string(&WriteKind::Update).unwrap();
        let delete_json = serde_json::to_string(&WriteKind::Delete).unwrap();
        assert_eq!(insert_json, "\"Insert\"");
        assert_eq!(update_json, "\"Update\"");
        assert_eq!(delete_json, "\"Delete\"");
    }

    #[test]
    fn test_delete_policy_default() {
        assert_eq!(DeletePolicy::default(), DeletePolicy::DeleteWins);
    }

    #[test]
    fn test_column_change_deleted_sentinel() {
        let change = ColumnChange {
            table: "tasks".to_string(),
            pk: "pk-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [1u8; 16],
            col_version: 3,
            cl: 3,
            seq: 0,
        };
        assert_eq!(change.cid, "__deleted");
        assert!(change.val.is_none());
    }

    #[test]
    fn test_sync_message_changeset_envelope() {
        let msg = SyncMessage::Changeset(SyncChangeset {
            site_id: [4u8; 16],
            db_version: 10,
            changes: vec![],
        });
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SyncMessage = serde_json::from_str(&json).unwrap();
        match deserialized {
            SyncMessage::Changeset(cs) => {
                assert_eq!(cs.db_version, 10);
                assert_eq!(cs.site_id, [4u8; 16]);
            }
        }
    }

    #[test]
    fn test_change_notification_clone() {
        let notif = ChangeNotification {
            table: "tasks".to_string(),
            kind: WriteKind::Insert,
            primary_key: "pk-1".to_string(),
            changed_columns: Some(vec!["title".to_string()]),
        };
        let cloned = notif.clone();
        assert_eq!(format!("{:?}", notif), format!("{:?}", cloned));
    }

    #[test]
    fn test_node_id_serializes_as_array() {
        let id: NodeId = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]");
        let deserialized: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }
}
