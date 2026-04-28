//! Core sync message types exchanged between nodes.
//!
//! These types are serialized with [`serde_json`] for request-response transport and stored
//! in shadow tables for conflict resolution.

use serde::{Deserialize, Serialize};

// ── Newtypes for domain concepts ──

/// A validated table name (e.g. "tasks", "expenses")
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableName(pub String);

/// The derived topic string used for peer group isolation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicString(pub String);

/// A row primary key value (always string-encoded)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrimaryKey(pub String);

/// A column name within a table
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnName(pub String);

/// An application-level peer identity (opaque string, app-defined)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AppId(pub String);

/// A 32-byte BLAKE3-keyed HMAC tag
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HmacTag(pub [u8; 32]);

/// Unique identifier for a node in the sync network.
///
/// A 16-byte array, typically derived from process ID + timestamp at startup,
/// or persisted in `_wavesync_meta` for stable identity across restarts.
/// Used as the final tiebreaker in column-level conflict resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub [u8; 16]);

// ── From impls for string newtypes ──

macro_rules! impl_string_newtype {
    ($ty:ident) => {
        impl From<String> for $ty {
            fn from(s: String) -> Self {
                Self(s)
            }
        }
        impl From<&str> for $ty {
            fn from(s: &str) -> Self {
                Self(s.to_string())
            }
        }
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        impl PartialEq<str> for $ty {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }
        impl PartialEq<&str> for $ty {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }
        impl PartialEq<String> for $ty {
            fn eq(&self, other: &String) -> bool {
                self.0 == *other
            }
        }
    };
}

impl_string_newtype!(TableName);
impl_string_newtype!(TopicString);
impl_string_newtype!(PrimaryKey);
impl_string_newtype!(ColumnName);
impl_string_newtype!(AppId);

impl std::fmt::Display for HmacTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
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
    pub table: TableName,
    /// Primary key of the affected row (as a string).
    pub pk: PrimaryKey,
    /// Column name, or `"__deleted"` for tombstones.
    pub cid: ColumnName,
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
    /// The db_version at which this change was created.
    /// Used for correct ordering in `get_changes_since`.
    #[serde(default)]
    pub db_version: u64,
}

/// A batch of column-level changes from a single write operation.
///
/// Produced by `WaveSyncDb` when it intercepts a write, then pushed
/// to peers via request-response and used for version vector sync.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncChangeset {
    /// The originating node's ID.
    pub site_id: NodeId,
    /// The db_version at which this changeset was created.
    pub db_version: u64,
    /// Individual column changes in this batch.
    pub changes: Vec<ColumnChange>,
}

/// Lightweight notification emitted after every local or remote write.
///
/// Subscribe via [`WaveSyncDb::change_rx()`](crate::WaveSyncDb::change_rx) to receive
/// these. Used by the Dioxus hooks to trigger reactive UI updates.
#[derive(Debug, Clone)]
pub struct ChangeNotification {
    /// Name of the table that was modified.
    pub table: TableName,
    /// Type of write that occurred.
    pub kind: WriteKind,
    /// Primary key of the affected row.
    pub primary_key: PrimaryKey,
    /// Which columns were changed (if known).
    pub changed_columns: Option<Vec<String>>,
    /// The post-write value of each column that was changed, as JSON.
    ///
    /// `None` for `Delete` and on the rare path where the values
    /// couldn't be captured (e.g. an unparsed raw `execute_unprepared`).
    /// Reactive hooks use this to apply changes in place via
    /// [`SyncedModel`](crate::SyncedModel) without re-querying SeaORM.
    pub column_values: Option<Vec<(ColumnName, serde_json::Value)>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_change_roundtrip_json() {
        let change = ColumnChange {
            table: "tasks".into(),
            pk: "pk-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("Hello")),
            site_id: NodeId([1u8; 16]),
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        };
        let json = serde_json::to_string(&change).unwrap();
        let deserialized: ColumnChange = serde_json::from_str(&json).unwrap();
        assert_eq!(change, deserialized);
    }

    #[test]
    fn test_sync_changeset_roundtrip_json() {
        let changeset = SyncChangeset {
            site_id: NodeId([2u8; 16]),
            db_version: 42,
            changes: vec![
                ColumnChange {
                    table: "tasks".into(),
                    pk: "pk-1".into(),
                    cid: "title".into(),
                    val: Some(serde_json::json!("First")),
                    site_id: NodeId([2u8; 16]),
                    col_version: 1,
                    cl: 1,
                    seq: 0,
                    db_version: 0,
                },
                ColumnChange {
                    table: "tasks".into(),
                    pk: "pk-1".into(),
                    cid: "done".into(),
                    val: Some(serde_json::json!(false)),
                    site_id: NodeId([2u8; 16]),
                    col_version: 1,
                    cl: 1,
                    seq: 1,
                    db_version: 0,
                },
            ],
        };
        let json = serde_json::to_string(&changeset).unwrap();
        let deserialized: SyncChangeset = serde_json::from_str(&json).unwrap();
        assert_eq!(changeset, deserialized);
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
            table: "tasks".into(),
            pk: "pk-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([1u8; 16]),
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        };
        assert_eq!(change.cid, "__deleted");
        assert!(change.val.is_none());
    }

    #[test]
    fn test_change_notification_clone() {
        let notif = ChangeNotification {
            table: "tasks".into(),
            kind: WriteKind::Insert,
            primary_key: "pk-1".into(),
            changed_columns: Some(vec!["title".to_string()]),
            column_values: None,
        };
        let cloned = notif.clone();
        assert_eq!(format!("{:?}", notif), format!("{:?}", cloned));
    }

    #[test]
    fn test_node_id_serializes_as_array() {
        let id = NodeId([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]");
        let deserialized: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_table_name_display() {
        let t = TableName::from("tasks");
        assert_eq!(t.to_string(), "tasks");
        assert_eq!(t, "tasks");
    }

    #[test]
    fn test_primary_key_from_string() {
        let pk = PrimaryKey::from("abc-123".to_string());
        assert_eq!(pk.0, "abc-123");
    }

    #[test]
    fn test_node_id_copy() {
        let a = NodeId([1u8; 16]);
        let b = a; // Copy
        assert_eq!(a, b);
    }
}
