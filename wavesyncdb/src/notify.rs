//! User-facing sync notifications.
//!
//! A per-table, type-safe policy that decides whether an *incoming remote*
//! change should surface an end-user notification (the WhatsApp-style "you got
//! new data" experience). The policy is declared on the entity type via
//! `#[derive(SyncNotify)]` + `impl SyncNotify`, so there is no stringly-typed
//! table matching in application code, and it is checked at compile time.
//!
//! ```ignore
//! #[derive(DeriveEntityModel, SyncEntity, SyncNotify)]
//! #[sea_orm(table_name = "messages")]
//! pub struct Model { pub id: String, pub text: String, pub chat_id: String }
//!
//! impl wavesyncdb::SyncNotify for message::Model {
//!     fn on_sync(ev: &SyncEvent<Self>) -> Option<Notification> {
//!         match ev.op {
//!             WriteKind::Insert => ev.row.as_ref().map(|m| {
//!                 Notification::new("New message", &m.text).group(format!("chat:{}", m.chat_id))
//!             }),
//!             _ => None, // edits / deletes stay silent
//!         }
//!     }
//! }
//! ```
//!
//! Native-only: the dispatch runs in the engine's remote-apply path, which does
//! not exist on the wasm32 web target. A web story is a follow-up.

use crate::messages::{ChangeNotification, ChangeSource, WriteKind};
use crate::synced_model::SyncedModel;

/// A remote change handed to an entity's notification policy.
///
/// `M` is the entity's SeaORM model.
pub struct SyncEvent<'a, M> {
    /// The operation that was applied.
    pub op: WriteKind,
    /// Primary key of the affected row, string-encoded.
    pub primary_key: &'a str,
    /// Where the change came from. Always [`ChangeSource::Remote`] here.
    pub source: ChangeSource,
    /// Column names that changed, when known.
    pub changed_columns: Option<&'a [String]>,
    /// The affected row, reconstructed from the change when possible.
    ///
    /// `Some` for inserts (all columns are present). `None` for deletes (the
    /// row is gone) and for partial updates where not every non-`Option` column
    /// was in the change. Policies needing full data on an update should
    /// re-query lazily in their notification consumer rather than block the
    /// sync engine.
    pub row: Option<M>,
}

/// A user-facing notification produced by a [`SyncNotify`] policy.
///
/// Delivered on [`WaveSyncDb::notification_rx`](crate::WaveSyncDb::notification_rx)
/// and rendered per-platform by the `use_sync_notifications` Dioxus hook.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Table the change belongs to (filled in by the library).
    pub table: String,
    /// Primary key of the affected row (filled in by the library).
    pub primary_key: String,
    /// The operation that triggered this notification (filled in by the library).
    pub op: WriteKind,
    /// Short headline shown to the user.
    pub title: String,
    /// Body text shown to the user.
    pub body: String,
    /// Optional grouping/coalescing key. Notifications sharing a key within a
    /// short window are collapsed (e.g. `"chat:42"` → one "new messages"
    /// notification instead of many). When `None`, `table:primary_key` is used.
    pub coalesce_key: Option<String>,
}

impl Notification {
    /// Build a notification with a title and body. The `table`, `primary_key`,
    /// and `op` fields are filled in by the library from the originating change.
    pub fn new(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            table: String::new(),
            primary_key: String::new(),
            op: WriteKind::Insert,
            title: title.into(),
            body: body.into(),
            coalesce_key: None,
        }
    }

    /// Set the coalescing/grouping key (see [`Notification::coalesce_key`]).
    pub fn group(mut self, key: impl Into<String>) -> Self {
        self.coalesce_key = Some(key.into());
        self
    }
}

/// Per-table notification policy. Implement this on a synced entity's model and
/// add `#[derive(SyncNotify)]` to register it; the library then calls
/// [`on_sync`](SyncNotify::on_sync) for every *remote* change to that table.
///
/// The policy is pure and synchronous: return `Some` to surface a notification,
/// `None` to stay silent. It is never called for the user's own local writes.
pub trait SyncNotify: SyncedModel + Sized {
    /// Decide whether a remote change should produce a user notification.
    fn on_sync(ev: &SyncEvent<Self>) -> Option<Notification>;
}

/// Type-erased per-table dispatch closure stored in the
/// [`NotificationRegistry`](crate::registry::NotificationRegistry).
pub type NotifyDispatch = Box<dyn Fn(&ChangeNotification) -> Option<Notification> + Send + Sync>;

/// Build the dispatch closure for a concrete entity type `M`. Called by the
/// `#[derive(SyncNotify)]`-generated registration.
///
/// The closure reconstructs the typed row from the change's column values via
/// [`SyncedModel::wavesync_from_changes`] (full only when all required columns
/// are present), assembles a [`SyncEvent`], invokes `M::on_sync`, and stamps
/// `table`/`primary_key`/`op` onto any returned [`Notification`] so the policy
/// doesn't have to. As a defensive guard it returns `None` for non-remote
/// changes even though the call site only invokes it on the remote path.
pub fn make_dispatch<M: SyncNotify + 'static>() -> NotifyDispatch {
    Box::new(move |cn: &ChangeNotification| {
        if !matches!(cn.source, ChangeSource::Remote { .. }) {
            return None;
        }
        let row = cn.column_values.as_ref().and_then(|cv| {
            let pairs: Vec<(String, serde_json::Value)> =
                cv.iter().map(|(c, v)| (c.0.clone(), v.clone())).collect();
            M::wavesync_from_changes("", &cn.primary_key.0, &pairs)
        });
        let ev = SyncEvent {
            op: cn.kind.clone(),
            primary_key: &cn.primary_key.0,
            source: cn.source,
            changed_columns: cn.changed_columns.as_deref(),
            row,
        };
        M::on_sync(&ev).map(|mut n| {
            n.table = cn.table.0.clone();
            n.primary_key = cn.primary_key.0.clone();
            n.op = cn.kind.clone();
            n
        })
    })
}

/// Link-time registration entry emitted by `#[derive(SyncNotify)]`.
///
/// Collected via [`inventory`] and consumed at `WaveSyncDb` build time to
/// populate the [`NotificationRegistry`](crate::registry::NotificationRegistry).
/// Mirrors [`SyncEntityInfo`](crate::registry::SyncEntityInfo).
pub struct NotifyEntityInfo {
    /// `module_path!()` of the entity (for optional crate-prefix scoping).
    pub module_path: &'static str,
    /// Produces `(table_name, dispatch_closure)` for this entity.
    pub make: fn() -> (String, NotifyDispatch),
}

inventory::collect!(NotifyEntityInfo);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ChangeNotification, ColumnName, NodeId, PrimaryKey};

    #[derive(Clone)]
    struct TestMsg {
        id: String,
        text: String,
    }

    // Minimal hand-written SyncedModel so we don't need the SeaORM derive in a
    // unit test. `id` and `text` are both required (non-Option).
    impl SyncedModel for TestMsg {
        fn wavesync_apply_change(&mut self, column: &str, value: &serde_json::Value) {
            match column {
                "id" => {
                    if let Ok(v) = serde_json::from_value(value.clone()) {
                        self.id = v;
                    }
                }
                "text" => {
                    if let Ok(v) = serde_json::from_value(value.clone()) {
                        self.text = v;
                    }
                }
                _ => {}
            }
        }

        fn wavesync_from_changes(
            _pk_column: &str,
            pk_value: &str,
            changes: &[(String, serde_json::Value)],
        ) -> Option<Self> {
            let mut id: Option<String> = None;
            let mut text: Option<String> = None;
            for (c, v) in changes {
                match c.as_str() {
                    "id" => id = serde_json::from_value(v.clone()).ok(),
                    "text" => text = serde_json::from_value(v.clone()).ok(),
                    _ => {}
                }
            }
            if id.is_none() {
                id = Some(pk_value.to_string());
            }
            Some(TestMsg {
                id: id?,
                text: text?,
            })
        }

        fn wavesync_pk_string(&self) -> String {
            self.id.clone()
        }
    }

    // Policy that surfaces whenever a typed row is available — lets us assert
    // exactly when row reconstruction succeeds.
    impl SyncNotify for TestMsg {
        fn on_sync(ev: &SyncEvent<Self>) -> Option<Notification> {
            ev.row
                .as_ref()
                .map(|m| Notification::new("changed", &m.text))
        }
    }

    fn change(
        kind: WriteKind,
        source: ChangeSource,
        cols: Option<&[(&str, &str)]>,
    ) -> ChangeNotification {
        ChangeNotification {
            table: "msgs".into(),
            kind,
            source,
            primary_key: PrimaryKey("m1".into()),
            changed_columns: cols.map(|c| c.iter().map(|(k, _)| k.to_string()).collect()),
            column_values: cols.map(|c| {
                c.iter()
                    .map(|(k, v)| {
                        (
                            ColumnName(k.to_string()),
                            serde_json::Value::String(v.to_string()),
                        )
                    })
                    .collect()
            }),
        }
    }

    fn remote() -> ChangeSource {
        ChangeSource::Remote {
            peer_site: NodeId([9u8; 16]),
        }
    }

    #[test]
    fn insert_with_full_columns_yields_typed_row_and_stamps_fields() {
        let dispatch = make_dispatch::<TestMsg>();
        let cn = change(
            WriteKind::Insert,
            remote(),
            Some(&[("id", "m1"), ("text", "hello")]),
        );
        let out = dispatch(&cn).expect("full insert should notify");
        assert_eq!(out.title, "changed");
        assert_eq!(out.body, "hello"); // proves the typed row was reconstructed
        assert_eq!(out.table, "msgs"); // stamped by make_dispatch
        assert_eq!(out.primary_key, "m1");
        assert_eq!(out.op, WriteKind::Insert);
    }

    #[test]
    fn partial_update_missing_required_column_has_no_row() {
        let dispatch = make_dispatch::<TestMsg>();
        // Only "id" present → "text" is missing → row cannot be reconstructed.
        let cn = change(WriteKind::Update, remote(), Some(&[("id", "m1")]));
        assert!(dispatch(&cn).is_none());
    }

    #[test]
    fn delete_has_no_row() {
        let dispatch = make_dispatch::<TestMsg>();
        let cn = change(WriteKind::Delete, remote(), None);
        assert!(dispatch(&cn).is_none());
    }

    #[test]
    fn local_changes_never_dispatch() {
        let dispatch = make_dispatch::<TestMsg>();
        let cn = change(
            WriteKind::Insert,
            ChangeSource::Local,
            Some(&[("id", "m1"), ("text", "hi")]),
        );
        assert!(dispatch(&cn).is_none(), "local writes must never notify");
    }
}
