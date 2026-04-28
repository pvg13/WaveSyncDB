//! Trait that lets reactive hooks update a model in place from a
//! [`ChangeNotification`](crate::ChangeNotification)'s `column_values`
//! payload — eliminating the per-notification SeaORM round-trip the
//! Dioxus hooks used to issue.
//!
//! The trait is auto-implemented for every entity that derives
//! [`SyncEntity`](crate::SyncEntity). Manual implementations are also
//! permitted when a struct is registered via `register_local`.

/// In-place update + reconstruction of a SeaORM model from JSON column data.
pub trait SyncedModel: Sized {
    /// Apply a single column change to an existing model.
    ///
    /// `column` must be the same column name used by SeaORM's column iterator
    /// (i.e. snake_case field name). Unknown columns are silently ignored —
    /// the macro emits an exhaustive match over the entity's fields.
    fn wavesync_apply_change(&mut self, column: &str, value: &serde_json::Value);

    /// Construct a fresh model from a primary key + a complete set of column
    /// changes. Returns `None` if any non-Option field is missing — caller
    /// should fall back to a SeaORM query in that case.
    ///
    /// `pk_value` is the stringified primary key as carried in
    /// [`ChangeNotification::primary_key`](crate::ChangeNotification). If the
    /// pk is also present in `changes`, the value from `changes` wins.
    fn wavesync_from_changes(
        pk_column: &str,
        pk_value: &str,
        changes: &[(String, serde_json::Value)],
    ) -> Option<Self>;

    /// Stringify the primary key field of this model the same way the SQL
    /// parser does — i.e. plain `Display`. Used by `use_synced_table` to
    /// locate the row to update inside its in-memory `Vec`.
    fn wavesync_pk_string(&self) -> String;
}
