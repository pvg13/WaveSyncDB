//! Trait that lets reactive hooks update a model in place from a
//! [`ChangeNotification`](crate::ChangeNotification)'s `column_values`
//! payload — eliminating the per-notification SeaORM round-trip the
//! Dioxus hooks used to issue.
//!
//! The trait is auto-implemented for every entity that derives
//! [`SyncEntity`](crate::SyncEntity). Manual implementations are also
//! permitted when a struct is registered via `register_local`.

/// `serde_json::from_value` with SQLite-spelling tolerance.
///
/// Values that were read through SQLite's `json_object()` — the capture
/// triggers, the catch-up JOIN, and remote full-row reads — spell booleans
/// as `0`/`1` integers, while values serialized from Rust models spell them
/// `true`/`false`. Strict decoding is tried first; on failure the two
/// spellings are bridged in both directions so a `bool` field accepts `0`/`1`
/// (including through `Option<bool>`) and a numeric field accepts a stray
/// JSON boolean from an older peer.
pub fn lenient_from_value<T: serde::de::DeserializeOwned>(v: &serde_json::Value) -> Option<T> {
    if let Ok(t) = serde_json::from_value::<T>(v.clone()) {
        return Some(t);
    }
    match v {
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i @ (0 | 1)) => serde_json::from_value::<T>(serde_json::Value::Bool(i == 1)).ok(),
            _ => None,
        },
        serde_json::Value::Bool(b) => {
            serde_json::from_value::<T>(serde_json::Value::Number((*b as i64).into())).ok()
        }
        _ => None,
    }
}

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

#[cfg(test)]
mod tests {
    use super::lenient_from_value;
    use serde_json::json;

    #[test]
    fn strict_decode_wins() {
        assert_eq!(lenient_from_value::<bool>(&json!(true)), Some(true));
        assert_eq!(lenient_from_value::<i64>(&json!(7)), Some(7));
        assert_eq!(lenient_from_value::<String>(&json!("x")), Some("x".into()));
        assert_eq!(lenient_from_value::<f64>(&json!(2.5)), Some(2.5));
    }

    #[test]
    fn sqlite_bool_spelling_bridges_to_bool() {
        assert_eq!(lenient_from_value::<bool>(&json!(1)), Some(true));
        assert_eq!(lenient_from_value::<bool>(&json!(0)), Some(false));
        assert_eq!(
            lenient_from_value::<Option<bool>>(&json!(1)),
            Some(Some(true))
        );
        // 2 is not a boolean spelling — must NOT coerce.
        assert_eq!(lenient_from_value::<bool>(&json!(2)), None);
    }

    #[test]
    fn model_bool_spelling_bridges_to_number() {
        assert_eq!(lenient_from_value::<i64>(&json!(true)), Some(1));
        assert_eq!(lenient_from_value::<i64>(&json!(false)), Some(0));
    }

    #[test]
    fn integers_still_decode_as_integers_first() {
        // 0/1 must stay numeric when the target IS numeric — the strict
        // pass handles it before any coercion runs.
        assert_eq!(lenient_from_value::<i64>(&json!(1)), Some(1));
        assert_eq!(lenient_from_value::<u8>(&json!(0)), Some(0));
    }

    #[test]
    fn null_and_mismatches_fail_cleanly() {
        assert_eq!(lenient_from_value::<bool>(&serde_json::Value::Null), None);
        assert_eq!(
            lenient_from_value::<Option<String>>(&serde_json::Value::Null),
            Some(None)
        );
        assert_eq!(lenient_from_value::<bool>(&json!("yes")), None);
    }
}
