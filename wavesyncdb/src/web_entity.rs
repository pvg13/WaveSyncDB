//! Browser-side mapping between a Rust struct and the engine's
//! column-bag representation.
//!
//! On native, the [`SyncEntity`](crate::SyncEntity) derive macro lives
//! on top of SeaORM and tells the engine how to serialize / deserialize
//! a row. The browser path has no SeaORM, so this trait is the explicit
//! contract apps implement to plug their domain types into
//! [`use_synced_table`](crate::dioxus::use_synced_table) and
//! [`WebSyncClient::submit`](crate::WebSyncClient::submit).
//!
//! Implementing it is mechanical — extract each field from the column
//! map in `from_columns`, dump each field back into a `(name, value)`
//! list in `to_columns`, return your primary-key field as a string in
//! `pk`. There's no derive macro yet; with patterns on a few apps it
//! could be added.
//!
//! ## Example
//!
//! ```ignore
//! use std::collections::HashMap;
//! use serde_json::Value;
//! use wavesyncdb::BrowserEntity;
//!
//! #[derive(Clone, Debug)]
//! struct Task { id: String, title: String, done: bool }
//!
//! impl BrowserEntity for Task {
//!     fn from_columns(pk: &str, cols: &HashMap<String, Value>) -> Self {
//!         Task {
//!             id: pk.to_string(),
//!             title: cols.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string(),
//!             done: cols.get("done").and_then(|v| v.as_bool()).unwrap_or(false),
//!         }
//!     }
//!     fn to_columns(&self) -> Vec<(String, Value)> {
//!         vec![
//!             ("title".into(), Value::String(self.title.clone())),
//!             ("done".into(), Value::Bool(self.done)),
//!         ]
//!     }
//!     fn pk(&self) -> String { self.id.clone() }
//! }
//! ```

use std::collections::HashMap;

use serde_json::Value;

/// Bidirectional mapping between an app entity and the engine's
/// column-bag wire shape.
///
/// `'static` is required because Dioxus signals carry the type around
/// across spawned futures; `Clone` is required because materialized
/// entities are cloned out of the signal for rendering.
pub trait BrowserEntity: Clone + 'static {
    /// Build an instance from the persisted column map.
    ///
    /// `pk` is passed separately because the engine stores primary keys
    /// outside the column bag (in the shadow row's key). Implementations
    /// usually copy `pk` into the entity's id field and pull the rest
    /// from `cols`.
    fn from_columns(pk: &str, cols: &HashMap<String, Value>) -> Self;

    /// Serialize the entity into the column-bag shape `submit_local_write`
    /// expects. The primary key is **not** included here — the caller
    /// passes [`Self::pk`] separately.
    fn to_columns(&self) -> Vec<(String, Value)>;

    /// Return the primary-key value as a string. Used as the shadow
    /// table key and to identify the row across peers.
    fn pk(&self) -> String;
}
