//! Browser-side mapping between a Rust struct and the engine's
//! column-bag representation.
//!
//! Most apps will derive this trait via [`SyncEntity`](crate::SyncEntity).
//! The same derive that emits SeaORM glue on native targets emits an
//! `impl BrowserEntity` on wasm — so a single struct definition powers
//! both [`use_synced_table`](crate::dioxus::use_synced_table) and
//! [`WebSyncClient::submit`](crate::WebSyncClient::submit) without
//! parallel mirror types.
//!
//! ## Derive (recommended)
//!
//! ```ignore
//! use wavesyncdb::SyncEntity;
//!
//! #[derive(Clone, Debug, Default, SyncEntity)]
//! pub struct Task {
//!     #[sea_orm(primary_key)]
//!     pub id: String,
//!     pub title: String,
//!     pub done: bool,
//! }
//! ```
//!
//! Each non-PK field must implement `Serialize + DeserializeOwned +
//! Default` — every common SeaORM-friendly field type (primitives,
//! `String`, `Option<T>`, `Vec<u8>`, chrono dates) does.
//!
//! ## Manual impl (escape hatch)
//!
//! If a field needs custom packing (e.g., a non-`Default` type, or a
//! field that shouldn't round-trip through JSON), implement the trait
//! by hand instead of deriving it. Extract each field from the column
//! map in `from_columns`, dump each field back into a `(name, value)`
//! list in `to_columns`, return your primary-key field as a string in
//! `pk`.

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
