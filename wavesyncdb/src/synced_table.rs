//! Cross-target metadata trait that lets [`dioxus::use_synced_table`]
//! and [`dioxus::SyncHandle`] hide native-vs-web differences behind a
//! single signature.
//!
//! Users almost always derive this via `#[derive(SyncEntity)]` — the
//! same derive that emits `impl SyncedModel` on native and
//! `impl BrowserEntity` on wasm32 also emits `impl SyncedTableEntity`
//! on both targets so the same struct works in shared UI components.
//!
//! [`dioxus::use_synced_table`]: crate::dioxus::use_synced_table
//! [`dioxus::SyncHandle`]: crate::dioxus::SyncHandle

#[cfg(not(target_arch = "wasm32"))]
use crate::synced_model::SyncedModel;

/// Native trait: the entity carries a SeaORM `EntityTrait` association
/// so the unified hook can issue queries via `E::Entity::find()`.
///
/// Implementations also satisfy [`SyncedModel`] — the same supertrait
/// the native `use_synced_table_db` hook uses to apply column changes
/// in place without a DB round trip.
#[cfg(not(target_arch = "wasm32"))]
pub trait SyncedTableEntity:
    SyncedModel + ::sea_orm::FromQueryResult + Clone + Send + Sync + 'static
where
    Self: ::sea_orm::IntoActiveModel<<Self as SyncedTableEntity>::ActiveModel>,
{
    /// The SeaORM `Entity` type whose `Model` is `Self`.
    type Entity: ::sea_orm::EntityTrait<Model = Self>;

    /// The active-model type used by [`crate::dioxus::SyncHandle::submit`]
    /// to insert/upsert rows. `DeriveEntityModel` generates this
    /// alongside the entity. The `IntoActiveModel` supertrait bound
    /// (above) is what makes `submit` able to call
    /// `entity.clone().into_active_model()` without a per-method bound.
    type ActiveModel: ::sea_orm::ActiveModelTrait<Entity = Self::Entity>
        + ::sea_orm::ActiveModelBehavior
        + Send;

    /// The table name registered with the engine — pulled from
    /// `#[sea_orm(table_name = "...")]` at the struct level.
    fn table_name() -> &'static str;
}

/// Web trait: the entity carries the column-bag round-trip impl
/// (`from_columns`/`to_columns`) via [`BrowserEntity`], plus the table
/// name so the unified hook doesn't need a separate `&str` parameter.
///
/// [`BrowserEntity`]: crate::BrowserEntity
#[cfg(target_arch = "wasm32")]
pub trait SyncedTableEntity: crate::BrowserEntity {
    /// The table name registered with the engine — pulled from
    /// `#[sea_orm(table_name = "...")]` at the struct level. The
    /// derive parses this on both targets even though no SeaORM types
    /// are involved on wasm32; the `sea_orm` helper attribute is
    /// claimed by `SyncEntity`'s `attributes(sea_orm)` declaration.
    fn table_name() -> &'static str;
}
