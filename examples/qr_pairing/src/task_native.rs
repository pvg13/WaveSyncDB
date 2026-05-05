//! SeaORM model for the task entity used by the native build.
//!
//! Mirrors the field set the web build's plain `Task` struct uses,
//! so writes from one side land cleanly on the other through the
//! shared CRDT shadow tables. The web side uses `BrowserEntity`
//! manually; the native side uses the `#[derive(SyncEntity)]` proc
//! macro that hooks into SeaORM at link time.

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};
use wavesyncdb::SyncEntity;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity, Serialize, Deserialize)]
#[sea_orm(table_name = "tasks")]
pub struct Model {
    /// String UUID — same shape as the web side generates so the
    /// shadow table key (`tasks|<pk>|<cid>`) matches across peers.
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub title: String,
    pub done: bool,
    /// Unix milliseconds. Stored as i64 because SQLite has no
    /// unsigned types; cast to u64 for display sorting.
    pub created_at: i64,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
