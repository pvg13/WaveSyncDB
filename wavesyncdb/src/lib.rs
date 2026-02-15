// pub mod instrument;
pub mod engine;
// mod types;
pub mod messages;

use std::sync::OnceLock;

use sqlx::{AnyConnection, AnyPool, Sqlite, SqlitePool, database::Database};
use tokio::sync::OnceCell;

pub mod crud;
pub mod sync;

pub use crud::{CrudModel, CrudError, Migration};
pub use sync::{SyncedModel, SyncError};

pub use engine::{WaveSyncBuilder, WaveSyncEngine};
// Database connection
pub static DATABASE: OnceCell<SqlitePool> = OnceCell::const_new();

#[cfg(feature = "derive")]
pub mod derive {
    pub use wavesyncdb_derive::*;
}

// Re-export sqlx for users of the library
pub use sqlx;
pub use async_trait;
