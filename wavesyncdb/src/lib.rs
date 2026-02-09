// pub mod instrument;
pub mod engine;
// mod types;
pub(crate) mod messages;

use std::sync::OnceLock;

use sqlx::{AnyConnection, AnyPool, database::Database};

mod crud;
mod sync;

// Database connection
static DATABASE: OnceLock<AnyPool> = OnceLock::new();

pub mod prelude {
    // pub use crate::instrument::WaveSyncInstrument;
    // pub use crate::types::DbQuery;
}
