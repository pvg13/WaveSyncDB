#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum WaveSyncError {
    #[error("Database error: {msg}")]
    Database { msg: String },
    #[error("Serialization error: {msg}")]
    Serialization { msg: String },
    #[error("Not initialized")]
    NotInitialized,
    #[error("IO error: {msg}")]
    Io { msg: String },
}

impl From<sea_orm::DbErr> for WaveSyncError {
    fn from(e: sea_orm::DbErr) -> Self {
        Self::Database { msg: e.to_string() }
    }
}

impl From<serde_json::Error> for WaveSyncError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization { msg: e.to_string() }
    }
}
