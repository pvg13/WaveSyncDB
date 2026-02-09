use crate::{crud::{Crud, CrudError}, engine::ENGINE_TX, messages::Operation};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Database connection failed: {0}")]
    CrudError(#[from] CrudError),

    #[error("Failed to send operation to engine: {0}")]
    EngineSendError(#[from] tokio::sync::mpsc::error::SendError<Operation>),
}

pub trait SyncedCrud where Self: Sized {
    type PrimaryKey;

    // Creates a new record in the database and returns the created instance
    async fn create_sync(value: &Self) -> Result<(), SyncError>;

    // Updates an existing record in the database with the current state of the instance
    async fn update_sync(&mut self) -> Result<(), SyncError>;

    // Deletes the record from the database corresponding to the instance's primary key
    async fn delete_sync(self) -> Result<(), SyncError>;

}

impl<T> SyncedCrud for T where T: Crud<PrimaryKey = i32> {
    type PrimaryKey = T::PrimaryKey;

    async fn create_sync(value: &Self) -> Result<(), SyncError> {
        // Insert value into the database
        Self::create(value).await?;

        ENGINE_TX.get().unwrap().send(Operation::Create(
            Self::table_name(),
            value.fields(), 
        )).await?;

        Ok(())
    }

    async fn update_sync(&mut self) -> Result<(), SyncError> {
        // Update value in the database
        Self::update(self).await?;

        ENGINE_TX.get().unwrap().send(Operation::Update(
            Self::table_name(),
            self.fields(), 
        )).await?;

        Ok(())
    }

    async fn delete_sync(self) -> Result<(), SyncError> {
        // Delete value from the database
        Self::delete_by_id(self.id()).await?;

        ENGINE_TX.get().unwrap().send(Operation::Delete(
            Self::table_name(),
            self.id(), 
        )).await?;

        Ok(())
    }
}