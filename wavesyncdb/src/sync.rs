use crate::{crud::{CrudModel, CrudError}, engine::ENGINE_TX, messages::Operation};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Database connection failed: {0}")]
    CrudError(#[from] CrudError),

    #[error("Failed to send operation to engine: {0}")]
    EngineSendError(#[from] tokio::sync::mpsc::error::SendError<Operation>),
}

#[async_trait::async_trait]
pub trait SyncedModel: CrudModel {
    type PrimaryKey;

    // Creates a new record in the database and returns the created instance
    async fn sync_create(&mut self) -> Result<(), SyncError>;

    // Updates an existing record in the database with the current state of the instance
    async fn sync_update(&mut self) -> Result<(), SyncError>;

    // Deletes the record from the database corresponding to the instance's primary key
    async fn sync_delete(&self) -> Result<(), SyncError>;


    // Convienience method to create, update and delete without needing to call the non-sync versions first
    async fn create_sync(&mut self) -> Result<(), SyncError> {
        self.create().await?;
        self.sync_create().await
    }

    async fn update_sync(&mut self) -> Result<(), SyncError> {
        self.update().await?;
        self.sync_update().await
    }

    async fn delete_sync(&self) -> Result<(), SyncError> {
        Self::delete_by_id(self.id()).await?;
        self.sync_delete().await
    }
}

#[async_trait::async_trait]
impl<T: CrudModel<PrimaryKey = i32> + Send + Sync> SyncedModel for T {
    type PrimaryKey = T::PrimaryKey;

    async fn sync_create(&mut self) -> Result<(), SyncError> {
        // Insert value into the database
        // self.create().await?;

        ENGINE_TX.get().unwrap().send(Operation::Create {
            table: Self::table_name(),
            fields: self.fields(), 
        }
        ).await?;

        Ok(())
    }

    async fn sync_update(&mut self) -> Result<(), SyncError> {
        // Update value in the database
        // self.update().await?;

        ENGINE_TX.get().unwrap().send(Operation::Update {
            table: Self::table_name(),
            pk: self.id(),
            fields: self.fields(), 
        }
        ).await?;

        Ok(())
    }

    async fn sync_delete(&self) -> Result<(), SyncError> {
        // Delete value from the database
        // Self::delete_by_id(self.id()).await?;

        ENGINE_TX.get().unwrap().send(Operation::Delete {
            table: Self::table_name(),
            pk: self.id(), 
        }
        ).await?;

        Ok(())
    }
}
