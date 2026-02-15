use wavesyncdb::crud::{CrudError, Migration};
use wavesyncdb::engine::WaveSyncEngine;
use wavesyncdb_derive::{CrudModel, SyncedCrudModel};
use wavesyncdb::crud::CrudModel;
use wavesyncdb::SyncedModel;


#[derive(CrudModel, SyncedCrudModel)]
pub struct User {
    #[primary_key]
    pub id: i32,
    pub name: String,
    pub email: String,
    pub is_active: bool,
    pub description: Option<String>,
}

pub async fn create_database_pool() {
    // Try to initialize the database connection pool if it hasn't been initialized yet
    let _ = wavesyncdb::DATABASE.get_or_init(|| async {
        sqlx::SqlitePool::connect("sqlite::memory:").await.expect("Failed to create database pool")
    }).await;
}

pub fn start_engine() {
    // Initialize the engine and start processing operations
    wavesyncdb::WaveSyncBuilder::new("test_sync_user")
        .build()
        .start();
}

pub async fn create_node() {

}


// Test the Sync
#[tokio::test]
async fn test_sync_model() {



    create_database_pool().await;

    start_engine();

    User::up().await.expect("Error creating users table"); // Create the users table

    // Create a new user instance
    let mut user = User::create_synced_new("Alice".to_string(), "alice@example.com".to_string(), true, Some("A description".to_string())).await.expect("Failed to create user");

    // Call the create method to insert the user into the database
    user.sync_create().await.expect("Failed to create user");

    // Update the user's name and call update_sync to update the record in the database
    user.name = "Bob".to_string();
    user.update().await.expect("Failed to update user");
    user.sync_update().await.expect("Failed to update user");

    // Call the get method to retrieve the user from the database and verify the name was updated
    let result = User::get(1).await.expect("Failed to retrieve user");
    assert_eq!(result.name, "Bob");

    // Call the delete_sync method to delete the user from the database
    
    user.sync_delete().await.expect("Failed to delete user");
    user.delete().await.expect("Failed to delete user");

    // Attempt to retrieve the deleted user from the database
    let result = User::get(1).await; // This should return an error since the user has been deleted

    // Assert that the result is an error, indicating that the user was successfully deleted
    assert!(result.is_err());

    // Finally, call the down method to drop the users table from the database
    User::down().await.expect("Error dropping users table"); // Drop the users table

    // Test that the users table has been dropped by attempting to retrieve all users, which should return an error
    let result = User::all().await; // This should return an error since the users table has been dropped
    assert!(result.is_err());
}