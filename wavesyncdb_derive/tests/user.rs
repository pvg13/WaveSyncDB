use wavesyncdb::crud::{CrudError, Migration};
use wavesyncdb_derive::CrudModel;
use wavesyncdb::crud::CrudModel;


#[derive(CrudModel)]
pub struct User {
    #[primary_key]
    pub id: i32,
    pub name: String,
    pub email: String,
}

// impl User {
//     pub fn new(id: i32, name: String, email: String) -> Self {
//         Self { id, name, email }
//     }
// }

// #[async_trait::async_trait]
// impl Migration for User {
//     async fn up() -> Result<(), CrudError> {
//         // Create the users table in the database
//         sqlx::query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
//             .execute(wavesyncdb::DATABASE.get().unwrap())
//             .await
//             .map(|_| ())?;

//         Ok::<(), CrudError>(())
//     }

//     async fn down() -> Result<(), CrudError> {
//         // Drop the users table from the database
//         sqlx::query("DROP TABLE IF EXISTS users")
//             .execute(wavesyncdb::DATABASE.get().unwrap())
//             .await
//             .map(|_| ())?;

//         Ok::<(), CrudError>(())
//     }
// }

pub async fn create_database_pool() {
    // Try to initialize the database connection pool if it hasn't been initialized yet
    let _ = wavesyncdb::DATABASE.get_or_init(|| async {
        sqlx::SqlitePool::connect("sqlite::memory:").await.expect("Failed to create database pool")
    }).await;
}

#[tokio::test]
async fn test_crud() {
    create_database_pool().await;

    User::up().await.expect("Error creating users table"); // Create the users table

    // Create a new user instance
    let mut user = User::create_new("Alice".to_string(), "alice@example.com".to_string()).await.expect("Failed to create user");

    // Call the create method to insert the user into the database
    // user.create().await.expect("Failed to create user");

    let result = User::get(1).await.expect("Failed to retrieve user"); // Retrieve the user from the database

    // Assert that the retrieved user data matches the original user data
    assert_eq!(user.id, result.id);
    assert_eq!(user.name, result.name);
    assert_eq!(user.email, result.email);

    // Update the user's name and email
    user.name = "Bob".to_string();
    user.email = "bob@example.com".to_string();

    // Call the update method to update the user in the database
    user.update().await.expect("Failed to update user");

    let mut updated_user = User::get(1).await.expect("Failed to retrieve updated user"); // Retrieve the updated user from the database

    // Assert that the updated user data matches the new values
    assert_eq!(updated_user.name, "Bob");
    assert_eq!(updated_user.email, "bob@example.com");

    // Change the updated user's name and email again to test the read method
    updated_user.name = "Charlie".to_string();
    updated_user.email = "charlie@example.com".to_string();

    updated_user.update().await.expect("Failed to update user with new values"); // Update the user in the database with the new values

    user.read().await.expect("Failed to read user"); // Call the read method to update the user instance with the data from the database

    // Assert that the user instance has been updated with the data from the database, which should be "Bob" and "
    assert_eq!(user.name, "Charlie");
    assert_eq!(user.email, "charlie@example.com");


    // Call the delete method to remove the user from the database
    user.delete().await.expect("Failed to delete user");

    // Attempt to retrieve the deleted user from the database
    let result = User::get(1).await; // This should return an error since the user has been deleted

    // Assert that the result is an error, indicating that the user was successfully deleted
    assert!(result.is_err());

    // Add multiple users to test the all() method
    let user3 = User::create_new("Charlie".to_string(), "charlie@example.com".to_string()).await.expect("Failed to create user");
    let user4 = User::create_new("David".to_string(), "david@example.com".to_string()).await.expect("Failed to create user");

    // User::create(&user3).await.expect("Failed to create user");
    // User::create(&user4).await.expect("Failed to create user");
    
    let all_users = User::all().await.expect("Failed to retrieve all users"); // Retrieve all users from the database

    // Assert that the all() method returns the correct number of users
    assert_eq!(all_users.len(), 2);

    // Assert that the retrieved users match the created users
    assert_eq!(all_users[0].id, user3.id);
    assert_eq!(all_users[0].name, user3.name);
    assert_eq!(all_users[0].email, user3.email);

    assert_eq!(all_users[1].id, user4.id);
    assert_eq!(all_users[1].name, user4.name);
    assert_eq!(all_users[1].email, user4.email);

    // Delete a user by primary key using the delete_by_id method
    User::delete_by_id(2).await.expect("Failed to delete user by id");

    // Asert that the user with id 2 has been deleted
    let result = User::get(2).await; // This should return an error since
    assert!(result.is_err());

    // Finally, call the down method to drop the users table from the database
    User::down().await.expect("Error dropping users table"); // Drop the users table

    // Test that the users table has been dropped by attempting to retrieve all users, which should return an error
    let result = User::all().await; // This should return an error since the users

    // Assert that the result is an error, indicating that the users table was successfully dropped
    assert!(result.is_err());
}
