use std::error::Error;


use thiserror::Error;

use crate::messages::OpValue;

#[derive(Error, Debug)]
pub enum CrudError {
    #[error("Database connection failed: {0}")]
    ConnectionError(#[from] sqlx::Error), // Automatically converts sqlx::Error to CrudError

    #[error("Record with ID {0} not found")]
    NotFound(String),

    #[error("Validation failed: {0}")]
    ValidationError(String),

    #[error("Unauthorized access to resource")]
    Unauthorized,

    #[error("Unknown internal error")]
    Unknown,

    #[error("Database not initialized")]
    DbNotInitialized
}

#[async_trait::async_trait]
pub trait CrudModel where Self: Sized {

    type PrimaryKey;

    // Creates a new record in the database and returns the created instance
    async fn create(&mut self) -> Result<(), CrudError>;

    // Reads a record from the database by its primary key and updates the instance with the retrieved data
    async fn read(&mut self) -> Result<(), CrudError>;

    // Updates an existing record in the database with the current state of the instance
    async fn update(&mut self) -> Result<(), CrudError>;

    // Deletes the record from the database corresponding to the instance's primary key
    async fn delete(self) -> Result<(), CrudError>;

    // Retrieves a record from the database by its primary key
    async fn get(id: Self::PrimaryKey) -> Result<Self, CrudError>;

    // Retrieves all records from the database
    async fn all() -> Result<Vec<Self>, CrudError>;

    // Delete record by primary key
    async fn delete_by_id(id: Self::PrimaryKey) -> Result<(), CrudError>;

    fn fields(&self) -> Vec<(String, OpValue)>;
    fn table_name() -> String;
    fn id(&self) -> Self::PrimaryKey;

}

#[async_trait::async_trait]
pub trait Migration {
    async fn up() -> Result<(), CrudError>;
    async fn down() -> Result<(), CrudError>;
}

// Example implementation for a User struct
/*
#[derive(Debug, Clone)]
struct User {
    id: i32,
    name: String,
    email: String,
}

impl User {
    fn new(id: i32, name: String, email: String) -> Self {
        User { id, name, email }
    }
}

impl Crud for User {
    type PrimaryKey = i32;

    async fn create(value: &Self) -> Result<(), CrudError> {
        // Insert the user into the database and return the created instance
        
    }

    async fn read(&mut self) -> Result<(), CrudError> {
        // Retrieve the user from the database using self.id and update self with the retrieved data
        // This is a placeholder implementation
    }

    async fn update(&mut self) -> Result<(), CrudError> {
        // Update the existing user record in the database with the current state of self
        // This is a placeholder implementation
    }

    async fn delete(self) -> Result<(), CrudError> {
        // Delete the user record from the database corresponding to self.id
        // This is a placeholder implementation
    }
}
*/


#[cfg(test)] 
mod test {
    use tokio::sync::OnceCell;

    use sqlx::{AnyPool, Row, Sqlite, SqlitePool};

    use crate::crud;

    use super::*;

    struct User {
        id: i32,
        name: String,
        email: String,
    }

    impl User {
        fn new(id: i32, name: String, email: String) -> Self {
            User { id, name, email }
        }
    }

    static DATABASE: OnceCell<SqlitePool> = OnceCell::const_new();

    pub async fn create_database_pool() {
        // Try to initialize the database connection pool if it hasn't been initialized yet
        let _ = DATABASE.get_or_init(|| async {
            sqlx::SqlitePool::connect("sqlite::memory:").await.expect("Failed to create database pool")
        }).await;

    }
    
    #[async_trait::async_trait]
    impl CrudModel for User {
        type PrimaryKey = i32;

        async fn create(&mut self) -> Result<(), CrudError> {
            // Insert the user into the database and return the created instance
            sqlx::query("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)")
                .bind(self.id)
                .bind(&self.name)
                .bind(&self.email)
                .execute(DATABASE.get().unwrap())
                .await.map(|_| ())?;

            Ok::<(), CrudError>(())
        }

        async fn read(&mut self) -> Result<(), CrudError> {
            // Retrieve the user from the database using self.id and update self with the retrieved data
            let row = sqlx::query("SELECT name, email FROM users WHERE id = $1")
                .bind(self.id)
                .fetch_one(DATABASE.get().unwrap())
                .await?;

            self.name = row.get("name");
            self.email = row.get("email");

            Ok::<(), CrudError>(())
        }

        async fn update(&mut self) -> Result<(), CrudError> {
            // Update the existing user record in the database with the current state of self
            
            sqlx::query("UPDATE users SET name = $1, email = $2 WHERE id = $3")
                .bind(&self.name)
                .bind(&self.email)
                .bind(self.id)
                .execute(DATABASE.get().unwrap())
                .await
                .map(|_| ())?;

            Ok::<(), CrudError>(())

        }

        async fn delete(self) -> Result<(), CrudError> {
            // Delete the user record from the database corresponding to self.id
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(self.id)
                .execute(DATABASE.get().unwrap())
                .await
                .map(|_| ())?;

            Ok::<(), CrudError>(())
        }

        async fn get(id: Self::PrimaryKey) -> Result<Self, CrudError> {
            // Retrieve a user from the database by its primary key
            let row = sqlx::query("SELECT id, name, email FROM users WHERE id = $1")
                .bind(id)
                .fetch_one(DATABASE.get().unwrap())
                .await?;

            Ok::<_, CrudError>(User {
                id: row.get("id"),
                name: row.get("name"),
                email: row.get("email"),
            })
        }

        async fn all() -> Result<Vec<Self>, CrudError> {
            // Retrieve all users from the database
            let rows = sqlx::query("SELECT id, name, email FROM users")
                .fetch_all(DATABASE.get().unwrap())
                .await?;

            Ok::<_, CrudError>(rows.into_iter().map(|row| User {
                id: row.get("id"),
                name: row.get("name"),
                email: row.get("email"),
            }).collect())

            // Ok::<(), CrudError>(())
        }

        async fn delete_by_id(id: Self::PrimaryKey) -> Result<(), CrudError> {
            // Delete a user from the database by its primary key
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(id)
                .execute(DATABASE.get().unwrap())
                .await
                .map(|_| ())?;

            Ok::<(), CrudError>(())
        }


        fn fields(&self) -> Vec<(String, OpValue)> {
            vec![
                ("id".to_string(), OpValue::Integer(self.id as i64)),
                ("name".to_string(), OpValue::Text(self.name.clone())),
                ("email".to_string(), OpValue::Text(self.email.clone())),
            ]
        }

        fn table_name() -> String {
            "users".to_string()
        }


        fn id(&self) -> Self::PrimaryKey {
            self.id
        }

    }

    #[async_trait::async_trait]
    impl Migration for User {
        async fn up() -> Result<(), CrudError> {
            // Create the users table in the database
            sqlx::query("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
                .execute(DATABASE.get().unwrap())
                .await
                .map(|_| ())?;

            Ok::<(), CrudError>(())
        }

        async fn down() -> Result<(), CrudError> {
            // Drop the users table from the database
            sqlx::query("DROP TABLE IF EXISTS users")
                .execute(DATABASE.get().unwrap())
                .await
                .map(|_| ())?;

            Ok::<(), CrudError>(())
        }
    }


    #[tokio::test]
    async fn test_crud() {
        create_database_pool().await;

        User::up().await.expect("Error creating users table"); // Create the users table

        // Create a new user instance
        let mut user = User::new(1, "Alice".to_string(), "alice@example.com".to_string());

        // Call the create method to insert the user into the database
        user.create().await.expect("Failed to create user");

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
        let mut user3 = User::new(2, "Charlie".to_string(), "charlie@example.com".to_string());
        let mut user4 = User::new(3, "David".to_string(), "david@example.com".to_string());

        user3.create().await.expect("Failed to create user");
        user4.create().await.expect("Failed to create user");

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

}
