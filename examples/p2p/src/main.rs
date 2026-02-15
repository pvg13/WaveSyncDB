mod model;

use model::Task;
use wavesyncdb::{CrudModel, WaveSyncBuilder, SyncedModel, Migration};


pub fn menu() {
    println!("1. Create Task");
    println!("2. Update Task");
    println!("3. Delete Task");
    println!("4. List Tasks");
    println!("5. Exit");
}

#[tokio::main]
pub async fn main() {

    env_logger::init();

    // Initialize the engine and start processing operations
    WaveSyncBuilder::new("p2p_tasks").build().start();

    let _ = wavesyncdb::DATABASE.get_or_init(|| async {
        wavesyncdb::sqlx::SqlitePool::connect("sqlite::memory:").await.expect("Failed to create database pool")
    }).await;

    Task::up().await.expect("Error creating tasks table"); // Create the tasks table

    loop {
        // Simple user interface to CRUD tasks
        menu();
        let mut choice = String::new();
        std::io::stdin().read_line(&mut choice).expect("Failed to read input");
        let choice = choice.trim().parse::<u32>().expect("Invalid input");

        match choice {
            1 => {
                println!("Enter task title:");
                let mut title = String::new();
                std::io::stdin().read_line(&mut title).expect("Failed to read input");
                let title = title.trim().to_string();

                Task::create_synced_new(title, None, false).await.expect("Failed to create task");
                // Task::create_sync(&task).await.expect("Failed to create task");
            },
            2 => {
                println!("Enter task ID to update:");
                let mut id = String::new();
                std::io::stdin().read_line(&mut id).expect("Failed to read input");
                let id = id.trim().parse::<i32>().expect("Invalid input");

                let mut task: Task = Task::get(id).await.expect("Failed to retrieve task");
                println!("Enter new task title:");
                let mut title = String::new();
                std::io::stdin().read_line(&mut title).expect("Failed to read input");
                task.title = title.trim().to_string();
                task.update_sync().await.expect("Failed to update task");
            },
            3 => {
                println!("Enter task ID to delete:");
                let mut id = String::new();
                std::io::stdin().read_line(&mut id).expect("Failed to read input");
                let id = id.trim().parse::<i32>().expect("Invalid input");
                let task = Task::get(id).await.expect("Failed to retrieve task");
                task.delete_sync().await.expect("Failed to delete task");
            },
            4 => {
                let tasks = Task::all().await.expect("Failed to retrieve tasks");
                for task in tasks {
                    println!("ID: {}, Title: {}", task.id, task.title);
                }
            },
            5 => {
                break;
            },
            _ => {
                println!("Invalid choice");
            },
        }
    }
}