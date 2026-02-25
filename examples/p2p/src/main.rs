mod entity;

use entity::task;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

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

    // Initialize WaveSyncDb — one call sets up DB + sync engine
    let db = WaveSyncDbBuilder::new("sqlite::memory:", "p2p_tasks")
        .build()
        .await
        .expect("Failed to create WaveSyncDb");

    // Auto-discover entities annotated with #[derive(SyncEntity)]
    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await
        .expect("Failed to sync schema");

    loop {
        menu();
        let mut choice = String::new();
        std::io::stdin()
            .read_line(&mut choice)
            .expect("Failed to read input");
        let choice = choice.trim().parse::<u32>().unwrap_or(0);

        match choice {
            1 => {
                println!("Enter task title:");
                let mut title = String::new();
                std::io::stdin()
                    .read_line(&mut title)
                    .expect("Failed to read input");
                let title = title.trim().to_string();

                let new_task = task::ActiveModel {
                    id: Set(Uuid::new_v4().to_string()),
                    title: Set(title),
                    completed: Set(false),
                    ..Default::default()
                };
                match new_task.insert(&db).await {
                    Ok(t) => println!("Created task: {} (id={})", t.title, t.id),
                    Err(e) => println!("Error creating task: {}", e),
                }
            }
            2 => {
                println!("Enter task ID to update:");
                let mut id = String::new();
                std::io::stdin()
                    .read_line(&mut id)
                    .expect("Failed to read input");
                let id = id.trim().to_string();

                match task::Entity::find_by_id(&id).one(&db).await {
                    Ok(Some(t)) => {
                        println!("Enter new title (current: {}):", t.title);
                        let mut title = String::new();
                        std::io::stdin()
                            .read_line(&mut title)
                            .expect("Failed to read input");

                        let mut active: task::ActiveModel = t.into();
                        active.title = Set(title.trim().to_string());
                        match active.update(&db).await {
                            Ok(t) => println!("Updated task: {}", t.title),
                            Err(e) => println!("Error updating task: {}", e),
                        }
                    }
                    Ok(None) => println!("Task not found"),
                    Err(e) => println!("Error: {}", e),
                }
            }
            3 => {
                println!("Enter task ID to delete:");
                let mut id = String::new();
                std::io::stdin()
                    .read_line(&mut id)
                    .expect("Failed to read input");
                let id = id.trim().to_string();

                match task::Entity::delete_by_id(&id).exec(&db).await {
                    Ok(res) => println!("Deleted {} task(s)", res.rows_affected),
                    Err(e) => println!("Error deleting task: {}", e),
                }
            }
            4 => {
                match task::Entity::find().all(&db).await {
                    Ok(tasks) => {
                        for t in tasks {
                            println!(
                                "ID: {}, Title: {}, Completed: {}",
                                t.id, t.title, t.completed
                            );
                        }
                    }
                    Err(e) => println!("Error listing tasks: {}", e),
                }
            }
            5 => break,
            _ => println!("Invalid choice"),
        }
    }
}
