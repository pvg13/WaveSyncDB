mod model;
mod schema;

use std::{error::Error, thread::sleep, time::Duration};

use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool, PooledConnection},
    sqlite::Sqlite,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use model::Task;
use tokio::time::sleep_until;
use wavesyncdb::instrument::dialects::DialectType;
use wavesyncdb::prelude::WaveSyncInstrument;

const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!();

fn start_database_client(
    database_url: String,
) -> Result<PooledConnection<ConnectionManager<SqliteConnection>>, Box<dyn Error>> {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = Pool::builder()
        .max_size(16) // tamaño del pool
        .min_idle(Some(2))
        .connection_timeout(Duration::from_secs(3)) // timeout al pedir conexión
        .build(manager)?;

    let mut conn = pool.get()?;
    // Run migrations if needed
    conn.run_pending_migrations(MIGRATIONS)
        .expect("Error running migrations");

    let (tx, rx) = tokio::sync::mpsc::channel(100);

    conn.set_instrumentation(WaveSyncInstrument::new(tx, DialectType::SQLite));

    let mut wavesync_engine = wavesyncdb::sync::WaveSyncEngine::new(rx, pool.get()?);

    tokio::spawn(async move {
        wavesync_engine.run().await;
    });

    Ok(conn)
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Create Alice

    let mut alice = start_database_client(format!("{}.1.db", database_url)).unwrap();
    let mut bob = start_database_client(format!("{}.2.db", database_url)).unwrap();

    std::thread::spawn(move || {
        loop {
            use schema::tasks::dsl::*;

            let results = tasks
                .limit(5)
                .select(Task::as_select())
                .load(&mut bob)
                .expect("Error loading posts");

            println!("Displaying {} posts", results.len());
            for post in results {
                println!("{}", post.title);
                println!("-----------\n");
                println!("{:?}", post.description);
            }
            sleep(Duration::from_secs(1));
        }
    });

    // Insert a new task
    loop {
        // Get user input
        let mut input = String::new();
        let user_input = std::io::stdin().read_line(&mut input);
        match user_input {
            Ok(_) => {
                let trimmed = input.trim();
                if trimmed.is_empty() {
                    break;
                }
                let new_task = Task {
                    id: None,
                    title: trimmed.to_string(),
                    description: None,
                    completed: false,
                    created_at: Some(chrono::Utc::now().naive_utc()),
                    updated_at: Some(chrono::Utc::now().naive_utc()),
                };
                diesel::insert_into(schema::tasks::table)
                    .values(&new_task)
                    .execute(&mut alice)
                    .expect("Error inserting new task");
            }
            Err(error) => {
                println!("error: {}", error);
            }
        }
    }

    Ok(())
}
