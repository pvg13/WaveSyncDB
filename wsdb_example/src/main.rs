mod schema;
mod model;


use std::{error::Error, time::Duration};

use diesel::{prelude::*, r2d2::{ConnectionManager, Pool}};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use wavesyncdb::prelude::WaveSyncInstrument;
use model::Task;
use wavesyncdb::instrument::dialects::DialectType;

const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!();


#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    dotenv().ok();
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = Pool::builder()
        .max_size(16)                               // tamaño del pool
        .min_idle(Some(2))
        .connection_timeout(Duration::from_secs(3)) // timeout al pedir conexión
        .build(manager)?;

    
    let mut conn = pool.get()?;
    // Run migrations if needed
    conn.run_pending_migrations(MIGRATIONS).expect("Error running migrations");

    let (tx, rx) = tokio::sync::mpsc::channel(100);

    conn.set_instrumentation(WaveSyncInstrument::new(tx, DialectType::SQLite));

    let mut wavesync_engine = wavesyncdb::sync::WaveSyncEngine::new(rx, pool.get()?);

    tokio::spawn(async move {
        wavesync_engine.run().await;
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
                    .execute(&mut conn)
                    .expect("Error inserting new task");

            }
            Err(error) => {
                println!("error: {}", error);
            }
        }
    }

    Ok(())
}