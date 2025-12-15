mod model;
mod schema;

use std::{error::Error, thread::sleep, time::Duration};

use chrono::Timelike;
use diesel::connection::Instrumentation;
use diesel::{
    prelude::*,
    
    sqlite::Sqlite,
};
use diesel_async::pooled_connection::deadpool::{Object, Pool};
use diesel_async::{AsyncMigrationHarness, sync_connection_wrapper::SyncConnectionWrapper};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use model::Task;
use tokio::time::sleep_until;
use wavesyncdb::instrument::dialects::DialectType;
use wavesyncdb::prelude::WaveSyncInstrument;
use diesel_async::{AsyncConnection, AsyncConnectionCore, RunQueryDsl};
use diesel_async::pooled_connection::{AsyncDieselConnectionManager};

const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!();

pub type AsyncSqliteConnection = SyncConnectionWrapper<SqliteConnection>;

pub type DbPool = Pool<AsyncSqliteConnection>;

async fn establish_pool(database_url: &str) -> Result<DbPool, Box<dyn Error>> {
    let manager = AsyncDieselConnectionManager::<AsyncSqliteConnection>::new(database_url);
    let pool = Pool::builder(manager)
        .max_size(16)
        .build()?;

    pool.get().await?; 
    
    Ok(pool)
}

async fn run_migrations(pool: &DbPool) -> Result<(), Box<dyn Error>> {
    let mut harness = AsyncMigrationHarness::new(pool.get().await?);

    // Run migrations if needed
    harness.run_pending_migrations(MIGRATIONS)
        .expect("Error running migrations");

    Ok(())
}


async fn start_wavesync(pool: &DbPool, conn: &mut impl AsyncConnection) -> Result<(), Box<dyn Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);

    conn.set_instrumentation(WaveSyncInstrument::new(tx, DialectType::SQLite));

    let mut wavesync_engine = wavesyncdb::sync::WaveSyncEngine::new(rx, pool.get().await?);

    tokio::spawn(async move {
        wavesync_engine.run().await;
    });

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Establish the pools

    let alice_pool = establish_pool(&format!("{}.1.db", database_url)).await.unwrap();
    let bob_pool = establish_pool(&format!("{}.2.db", database_url)).await.unwrap();

    // Run the migrations
    let _ = run_migrations(&alice_pool).await;
    let _ = run_migrations(&bob_pool).await;

    let mut alice = alice_pool.get().await?;
    let mut bob = bob_pool.get().await?;

    // Start wavesync on both
    let _ = start_wavesync(&alice_pool, &mut alice).await;
    let _ = start_wavesync(&bob_pool, &mut bob).await;

    // Get the actual connections for the application
    

    tokio::spawn(async move {
        loop {
            use schema::tasks::dsl::*;

            let results = tasks
                .limit(5)
                .select(Task::as_select())
                .load(&mut bob).await
                .expect("Error loading posts");

            println!("Displaying {} posts", results.len());
            for post in results {
                println!("{}", post.title);
            }
            println!("------------------\n");
            tokio::time::sleep(Duration::from_secs(10)).await;
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
                    ..Task::default()
                };
                diesel::insert_into(schema::tasks::table)
                    .values(&new_task)
                    .execute(&mut alice)
                    .await
                    .expect("Error inserting new task");
            }
            Err(error) => {
                println!("error: {}", error);
            }
        }
    }

    Ok(())
}
