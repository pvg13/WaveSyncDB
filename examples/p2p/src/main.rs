mod model;
mod schema;

use std::{error::Error, time::Duration};

use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{prelude::*, sqlite::Sqlite};

use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;
use model::Task;
use wavesyncdb::instrument::dialects::DialectType;

const MIGRATIONS: EmbeddedMigrations = diesel_migrations::embed_migrations!();

type WSPool = Pool<ConnectionManager<SqliteConnection>>;

fn establish_pool(database_url: &str) -> Result<WSPool, Box<dyn Error>> {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = Pool::builder().max_size(16).build(manager)?;

    Ok(pool)
}

fn run_migrations(pool: &mut impl MigrationHarness<Sqlite>) -> Result<(), Box<dyn Error>> {
    // Run migrations if needed
    pool.run_pending_migrations(MIGRATIONS)
        .expect("Error running migrations");

    Ok(())
}

fn start_wavesync(pool: &WSPool, conn: &mut impl Connection) -> Result<(), Box<dyn Error>> {
    let builder = wavesyncdb::sync::WaveSyncBuilder::new(pool.get()?, "testtopic")
        .connect(conn, DialectType::SQLite);

    let mut wavesyncdb = builder.build();

    tokio::spawn(async move {
        wavesyncdb.run().await;
    });

    Ok(())
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    env_logger::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Establish the pools

    let alice_pool = establish_pool(&format!("{}.1.db", database_url))
        .unwrap();
    let bob_pool = establish_pool(&format!("{}.2.db", database_url))
        .unwrap();

    let mut alice = alice_pool
        .get()
        .expect("Failed to get connection from pool");
    let mut bob = bob_pool.get().expect("Failed to get connection from pool");

    // Run the migrations
    run_migrations(&mut alice)?;
    run_migrations(&mut bob)?;

    // Start wavesync on both

    start_wavesync(&alice_pool, &mut alice)?;
    start_wavesync(&bob_pool, &mut bob)?;

    // Get the actual connections for the application

    tokio::spawn(async move {
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
                    .expect("Error inserting new task");
            }
            Err(error) => {
                println!("error: {}", error);
            }
        }
    }

    Ok(())
}
