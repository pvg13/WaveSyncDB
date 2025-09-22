mod schema;
mod model;


use diesel::prelude::*;
use dotenvy::dotenv;
use wavesyncdb::WaveSyncInstrument;
use model::Task;

pub fn main() {

    dotenv().ok();
    env_logger::init();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut connection = SqliteConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url));

    connection.set_instrumentation(WaveSyncInstrument {});

    // Your insertion logic here
    log::debug!("Connected to the database successfully.");

    // Example insertion (assuming a table named `tasks` exists)

    use crate::schema::tasks::dsl::*;
    // use wavesyncdb::schema::tasks;
    let task = Task {
        id: None,
        title: "Sample Task".to_string(),
        description: Some("This is a sample task".to_string()),
        completed: false,
        created_at: None,
        updated_at: None,
    };

    diesel::insert_into(tasks)
        .values(&task)
        .execute(&mut connection)
        .expect("Error inserting new task");

}