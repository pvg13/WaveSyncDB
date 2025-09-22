mod schema;
mod model;


use std::{error::Error, time::Duration};

use diesel::prelude::*;
use dotenvy::dotenv;
use wavesyncdb::prelude::WaveSyncInstrument;
use model::Task;
use libp2p::{futures::StreamExt, noise, ping, swarm::SwarmEvent, tcp, yamux, Multiaddr};


// 

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    dotenv().ok();
    env_logger::init();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut connection = SqliteConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url));

    connection.set_instrumentation(WaveSyncInstrument::new());

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

    // Update
    diesel::update(tasks.filter(id.eq(1)))
        .set(completed.eq(true))
        .execute(&mut connection)
        .expect("Error updating task");

    // Delete
    diesel::delete(tasks.filter(id.eq(1)))
        .execute(&mut connection)
        .expect("Error deleting task");

    log::debug!("Operations completed successfully.");

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?.with_behaviour(|_| ping::Behaviour::default())?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
        })
        .build();

    // Tell the swarm to listen on all interfaces and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = std::env::args().nth(1) {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        println!("Dialed {addr}")
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
            SwarmEvent::Behaviour(event) => println!("{event:?}"),
            _ => {}
        }
    }

    Ok(())
}