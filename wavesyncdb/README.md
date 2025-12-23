# WaveSyncDB

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

**WaveSyncDB** is a lightweight, distributed database synchronization engine designed for high-consistency data replication. It bridges the gap between real-time data streaming and relational storage, ensuring that your decentralized nodes stay in perfect harmony with low-latency "wave" updates.

---

## Features

- **Real-Time Synchronization:** Propagate data changes across multiple nodes instantly.
- **Conflict Resolution:** Built-in logic to handle data collisions in multi-master environments.
- **Strong Consistency:** Ensures data integrity across distributed systems using efficient sync protocols.
- **Developer Friendly:** Designed to be easily integrated into existing workflows with minimal configuration.

## Installation

To get started with WaveSyncDB, clone the repository:

```bash
git clone [https://github.com/pvg13/WaveSyncDB.git](https://github.com/pvg13/WaveSyncDB.git)
cd WaveSyncDB
```

## Usage

Check out the `examples` folder for more complex usage

For a quick start:

```rust

pub fn main() {

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    // Create a pool connection
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = Pool::builder()
        .max_size(16)
        .build(manager)?;
    
    // Connection used by the user
    let conn = pool.get().unwrap();

    // Set up the channels
    let (tx, rx) = tokio::sync::mpsc::channel(100);

    // Add a custom topic
    let topic = "topic";

    // Set the instrumentation as the WaveSyncInstrument
    conn.set_instrumentation(WaveSyncInstrument::new(tx, topic, DialectType::SQLite));

    // Start the Wavesync engine to run in the background
    let mut wavesync_engine = wavesyncdb::sync::WaveSyncEngine::new(rx, pool.get()?, topic);

    tokio::spawn(async move {
        wavesync_engine.run().await;
    });

    // Use the diesel connection as usual

    ...

    diesel::insert_into(schema::tasks::table)
                        .values(&new_task)
                        .execute(&mut alice)
                        .expect("Error inserting new task");
}

```