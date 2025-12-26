use diesel::{Connection, SqliteConnection};
use tokio::sync::mpsc;
use wavesyncdb::sync::{WaveSyncBuilder, WaveSyncEngine};


pub fn new_pool() -> diesel::r2d2::Pool<diesel::r2d2::ConnectionManager<SqliteConnection>> {
    let manager = diesel::r2d2::ConnectionManager::<SqliteConnection>::new(":memory:");
    diesel::r2d2::Pool::builder()
        .max_size(4)
        .build(manager)
        .expect("Failed to create pool")
}

pub fn new_node() -> impl Connection {
    let pool = new_pool();
    let connection = pool.get().expect("Failed to get connection from pool");
    let mut conn = pool.get().expect("Failed to get connection from pool");
    
    let mut wavesyncdb = WaveSyncBuilder::new(connection, "testtopic")
        .connect(&mut conn)
        .build();


    tokio::spawn(async move {
        wavesyncdb.run().await;
    });


    conn 

    
}