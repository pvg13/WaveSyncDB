//! WaveSyncDB performance benchmarks.
//!
//! Measures the real cost of sync interception and the end-to-end
//! propagation latency between peers.
//!
//! Run with: cargo bench -p wavesyncdb --bench sync_benchmarks

use std::time::{Duration, Instant};

use sea_orm::{ActiveModelTrait, ConnectionTrait, Database, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{NodeId, WaveSyncDb, WaveSyncDbBuilder};

// ── Test entity ──

mod task {
    use sea_orm::entity::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
    #[sea_orm(table_name = "tasks")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub title: String,
        pub completed: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
    impl ActiveModelBehavior for ActiveModel {}
}

fn make_node_id(seed: u8) -> NodeId {
    let mut id = [0u8; 16];
    id[0] = seed;
    id[15] = 0xBE; // bench seed marker
    NodeId(id)
}

fn mem_db(name: &str) -> String {
    let unique = Uuid::new_v4().simple().to_string();
    let path = std::env::temp_dir().join(format!("wavesync_bench_{name}_{unique}.db"));
    format!("sqlite:{}?mode=rwc", path.display())
}

// ── Benchmarks ──

#[tokio::main]
async fn main() {
    println!("=== WaveSyncDB Performance Benchmarks ===\n");

    bench_write_overhead().await;
    bench_read_overhead().await;
    bench_shadow_table_cost().await;
    bench_sql_parsing().await;
    bench_sync_propagation().await;
    bench_conflict_resolution().await;
    bench_batch_writes().await;

    println!("\n=== Done ===");
}

/// Measure the overhead of writing through WaveSyncDb vs raw SQLite.
async fn bench_write_overhead() {
    println!("--- Write Interception Overhead ---");

    let n = 500;
    let db_url = mem_db("write_overhead");

    // Raw SQLite (no sync)
    let raw_db = Database::connect(&db_url).await.unwrap();
    raw_db
        .execute_unprepared(
            "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', completed INTEGER NOT NULL DEFAULT 0)",
        )
        .await
        .unwrap();

    let start = Instant::now();
    for i in 0..n {
        raw_db
            .execute_unprepared(&format!(
                "INSERT INTO tasks (id, title, completed) VALUES ('raw-{i}', 'Task {i}', 0)"
            ))
            .await
            .unwrap();
    }
    let raw_elapsed = start.elapsed();
    raw_db.close().await.unwrap();
    std::fs::remove_file(db_url.replace("sqlite:", "").split('?').next().unwrap()).ok();

    // WaveSyncDb (with sync interception)
    let sync_url = mem_db("write_overhead_sync");
    let sync_db = WaveSyncDbBuilder::new(&sync_url, "bench-write")
        .with_node_id(make_node_id(230))
        .with_sync_interval(Duration::from_secs(600)) // disable periodic sync
        .build()
        .await
        .unwrap();
    sync_db
        .schema()
        .register(task::Entity)
        .sync()
        .await
        .unwrap();

    let start = Instant::now();
    for i in 0..n {
        task::ActiveModel {
            id: Set(format!("sync-{i}")),
            title: Set(format!("Task {i}")),
            completed: Set(false),
            ..Default::default()
        }
        .insert(&sync_db)
        .await
        .unwrap();
    }
    let sync_elapsed = start.elapsed();
    sync_db.shutdown().await;

    let raw_per_op = raw_elapsed / n;
    let sync_per_op = sync_elapsed / n;
    let overhead = if raw_per_op.as_nanos() > 0 {
        ((sync_per_op.as_nanos() as f64 / raw_per_op.as_nanos() as f64) - 1.0) * 100.0
    } else {
        0.0
    };

    println!("  Raw SQLite:   {n} inserts in {raw_elapsed:?} ({raw_per_op:?}/op)");
    println!("  WaveSyncDb:   {n} inserts in {sync_elapsed:?} ({sync_per_op:?}/op)");
    println!("  Overhead:     {overhead:.1}%");
    println!();
}

/// Measure read performance (reads bypass sync interception).
async fn bench_read_overhead() {
    println!("--- Read Performance ---");

    let n = 1000;
    let db_url = mem_db("read_overhead");
    let db = WaveSyncDbBuilder::new(&db_url, "bench-read")
        .with_node_id(make_node_id(231))
        .with_sync_interval(Duration::from_secs(600))
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    // Seed data
    for i in 0..100 {
        task::ActiveModel {
            id: Set(format!("read-{i}")),
            title: Set(format!("Task {i}")),
            completed: Set(false),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
    }

    // Benchmark reads
    let start = Instant::now();
    for _ in 0..n {
        let _ = task::Entity::find().all(&db).await.unwrap();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed / n;

    println!("  {n} SELECT * (100 rows): {elapsed:?} ({per_op:?}/query)");
    db.shutdown().await;
    println!();
}

/// Measure shadow table write cost in isolation.
async fn bench_shadow_table_cost() {
    println!("--- Shadow Table Cost ---");

    let n = 500;
    let db_url = mem_db("shadow_cost");
    let db = Database::connect(&db_url).await.unwrap();

    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', completed INTEGER NOT NULL DEFAULT 0)",
    )
    .await
    .unwrap();
    wavesyncdb::shadow::create_shadow_table(&db, "tasks")
        .await
        .unwrap();
    wavesyncdb::shadow::create_meta_table(&db).await.unwrap();

    let site_id = NodeId([1u8; 16]);

    let start = Instant::now();
    for i in 0..n {
        wavesyncdb::shadow::upsert_clock_entry(
            &db,
            "tasks",
            &format!("pk-{i}"),
            "title",
            1,
            i as u64 + 1,
            &site_id,
            0,
        )
        .await
        .unwrap();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed / n;

    println!("  {n} shadow upserts: {elapsed:?} ({per_op:?}/op)");
    db.close().await.unwrap();
    println!();
}

/// Measure SQL parsing speed (classify_write + parse_write_full).
async fn bench_sql_parsing() {
    println!("--- SQL Parsing ---");

    let n = 10_000u32;
    let sql =
        "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('task-1', 'Buy milk', 0)";

    let start = Instant::now();
    for _ in 0..n {
        let _ = wavesyncdb::classify_write(sql);
    }
    let classify_elapsed = start.elapsed();

    let start = Instant::now();
    for _ in 0..n {
        let _ = wavesyncdb::parse_write_full(sql, "id");
    }
    let parse_elapsed = start.elapsed();

    println!(
        "  classify_write:   {n} calls in {classify_elapsed:?} ({:?}/op)",
        classify_elapsed / n
    );
    println!(
        "  parse_write_full: {n} calls in {parse_elapsed:?} ({:?}/op)",
        parse_elapsed / n
    );
    println!();
}

/// Measure end-to-end sync propagation between two peers.
async fn bench_sync_propagation() {
    println!("--- Sync Propagation (peer-to-peer via mDNS) ---");

    let topic = format!("bench-sync-{}", Uuid::new_v4());
    let db_a_url = mem_db("sync_a");
    let db_b_url = mem_db("sync_b");

    let peer_a = WaveSyncDbBuilder::new(&db_a_url, &topic)
        .with_node_id(make_node_id(232))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&db_b_url, &topic)
        .with_node_id(make_node_id(233))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Wait for peers to discover each other
    let discovery_start = Instant::now();
    loop {
        let status = peer_a.network_status();
        if status.connected_peers.len() > 0 {
            break;
        }
        if discovery_start.elapsed() > Duration::from_secs(10) {
            println!("  SKIPPED: peers did not discover each other in 10s");
            peer_a.shutdown().await;
            peer_b.shutdown().await;
            println!();
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let discovery_time = discovery_start.elapsed();
    println!("  Peer discovery: {discovery_time:?}");

    // Measure propagation: A writes, measure until B sees it
    let id = Uuid::new_v4().to_string();
    let write_start = Instant::now();
    task::ActiveModel {
        id: Set(id.clone()),
        title: Set("Benchmark task".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Poll B until it has the row
    loop {
        let rows = task::Entity::find().all(&peer_b).await.unwrap();
        if rows.iter().any(|r| r.id == id) {
            break;
        }
        if write_start.elapsed() > Duration::from_secs(15) {
            println!("  TIMEOUT: propagation did not complete in 15s");
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let propagation_time = write_start.elapsed();
    println!("  Write → remote visible: {propagation_time:?}");

    peer_a.shutdown().await;
    peer_b.shutdown().await;
    println!();
}

/// Measure conflict resolution throughput.
async fn bench_conflict_resolution() {
    println!("--- Conflict Resolution ---");

    use wavesyncdb::conflict;

    let n = 100_000u32;
    let remote_val = serde_json::to_vec(&serde_json::json!("remote value")).unwrap();
    let local_val = serde_json::to_vec(&serde_json::json!("local value")).unwrap();
    let remote_site = NodeId([2u8; 16]);
    let local_site = NodeId([1u8; 16]);

    let start = Instant::now();
    for i in 0..n {
        let _ = conflict::should_apply_column(
            (i as u64) + 1,
            &remote_val,
            &remote_site,
            i as u64,
            &local_val,
            &local_site,
        );
    }
    let elapsed = start.elapsed();

    println!(
        "  {n} conflict resolutions: {elapsed:?} ({:?}/op)",
        elapsed / n
    );
    println!();
}

/// Measure batch write throughput.
async fn bench_batch_writes() {
    println!("--- Batch Write Throughput ---");

    let batch_sizes = [10, 50, 100, 500];

    for &size in &batch_sizes {
        let db_url = mem_db(&format!("batch_{size}"));
        let db = WaveSyncDbBuilder::new(&db_url, "bench-batch")
            .with_node_id(make_node_id(234))
            .with_sync_interval(Duration::from_secs(600))
            .build()
            .await
            .unwrap();
        db.schema().register(task::Entity).sync().await.unwrap();

        let start = Instant::now();
        for i in 0..size {
            task::ActiveModel {
                id: Set(format!("batch-{i}")),
                title: Set(format!("Task {i}")),
                completed: Set(false),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
        }
        let elapsed = start.elapsed();
        let per_op = elapsed / size;
        let ops_sec = if elapsed.as_secs_f64() > 0.0 {
            size as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        println!("  {size:>4} writes: {elapsed:?} ({per_op:?}/op, {ops_sec:.0} ops/s)");
        db.shutdown().await;
    }
    println!();
}
