//! Bench 3 — bulk catch-up throughput.
//!
//! Peer A writes N rows while Peer B is detached. Then B is built and
//! attached on the same topic. The bench measures total wall-clock time
//! from B's startup until B has all N rows in its local SQLite. This
//! exercises the version-vector catch-up path with the largest possible
//! single round.

use std::time::{Duration, Instant};

use bench_common::{make_peer, task, temp_db_url};
use sea_orm::{ActiveModelTrait, EntityTrait, PaginatorTrait, Set};
use uuid::Uuid;

const N: usize = 1_000;
const TIMEOUT_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Off)
        .try_init();

    println!("bench_catchup — N = {} rows", N);
    println!();

    let topic = format!("bench-catchup-{}", Uuid::new_v4().simple());

    // Stage 1: bring up A alone, write N rows.
    println!("Stage 1: writing {} rows on Peer A (no Peer B yet)...", N);
    let url_a = temp_db_url("catchup_a");
    let peer_a = make_peer(&url_a, &topic, 0xA1, true).await;

    let t = Instant::now();
    for i in 0..N {
        task::ActiveModel {
            id: Set(format!("row-{:05}", i)),
            title: Set(format!("bench-row-{}", i)),
            completed: Set(false),
        }
        .insert(&peer_a)
        .await
        .unwrap();
    }
    let write_secs = t.elapsed().as_secs_f64();
    println!(
        "  Peer A wrote {} rows in {:.2}s ({:.0}/s)",
        N,
        write_secs,
        N as f64 / write_secs
    );

    // Stage 2: bring up Peer B, time until it sees all N rows.
    println!();
    println!("Stage 2: bringing up Peer B and waiting for convergence...");
    let url_b = temp_db_url("catchup_b");

    let t = Instant::now();
    let peer_b = make_peer(&url_b, &topic, 0xB2, true).await;
    let startup = t.elapsed().as_secs_f64();
    println!("  Peer B engine startup: {:.2}s", startup);

    // Wait for catch-up
    let deadline = Instant::now() + Duration::from_secs(TIMEOUT_SECS);
    let catch_t = Instant::now();
    loop {
        let count = task::Entity::find().count(&peer_b).await.unwrap_or(0);
        if count as usize >= N {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "timed out after {TIMEOUT_SECS}s — Peer B has only {count}/{N} rows"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let catchup_secs = catch_t.elapsed().as_secs_f64();

    let total_secs = startup + catchup_secs;

    println!("  Catch-up duration:   {:.2}s", catchup_secs);
    println!("  Total (startup + sync): {:.2}s", total_secs);
    println!(
        "  Throughput:          {:.0} rows/s during catch-up",
        N as f64 / catchup_secs
    );
    println!();

    let json = serde_json::json!({
        "bench": "catchup",
        "n": N,
        "write_phase_secs": write_secs,
        "engine_startup_secs": startup,
        "catchup_secs": catchup_secs,
        "total_secs": total_secs,
        "rows_per_sec_catchup": N as f64 / catchup_secs,
    });
    println!("BENCH_RESULT: {}", json);

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}
