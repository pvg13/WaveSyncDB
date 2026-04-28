//! Bench 2 — peer-to-peer write latency.
//!
//! Two in-process peers on the same topic, mDNS enabled. Peer B subscribes
//! to `change_rx` before each round; we time from `task.insert(&peer_a)`
//! returning to `peer_b.change_rx().recv().await` resolving for the matching
//! row. Reports p50 / p95 / p99 / max.

use std::time::{Duration, Instant};

use bench_common::{make_peer, percentile, task, temp_db_url};
use sea_orm::{ActiveModelTrait, Set};
use tokio::sync::broadcast::error::RecvError;
use uuid::Uuid;
use wavesyncdb::WriteKind;

const N: usize = 100;
const WAIT_FOR_PEER_TIMEOUT_SECS: u64 = 30;

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Off)
        .try_init();

    println!("bench_lan_sync — N = {} sequential inserts", N);
    println!();

    let topic = format!("bench-lan-sync-{}", Uuid::new_v4().simple());
    let url_a = temp_db_url("lan_a");
    let url_b = temp_db_url("lan_b");

    let peer_a = make_peer(&url_a, &topic, 0xA1, true).await;
    let peer_b = make_peer(&url_b, &topic, 0xB2, true).await;

    println!("Waiting for mDNS peers to discover each other...");
    wait_for_connection(&peer_a, &peer_b).await;
    println!("Connected. Running {} timed inserts...", N);
    println!();

    let mut samples_us: Vec<u128> = Vec::with_capacity(N);

    for i in 0..N {
        let row_id = format!("row-{:04}", i);
        let mut rx = peer_b.change_rx();

        let t = Instant::now();
        task::ActiveModel {
            id: Set(row_id.clone()),
            title: Set(format!("bench {}", i)),
            completed: Set(false),
        }
        .insert(&peer_a)
        .await
        .unwrap();

        // Wait for the matching notification on B.
        loop {
            match rx.recv().await {
                Ok(n)
                    if n.table == "tasks"
                        && matches!(n.kind, WriteKind::Insert)
                        && n.primary_key.0 == row_id =>
                {
                    samples_us.push(t.elapsed().as_micros());
                    break;
                }
                Ok(_) => continue,
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => panic!("peer_b change_rx closed mid-bench"),
            }
        }
    }

    let mut sorted = samples_us.clone();
    let p50 = percentile(&mut sorted, 50.0) as f64 / 1000.0;
    let mut sorted = samples_us.clone();
    let p95 = percentile(&mut sorted, 95.0) as f64 / 1000.0;
    let mut sorted = samples_us.clone();
    let p99 = percentile(&mut sorted, 99.0) as f64 / 1000.0;
    let max = samples_us.iter().copied().max().unwrap_or(0) as f64 / 1000.0;
    let mean: f64 = samples_us.iter().map(|v| *v as f64).sum::<f64>()
        / samples_us.len() as f64
        / 1000.0;

    println!(
        "{:<10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "metric", "mean", "p50", "p95", "p99", "max"
    );
    println!(
        "{:-<10} {:->10} {:->10} {:->10} {:->10} {:->10}",
        "", "", "", "", "", ""
    );
    println!(
        "{:<10} {:>8.2}ms {:>8.2}ms {:>8.2}ms {:>8.2}ms {:>8.2}ms",
        "latency", mean, p50, p95, p99, max
    );

    let json = serde_json::json!({
        "bench": "lan_sync",
        "n": N,
        "latency_ms": {
            "mean": mean,
            "p50": p50,
            "p95": p95,
            "p99": p99,
            "max": max,
        },
    });
    println!();
    println!("BENCH_RESULT: {}", json);

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

async fn wait_for_connection(peer_a: &wavesyncdb::WaveSyncDb, peer_b: &wavesyncdb::WaveSyncDb) {
    let deadline = Instant::now() + Duration::from_secs(WAIT_FOR_PEER_TIMEOUT_SECS);
    loop {
        let a_seen_b = !peer_a.network_status().connected_peers.is_empty();
        let b_seen_a = !peer_b.network_status().connected_peers.is_empty();
        if a_seen_b && b_seen_a {
            return;
        }
        if Instant::now() > deadline {
            panic!("peers never discovered each other within {WAIT_FOR_PEER_TIMEOUT_SECS}s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
