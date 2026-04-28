//! Bench 4 — concurrent same-column conflict resolution.
//!
//! Two peers concurrently update `tasks.title` on the same row, K times
//! each, in parallel. After both peers finish, we wait for convergence
//! (both must report the same title). Reports total wall-clock duration,
//! total updates applied, throughput, and asserts correctness.

use std::time::{Duration, Instant};

use bench_common::{make_peer, task, temp_db_url};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;

const K: usize = 250;
const ROW_ID: &str = "row-conflict";
const TIMEOUT_SECS: u64 = 60;

#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Off)
        .try_init();

    println!(
        "bench_conflict — 2 peers × {} updates to a single column",
        K
    );
    println!();

    let topic = format!("bench-conflict-{}", Uuid::new_v4().simple());
    let url_a = temp_db_url("conflict_a");
    let url_b = temp_db_url("conflict_b");

    let peer_a = make_peer(&url_a, &topic, 0xC1, true).await;
    let peer_b = make_peer(&url_b, &topic, 0xC2, true).await;

    // Wait for them to discover each other.
    println!("Waiting for mDNS discovery...");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let a = !peer_a.network_status().connected_peers.is_empty();
        let b = !peer_b.network_status().connected_peers.is_empty();
        if a && b {
            break;
        }
        if Instant::now() > deadline {
            panic!("peers never discovered each other");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Seed the row on Peer A and let it propagate.
    task::ActiveModel {
        id: Set(ROW_ID.into()),
        title: Set("seed".into()),
        completed: Set(false),
    }
    .insert(&peer_a)
    .await
    .unwrap();

    // Wait until B sees the seed row before starting concurrent writes.
    wait_for_seed(&peer_b).await;

    println!("Seed row visible on both peers. Starting concurrent updates...");
    println!();

    let t = Instant::now();
    let a_handle = {
        let peer = peer_a.clone();
        tokio::spawn(async move {
            for i in 0..K {
                task::ActiveModel {
                    id: Set(ROW_ID.into()),
                    title: Set(format!("A-{:04}", i)),
                    completed: Set(false),
                }
                .update(&peer)
                .await
                .unwrap();
            }
        })
    };
    let b_handle = {
        let peer = peer_b.clone();
        tokio::spawn(async move {
            for i in 0..K {
                task::ActiveModel {
                    id: Set(ROW_ID.into()),
                    title: Set(format!("B-{:04}", i)),
                    completed: Set(false),
                }
                .update(&peer)
                .await
                .unwrap();
            }
        })
    };

    a_handle.await.unwrap();
    b_handle.await.unwrap();
    let writes_done = t.elapsed().as_secs_f64();
    println!(
        "Both peers finished {} updates each ({}-update total) in {:.2}s",
        K,
        K * 2,
        writes_done
    );

    // Wait for convergence.
    println!("Waiting for convergence...");
    let conv_t = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(TIMEOUT_SECS);
    let final_title = loop {
        let a_title = read_title(&peer_a).await;
        let b_title = read_title(&peer_b).await;
        if let (Some(a), Some(b)) = (&a_title, &b_title)
            && a == b
        {
            break a.clone();
        }
        if Instant::now() > deadline {
            panic!(
                "convergence timeout: a={:?} b={:?}",
                a_title, b_title
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let convergence_secs = conv_t.elapsed().as_secs_f64();
    let total = writes_done + convergence_secs;

    println!("  Convergence after concurrent writes: {:.2}s", convergence_secs);
    println!("  Total (writes + convergence):        {:.2}s", total);
    println!(
        "  Throughput (updates applied / total):    {:.0}/s",
        (K * 2) as f64 / total
    );
    println!("  Final agreed title:                  {}", final_title);
    println!();

    let json = serde_json::json!({
        "bench": "conflict",
        "k_per_peer": K,
        "total_updates": K * 2,
        "writes_secs": writes_done,
        "convergence_secs": convergence_secs,
        "total_secs": total,
        "updates_per_sec_total": (K * 2) as f64 / total,
        "final_agreed_title": final_title,
    });
    println!("BENCH_RESULT: {}", json);

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

async fn read_title(peer: &wavesyncdb::WaveSyncDb) -> Option<String> {
    task::Entity::find_by_id(ROW_ID.to_string())
        .one(peer)
        .await
        .unwrap()
        .map(|m| m.title)
}

async fn wait_for_seed(peer: &wavesyncdb::WaveSyncDb) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(Some(_)) = task::Entity::find_by_id(ROW_ID.to_string())
            .one(peer)
            .await
        {
            return;
        }
        if Instant::now() > deadline {
            panic!("seed row never visible on second peer");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
