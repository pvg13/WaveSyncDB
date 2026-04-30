//! Scenario 2 — partition + recovery.
//!
//! Two peers sync normally, then Bob's container is stopped (simulating
//! a crashed app, a phone going to sleep, or a laptop lid close). Alice
//! keeps writing locally while Bob is gone. Bob's container is started
//! back up — Bob reads its existing SQLite state from a persistent
//! Docker volume, the engine restarts, reconnects via the relay, and
//! catches up the missed writes via the version-vector exchange.
//!
//! This exercises three things together:
//! 1. **Real partition**: a stopped container truly has no libp2p path
//!    to the rest of the mesh. Stopping the relay alone wouldn't work
//!    because the two peers, once introduced, hold a direct connection
//!    over the Docker bridge that the relay isn't on the data path of.
//! 2. **Restart durability**: Bob's local SQLite has to survive the
//!    container stop and the engine has to resume from the persisted
//!    `db_version` and shadow tables.
//! 3. **Catch-up correctness**: post-restart, Bob has to converge to
//!    Alice's full state.

use std::time::Duration;

use anyhow::Result;
use bollard::Docker;
use wavesyncdb_e2e::WaveSyncE2eHarness;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_restart_then_rejoin_converges() -> Result<()> {
    let mut harness = WaveSyncE2eHarness::new()
        .with_passphrase("partition-test")
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await?;

    // Phase 1: sync a baseline row through the relay.
    harness
        .peer("alice")
        .insert_task("seed", "baseline", false)
        .await?;
    harness
        .peer("bob")
        .wait_for_task("seed", "baseline", Duration::from_secs(20))
        .await?;

    // Phase 2: stop Bob's container. Alice keeps writing locally;
    // those writes won't reach Bob (no other peer to fan-out to).
    let bob_id = harness.peer("bob").container.id().to_string();
    stop_container(&bob_id).await?;

    for i in 0..5 {
        let id = format!("offline-{i}");
        let title = format!("written while bob was offline {i}");
        harness.peer("alice").insert_task(&id, &title, false).await?;
    }

    // Phase 3: start Bob back up. Bob's `/data/peer.db` survives the
    // restart (declared as VOLUME in the test-peer Dockerfile), so the
    // engine resumes from the prior shadow-table state and triggers a
    // version-vector catch-up.
    start_container(&bob_id).await?;

    // The host port forwarding is reassigned on restart — refresh it
    // before any further HTTP calls.
    harness.peer_mut("bob").refresh_base_url().await?;

    let bob_url = harness.peer("bob").base_url.clone();
    wait_for_http_ready(&bob_url, Duration::from_secs(30)).await?;

    for i in 0..5 {
        let id = format!("offline-{i}");
        let title = format!("written while bob was offline {i}");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, Duration::from_secs(45))
            .await?;
    }

    // Final convergence check: every row, byte for byte.
    let mut a = harness.peer("alice").list_tasks().await?;
    let mut b = harness.peer("bob").list_tasks().await?;
    a.sort_by(|x, y| x.id.cmp(&y.id));
    b.sort_by(|x, y| x.id.cmp(&y.id));
    assert_eq!(a, b, "peers diverged after bob's restart");
    assert_eq!(a.len(), 6, "expected 1 seed + 5 offline writes, got {a:?}");

    Ok(())
}

async fn stop_container(id: &str) -> Result<()> {
    Docker::connect_with_local_defaults()?
        .stop_container(id, None)
        .await
        .map_err(|e| anyhow::anyhow!("stop_container failed: {e}"))
}

async fn start_container(id: &str) -> Result<()> {
    Docker::connect_with_local_defaults()?
        .start_container(id, None)
        .await
        .map_err(|e| anyhow::anyhow!("start_container failed: {e}"))
}

/// Block until `GET {base_url}/health` returns 2xx, or time out.
async fn wait_for_http_ready(base_url: &str, timeout: Duration) -> Result<()> {
    let url = format!("{base_url}/health");
    let start = std::time::Instant::now();
    let client = reqwest::Client::new();
    loop {
        match client.get(&url).send().await {
            Ok(r) if r.status().is_success() => return Ok(()),
            _ => {}
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("HTTP {url} did not become ready within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
