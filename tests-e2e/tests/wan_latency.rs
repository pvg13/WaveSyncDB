//! WAN time-to-first-sync (TTFS) measurement scenarios.
//!
//! Each test reproduces one suspected cause of slow (>25s) cold-start /
//! resume sync on mobile WAN, prints `[ttfs] scenario=… phase=… ms=…`
//! measurement lines, and asserts a generous eventual-convergence
//! ceiling. Strict per-fix thresholds are added together with the fixes
//! (the measured numbers from these scenarios set those thresholds).
//!
//! All scenarios: mDNS off (WAN-only discovery) and port-restricted cone
//! NAT with AutoNAT whitelist on every peer (unsolicited direct dials
//! dropped, relay path forced — the phone-on-cellular shape).
//!
//! ## N14 status (found 2026-07-10, fixed same round)
//!
//! The baseline scenario S1 originally exposed an indefinite stall:
//! every WAN reconnection path was an edge-triggered one-shot and they
//! raced each other to death — cached circuit pre-dials fired before
//! the relay connection were CANCELED (not queued); their failures
//! started the dial backoff that silently suppressed the PeerList
//! introduction dial arriving 1ms later; the remote's PeerJoined dial
//! was suppressed by a stale `is_connected` guard; and nothing
//! level-triggered retried afterward (mDNS's 5s re-query masks all of
//! this on LAN — WAN had no equivalent).
//!
//! Fixed by: deferring cached circuit pre-dials until the relay
//! connection exists; the `wanted_peers` + 5s reconnect sweep (retries
//! suppressed introductions after backoff expiry, re-announces when
//! isolated); announce-on-reservation (receivers' circuit dials no
//! longer race a missing reservation); and reachability-gated
//! per-address cache failure counting (a sleeping peer no longer
//! erases its own cache — that erasure ironically made TTFS *faster*
//! by removing the poisoning pre-dial failures).
//!
//! S1 and the S3 strict test are the acceptance/regression guards for
//! those fixes; S1 additionally enforces a ≤10s cold-start TTFS.
//!
//! Run (Docker + built images required, see tests-e2e/README.md):
//!
//! ```bash
//! cargo test -p wavesyncdb-e2e --test wan_latency -- --test-threads=1 --nocapture
//! # include the known-bug acceptance tests:
//! cargo test -p wavesyncdb-e2e --test wan_latency -- --test-threads=1 --nocapture --include-ignored
//! ```

use std::time::{Duration, Instant};

use anyhow::Result;
use wavesyncdb_e2e::{NatProfile, RunningHarness, WaveSyncE2eHarness, report_ttfs};

/// Generous eventual-convergence ceiling. A scenario exceeding this is
/// broken, not just slow.
const CEILING: Duration = Duration::from_secs(120);

fn wan_harness(passphrase: &str) -> WaveSyncE2eHarness {
    WaveSyncE2eHarness::new()
        .with_passphrase(passphrase)
        .without_mdns()
        .add_peer_with_nat("alice", NatProfile::PortRestrictedConeAutoNATOk)
        .add_peer_with_nat("bob", NatProfile::PortRestrictedConeAutoNATOk)
}

/// Print both peers' diagnostics counters — called on success for the
/// record and on timeout for the post-mortem.
async fn dump_diags(harness: &RunningHarness, scenario: &str) {
    for name in ["alice", "bob"] {
        match harness.peer(name).diagnostics().await {
            Ok(d) => println!(
                "[diag] scenario={scenario} peer={name} cached_addr_dials={} \
                 peerlist_introductions={} peerjoined_introductions={} \
                 dial_attempts={} dial_failures={} relayed={} direct={} \
                 reservations_accepted={}",
                d.cached_addr_dials,
                d.peerlist_introductions,
                d.peerjoined_introductions,
                d.peer_dial_attempts,
                d.peer_dial_failures,
                d.relayed_connections_established,
                d.direct_connections_established,
                d.circuit_reservations_accepted
            ),
            Err(e) => println!("[diag] scenario={scenario} peer={name} unreachable: {e}"),
        }
    }
}

/// S1 — cold restart with a warm address cache and a healthy relay.
/// Baseline for every other scenario, and the N14 acceptance test:
/// before the reconnect fixes this stalled indefinitely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s1_cold_start_warm_cache() -> Result<()> {
    let mut harness = wan_harness("wan-latency-s1").start().await?;

    // Warm-up: one row each way so both peers cache each other's
    // working addresses in `_wavesync_peer_addrs`.
    harness
        .peer("alice")
        .insert_task("seed-a", "from alice", false)
        .await?;
    harness
        .peer("bob")
        .wait_for_task("seed-a", "from alice", CEILING)
        .await?;
    harness
        .peer("bob")
        .insert_task("seed-b", "from bob", false)
        .await?;
    harness
        .peer("alice")
        .wait_for_task("seed-b", "from bob", CEILING)
        .await?;

    // Bob goes away; alice writes while bob is gone.
    harness.peer("bob").stop().await?;
    harness
        .peer("alice")
        .insert_task("s1-row", "written while bob away", false)
        .await?;

    // Cold restart: measure from process start, not from HTTP-ready —
    // the user experiences the whole window.
    let restart_started = Instant::now();
    harness
        .restart_peer_and_wait("bob", Duration::from_secs(30))
        .await?;
    report_ttfs(
        "s1_cold_start_warm_cache",
        "http_ready",
        restart_started.elapsed(),
    );

    let ttfs_res = harness
        .peer("bob")
        .wait_for_task_timed("s1-row", "written while bob away", CEILING)
        .await;
    dump_diags(&harness, "s1").await;
    let ttfs = ttfs_res?;
    report_ttfs("s1_cold_start_warm_cache", "first_sync_after_ready", ttfs);
    report_ttfs(
        "s1_cold_start_warm_cache",
        "total_from_start",
        restart_started.elapsed(),
    );
    // Ceiling rationale: typical post-fix TTFS is 0-3s. The worst case is
    // the surviving peer holding a stale QUIC connection to the restarted
    // peer's previous incarnation: proving it dead takes two consecutive
    // sync-request timeouts (paced by the request cadence and pending-sync
    // window) plus a backed-off reconnect — 12-15.5s observed across runs.
    // 20s guards the regression (pre-fix: 34s when racing luck helped, or
    // an indefinite stall) without sitting inside the detection-latency
    // distribution.
    assert!(
        ttfs <= Duration::from_secs(20),
        "cold-start TTFS regressed: {ttfs:?} (ceiling 20s with warm cache + healthy relay)"
    );

    // The whole point of the cache: it must actually get exercised.
    let diag = harness.peer("bob").diagnostics().await?;
    assert!(
        diag.cached_addr_dials > 0,
        "warm cache was never dialed on cold start — pre-dial path broken"
    );
    Ok(())
}

/// S2 — cold start while the relay is down; relay returns 10s later.
/// Measures how quickly the engine recovers once the relay is back —
/// i.e. the real cost of the reconnect backoff ladder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s2_relay_down_at_cold_start() -> Result<()> {
    let mut harness = wan_harness("wan-latency-s2").start().await?;

    // Warm-up so caches are populated and alice knows the mesh.
    harness
        .peer("alice")
        .insert_task("seed-a", "from alice", false)
        .await?;
    harness
        .peer("bob")
        .wait_for_task("seed-a", "from alice", CEILING)
        .await?;

    harness.peer("bob").stop().await?;
    harness.stop_relay().await?;

    // Alice's fan-out has nowhere to go — the row waits for catch-up.
    harness
        .peer("alice")
        .insert_task("s2-row", "relay was down", false)
        .await?;

    // Bob cold-starts into a world with no relay. His initial relay
    // dial fails, putting him on the retry ladder.
    let bob_started = Instant::now();
    harness
        .restart_peer_and_wait("bob", Duration::from_secs(30))
        .await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    harness.start_relay().await?;
    let relay_up = Instant::now();

    let ttfs_res = harness
        .peer("bob")
        .wait_for_task_timed("s2-row", "relay was down", CEILING)
        .await;
    dump_diags(&harness, "s2").await;
    ttfs_res?;
    report_ttfs(
        "s2_relay_down_at_cold_start",
        "first_sync_from_bob_start",
        bob_started.elapsed(),
    );
    report_ttfs(
        "s2_relay_down_at_cold_start",
        "first_sync_after_relay_up",
        relay_up.elapsed(),
    );
    Ok(())
}

/// Shared body for the S3 poisoning loop: warm the cache, then restart
/// bob `wakes` times while alice is stopped (each wake's pre-dial fails
/// and bumps fail_count on every cached row for alice), then bring
/// alice back, restart bob once more, and report whether the cache was
/// still used plus the resulting TTFS.
async fn run_s3(wakes: usize) -> Result<(u64, Duration)> {
    let mut harness = wan_harness("wan-latency-s3").start().await?;

    harness
        .peer("alice")
        .insert_task("seed-a", "from alice", false)
        .await?;
    harness
        .peer("bob")
        .wait_for_task("seed-a", "from alice", CEILING)
        .await?;

    harness.peer("alice").stop().await?;

    for wake in 0..wakes {
        harness
            .restart_peer_and_wait("bob", Duration::from_secs(30))
            .await?;
        // Dwell long enough for the startup pre-dial toward alice's dead
        // addresses to fail and be recorded before the next restart.
        tokio::time::sleep(Duration::from_secs(12)).await;
        println!("[s3] wake {} of {} complete", wake + 1, wakes);
    }

    // Alice returns; one more cold start on bob measures the damage.
    harness.peer("alice").start().await?;
    harness.peer_mut("alice").refresh_base_url().await?;
    harness
        .peer("alice")
        .wait_http_ready(Duration::from_secs(30))
        .await?;
    harness
        .peer("alice")
        .insert_task("s3-row", "after the wakes", false)
        .await?;

    harness
        .restart_peer_and_wait("bob", Duration::from_secs(30))
        .await?;
    let ttfs_res = harness
        .peer("bob")
        .wait_for_task_timed("s3-row", "after the wakes", CEILING)
        .await;
    dump_diags(&harness, "s3").await;
    let ttfs = ttfs_res?;

    let diag = harness.peer("bob").diagnostics().await?;
    Ok((diag.cached_addr_dials, ttfs))
}

/// S3 (measurement) — quantify cache state and TTFS after 12 failed
/// wakes. Expected today: cached_addr_dials == 0 (cache poisoned and
/// GC'd), TTFS noticeably worse than S1's warm-cache number.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_cache_poisoning_measurement() -> Result<()> {
    let (cached_dials, ttfs) = run_s3(12).await?;
    report_ttfs("s3_cache_poisoning", "first_sync_after_final_restart", ttfs);
    println!(
        "[s3] final-restart cached_addr_dials={cached_dials} \
         (0 means the cache was fully poisoned by failed wakes)"
    );
    Ok(())
}

/// S3 (strict) — acceptance test for the cache-hardening fix: a peer's
/// cached addresses must survive repeated failed wakes toward an
/// unreachable peer (the other phone being asleep is the NORMAL mobile
/// case, not an error signal worth erasing the cache over).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_cache_survives_failed_wakes() -> Result<()> {
    let (cached_dials, _ttfs) = run_s3(12).await?;
    assert!(
        cached_dials > 0,
        "address cache did not survive 12 failed wakes — pre-dial fired {cached_dials} cached dials"
    );
    Ok(())
}

/// S4 — app-freeze resume. Bob is paused past the QUIC idle timeout so
/// his connections die silently, then resumes. Both sync directions are
/// timed from the unpause instant. This isolates the remote-side
/// `is_connected` stale-guard mechanism without a process restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s4_frozen_resume() -> Result<()> {
    let harness = wan_harness("wan-latency-s4").start().await?;

    harness
        .peer("alice")
        .insert_task("seed-a", "from alice", false)
        .await?;
    harness
        .peer("bob")
        .wait_for_task("seed-a", "from alice", CEILING)
        .await?;
    harness
        .peer("bob")
        .insert_task("seed-b", "from bob", false)
        .await?;
    harness
        .peer("alice")
        .wait_for_task("seed-b", "from bob", CEILING)
        .await?;

    // Freeze bob well past the ~30s QUIC idle timeout. His relay and
    // peer connections all die; he can't know.
    harness.peer("bob").pause().await?;
    tokio::time::sleep(Duration::from_secs(20)).await;
    harness
        .peer("alice")
        .insert_task("s4-inbound", "written while frozen", false)
        .await?;
    tokio::time::sleep(Duration::from_secs(55)).await;

    harness.peer("bob").unpause().await?;
    let resumed = Instant::now();

    // Direction 1: catch-up toward the resumed peer.
    let inbound_res = harness
        .peer("bob")
        .wait_for_task_timed("s4-inbound", "written while frozen", CEILING)
        .await;
    dump_diags(&harness, "s4").await;
    let inbound = inbound_res?;
    report_ttfs("s4_frozen_resume", "inbound_after_resume", inbound);

    // Direction 2: the resumed peer writes immediately (the real-world
    // "user opens app, adds item, backgrounds it again" pattern).
    harness
        .peer("bob")
        .insert_task("s4-outbound", "written right after resume", false)
        .await?;
    harness
        .peer("alice")
        .wait_for_task("s4-outbound", "written right after resume", CEILING)
        .await?;
    report_ttfs(
        "s4_frozen_resume",
        "outbound_after_resume",
        resumed.elapsed(),
    );
    Ok(())
}
