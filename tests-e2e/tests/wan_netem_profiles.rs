//! Network-conditions benchmark — measures WaveSyncDB convergence
//! under simulated WAN profiles (cellular, satellite, lossy LAN).
//!
//! These tests are **not** part of the regular CI run. They take
//! several minutes per profile, depend on the kernel's `tc netem`
//! qdisc, and require Docker containers with the `NET_ADMIN`
//! capability. They run locally only.
//!
//! ## Running
//!
//! ```bash
//! ./tests-e2e/build-images.sh   # one-time, image needs iproute2
//! cargo test -p wavesyncdb-e2e --test wan_netem_profiles \
//!     -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `--test-threads=1` because each profile run shapes a Docker bridge
//! interface — concurrent runs would interleave traffic shapes.
//!
//! ## What we measure
//!
//! For each profile:
//!
//! - **time-to-first-sync** — wall-clock from harness `start()` to
//!   the moment Bob sees Alice's first row. Captures cold-start
//!   discovery + relay reservation + circuit dial + first push.
//! - **steady-state propagation p50 / p95** — Alice writes 50 rows;
//!   we measure the time between Alice's `insert` returning and Bob
//!   first seeing each row. Median + 95th-percentile across the run.
//! - **partition-recovery** — apply a "blackhole" netem (100% loss)
//!   for 10 seconds, restore the original profile, measure how long
//!   until a write made *during* the blackhole reaches Bob.
//!
//! Results print as a markdown table at the end of each test, easy
//! to paste into a perf-tracking doc. We don't assert hard
//! thresholds — the point is comparative numbers across profiles
//! and across changes to the engine, not pass/fail gates.

use std::time::{Duration, Instant};

use wavesyncdb_e2e::{NetemProfile, WaveSyncE2eHarness};

/// Total writes per steady-state phase. 50 keeps the run under a
/// minute on every profile while still giving p95 a stable estimate.
const STEADY_STATE_WRITES: usize = 50;

/// Per-write convergence timeout. Cellular_bad propagation can spike
/// to several seconds on a single write — give it room.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// First-write timeout (cold start). Includes harness startup, libp2p
/// dial, circuit-relay reservation. Larger than steady-state.
const COLD_START_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct ProfileResult {
    profile: &'static str,
    time_to_first_sync_ms: u128,
    steady_p50_ms: u128,
    steady_p95_ms: u128,
    partition_recovery_ms: u128,
}

async fn run_profile(profile: NetemProfile) -> ProfileResult {
    let profile_name = profile.name;
    eprintln!("\n==== Running profile: {profile_name} ====");

    let mut harness = WaveSyncE2eHarness::new()
        .with_passphrase("netem-bench")
        .with_netem(profile.clone())
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // ── Phase 1: time-to-first-sync ──────────────────────────────
    let t_first = Instant::now();
    harness
        .peer("alice")
        .insert_task("first", "hello", false)
        .await
        .expect("alice initial insert");
    harness
        .peer("bob")
        .wait_for_task("first", "hello", COLD_START_TIMEOUT)
        .await
        .expect("first-sync timeout");
    let time_to_first_sync_ms = t_first.elapsed().as_millis();
    eprintln!("  time_to_first_sync = {time_to_first_sync_ms} ms");

    // ── Phase 2: steady-state propagation ────────────────────────
    let mut latencies_ms: Vec<u128> = Vec::with_capacity(STEADY_STATE_WRITES);
    for i in 0..STEADY_STATE_WRITES {
        let id = format!("steady-{i}");
        let title = format!("write {i}");
        let t = Instant::now();
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice steady insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, WRITE_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("steady-state convergence timeout on write {i}"));
        latencies_ms.push(t.elapsed().as_millis());
    }
    latencies_ms.sort_unstable();
    let p50 = latencies_ms[STEADY_STATE_WRITES / 2];
    let p95 = latencies_ms[(STEADY_STATE_WRITES * 95) / 100];
    eprintln!("  steady_p50 = {p50} ms,  steady_p95 = {p95} ms");

    // ── Phase 3: partition recovery ──────────────────────────────
    // Apply 100% loss on Alice's eth0 ("Alice walked into a tunnel").
    // Bob keeps its original profile and Bob's HTTP port-forward stays
    // reachable from the host, so we write via Bob during the
    // blackhole — Bob's libp2p push to Alice drops at Alice's eth0.
    // After the blackhole lifts, we measure how long Alice takes to
    // catch up. This is exactly the "phone reception comes back"
    // scenario.
    //
    // We can't write through Alice during the blackhole because 100%
    // egress loss also drops the response packets to the host's port
    // forward. Selective filtering (only shape inter-container
    // traffic) is a TODO if we ever need to test "write while
    // partitioned" specifically; for now the write-via-bob shape
    // captures the same convergence behavior.
    let blackhole = NetemProfile::custom("blackhole", 1, 0, 100.0, None);
    harness
        .peer_mut("alice")
        .set_netem(blackhole)
        .await
        .expect("apply blackhole");

    harness
        .peer("bob")
        .insert_task("during-partition", "offline write", false)
        .await
        .expect("bob insert during alice blackhole");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let t_heal = Instant::now();
    harness
        .peer_mut("alice")
        .set_netem(profile.clone())
        .await
        .expect("restore profile");

    // Generous timeout: 5s of 100% loss can kill the libp2p QUIC
    // connection, forcing a full reconnect (relay dial + circuit
    // reservation + identify + first sync). 120s gives us margin to
    // observe the *real* recovery time across all profiles.
    harness
        .peer("alice")
        .wait_for_task(
            "during-partition",
            "offline write",
            Duration::from_secs(120),
        )
        .await
        .expect("alice did not catch up after partition");
    let partition_recovery_ms = t_heal.elapsed().as_millis();
    eprintln!("  partition_recovery = {partition_recovery_ms} ms");

    ProfileResult {
        profile: profile_name,
        time_to_first_sync_ms,
        steady_p50_ms: p50,
        steady_p95_ms: p95,
        partition_recovery_ms,
    }
}

fn print_results_table(rows: &[ProfileResult]) {
    eprintln!("\n=== Netem profile benchmark results ===\n");
    eprintln!(
        "| profile        | first-sync | steady p50 | steady p95 | partition recovery |"
    );
    eprintln!(
        "| -------------- | ---------- | ---------- | ---------- | ------------------ |"
    );
    for r in rows {
        eprintln!(
            "| {:<14} | {:>7} ms | {:>7} ms | {:>7} ms | {:>15} ms |",
            r.profile,
            r.time_to_first_sync_ms,
            r.steady_p50_ms,
            r.steady_p95_ms,
            r.partition_recovery_ms,
        );
    }
    eprintln!();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; requires netem + Docker. Run with `cargo test --test wan_netem_profiles -- --ignored --nocapture --test-threads=1`."]
async fn netem_profile_bench_lan_fast() {
    let r = run_profile(NetemProfile::lan_fast()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_cellular_fair() {
    let r = run_profile(NetemProfile::cellular_fair()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_cellular_bad() {
    let r = run_profile(NetemProfile::cellular_bad()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_satellite() {
    let r = run_profile(NetemProfile::satellite()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_lossy_lan() {
    let r = run_profile(NetemProfile::lossy_lan()).await;
    print_results_table(&[r]);
}

/// Single test that runs the full matrix sequentially and prints one
/// consolidated table. Slower (5–10 minutes) but produces a single
/// artifact for perf comparison across engine versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark, full matrix; run explicitly with --ignored."]
async fn netem_profile_bench_full_matrix() {
    let mut rows = Vec::new();
    for p in [
        NetemProfile::lan_fast(),
        NetemProfile::cellular_fair(),
        NetemProfile::cellular_bad(),
        NetemProfile::satellite(),
        NetemProfile::lossy_lan(),
    ] {
        rows.push(run_profile(p).await);
    }
    print_results_table(&rows);
}
