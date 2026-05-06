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

    // Profile-aware timeout. 5s of 100% loss kills the libp2p QUIC
    // connection, forcing a full reconnect (relay dial + circuit
    // reservation + identify + first push). On low-RTT profiles a
    // few seconds is enough; on satellite (1.2s RTT) each step
    // costs an RTT and 120s isn't enough. Scale: 60s baseline plus
    // 100ms per ms of one-way latency, capped at 5 minutes.
    let partition_timeout_secs = (60 + profile.latency_ms / 10).min(300) as u64;
    harness
        .peer("alice")
        .wait_for_task(
            "during-partition",
            "offline write",
            Duration::from_secs(partition_timeout_secs),
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
async fn netem_profile_bench_ideal() {
    let r = run_profile(NetemProfile::ideal()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_lan_fast() {
    let r = run_profile(NetemProfile::lan_fast()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_ethernet_gigabit() {
    let r = run_profile(NetemProfile::ethernet_gigabit()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_wifi_home() {
    let r = run_profile(NetemProfile::wifi_home()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_wifi_busy() {
    let r = run_profile(NetemProfile::wifi_busy()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_wifi_distant() {
    let r = run_profile(NetemProfile::wifi_distant()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_mobile_5g() {
    let r = run_profile(NetemProfile::mobile_5g()).await;
    print_results_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark; see file-level docs."]
async fn netem_profile_bench_mobile_3g() {
    let r = run_profile(NetemProfile::mobile_3g()).await;
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
/// consolidated table. Slower (~15 minutes for the full set) but
/// produces a single artifact for perf comparison across engine
/// versions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only WAN benchmark, full matrix; run explicitly with --ignored."]
async fn netem_profile_bench_full_matrix() {
    let mut rows = Vec::new();
    for p in [
        NetemProfile::ideal(),
        NetemProfile::lan_fast(),
        NetemProfile::ethernet_gigabit(),
        NetemProfile::wifi_home(),
        NetemProfile::wifi_busy(),
        NetemProfile::wifi_distant(),
        NetemProfile::mobile_5g(),
        NetemProfile::cellular_fair(),
        NetemProfile::cellular_bad(),
        NetemProfile::mobile_3g(),
        NetemProfile::satellite(),
        NetemProfile::lossy_lan(),
    ] {
        rows.push(run_profile(p).await);
    }
    print_results_table(&rows);
}

// ─── Extended-scenario benches ────────────────────────────────────────
//
// These exercise specific failure shapes the basic profile bench
// doesn't cover: long offline periods (30s blackhole + writes during
// the gap), container restart (engine cold-restart with the cached
// peer-addresses path from #29), and asymmetric pairs (one peer on
// good network, one on bad).

/// Result of an extended-scenario bench.
#[derive(Debug)]
struct ScenarioResult {
    scenario: String,
    profile: String,
    /// How long it took for the scenario's terminal assertion to
    /// converge after whatever recovery event triggered it.
    recovery_ms: u128,
    /// How many of the writes-during-disruption successfully made it
    /// to the receiving peer.
    writes_recovered: usize,
    /// Total writes attempted during the disruption window. Pass-rate
    /// is `writes_recovered / writes_total`.
    writes_total: usize,
}

fn print_scenario_table(rows: &[ScenarioResult]) {
    eprintln!("\n=== Extended-scenario benchmark results ===\n");
    eprintln!(
        "| scenario                  | profile         | recovery   | recovered |"
    );
    eprintln!(
        "| ------------------------- | --------------- | ---------- | --------- |"
    );
    for r in rows {
        eprintln!(
            "| {:<25} | {:<15} | {:>7} ms | {:>4}/{:<4} |",
            r.scenario,
            r.profile,
            r.recovery_ms,
            r.writes_recovered,
            r.writes_total,
        );
    }
    eprintln!();
}

/// "Phone in airplane mode for 30 seconds, then comes back online."
/// Bob writes 10 rows while Alice's libp2p is blackholed. After heal,
/// every write must reach Alice and we measure how long the *last*
/// missing write takes to land — the worst-case catch-up delay.
async fn run_long_offline_scenario(profile: NetemProfile) -> ScenarioResult {
    let profile_name = profile.name.to_string();
    let scenario = "long_offline_30s".to_string();
    eprintln!("\n==== {scenario} on profile: {profile_name} ====");

    let mut harness = WaveSyncE2eHarness::new()
        .with_passphrase("netem-bench")
        .with_netem(profile.clone())
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // Establish baseline connectivity first — we need both sides to be
    // synced before the partition so the recovery path exercises
    // queue-and-forward, not first-time-sync.
    harness
        .peer("alice")
        .insert_task("baseline", "syncing", false)
        .await
        .expect("baseline insert");
    harness
        .peer("bob")
        .wait_for_task("baseline", "syncing", COLD_START_TIMEOUT)
        .await
        .expect("baseline sync");

    // Apply blackhole to Alice for 30s. Bob writes 10 rows during it.
    let blackhole = NetemProfile::custom("blackhole", 1, 0, 100.0, None);
    harness
        .peer_mut("alice")
        .set_netem(blackhole)
        .await
        .expect("apply blackhole");

    let writes_total = 10;
    for i in 0..writes_total {
        let id = format!("offline-{i}");
        let title = format!("write {i}");
        harness
            .peer("bob")
            .insert_task(&id, &title, false)
            .await
            .expect("bob insert during partition");
    }

    tokio::time::sleep(Duration::from_secs(30)).await;

    let t_heal = Instant::now();
    harness
        .peer_mut("alice")
        .set_netem(profile.clone())
        .await
        .expect("restore profile");

    // Wait for the LAST offline write to reach Alice — that's the
    // worst-case catch-up time. If any write fails to converge in 180s
    // we count it as lost and move on (the result row makes that
    // explicit).
    let mut writes_recovered = 0usize;
    for i in 0..writes_total {
        let id = format!("offline-{i}");
        let title = format!("write {i}");
        if harness
            .peer("alice")
            .wait_for_task(&id, &title, Duration::from_secs(180))
            .await
            .is_ok()
        {
            writes_recovered += 1;
        } else {
            eprintln!("  WARNING: write {i} did not converge");
        }
    }
    let recovery_ms = t_heal.elapsed().as_millis();
    eprintln!(
        "  long_offline_30s recovery = {recovery_ms} ms,  recovered = {writes_recovered}/{writes_total}"
    );

    ScenarioResult {
        scenario,
        profile: profile_name,
        recovery_ms,
        writes_recovered,
        writes_total,
    }
}

/// "User force-quit the app and re-opened it." Stops Alice's
/// container and starts it again; the engine restarts with no
/// in-memory state but a populated `_wavesync_peer_addrs` cache from
/// the previous session (PR #29). We measure how long after restart
/// Alice catches up on rows Bob inserted while she was down.
async fn run_container_restart_scenario(profile: NetemProfile) -> ScenarioResult {
    let profile_name = profile.name.to_string();
    let scenario = "container_restart".to_string();
    eprintln!("\n==== {scenario} on profile: {profile_name} ====");

    let mut harness = WaveSyncE2eHarness::new()
        .with_passphrase("netem-bench")
        .with_netem(profile.clone())
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // Sync once so the cached peer-addrs table on Alice has Bob's
    // address recorded — that's what the restart must benefit from.
    harness
        .peer("alice")
        .insert_task("baseline", "syncing", false)
        .await
        .expect("baseline insert");
    harness
        .peer("bob")
        .wait_for_task("baseline", "syncing", COLD_START_TIMEOUT)
        .await
        .expect("baseline sync");
    // Give the spawned `record_success` task a moment to commit.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop Alice's container. testcontainers' `stop` keeps state on
    // the named volume mount — when we start again, the SQLite DB is
    // intact and `_wavesync_peer_addrs` survives.
    eprintln!("  stopping alice...");
    harness
        .peer_mut("alice")
        .container
        .stop()
        .await
        .expect("stop alice");

    // Bob writes while Alice is offline.
    let writes_total = 5;
    for i in 0..writes_total {
        let id = format!("restart-{i}");
        let title = format!("offline write {i}");
        harness
            .peer("bob")
            .insert_task(&id, &title, false)
            .await
            .expect("bob insert during alice-down");
    }

    eprintln!("  starting alice...");
    harness
        .peer_mut("alice")
        .container
        .start()
        .await
        .expect("start alice");
    harness
        .peer_mut("alice")
        .refresh_base_url()
        .await
        .expect("refresh base url");

    let t_back = Instant::now();
    let mut writes_recovered = 0usize;
    for i in 0..writes_total {
        let id = format!("restart-{i}");
        let title = format!("offline write {i}");
        if harness
            .peer("alice")
            .wait_for_task(&id, &title, Duration::from_secs(180))
            .await
            .is_ok()
        {
            writes_recovered += 1;
        } else {
            eprintln!("  WARNING: write {i} did not converge after restart");
        }
    }
    let recovery_ms = t_back.elapsed().as_millis();
    eprintln!(
        "  container_restart recovery = {recovery_ms} ms,  recovered = {writes_recovered}/{writes_total}"
    );

    ScenarioResult {
        scenario,
        profile: profile_name,
        recovery_ms,
        writes_recovered,
        writes_total,
    }
}

/// Asymmetric pair: Alice on `cellular_bad`, Bob on `wifi_home`.
/// Common real-world topology (mobile user syncing with desktop).
/// Convergence speed is gated by the *worse* peer; this measures
/// p95 propagation in both directions.
async fn run_asymmetric_scenario() -> ScenarioResult {
    let scenario = "asymmetric_cellular_bad_x_wifi_home".to_string();
    eprintln!("\n==== {scenario} ====");

    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("netem-bench")
        .add_peer_with_netem("alice", NetemProfile::cellular_bad())
        .add_peer_with_netem("bob", NetemProfile::wifi_home())
        .start()
        .await
        .expect("harness start");

    let writes_total = 20;
    let mut latencies_ms = Vec::with_capacity(writes_total * 2);
    let t_start = Instant::now();

    // Alice → Bob (the slow → fast direction).
    for i in 0..writes_total {
        let id = format!("a2b-{i}");
        let title = format!("alice write {i}");
        let t = Instant::now();
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, WRITE_TIMEOUT)
            .await
            .expect("bob receive");
        latencies_ms.push(t.elapsed().as_millis());
    }
    // Bob → Alice (fast → slow).
    for i in 0..writes_total {
        let id = format!("b2a-{i}");
        let title = format!("bob write {i}");
        let t = Instant::now();
        harness
            .peer("bob")
            .insert_task(&id, &title, false)
            .await
            .expect("bob insert");
        harness
            .peer("alice")
            .wait_for_task(&id, &title, WRITE_TIMEOUT)
            .await
            .expect("alice receive");
        latencies_ms.push(t.elapsed().as_millis());
    }

    latencies_ms.sort_unstable();
    let p95 = latencies_ms[(latencies_ms.len() * 95) / 100];
    eprintln!(
        "  asymmetric p95 = {p95} ms over {} writes ({}ms total)",
        latencies_ms.len(),
        t_start.elapsed().as_millis()
    );

    ScenarioResult {
        scenario,
        profile: "alice=cellular_bad, bob=wifi_home".to_string(),
        recovery_ms: p95,
        writes_recovered: latencies_ms.len(),
        writes_total: latencies_ms.len(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; long-offline scenario."]
async fn netem_scenario_long_offline_cellular_fair() {
    let r = run_long_offline_scenario(NetemProfile::cellular_fair()).await;
    print_scenario_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; long-offline scenario, harder profile."]
async fn netem_scenario_long_offline_cellular_bad() {
    let r = run_long_offline_scenario(NetemProfile::cellular_bad()).await;
    print_scenario_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; container restart scenario."]
async fn netem_scenario_container_restart_lan_fast() {
    let r = run_container_restart_scenario(NetemProfile::lan_fast()).await;
    print_scenario_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; container restart on cellular."]
async fn netem_scenario_container_restart_cellular_fair() {
    let r = run_container_restart_scenario(NetemProfile::cellular_fair()).await;
    print_scenario_table(&[r]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; asymmetric peer profiles."]
async fn netem_scenario_asymmetric_peers() {
    let r = run_asymmetric_scenario().await;
    print_scenario_table(&[r]);
}

/// Run every extended scenario sequentially and emit one consolidated
/// table. Slower (~10 minutes) but useful for sanity-checking after
/// engine changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only; full extended-scenario matrix."]
async fn netem_scenario_full_matrix() {
    let mut rows = Vec::new();
    rows.push(run_long_offline_scenario(NetemProfile::cellular_fair()).await);
    rows.push(run_long_offline_scenario(NetemProfile::cellular_bad()).await);
    rows.push(run_container_restart_scenario(NetemProfile::lan_fast()).await);
    rows.push(run_container_restart_scenario(NetemProfile::cellular_fair()).await);
    rows.push(run_asymmetric_scenario().await);
    print_scenario_table(&rows);
}
