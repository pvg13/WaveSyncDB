//! Relay-mailbox durability scenarios: the encrypted store-and-forward log.
//!
//! The mailbox exists for the case the fan-out + reconcile paths cannot
//! cover: BOTH peers offline at once. The writer's changeset is sealed
//! client-side (XChaCha20-Poly1305 under a group-derived key) and appended
//! durably at the relay (acked only after fsync); the reader drains it on
//! its next wake — no peer needs to be reachable. These scenarios exercise
//! the acceptance criteria end to end:
//!
//! * M1 — both-offline durability: writer freezes right after the durable
//!   append; reader wakes into a world with zero reachable peers and must
//!   still converge, promptly, from the mailbox alone.
//! * M2 — relay restart between append and drain: an acked entry survives.
//! * M3 — E2E confidentiality: the relay's on-disk store contains no
//!   changeset plaintext.
//! * M4 — offline beyond the mailbox TTL: the miss is detected (gap) and
//!   the engine falls back to the version-vector reconcile automatically.
//!
//! Setup mirrors `wan_latency.rs`: mDNS off, port-restricted-cone NAT —
//! the phone-on-cellular shape where the relay is the only path.
//!
//! Run (Docker + built images required, see tests-e2e/README.md):
//!
//! ```bash
//! cargo test -p wavesyncdb-e2e --test relay_mailbox -- --test-threads=1 --nocapture
//! ```

use std::time::Duration;

use anyhow::Result;
use wavesyncdb_e2e::{NatProfile, RunningHarness, WaveSyncE2eHarness, report_ttfs};

/// Generous eventual-convergence ceiling (same as wan_latency).
const CEILING: Duration = Duration::from_secs(120);

/// Simulated OS-granted push budget (the mobile default).
const PUSH_BUDGET: Duration = Duration::from_secs(20);

/// The acked-appends metric samples (counter registered without the
/// suffix; the OpenMetrics encoder appends `_total`).
const APPENDS_OK: (&str, &str) = ("relay_mailbox_appends_total", "outcome=\"ok\"");

fn mailbox_harness(passphrase: &str) -> WaveSyncE2eHarness {
    WaveSyncE2eHarness::new()
        .with_passphrase(passphrase)
        .without_mdns()
        .add_peer_with_nat("alice", NatProfile::PortRestrictedConeAutoNATOk)
        .add_peer_with_nat("bob", NatProfile::PortRestrictedConeAutoNATOk)
        // The relay's /data is a volume attached to the container, so the
        // mailbox survives `stop_relay`/`start_relay` (M2).
        .with_relay_env("MAILBOX_DB", "/data/mailbox.db")
}

/// Warm both directions once so address caches, peer versions, and both
/// peers' mailbox cursors are populated (the realistic steady state).
async fn warm_up(harness: &RunningHarness) -> Result<()> {
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
    Ok(())
}

/// Write on alice and wait until the relay has durably acked the mailbox
/// append for it — the append runs in parallel with the (doomed) fan-out,
/// so freezing alice on the row alone would race it.
async fn write_and_await_append(harness: &RunningHarness, id: &str, title: &str) -> Result<()> {
    let acked_before = harness.relay_metric_value(APPENDS_OK.0, APPENDS_OK.1).await;
    harness.peer("alice").insert_task(id, title, false).await?;
    harness
        .wait_for_relay_metric(
            APPENDS_OK.0,
            APPENDS_OK.1,
            acked_before + 1.0,
            Duration::from_secs(15),
        )
        .await
}

/// M1 — both peers offline: the write survives at the relay and the woken
/// reader converges from the mailbox alone, inside the push budget, with
/// the wake reporting success promptly (not by burning the whole budget
/// waiting for a PeerSynced that cannot come).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m1_both_offline_durability() -> Result<()> {
    let harness = mailbox_harness("relay-mailbox-m1").start().await?;
    warm_up(&harness).await?;

    // Reader goes dark first; then the writer writes and — as soon as the
    // relay has fsynced the sealed entry — goes dark too.
    harness.peer("bob").pause().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    write_and_await_append(&harness, "m1-row", "written while both offline").await?;
    harness.peer("alice").pause().await?;

    // Reader wakes into a world with zero reachable peers.
    harness.peer("bob").unpause().await?;
    let outcome = harness.peer("bob").push_wake(PUSH_BUDGET).await?;
    report_ttfs(
        "m1_both_offline",
        "push_wake_return",
        Duration::from_millis(outcome.elapsed_ms),
    );
    assert_eq!(
        outcome.result, "synced",
        "wake must succeed from the mailbox alone (writer frozen): {outcome:?}"
    );
    assert!(
        Duration::from_millis(outcome.elapsed_ms) <= PUSH_BUDGET,
        "wake must finish inside the push budget: {outcome:?}"
    );

    let ttfs = harness
        .peer("bob")
        .wait_for_task_timed(
            "m1-row",
            "written while both offline",
            Duration::from_secs(5),
        )
        .await?;
    report_ttfs("m1_both_offline", "row_visible_after_wake", ttfs);

    let diags = harness.peer("bob").diagnostics().await?;
    assert!(
        diags.mailbox_entries_drained > 0,
        "the row must have arrived via the mailbox drain, got {diags:?}"
    );

    // The writer comes back: normal bidirectional sync must still work.
    harness.peer("alice").unpause().await?;
    harness
        .peer("bob")
        .insert_task("m1-after", "bob after wake", false)
        .await?;
    harness
        .peer("alice")
        .wait_for_task("m1-after", "bob after wake", CEILING)
        .await?;
    Ok(())
}

/// M2 — relay restarts between the acked append and the drain: the entry
/// is fsynced before the ack, so it must survive and still be delivered.
///
/// The reader stays frozen past the engine's ~60s suspension-gap threshold
/// (the phone-in-pocket-during-relay-maintenance shape) so the push wake
/// force-resets the relay connection. That is load-bearing for the budget:
/// the relay restart silently killed the frozen reader's QUIC connections,
/// and without the forced reset the engine would wait out QUIC's ~30s dead
/// connection detection — which a 20s push window doesn't have.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m2_relay_restart_mid_flight() -> Result<()> {
    let harness = mailbox_harness("relay-mailbox-m2").start().await?;
    warm_up(&harness).await?;

    harness.peer("bob").pause().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    write_and_await_append(&harness, "m2-row", "survives relay restart").await?;

    // Restart the relay (same container — /data volume and identity are
    // preserved).
    harness.stop_relay().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    harness.start_relay().await?;

    // Keep the reader frozen past the suspension-gap threshold (see the
    // doc comment), then freeze the writer before the reader wakes.
    tokio::time::sleep(Duration::from_secs(60)).await;
    harness.peer("alice").pause().await?;

    harness.peer("bob").unpause().await?;
    let outcome = harness.peer("bob").push_wake(PUSH_BUDGET).await?;
    report_ttfs(
        "m2_relay_restart",
        "push_wake_return",
        Duration::from_millis(outcome.elapsed_ms),
    );
    assert_eq!(
        outcome.result, "synced",
        "acked entry must survive the relay restart: {outcome:?}"
    );
    harness
        .peer("bob")
        .wait_for_task_timed("m2-row", "survives relay restart", Duration::from_secs(5))
        .await?;

    harness.peer("alice").unpause().await?;
    Ok(())
}

/// M3 — E2E confidentiality: the relay's on-disk mailbox holds ciphertext
/// only. A raw byte scan of the SQLite file (+ WAL) must not contain the
/// distinctive plaintext marker, while a positive control proves we are
/// reading the real store (not an empty/missing file).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m3_relay_stores_only_ciphertext() -> Result<()> {
    let harness = mailbox_harness("relay-mailbox-m3").start().await?;
    warm_up(&harness).await?;

    let marker = "MAILBOX-PLAINTEXT-CANARY-9f3c1a";
    write_and_await_append(&harness, "m3-row", marker).await?;

    let store_bytes = harness.relay_read_files("/data/mailbox.db*").await?;
    // Positive control: the schema string proves the scan read the real
    // store file (an empty read must not silently pass the negative check).
    let store_text = String::from_utf8_lossy(&store_bytes);
    assert!(
        store_text.contains("mailbox_entries"),
        "expected the mailbox schema in the scanned bytes ({} bytes read) — wrong path?",
        store_bytes.len()
    );
    // The actual confidentiality assertion. Plaintext changeset JSON would
    // appear verbatim inside the DB pages (values are not compressed);
    // check the column value, the table name, and the row id.
    for needle in [marker, "m3-row"] {
        assert!(
            !store_text.contains(needle),
            "plaintext {needle:?} leaked into the relay's mailbox store"
        );
    }
    // Sanity: the same marker IS visible plaintext on the peers.
    harness
        .peer("bob")
        .wait_for_task("m3-row", marker, CEILING)
        .await?;
    Ok(())
}

/// M4 — reader offline beyond the mailbox TTL: its cursor's continuation
/// is GC'd at the relay; the wake must detect the gap and automatically
/// fall back to the version-vector reconcile (the writer is reachable
/// again by then) — never silently report "caught up".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn m4_ttl_expiry_falls_back_to_reconcile() -> Result<()> {
    let harness = mailbox_harness("relay-mailbox-m4")
        // Aggressive aging so "offline beyond the TTL" fits in a test run.
        .with_relay_env("MAILBOX_TTL_SECS", "5")
        .with_relay_env("MAILBOX_GC_INTERVAL_SECS", "1")
        .start()
        .await?;
    warm_up(&harness).await?;

    harness.peer("bob").pause().await?;
    tokio::time::sleep(Duration::from_secs(2)).await;
    write_and_await_append(&harness, "m4-row", "older than the mailbox ttl").await?;

    // Let the entry age out (TTL 5s + GC cadence 1s + slack). The writer
    // stays up: after the gap is detected, it is the reconcile source.
    tokio::time::sleep(Duration::from_secs(10)).await;

    harness.peer("bob").unpause().await?;
    let _ = harness.peer("bob").push_wake(PUSH_BUDGET).await?;

    // Convergence must come via the reconcile fallback.
    harness
        .peer("bob")
        .wait_for_task("m4-row", "older than the mailbox ttl", CEILING)
        .await?;
    let diags = harness.peer("bob").diagnostics().await?;
    assert!(
        diags.mailbox_gap_fallbacks > 0,
        "the TTL miss must be detected as a gap (not silently skipped): {diags:?}"
    );
    Ok(())
}
