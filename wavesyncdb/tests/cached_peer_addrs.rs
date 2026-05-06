//! End-to-end test for the cached-peer-addresses cold-start cache (#29).
//!
//! Validates the user-visible promise: after pairing once, restarting a
//! peer's engine pre-dials the cached address before discovery has time
//! to fire. We don't assert exact reconnect timing (CI runners vary too
//! much) — the signal we look for is the [`cached_addr_dials`] counter
//! incrementing at startup, which proves:
//!
//! 1. The cache was populated during the first session
//!    (via `record_success` on `ConnectionEstablished`).
//! 2. The cache survived process / engine restart
//!    (it lives in `_wavesync_peer_addrs` on disk).
//! 3. The new engine actually dialed a cached address at startup
//!    (via `predial_cached_addrs`).
//!
//! Single-threaded per CLAUDE.md Rule 2.13: mDNS is process-wide and
//! parallel tests cross-discover.
//!
//! Run with `cargo test -p wavesyncdb --test cached_peer_addrs -- --test-threads=1`.

mod common;

use std::time::Duration;
use uuid::Uuid;

use common::{assert_eventually, make_peer, mem_db};

#[tokio::test]
async fn cached_addresses_used_at_cold_start() {
    let _ = env_logger::try_init();
    let topic = format!("test-cached-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    // Stable backing file for Bob — we'll close and reopen against it.
    let bob_db_url = mem_db("cached_bob");
    let alice_db_url = mem_db("cached_alice");

    // First pairing: bring both up, wait until they discover each
    // other and complete at least one successful peer dial — that's
    // the moment `record_success` writes the cached row.
    {
        let alice = make_peer(&alice_db_url, &topic, 200).await;
        let bob = make_peer(&bob_db_url, &topic, 201).await;

        assert_eventually("alice and bob connect via mDNS", timeout, || async {
            let a = alice.diagnostics();
            let b = bob.diagnostics();
            a.peer_dial_successes >= 1 && b.peer_dial_successes >= 1
        })
        .await;

        // Hold both alive briefly so the spawned record_success
        // tokio task has time to commit before Bob's drop.
        tokio::time::sleep(Duration::from_millis(500)).await;
        // alice and bob drop here — engines abort (via WaveSyncDbInner Drop).
    }

    // Give the OS a moment to release file handles / sockets so the
    // next mDNS / QUIC bind doesn't trip over the previous one.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cold-start Bob against the same DB. The cached row from the
    // previous session must drive a startup pre-dial.
    let bob2 = make_peer(&bob_db_url, &topic, 201).await;

    assert_eventually(
        "bob's restart pre-dials the cached alice address",
        Duration::from_secs(5),
        || async { bob2.diagnostics().cached_addr_dials >= 1 },
    )
    .await;

    let snap = bob2.diagnostics();
    assert!(
        snap.cached_addr_dials >= 1,
        "expected cached_addr_dials >= 1 after restart with non-empty cache, got {}",
        snap.cached_addr_dials
    );
    // The pre-dial is *also* a peer_dial_attempt — sanity-check that
    // we didn't double-decrement or otherwise drift the metrics.
    assert!(
        snap.peer_dial_attempts >= snap.cached_addr_dials,
        "peer_dial_attempts ({}) must include cached_addr_dials ({})",
        snap.peer_dial_attempts,
        snap.cached_addr_dials
    );
}
