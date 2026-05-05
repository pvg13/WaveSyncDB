//! Engine-diagnostics integration tests.
//!
//! Validates that the counters surfaced through
//! [`wavesyncdb::WaveSyncDb::diagnostics`] actually move during a
//! real two-peer scenario. The point isn't to assert exact values
//! (timing-sensitive on CI runners) — it's to catch silent regressions
//! where a future refactor accidentally bypasses the increment site.
//!
//! Single-threaded per CLAUDE.md Rule 2.13: mDNS discovery is process-wide
//! and parallel tests cross-discover.
//!
//! Run with `cargo test -p wavesyncdb --test diagnostics -- --test-threads=1`.

mod common;

use std::time::Duration;
use uuid::Uuid;

use common::{assert_eventually, make_peer, mem_db};

#[tokio::test]
async fn test_diagnostics_counters_fire_during_two_peer_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-diag-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    // Both peers fresh; mDNS will discover them on the local interface.
    let alice = make_peer(&mem_db("diag_alice"), &topic, 240).await;
    let bob = make_peer(&mem_db("diag_bob"), &topic, 241).await;

    // Both engines come up with all counters at zero.
    let initial = alice.diagnostics();
    assert_eq!(initial.mdns_discoveries, 0);
    assert_eq!(initial.peer_dial_attempts, 0);
    assert_eq!(initial.peer_dial_successes, 0);

    // Wait until both sides have seen each other end-to-end. The earliest
    // signal is mDNS discovery; success happens after the dial completes.
    assert_eventually(
        "alice sees bob via mDNS + dials successfully",
        timeout,
        || async {
            let s = alice.diagnostics();
            s.mdns_discoveries >= 1 && s.peer_dial_successes >= 1
        },
    )
    .await;

    let alice_after = alice.diagnostics();
    let bob_after = bob.diagnostics();

    // Sanity: mDNS produced at least one peer-id arrival on Alice (the
    // helper dedups by peer-id within a single Discovered event, so this
    // counts unique peers, not addresses).
    assert!(
        alice_after.mdns_discoveries >= 1,
        "alice mdns_discoveries = {}",
        alice_after.mdns_discoveries
    );

    // Sanity: at least one of {Alice, Bob} dialed the other successfully.
    // libp2p races both sides; whichever wins gets the success on its
    // counter and the other observes a ConnectionEstablished as the
    // *listener*. So we OR the two counts rather than asserting both.
    assert!(
        alice_after.peer_dial_successes + bob_after.peer_dial_successes >= 1,
        "no successful peer dial counted: alice={} bob={}",
        alice_after.peer_dial_successes,
        bob_after.peer_dial_successes
    );

    // Sanity: at least one dial attempt was actually made — guards
    // against the increment getting deleted in a future refactor.
    assert!(
        alice_after.peer_dial_attempts + bob_after.peer_dial_attempts >= 1,
        "no peer dial attempt counted: alice={} bob={}",
        alice_after.peer_dial_attempts,
        bob_after.peer_dial_attempts
    );
}
