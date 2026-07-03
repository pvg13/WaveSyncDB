//! Engine-diagnostics integration tests.
//!
//! Validates that the counters surfaced through
//! [`wavesyncdb::WaveSyncDb::diagnostics`] actually move during a
//! real two-peer scenario. The point isn't to assert exact values
//! (timing-sensitive on CI runners) — it's to catch silent regressions
//! where a future refactor accidentally bypasses the increment site.
//!
//! Must run single-threaded: mDNS discovery is process-wide and parallel
//! tests cross-discover each other's peers, causing nondeterministic failures.
//!
//! Run with `cargo test -p wavesyncdb --test diagnostics -- --test-threads=1`.

mod common;

use std::time::Duration;
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

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

// Node ID seeds 72-73 (see CLAUDE.md Section 6 seed range table).
#[tokio::test]
async fn test_diagnostics_byte_accounting_relay_vs_direct() {
    let _ = env_logger::try_init();
    let topic = format!("test-diag-bytes-{}", Uuid::new_v4());
    let passphrase = "diag-bytes-secret";
    let timeout = Duration::from_secs(15);

    // A passphrase is required here: unsigned (no-key) messages never reach
    // the HMAC sign/verify call sites, so byte accounting is unobservable
    // without a configured group key (see `crate::diagnostics` and Rule 2.7 —
    // production groups run passphrases, so this is the representative case).
    let alice = WaveSyncDbBuilder::new(&mem_db("diag_bytes_alice"), &topic)
        .with_node_id(common::make_node_id(72))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create alice");
    alice
        .schema()
        .register(common::task::Entity)
        .sync()
        .await
        .expect("Failed to sync alice schema");

    let bob = WaveSyncDbBuilder::new(&mem_db("diag_bytes_bob"), &topic)
        .with_node_id(common::make_node_id(73))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("Failed to create bob");
    bob.schema()
        .register(common::task::Entity)
        .sync()
        .await
        .expect("Failed to sync bob schema");

    // No initial (pre-discovery) zero-count assertion here: unlike the mDNS
    // counters above, mDNS discovery + the connect-triggered version-vector
    // catch-up can complete within the wall time `build()` + `sync()` already
    // take for both peers, so an "initial" read is racy by construction, not
    // just under parallel-test contention.

    // mDNS discovery + connection triggers a version-vector catch-up on both
    // sides (`initiate_sync_for_peer`), which signs a request and (on the
    // peer side) signs+sends a response even with nothing to sync yet — that
    // round trip alone moves the byte counters, with no writes required.
    assert_eventually(
        "alice's direct byte counters move after LAN sync",
        timeout,
        || async {
            let s = alice.diagnostics();
            s.direct_bytes_out > 0 && s.direct_bytes_in > 0
        },
    )
    .await;

    assert_eventually(
        "bob's direct byte counters move after LAN sync",
        timeout,
        || async {
            let s = bob.diagnostics();
            s.direct_bytes_out > 0 && s.direct_bytes_in > 0
        },
    )
    .await;

    let alice_after = alice.diagnostics();
    let bob_after = bob.diagnostics();

    // Both peers are on the same LAN interface (mDNS discovery, no relay
    // configured), so no traffic should ever land in the relay bucket.
    assert_eq!(alice_after.relay_bytes_out, 0, "alice relay_bytes_out");
    assert_eq!(alice_after.relay_bytes_in, 0, "alice relay_bytes_in");
    assert_eq!(bob_after.relay_bytes_out, 0, "bob relay_bytes_out");
    assert_eq!(bob_after.relay_bytes_in, 0, "bob relay_bytes_in");

    assert_eq!(alice_after.relay_traffic_ratio(), Some(0.0));
    assert_eq!(bob_after.relay_traffic_ratio(), Some(0.0));
}
