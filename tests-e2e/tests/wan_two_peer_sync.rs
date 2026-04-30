//! Scenario 1 — two peers sync over the relay.
//!
//! Brings up the real `wavesync-relay` and two `test-peer` containers
//! on a shared Docker network, writes a row through Peer A's REST API
//! (which goes through `WaveSyncDb`'s normal write path), and asserts
//! Peer B sees the same row within a generous timeout.
//!
//! This exercises the parts the in-process integration tests can't:
//! the compiled relay binary, real circuit-relay reservation, real
//! libp2p QUIC over the Docker bridge, end-to-end HMAC verification.

use std::time::Duration;

use wavesyncdb_e2e::WaveSyncE2eHarness;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_peer_wan_sync_via_relay() {
    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("e2e-shared-secret")
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    let alice = harness.peer("alice");
    let bob = harness.peer("bob");

    // Write a row on Alice; expect it on Bob.
    alice
        .insert_task("task-1", "buy milk", false)
        .await
        .expect("alice insert");

    bob.wait_for_task("task-1", "buy milk", Duration::from_secs(20))
        .await
        .expect("bob did not converge");

    // Update from Bob; expect it on Alice (verifies bidirectional).
    bob.update_task("task-1", "buy oat milk", true)
        .await
        .expect("bob update");

    alice
        .wait_for_task("task-1", "buy oat milk", Duration::from_secs(20))
        .await
        .expect("alice did not see update");

    let alice_view = alice.get_task("task-1").await.unwrap().unwrap();
    let bob_view = bob.get_task("task-1").await.unwrap().unwrap();
    assert_eq!(alice_view, bob_view, "peers diverged");
    assert!(alice_view.completed);
}
