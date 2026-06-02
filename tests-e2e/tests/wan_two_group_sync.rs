//! Scenario — two peers, two groups, relay-only (no mDNS).
//!
//! Regression guard for the multi-group rendezvous-discovery bug: the
//! discover side reused the default group's pagination cookie for every
//! namespace, so once the default namespace populated a non-`None`
//! cookie, the secondary namespace's discover was issued with that
//! foreign cookie and the server returned no registrations — the
//! secondary-group peer was never dialed over the relay.
//!
//! mDNS is disabled so the relay/rendezvous server is the only discovery
//! path; on a real LAN mDNS masks the bug by discovering peers directly.
//! Both peers join a secondary "household" group on top of their default
//! group; we assert writes propagate in BOTH groups, with the secondary
//! group being the one that was broken.

use std::time::Duration;

use wavesyncdb_e2e::WaveSyncE2eHarness;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_group_wan_sync_via_relay() {
    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("e2e-shared-secret")
        .with_secondary_group("household", "household-secret")
        .without_mdns()
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    let alice = harness.peer("alice");
    let bob = harness.peer("bob");

    // Default group still syncs over the relay (proves the relay/rendezvous
    // path itself is up — this matched real-world behaviour where the
    // personal/default group synced fine while the household did not).
    alice
        .insert_task("p-1", "personal item", false)
        .await
        .expect("alice insert default-group task");
    bob.wait_for_task("p-1", "personal item", Duration::from_secs(30))
        .await
        .expect("bob did not converge on default-group write");

    // Secondary group — the path the cookie bug broke. With the fix,
    // rendezvous discover returns the household namespace's registrations
    // and bob dials alice over the relay, so the write propagates.
    alice
        .insert_task_g2("h-1", "buy groceries", false)
        .await
        .expect("alice insert household task");
    bob.wait_for_task_g2("h-1", "buy groceries", Duration::from_secs(30))
        .await
        .expect("bob did not converge on secondary-group write (multi-group rendezvous discovery)");

    // Bidirectional in the secondary group.
    bob.insert_task_g2("h-2", "take out trash", true)
        .await
        .expect("bob insert household task");
    alice
        .wait_for_task_g2("h-2", "take out trash", Duration::from_secs(30))
        .await
        .expect("alice did not see bob's secondary-group write");
}
