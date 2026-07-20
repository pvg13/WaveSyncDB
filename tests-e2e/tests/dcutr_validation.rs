//! DCUtR validation bench — runs cellular_fair, exercises sync long
//! enough for the DCUtR upgrade attempts to register, and reads the
//! resulting `dcutr_upgrades_*` counters out of each peer.
//!
//! Closes the third acceptance criterion of #40: confirms the engine
//! is actually attempting (and ideally succeeding at) direct-connection
//! upgrades, and gives a number we can track over time.
//!
//! Local-only — depends on the netem harness and Docker. Run with:
//!
//! ```bash
//! ./tests-e2e/build-images.sh
//! cargo test -p wavesyncdb-e2e --test dcutr_validation \
//!     -- --ignored --nocapture
//! ```

use std::time::Duration;

use wavesyncdb_e2e::{NatProfile, NetemProfile, WaveSyncE2eHarness};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Local-only DCUtR validation; requires Docker + netem."]
async fn dcutr_validation_cellular_fair() {
    // We use cellular_fair (not lan_fast) because LAN profiles let
    // peers connect directly so fast that DCUtR never has a relay
    // path to upgrade. Cellular_fair forces the relay-first path
    // long enough for the DCUtR behaviour to fire its upgrade dial.
    //
    // NAT shape: `PortRestrictedConeAutoNATOk` — blocks unsolicited
    // peer-to-peer inbound (so libp2p must use the relay first), but
    // whitelists the relay's bridge IP so AutoNAT verification dials
    // succeed and the engine keeps advertising direct addresses
    // for DCUtR to attempt the hole-punch toward.
    let profile = NetemProfile::cellular_fair();

    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("dcutr-bench")
        .with_netem(profile.clone())
        .with_nat(NatProfile::PortRestrictedConeAutoNATOk)
        // NAT'd peers are by definition not on one LAN: without this,
        // mDNS on the shared docker bridge discovers the peers to each
        // other instantly and the scenario measures LAN discovery, not
        // relay introduction.
        .without_mdns()
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // Drive sync for a while: enough writes to keep the connection
    // healthy across the DCUtR upgrade window. DCUtR fires once the
    // peers have exchanged identify messages and the relay-routed
    // connection is settled — typically within a few seconds.
    for i in 0..20 {
        let id = format!("warmup-{i}");
        let title = format!("warmup write {i}");
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, Duration::from_secs(15))
            .await
            .expect("bob receive");
    }

    // Give DCUtR a generous wall-clock window to attempt the upgrade.
    // The behaviour is event-driven, not polled — but its triggers
    // (identify push, relay reservation event) take a few seconds to
    // settle on cellular_fair.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let alice = harness
        .peer("alice")
        .diagnostics()
        .await
        .expect("alice diag");
    let bob = harness.peer("bob").diagnostics().await.expect("bob diag");

    eprintln!("\n=== DCUtR validation on profile: {} ===\n", profile.name);
    eprintln!(
        "Alice: attempted={}  succeeded={}",
        alice.dcutr_upgrades_attempted, alice.dcutr_upgrades_succeeded
    );
    eprintln!(
        "Bob:   attempted={}  succeeded={}",
        bob.dcutr_upgrades_attempted, bob.dcutr_upgrades_succeeded
    );

    // Relay-cost / DERP demotion readout (#84): once a direct path supersedes a
    // relay-carried one (via DCUtR or a naturally-formed direct connection), the
    // engine closes the relay connection so steady-state data leaves the relay.
    eprintln!(
        "Alice relay-cost: relayed_established={}  direct_established={}  demoted={}",
        alice.relayed_connections_established,
        alice.direct_connections_established,
        alice.relay_connections_demoted
    );
    eprintln!(
        "Bob   relay-cost: relayed_established={}  direct_established={}  demoted={}",
        bob.relayed_connections_established,
        bob.direct_connections_established,
        bob.relay_connections_demoted
    );

    // Sync-payload byte split (#84 gauge): what fraction of the actual data
    // rode the relay vs a direct path. Counted at the payload verify/sign
    // sites, classified by the carrying connection.
    for (name, d) in [("Alice", &alice), ("Bob", &bob)] {
        let relay = d.relay_bytes_in + d.relay_bytes_out;
        let direct = d.direct_bytes_in + d.direct_bytes_out;
        let total = relay + direct;
        let ratio = if total == 0 {
            f64::NAN
        } else {
            relay as f64 / total as f64
        };
        eprintln!(
            "{name} bytes: relay={relay} (in={} out={})  direct={direct} (in={} out={})  relay_ratio={ratio:.3}",
            d.relay_bytes_in, d.relay_bytes_out, d.direct_bytes_in, d.direct_bytes_out,
        );
    }
    // Demotions can never exceed the relay connections we ever established.
    assert!(
        alice.relay_connections_demoted <= alice.relayed_connections_established,
        "alice demoted {} > relayed-established {}",
        alice.relay_connections_demoted,
        alice.relayed_connections_established
    );
    assert!(
        bob.relay_connections_demoted <= bob.relayed_connections_established,
        "bob demoted {} > relayed-established {}",
        bob.relay_connections_demoted,
        bob.relayed_connections_established
    );

    let total_attempted = alice.dcutr_upgrades_attempted + bob.dcutr_upgrades_attempted;
    let total_succeeded = alice.dcutr_upgrades_succeeded + bob.dcutr_upgrades_succeeded;
    if total_attempted > 0 {
        let pct = (total_succeeded as f64 / total_attempted as f64) * 100.0;
        eprintln!("Combined success rate: {total_succeeded}/{total_attempted} = {pct:.1}%\n");
    } else {
        eprintln!(
            "No DCUtR attempts observed — peers connected directly without going through the relay path. \
             This is normal on the Docker bridge if both peers can reach each other's IP without NAT.\n"
        );
    }

    // Soft assertion — we're documenting behavior, not gating CI.
    // Even on the Docker bridge (no NAT) we expect *some* DCUtR
    // activity because the relay path is established first when a
    // relay address is configured. If `attempted == 0` consistently,
    // it's worth investigating whether DCUtR is actually being
    // exercised by the test environment at all — that would be a
    // gap in the bench, not a bug.
    if total_attempted == 0 {
        eprintln!(
            "Note: zero DCUtR attempts even with PortRestrictedConeAutoNATOk. \
             AutoNAT now succeeds (engine reports the address as Public), so \
             the engine *does* advertise direct addresses. But the conntrack \
             filter is permissive enough that simultaneous dial-out from both \
             peers (triggered by the relay's PeerJoined introduction) creates \
             matching ESTABLISHED entries on both sides — peers form a direct \
             connection naturally without DCUtR's coordination. To force the \
             relay path and actually exercise DCUtR, the bench uses the \
             symmetric-NAT shape below (see dcutr_validation_symmetric_nat)."
        );
    }
}

/// #51 — the shape that actually exercises DCUtR.
///
/// `NatProfile::SymmetricNat` adds per-flow source-port randomization
/// (`MASQUERADE --random-fully`) on outbound UDP: the listen port each peer
/// advertises is never the port the other side's flow observes, so the
/// simultaneous dials triggered by the relay introduction match no conntrack
/// entry and are dropped on both sides. The relay circuit becomes the only
/// initial path and DCUtR's coordination the only route to a direct
/// connection.
///
/// Guarantees validated here:
/// 1. Sync converges regardless — via relay circuits (wire-verified: under
///    this shape no direct QUIC handshake can complete; dials to observed
///    ports retransmit unanswered).
/// 2. The connection and the payload are correctly ACCOUNTED as relayed —
///    this scenario found and now guards the inbound-circuit
///    misclassification (endpoint_is_relayed): before the fix, the inbound
///    circuit counted as a direct connection, flipped `peer_via_relay`,
///    and demoted a peer's only working circuit.
///
/// Deliberately NOT asserted: `dcutr_upgrades_attempted >= 1`. Under this
/// shape AutoNAT dial-backs fail (the dial-back targets the masqueraded
/// observed port, where no socket listens), the engine confirms no direct
/// external address, and rust-libp2p's dcutr never initiates for lack of
/// punch candidates. That is an ENGINE gap (observed identify addresses
/// should qualify as candidates), tracked separately in M1 — when it is
/// fixed, this test should start asserting engagement.
///
/// The byte readout doubles as M1's worst-case relay-payload-ratio
/// baseline (docs/milestones/m1-minimize-relay-dependence.md).
#[tokio::test]
#[ignore = "Local-only DCUtR validation; requires Docker + netem."]
async fn dcutr_validation_symmetric_nat() {
    let profile = NetemProfile::cellular_fair();

    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("dcutr-symmetric")
        .with_netem(profile.clone())
        .with_nat(NatProfile::SymmetricNat)
        // Symmetric-NAT peers are by definition not on one LAN; mDNS on
        // the shared bridge would bypass the shape entirely (and its
        // pre-rule discovery was one half of the startup race the
        // entrypoint-applied rules close).
        .without_mdns()
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // Bidirectional writes: convergence must hold even if every byte rides
    // relay circuits. Generous waits — circuit + cellular_fair latency.
    for i in 0..10 {
        let id = format!("sym-a{i}");
        let title = format!("alice write {i}");
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, Duration::from_secs(30))
            .await
            .expect("bob receive (relay circuit path)");
    }
    harness
        .peer("bob")
        .insert_task("sym-b0", "bob write 0", false)
        .await
        .expect("bob insert");
    harness
        .peer("alice")
        .wait_for_task("sym-b0", "bob write 0", Duration::from_secs(30))
        .await
        .expect("alice receive (relay circuit path)");

    // DCUtR window: the upgrade attempts fire off identify + reservation
    // events after the circuit settles.
    tokio::time::sleep(Duration::from_secs(10)).await;

    let alice = harness
        .peer("alice")
        .diagnostics()
        .await
        .expect("alice diag");
    let bob = harness.peer("bob").diagnostics().await.expect("bob diag");

    eprintln!(
        "\n=== DCUtR validation on symmetric NAT ({}) ===\n",
        profile.name
    );
    eprintln!(
        "Alice: attempted={}  succeeded={}   Bob: attempted={}  succeeded={}",
        alice.dcutr_upgrades_attempted,
        alice.dcutr_upgrades_succeeded,
        bob.dcutr_upgrades_attempted,
        bob.dcutr_upgrades_succeeded
    );
    eprintln!(
        "Alice relay-cost: relayed_established={}  direct_established={}  demoted={}",
        alice.relayed_connections_established,
        alice.direct_connections_established,
        alice.relay_connections_demoted
    );
    eprintln!(
        "Bob   relay-cost: relayed_established={}  direct_established={}  demoted={}",
        bob.relayed_connections_established,
        bob.direct_connections_established,
        bob.relay_connections_demoted
    );
    for (name, d) in [("Alice", &alice), ("Bob", &bob)] {
        let relay = d.relay_bytes_in + d.relay_bytes_out;
        let direct = d.direct_bytes_in + d.direct_bytes_out;
        let total = relay + direct;
        let ratio = if total == 0 {
            f64::NAN
        } else {
            relay as f64 / total as f64
        };
        eprintln!(
            "{name} bytes: relay={relay} (in={} out={})  direct={direct} (in={} out={})  relay_ratio={ratio:.3}",
            d.relay_bytes_in, d.relay_bytes_out, d.direct_bytes_in, d.direct_bytes_out,
        );
    }

    // The shape's guarantee: everything rode the relay, and was accounted
    // as such. Each peer holds circuits (relayed >= 1) and the sync payload
    // classifies overwhelmingly relayed. Before the endpoint_is_relayed fix
    // this failed: the inbound circuit counted as direct, the payload split
    // ~50/50, and a spurious demotion closed one of the circuits.
    assert!(
        alice.relayed_connections_established >= 1 && bob.relayed_connections_established >= 1,
        "both peers must hold relay circuits under symmetric NAT (alice={}, bob={})",
        alice.relayed_connections_established,
        bob.relayed_connections_established,
    );
    for (name, d) in [("alice", &alice), ("bob", &bob)] {
        let relay = d.relay_bytes_in + d.relay_bytes_out;
        let direct = d.direct_bytes_in + d.direct_bytes_out;
        let total = relay + direct;
        assert!(total > 0, "{name} counted no sync payload at all");
        let ratio = relay as f64 / total as f64;
        assert!(
            ratio > 0.9,
            "{name}: sync payload must classify as relay-carried under symmetric NAT \
             (ratio {ratio:.3}; misclassification regression?)"
        );
    }

    let total_attempted = alice.dcutr_upgrades_attempted + bob.dcutr_upgrades_attempted;
    eprintln!(
        "DCUtR terminal outcomes under symmetric NAT: attempted={total_attempted} \
         (behaviour-level punch attempts DO run — see debug logs — but each \
         failed punch dial takes a slow QUIC timeout and the counter only \
         counts terminal events after max retries, so short windows read 0)"
    );
}

/// #110 — the class where the hole-punch can genuinely land: port-restricted
/// cone WITHOUT the AutoNAT whitelist.
///
/// No whitelist ⇒ AutoNAT dial-backs fail ⇒ the engine advertises only its
/// circuit address ⇒ relay introductions cannot short-circuit into
/// simultaneous direct dials (they only carry the circuit). The peers meet
/// over relay circuits, DCUtR exchanges identify-observed addresses (which
/// equal the real listen ports — no port rewriting on a cone), and the
/// coordinated simultaneous dials punch the conntrack filters on both
/// sides. Success = a direct connection supersedes the circuit and the
/// steady-state payload leaves the relay.
#[tokio::test]
#[ignore = "Local-only DCUtR validation; requires Docker + netem."]
async fn dcutr_validation_port_restricted_no_whitelist() {
    let profile = NetemProfile::cellular_fair();

    let harness = WaveSyncE2eHarness::new()
        .with_passphrase("dcutr-cone-nowl")
        .with_netem(profile.clone())
        .with_nat(NatProfile::PortRestrictedCone)
        .without_mdns()
        .add_peer("alice")
        .add_peer("bob")
        .start()
        .await
        .expect("harness start");

    // Converge over whatever path exists first.
    for i in 0..10 {
        let id = format!("cone-a{i}");
        let title = format!("alice write {i}");
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, Duration::from_secs(30))
            .await
            .expect("bob receive");
    }

    // Generous punch window: CONNECT roundtrips + simultaneous dials +
    // (on failure) dcutr's internal re-attempts each gated on a QUIC dial
    // timeout.
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Post-punch traffic so the byte split reflects the upgraded path.
    for i in 0..5 {
        let id = format!("cone-b{i}");
        let title = format!("post-punch write {i}");
        harness
            .peer("alice")
            .insert_task(&id, &title, false)
            .await
            .expect("alice insert");
        harness
            .peer("bob")
            .wait_for_task(&id, &title, Duration::from_secs(30))
            .await
            .expect("bob receive");
    }

    let alice = harness
        .peer("alice")
        .diagnostics()
        .await
        .expect("alice diag");
    let bob = harness.peer("bob").diagnostics().await.expect("bob diag");

    eprintln!(
        "\n=== DCUtR validation on port-restricted cone, no AutoNAT whitelist ({}) ===\n",
        profile.name
    );
    eprintln!(
        "Alice: attempted={}  succeeded={}   Bob: attempted={}  succeeded={}",
        alice.dcutr_upgrades_attempted,
        alice.dcutr_upgrades_succeeded,
        bob.dcutr_upgrades_attempted,
        bob.dcutr_upgrades_succeeded
    );
    eprintln!(
        "Alice relay-cost: relayed_established={}  direct_established={}  demoted={}",
        alice.relayed_connections_established,
        alice.direct_connections_established,
        alice.relay_connections_demoted
    );
    eprintln!(
        "Bob   relay-cost: relayed_established={}  direct_established={}  demoted={}",
        bob.relayed_connections_established,
        bob.direct_connections_established,
        bob.relay_connections_demoted
    );
    for (name, d) in [("Alice", &alice), ("Bob", &bob)] {
        let relay = d.relay_bytes_in + d.relay_bytes_out;
        let direct = d.direct_bytes_in + d.direct_bytes_out;
        let total = relay + direct;
        let ratio = if total == 0 {
            f64::NAN
        } else {
            relay as f64 / total as f64
        };
        eprintln!(
            "{name} bytes: relay={relay} (in={} out={})  direct={direct} (in={} out={})  relay_ratio={ratio:.3}",
            d.relay_bytes_in, d.relay_bytes_out, d.direct_bytes_in, d.direct_bytes_out,
        );
    }

    // The milestone-relevant outcome: a direct path formed and carried the
    // post-punch payload off the relay.
    assert!(
        alice.direct_connections_established >= 1 && bob.direct_connections_established >= 1,
        "the punch (or coordinated direct dials) must land a direct connection on a \
         port-restricted cone (alice direct={}, bob direct={})",
        alice.direct_connections_established,
        bob.direct_connections_established,
    );
}
