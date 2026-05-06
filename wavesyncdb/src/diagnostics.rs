//! Engine-wide diagnostics counters.
//!
//! Engine code increments [`Counters`] (via the `Arc<Counters>` shared with
//! `WaveSyncDbInner`); applications read [`Snapshot`]s via
//! [`WaveSyncDb::diagnostics`](crate::WaveSyncDb::diagnostics).
//!
//! ## Why this exists
//!
//! Network-level bug-hunting at the libp2p layer is hard without
//! quantitative state. Concretely: PR #25 was about a circuit-relay
//! reservation storm that produced 19+ `ReservationReqAccepted` events
//! within 1s of pairing, but with no in-engine signal we caught it only
//! by reading the relay log line by line. With these counters the same
//! bug becomes a single comparison: `circuit_reservation_attempts`
//! before vs. after a code change. See [issue #27] for the broader
//! rationale.
//!
//! ## Why atomic counters
//!
//! The engine task increments from one thread; UI / debug-panel /
//! Dioxus-hook readers run on others. Keeping the storage in
//! [`AtomicU64`] makes a snapshot a sequence of `Relaxed` loads — no
//! lock acquisition on the hot path — and removes the risk of a slow
//! reader blocking the engine event loop. The snapshot is a *consistent
//! enough* view for human/UI consumption (each counter is read
//! atomically; cross-counter consistency is not guaranteed and not
//! needed for the use case).
//!
//! ## What this is **not**
//!
//! * Not a Prometheus / OpenTelemetry exporter — counters live only in
//!   memory; an exporter is a follow-up if we ever ship a server-mode
//!   build (see issue #27 non-goals).
//! * Not retained across engine restarts; the snapshot reflects the
//!   current process's lifetime.
//! * Not per-peer; per-peer metrics extend [`crate::network_status::PeerInfo`]
//!   in the next phase of this work.
//!
//! [issue #27]: https://github.com/pvg13/WaveSyncDB/issues/27

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Atomic counters incremented by the engine task. Cloned via `Arc` between
/// the engine and the owning [`WaveSyncDb`](crate::WaveSyncDb).
#[derive(Default, Debug)]
pub(crate) struct Counters {
    /// `swarm.listen_on(<relay>/p2p-circuit)` calls that the engine
    /// actually issued (not the ones short-circuited by
    /// `try_listen_on_circuit`'s idempotency guard from PR #25).
    pub circuit_reservation_attempts: AtomicU64,
    /// `relay::client::Event::ReservationReqAccepted` events the engine
    /// observed, including renewals. Pre-PR-#25 a single pairing emitted
    /// 19+ of these in a 1s burst — a healthy run shows 1 + the
    /// proactive-renewal cadence.
    pub circuit_reservations_accepted: AtomicU64,

    /// `swarm.dial` calls the engine routed through `try_dial_relay`,
    /// `dial_introduced_peer`, mDNS dial, or rendezvous dial — counts
    /// *attempts*, not unique peers.
    pub peer_dial_attempts: AtomicU64,
    /// `SwarmEvent::ConnectionEstablished` events for non-infrastructure
    /// peers. The ratio of `peer_dial_successes / peer_dial_attempts` is
    /// the per-engine dial-success rate (under 50% on most NAT pairs is
    /// expected and acceptable; under 5% indicates a real problem).
    pub peer_dial_successes: AtomicU64,
    /// `SwarmEvent::OutgoingConnectionError` for non-infrastructure peers.
    /// Most of these are routine (transport not supported, peer behind
    /// symmetric NAT); a sustained spike is a discovery-layer bug.
    pub peer_dial_failures: AtomicU64,

    /// Distinct peer-id discoveries via the mDNS `Discovered` event.
    /// Counts peer-id arrivals, not address arrivals — multiple addresses
    /// for the same peer in one mDNS query increment this once.
    pub mdns_discoveries: AtomicU64,
    /// Peer introductions returned by the relay in a `PushResponse::PeerList`
    /// (i.e. peers that were on the topic when *we* announced presence).
    /// Pre-introduction-dedup the same peer could be counted N times per
    /// announce cycle if it had N addresses; post-fix this counts unique
    /// peer-ids.
    pub peerlist_introductions: AtomicU64,
    /// Peer introductions delivered via `PushRequest::PeerJoined`
    /// (i.e. someone joined the topic *after* we announced).
    pub peerjoined_introductions: AtomicU64,

    /// `swarm.dial` calls fired from the cached-peer-addresses cache at
    /// engine startup (see [`crate::peer_addrs`]). The success path is
    /// already counted in `peer_dial_successes` via `ConnectionEstablished`;
    /// this counter quantifies how much faster cold-start ought to be on
    /// runs with a warm cache.
    pub cached_addr_dials: AtomicU64,

    /// `dcutr::Event` arrivals — every direct-connection upgrade attempt
    /// the relay-routed connection produced. Each event resolves to either
    /// a success (counted in [`Self::dcutr_upgrades_succeeded`]) or a
    /// failure. Subtraction gives the failure count.
    pub dcutr_upgrades_attempted: AtomicU64,
    /// `dcutr::Event` with `Ok(_)` result — successful upgrades from
    /// circuit-relay to a direct peer-to-peer connection. After a
    /// successful upgrade, sync traffic flows direct and the relay stops
    /// being on the data path. The ratio
    /// `dcutr_upgrades_succeeded / dcutr_upgrades_attempted` tracks how
    /// often hole-punching wins under the network conditions the engine
    /// is actually facing — typically ~70% on mixed home / office NATs,
    /// ~10–30% on cellular (carrier-grade NAT defeats hole punching).
    pub dcutr_upgrades_succeeded: AtomicU64,
}

impl Counters {
    /// Read every counter into a [`Snapshot`]. `Relaxed` ordering: each
    /// counter is read atomically but cross-counter consistency is not
    /// guaranteed (and not needed for human/UI consumption).
    pub(crate) fn snapshot(&self) -> Snapshot {
        Snapshot {
            circuit_reservation_attempts: self.circuit_reservation_attempts.load(Ordering::Relaxed),
            circuit_reservations_accepted: self
                .circuit_reservations_accepted
                .load(Ordering::Relaxed),
            peer_dial_attempts: self.peer_dial_attempts.load(Ordering::Relaxed),
            peer_dial_successes: self.peer_dial_successes.load(Ordering::Relaxed),
            peer_dial_failures: self.peer_dial_failures.load(Ordering::Relaxed),
            mdns_discoveries: self.mdns_discoveries.load(Ordering::Relaxed),
            peerlist_introductions: self.peerlist_introductions.load(Ordering::Relaxed),
            peerjoined_introductions: self.peerjoined_introductions.load(Ordering::Relaxed),
            cached_addr_dials: self.cached_addr_dials.load(Ordering::Relaxed),
            dcutr_upgrades_attempted: self.dcutr_upgrades_attempted.load(Ordering::Relaxed),
            dcutr_upgrades_succeeded: self.dcutr_upgrades_succeeded.load(Ordering::Relaxed),
        }
    }
}

/// Read-only snapshot of the engine's diagnostics counters.
///
/// Obtained via [`WaveSyncDb::diagnostics`](crate::WaveSyncDb::diagnostics).
/// Each field is a monotonically-increasing count over the engine's
/// lifetime — to derive a rate, sample twice and divide by elapsed time.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub circuit_reservation_attempts: u64,
    pub circuit_reservations_accepted: u64,
    pub peer_dial_attempts: u64,
    pub peer_dial_successes: u64,
    pub peer_dial_failures: u64,
    pub mdns_discoveries: u64,
    pub peerlist_introductions: u64,
    pub peerjoined_introductions: u64,
    pub cached_addr_dials: u64,
    pub dcutr_upgrades_attempted: u64,
    pub dcutr_upgrades_succeeded: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn snapshot_returns_zero_for_fresh_counters() {
        let c = Counters::default();
        assert_eq!(c.snapshot(), Snapshot::default());
    }

    #[test]
    fn snapshot_observes_increments_across_threads() {
        let c = Arc::new(Counters::default());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = Arc::clone(&c);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    c.peer_dial_attempts.fetch_add(1, Ordering::Relaxed);
                    c.peer_dial_successes.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let snap = c.snapshot();
        assert_eq!(snap.peer_dial_attempts, 4_000);
        assert_eq!(snap.peer_dial_successes, 4_000);
        // Untouched counters stay zero.
        assert_eq!(snap.mdns_discoveries, 0);
    }
}
