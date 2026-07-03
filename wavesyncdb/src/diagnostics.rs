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
//! * [`PeerHealthStore`] tracks the per-peer substrate (bytes, sync/converge
//!   timestamps, catch-up RTT), but nothing here writes to it yet — the
//!   engine plumbing that feeds it and mirrors it onto
//!   [`crate::network_status::PeerInfo`] lands in a later change.
//!
//! [issue #27]: https://github.com/pvg13/WaveSyncDB/issues/27

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use libp2p::PeerId;
use serde::{Deserialize, Serialize};

/// Upper bounds (inclusive, milliseconds) for the sync-RTT histogram
/// buckets — Prometheus-style "le" (less-or-equal) semantics. A round trip
/// slower than the last threshold falls into the implicit `+Inf` overflow
/// bucket (see [`RTT_BUCKET_COUNT`]).
const RTT_BUCKETS_MS: [u64; 13] = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// `RTT_BUCKETS_MS` buckets plus one implicit `+Inf` overflow bucket.
const RTT_BUCKET_COUNT: usize = RTT_BUCKETS_MS.len() + 1;

/// Current unix-epoch time in milliseconds. Display/diagnostics only —
/// never an input to conflict resolution, which stays fully deterministic
/// on `(col_version, value_bytes, site_id)`.
// Wired by the engine in the next change: no call site outside tests yet.
#[allow(dead_code)]
pub(crate) fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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

    /// `ConnectionEstablished` for a non-infrastructure peer whose remote
    /// address is a circuit-relay address (`/.../p2p-circuit/...`) — i.e. the
    /// connection (and any data over it) is being carried by the relay server,
    /// which costs relay bandwidth/infrastructure. A DCUtR upgrade later
    /// establishes a separate *direct* connection (counted in
    /// [`Self::direct_connections_established`]); comparing the two against
    /// [`Self::dcutr_upgrades_succeeded`] shows how much traffic the relay is
    /// actually carrying vs. how often peers move off it.
    pub relayed_connections_established: AtomicU64,
    /// `ConnectionEstablished` for a non-infrastructure peer over a direct
    /// (non-circuit) address — the relay is not on the data path for it.
    pub direct_connections_established: AtomicU64,

    /// Reconcile-digest exchanges (#82) that *proved* convergence — the peer
    /// returned a digest equal to ours, so the two databases are byte-identical
    /// for that group. This is the convergence signal the version-vector
    /// catch-up cannot provide (matching `db_version` is height, not equality).
    pub reconcile_converged: AtomicU64,
    /// Reconcile-digest exchanges that found a mismatch — the peers differ and
    /// the version-vector catch-up is relied on to transfer the diff.
    pub reconcile_diverged: AtomicU64,
    /// Fan-out pushes re-sent from the pending-push retry set (#81) — a local
    /// changeset that wasn't confirmed delivered by its initial real-time push
    /// and got redelivered on the short retry cadence. Steady state is 0; a
    /// rising count means real-time pushes are being dropped (slow/lossy peers).
    pub pending_pushes_redelivered: AtomicU64,
    /// Relay-carried (circuit) connections we proactively closed once a direct
    /// path to the same peer came up (#84 DERP demotion). Each increment is a
    /// peer whose steady-state data moved off the paid relay onto a direct
    /// connection. Read alongside `relayed_connections_established` to see how
    /// much relay traffic the demotion is shedding.
    pub relay_connections_demoted: AtomicU64,

    /// Wire bytes sent to peers over a circuit-relay connection. Counted at
    /// the HMAC-sign call sites, which already run `serde_json::to_vec` on
    /// every outbound message — `bytes.len()` there is the wire body size,
    /// so this is free (no extra serialization).
    pub relay_bytes_out: AtomicU64,
    /// Wire bytes received from peers over a circuit-relay connection.
    pub relay_bytes_in: AtomicU64,
    /// Wire bytes sent to peers over a direct (non-relay) connection.
    pub direct_bytes_out: AtomicU64,
    /// Wire bytes received from peers over a direct (non-relay) connection.
    pub direct_bytes_in: AtomicU64,

    /// Per-bucket counts for catch-up (version-vector) round-trip latency.
    /// Index `i` for `i < RTT_BUCKETS_MS.len()` counts round trips with
    /// `rtt_ms <= RTT_BUCKETS_MS[i]`; the last index is the `+Inf` overflow
    /// bucket for anything slower than the highest threshold. See
    /// [`Counters::observe_sync_rtt`].
    pub sync_rtt_buckets: [AtomicU64; RTT_BUCKET_COUNT],
}

impl Counters {
    /// Record a catch-up (version-vector) round-trip time into the
    /// histogram. Linear scan over the 13 thresholds — cheap at this
    /// message rate and keeps the bucket boundaries readable as a flat
    /// array literal rather than a binary-search table.
    // Wired by the engine in the next change: no call site exists yet.
    #[allow(dead_code)]
    pub(crate) fn observe_sync_rtt(&self, ms: u64) {
        let idx = RTT_BUCKETS_MS
            .iter()
            .position(|&le| ms <= le)
            .unwrap_or(RTT_BUCKET_COUNT - 1);
        self.sync_rtt_buckets[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Render the bucket counters as `(le_ms, count)` pairs, `u64::MAX`
    /// marking the `+Inf` overflow bucket.
    fn sync_rtt_histogram(&self) -> Vec<(u64, u64)> {
        let mut hist: Vec<(u64, u64)> = RTT_BUCKETS_MS
            .iter()
            .zip(self.sync_rtt_buckets.iter())
            .map(|(&le, count)| (le, count.load(Ordering::Relaxed)))
            .collect();
        hist.push((
            u64::MAX,
            self.sync_rtt_buckets[RTT_BUCKET_COUNT - 1].load(Ordering::Relaxed),
        ));
        hist
    }

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
            relayed_connections_established: self
                .relayed_connections_established
                .load(Ordering::Relaxed),
            direct_connections_established: self
                .direct_connections_established
                .load(Ordering::Relaxed),
            reconcile_converged: self.reconcile_converged.load(Ordering::Relaxed),
            reconcile_diverged: self.reconcile_diverged.load(Ordering::Relaxed),
            pending_pushes_redelivered: self.pending_pushes_redelivered.load(Ordering::Relaxed),
            relay_connections_demoted: self.relay_connections_demoted.load(Ordering::Relaxed),
            relay_bytes_out: self.relay_bytes_out.load(Ordering::Relaxed),
            relay_bytes_in: self.relay_bytes_in.load(Ordering::Relaxed),
            direct_bytes_out: self.direct_bytes_out.load(Ordering::Relaxed),
            direct_bytes_in: self.direct_bytes_in.load(Ordering::Relaxed),
            sync_rtt_histogram: self.sync_rtt_histogram(),
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
    pub relayed_connections_established: u64,
    pub direct_connections_established: u64,
    pub reconcile_converged: u64,
    pub reconcile_diverged: u64,
    pub pending_pushes_redelivered: u64,
    pub relay_connections_demoted: u64,

    /// New fields default on deserialize so a snapshot serialized before
    /// they existed still parses (e.g. a persisted diagnostics dump, or an
    /// e2e client mid-upgrade).
    #[serde(default)]
    pub relay_bytes_out: u64,
    #[serde(default)]
    pub relay_bytes_in: u64,
    #[serde(default)]
    pub direct_bytes_out: u64,
    #[serde(default)]
    pub direct_bytes_in: u64,
    /// Catch-up round-trip-time histogram as `(le_ms, count)` pairs,
    /// `u64::MAX` marking the `+Inf` overflow bucket. See
    /// [`Counters::observe_sync_rtt`] for bucket boundaries.
    #[serde(default)]
    pub sync_rtt_histogram: Vec<(u64, u64)>,
}

impl Snapshot {
    /// Fraction of total (relay + direct) wire bytes that went over the
    /// circuit relay — the client-facing gauge for #84 DERP-demotion
    /// effectiveness. `None` when no traffic has been counted yet (avoids
    /// a misleading `0.0`, which would read as "all traffic is direct").
    pub fn relay_traffic_ratio(&self) -> Option<f64> {
        let relay = self.relay_bytes_in + self.relay_bytes_out;
        let direct = self.direct_bytes_in + self.direct_bytes_out;
        let total = relay + direct;
        if total == 0 {
            None
        } else {
            Some(relay as f64 / total as f64)
        }
    }
}

/// Per-peer health snapshot returned by [`PeerHealthStore::snapshot_for`].
/// Mirrors the fields the engine attaches to
/// [`crate::network_status::PeerInfo`] in a later change.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
// Wired by the engine in the next change: constructed only in tests today.
#[allow(dead_code)]
pub(crate) struct PeerHealth {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub last_synced_at_ms: Option<u64>,
    pub last_converged_at_ms: Option<u64>,
    pub sync_rtt_ms: Option<u64>,
}

/// Per-peer health bookkeeping, shared via `Arc` between the engine loop
/// and spawned responder tasks. Several HMAC response-sign sites
/// (`sync_handler.rs`'s version-vector responder among them) run inside
/// `tokio::spawn` with no `&mut self` available, so this has to be a
/// cloneable shared handle rather than `EngineRunner`-private state.
///
/// A `std::sync::Mutex` (not `tokio::sync::Mutex`) is deliberate: every
/// critical section here is a couple of `HashMap` operations with no
/// `.await` inside it, so the lock is held for a bounded, tiny amount of
/// time and never across a suspension point.
#[derive(Debug, Default)]
// Wired by the engine in the next change: no producer/consumer call sites yet.
#[allow(dead_code)]
pub(crate) struct PeerHealthStore(Mutex<HashMap<PeerId, PeerHealth>>);

// Wired by the engine in the next change: no producer/consumer call sites yet.
#[allow(dead_code)]
impl PeerHealthStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Read-only snapshot for one peer; `None` if nothing has been
    /// recorded for it yet (e.g. discovered but never synced).
    pub(crate) fn snapshot_for(&self, peer: &PeerId) -> Option<PeerHealth> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(peer)
            .copied()
    }

    /// Record wire bytes transferred with `peer`. `inbound = true` adds to
    /// `bytes_in`, `false` to `bytes_out`.
    pub(crate) fn record_bytes(&self, peer: PeerId, n: u64, inbound: bool) {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(peer).or_default();
        if inbound {
            entry.bytes_in += n;
        } else {
            entry.bytes_out += n;
        }
    }

    /// Stamp `peer` as synced right now — applied a `ChangesetResponse`,
    /// applied an inbound `Push`, or received a `PushAck` from it.
    pub(crate) fn stamp_synced(&self, peer: PeerId) {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(peer).or_default().last_synced_at_ms = Some(unix_now_ms());
    }

    /// Stamp `peer` as converged right now (its reconcile digest matched
    /// ours — see `Counters::reconcile_converged`).
    pub(crate) fn stamp_converged(&self, peer: PeerId) {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(peer).or_default().last_converged_at_ms = Some(unix_now_ms());
    }

    /// Record the latest catch-up round-trip time observed for `peer`.
    pub(crate) fn record_rtt(&self, peer: PeerId, rtt_ms: u64) {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        guard.entry(peer).or_default().sync_rtt_ms = Some(rtt_ms);
    }

    /// Drop all bookkeeping for `peer`. Call on the final
    /// `PeerDisconnected` (not a transient reconnect) so the map doesn't
    /// grow unboundedly across a long-lived engine's peer churn.
    pub(crate) fn prune(&self, peer: &PeerId) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn snapshot_returns_zero_for_fresh_counters() {
        let c = Counters::default();
        // `Snapshot::default()` has an *empty* histogram Vec, but a fresh
        // `Counters` renders one zero-count entry per bucket (including
        // +Inf) — both are "zero traffic", just different shapes. Build
        // the populated-but-zero shape explicitly rather than comparing
        // against `Snapshot::default()`.
        let expected = Snapshot {
            sync_rtt_histogram: RTT_BUCKETS_MS
                .iter()
                .map(|&le| (le, 0))
                .chain(std::iter::once((u64::MAX, 0)))
                .collect(),
            ..Snapshot::default()
        };
        assert_eq!(c.snapshot(), expected);
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

    #[test]
    fn observe_sync_rtt_first_bucket_boundary() {
        let c = Counters::default();
        c.observe_sync_rtt(1);
        let hist = c.snapshot().sync_rtt_histogram;
        assert_eq!(hist.len(), RTT_BUCKET_COUNT);
        assert_eq!(hist[0], (1, 1));
        // Every other bucket, including +Inf, stays untouched.
        assert!(hist[1..].iter().all(|&(_, count)| count == 0));
    }

    #[test]
    fn observe_sync_rtt_overflow_bucket_boundary() {
        let c = Counters::default();
        c.observe_sync_rtt(10_001);
        let hist = c.snapshot().sync_rtt_histogram;
        let (le, count) = *hist.last().unwrap();
        assert_eq!(le, u64::MAX);
        assert_eq!(count, 1);
        // Every finite bucket stays untouched.
        assert!(hist[..hist.len() - 1].iter().all(|&(_, count)| count == 0));
    }

    #[test]
    fn relay_traffic_ratio_none_when_no_traffic_counted() {
        let snap = Snapshot::default();
        assert_eq!(snap.relay_traffic_ratio(), None);
    }

    #[test]
    fn relay_traffic_ratio_computes_fraction() {
        let snap = Snapshot {
            relay_bytes_out: 30,
            direct_bytes_out: 70,
            ..Snapshot::default()
        };
        assert_eq!(snap.relay_traffic_ratio(), Some(0.3));
    }

    #[test]
    fn peer_health_store_roundtrip_and_prune() {
        let store = PeerHealthStore::new();
        let peer = PeerId::random();
        assert_eq!(store.snapshot_for(&peer), None);

        store.record_bytes(peer, 100, true);
        store.record_bytes(peer, 50, false);
        store.stamp_synced(peer);
        store.stamp_converged(peer);
        store.record_rtt(peer, 42);

        let health = store.snapshot_for(&peer).expect("peer was recorded");
        assert_eq!(health.bytes_in, 100);
        assert_eq!(health.bytes_out, 50);
        assert!(health.last_synced_at_ms.is_some());
        assert!(health.last_converged_at_ms.is_some());
        assert_eq!(health.sync_rtt_ms, Some(42));

        store.prune(&peer);
        assert_eq!(store.snapshot_for(&peer), None);
    }
}
