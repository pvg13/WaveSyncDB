//! Prometheus metrics for the relay: app-layer families updated by the
//! event loop, plus the circuit ledger that meters circuit-seconds — the
//! billing signal for relay-carried traffic. Per-circuit BYTES are not
//! surfaced by libp2p (the relay counts them internally only to enforce
//! limits), so duration is the exact meterable quantity; global bytes come
//! from the bandwidth transport metrics. Label discipline: never PeerId
//! (unbounded cardinality); topics use the short 12-char prefix.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use libp2p::PeerId;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

/// Truncates a derived topic to a bounded-cardinality label, mirroring the
/// `short_topic` helper in `main.rs` so metric labels line up with log lines.
/// Tries the current `wavesync2-` prefix first, then the pre-KDF-cutover
/// `wavesync-` prefix (belt-and-braces — a stale peer or log replay might
/// still carry the old form), then passes anything else through untouched.
fn short(s: &str) -> String {
    if let Some(hex) = s.strip_prefix("wavesync2-")
        && hex.len() > 10
    {
        return format!("wavesync2-{}…", &hex[..10]);
    }
    match s.strip_prefix("wavesync-") {
        Some(hex) if hex.len() > 10 => format!("wavesync-{}…", &hex[..10]),
        _ => s.to_string(),
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabel {
    pub outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicOutcomeLabel {
    pub topic: String,
    pub outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PushLabel {
    pub platform: String,
    pub outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectionLabel {
    pub direction: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ReservationOutcome {
    Accepted,
    Denied,
    Renewed,
}

impl ReservationOutcome {
    fn as_str(&self) -> &'static str {
        match self {
            ReservationOutcome::Accepted => "accepted",
            ReservationOutcome::Denied => "denied",
            ReservationOutcome::Renewed => "renewed",
        }
    }
}

/// App-layer metrics: connections, reservations, rendezvous, and push
/// delivery. All fields are `Family`/`Gauge` handles — cheap to clone
/// (internally `Arc`'d) so this can be handed to every task that needs to
/// record an event.
#[derive(Clone)]
pub struct RelayMetrics {
    connections_total: Family<DirectionLabel, Counter<u64>>,
    connected_peers: Gauge<i64>,
    reservations_total: Family<OutcomeLabel, Counter<u64>>,
    active_reservations: Gauge<i64>,
    rendezvous_registrations_total: Family<TopicLabel, Counter<u64>>,
    rendezvous_discovers_total: Counter<u64>,
    push_notifies_total: Family<TopicLabel, Counter<u64>>,
    pushes_sent_total: Family<PushLabel, Counter<u64>>,
    registered_tokens: Gauge<i64>,
}

impl RelayMetrics {
    pub fn new(registry: &mut Registry) -> Self {
        // NOTE on names: prometheus-client's OpenMetrics text encoder appends
        // "_total" to every Counter sample unconditionally (it does not check
        // whether the registered name already ends in "_total"). Registering
        // a counter as "foo_total" therefore wires up as "foo_total_total".
        // Every counter family below is registered WITHOUT the "_total"
        // suffix so the wire name comes out right; Gauges are exempt (the
        // encoder doesn't touch gauge names).
        let connections_total = Family::<DirectionLabel, Counter<u64>>::default();
        registry.register(
            "relay_connections",
            "Total libp2p connections established, by direction",
            connections_total.clone(),
        );

        let connected_peers = Gauge::<i64>::default();
        registry.register(
            "relay_connected_peers",
            "Currently connected peers",
            connected_peers.clone(),
        );

        let reservations_total = Family::<OutcomeLabel, Counter<u64>>::default();
        registry.register(
            "relay_reservations",
            "Circuit relay v2 reservation requests, by outcome",
            reservations_total.clone(),
        );

        let active_reservations = Gauge::<i64>::default();
        registry.register(
            "relay_active_reservations",
            "Currently active circuit relay v2 reservations",
            active_reservations.clone(),
        );

        // Topic labels below are bounded by `short()` to a fixed-width
        // prefix, but on a relay serving many independent groups the
        // cardinality still grows with the number of distinct groups seen.
        // If that ever becomes a problem on a public relay, a future
        // `--metrics-topic-labels=off` flag could degrade these families to
        // a single unlabeled total instead of dropping them outright.
        let rendezvous_registrations_total = Family::<TopicLabel, Counter<u64>>::default();
        registry.register(
            "relay_rendezvous_registrations",
            "Rendezvous registrations received, by topic",
            rendezvous_registrations_total.clone(),
        );

        let rendezvous_discovers_total = Counter::<u64>::default();
        registry.register(
            "relay_rendezvous_discovers",
            "Rendezvous discover requests served",
            rendezvous_discovers_total.clone(),
        );

        let push_notifies_total = Family::<TopicLabel, Counter<u64>>::default();
        registry.register(
            "relay_push_notifies",
            "Push wake-up notifications triggered, by topic",
            push_notifies_total.clone(),
        );

        let pushes_sent_total = Family::<PushLabel, Counter<u64>>::default();
        registry.register(
            "relay_pushes_sent",
            "Push notifications sent, by platform and outcome",
            pushes_sent_total.clone(),
        );

        let registered_tokens = Gauge::<i64>::default();
        registry.register(
            "relay_registered_tokens",
            "Currently registered push tokens",
            registered_tokens.clone(),
        );

        Self {
            connections_total,
            connected_peers,
            reservations_total,
            active_reservations,
            rendezvous_registrations_total,
            rendezvous_discovers_total,
            push_notifies_total,
            pushes_sent_total,
            registered_tokens,
        }
    }

    pub fn connection_established(&self, inbound: bool) {
        let direction = if inbound { "inbound" } else { "outbound" }.to_string();
        self.connections_total
            .get_or_create(&DirectionLabel { direction })
            .inc();
    }

    pub fn set_connected_peers(&self, n: i64) {
        self.connected_peers.set(n);
    }

    pub fn reservation(&self, outcome: ReservationOutcome) {
        self.reservations_total
            .get_or_create(&OutcomeLabel {
                outcome: outcome.as_str().to_string(),
            })
            .inc();
        if matches!(outcome, ReservationOutcome::Accepted) {
            self.active_reservations.inc();
        }
    }

    pub fn reservation_ended(&self) {
        self.active_reservations.dec();
    }

    pub fn rendezvous_registered(&self, topic: &str) {
        self.rendezvous_registrations_total
            .get_or_create(&TopicLabel {
                topic: short(topic),
            })
            .inc();
    }

    pub fn rendezvous_discover_served(&self) {
        self.rendezvous_discovers_total.inc();
    }

    pub fn push_notify(&self, topic: &str) {
        self.push_notifies_total
            .get_or_create(&TopicLabel {
                topic: short(topic),
            })
            .inc();
    }

    pub fn push_sent(&self, platform: &str, outcome: &str) {
        self.pushes_sent_total
            .get_or_create(&PushLabel {
                platform: platform.to_string(),
                outcome: outcome.to_string(),
            })
            .inc();
    }

    pub fn set_registered_tokens(&self, n: i64) {
        self.registered_tokens.set(n);
    }
}

/// Computes the topic attributed to a circuit between `src` and `dst`: the
/// single topic both peers have in common. Zero or more than one shared
/// topic is ambiguous (or the pair has no relationship we can attribute a
/// cost to) and is reported as `"unknown"` rather than guessed.
pub(crate) fn attribute_topic(
    src: &PeerId,
    dst: &PeerId,
    topics_of: &dyn Fn(&PeerId) -> Vec<String>,
) -> String {
    let src_topics = topics_of(src);
    let dst_topics: HashSet<String> = topics_of(dst).into_iter().collect();
    let common: HashSet<&String> = src_topics
        .iter()
        .filter(|t| dst_topics.contains(*t))
        .collect();
    match common.len() {
        1 => short(common.into_iter().next().unwrap()),
        _ => "unknown".to_string(),
    }
}

/// Tracks in-flight relay circuits so their duration can be attributed to a
/// topic and billed as circuit-seconds — the only meterable quantity libp2p
/// exposes per circuit (see module docs).
pub struct CircuitLedger {
    circuits_opened_total: Family<TopicLabel, Counter<u64>>,
    circuits_denied_total: Counter<u64>,
    circuits_closed_total: Family<TopicOutcomeLabel, Counter<u64>>,
    circuit_seconds_total: Family<TopicLabel, Counter<f64, AtomicU64>>,
    active_circuits: Gauge<i64>,
    // libp2p allows multiple concurrent circuits between the same (src, dst)
    // pair, and `CircuitClosed` carries no circuit id to disambiguate which
    // one ended. A per-pair queue (rather than a single slot) lets each
    // concurrent circuit be tracked and billed independently; `close` pops
    // the front (FIFO), billing the oldest still-open circuit for that pair
    // as the deterministic fair approximation when the real identity is
    // unknowable from the event alone.
    active: HashMap<(PeerId, PeerId), VecDeque<(Instant, String)>>,
}

impl CircuitLedger {
    pub fn new(registry: &mut Registry) -> Self {
        // See the NOTE in `RelayMetrics::new` above: counters register
        // without "_total" because the OpenMetrics encoder appends it itself.
        let circuits_opened_total = Family::<TopicLabel, Counter<u64>>::default();
        registry.register(
            "relay_circuits_opened",
            "Relay circuits opened, by attributed topic",
            circuits_opened_total.clone(),
        );

        let circuits_denied_total = Counter::<u64>::default();
        registry.register(
            "relay_circuits_denied",
            "Relay circuit requests denied",
            circuits_denied_total.clone(),
        );

        let circuits_closed_total = Family::<TopicOutcomeLabel, Counter<u64>>::default();
        registry.register(
            "relay_circuits_closed",
            "Relay circuits closed, by attributed topic and outcome",
            circuits_closed_total.clone(),
        );

        // Topic-labeled — see the cardinality note in `RelayMetrics::new`.
        let circuit_seconds_total = Family::<TopicLabel, Counter<f64, AtomicU64>>::default();
        registry.register(
            "relay_circuit_seconds",
            "Cumulative relay circuit duration in seconds, by attributed topic — the billing \
             meter. Values accrue only when a circuit closes (or is pruned by the leak-guard \
             sweep, which now bills the same way) — a relay restart loses any in-flight \
             circuit's accumulated-but-unclosed time.",
            circuit_seconds_total.clone(),
        );

        let active_circuits = Gauge::<i64>::default();
        registry.register(
            "relay_active_circuits",
            "Currently open relay circuits",
            active_circuits.clone(),
        );

        Self {
            circuits_opened_total,
            circuits_denied_total,
            circuits_closed_total,
            circuit_seconds_total,
            active_circuits,
            active: HashMap::new(),
        }
    }

    pub fn open(
        &mut self,
        src: PeerId,
        dst: PeerId,
        now: Instant,
        topics_of: &dyn Fn(&PeerId) -> Vec<String>,
    ) {
        let topic = attribute_topic(&src, &dst, topics_of);
        self.circuits_opened_total
            .get_or_create(&TopicLabel {
                topic: topic.clone(),
            })
            .inc();
        self.active_circuits.inc();
        self.active
            .entry((src, dst))
            .or_default()
            .push_back((now, topic));
    }

    pub fn denied(&mut self) {
        self.circuits_denied_total.inc();
    }

    pub fn close(&mut self, src: PeerId, dst: PeerId, now: Instant, ok: bool) {
        let key = (src, dst);
        let popped = match self.active.get_mut(&key) {
            Some(queue) => {
                let popped = queue.pop_front();
                if queue.is_empty() {
                    self.active.remove(&key);
                }
                popped
            }
            None => None,
        };
        match popped {
            Some((opened_at, topic)) => {
                let elapsed = now.saturating_duration_since(opened_at);
                self.circuit_seconds_total
                    .get_or_create(&TopicLabel {
                        topic: topic.clone(),
                    })
                    .inc_by(elapsed.as_secs_f64());
                let outcome = if ok { "ok" } else { "error" }.to_string();
                self.circuits_closed_total
                    .get_or_create(&TopicOutcomeLabel { topic, outcome })
                    .inc();
                self.active_circuits.dec();
            }
            None => {
                // No matching `open` — the circuit was never tracked (e.g. a
                // close event for a circuit that predates this process, or a
                // bug elsewhere). Counted as an error so it's visible without
                // being attributable to a topic.
                self.circuits_closed_total
                    .get_or_create(&TopicOutcomeLabel {
                        topic: "unknown".to_string(),
                        outcome: "error".to_string(),
                    })
                    .inc();
            }
        }
    }

    /// Prunes entries older than `max_age` that never received a matching
    /// `close` — a leak guard against missed relay events, not a normal
    /// code path. Before dropping each pruned entry, its elapsed time is
    /// billed to `circuit_seconds_total` under its attributed topic, same as
    /// a normal `close`: for a billing meter, pruning without crediting the
    /// time already spent would silently under-bill any legitimate
    /// long-lived circuit that happens to outlive the sweep age — the real
    /// `CircuitClosed` event, if it ever arrives, finds no matching `open`
    /// and is counted as an orphan error instead of double-billing.
    /// Returns the number of entries pruned.
    pub fn sweep(&mut self, now: Instant, max_age: Duration) -> usize {
        let mut pruned = 0usize;
        // Family is cheap to clone (internally Arc'd) — cloned out here so
        // the billing closure below doesn't need to borrow `self` while
        // `self.active` is already mutably borrowed by `retain`.
        let circuit_seconds_total = self.circuit_seconds_total.clone();
        self.active.retain(|_, queue| {
            let before = queue.len();
            queue.retain(|(opened_at, topic)| {
                let elapsed = now.saturating_duration_since(*opened_at);
                let expired = elapsed > max_age;
                if expired {
                    circuit_seconds_total
                        .get_or_create(&TopicLabel {
                            topic: topic.clone(),
                        })
                        .inc_by(elapsed.as_secs_f64());
                }
                !expired
            });
            pruned += before - queue.len();
            !queue.is_empty()
        });
        if pruned > 0 {
            self.active_circuits.dec_by(pruned as i64);
            tracing::warn!(
                "CircuitLedger: swept {pruned} leaked circuit entries older than {max_age:?} (billed elapsed seconds before removal)"
            );
        }
        pruned
    }

    pub fn active_count(&self) -> usize {
        self.active.values().map(|q| q.len()).sum()
    }
}

/// Serves `/metrics` (OpenMetrics text exposition of `registry`) and
/// `/healthz` (plain liveness check) on `addr`. Runs until the returned
/// future is dropped/aborted — callers `tokio::spawn` this alongside the
/// swarm event loop.
pub async fn serve_metrics(
    addr: std::net::SocketAddr,
    registry: std::sync::Arc<std::sync::Mutex<Registry>>,
) -> std::io::Result<()> {
    use axum::response::IntoResponse;
    use axum::{Router, routing::get};

    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let registry = registry.clone();
                async move {
                    let mut body = String::new();
                    // A poisoned mutex is still structurally readable; scrape endpoint must degrade gracefully.
                    let reg = registry
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match prometheus_client::encoding::text::encode(&mut body, &reg) {
                        Ok(()) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                "application/openmetrics-text; version=1.0.0; charset=utf-8",
                            )],
                            body,
                        )
                            .into_response(),
                        Err(e) => (
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            format!("encode error: {e}"),
                        )
                            .into_response(),
                    }
                }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Metrics endpoint on http://{addr}/metrics");
    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::PeerId;
    use std::time::{Duration, Instant};

    fn topics<'a>(map: &'a [(&'a PeerId, &'a [&'a str])]) -> impl Fn(&PeerId) -> Vec<String> + 'a {
        move |p| {
            map.iter()
                .find(|(id, _)| *id == p)
                .map(|(_, ts)| ts.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default()
        }
    }

    fn registry_text(registry: &prometheus_client::registry::Registry) -> String {
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, registry).unwrap();
        out
    }

    #[test]
    fn short_truncates_wavesync2_topics() {
        let hex = "a".repeat(64);
        let topic = format!("wavesync2-{hex}");
        assert_eq!(short(&topic), format!("wavesync2-{}…", &hex[..10]));
    }

    #[test]
    fn short_falls_back_to_pre_cutover_prefix() {
        // Belt-and-braces: a stale pre-KDF-cutover topic must still
        // truncate sensibly rather than passing through unbounded.
        let hex = "b".repeat(64);
        let topic = format!("wavesync-{hex}");
        assert_eq!(short(&topic), format!("wavesync-{}…", &hex[..10]));
    }

    #[test]
    fn short_passes_through_unrecognized_strings() {
        assert_eq!(short("unknown"), "unknown");
    }

    #[test]
    fn circuit_seconds_accumulate_per_topic() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let (a, b) = (PeerId::random(), PeerId::random());
        let t0 = Instant::now();
        let shared_topic: [(&PeerId, &[&str]); 2] =
            [(&a, &["wavesync2-abc"]), (&b, &["wavesync2-abc"])];
        let lookup = topics(&shared_topic);

        ledger.open(a, b, t0, &lookup);
        assert_eq!(ledger.active_count(), 1);
        ledger.close(a, b, t0 + Duration::from_secs(7), true);
        assert_eq!(ledger.active_count(), 0);

        // Sequential circuit on the same pair adds up.
        ledger.open(a, b, t0 + Duration::from_secs(10), &lookup);
        ledger.close(a, b, t0 + Duration::from_secs(13), true);

        let text = registry_text(&reg);
        // 7 + 3 seconds attributed to the single shared topic (short label).
        // Exact sample-line prefix (not a bare substring) so a regression
        // that doubles the "_total" suffix (`..._total_total{`) would fail
        // this assertion instead of silently matching.
        assert!(
            text.contains("\nrelay_circuit_seconds_total{topic=\"wavesync2-abc\"} 10"),
            "sample line not found as expected:\n{text}"
        );
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn attribution_matrix() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let t0 = Instant::now();
        let (a, b, c, d) = (
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
            PeerId::random(),
        );

        // No shared topic -> unknown
        let no_overlap: [(&PeerId, &[&str]); 2] = [(&a, &["t1"]), (&b, &["t2"])];
        let lookup = topics(&no_overlap);
        ledger.open(a, b, t0, &lookup);
        ledger.close(a, b, t0 + Duration::from_secs(1), true);

        // Several shared topics -> unknown (ambiguous)
        let ambiguous: [(&PeerId, &[&str]); 2] = [(&c, &["t1", "t2"]), (&d, &["t1", "t2"])];
        let lookup = topics(&ambiguous);
        ledger.open(c, d, t0, &lookup);
        ledger.close(c, d, t0 + Duration::from_secs(1), true);

        // Unknown peers entirely -> unknown
        let (e, f) = (PeerId::random(), PeerId::random());
        let none: [(&PeerId, &[&str]); 0] = [];
        let lookup = topics(&none);
        ledger.open(e, f, t0, &lookup);
        ledger.close(e, f, t0 + Duration::from_secs(1), true);

        let text = registry_text(&reg);
        assert!(
            text.contains("\nrelay_circuits_closed_total{topic=\"unknown\",outcome=\"ok\"} 3"),
            "sample line not found as expected:\n{text}"
        );
        assert!(
            !text.contains("topic=\"t1\""),
            "no single-shared-topic circuit existed"
        );
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn orphan_close_is_safe_and_counted_as_error() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let (a, b) = (PeerId::random(), PeerId::random());
        ledger.close(a, b, Instant::now(), true); // never opened
        assert_eq!(ledger.active_count(), 0);
        let text = registry_text(&reg);
        assert!(
            text.contains("\nrelay_circuits_closed_total{topic=\"unknown\",outcome=\"error\"} 1"),
            "sample line not found as expected:\n{text}"
        );
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn sweep_caps_leaked_entries() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let t0 = Instant::now();
        ledger.open(PeerId::random(), PeerId::random(), t0, &topics(&[]));
        assert_eq!(
            ledger.sweep(
                t0 + Duration::from_secs(90_000),
                Duration::from_secs(86_400)
            ),
            1
        );
        assert_eq!(ledger.active_count(), 0);
    }

    /// A pruned entry must not be billed zero seconds just because it never
    /// received a matching `close` — otherwise a legitimate long-lived
    /// circuit that happens to outlive the sweep age gets its real duration
    /// silently discarded from the billing meter.
    #[test]
    fn sweep_bills_pruned_entries_before_removal() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let t0 = Instant::now();
        let (a, b) = (PeerId::random(), PeerId::random());
        let shared_topic: [(&PeerId, &[&str]); 2] =
            [(&a, &["wavesync2-abc"]), (&b, &["wavesync2-abc"])];
        let lookup = topics(&shared_topic);

        ledger.open(a, b, t0, &lookup);
        assert_eq!(
            ledger.sweep(
                t0 + Duration::from_secs(90_000),
                Duration::from_secs(86_400)
            ),
            1
        );
        assert_eq!(ledger.active_count(), 0);

        let text = registry_text(&reg);
        assert!(
            text.contains("\nrelay_circuit_seconds_total{topic=\"wavesync2-abc\"} 90000"),
            "pruned entry must be credited its elapsed seconds, not dropped silently:\n{text}"
        );
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn concurrent_same_pair_circuits_bill_independently() {
        let mut reg = prometheus_client::registry::Registry::default();
        let mut ledger = CircuitLedger::new(&mut reg);
        let (a, b) = (PeerId::random(), PeerId::random());
        let t0 = Instant::now();
        let shared_topic: [(&PeerId, &[&str]); 2] =
            [(&a, &["wavesync2-abc"]), (&b, &["wavesync2-abc"])];
        let lookup = topics(&shared_topic);

        ledger.open(a, b, t0, &lookup);
        ledger.open(a, b, t0 + Duration::from_secs(2), &lookup); // concurrent 2nd circuit
        assert_eq!(ledger.active_count(), 2);

        // FIFO: first close bills the first-opened circuit (10s), second the other (8s).
        ledger.close(a, b, t0 + Duration::from_secs(10), true);
        assert_eq!(ledger.active_count(), 1);
        ledger.close(a, b, t0 + Duration::from_secs(10), true);
        assert_eq!(ledger.active_count(), 0);

        let text = registry_text(&reg);
        assert!(
            text.contains("\nrelay_circuit_seconds_total{topic=\"wavesync2-abc\"} 18"),
            "sample line not found as expected:\n{text}"
        ); // 10 + 8 seconds, both billed
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn relay_metrics_families_render() {
        let mut reg = prometheus_client::registry::Registry::default();
        let m = RelayMetrics::new(&mut reg);
        m.connection_established(true);
        m.set_connected_peers(1);
        m.reservation(ReservationOutcome::Accepted);
        m.rendezvous_registered("wavesync2-abcdef123456");
        m.rendezvous_discover_served();
        m.push_notify("wavesync2-abcdef123456");
        m.push_sent("fcm", "budget_denied");
        m.set_registered_tokens(3);
        let text = registry_text(&reg);

        // Exact sample-line prefixes for every counter family — a bare
        // substring match (e.g. `text.contains("relay_connections_total")`)
        // would still pass against a doubled `..._total_total{` suffix, so
        // each check anchors on the `{` (or the line start for the
        // no-label counter) immediately after the single `_total`.
        for line_prefix in [
            "\nrelay_connections_total{",
            "\nrelay_reservations_total{",
            "\nrelay_rendezvous_registrations_total{",
            "\nrelay_rendezvous_discovers_total ",
            "\nrelay_push_notifies_total{",
            "\nrelay_pushes_sent_total{",
        ] {
            assert!(
                text.contains(line_prefix),
                "missing {line_prefix:?} in:\n{text}"
            );
        }
        // Gauges are exempt from the encoder's "_total" suffix — assert the
        // bare names so a regression that accidentally suffixed a gauge
        // would also be caught.
        for line_prefix in ["\nrelay_connected_peers ", "\nrelay_registered_tokens "] {
            assert!(
                text.contains(line_prefix),
                "missing {line_prefix:?} in:\n{text}"
            );
        }
        assert!(text.contains("outcome=\"budget_denied\""));
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[test]
    fn push_sent_coalesced_outcome_renders() {
        // Dedicated coverage for the wake-coalescing outcome value (relay push
        // coalescing) — a distinct label value on the same `pushes_sent_total`
        // family already exercised above, not a new metric.
        let mut reg = prometheus_client::registry::Registry::default();
        let m = RelayMetrics::new(&mut reg);
        m.push_sent("apns", "coalesced");
        let text = registry_text(&reg);

        assert!(
            text.contains("\nrelay_pushes_sent_total{"),
            "missing pushes_sent family in:\n{text}"
        );
        assert!(text.contains("outcome=\"coalesced\""));
        assert!(
            !text.contains("_total_total"),
            "doubled _total suffix:\n{text}"
        );
    }

    #[tokio::test]
    async fn endpoint_serves_openmetrics_and_health() {
        let mut reg = prometheus_client::registry::Registry::default();
        let m = RelayMetrics::new(&mut reg);
        m.connection_established(false);
        let reg = std::sync::Arc::new(std::sync::Mutex::new(reg));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // free it; serve_metrics rebinds (fine for a test port)
        let server = tokio::spawn(serve_metrics(addr, reg));
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let body = reqwest::get(format!("http://{addr}/metrics"))
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(body.contains("\nrelay_connections_total{"));
        assert!(
            !body.contains("_total_total"),
            "doubled _total suffix:\n{body}"
        );
        assert!(body.contains("# EOF"), "OpenMetrics text ends with # EOF");
        let health = reqwest::get(format!("http://{addr}/healthz"))
            .await
            .unwrap();
        assert_eq!(health.status(), 200);
        server.abort();
    }
}
