//! E2E test harness for WaveSyncDB. Spawns the real `wavesync-relay` and
//! `test-peer` Docker images, wires them onto a shared Docker network,
//! and exposes a typed Rust API the scenarios use to drive writes /
//! reads / network manipulation.
//!
//! Build the images once:
//!
//! ```bash
//! ./tests-e2e/build-images.sh
//! ```
//!
//! Then run scenarios as normal Rust integration tests:
//!
//! ```bash
//! cargo test -p wavesyncdb-e2e --tests
//! ```

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use bollard::Docker;
use libp2p::identity::Keypair;
use serde::{Deserialize, Serialize};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

pub mod task_entity;

/// Linux `tc netem` parameters applied to a peer container's `eth0`
/// egress to simulate real network conditions during E2E tests.
///
/// Egress shaping is symmetric enough for our purposes: each peer
/// applies the same profile, so traffic between them experiences
/// roughly the same conditions in both directions. This models the
/// common reality where a peer's own uplink is the bottleneck.
///
/// Built-in profiles cover the conditions we actually care about.
/// Custom profiles can be constructed with [`NetemProfile::custom`].
#[derive(Debug, Clone)]
pub struct NetemProfile {
    pub name: &'static str,
    pub latency_ms: u32,
    pub jitter_ms: u32,
    pub loss_pct: f32,
    /// Bandwidth cap in kilobits per second. `None` for unshaped.
    pub rate_kbit: Option<u32>,
}

impl NetemProfile {
    /// Ideal: zero added latency, no loss, no cap. Baseline for
    /// "what would convergence look like with no network at all".
    /// Useful for measuring engine overhead in isolation.
    pub fn ideal() -> Self {
        Self {
            name: "ideal",
            latency_ms: 0,
            jitter_ms: 0,
            loss_pct: 0.0,
            rate_kbit: None,
        }
    }

    /// Fast LAN — sub-millisecond latency, no loss, no cap. Sanity
    /// baseline so a netem run can be compared against the unshaped
    /// case without rewiring the harness.
    pub fn lan_fast() -> Self {
        Self {
            name: "lan_fast",
            latency_ms: 1,
            jitter_ms: 0,
            loss_pct: 0.0,
            rate_kbit: None,
        }
    }

    /// Wired gigabit Ethernet: 1ms latency, no jitter, 1 Gbps cap.
    /// Approximates a quiet datacenter link.
    pub fn ethernet_gigabit() -> Self {
        Self {
            name: "ethernet_gigabit",
            latency_ms: 1,
            jitter_ms: 0,
            loss_pct: 0.0,
            rate_kbit: Some(1_000_000),
        }
    }

    /// Home WiFi (typical 5 GHz, close to AP, light contention):
    /// 5ms latency, 1ms jitter, 50 Mbps cap. The conditions most
    /// "this app is on my home network" interactions actually face.
    pub fn wifi_home() -> Self {
        Self {
            name: "wifi_home",
            latency_ms: 5,
            jitter_ms: 1,
            loss_pct: 0.0,
            rate_kbit: Some(50_000),
        }
    }

    /// Busy / contended WiFi (open office, coffee shop): 30ms RTT,
    /// 10ms jitter, 0.1% loss, 10 Mbps. Models the "WiFi works but
    /// you can feel it" experience that triggers most "is the app
    /// broken?" support tickets.
    pub fn wifi_busy() -> Self {
        Self {
            name: "wifi_busy",
            latency_ms: 15,
            jitter_ms: 10,
            loss_pct: 0.1,
            rate_kbit: Some(10_000),
        }
    }

    /// Edge-of-coverage WiFi (far from AP, walls in the way):
    /// 80ms latency, 30ms jitter, 1% loss, 2 Mbps. The hand-off
    /// boundary where users typically blame the WiFi for the
    /// app's responsiveness.
    pub fn wifi_distant() -> Self {
        Self {
            name: "wifi_distant",
            latency_ms: 80,
            jitter_ms: 30,
            loss_pct: 1.0,
            rate_kbit: Some(2_000),
        }
    }

    /// Modern 5G (mid-band, good signal): 30ms RTT, 5ms jitter,
    /// 0.1% loss, 100 Mbps. Increasingly the default for mobile
    /// in 2026.
    pub fn mobile_5g() -> Self {
        Self {
            name: "mobile_5g",
            latency_ms: 15,
            jitter_ms: 5,
            loss_pct: 0.1,
            rate_kbit: Some(100_000),
        }
    }

    /// Legacy 3G / EDGE-grade mobile: 200ms RTT, 30ms jitter, 1%
    /// loss, 384 kbps. Still the only choice in many regions; an
    /// engine that "works" on cellular_fair but melts on 3G is
    /// rejecting users we care about.
    pub fn mobile_3g() -> Self {
        Self {
            name: "mobile_3g",
            latency_ms: 100,
            jitter_ms: 30,
            loss_pct: 1.0,
            rate_kbit: Some(384),
        }
    }

    /// Cellular fair (typical 4G urban): 80ms RTT (40ms each way),
    /// 20ms jitter, 0.5% loss, 5 Mbps cap. The "happy path" for a
    /// phone with reception.
    pub fn cellular_fair() -> Self {
        Self {
            name: "cellular_fair",
            latency_ms: 40,
            jitter_ms: 20,
            loss_pct: 0.5,
            rate_kbit: Some(5_000),
        }
    }

    /// Cellular bad (edge / congested 4G / weak 3G): 400ms RTT,
    /// 100ms jitter, 3% loss, 1 Mbps cap. Where most apps that
    /// "feel slow" actually live.
    pub fn cellular_bad() -> Self {
        Self {
            name: "cellular_bad",
            latency_ms: 200,
            jitter_ms: 100,
            loss_pct: 3.0,
            rate_kbit: Some(1_000),
        }
    }

    /// Geostationary satellite: 1.2s RTT (600ms each way), 50ms
    /// jitter, 0.5% loss, 1 Mbps cap. Stress-tests our retry /
    /// timeout choices on the libp2p side.
    pub fn satellite() -> Self {
        Self {
            name: "satellite",
            latency_ms: 600,
            jitter_ms: 50,
            loss_pct: 0.5,
            rate_kbit: Some(1_000),
        }
    }

    /// Lossy LAN: low latency but 5% packet loss. Isolates the
    /// effect of loss on convergence without RTT confounds.
    pub fn lossy_lan() -> Self {
        Self {
            name: "lossy_lan",
            latency_ms: 1,
            jitter_ms: 0,
            loss_pct: 5.0,
            rate_kbit: None,
        }
    }

    pub fn custom(
        name: &'static str,
        latency_ms: u32,
        jitter_ms: u32,
        loss_pct: f32,
        rate_kbit: Option<u32>,
    ) -> Self {
        Self {
            name,
            latency_ms,
            jitter_ms,
            loss_pct,
            rate_kbit,
        }
    }

    /// Build the `tc qdisc add ...` command line that realises this
    /// profile. Caller runs it inside the container via `docker exec`.
    fn tc_args(&self, action: &str) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "tc".into(),
            "qdisc".into(),
            action.into(),
            "dev".into(),
            "eth0".into(),
            "root".into(),
            "netem".into(),
        ];
        if self.latency_ms > 0 {
            args.push("delay".into());
            args.push(format!("{}ms", self.latency_ms));
            if self.jitter_ms > 0 {
                args.push(format!("{}ms", self.jitter_ms));
                // Slight pareto distribution feels more like real
                // network jitter than uniform.
                args.push("distribution".into());
                args.push("normal".into());
            }
        }
        if self.loss_pct > 0.0 {
            args.push("loss".into());
            args.push(format!("{}%", self.loss_pct));
        }
        if let Some(rate) = self.rate_kbit {
            args.push("rate".into());
            args.push(format!("{}kbit", rate));
        }
        args
    }
}

/// Image tag for the bundled `wavesync-relay`. Build with
/// `tests-e2e/build-images.sh`.
const RELAY_IMAGE: &str = "wavesync-relay:e2e";
/// Image tag for the test-peer (HTTP-wrapped `WaveSyncDb`). Build with
/// `tests-e2e/build-images.sh`.
const PEER_IMAGE: &str = "wavesync-test-peer:e2e";

/// libp2p QUIC port the relay listens on inside its container.
const RELAY_QUIC_PORT: u16 = 4001;
/// Prometheus metrics port the relay exposes inside its container.
const RELAY_METRICS_PORT: u16 = 9464;
/// HTTP port the test-peer listens on inside its container.
const PEER_HTTP_PORT: u16 = 8080;

/// NAT-topology shape applied to a peer's container via `iptables`.
///
/// Real NAT shapes the engine has to deal with in the wild — the
/// Docker bridge by itself has no NAT, so peers can dial each other's
/// bridge IPs directly via libp2p `identify` and never exercise the
/// circuit-relay → direct upgrade path. Applying a NAT profile blocks
/// the easy path, forcing libp2p to use the relay first and giving
/// DCUtR something to upgrade.
///
/// The simulation is intentionally minimal — we don't need exact
/// behavioural parity with every router on Earth, only enough to
/// exercise the engine's NAT-traversal code paths.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NatProfile {
    /// No NAT (default). Peers connect directly via the Docker bridge.
    #[default]
    Open,

    /// Port-restricted cone NAT — the most permissive non-trivial
    /// shape. Inbound packets are accepted only on flows for which an
    /// outbound packet has previously been observed (conntrack-based).
    /// libp2p hole-punching via DCUtR succeeds on this shape because
    /// both peers initiate near-simultaneously, each side's conntrack
    /// then accepts the inbound. Implemented as:
    ///
    /// ```text
    /// iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    /// iptables -A INPUT -i lo -j ACCEPT
    /// iptables -A INPUT -p tcp -j ACCEPT       # leave HTTP forward intact
    /// iptables -A INPUT -p udp -j DROP         # block unsolicited UDP
    /// ```
    PortRestrictedCone,

    /// Port-restricted cone NAT, **but with the AutoNAT server's IP
    /// whitelisted** for unsolicited inbound. Models the realistic
    /// case where a peer trusts a known-good AutoNAT/relay server to
    /// dial it back for reachability verification, while still
    /// blocking arbitrary peer-to-peer unsolicited inbound.
    ///
    /// Without this exception the previous shape is too strict for
    /// libp2p's NAT-traversal flow: AutoNAT v2 dial-backs are
    /// themselves unsolicited inbound and get dropped, the engine
    /// then downgrades to private and stops advertising direct
    /// addresses, and DCUtR loses any direct address to attempt the
    /// hole-punch toward.
    ///
    /// The harness fills the whitelist IP automatically with the
    /// relay container's bridge address (the relay also acts as the
    /// AutoNAT server in our setup).
    ///
    /// ```text
    /// iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    /// iptables -A INPUT -i lo -j ACCEPT
    /// iptables -A INPUT -p tcp -j ACCEPT
    /// iptables -A INPUT -s <autonat-server-ip> -j ACCEPT  # whitelist
    /// iptables -A INPUT -p udp -j DROP
    /// ```
    PortRestrictedConeAutoNATOk,

    /// Symmetric NAT (#51) — the shape that defeats simultaneous-dial
    /// hole-punching, making DCUtR coordination the only direct path.
    ///
    /// The cone shapes above only *filter* inbound; the peer's real
    /// listen port is what every destination observes, so when the relay
    /// introduces two peers and both dial at once, each side's conntrack
    /// entry accepts the other's inbound and a direct connection forms
    /// with `dcutr_upgrades_attempted = 0`. A symmetric NAT instead
    /// assigns a **different, unpredictable source port per flow**: the
    /// port a peer advertises (its listen port) never matches the port
    /// any destination actually observes, so a dial toward the
    /// advertised port matches no conntrack entry and is dropped.
    ///
    /// Simulated with per-flow source-port randomization on outbound
    /// UDP, on top of the AutoNAT-whitelisted cone filter (the
    /// whitelist keeps the engine advertising direct addresses, so
    /// DCUtR has something to attempt — without it the engine downgrades
    /// to private and never tries):
    ///
    /// ```text
    /// <PortRestrictedConeAutoNATOk INPUT rules>
    /// iptables -t nat -A POSTROUTING -p udp -j MASQUERADE --random-fully
    /// ```
    ///
    /// True symmetric NAT defeats even coordinated hole-punching by
    /// design; the shape's guarantee is that no direct QUIC handshake can
    /// complete (wire-verified: dials to observed ports retransmit
    /// unanswered) and sync converges via relay circuits — which is also
    /// the shape's role as M1's worst-case relay-payload-ratio baseline.
    /// Note: today the ENGINE never even attempts DCUtR here (AutoNAT
    /// dial-backs target the masqueraded observed port and fail, so no
    /// punch candidates exist) — an M1-tracked engine gap; when fixed,
    /// `dcutr_validation_symmetric_nat` should assert engagement.
    SymmetricNat,
}

impl NatProfile {
    /// Build the iptables command sequence applying this profile.
    /// `autonat_server_ip` is whitelisted for unsolicited inbound when
    /// the variant requires it
    /// ([`NatProfile::PortRestrictedConeAutoNATOk`]); ignored for
    /// variants that don't need an AutoNAT exception.
    /// Returns an empty list for [`NatProfile::Open`].
    fn iptables_argvs(&self, autonat_server_ip: Option<&str>) -> Vec<Vec<String>> {
        let conntrack_accept = argv(&[
            "iptables",
            "-A",
            "INPUT",
            "-m",
            "conntrack",
            "--ctstate",
            "ESTABLISHED,RELATED",
            "-j",
            "ACCEPT",
        ]);
        let lo_accept = argv(&["iptables", "-A", "INPUT", "-i", "lo", "-j", "ACCEPT"]);
        let tcp_accept = argv(&["iptables", "-A", "INPUT", "-p", "tcp", "-j", "ACCEPT"]);
        let udp_drop = argv(&["iptables", "-A", "INPUT", "-p", "udp", "-j", "DROP"]);
        match self {
            NatProfile::Open => Vec::new(),
            NatProfile::PortRestrictedCone => {
                vec![conntrack_accept, lo_accept, tcp_accept, udp_drop]
            }
            NatProfile::PortRestrictedConeAutoNATOk => {
                let mut rules = vec![conntrack_accept, lo_accept, tcp_accept];
                if let Some(ip) = autonat_server_ip {
                    // Whitelist before the catch-all UDP DROP so this
                    // rule wins for the autonat server's verification
                    // dials. Without an IP supplied, this variant
                    // degrades to plain PortRestrictedCone behaviour.
                    rules.push(argv(&["iptables", "-A", "INPUT", "-s", ip, "-j", "ACCEPT"]));
                }
                rules.push(udp_drop);
                rules
            }
            NatProfile::SymmetricNat => {
                let mut rules = vec![conntrack_accept, lo_accept, tcp_accept];
                if let Some(ip) = autonat_server_ip {
                    rules.push(argv(&["iptables", "-A", "INPUT", "-s", ip, "-j", "ACCEPT"]));
                }
                rules.push(udp_drop);
                // The symmetric half: every outbound UDP flow leaves with
                // a fully randomized source port, so the listen port a
                // peer advertises is never the port any destination
                // observes. UDP only — the harness's HTTP control API
                // (TCP) must keep its real ports.
                rules.push(argv(&[
                    "iptables",
                    "-t",
                    "nat",
                    "-A",
                    "POSTROUTING",
                    "-p",
                    "udp",
                    "-j",
                    "MASQUERADE",
                    "--random-fully",
                ]));
                rules
            }
        }
    }

    /// Short name used in benchmark logs.
    pub fn name(&self) -> &'static str {
        match self {
            NatProfile::Open => "open",
            NatProfile::PortRestrictedCone => "port_restricted_cone",
            NatProfile::PortRestrictedConeAutoNATOk => "port_restricted_cone_autonat_ok",
            NatProfile::SymmetricNat => "symmetric_nat",
        }
    }

    /// Whether the harness needs to look up the AutoNAT-server IP
    /// before applying this profile. Used to decide if the relay's
    /// bridge address must be resolved before starting peers.
    fn needs_autonat_whitelist(&self) -> bool {
        matches!(
            self,
            NatProfile::PortRestrictedConeAutoNATOk | NatProfile::SymmetricNat
        )
    }
}

fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

/// Builder for an end-to-end harness.
#[derive(Default)]
pub struct WaveSyncE2eHarness {
    peers: Vec<PeerSpec>,
    topic: String,
    passphrase: Option<String>,
    /// Default netem profile applied to every peer at startup. Per-peer
    /// overrides set via [`Self::add_peer_with_netem`] beat this default.
    /// `None` ⇒ no shaping anywhere (today's behaviour).
    default_netem: Option<NetemProfile>,
    /// Default NAT profile applied to every peer. Per-peer overrides
    /// set via [`Self::add_peer_with_nat`] beat this default.
    default_nat: NatProfile,
    /// When `false`, peers are started with mDNS disabled so the only
    /// discovery path is the relay/rendezvous server. Used to reproduce
    /// WAN-only bugs that LAN mDNS would otherwise mask.
    mdns_enabled: bool,
    /// Optional secondary (non-default) group every peer joins at runtime
    /// in addition to the default group: `(topic, passphrase)`. Drives the
    /// peer's `/g2/...` routes. `None` ⇒ single-group (today's behaviour).
    secondary_group: Option<(String, String)>,
    /// Extra environment variables for the relay container, applied on top
    /// of the fixed set (identity, external address, metrics). Used to
    /// enable/configure optional relay subsystems per scenario (e.g.
    /// `MAILBOX_DB` + a short `MAILBOX_TTL_SECS` for the TTL-fallback test).
    relay_env: Vec<(String, String)>,
    /// Extra environment variables applied to EVERY peer container, on top
    /// of the fixed set. The peer analogue of `relay_env` (e.g.
    /// `MAILBOX_APPEND_AFTER_SECS` for the #107 dial scenarios).
    peer_env: Vec<(String, String)>,
}

/// Configuration for a single peer in the harness.
pub struct PeerSpec {
    pub name: String,
    pub passphrase: Option<String>,
    /// Per-peer netem override. `None` ⇒ use the harness default.
    pub netem: Option<NetemProfile>,
    /// Per-peer NAT override. `None` ⇒ use the harness default.
    pub nat: Option<NatProfile>,
}

impl WaveSyncE2eHarness {
    /// Begin a new harness with a unique topic. Every harness instance
    /// gets a UUID-suffixed topic so parallel test runs never see each
    /// other's traffic, even if Docker network isolation leaks.
    pub fn new() -> Self {
        Self {
            peers: Vec::new(),
            topic: format!("e2e-{}", Uuid::new_v4().simple()),
            passphrase: None,
            default_netem: None,
            default_nat: NatProfile::Open,
            mdns_enabled: true,
            secondary_group: None,
            relay_env: Vec::new(),
            peer_env: Vec::new(),
        }
    }

    /// Set a passphrase shared by every peer the harness creates. When
    /// unset, peers run unauthenticated — useful for testing the
    /// HMAC-rejection path.
    pub fn with_passphrase(mut self, p: impl Into<String>) -> Self {
        self.passphrase = Some(p.into());
        self
    }

    /// Add a peer to the harness. The name is used as the peer's
    /// container hostname inside the Docker network and shows up in
    /// scenario assertion failures.
    pub fn add_peer(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let passphrase = self.passphrase.clone();
        self.peers.push(PeerSpec {
            name,
            passphrase,
            netem: None,
            nat: None,
        });
        self
    }

    /// Add a peer with an explicit netem profile that overrides the
    /// harness default. Use to model asymmetric scenarios (e.g. one
    /// peer on cellular, one on LAN).
    pub fn add_peer_with_netem(mut self, name: impl Into<String>, profile: NetemProfile) -> Self {
        let name = name.into();
        let passphrase = self.passphrase.clone();
        self.peers.push(PeerSpec {
            name,
            passphrase,
            netem: Some(profile),
            nat: None,
        });
        self
    }

    /// Add a peer with an explicit NAT profile that overrides the
    /// harness default. Use to model asymmetric scenarios (e.g. one
    /// peer behind a phone-style NAT, one on a public IP).
    pub fn add_peer_with_nat(mut self, name: impl Into<String>, nat: NatProfile) -> Self {
        let name = name.into();
        let passphrase = self.passphrase.clone();
        self.peers.push(PeerSpec {
            name,
            passphrase,
            netem: None,
            nat: Some(nat),
        });
        self
    }

    /// Apply this netem profile to every peer (unless a per-peer
    /// override beats it). Container `eth0` egress only — see
    /// [`NetemProfile`] docs for what that does and doesn't model.
    pub fn with_netem(mut self, profile: NetemProfile) -> Self {
        self.default_netem = Some(profile);
        self
    }

    /// Apply this NAT profile to every peer (unless a per-peer
    /// override beats it). Implemented via `iptables` inside each peer
    /// container — see [`NatProfile`] docs.
    pub fn with_nat(mut self, nat: NatProfile) -> Self {
        self.default_nat = nat;
        self
    }

    /// Disable mDNS on every peer so the relay/rendezvous server is the
    /// only discovery path. Use to reproduce WAN-only behaviour on the
    /// Docker bridge (where mDNS multicast would otherwise let peers find
    /// each other directly and mask relay-path bugs).
    pub fn without_mdns(mut self) -> Self {
        self.mdns_enabled = false;
        self
    }

    /// Set an extra environment variable on the relay container (see the
    /// `relay_env` field). Later calls with the same key append — the
    /// container runtime keeps the last value.
    pub fn with_relay_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.relay_env.push((key.into(), value.into()));
        self
    }

    /// Set an extra environment variable on every peer container (see the
    /// `peer_env` field). Later calls with the same key append — the
    /// container runtime keeps the last value.
    pub fn with_peer_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.peer_env.push((key.into(), value.into()));
        self
    }

    /// Have every peer join a secondary (non-default) group in addition
    /// to the default one. The group is reachable through the peer's
    /// `/g2/...` routes (`insert_task_g2`, `wait_for_task_g2`, …). Models
    /// a consumer app that joins a shared group (e.g. a household) at
    /// runtime on top of each member's personal/default group.
    pub fn with_secondary_group(
        mut self,
        topic: impl Into<String>,
        passphrase: impl Into<String>,
    ) -> Self {
        self.secondary_group = Some((topic.into(), passphrase.into()));
        self
    }

    /// Bring up the network: relay → peers → wait for engines to
    /// announce ready. Returns a `RunningHarness` with handles for
    /// container lifecycle and HTTP clients.
    pub async fn start(self) -> Result<RunningHarness> {
        if self.peers.is_empty() {
            bail!("harness needs at least one peer");
        }

        // Suffix every container name + the network with a per-harness
        // UUID so concurrent cargo-test invocations never collide.
        let suffix = Uuid::new_v4().simple().to_string();

        // 1. Generate the relay's libp2p identity up-front so every peer
        //    can dial `/dns4/<relay-host>/udp/4001/quic-v1/p2p/<peer-id>`
        //    without a runtime discovery step.
        let relay_keypair = Keypair::generate_ed25519();
        let relay_peer_id = relay_keypair.public().to_peer_id();
        let relay_identity_b64 = B64.encode(
            relay_keypair
                .to_protobuf_encoding()
                .context("encode keypair")?,
        );

        // 2. Per-harness Docker network.
        let net_name = format!("wavesync-e2e-{suffix}");

        // 3. Start the relay. The peers dial it by container hostname,
        //    which Docker's embedded DNS resolves on the bridge network.
        //    Use a "relay-<suffix>" hostname so concurrent harnesses
        //    don't fight over the name.
        let relay_host = format!("relay-{suffix}");
        let relay_external = format!("/dns4/{relay_host}/udp/{}/quic-v1", RELAY_QUIC_PORT);
        let mut relay_img = GenericImage::new(
            RELAY_IMAGE.split(':').next().unwrap(),
            RELAY_IMAGE.split(':').nth(1).unwrap(),
        )
        .with_exposed_port(RELAY_QUIC_PORT.udp())
        .with_exposed_port(RELAY_METRICS_PORT.tcp())
        // The relay logs via tracing_subscriber::fmt(), which writes to
        // STDOUT (the pre-tracing env_logger wrote to stderr — waiting
        // on the wrong stream times out container startup at 60s).
        .with_wait_for(WaitFor::message_on_stdout("Listening on"))
        .with_env_var("IDENTITY_KEYPAIR", &relay_identity_b64)
        .with_env_var("EXTERNAL_ADDRESS", &relay_external)
        .with_env_var("RUST_LOG", "info")
        .with_env_var("METRICS_ADDR", "0.0.0.0:9464");
        for (key, value) in &self.relay_env {
            relay_img = relay_img.with_env_var(key.clone(), value.clone());
        }
        let relay = relay_img
            .with_network(&net_name)
            .with_container_name(&relay_host)
            .start()
            .await
            .context("start relay container")?;

        let relay_addr = format!(
            "/dns4/{relay_host}/udp/{}/quic-v1/p2p/{}",
            RELAY_QUIC_PORT, relay_peer_id
        );

        // 3b. If any peer's effective NAT profile needs to whitelist
        //     the AutoNAT server, resolve the relay's bridge IP now.
        //     The relay also acts as the AutoNAT server in our setup
        //     (via libp2p autonat::v2::server, transitively).
        let any_needs_autonat_whitelist = self
            .peers
            .iter()
            .map(|s| s.nat.unwrap_or(self.default_nat))
            .any(|n| n.needs_autonat_whitelist());
        let autonat_server_ip = if any_needs_autonat_whitelist {
            let ip = relay
                .get_bridge_ip_address()
                .await
                .context("resolve relay bridge IP for AutoNAT whitelist")?
                .to_string();
            eprintln!("[harness] PortRestrictedConeAutoNATOk: whitelisting relay bridge IP {ip}");
            Some(ip)
        } else {
            None
        };

        // 4. Start each peer with a suffixed container name so
        //    `harness.peer("alice")` still works (we use `spec.name` as
        //    the lookup key, while the container name carries the
        //    harness suffix).
        let mut peers = Vec::with_capacity(self.peers.len());
        for spec in self.peers {
            let container_name = format!("{}-{suffix}", spec.name);
            let effective_netem = spec.netem.clone().or_else(|| self.default_netem.clone());
            let effective_nat = spec.nat.unwrap_or(self.default_nat);

            let mut img = GenericImage::new(
                PEER_IMAGE.split(':').next().unwrap(),
                PEER_IMAGE.split(':').nth(1).unwrap(),
            )
            .with_exposed_port(PEER_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("test-peer ready"))
            .with_env_var("BIND_ADDR", format!("0.0.0.0:{}", PEER_HTTP_PORT))
            .with_env_var("DB_URL", "sqlite:/data/peer.db?mode=rwc")
            .with_env_var("TOPIC", &self.topic)
            .with_env_var("RELAY_ADDR", &relay_addr)
            .with_env_var(
                "RUST_LOG",
                "info,wavesyncdb::engine::mailbox_manager=debug,libp2p_swarm=warn",
            )
            .with_network(&net_name)
            .with_container_name(&container_name);

            if let Some(ref p) = spec.passphrase {
                img = img.with_env_var("PASSPHRASE", p.clone());
            }

            for (key, value) in &self.peer_env {
                img = img.with_env_var(key.clone(), value.clone());
            }

            if !self.mdns_enabled {
                img = img.with_env_var("MDNS_ENABLED", "false");
            }

            if let Some((ref topic, ref pass)) = self.secondary_group {
                img = img
                    .with_env_var("SECONDARY_TOPIC", topic.clone())
                    .with_env_var("SECONDARY_PASSPHRASE", pass.clone());
            }

            // `tc qdisc add ... netem` and `iptables -A INPUT ...` both
            // require NET_ADMIN. Add the capability whenever EITHER
            // shaping or NAT is configured so the unshaped+open default
            // keeps the same security posture as before.
            let needs_netadmin = effective_netem.is_some() || effective_nat != NatProfile::Open;
            if needs_netadmin {
                img = img.with_cap_add("NET_ADMIN");
            }

            // NAT rules ride an env var and are applied by the container
            // ENTRYPOINT before the engine binary starts. Exec'ing them
            // after start() raced the engine's first dials: any flow
            // established rule-free (mDNS discovery on the shared bridge
            // is instant) was grandfathered forever by the conntrack
            // ESTABLISHED accept, silently voiding the NAT shape. The
            // entrypoint also re-applies rules on container restart,
            // which the post-start exec never did.
            if effective_nat != NatProfile::Open {
                let rules = effective_nat
                    .iptables_argvs(autonat_server_ip.as_deref())
                    .into_iter()
                    .map(|argv| argv[1..].join(" "))
                    .collect::<Vec<_>>()
                    .join("\n");
                img = img.with_env_var("IPTABLES_RULES", rules);
            }

            let container = img
                .start()
                .await
                .with_context(|| format!("start peer container {}", spec.name))?;

            // Apply the netem rule once the container is up. We do this
            // *after* `start()` returns so the engine has already bound
            // its sockets — kernel-level shaping then applies to all
            // subsequent traffic on `eth0`.
            if let Some(ref profile) = effective_netem {
                apply_netem_to_container(&container, profile)
                    .await
                    .with_context(|| {
                        format!("apply netem profile {} to {}", profile.name, spec.name)
                    })?;
            }

            let host_port = container
                .get_host_port_ipv4(PEER_HTTP_PORT)
                .await
                .context("resolve peer host port")?;
            let base_url = format!("http://127.0.0.1:{host_port}");

            peers.push(RunningPeer {
                name: spec.name,
                base_url,
                container,
                netem: effective_netem,
                nat: effective_nat,
            });
        }

        Ok(RunningHarness {
            relay,
            peers,
            net_name,
            topic: self.topic,
        })
    }
}

/// Live harness instance. Drops tear down all containers (testcontainers
/// owns the lifecycle).
pub struct RunningHarness {
    pub relay: ContainerAsync<GenericImage>,
    peers: Vec<RunningPeer>,
    pub net_name: String,
    pub topic: String,
}

pub struct RunningPeer {
    pub name: String,
    pub base_url: String,
    pub container: ContainerAsync<GenericImage>,
    /// Netem profile currently applied to this peer's `eth0` egress,
    /// or `None` if unshaped. Use [`Self::set_netem`] /
    /// [`Self::clear_netem`] to change it mid-test.
    pub netem: Option<NetemProfile>,
    /// NAT profile currently applied to this peer's INPUT chain.
    /// Set at startup; today there's no API to change mid-test (would
    /// require careful conntrack-flush handling — add when needed).
    pub nat: NatProfile,
}

impl RunningPeer {
    /// Refresh `base_url` by re-querying the Docker daemon for the
    /// current host port. Call after a `docker stop`/`docker start`
    /// cycle on the underlying container — the host port can change.
    pub async fn refresh_base_url(&mut self) -> Result<()> {
        let host_port = self
            .container
            .get_host_port_ipv4(PEER_HTTP_PORT)
            .await
            .context("re-resolve peer host port")?;
        self.base_url = format!("http://127.0.0.1:{host_port}");
        Ok(())
    }

    /// Replace this peer's netem profile mid-test. Used by partition /
    /// degradation scenarios that simulate "the user walked into a
    /// tunnel" or "their connection recovered". Errors if the
    /// container was started without `NET_ADMIN` (i.e. without a
    /// netem profile in the harness builder) — `tc` will refuse the
    /// `qdisc` change.
    pub async fn set_netem(&mut self, profile: NetemProfile) -> Result<()> {
        // `tc qdisc change` updates an existing root qdisc in place.
        // If there's no root qdisc yet (peer started unshaped), we add
        // it instead.
        let action = if self.netem.is_some() {
            "change"
        } else {
            "add"
        };
        run_tc(&self.container, &profile.tc_args(action))
            .await
            .with_context(|| format!("set netem {}", profile.name))?;
        self.netem = Some(profile);
        Ok(())
    }

    /// Drop netem shaping. Subsequent traffic flows unshaped.
    pub async fn clear_netem(&mut self) -> Result<()> {
        if self.netem.is_none() {
            return Ok(());
        }
        let args = vec![
            "tc".into(),
            "qdisc".into(),
            "del".into(),
            "dev".into(),
            "eth0".into(),
            "root".into(),
        ];
        run_tc(&self.container, &args)
            .await
            .context("clear netem")?;
        self.netem = None;
        Ok(())
    }
}

/// Connect to the local Docker daemon. All lifecycle helpers below go
/// through bollard directly because testcontainers deliberately doesn't
/// expose stop/start/pause on a running container.
fn docker() -> Result<Docker> {
    Docker::connect_with_local_defaults().context("connect to local Docker daemon")
}

impl RunningPeer {
    /// Stop this peer's container (SIGTERM + wait). The `/data` volume
    /// survives, so the peer's SQLite — including `_wavesync_peer_addrs`
    /// — is intact on the next `start()`.
    pub async fn stop(&self) -> Result<()> {
        docker()?
            .stop_container(self.container.id(), None)
            .await
            .with_context(|| format!("stop container for peer {}", self.name))
    }

    /// Start a previously stopped container. The host port mapping is
    /// reassigned — callers must `refresh_base_url()` before HTTP calls
    /// (or use `RunningHarness::restart_peer_and_wait` which does both).
    pub async fn start(&self) -> Result<()> {
        docker()?
            .start_container(self.container.id(), None)
            .await
            .with_context(|| format!("start container for peer {}", self.name))
    }

    /// Freeze every process in the container (cgroup freezer). Sockets
    /// stay open but nothing is scheduled — the closest Docker analogue
    /// to a mobile OS freezing a backgrounded app. Remote idle timeouts
    /// kill the frozen peer's connections without it noticing.
    pub async fn pause(&self) -> Result<()> {
        docker()?
            .pause_container(self.container.id())
            .await
            .with_context(|| format!("pause container for peer {}", self.name))
    }

    /// Unfreeze a paused container — the "app returned to foreground"
    /// moment. The engine resumes with whatever stale state it had.
    pub async fn unpause(&self) -> Result<()> {
        docker()?
            .unpause_container(self.container.id())
            .await
            .with_context(|| format!("unpause container for peer {}", self.name))
    }

    /// Block until `GET /health` on this peer returns 2xx, or time out.
    pub async fn wait_http_ready(&self, timeout: Duration) -> Result<()> {
        let url = format!("{}/health", self.base_url);
        let start = Instant::now();
        let client = http_client();
        loop {
            if let Ok(r) = client.get(&url).send().await
                && r.status().is_success()
            {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                bail!("peer {} HTTP not ready within {:?}", self.name, timeout);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// `wait_for_task`, but returns how long visibility took. The
    /// measurement primitive for the WAN-latency scenarios.
    pub async fn wait_for_task_timed(
        &self,
        id: &str,
        title: &str,
        timeout: Duration,
    ) -> Result<Duration> {
        let start = Instant::now();
        self.wait_for_task_at("tasks", id, title, timeout).await?;
        Ok(start.elapsed())
    }
}

impl RunningHarness {
    /// stop → start → refresh host port → wait for HTTP. The standard
    /// "cold restart" building block for latency scenarios.
    pub async fn restart_peer_and_wait(&mut self, name: &str, timeout: Duration) -> Result<()> {
        self.peer(name).stop().await?;
        self.peer(name).start().await?;
        let peer = self.peer_mut(name);
        peer.refresh_base_url().await?;
        peer.wait_http_ready(timeout).await
    }

    /// Stop the relay container. Peers lose their reservation and all
    /// relay-path discovery until `start_relay`.
    pub async fn stop_relay(&self) -> Result<()> {
        docker()?
            .stop_container(self.relay.id(), None)
            .await
            .context("stop relay container")
    }

    /// Start the relay back up. Identity is pinned via IDENTITY_KEYPAIR,
    /// so the PeerId — and therefore every peer's configured relay
    /// multiaddr — is still valid.
    pub async fn start_relay(&self) -> Result<()> {
        docker()?
            .start_container(self.relay.id(), None)
            .await
            .context("start relay container")
    }
}

/// Emit one machine-greppable measurement line. Grep test output for
/// `[ttfs]` to collect the numbers that set fix thresholds.
pub fn report_ttfs(scenario: &str, phase: &str, elapsed: Duration) {
    println!(
        "[ttfs] scenario={} phase={} ms={}",
        scenario,
        phase,
        elapsed.as_millis()
    );
}

/// Run a `tc` invocation inside a container and assert exit-code 0.
/// Used by the netem helpers in the harness; not directly part of the
/// scenario API.
async fn run_tc(container: &ContainerAsync<GenericImage>, argv: &[String]) -> Result<()> {
    let cmd =
        ExecCommand::new(argv.iter().cloned()).with_cmd_ready_condition(CmdWaitFor::exit_code(0));
    container
        .exec(cmd)
        .await
        .with_context(|| format!("docker exec {:?}", argv))?;
    Ok(())
}

/// Apply a netem profile by `tc qdisc add`. Used at startup; after
/// startup callers should go through [`RunningPeer::set_netem`] which
/// handles add-vs-change.
async fn apply_netem_to_container(
    container: &ContainerAsync<GenericImage>,
    profile: &NetemProfile,
) -> Result<()> {
    run_tc(container, &profile.tc_args("add")).await
}

// NAT rules are no longer exec'd post-start: they ride the
// IPTABLES_RULES env var and are applied by the container entrypoint
// before the engine binary starts (see docker/peer-entrypoint.sh and
// the rendering site in `start()`).

impl RunningHarness {
    /// Look up a peer client by container name.
    pub fn peer(&self, name: &str) -> &RunningPeer {
        self.peers
            .iter()
            .find(|p| p.name == name)
            .unwrap_or_else(|| panic!("no peer named {name} in harness"))
    }

    /// Mutable variant of [`peer`]. Use for actions that re-bind the
    /// host port (e.g., `RunningPeer::refresh_base_url` after a
    /// container restart).
    pub fn peer_mut(&mut self, name: &str) -> &mut RunningPeer {
        let idx = self
            .peers
            .iter()
            .position(|p| p.name == name)
            .unwrap_or_else(|| panic!("no peer named {name} in harness"));
        &mut self.peers[idx]
    }

    /// Get the relay's Prometheus metrics endpoint URL.
    pub async fn relay_metrics_url(&self) -> Result<String> {
        let host_port = self
            .relay
            .get_host_port_ipv4(RELAY_METRICS_PORT)
            .await
            .context("resolve relay metrics host port")?;
        Ok(format!("http://127.0.0.1:{host_port}"))
    }

    /// Fetch the relay's OpenMetrics text exposition.
    pub async fn relay_metrics_text(&self) -> Result<String> {
        let url = self.relay_metrics_url().await?;
        Ok(reqwest::get(format!("{url}/metrics")).await?.text().await?)
    }

    /// Sum the current value of every sample of a relay metric whose
    /// exposition line starts with `metric_name` and contains `filter`
    /// (pass "" for no label filter).
    pub async fn relay_metric_value(&self, metric_name: &str, filter: &str) -> f64 {
        let text = self.relay_metrics_text().await.unwrap_or_default();
        text.lines()
            .filter(|l| !l.starts_with('#') && l.starts_with(metric_name) && l.contains(filter))
            .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
            .sum()
    }

    /// Poll the relay metrics until the summed samples of `metric_name`
    /// (label-filtered by `filter`, "" = all) reach `min`. Used e.g. to
    /// wait for a mailbox append to be durably acked before freezing the
    /// writer — asserting on the row alone would race the async append.
    pub async fn wait_for_relay_metric(
        &self,
        metric_name: &str,
        filter: &str,
        min: f64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.relay_metric_value(metric_name, filter).await >= min {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!("relay metric {metric_name}{filter} did not reach {min} within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Read files from inside the relay container by shell glob (e.g.
    /// `/data/mailbox.db*` to cover the SQLite main file plus its WAL)
    /// and return the concatenated raw bytes. Missing globs yield empty
    /// output rather than an error — callers should pair a negative scan
    /// with a positive control (e.g. assert the schema string is present)
    /// so an empty read can't silently pass. Used by the "relay stores
    /// only ciphertext" assertion — the relay image has no sqlite3
    /// binary, but a raw byte scan is sufficient: plaintext changeset
    /// JSON would appear verbatim inside the DB pages.
    pub async fn relay_read_files(&self, glob: &str) -> Result<Vec<u8>> {
        let cmd = ExecCommand::new([
            "sh".to_string(),
            "-c".to_string(),
            format!("cat {glob} 2>/dev/null; true"),
        ])
        .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
        let mut result = self
            .relay
            .exec(cmd)
            .await
            .with_context(|| format!("docker exec cat {glob}"))?;
        result
            .stdout_to_vec()
            .await
            .with_context(|| format!("read {glob} from relay container"))
    }
}

/// One shared, pooled HTTP client for every harness→peer call.
///
/// Load-bearing for latency measurements, not a style choice: a fresh
/// `reqwest::Client` per call opens a fresh TCP connection per call, and
/// under a lossy netem profile a dropped SYN retransmits at the kernel's
/// 1s initial RTO — which showed up as a fake ~1.1s steady-state p95
/// "sync tail" (#38; sync itself was at ~59ms with pushes fully acked).
/// A pooled connection's losses recover at RTO_MIN (~200ms) instead, and
/// most polls reuse the warm connection entirely.
fn http_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Per-peer HTTP API matching the routes in `bin/test_peer.rs`.
impl RunningPeer {
    /// Insert a task via SeaORM through this peer's WaveSyncDb.
    pub async fn insert_task(&self, id: &str, title: &str, completed: bool) -> Result<()> {
        let resp = http_client()
            .post(format!("{}/tasks", self.base_url))
            .json(&Task {
                id: id.into(),
                title: title.into(),
                completed,
            })
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("insert_task on {} failed: {}", self.name, resp.status());
        }
        Ok(())
    }

    /// Update a task via SeaORM through this peer's WaveSyncDb.
    pub async fn update_task(&self, id: &str, title: &str, completed: bool) -> Result<()> {
        let resp = http_client()
            .put(format!("{}/tasks/{}", self.base_url, id))
            .json(&Task {
                id: id.into(),
                title: title.into(),
                completed,
            })
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("update_task on {} failed: {}", self.name, resp.status());
        }
        Ok(())
    }

    /// Fetch a single task by primary key, returning `None` if the row
    /// hasn't reached this peer yet.
    pub async fn get_task(&self, id: &str) -> Result<Option<Task>> {
        self.get_task_at("tasks", id).await
    }

    /// `insert_task`, but against the secondary group's `/g2/tasks` route.
    pub async fn insert_task_g2(&self, id: &str, title: &str, completed: bool) -> Result<()> {
        let resp = http_client()
            .post(format!("{}/g2/tasks", self.base_url))
            .json(&Task {
                id: id.into(),
                title: title.into(),
                completed,
            })
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("insert_task_g2 on {} failed: {}", self.name, resp.status());
        }
        Ok(())
    }

    /// `get_task`, but against the secondary group's `/g2/tasks` route.
    pub async fn get_task_g2(&self, id: &str) -> Result<Option<Task>> {
        self.get_task_at("g2/tasks", id).await
    }

    /// Shared GET-by-id used by both `get_task` and `get_task_g2`. `route`
    /// is the collection path segment (`tasks` or `g2/tasks`).
    async fn get_task_at(&self, route: &str, id: &str) -> Result<Option<Task>> {
        let resp = http_client()
            .get(format!("{}/{}/{}", self.base_url, route, id))
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            bail!("get_task on {} failed: {}", self.name, resp.status());
        }
        Ok(Some(resp.json::<Task>().await?))
    }

    /// List every task this peer's local SQLite currently holds.
    pub async fn list_tasks(&self) -> Result<Vec<Task>> {
        Ok(http_client()
            .get(format!("{}/tasks", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<Task>>()
            .await?)
    }

    /// Number of currently-connected libp2p peers (relay + sync peers).
    pub async fn connected_peer_count(&self) -> Result<usize> {
        Ok(http_client()
            .get(format!("{}/peers", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<PeersResponse>()
            .await?
            .connected)
    }

    /// Snapshot of the engine's diagnostics counters at this peer.
    /// Returns the wire-compatible JSON representation; we re-parse it
    /// as a typed value so scenarios can assert on specific counters.
    pub async fn diagnostics(&self) -> Result<DiagnosticsSnapshot> {
        Ok(http_client()
            .get(format!("{}/diagnostics", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<DiagnosticsSnapshot>()
            .await?)
    }

    /// Trigger a simulated not-killed push wake at this peer (see the test
    /// peer's `/push_wake` route): the peer runs the shared background-sync
    /// entry point against its own live database, which must reuse the live
    /// in-process engine. `timeout` is the simulated OS push budget.
    pub async fn push_wake(&self, timeout: Duration) -> Result<PushWakeOutcome> {
        Ok(http_client()
            .post(format!("{}/push_wake", self.base_url))
            // Generous HTTP timeout: the endpoint blocks for up to `timeout`.
            .timeout(timeout + Duration::from_secs(15))
            .json(&serde_json::json!({ "timeout_secs": timeout.as_secs() }))
            .send()
            .await?
            .error_for_status()?
            .json::<PushWakeOutcome>()
            .await?)
    }

    /// Block until a task with the given pk is visible at this peer, or
    /// time out. Used by scenarios to assert convergence.
    ///
    /// Transient HTTP errors (connection refused, 5xx) are treated as
    /// "row not yet available" and retried — they happen routinely
    /// during scenarios that bring containers up and down, where the
    /// port forward briefly disappears between `stop` and `start`.
    /// Only a timeout is fatal.
    pub async fn wait_for_task(&self, id: &str, title: &str, timeout: Duration) -> Result<()> {
        self.wait_for_task_at("tasks", id, title, timeout).await
    }

    /// `wait_for_task`, but polling the secondary group's `/g2/tasks`
    /// route. This is the assertion that reproduces the multi-group
    /// rendezvous-discovery bug: with mDNS disabled, a secondary-group
    /// write only reaches the other peer if relay-path discovery returns
    /// the secondary namespace's registrations.
    pub async fn wait_for_task_g2(&self, id: &str, title: &str, timeout: Duration) -> Result<()> {
        self.wait_for_task_at("g2/tasks", id, title, timeout).await
    }

    async fn wait_for_task_at(
        &self,
        route: &str,
        id: &str,
        title: &str,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut interval = Duration::from_millis(50);
        loop {
            // Transient HTTP errors must NOT short-circuit the wait
            // — that's how container-restart scenarios were silently
            // masking convergence as failure. Treat any error as
            // "not yet" and retry until the actual timeout.
            match self.get_task_at(route, id).await {
                Ok(Some(t)) if t.title == title => return Ok(()),
                _ => {}
            }
            if start.elapsed() >= timeout {
                bail!(
                    "peer {} did not see task {{id={}, title={}}} on /{} within {:?}",
                    self.name,
                    id,
                    title,
                    route,
                    timeout
                );
            }
            tokio::time::sleep(interval).await;
            interval = (interval * 2).min(Duration::from_millis(500));
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Task {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Debug, Deserialize)]
struct PeersResponse {
    connected: usize,
}

/// Outcome of a simulated push wake (`POST /push_wake`). `result` mirrors
/// `wavesyncdb::background_sync::BackgroundSyncResult` as a lowercase tag:
/// `"synced"`, `"no_peers"`, or `"timed_out"`.
#[derive(Debug, Clone, Deserialize)]
pub struct PushWakeOutcome {
    pub result: String,
    pub peers_synced: usize,
    pub elapsed_ms: u64,
}

/// Diagnostics counter snapshot returned by `GET /diagnostics`. Field
/// names mirror `wavesyncdb::diagnostics::Snapshot` exactly (22 fields,
/// kept in lockstep with the source struct) so the JSON returned by the
/// test-peer is wire-compatible. We don't import `wavesyncdb` here
/// directly — keeping the harness type-only so the e2e crate doesn't
/// pull the full engine surface. Every field is `#[serde(default)]` so a
/// snapshot from an older/newer test-peer binary still deserializes.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct DiagnosticsSnapshot {
    #[serde(default)]
    pub circuit_reservation_attempts: u64,
    #[serde(default)]
    pub circuit_reservations_accepted: u64,
    #[serde(default)]
    pub peer_dial_attempts: u64,
    #[serde(default)]
    pub peer_dial_successes: u64,
    #[serde(default)]
    pub peer_dial_failures: u64,
    #[serde(default)]
    pub mdns_discoveries: u64,
    #[serde(default)]
    pub peerlist_introductions: u64,
    #[serde(default)]
    pub peerjoined_introductions: u64,
    #[serde(default)]
    pub cached_addr_dials: u64,
    #[serde(default)]
    pub dcutr_upgrades_attempted: u64,
    #[serde(default)]
    pub dcutr_upgrades_succeeded: u64,
    #[serde(default)]
    pub relayed_connections_established: u64,
    #[serde(default)]
    pub direct_connections_established: u64,
    #[serde(default)]
    pub reconcile_converged: u64,
    #[serde(default)]
    pub reconcile_diverged: u64,
    #[serde(default)]
    pub pending_pushes_redelivered: u64,
    #[serde(default)]
    pub relay_connections_demoted: u64,
    #[serde(default)]
    pub relay_bytes_out: u64,
    #[serde(default)]
    pub relay_bytes_in: u64,
    #[serde(default)]
    pub direct_bytes_out: u64,
    #[serde(default)]
    pub direct_bytes_in: u64,
    #[serde(default)]
    pub mailbox_entries_drained: u64,
    #[serde(default)]
    pub mailbox_gap_fallbacks: u64,
    #[serde(default)]
    pub mailbox_appends_skipped: u64,
    #[serde(default)]
    pub catchup_rounds: u64,
    #[serde(default)]
    pub catchup_responses_applied: u64,
    #[serde(default)]
    pub sync_rtt_histogram: Vec<(u64, u64)>,
}
