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
/// HTTP port the test-peer listens on inside its container.
const PEER_HTTP_PORT: u16 = 8080;

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
}

/// Configuration for a single peer in the harness.
pub struct PeerSpec {
    pub name: String,
    pub passphrase: Option<String>,
    /// Per-peer netem override. `None` ⇒ use the harness default.
    pub netem: Option<NetemProfile>,
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
        });
        self
    }

    /// Add a peer with an explicit netem profile that overrides the
    /// harness default. Use to model asymmetric scenarios (e.g. one
    /// peer on cellular, one on LAN).
    pub fn add_peer_with_netem(
        mut self,
        name: impl Into<String>,
        profile: NetemProfile,
    ) -> Self {
        let name = name.into();
        let passphrase = self.passphrase.clone();
        self.peers.push(PeerSpec {
            name,
            passphrase,
            netem: Some(profile),
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
        let relay = GenericImage::new(
            RELAY_IMAGE.split(':').next().unwrap(),
            RELAY_IMAGE.split(':').nth(1).unwrap(),
        )
        .with_exposed_port(RELAY_QUIC_PORT.udp())
        .with_wait_for(WaitFor::message_on_stderr("Listening on"))
        .with_env_var("IDENTITY_KEYPAIR", &relay_identity_b64)
        .with_env_var("EXTERNAL_ADDRESS", &relay_external)
        .with_env_var("RUST_LOG", "info")
        .with_network(&net_name)
        .with_container_name(&relay_host)
        .start()
        .await
        .context("start relay container")?;

        let relay_addr = format!(
            "/dns4/{relay_host}/udp/{}/quic-v1/p2p/{}",
            RELAY_QUIC_PORT, relay_peer_id
        );

        // 4. Start each peer with a suffixed container name so
        //    `harness.peer("alice")` still works (we use `spec.name` as
        //    the lookup key, while the container name carries the
        //    harness suffix).
        let mut peers = Vec::with_capacity(self.peers.len());
        for spec in self.peers {
            let container_name = format!("{}-{suffix}", spec.name);
            let effective_netem = spec.netem.clone().or_else(|| self.default_netem.clone());

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
            .with_env_var("RUST_LOG", "info,libp2p_swarm=warn")
            .with_network(&net_name)
            .with_container_name(&container_name);

            if let Some(ref p) = spec.passphrase {
                img = img.with_env_var("PASSPHRASE", p.clone());
            }

            // `tc qdisc add ... netem` requires NET_ADMIN. We add the
            // capability only when a profile is present so the unshaped
            // default keeps the same security posture as before.
            if effective_netem.is_some() {
                img = img.with_cap_add("NET_ADMIN");
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
        let action = if self.netem.is_some() { "change" } else { "add" };
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

/// Run a `tc` invocation inside a container and assert exit-code 0.
/// Used by the netem helpers in the harness; not directly part of the
/// scenario API.
async fn run_tc(
    container: &ContainerAsync<GenericImage>,
    argv: &[String],
) -> Result<()> {
    let cmd = ExecCommand::new(argv.iter().cloned())
        .with_cmd_ready_condition(CmdWaitFor::exit_code(0));
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
}

/// Per-peer HTTP API matching the routes in `bin/test_peer.rs`.
impl RunningPeer {
    /// Insert a task via SeaORM through this peer's WaveSyncDb.
    pub async fn insert_task(&self, id: &str, title: &str, completed: bool) -> Result<()> {
        let resp = reqwest::Client::new()
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
        let resp = reqwest::Client::new()
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
        let resp = reqwest::Client::new()
            .get(format!("{}/tasks/{}", self.base_url, id))
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
        Ok(reqwest::Client::new()
            .get(format!("{}/tasks", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<Task>>()
            .await?)
    }

    /// Number of currently-connected libp2p peers (relay + sync peers).
    pub async fn connected_peer_count(&self) -> Result<usize> {
        Ok(reqwest::Client::new()
            .get(format!("{}/peers", self.base_url))
            .send()
            .await?
            .error_for_status()?
            .json::<PeersResponse>()
            .await?
            .connected)
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
        let start = Instant::now();
        let mut interval = Duration::from_millis(50);
        loop {
            // Transient HTTP errors must NOT short-circuit the wait
            // — that's how container-restart scenarios were silently
            // masking convergence as failure. Treat any error as
            // "not yet" and retry until the actual timeout.
            match self.get_task(id).await {
                Ok(Some(t)) if t.title == title => return Ok(()),
                _ => {}
            }
            if start.elapsed() >= timeout {
                bail!(
                    "peer {} did not see task {{id={}, title={}}} within {:?}",
                    self.name,
                    id,
                    title,
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
