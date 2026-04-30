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
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use uuid::Uuid;

pub mod task_entity;

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
}

/// Configuration for a single peer in the harness.
pub struct PeerSpec {
    pub name: String,
    pub passphrase: Option<String>,
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
        self.peers.push(PeerSpec { name, passphrase });
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
        let relay_identity_b64 =
            B64.encode(relay_keypair.to_protobuf_encoding().context("encode keypair")?);

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

            let container = img
                .start()
                .await
                .with_context(|| format!("start peer container {}", spec.name))?;

            let host_port = container
                .get_host_port_ipv4(PEER_HTTP_PORT)
                .await
                .context("resolve peer host port")?;
            let base_url = format!("http://127.0.0.1:{host_port}");

            peers.push(RunningPeer {
                name: spec.name,
                base_url,
                container,
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
    pub async fn wait_for_task(
        &self,
        id: &str,
        title: &str,
        timeout: Duration,
    ) -> Result<()> {
        let start = Instant::now();
        let mut interval = Duration::from_millis(50);
        loop {
            if let Some(t) = self.get_task(id).await?
                && t.title == title
            {
                return Ok(());
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

