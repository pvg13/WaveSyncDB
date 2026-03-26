//! P2P sync engine powered by libp2p.
//!
//! The engine runs as a background tokio task, managing a libp2p swarm with:
//! - **request-response** for real-time push (fan-out) and version vector catch-up sync
//! - **mDNS** for local peer discovery
//! - **QUIC + TCP** transports with Noise encryption and Yamux multiplexing
//! - **relay + dcutr + autonat** for NAT traversal (WIP)
//!
//! Local write operations arrive via an mpsc channel from [`WaveSyncDb`](crate::WaveSyncDb)
//! as [`SyncChangeset`]s and are pushed to all connected peers via request-response.
//! Incoming remote changesets are applied column-by-column using per-column
//! Lamport clocks for conflict resolution.

pub(crate) mod auth_protocol;
pub(crate) mod behaviour;
pub(crate) mod push_protocol;
pub(crate) mod snapshot_protocol;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use behaviour::{WaveSyncBehaviour, WaveSyncBehaviourEvent};
use futures::FutureExt;
use libp2p::{
    Multiaddr, SwarmBuilder, autonat, dcutr, dns, futures::StreamExt, identify, identity, mdns,
    noise, ping, relay, rendezvous, request_response, swarm::SwarmEvent, yamux,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use std::panic::AssertUnwindSafe;
use tokio::sync::{Notify, broadcast, mpsc};

use crate::auth::GroupKey;
use crate::conflict;
use crate::messages::{ChangeNotification, ColumnChange, NodeId, SyncChangeset, WriteKind};
use crate::peer_tracker;
use crate::protocol::SyncRequest;
use crate::registry::TableRegistry;
use crate::shadow;

/// Commands sent from the application to the P2P engine.
#[derive(Debug)]
pub enum EngineCommand {
    /// App resumed from background — clear stale peers, restart mDNS, re-sync.
    Resume,
    /// Network interface changed (WiFi ↔ cellular) — force-disconnect all
    /// connections and re-establish on the new interface.
    NetworkTransition,
    /// Request a full sync from peers (user-triggered).
    RequestFullSync,
    /// Register a push notification token with the relay server.
    RegisterPushToken { platform: String, token: String },
    /// Graceful shutdown — stop the engine loop.
    Shutdown,
}

/// Configuration for the sync engine.
pub struct EngineConfig {
    /// How often to run periodic version vector sync (default: 30s).
    pub sync_interval: Duration,
    /// How often mDNS sends queries (default: 5s for fast LAN discovery).
    pub mdns_query_interval: Duration,
    /// How long mDNS records stay valid (default: 30s).
    pub mdns_ttl: Duration,
    /// Static bootstrap peers to dial on startup.
    pub bootstrap_peers: Vec<Multiaddr>,
    /// Relay server multiaddr for NAT traversal.
    pub relay_server: Option<Multiaddr>,
    /// Rendezvous server multiaddr for WAN peer discovery.
    pub rendezvous_server: Option<Multiaddr>,
    /// How often to discover peers via rendezvous (default: 60s).
    pub rendezvous_discover_interval: Duration,
    /// TTL for rendezvous registration in seconds (default: 300s).
    pub rendezvous_ttl: u64,
    /// Whether to listen on IPv6 in addition to IPv4.
    pub ipv6: bool,
    /// Push notification token: (platform, device_token).
    /// Platform is "Fcm" or "Apns".
    pub push_token: Option<(String, String)>,
    /// API key for managed relay authentication.
    pub api_key: Option<String>,
    /// Interval for libp2p ping keep-alives (default: 90s).
    /// Must be shorter than CGNAT mapping timeouts (typically 2–5 min for UDP).
    pub keep_alive_interval: Duration,
    /// Maximum relay circuit duration the server allows (default: 3600s).
    /// The engine proactively renews at 80% of this duration.
    pub circuit_max_duration: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(30),
            mdns_query_interval: Duration::from_secs(5),
            mdns_ttl: Duration::from_secs(30),
            bootstrap_peers: Vec::new(),
            relay_server: None,
            rendezvous_server: None,
            rendezvous_discover_interval: Duration::from_secs(60),
            rendezvous_ttl: 300,
            ipv6: false,
            push_token: None,
            api_key: None,
            keep_alive_interval: Duration::from_secs(90),
            circuit_max_duration: Duration::from_secs(3600),
        }
    }
}

impl EngineConfig {
    /// Build the libp2p mDNS config from our settings.
    pub(crate) fn mdns_config(&self) -> mdns::Config {
        mdns::Config {
            query_interval: self.mdns_query_interval,
            ttl: self.mdns_ttl,
            ..Default::default()
        }
    }
}

/// Start the P2P sync engine in a background tokio task.
#[allow(clippy::too_many_arguments)]
pub fn start_engine(
    db: DatabaseConnection,
    sync_rx: mpsc::Receiver<SyncChangeset>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    site_id: NodeId,
    topic: String,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
    cmd_rx: mpsc::Receiver<EngineCommand>,
    group_key: Option<GroupKey>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let event_tx = network_event_tx.clone();
        let result = AssertUnwindSafe(run_engine(
            db,
            sync_rx,
            change_tx,
            registry,
            site_id,
            topic,
            config,
            registry_ready,
            cmd_rx,
            group_key,
            network_status,
            network_event_tx,
        ))
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(())) => log::info!("Engine shut down cleanly"),
            Ok(Err(e)) => {
                let reason = format!("{e}");
                log::error!("Engine error: {reason}");
                let _ = event_tx.send(crate::network_status::NetworkEvent::EngineFailed { reason });
            }
            Err(panic) => {
                let msg = panic
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                log::error!("Engine panicked: {msg}");
                let _ = event_tx.send(crate::network_status::NetworkEvent::EngineFailed {
                    reason: format!("panic: {msg}"),
                });
            }
        }
    })
}

/// Build the libp2p swarm with DNS resolution.
///
/// Tries system DNS first (`/etc/resolv.conf`). If that fails (e.g. on Android
/// where `/etc/resolv.conf` does not exist), falls back to Google public DNS
/// via `dns::ResolverConfig::google()`.
fn build_swarm(
    keypair: identity::Keypair,
    mdns_config: mdns::Config,
    keep_alive_interval: Duration,
) -> Result<libp2p::Swarm<WaveSyncBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    // Try system DNS first (reads /etc/resolv.conf — works on desktop/server)
    let system_result = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns();

    match system_result {
        Ok(builder) => {
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(builder
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
        Err(e) => {
            log::warn!(
                "System DNS resolver failed (expected on Android): {e}. \
                 Falling back to Google public DNS."
            );
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_tcp(
                    Default::default(),
                    noise::Config::new,
                    yamux::Config::default,
                )?
                .with_quic()
                .with_dns_config(dns::ResolverConfig::google(), dns::ResolverOpts::default())
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_engine(
    db: DatabaseConnection,
    mut sync_rx: mpsc::Receiver<SyncChangeset>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    site_id: NodeId,
    topic_name: String,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
    cmd_rx: mpsc::Receiver<EngineCommand>,
    group_key: Option<GroupKey>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let keypair = identity::Keypair::generate_ed25519();
    let mdns_config = config.mdns_config();

    let swarm = build_swarm(keypair.clone(), mdns_config, config.keep_alive_interval)?;

    let local_peer_id = keypair.public().to_peer_id();

    // Load current db_version
    let local_db_version = shadow::get_db_version(&db).await?;

    let (snapshot_resp_tx, snapshot_resp_rx) = mpsc::channel::<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>(8);

    let (remote_changeset_tx, remote_changeset_rx) = mpsc::channel::<Vec<ColumnChange>>(32);

    let effective_topic = match &group_key {
        Some(gk) => gk.derive_topic(&topic_name),
        None => topic_name.clone(),
    };

    // Determine rendezvous namespace (same as effective_topic — already PSK-derived)
    let rendezvous_namespace = effective_topic.clone();

    let push_token = config.push_token.clone();
    let api_key = config.api_key.clone();

    // Collect infrastructure peer IDs (relay + rendezvous) so they are excluded
    // from self.peers, peer counts, and sync fan-out.
    let mut infrastructure_peers = std::collections::HashSet::new();
    if let Some(ref addr) = config.relay_server
        && let Some(libp2p::multiaddr::Protocol::P2p(pid)) = addr.iter().last()
    {
        infrastructure_peers.insert(pid);
    }
    if let Some(ref addr) = config.rendezvous_server
        && let Some(libp2p::multiaddr::Protocol::P2p(pid)) = addr.iter().last()
    {
        infrastructure_peers.insert(pid);
    }

    let mut engine = EngineRunner {
        swarm,
        peers: HashMap::new(),
        db,
        change_tx,
        registry,
        local_peer_id,
        site_id,
        topic_name: effective_topic,
        config,
        local_db_version,
        peer_db_versions: HashMap::new(),
        peer_reported_versions: HashMap::new(),
        snapshot_resp_tx,
        snapshot_resp_rx,
        remote_changeset_tx,
        remote_changeset_rx,
        registry_ready,
        registry_is_ready: false,
        cmd_rx,
        group_key,
        relay_state: RelayState::Disabled,
        nat_status: NatStatus::Unknown,
        rendezvous_namespace,
        rendezvous_cookie: None,
        rendezvous_registered: false,
        bootstrap_peers: std::collections::HashSet::new(),
        rejected_peers: std::collections::HashSet::new(),
        infrastructure_peers,
        pending_sync_peers: std::collections::HashSet::new(),
        push_token,
        push_registered: false,
        network_status,
        network_event_tx,
        resume_sync_deadline: None,
        api_key,
        keypair,
        nat_assumption_deadline: None,
        circuit_accepted_at: None,
        circuit_retry_count: 0,
    };

    // Set initial network status with local_peer_id and topic
    engine.update_network_status();
    engine.emit_network_event(crate::network_status::NetworkEvent::EngineStarted);

    engine.run(&mut sync_rx).await
}

/// State machine for relay server connection lifecycle.
#[derive(Debug)]
enum RelayState {
    /// No relay server configured.
    Disabled,
    /// Connecting to relay server (with retry count for backoff).
    Connecting { retry_count: u32 },
    /// TCP/QUIC connection established with relay peer but circuit not yet reserved.
    Connected {
        relay_peer_id: libp2p::PeerId,
        connected_at: tokio::time::Instant,
    },
    /// Listening on a relay circuit (reservation accepted).
    Listening { relay_peer_id: libp2p::PeerId },
}

/// Detected NAT status from AutoNAT.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NatStatus {
    Unknown,
    Public,
    Private,
}

struct EngineRunner {
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    local_peer_id: libp2p::PeerId,
    site_id: NodeId,
    topic_name: String,
    config: EngineConfig,
    local_db_version: u64,
    peer_db_versions: HashMap<libp2p::PeerId, u64>,
    /// Display-only peer versions from incoming requests (NOT used for sync decisions).
    peer_reported_versions: HashMap<libp2p::PeerId, u64>,
    snapshot_resp_tx: mpsc::Sender<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    snapshot_resp_rx: mpsc::Receiver<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    /// Channel for queuing remote changesets to be applied sequentially.
    remote_changeset_tx: mpsc::Sender<Vec<ColumnChange>>,
    remote_changeset_rx: mpsc::Receiver<Vec<ColumnChange>>,
    registry_ready: Arc<Notify>,
    registry_is_ready: bool,
    cmd_rx: mpsc::Receiver<EngineCommand>,
    group_key: Option<GroupKey>,
    /// Relay connection state machine.
    relay_state: RelayState,
    /// Detected NAT status from AutoNAT probes.
    nat_status: NatStatus,
    /// Rendezvous namespace for peer discovery.
    rendezvous_namespace: String,
    /// Rendezvous discovery pagination cookie.
    rendezvous_cookie: Option<rendezvous::Cookie>,
    /// Whether we have an active rendezvous registration.
    rendezvous_registered: bool,
    /// Set of bootstrap peer IDs for tracking.
    bootstrap_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers rejected due to topic mismatch — never re-add via mDNS.
    rejected_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Infrastructure peers (relay, rendezvous) — excluded from peer count and sync fan-out.
    infrastructure_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers with an in-flight sync request — prevents flooding request-response.
    pending_sync_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Push notification token to register with relay: (platform, device_token).
    push_token: Option<(String, String)>,
    /// Whether push token has been registered with the relay.
    push_registered: bool,
    /// Shared network status snapshot, read by consumers.
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    /// Broadcast sender for network events.
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    /// Optional deadline for a post-resume sync retry (gives mDNS/rendezvous time to rediscover).
    resume_sync_deadline: Option<tokio::time::Instant>,
    /// API key for managed relay authentication.
    api_key: Option<String>,
    /// Keypair used for signing auth challenges (same identity as the swarm).
    keypair: identity::Keypair,
    /// Deadline after which we assume Private NAT if AutoNAT hasn't completed
    /// and the relay is connected. `None` once resolved or if no relay configured.
    nat_assumption_deadline: Option<tokio::time::Instant>,
    /// When the current relay circuit reservation was accepted (for proactive renewal).
    circuit_accepted_at: Option<tokio::time::Instant>,
    /// Number of circuit reservation retries while stuck in `Connected` state.
    circuit_retry_count: u32,
}

impl EngineRunner {
    /// Rebuild the full network status snapshot from internal state.
    fn update_network_status(&self) {
        use crate::network_status as ns;

        let connected_peers = self
            .peers
            .iter()
            .filter(|(peer_id, _)| !self.infrastructure_peers.contains(peer_id))
            .map(|(peer_id, addr)| ns::PeerInfo {
                peer_id: ns::PeerId(peer_id.to_string()),
                address: addr.to_string(),
                db_version: self
                    .peer_db_versions
                    .get(peer_id)
                    .copied()
                    .or_else(|| self.peer_reported_versions.get(peer_id).copied()),
                is_bootstrap: self.bootstrap_peers.contains(peer_id),
                is_group_member: !self.rejected_peers.contains(peer_id),
            })
            .collect();

        let relay_status = match &self.relay_state {
            RelayState::Disabled => ns::RelayStatus::Disabled,
            RelayState::Connecting { .. } => ns::RelayStatus::Connecting,
            RelayState::Connected { .. } => ns::RelayStatus::Connected,
            RelayState::Listening { .. } => ns::RelayStatus::Listening,
        };

        let nat_status = match self.nat_status {
            NatStatus::Unknown => ns::NatStatus::Unknown,
            NatStatus::Public => ns::NatStatus::Public,
            NatStatus::Private => ns::NatStatus::Private,
        };

        let status = ns::NetworkStatus {
            local_peer_id: ns::PeerId(self.local_peer_id.to_string()),
            connected_peers,
            topic: self.topic_name.clone(),
            relay_status,
            nat_status,
            rendezvous_registered: self.rendezvous_registered,
            push_registered: self.push_registered,
            local_db_version: self.local_db_version,
            registry_ready: self.registry_is_ready,
        };

        *self.network_status.write().unwrap() = status;
    }

    /// Emit a network event on the broadcast channel, ignoring no-receiver errors.
    fn emit_network_event(&self, event: crate::network_status::NetworkEvent) {
        let _ = self.network_event_tx.send(event);
    }

    async fn run(
        &mut self,
        sync_rx: &mut mpsc::Receiver<SyncChangeset>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // IPv4 listen addresses — TCP is required, QUIC is best-effort
        self.swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())?;
        if let Err(e) = self
            .swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())
        {
            log::warn!("QUIC IPv4 listen failed (non-fatal, TCP still active): {e}");
        }

        // IPv6 listen addresses (opt-in) — TCP is required, QUIC is best-effort
        if self.config.ipv6 {
            self.swarm.listen_on("/ip6/::/tcp/0".parse().unwrap())?;
            if let Err(e) = self
                .swarm
                .listen_on("/ip6/::/udp/0/quic-v1".parse().unwrap())
            {
                log::warn!("QUIC IPv6 listen failed (non-fatal, TCP still active): {e}");
            }
        }

        // If a relay server is configured, dial it
        if let Some(ref relay_addr) = self.config.relay_server.clone() {
            if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                log::warn!("Failed to dial relay server {}: {}", relay_addr, e);
            } else {
                log::info!("Dialing relay server: {}", relay_addr);
                self.relay_state = RelayState::Connecting { retry_count: 0 };
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Connecting,
                ));
                self.update_network_status();
            }
        }

        // If a rendezvous server is configured, dial it (unless already dialing as relay)
        if let Some(ref rendezvous_addr) = self.config.rendezvous_server.clone() {
            let already_dialing = self.config.relay_server.as_ref().is_some_and(|relay_addr| {
                let relay_peer = relay_addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                    _ => None,
                });
                let rv_peer = rendezvous_addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                    _ => None,
                });
                relay_peer.is_some() && relay_peer == rv_peer
            });

            if already_dialing {
                log::info!("Rendezvous server is same peer as relay — skipping duplicate dial");
            } else if let Err(e) = self.swarm.dial(rendezvous_addr.clone()) {
                log::warn!(
                    "Failed to dial rendezvous server {}: {}",
                    rendezvous_addr,
                    e
                );
            } else {
                log::info!("Dialing rendezvous server: {}", rendezvous_addr);
            }
        }

        // Dial bootstrap peers
        for addr in self.config.bootstrap_peers.clone() {
            // Extract peer ID from the multiaddr if present
            if let Some(libp2p::multiaddr::Protocol::P2p(peer_id)) = addr.iter().last() {
                self.bootstrap_peers.insert(peer_id);
            }
            if let Err(e) = self.swarm.dial(addr.clone()) {
                log::warn!("Failed to dial bootstrap peer {}: {}", addr, e);
            } else {
                log::info!("Dialing bootstrap peer: {}", addr);
            }
        }

        let mut sync_interval = tokio::time::interval(self.config.sync_interval);
        let mut rendezvous_interval =
            tokio::time::interval(self.config.rendezvous_discover_interval);
        let has_rendezvous = self.config.rendezvous_server.is_some();
        let registry_deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(registry_deadline);

        // Relay reconnection timer (only active when relay is configured but disconnected)
        // Use interval_at to avoid immediate first tick causing a duplicate dial
        let mut relay_reconnect = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        let has_relay = self.config.relay_server.is_some();

        loop {
            tokio::select! {
                Some(changeset) = sync_rx.recv() => {
                    self.handle_local_changeset(changeset).await;
                },
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                },
                _ = sync_interval.tick() => {
                    // Periodic version vector sync with all known peers
                    if self.registry_is_ready {
                        self.sync_all_known_peers().await;
                    }
                },
                _ = rendezvous_interval.tick(), if has_rendezvous => {
                    self.rendezvous_discover();
                },
                _ = relay_reconnect.tick(), if has_relay => {
                    self.maybe_reconnect_relay();
                },
                Some((channel, response)) = self.snapshot_resp_rx.recv() => {
                    if let Err(resp) = self.swarm.behaviour_mut().snapshot.send_response(channel, response) {
                        log::error!("Failed to send sync response: {:?}", resp);
                    }
                },
                Some(changes) = self.remote_changeset_rx.recv() => {
                    apply_remote_changeset(&self.db, &self.change_tx, &self.registry, &changes).await;
                },
                _ = self.registry_ready.notified(), if !self.registry_is_ready => {
                    self.registry_is_ready = true;
                    self.update_network_status();
                    log::info!("Registry ready, syncing all known peers");
                    self.sync_all_known_peers().await;
                },
                _ = &mut registry_deadline, if !self.registry_is_ready => {
                    log::error!("Schema registry not ready after 30s — proceeding without sync tables");
                    self.registry_is_ready = true;
                    self.update_network_status();
                },
                _ = async {
                    match self.resume_sync_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if self.resume_sync_deadline.is_some() => {
                    log::info!("Post-resume sync retry");
                    self.resume_sync_deadline = None;
                    self.pending_sync_peers.clear();
                    if self.registry_is_ready {
                        self.sync_all_known_peers().await;
                    }
                },
                _ = async {
                    match self.nat_assumption_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if self.nat_assumption_deadline.is_some() => {
                    self.nat_assumption_deadline = None;
                    if self.nat_status == NatStatus::Unknown
                        && matches!(self.relay_state, RelayState::Connected { .. } | RelayState::Listening { .. })
                    {
                        log::info!("AutoNAT timeout — relay connected, assuming Private NAT");
                        self.nat_status = NatStatus::Private;
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::NatStatusChanged(
                                crate::network_status::NatStatus::Private,
                            ),
                        );
                        self.update_network_status();
                    }
                },
                _ = async {
                    match self.circuit_accepted_at {
                        Some(at) => {
                            let renew_after = self.config.circuit_max_duration.mul_f64(0.8);
                            tokio::time::sleep_until(at + renew_after).await
                        }
                        None => std::future::pending().await,
                    }
                }, if self.circuit_accepted_at.is_some()
                    && matches!(self.relay_state, RelayState::Listening { .. }) => {
                    log::info!("Proactively renewing relay circuit (80% of max duration)");
                    self.circuit_accepted_at = None;
                    if let Some(ref relay_addr) = self.config.relay_server {
                        let circuit_addr = relay_addr
                            .clone()
                            .with(libp2p::multiaddr::Protocol::P2pCircuit);
                        if let Err(e) = self.swarm.listen_on(circuit_addr) {
                            log::warn!("Failed to renew relay circuit: {e}");
                        }
                    }
                },
                Some(cmd) = self.cmd_rx.recv() => {
                    if self.handle_command(cmd).await {
                        break Ok(());
                    }
                },
            }
        }
    }

    /// Dispatch an engine command received from the application.
    /// Returns `true` if the engine should shut down.
    async fn handle_command(&mut self, cmd: EngineCommand) -> bool {
        match cmd {
            EngineCommand::Resume => {
                while let Ok(EngineCommand::Resume) = self.cmd_rx.try_recv() {}
                self.handle_resume().await;
                false
            }
            EngineCommand::NetworkTransition => {
                // Drain duplicate NetworkTransition commands
                while let Ok(EngineCommand::NetworkTransition) = self.cmd_rx.try_recv() {}
                log::info!("Network transition detected — force-disconnecting all peers");
                self.handle_resume().await;
                false
            }
            EngineCommand::RequestFullSync => {
                log::info!("Full sync requested by user");
                // Reset peer versions to trigger full re-sync
                self.peer_db_versions.clear();
                self.peer_reported_versions.clear();
                self.pending_sync_peers.clear();
                self.trigger_rediscovery();
                if self.config.relay_server.is_some() {
                    self.maybe_reconnect_relay();
                }
                if self.config.rendezvous_server.is_some() {
                    self.rendezvous_discover();
                }
                self.sync_all_known_peers().await;
                self.resume_sync_deadline =
                    Some(tokio::time::Instant::now() + Duration::from_secs(2));
                false
            }
            EngineCommand::RegisterPushToken { platform, token } => {
                log::info!("Registering push token (platform: {platform})");
                self.push_token = Some((platform, token));
                self.push_registered = false;
                // If relay is already connected, register immediately
                if let RelayState::Connected { relay_peer_id, .. }
                | RelayState::Listening { relay_peer_id } = self.relay_state
                {
                    self.maybe_register_push_token(relay_peer_id);
                }
                false
            }
            EngineCommand::Shutdown => {
                log::info!("Engine shutdown requested");
                true
            }
        }
    }

    async fn handle_resume(&mut self) {
        log::info!("App resumed — triggering rediscovery and sync");

        // Clear version maps so the next sync requests all changes since the
        // last *persisted* peer version, not stale in-memory values that may
        // have drifted during a network transition.
        self.peer_db_versions.clear();
        self.peer_reported_versions.clear();
        self.pending_sync_peers.clear();

        // Force-disconnect relay — the TCP socket is likely dead on the old
        // network interface. ConnectionClosed handler will reset relay_state
        // and trigger reconnect.
        if let RelayState::Connected { relay_peer_id, .. }
        | RelayState::Listening { relay_peer_id } = self.relay_state
        {
            log::info!("Resume: disconnecting relay {relay_peer_id} for clean reconnection");
            let _ = self.swarm.disconnect_peer_id(relay_peer_id);
        }

        // Force-disconnect all non-infrastructure peers (likely dead sockets)
        let stale: Vec<_> = self.peers.keys().cloned().collect();
        for pid in stale {
            let _ = self.swarm.disconnect_peer_id(pid);
        }

        // 1. Trigger mDNS rediscovery (LAN)
        self.trigger_rediscovery();

        // 2. WAN: reconnect relay if disconnected
        if self.config.relay_server.is_some() {
            self.maybe_reconnect_relay();
        }

        // 3. WAN: trigger rendezvous rediscovery
        if self.config.rendezvous_server.is_some() {
            self.rendezvous_discover();
        }

        // 4. Sync with any peers still connected (may be none — that's OK)
        self.sync_all_known_peers().await;

        // 5. Schedule a delayed retry to catch peers rediscovered via mDNS/rendezvous
        self.resume_sync_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
    }

    async fn handle_local_changeset(&mut self, changeset: SyncChangeset) {
        // Update local db_version
        self.local_db_version = self.local_db_version.max(changeset.db_version);
        self.update_network_status();

        // Fan-out: push changeset to all connected peers via request-response
        let peer_ids: Vec<libp2p::PeerId> = self
            .peers
            .keys()
            .filter(|p| !self.rejected_peers.contains(p))
            .filter(|p| !self.infrastructure_peers.contains(p))
            .cloned()
            .collect();

        if peer_ids.is_empty() {
            log::debug!("No peers to push changeset to");
            return;
        }

        for peer_id in &peer_ids {
            let mut req = SyncRequest::Push {
                changeset: changeset.clone(),
                topic: self.topic_name.clone(),
                hmac: None,
            };

            if let Some(ref gk) = self.group_key {
                // Serialize with hmac: None, compute MAC, then set hmac
                if let Ok(bytes) = serde_json::to_vec(&req) {
                    let tag = gk.mac(&bytes);
                    let SyncRequest::Push { ref mut hmac, .. } = req else {
                        unreachable!()
                    };
                    *hmac = Some(tag);
                }
            }

            self.swarm
                .behaviour_mut()
                .snapshot
                .send_request(peer_id, req);
        }

        log::info!(
            "Pushed changeset (db_version={}) to {} peers",
            changeset.db_version,
            peer_ids.len()
        );

        // Notify relay to send push notifications to sleeping mobile peers
        self.notify_relay_topic();
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<WaveSyncBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address:?}");
                // If this is a relay circuit address, add it as external so
                // rendezvous and identify advertise it to remote peers
                if address
                    .iter()
                    .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2pCircuit))
                {
                    self.swarm.add_external_address(address.clone());
                    log::info!("Added relay circuit as external address: {address}");
                    // Re-register with rendezvous so the new circuit address is advertised
                    if let Some(ref rv_addr) = self.config.rendezvous_server
                        && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                            rv_addr.iter().last()
                        && self.swarm.is_connected(&rv_peer_id)
                    {
                        self.rendezvous_register(rv_peer_id);
                    }
                } else {
                    // Non-circuit address (new network interface) — reconnect relay if needed
                    if matches!(self.relay_state, RelayState::Connecting { .. })
                        && let Some(ref relay_addr) = self.config.relay_server
                    {
                        log::info!("New listen address detected, attempting relay reconnection");
                        if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                            log::warn!("Relay redial on new address failed: {e}");
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Sent {
                peer_id,
                ..
            })) => {
                log::info!("Sent identify info to {peer_id:?}");
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(
                identify::Event::Received { info, .. },
            )) => {
                log::info!("Received identify info: {:?}", info.protocol_version);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Ping(ping::Event {
                peer,
                connection,
                result,
            })) => {
                if let Err(ref e) = result {
                    log::warn!("Ping failed for {peer:?} over {connection:?}: {e}");
                    // If relay ping fails, log prominently — libp2p will close the
                    // connection, triggering ConnectionClosed → reconnect.
                    if matches!(
                        &self.relay_state,
                        RelayState::Connected { relay_peer_id, .. }
                        | RelayState::Listening { relay_peer_id }
                        if peer == *relay_peer_id
                    ) {
                        log::warn!("Ping to relay server failed — connection will be closed");
                    }
                } else {
                    log::debug!("Ping event with {peer:?} over {connection:?}: {result:?}");
                }
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Mdns(event)) => {
                self.handle_mdns(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Snapshot(event)) => {
                self.handle_snapshot(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::RelayClient(event)) => {
                self.handle_relay_client(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Dcutr(event)) => {
                self.handle_dcutr(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Autonat(event)) => {
                self.handle_autonat(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Rendezvous(event)) => {
                self.handle_rendezvous(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Push(event)) => {
                self.handle_push_event(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Auth(event)) => {
                self.handle_auth_challenge(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::AuthResult(event)) => {
                self.handle_auth_result(event);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                log::info!("Connection established with {peer_id}");

                // If this is the relay server, transition state
                if let Some(ref relay_addr) = self.config.relay_server
                    && let Some(libp2p::multiaddr::Protocol::P2p(relay_peer_id)) =
                        relay_addr.iter().last()
                    && peer_id == relay_peer_id
                {
                    log::info!("Connected to relay server {peer_id}");
                    self.infrastructure_peers.insert(peer_id);
                    self.circuit_retry_count = 0;
                    self.relay_state = RelayState::Connected {
                        relay_peer_id: peer_id,
                        connected_at: tokio::time::Instant::now(),
                    };
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connected,
                        ),
                    );
                    self.update_network_status();

                    // Eagerly listen on relay circuit — AutoNAT v2 probes timeout on mobile
                    // (server can't dial back through NAT), so we cannot wait for NAT detection.
                    let circuit_addr = relay_addr
                        .clone()
                        .with(libp2p::multiaddr::Protocol::P2pCircuit);
                    if let Err(e) = self.swarm.listen_on(circuit_addr.clone()) {
                        log::warn!("Failed to listen on relay circuit: {e}");
                    } else {
                        log::info!("Listening on relay circuit (eager): {circuit_addr}");
                    }

                    // Start NAT assumption timer: if AutoNAT hasn't completed
                    // after 30s (common on mobile CGNAT), assume Private.
                    if self.nat_status == NatStatus::Unknown
                        && self.nat_assumption_deadline.is_none()
                    {
                        self.nat_assumption_deadline =
                            Some(tokio::time::Instant::now() + Duration::from_secs(30));
                    }

                    // Register push token with relay if configured
                    self.maybe_register_push_token(peer_id);
                }

                // If this is a rendezvous server, discover peers
                // Don't register yet — no external addresses available until relay circuit arrives.
                // Registration will happen in NewListenAddr when circuit address is added.
                if let Some(ref rendezvous_addr) = self.config.rendezvous_server
                    && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                        rendezvous_addr.iter().last()
                    && peer_id == rv_peer_id
                {
                    log::info!("Connected to rendezvous server {peer_id}");
                    self.infrastructure_peers.insert(peer_id);
                    self.rendezvous_discover();
                }

                // If this is a bootstrap peer, add to peers and initiate sync
                if self.bootstrap_peers.contains(&peer_id) {
                    let addr = endpoint.get_remote_address().clone();
                    self.peers.insert(peer_id, addr.clone());

                    self.emit_network_event(crate::network_status::NetworkEvent::PeerConnected(
                        crate::network_status::PeerInfo {
                            peer_id: crate::network_status::PeerId(peer_id.to_string()),
                            address: addr.to_string(),
                            db_version: None,
                            is_bootstrap: true,
                            is_group_member: true,
                        },
                    ));
                    self.update_network_status();

                    let db = self.db.clone();
                    let peer_str = peer_id.to_string();
                    tokio::spawn(async move {
                        let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
                    });
                }

                if self.registry_is_ready
                    && !self.rejected_peers.contains(&peer_id)
                    && !self.infrastructure_peers.contains(&peer_id)
                {
                    // Ensure reconnecting peer is tracked for future periodic syncs
                    // (may have been removed by mDNS Expired or never added for inbound connections)
                    self.peers
                        .entry(peer_id)
                        .or_insert_with(|| endpoint.get_remote_address().clone());
                    // Refresh local_db_version in case spawned tasks updated it
                    self.local_db_version = shadow::get_db_version(&self.db)
                        .await
                        .unwrap_or(self.local_db_version);
                    self.initiate_sync_for_peer(peer_id);
                }
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                log::debug!("Connection closed with {peer_id} ({num_established} remaining)");

                // Clean pending_sync_peers so the peer can be re-synced on reconnect
                self.pending_sync_peers.remove(&peer_id);

                // Only act on relay/rendezvous disconnect when ALL connections are gone
                if num_established > 0 {
                    // Still have active connections to this peer — nothing to do
                } else if let RelayState::Connected { relay_peer_id, .. }
                | RelayState::Listening { relay_peer_id } = &self.relay_state
                    && peer_id == *relay_peer_id
                {
                    log::warn!("Lost connection to relay server {peer_id}");
                    self.relay_state = RelayState::Connecting { retry_count: 0 };
                    self.circuit_accepted_at = None;
                    // Reset push registration — server lost our token
                    self.push_registered = false;
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connecting,
                        ),
                    );
                    // If relay also serves rendezvous, reset that too
                    if let Some(ref rv_addr) = self.config.rendezvous_server
                        && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                            rv_addr.iter().last()
                        && peer_id == rv_peer_id
                    {
                        log::warn!("Rendezvous server also disconnected (same as relay)");
                        self.rendezvous_registered = false;
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::RendezvousStatusChanged {
                                registered: false,
                            },
                        );
                    }
                    self.update_network_status();

                    // Attempt immediate reconnection (timer is fallback if this fails)
                    if let Some(ref relay_addr) = self.config.relay_server {
                        log::info!("Attempting immediate relay reconnection");
                        if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                            log::warn!("Immediate relay redial failed: {e}");
                        }
                    }
                }

                // If rendezvous server disconnected (and is different from relay)
                if num_established == 0
                    && let Some(ref rv_addr) = self.config.rendezvous_server
                    && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                        rv_addr.iter().last()
                    && peer_id == rv_peer_id
                    && !matches!(&self.relay_state, RelayState::Connecting { .. })
                {
                    // Only reset if we didn't already handle it above (relay == rendezvous case)
                    if self.rendezvous_registered {
                        log::warn!("Lost connection to rendezvous server {peer_id}");
                        self.rendezvous_registered = false;
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::RendezvousStatusChanged {
                                registered: false,
                            },
                        );
                        self.update_network_status();
                    }
                }

                // When a non-infrastructure peer fully disconnects and has sync history,
                // immediately try rendezvous discovery to find them via WAN
                if num_established == 0
                    && !self.infrastructure_peers.contains(&peer_id)
                    && self.peer_db_versions.contains_key(&peer_id)
                {
                    log::info!(
                        "Peer {peer_id} disconnected with sync history, triggering rendezvous discover"
                    );
                    self.rendezvous_discover();
                }
            }
            SwarmEvent::ListenerClosed { reason, .. } => {
                if let Err(ref e) = reason {
                    log::warn!("Listener closed with error: {e}");
                }
                // If relay was in Listening state, reset to Connected and re-request circuit
                if let RelayState::Listening { relay_peer_id } = self.relay_state {
                    log::warn!("Relay listener closed, re-requesting circuit reservation");
                    self.circuit_retry_count = 0;
                    self.relay_state = RelayState::Connected {
                        relay_peer_id,
                        connected_at: tokio::time::Instant::now(),
                    };
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connected,
                        ),
                    );
                    self.update_network_status();
                    // Re-request immediately (don't wait for AutoNAT)
                    if let Some(ref relay_addr) = self.config.relay_server {
                        let circuit_addr = relay_addr
                            .clone()
                            .with(libp2p::multiaddr::Protocol::P2pCircuit);
                        if let Err(e) = self.swarm.listen_on(circuit_addr) {
                            log::warn!("Failed to re-listen on relay circuit: {e}");
                        }
                    }
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                log::warn!("Listen address expired: {address}");
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                log::warn!("Outgoing connection error to {peer_id:?}: {error}");
                if let Some(pid) = peer_id
                    && !self.swarm.is_connected(&pid)
                    && self.peers.remove(&pid).is_some()
                {
                    self.pending_sync_peers.remove(&pid);
                    self.emit_network_event(crate::network_status::NetworkEvent::PeerDisconnected(
                        crate::network_status::PeerId(pid.to_string()),
                    ));
                    self.update_network_status();
                }
            }
            _ => {}
        }
    }

    async fn sync_all_known_peers(&mut self) {
        // Refresh local_db_version from DB before syncing, in case spawned tasks
        // (apply_remote_changeset) have incremented it since we last checked.
        self.local_db_version = shadow::get_db_version(&self.db)
            .await
            .unwrap_or(self.local_db_version);

        let peer_ids: Vec<libp2p::PeerId> = self
            .peers
            .keys()
            .filter(|p| !self.infrastructure_peers.contains(p))
            .cloned()
            .collect();
        for peer_id in peer_ids {
            self.initiate_sync_for_peer(peer_id);
        }
    }

    fn initiate_sync_for_peer(&mut self, peer_id: libp2p::PeerId) {
        if self.pending_sync_peers.contains(&peer_id) {
            return;
        }
        if self.rejected_peers.contains(&peer_id) {
            return;
        }
        if self.infrastructure_peers.contains(&peer_id) {
            return;
        }

        let their_last_db_version = self.peer_db_versions.get(&peer_id).copied().unwrap_or(0);

        log::info!(
            "Requesting version vector sync from peer {peer_id} (their last known version: {their_last_db_version})"
        );

        let mut req = SyncRequest::VersionVector {
            my_db_version: self.local_db_version,
            your_last_db_version: their_last_db_version,
            site_id: self.site_id,
            topic: self.topic_name.clone(),
            hmac: None,
        };

        if let Some(ref gk) = self.group_key {
            // Serialize with hmac: None, compute MAC, then set hmac
            if let Ok(bytes) = serde_json::to_vec(&req) {
                let tag = gk.mac(&bytes);
                if let SyncRequest::VersionVector { ref mut hmac, .. } = req {
                    *hmac = Some(tag);
                }
            }
        }

        let _req_id = self
            .swarm
            .behaviour_mut()
            .snapshot
            .send_request(&peer_id, req);
        self.pending_sync_peers.insert(peer_id);
    }

    fn handle_mdns(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, multiaddr) in list {
                    // Never re-add peers rejected for topic mismatch
                    if self.rejected_peers.contains(&peer_id) {
                        continue;
                    }
                    // Skip dial and address update if already tracked and connected —
                    // avoids duplicate dials from multi-address mDNS discovery
                    // (TCP+QUIC × multiple IPs), but still allow sync initiation below
                    if self.peers.contains_key(&peer_id) && self.swarm.is_connected(&peer_id) {
                        if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                        continue;
                    }
                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                    // Dial if not currently connected (handles both new peers and reconnections
                    // after network disruption where the peer is still in self.peers but the
                    // TCP/QUIC connection is dead)
                    if !self.swarm.is_connected(&peer_id)
                        && let Err(e) = self.swarm.dial(multiaddr.clone())
                    {
                        log::warn!("Failed to dial peer {peer_id}: {e}");
                    }
                    self.peers.insert(peer_id, multiaddr.clone());

                    self.emit_network_event(crate::network_status::NetworkEvent::PeerConnected(
                        crate::network_status::PeerInfo {
                            peer_id: crate::network_status::PeerId(peer_id.to_string()),
                            address: multiaddr.to_string(),
                            db_version: self.peer_db_versions.get(&peer_id).copied(),
                            is_bootstrap: self.bootstrap_peers.contains(&peer_id),
                            is_group_member: true,
                        },
                    ));
                    self.update_network_status();

                    // Register peer and update last_seen
                    let db = self.db.clone();
                    let peer_str = peer_id.to_string();
                    tokio::spawn(async move {
                        let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
                    });

                    if self.registry_is_ready && self.swarm.is_connected(&peer_id) {
                        self.initiate_sync_for_peer(peer_id);
                    }
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, multiaddr) in list {
                    log::debug!("Expired peer {peer_id} at {multiaddr}");
                    self.peers.remove(&peer_id);
                    self.pending_sync_peers.remove(&peer_id);
                    self.emit_network_event(crate::network_status::NetworkEvent::PeerDisconnected(
                        crate::network_status::PeerId(peer_id.to_string()),
                    ));
                    self.update_network_status();
                }
            }
        }
    }

    fn handle_relay_client(&mut self, event: relay::client::Event) {
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } => {
                log::info!("Relay reservation accepted by {relay_peer_id}");
                self.relay_state = RelayState::Listening { relay_peer_id };
                self.circuit_retry_count = 0;
                self.circuit_accepted_at = Some(tokio::time::Instant::now());
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Listening,
                ));
                self.update_network_status();
            }
            _ => {
                log::info!("Relay client event (non-acceptance): {:?}", event);
            }
        }
    }

    fn handle_dcutr(&mut self, event: dcutr::Event) {
        let peer = event.remote_peer_id;
        match event.result {
            Ok(_) => {
                log::info!("DCUtR: direct connection upgrade succeeded with {peer}");
            }
            Err(error) => {
                log::debug!("DCUtR: direct connection upgrade failed with {peer}: {error}");
            }
        }
    }

    fn handle_autonat(&mut self, event: autonat::v2::client::Event) {
        // AutoNAT completed — cancel the assumption timer (real result takes precedence)
        self.nat_assumption_deadline = None;
        match &event.result {
            Ok(()) => {
                log::info!(
                    "AutoNAT: address {} is reachable (tested by {})",
                    event.tested_addr,
                    event.server
                );
                let changed = self.nat_status != NatStatus::Public;
                self.nat_status = NatStatus::Public;
                if changed {
                    self.emit_network_event(crate::network_status::NetworkEvent::NatStatusChanged(
                        crate::network_status::NatStatus::Public,
                    ));
                    self.update_network_status();
                }
            }
            Err(e) => {
                log::info!(
                    "AutoNAT: address {} is NOT reachable (tested by {}): {e}",
                    event.tested_addr,
                    event.server
                );
                let changed = self.nat_status != NatStatus::Private;
                self.nat_status = NatStatus::Private;
                if changed {
                    self.emit_network_event(crate::network_status::NetworkEvent::NatStatusChanged(
                        crate::network_status::NatStatus::Private,
                    ));
                    self.update_network_status();
                }
                // Relay circuit is now requested eagerly on ConnectionEstablished,
                // so we no longer trigger it from AutoNAT. NAT status is still tracked
                // above for NetworkStatus reporting.
            }
        }
    }

    fn handle_rendezvous(&mut self, event: rendezvous::client::Event) {
        match event {
            rendezvous::client::Event::Registered {
                rendezvous_node,
                ttl,
                namespace,
            } => {
                log::info!(
                    "Registered at rendezvous server {rendezvous_node} with namespace '{namespace}' (TTL: {ttl}s)"
                );
                self.rendezvous_registered = true;
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RendezvousStatusChanged {
                        registered: true,
                    },
                );
                self.update_network_status();
            }
            rendezvous::client::Event::RegisterFailed {
                rendezvous_node,
                namespace,
                error,
            } => {
                log::warn!(
                    "Rendezvous registration failed at {rendezvous_node} namespace '{namespace}': {error:?}"
                );
                self.rendezvous_registered = false;
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RendezvousStatusChanged {
                        registered: false,
                    },
                );
                self.update_network_status();
            }
            rendezvous::client::Event::Discovered {
                rendezvous_node,
                registrations,
                cookie,
            } => {
                log::info!(
                    "Discovered {} peers via rendezvous at {rendezvous_node}",
                    registrations.len()
                );
                self.rendezvous_cookie = Some(cookie);

                for registration in registrations {
                    let peer_id = registration.record.peer_id();
                    if peer_id == self.local_peer_id {
                        continue;
                    }

                    for addr in registration.record.addresses() {
                        log::info!("Rendezvous discovered peer {peer_id} at {addr}");
                        if !self.swarm.is_connected(&peer_id) {
                            if let Err(e) = self.swarm.dial(addr.clone()) {
                                log::warn!("Failed to dial rendezvous peer {peer_id}: {e}");
                            }
                        } else if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                    }
                }
            }
            rendezvous::client::Event::DiscoverFailed {
                rendezvous_node,
                namespace,
                error,
            } => {
                log::warn!(
                    "Rendezvous discovery failed at {rendezvous_node} namespace {namespace:?}: {error:?}"
                );
            }
            rendezvous::client::Event::Expired { peer } => {
                log::debug!("Rendezvous registration expired for peer {peer}");
            }
        }
    }

    /// Register with the rendezvous server.
    fn rendezvous_register(&mut self, server_peer_id: libp2p::PeerId) {
        let namespace = match rendezvous::Namespace::new(self.rendezvous_namespace.clone()) {
            Ok(ns) => ns,
            Err(e) => {
                log::error!("Invalid rendezvous namespace: {e:?}");
                return;
            }
        };

        match self.swarm.behaviour_mut().rendezvous.register(
            namespace,
            server_peer_id,
            None, // Let server assign default TTL (server MIN_TTL=7200s)
        ) {
            Ok(()) => {
                log::info!("Sent rendezvous registration to {server_peer_id}");
            }
            Err(e) => {
                log::warn!("Failed to send rendezvous registration: {e}");
            }
        }
    }

    /// Discover peers from the rendezvous server.
    fn rendezvous_discover(&mut self) {
        let server_peer_id = match &self.config.rendezvous_server {
            Some(addr) => match addr.iter().last() {
                Some(libp2p::multiaddr::Protocol::P2p(peer_id)) => peer_id,
                _ => {
                    log::debug!("Rendezvous server address has no peer ID, skipping discover");
                    return;
                }
            },
            None => return,
        };

        if !self.swarm.is_connected(&server_peer_id) {
            log::debug!("Not connected to rendezvous server, skipping discover");
            return;
        }

        let namespace = match rendezvous::Namespace::new(self.rendezvous_namespace.clone()) {
            Ok(ns) => ns,
            Err(e) => {
                log::error!("Invalid rendezvous namespace: {e:?}");
                return;
            }
        };

        // Re-register if we have external addresses (avoids NoExternalAddresses error).
        // Handles TTL expiry and stale state after silent disconnects.
        if self.swarm.external_addresses().count() > 0 {
            self.rendezvous_register(server_peer_id);
        }

        self.swarm.behaviour_mut().rendezvous.discover(
            Some(namespace),
            self.rendezvous_cookie.clone(),
            None,
            server_peer_id,
        );
    }

    /// Attempt to reconnect to the relay server if disconnected.
    fn maybe_reconnect_relay(&mut self) {
        match &self.relay_state {
            RelayState::Connecting { retry_count } => {
                if let Some(ref relay_addr) = self.config.relay_server {
                    let count = *retry_count;
                    // Exponential backoff via tick-skipping: dial every 2^min(count,3) ticks
                    // With 5s base interval: 5s, 10s, 20s, 40s, 40s, 40s...
                    let skip = 1u32 << count.min(3); // 1, 2, 4, 8, 8, 8...
                    if count % skip == 0 {
                        log::info!("Attempting relay reconnection (attempt {})", count + 1);
                        if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                            log::warn!("Failed to redial relay server: {}", e);
                        }
                    }
                    self.relay_state = RelayState::Connecting {
                        retry_count: count + 1,
                    };
                }
            }
            RelayState::Connected {
                relay_peer_id,
                connected_at,
            } => {
                // Circuit reservation hasn't completed — retry if stuck for >5s
                if connected_at.elapsed() > Duration::from_secs(5) {
                    self.circuit_retry_count += 1;
                    if self.circuit_retry_count >= 3 {
                        log::warn!(
                            "Circuit reservation failed {} times, forcing full relay reconnect",
                            self.circuit_retry_count
                        );
                        let pid = *relay_peer_id;
                        self.circuit_retry_count = 0;
                        let _ = self.swarm.disconnect_peer_id(pid);
                        // ConnectionClosed handler will reset to Connecting and trigger full reconnect
                    } else {
                        log::info!(
                            "Relay stuck in Connected for >5s, retrying circuit reservation (attempt {})",
                            self.circuit_retry_count
                        );
                        if let Some(ref relay_addr) = self.config.relay_server {
                            let circuit_addr = relay_addr
                                .clone()
                                .with(libp2p::multiaddr::Protocol::P2pCircuit);
                            if let Err(e) = self.swarm.listen_on(circuit_addr) {
                                log::warn!("Retry listen_on relay circuit failed: {e}");
                            }
                        }
                        // Reset connected_at to space out retries
                        let pid = *relay_peer_id;
                        self.relay_state = RelayState::Connected {
                            relay_peer_id: pid,
                            connected_at: tokio::time::Instant::now(),
                        };
                    }
                }
            }
            RelayState::Disabled | RelayState::Listening { .. } => {
                // No action needed
            }
        }
    }

    fn trigger_rediscovery(&mut self) {
        log::info!("Triggering mDNS rediscovery");
        match mdns::tokio::Behaviour::new(self.config.mdns_config(), self.local_peer_id) {
            Ok(new_mdns) => {
                self.swarm.behaviour_mut().mdns =
                    libp2p::swarm::behaviour::toggle::Toggle::from(Some(new_mdns));
            }
            Err(e) => log::warn!("mDNS unavailable during rediscovery: {e}"),
        }
    }

    fn handle_snapshot(
        &mut self,
        event: request_response::Event<crate::protocol::SyncRequest, crate::protocol::SyncResponse>,
    ) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    log::info!("Received sync request from peer {peer}: {request:?}");

                    match request {
                        SyncRequest::VersionVector {
                            my_db_version,
                            your_last_db_version,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            // Verify HMAC if group key is configured
                            if let Some(ref gk) = self.group_key {
                                let tag = match req_hmac {
                                    Some(t) => t,
                                    None => {
                                        log::debug!(
                                            "Rejecting unauthenticated sync request from peer {peer}"
                                        );
                                        return;
                                    }
                                };
                                // Re-serialize with hmac: None for verification
                                let verify_req = SyncRequest::VersionVector {
                                    my_db_version,
                                    your_last_db_version,
                                    site_id: peer_site_id,
                                    topic: peer_topic.clone(),
                                    hmac: None,
                                };
                                if let Ok(bytes) = serde_json::to_vec(&verify_req)
                                    && !gk.verify(&bytes, &tag)
                                {
                                    log::debug!(
                                        "Rejecting sync request with invalid HMAC from peer {peer}"
                                    );
                                    return;
                                }
                            }

                            // Reject requests from peers on a different topic
                            if !peer_topic.is_empty() && peer_topic != self.topic_name {
                                log::debug!(
                                    "Ignoring sync request from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                                    self.topic_name
                                );
                                // Permanently reject this peer so mDNS won't re-add it
                                self.rejected_peers.insert(peer);
                                self.pending_sync_peers.remove(&peer);
                                self.peers.remove(&peer);
                                self.peer_db_versions.remove(&peer);
                                self.peer_reported_versions.remove(&peer);
                                self.emit_network_event(
                                    crate::network_status::NetworkEvent::PeerRejected(
                                        crate::network_status::PeerId(peer.to_string()),
                                    ),
                                );
                                self.update_network_status();
                                return;
                            }

                            // NOTE: Do NOT update peer_db_versions here. The peer's
                            // reported db_version tells us what THEY have, but we haven't
                            // received their data yet. peer_db_versions is only updated in
                            // the response handler where we actually receive and process
                            // changes. Updating here would cause us to skip changes in the
                            // next sync request (your_last_db_version would be too high).
                            //
                            // However, track in peer_reported_versions for display purposes.
                            let reported = self.peer_reported_versions.entry(peer).or_insert(0);
                            *reported = (*reported).max(my_db_version);

                            self.emit_network_event(
                                crate::network_status::NetworkEvent::PeerSynced {
                                    peer_id: crate::network_status::PeerId(peer.to_string()),
                                    db_version: my_db_version,
                                },
                            );
                            self.update_network_status();

                            // Spawn async task to query changes and respond
                            let db = self.db.clone();
                            let registry = self.registry.clone();
                            let resp_tx = self.snapshot_resp_tx.clone();
                            let local_db_version = self.local_db_version;
                            let local_site_id = self.site_id;
                            let change_tx = self.change_tx.clone();
                            let topic_name = self.topic_name.clone();
                            let group_key = self.group_key.clone();

                            tokio::spawn(async move {
                                // Get changes since the peer's last known version of us
                                let changes = match shadow::get_changes_since(
                                    &db,
                                    &registry,
                                    your_last_db_version,
                                )
                                .await
                                {
                                    Ok(c) => c,
                                    Err(e) => {
                                        log::error!(
                                            "Failed to get changes since {}: {}",
                                            your_last_db_version,
                                            e
                                        );
                                        Vec::new()
                                    }
                                };

                                let mut resp = crate::protocol::SyncResponse::ChangesetResponse {
                                    changes,
                                    my_db_version: local_db_version,
                                    your_last_db_version: my_db_version,
                                    site_id: local_site_id,
                                    topic: topic_name,
                                    hmac: None,
                                };

                                // Sign response if group key is configured
                                if let Some(ref gk) = group_key
                                    && let Ok(bytes) = serde_json::to_vec(&resp)
                                {
                                    let tag = gk.mac(&bytes);
                                    if let crate::protocol::SyncResponse::ChangesetResponse {
                                        ref mut hmac,
                                        ..
                                    } = resp
                                    {
                                        *hmac = Some(tag);
                                    }
                                }

                                if let Err(e) = resp_tx.send((channel, resp)).await {
                                    log::error!("Failed to queue sync response: {}", e);
                                }

                                // Also persist peer version
                                let _ = peer_tracker::upsert_peer_version(
                                    &db,
                                    &peer.to_string(),
                                    &peer_site_id,
                                    my_db_version,
                                )
                                .await;

                                let _ = change_tx; // keep alive
                            });
                        }
                        SyncRequest::Push {
                            changeset,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            // Verify HMAC if group key is configured
                            if let Some(ref gk) = self.group_key {
                                let tag = match req_hmac {
                                    Some(t) => t,
                                    None => {
                                        log::debug!(
                                            "Rejecting unauthenticated push from peer {peer}"
                                        );
                                        return;
                                    }
                                };
                                let verify_req = SyncRequest::Push {
                                    changeset: changeset.clone(),
                                    topic: peer_topic.clone(),
                                    hmac: None,
                                };
                                if let Ok(bytes) = serde_json::to_vec(&verify_req)
                                    && !gk.verify(&bytes, &tag)
                                {
                                    log::debug!(
                                        "Rejecting push with invalid HMAC from peer {peer}"
                                    );
                                    return;
                                }
                            }

                            // Reject pushes from peers on a different topic
                            if !peer_topic.is_empty() && peer_topic != self.topic_name {
                                log::debug!(
                                    "Ignoring push from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                                    self.topic_name
                                );
                                self.rejected_peers.insert(peer);
                                self.peers.remove(&peer);
                                self.peer_db_versions.remove(&peer);
                                self.peer_reported_versions.remove(&peer);
                                self.emit_network_event(
                                    crate::network_status::NetworkEvent::PeerRejected(
                                        crate::network_status::PeerId(peer.to_string()),
                                    ),
                                );
                                self.update_network_status();
                                return;
                            }

                            // Track peer's db_version (use max to avoid overwriting with stale values)
                            let entry = self.peer_db_versions.entry(peer).or_insert(0);
                            *entry = (*entry).max(changeset.db_version);
                            let reported = self.peer_reported_versions.entry(peer).or_insert(0);
                            *reported = (*reported).max(changeset.db_version);

                            log::info!(
                                "Received push from peer {peer} with {} changes at db_version {}",
                                changeset.changes.len(),
                                changeset.db_version,
                            );

                            // Send PushAck immediately via response channel
                            let resp_tx = self.snapshot_resp_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = resp_tx
                                    .send((channel, crate::protocol::SyncResponse::PushAck))
                                    .await
                                {
                                    log::error!("Failed to send PushAck: {e}");
                                }
                            });

                            // Queue changeset for sequential application in the main loop
                            if let Err(e) = self.remote_changeset_tx.try_send(changeset.changes) {
                                log::warn!("Remote changeset queue full, dropping push: {e}");
                            }
                        }
                    }
                }
                request_response::Message::Response { response, .. } => {
                    self.pending_sync_peers.remove(&peer);
                    log::info!("Received sync response from peer {peer}");

                    match response {
                        crate::protocol::SyncResponse::ChangesetResponse {
                            changes,
                            my_db_version,
                            your_last_db_version,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: resp_hmac,
                        } => {
                            // Verify HMAC if group key is configured
                            if let Some(ref gk) = self.group_key {
                                let tag = match resp_hmac {
                                    Some(t) => t,
                                    None => {
                                        log::debug!(
                                            "Rejecting unauthenticated sync response from peer {peer}"
                                        );
                                        return;
                                    }
                                };
                                let verify_resp =
                                    crate::protocol::SyncResponse::ChangesetResponse {
                                        changes: changes.clone(),
                                        my_db_version,
                                        your_last_db_version,
                                        site_id: peer_site_id,
                                        topic: peer_topic.clone(),
                                        hmac: None,
                                    };
                                if let Ok(bytes) = serde_json::to_vec(&verify_resp)
                                    && !gk.verify(&bytes, &tag)
                                {
                                    log::debug!(
                                        "Rejecting sync response with invalid HMAC from peer {peer}"
                                    );
                                    return;
                                }
                            }

                            // Ignore responses from peers on a different topic
                            if !peer_topic.is_empty() && peer_topic != self.topic_name {
                                log::debug!(
                                    "Ignoring sync response from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                                    self.topic_name
                                );
                                // Permanently reject this peer so mDNS won't re-add it
                                self.rejected_peers.insert(peer);
                                self.pending_sync_peers.remove(&peer);
                                self.peers.remove(&peer);
                                self.peer_db_versions.remove(&peer);
                                self.peer_reported_versions.remove(&peer);
                                self.emit_network_event(
                                    crate::network_status::NetworkEvent::PeerRejected(
                                        crate::network_status::PeerId(peer.to_string()),
                                    ),
                                );
                                self.update_network_status();
                                return;
                            }

                            // Update our knowledge of this peer's version
                            self.peer_db_versions.insert(peer, my_db_version);
                            let reported = self.peer_reported_versions.entry(peer).or_insert(0);
                            *reported = (*reported).max(my_db_version);
                            self.emit_network_event(
                                crate::network_status::NetworkEvent::PeerSynced {
                                    peer_id: crate::network_status::PeerId(peer.to_string()),
                                    db_version: my_db_version,
                                },
                            );
                            self.update_network_status();

                            // Update local db_version with Lamport semantics
                            let lamport_bump = if my_db_version > self.local_db_version {
                                self.local_db_version = my_db_version;
                                true
                            } else {
                                false
                            };

                            if changes.is_empty() {
                                log::info!(
                                    "Version vector sync with peer {peer}: already up to date"
                                );
                                // Still need to persist the Lamport bump even if no changes
                                if lamport_bump {
                                    let db = self.db.clone();
                                    tokio::spawn(async move {
                                        let _ = shadow::set_db_version(&db, my_db_version).await;
                                    });
                                }
                            } else {
                                log::info!(
                                    "Received {} changes from peer {peer} (their db_version: {})",
                                    changes.len(),
                                    my_db_version,
                                );

                                // Persist Lamport bump and peer version in a spawned task,
                                // but queue changeset for sequential application in main loop.
                                let db = self.db.clone();
                                let peer_str = peer.to_string();
                                tokio::spawn(async move {
                                    // Persist Lamport bump BEFORE applying changes so
                                    // increment_db_version reads the adjusted base value.
                                    if lamport_bump {
                                        let _ = shadow::set_db_version(&db, my_db_version).await;
                                    }

                                    // Persist peer version
                                    let _ = peer_tracker::upsert_peer_version(
                                        &db,
                                        &peer_str,
                                        &peer_site_id,
                                        my_db_version,
                                    )
                                    .await;
                                });

                                if let Err(e) = self.remote_changeset_tx.try_send(changes) {
                                    log::warn!(
                                        "Remote changeset queue full, dropping sync response: {e}"
                                    );
                                }
                            }
                        }
                        crate::protocol::SyncResponse::PushAck => {
                            log::debug!("Received PushAck from peer {peer}");
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                self.pending_sync_peers.remove(&peer);
                log::warn!("Sync request to {peer} failed: {error}");
                // Connection might be dead — re-dial if we know the peer's address
                if let Some(addr) = self.peers.get(&peer).cloned()
                    && !self.swarm.is_connected(&peer)
                {
                    log::info!("Re-dialing {peer} after outbound failure");
                    let _ = self.swarm.dial(addr);
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                log::warn!("Sync inbound from {peer} failed: {error}");
            }
            _ => {}
        }
    }

    fn handle_push_event(
        &mut self,
        event: request_response::Event<push_protocol::PushRequest, push_protocol::PushResponse>,
    ) {
        match event {
            request_response::Event::Message {
                message: request_response::Message::Response { response, .. },
                ..
            } => match response {
                push_protocol::PushResponse::Ok => {
                    log::debug!("Push request acknowledged by relay");
                }
                push_protocol::PushResponse::Error { message } => {
                    log::warn!("Push request error from relay: {message}");
                }
            },
            request_response::Event::OutboundFailure { error, .. } => {
                log::warn!("Push request failed: {error}");
            }
            _ => {}
        }
    }

    fn handle_auth_challenge(
        &mut self,
        event: request_response::Event<auth_protocol::AuthChallenge, auth_protocol::AuthResponse>,
    ) {
        use request_response::{Event, Message};
        if let Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } = event
        {
            let api_key = match &self.api_key {
                Some(k) => k.clone(),
                None => {
                    log::warn!("Received auth challenge from {peer} but no API key configured");
                    return;
                }
            };

            let nonce_sig = match self.keypair.sign(&request.nonce) {
                Ok(sig) => sig,
                Err(e) => {
                    log::error!("Failed to sign auth nonce: {e}");
                    return;
                }
            };

            let response = auth_protocol::AuthResponse { api_key, nonce_sig };
            if let Err(e) = self
                .swarm
                .behaviour_mut()
                .auth
                .send_response(channel, response)
            {
                log::error!("Failed to send auth response to relay {peer}: {e:?}");
            } else {
                log::info!("Auth response sent to relay {peer}");
            }
        }
    }

    fn handle_auth_result(
        &mut self,
        event: request_response::Event<auth_protocol::AuthResult, ()>,
    ) {
        use request_response::{Event, Message};
        if let Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } = event
        {
            // Ack immediately
            let _ = self
                .swarm
                .behaviour_mut()
                .auth_result
                .send_response(channel, ());

            if request.accepted {
                log::info!("Relay {peer} accepted auth — managed relay active");
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Connected,
                ));
            } else {
                let reason = request.reason.as_deref().unwrap_or("invalid API key");
                log::warn!("Relay {peer} rejected auth: {reason}");
                self.emit_network_event(crate::network_status::NetworkEvent::EngineFailed {
                    reason: format!("Relay auth rejected: {reason}"),
                });
            }
        }
    }

    /// Register push token with the relay server if we have one and are connected.
    fn maybe_register_push_token(&mut self, relay_peer_id: libp2p::PeerId) {
        if self.push_registered {
            return;
        }
        let (platform, token) = match &self.push_token {
            Some(pt) => pt.clone(),
            None => return,
        };

        let push_platform = match platform.as_str() {
            "Fcm" => push_protocol::PushPlatform::Fcm,
            "Apns" => push_protocol::PushPlatform::Apns,
            other => {
                log::warn!("Unknown push platform: {other}");
                return;
            }
        };

        let req = push_protocol::PushRequest::RegisterToken {
            topic: self.topic_name.clone(),
            platform: push_platform,
            token,
        };

        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
        self.push_registered = true;
        self.update_network_status();
        log::info!("Sent push token registration to relay {relay_peer_id}");
    }

    /// Send a NotifyTopic request to the relay after pushing changesets to peers.
    fn notify_relay_topic(&mut self) {
        let relay_peer_id = match &self.relay_state {
            RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } => *relay_peer_id,
            _ => return,
        };

        let req = push_protocol::PushRequest::NotifyTopic {
            topic: self.topic_name.clone(),
            sender_site_id: self
                .site_id
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        };

        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
    }
}

/// Apply a set of remote column changes to the local database.
async fn apply_remote_changeset(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    changes: &[ColumnChange],
) {
    // Increment local db_version once for the batch of remote changes
    let local_db_version = match shadow::increment_db_version(db).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to increment db_version: {e}");
            return;
        }
    };

    // Group changes by (table, pk) for efficient processing
    let mut grouped: HashMap<(&str, &str), Vec<&ColumnChange>> = HashMap::new();
    for change in changes {
        grouped
            .entry((&change.table, &change.pk))
            .or_default()
            .push(change);
    }

    for ((table, pk), row_changes) in &grouped {
        let meta = match registry.get(table) {
            Some(m) => m,
            None => {
                log::warn!("Rejecting remote changes for unregistered table: {}", table);
                continue;
            }
        };

        let mut any_applied = false;
        let mut changed_columns = Vec::new();
        let mut is_delete = false;

        // Check for delete first
        let delete_change = row_changes.iter().find(|c| c.cid == "__deleted");
        if let Some(change) = delete_change {
            let local_entries = shadow::get_clock_entries_for_row(db, table, pk)
                .await
                .unwrap_or_default();
            let local_max_cv = local_entries
                .iter()
                .map(|e| e.col_version)
                .max()
                .unwrap_or(0);

            if conflict::should_apply_delete(change.cl, local_max_cv, &meta.delete_policy) {
                let delete_sql = format!(
                    "DELETE FROM \"{}\" WHERE \"{}\" = $1",
                    table, meta.primary_key_column
                );
                if let Err(e) = db
                    .execute_raw(sea_orm::Statement::from_sql_and_values(
                        sea_orm::DatabaseBackend::Sqlite,
                        &delete_sql,
                        [pk.to_string().into()],
                    ))
                    .await
                {
                    log::error!("Failed to delete row {}/{}: {}", table, pk, e);
                    continue;
                }

                let _ = shadow::delete_clock_entries(db, table, pk).await;
                let _ = shadow::insert_tombstone(
                    db,
                    table,
                    pk,
                    change.col_version,
                    local_db_version,
                    &change.site_id,
                )
                .await;

                any_applied = true;
                is_delete = true;
            }
        } else {
            // Collect winning columns
            let exists = row_exists(db, table, &meta.primary_key_column, pk).await;
            let mut winning_columns: Vec<(String, sea_orm::Value)> = Vec::new();
            let mut pending_shadow_updates: Vec<(String, u64, crate::messages::NodeId, u32)> =
                Vec::new();

            for change in row_changes {
                let (local_cv, local_site) =
                    shadow::get_col_version_with_site(db, table, pk, &change.cid)
                        .await
                        .unwrap_or((0, [0u8; 16]));

                let remote_val_bytes = serde_json::to_vec(&change.val).unwrap_or_default();
                let remote_site = change.site_id;

                let should_apply = if local_cv == 0 {
                    true
                } else if change.col_version != local_cv {
                    // Clear winner by version — no need to fetch local value
                    change.col_version > local_cv
                } else {
                    // Tied col_version — need local value bytes for deterministic tiebreak
                    let local_val_bytes =
                        get_local_value_bytes(db, table, &meta.primary_key_column, pk, &change.cid)
                            .await;
                    conflict::should_apply_column(
                        change.col_version,
                        &remote_val_bytes,
                        &remote_site,
                        local_cv,
                        &local_val_bytes,
                        &local_site,
                    )
                };

                if should_apply {
                    winning_columns
                        .push((change.cid.clone(), json_to_sea_value(change.val.as_ref())));
                    changed_columns.push(change.cid.clone());

                    // Defer shadow write until after DB write succeeds
                    pending_shadow_updates.push((
                        change.cid.clone(),
                        change.col_version,
                        remote_site,
                        change.seq,
                    ));
                }
            }

            if !winning_columns.is_empty() {
                if exists {
                    // UPDATE each winning column
                    for (col, val) in &winning_columns {
                        let update_sql = format!(
                            "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                            table, col, meta.primary_key_column
                        );
                        if let Err(e) = db
                            .execute_raw(sea_orm::Statement::from_sql_and_values(
                                sea_orm::DatabaseBackend::Sqlite,
                                &update_sql,
                                [val.clone(), pk.to_string().into()],
                            ))
                            .await
                        {
                            log::error!("Failed to update column {}/{}/{}: {}", table, pk, col, e);
                        }
                    }
                    // Row existed — flush shadow writes
                    for (cid, cv, site, seq) in &pending_shadow_updates {
                        let _ = shadow::upsert_clock_entry(
                            db,
                            table,
                            pk,
                            cid,
                            *cv,
                            local_db_version,
                            site,
                            *seq,
                        )
                        .await;
                    }
                    any_applied = true;
                } else {
                    // INSERT OR IGNORE — silently skips if row was created by a concurrent task
                    let mut col_names = vec![format!("\"{}\"", meta.primary_key_column)];
                    let mut values: Vec<sea_orm::Value> = vec![pk.to_string().into()];

                    for (col, val) in &winning_columns {
                        if *col != meta.primary_key_column {
                            col_names.push(format!("\"{}\"", col));
                            values.push(val.clone());
                        }
                    }

                    let placeholders: Vec<String> =
                        (1..=values.len()).map(|i| format!("${}", i)).collect();

                    let insert_sql = format!(
                        "INSERT OR IGNORE INTO \"{}\" ({}) VALUES ({})",
                        table,
                        col_names.join(", "),
                        placeholders.join(", ")
                    );

                    let _ = db
                        .execute_raw(sea_orm::Statement::from_sql_and_values(
                            sea_orm::DatabaseBackend::Sqlite,
                            &insert_sql,
                            values,
                        ))
                        .await;

                    // UPDATE each winning column individually — works whether INSERT
                    // succeeded or was ignored due to concurrent insert
                    for (col, val) in &winning_columns {
                        if *col != meta.primary_key_column {
                            let update_sql = format!(
                                "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                                table, col, meta.primary_key_column
                            );
                            if let Err(e) = db
                                .execute_raw(sea_orm::Statement::from_sql_and_values(
                                    sea_orm::DatabaseBackend::Sqlite,
                                    &update_sql,
                                    [val.clone(), pk.to_string().into()],
                                ))
                                .await
                            {
                                log::error!(
                                    "Failed to update column {}/{}/{}: {}",
                                    table,
                                    pk,
                                    col,
                                    e
                                );
                            }
                        }
                    }

                    // Verify INSERT actually created the row before writing shadow
                    if row_exists(db, table, &meta.primary_key_column, pk).await {
                        for (cid, cv, site, seq) in &pending_shadow_updates {
                            let _ = shadow::upsert_clock_entry(
                                db,
                                table,
                                pk,
                                cid,
                                *cv,
                                local_db_version,
                                site,
                                *seq,
                            )
                            .await;
                        }
                        any_applied = true;
                    } else {
                        log::debug!(
                            "Row {}/{} not created (likely missing NOT NULL columns from \
                             out-of-order delivery), deferring shadow updates",
                            table,
                            pk
                        );
                        // Shadow stays clean — next sync cycle or full INSERT will succeed
                    }
                }
            }
        }

        if any_applied {
            let kind = if is_delete {
                WriteKind::Delete
            } else {
                WriteKind::Insert
            };
            let _ = change_tx.send(ChangeNotification {
                table: table.to_string(),
                kind,
                primary_key: pk.to_string(),
                changed_columns: if changed_columns.is_empty() {
                    None
                } else {
                    Some(changed_columns)
                },
            });
        }
    }
}

/// Check if a row exists in a table.
async fn row_exists(db: &DatabaseConnection, table: &str, pk_col: &str, pk: &str) -> bool {
    let sql = format!(
        "SELECT 1 FROM \"{}\" WHERE \"{}\" = $1 LIMIT 1",
        table, pk_col
    );
    db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .await
    .ok()
    .flatten()
    .is_some()
}

/// Convert a JSON value to a SeaORM value for parameterized queries.
fn json_to_sea_value(v: Option<&serde_json::Value>) -> sea_orm::Value {
    match v {
        None | Some(serde_json::Value::Null) => sea_orm::Value::String(None),
        Some(serde_json::Value::Bool(b)) => sea_orm::Value::Int(Some(if *b { 1 } else { 0 })),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                sea_orm::Value::BigInt(Some(i))
            } else if let Some(f) = n.as_f64() {
                sea_orm::Value::Double(Some(f))
            } else {
                sea_orm::Value::String(Some(n.to_string()))
            }
        }
        Some(serde_json::Value::String(s)) => sea_orm::Value::String(Some(s.clone())),
        Some(other) => sea_orm::Value::String(Some(other.to_string())),
    }
}

/// Fetch the current value of a column as JSON-serialized bytes for conflict tiebreaking.
async fn get_local_value_bytes(
    db: &DatabaseConnection,
    table: &str,
    pk_col: &str,
    pk: &str,
    cid: &str,
) -> Vec<u8> {
    let sql = format!(
        "SELECT json_object('v', \"{}\") as json_val FROM \"{}\" WHERE \"{}\" = $1",
        cid, table, pk_col
    );
    let result = db
        .query_one_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            &sql,
            [pk.into()],
        ))
        .await
        .ok()
        .flatten();

    match result {
        Some(qr) => {
            let raw: Option<String> = qr.try_get("", "json_val").ok();
            let val = raw.and_then(|s| {
                let obj: serde_json::Value = serde_json::from_str(&s).ok()?;
                Some(obj.get("v")?.clone())
            });
            serde_json::to_vec(&val).unwrap_or_default()
        }
        None => serde_json::to_vec(&Option::<serde_json::Value>::None).unwrap_or_default(),
    }
}

/// Strip the RETURNING clause from a SQL statement.
#[cfg(test)]
fn strip_returning(sql: &str) -> String {
    if let Some(pos) = sql.to_ascii_uppercase().rfind(" RETURNING ") {
        sql[..pos].to_string()
    } else {
        sql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::ColumnChange;
    use crate::registry::TableMeta;
    use sea_orm::Database;
    use std::sync::Arc;

    async fn setup_engine_test_db() -> (sea_orm::DatabaseConnection, Arc<TableRegistry>) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::shadow::create_meta_table(&db).await.unwrap();
        crate::peer_tracker::create_peer_versions_table(&db)
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)"
        ).await.unwrap();
        crate::shadow::create_shadow_table(&db, "tasks")
            .await
            .unwrap();
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
        (db, registry)
    }

    // ── strip_returning tests ──

    #[test]
    fn test_strip_returning_with_clause() {
        let result = strip_returning(r#"INSERT INTO "tasks" ("id") VALUES ('1') RETURNING "id""#);
        assert!(
            !result.contains("RETURNING"),
            "Expected RETURNING clause to be stripped, got: {result}"
        );
    }

    #[test]
    fn test_strip_returning_without_clause() {
        let input = r#"INSERT INTO "tasks" ("id") VALUES ('1')"#;
        let result = strip_returning(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_strip_returning_case_insensitive() {
        let result = strip_returning(r#"INSERT INTO "tasks" ("id") VALUES ('1') Returning "id""#);
        assert!(
            !result.contains("Returning"),
            "Expected case-insensitive RETURNING clause to be stripped, got: {result}"
        );
    }

    // ── apply_remote_changeset tests ──

    #[tokio::test]
    async fn test_apply_remote_changeset_unregistered_table() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![ColumnChange {
            table: "unknown".to_string(),
            pk: "1".to_string(),
            cid: "col".to_string(),
            val: Some(serde_json::json!("value")),
            site_id: [2u8; 16],
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification for unregistered table"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_insert() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![
            ColumnChange {
                table: "tasks".to_string(),
                pk: "test-1".to_string(),
                cid: "id".to_string(),
                val: Some(serde_json::json!("test-1")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "test-1".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Test Task")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "test-1".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(0)),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "test-1");

        // Verify row exists
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'test-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Test Task");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert a row first
        db.execute_unprepared("INSERT INTO tasks VALUES ('del-1', 'To Delete', 0)")
            .await
            .unwrap();

        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "del-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [2u8; 16],
            col_version: 10,
            cl: 10,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "del-1");

        // Verify row is deleted
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) as cnt FROM tasks WHERE id = 'del-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let count: i32 = result.try_get("", "cnt").unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_column_conflict_higher_version_wins() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data
        db.execute_unprepared("INSERT INTO tasks VALUES ('c-1', 'Original', 0)")
            .await
            .unwrap();
        // Set local clock entry with version 5
        crate::shadow::upsert_clock_entry(&db, "tasks", "c-1", "title", 5, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote change with higher version (10) should win
        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "c-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("Remote Winner")),
            site_id: [2u8; 16],
            col_version: 10,
            cl: 10,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'c-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Remote Winner");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_lower_version_loses() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data and set local clock high
        db.execute_unprepared("INSERT INTO tasks VALUES ('lv-1', 'Local', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db, "tasks", "lv-1", "title", 10, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote change with LOWER version (3) should lose
        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "lv-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("Remote Loser")),
            site_id: [2u8; 16],
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'lv-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(
            title, "Local",
            "Lower version remote should not overwrite local"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_different_columns_both_survive() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data
        db.execute_unprepared("INSERT INTO tasks VALUES ('dc-1', 'Original', 0)")
            .await
            .unwrap();
        // Set local clock for title only
        crate::shadow::upsert_clock_entry(&db, "tasks", "dc-1", "title", 1, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote changes: higher version for title, and a new column "done"
        let changes = vec![
            ColumnChange {
                table: "tasks".to_string(),
                pk: "dc-1".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Remote Title")),
                site_id: [2u8; 16],
                col_version: 5,
                cl: 5,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "dc-1".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(1)),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
        ];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title, done FROM tasks WHERE id = 'dc-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        let done: i32 = result.try_get_by_index(1).unwrap();
        assert_eq!(title, "Remote Title");
        assert_eq!(done, 1, "Both columns should be updated");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete_lower_cl_rejected() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks VALUES ('dlcl-1', 'Keep Me', 0)")
            .await
            .unwrap();
        // Set a high local clock
        crate::shadow::upsert_clock_entry(&db, "tasks", "dlcl-1", "title", 10, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote delete with low causal length — should be rejected
        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "dlcl-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [2u8; 16],
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        // Row should still exist
        let exists = row_exists(&db, "tasks", "id", "dlcl-1").await;
        assert!(
            exists,
            "Row should NOT be deleted when remote cl < local max cv"
        );
        assert!(
            rx.try_recv().is_err(),
            "No notification for rejected delete"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete_wins_on_tie() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks VALUES ('dw-1', 'Tie Delete', 0)")
            .await
            .unwrap();
        // Set local clock to 5
        crate::shadow::upsert_clock_entry(&db, "tasks", "dw-1", "title", 5, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote delete with cl=5 (tie) — DeleteWins policy (default)
        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "dw-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [2u8; 16],
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let exists = row_exists(&db, "tasks", "id", "dw-1").await;
        assert!(!exists, "DeleteWins: tie should delete the row");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_add_wins_on_tie() {
        let (db, _) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Register with AddWins policy
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::AddWins,
        });

        db.execute_unprepared("INSERT INTO tasks VALUES ('aw-1', 'Tie Keep', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db, "tasks", "aw-1", "title", 5, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Remote delete with cl=5 (tie) — AddWins policy
        let changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "aw-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [2u8; 16],
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        let exists = row_exists(&db, "tasks", "id", "aw-1").await;
        assert!(exists, "AddWins: tie should keep the row");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_insert_after_delete() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert then delete locally
        db.execute_unprepared("INSERT INTO tasks VALUES ('iad-1', 'Deleted', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db, "tasks", "iad-1", "title", 1, 1, &[1u8; 16], 0)
            .await
            .unwrap();

        // Apply remote delete
        let delete_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "iad-1".to_string(),
            cid: "__deleted".to_string(),
            val: None,
            site_id: [2u8; 16],
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(&db, &tx, &registry, &delete_changes).await;
        assert!(!row_exists(&db, "tasks", "id", "iad-1").await);

        // Now apply remote insert with higher versions (N3 regression)
        let insert_changes = vec![
            ColumnChange {
                table: "tasks".to_string(),
                pk: "iad-1".to_string(),
                cid: "id".to_string(),
                val: Some(serde_json::json!("iad-1")),
                site_id: [3u8; 16],
                col_version: 10,
                cl: 10,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "iad-1".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Reinserted")),
                site_id: [3u8; 16],
                col_version: 10,
                cl: 10,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "iad-1".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(0)),
                site_id: [3u8; 16],
                col_version: 10,
                cl: 10,
                seq: 2,
                db_version: 0,
            },
        ];
        apply_remote_changeset(&db, &tx, &registry, &insert_changes).await;

        assert!(
            row_exists(&db, "tasks", "id", "iad-1").await,
            "Row should reappear after re-insert"
        );
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'iad-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Reinserted");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_multiple_rows() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-1".to_string(),
                cid: "id".to_string(),
                val: Some(serde_json::json!("mr-1")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-1".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Row 1")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-1".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(0)),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-2".to_string(),
                cid: "id".to_string(),
                val: Some(serde_json::json!("mr-2")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-2".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Row 2")),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "mr-2".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(1)),
                site_id: [2u8; 16],
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        assert!(row_exists(&db, "tasks", "id", "mr-1").await);
        assert!(row_exists(&db, "tasks", "id", "mr-2").await);
    }

    #[tokio::test]
    async fn test_convergence_tied_col_version() {
        // Two peers insert the same PK offline, both at col_version=1.
        // Deterministic tiebreaker must produce the same winner on both sides.
        let site_a: crate::messages::NodeId = [1u8; 16];
        let site_b: crate::messages::NodeId = [2u8; 16]; // B > A

        // Simulate Peer A's DB: has A's data locally, receives B's data
        let (db_a, registry_a) = setup_engine_test_db().await;
        let (tx_a, _rx_a) = broadcast::channel::<ChangeNotification>(16);

        // A wrote locally: title="A-value"
        db_a.execute_unprepared("INSERT INTO tasks VALUES ('tied-1', 'A-value', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "id", 1, 1, &site_a, 0)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "title", 1, 1, &site_a, 1)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "done", 1, 1, &site_a, 2)
            .await
            .unwrap();

        // B's changes arrive at A (same col_version=1, different value)
        let b_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "tied-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("B-value")),
            site_id: site_b,
            col_version: 1,
            cl: 1,
            seq: 1,
            db_version: 0,
        }];
        apply_remote_changeset(&db_a, &tx_a, &registry_a, &b_changes).await;

        // Simulate Peer B's DB: has B's data locally, receives A's data
        let (db_b, registry_b) = setup_engine_test_db().await;
        let (tx_b, _rx_b) = broadcast::channel::<ChangeNotification>(16);

        // B wrote locally: title="B-value"
        db_b.execute_unprepared("INSERT INTO tasks VALUES ('tied-1', 'B-value', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "id", 1, 1, &site_b, 0)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "title", 1, 1, &site_b, 1)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "done", 1, 1, &site_b, 2)
            .await
            .unwrap();

        // A's changes arrive at B
        let a_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "tied-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("A-value")),
            site_id: site_a,
            col_version: 1,
            cl: 1,
            seq: 1,
            db_version: 0,
        }];
        apply_remote_changeset(&db_b, &tx_b, &registry_b, &a_changes).await;

        // Both peers should converge to the same value
        let result_a = db_a
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'tied-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_a: String = result_a.try_get_by_index(0).unwrap();

        let result_b = db_b
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'tied-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_b: String = result_b.try_get_by_index(0).unwrap();

        assert_eq!(
            title_a, title_b,
            "Both peers must converge to the same value (got A='{}', B='{}')",
            title_a, title_b
        );
    }

    #[tokio::test]
    async fn test_convergence_offline_diverged_updates() {
        // Two peers update the same column offline (different col_versions).
        // Higher col_version must win deterministically.
        let site_a: crate::messages::NodeId = [1u8; 16];
        let site_b: crate::messages::NodeId = [2u8; 16];

        // Peer A: updated title twice (col_version=3)
        let (db_a, registry_a) = setup_engine_test_db().await;
        let (tx_a, _rx_a) = broadcast::channel::<ChangeNotification>(16);

        db_a.execute_unprepared("INSERT INTO tasks VALUES ('div-1', 'A-latest', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "div-1", "title", 3, 3, &site_a, 0)
            .await
            .unwrap();

        // Peer B: updated title once (col_version=2)
        let (db_b, registry_b) = setup_engine_test_db().await;
        let (tx_b, _rx_b) = broadcast::channel::<ChangeNotification>(16);

        db_b.execute_unprepared("INSERT INTO tasks VALUES ('div-1', 'B-latest', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "div-1", "title", 2, 2, &site_b, 0)
            .await
            .unwrap();

        // B's changes arrive at A (col_version=2 < 3, should lose)
        let b_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "div-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("B-latest")),
            site_id: site_b,
            col_version: 2,
            cl: 2,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(&db_a, &tx_a, &registry_a, &b_changes).await;

        // A's changes arrive at B (col_version=3 > 2, should win)
        let a_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "div-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("A-latest")),
            site_id: site_a,
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(&db_b, &tx_b, &registry_b, &a_changes).await;

        // Both should converge to A-latest (higher col_version)
        let result_a = db_a
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'div-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_a: String = result_a.try_get_by_index(0).unwrap();

        let result_b = db_b
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'div-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_b: String = result_b.try_get_by_index(0).unwrap();

        assert_eq!(title_a, "A-latest", "A should keep its value (higher cv)");
        assert_eq!(title_b, "A-latest", "B should accept A's value (higher cv)");
        assert_eq!(title_a, title_b, "Both peers must converge");
    }

    async fn setup_engine_test_db_no_defaults() -> (sea_orm::DatabaseConnection, Arc<TableRegistry>)
    {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::shadow::create_meta_table(&db).await.unwrap();
        crate::peer_tracker::create_peer_versions_table(&db)
            .await
            .unwrap();
        // No DEFAULT on any NOT NULL column — INSERT missing columns will fail
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL)",
        )
        .await
        .unwrap();
        crate::shadow::create_shadow_table(&db, "tasks")
            .await
            .unwrap();
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
        (db, registry)
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_out_of_order_update_before_insert() {
        // UPDATE arrives for a non-existent row → shadow must stay clean.
        // Then INSERT arrives → row is created with all columns, shadow populated.
        let (db, registry) = setup_engine_test_db_no_defaults().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);
        let site = [2u8; 16];

        // Step 1: Send UPDATE for non-existent row (only "title" column, cv=2)
        let update_changes = vec![ColumnChange {
            table: "tasks".to_string(),
            pk: "ooo-1".to_string(),
            cid: "title".to_string(),
            val: Some(serde_json::json!("Updated Title")),
            site_id: site,
            col_version: 2,
            cl: 2,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(&db, &tx, &registry, &update_changes).await;

        // Row should NOT exist
        assert!(
            !row_exists(&db, "tasks", "id", "ooo-1").await,
            "Row should not exist after out-of-order UPDATE"
        );

        // Shadow should be clean (no entries for this row)
        let (cv, _) = crate::shadow::get_col_version_with_site(&db, "tasks", "ooo-1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 0, "Shadow should be clean after failed INSERT");

        // No notification should have been sent
        assert!(
            rx.try_recv().is_err(),
            "No notification expected for failed out-of-order UPDATE"
        );

        // Step 2: Send full INSERT (all columns, cv=1)
        let insert_changes = vec![
            ColumnChange {
                table: "tasks".to_string(),
                pk: "ooo-1".to_string(),
                cid: "id".to_string(),
                val: Some(serde_json::json!("ooo-1")),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "ooo-1".to_string(),
                cid: "title".to_string(),
                val: Some(serde_json::json!("Original Title")),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".to_string(),
                pk: "ooo-1".to_string(),
                cid: "done".to_string(),
                val: Some(serde_json::json!(0)),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];
        apply_remote_changeset(&db, &tx, &registry, &insert_changes).await;

        // Row should now exist with INSERT's values (shadow was clean, so cv=1 wins)
        assert!(
            row_exists(&db, "tasks", "id", "ooo-1").await,
            "Row should exist after INSERT"
        );

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'ooo-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(
            title, "Original Title",
            "INSERT's value should win since shadow was clean"
        );

        // Shadow should now be populated with cv=1
        let (cv, _) = crate::shadow::get_col_version_with_site(&db, "tasks", "ooo-1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 1, "Shadow should have cv=1 from the INSERT");

        // Notification should have been sent for the INSERT
        let notif = rx.try_recv().expect("Expected notification for INSERT");
        assert_eq!(notif.primary_key, "ooo-1");
    }

    #[tokio::test]
    async fn test_get_col_version_with_site() {
        let (db, _registry) = setup_engine_test_db().await;
        let site_id = [42u8; 16];

        // No entry yet
        let (cv, sid) = crate::shadow::get_col_version_with_site(&db, "tasks", "pk1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 0);
        assert_eq!(sid, [0u8; 16]);

        // Insert a clock entry
        crate::shadow::upsert_clock_entry(&db, "tasks", "pk1", "title", 5, 1, &site_id, 0)
            .await
            .unwrap();

        let (cv, sid) = crate::shadow::get_col_version_with_site(&db, "tasks", "pk1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 5);
        assert_eq!(sid, site_id);
    }
}
