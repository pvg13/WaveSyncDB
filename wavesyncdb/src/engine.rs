//! P2P sync engine powered by libp2p.
//!
//! The engine runs as a background tokio task, managing a libp2p swarm with:
//! - **gossipsub** for real-time push of column-level CRDT changesets
//! - **mDNS** for local peer discovery
//! - **QUIC + TCP** transports with Noise encryption and Yamux multiplexing
//! - **request-response** for version vector based catch-up sync
//! - **relay + dcutr + autonat** for NAT traversal (WIP)
//!
//! Local write operations arrive via an mpsc channel from [`WaveSyncDb`](crate::WaveSyncDb)
//! as [`SyncChangeset`]s and are published to gossipsub.
//! Incoming remote changesets are applied column-by-column using per-column
//! Lamport clocks for conflict resolution.

pub(crate) mod behaviour;
pub(crate) mod push_protocol;
pub(crate) mod snapshot_protocol;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use behaviour::{WaveSyncBehaviour, WaveSyncBehaviourEvent};
use futures::FutureExt;
use libp2p::{
    Multiaddr, SwarmBuilder, autonat, dcutr, dns,
    futures::StreamExt,
    gossipsub::{self, IdentTopic},
    identify, identity, mdns, noise, ping, relay, rendezvous, request_response,
    swarm::SwarmEvent,
    yamux,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use std::panic::AssertUnwindSafe;
use tokio::sync::{Notify, broadcast, mpsc};

use crate::auth::GroupKey;
use crate::conflict;
use crate::messages::{
    AuthenticatedMessage, ChangeNotification, ColumnChange, NodeId, SyncChangeset, SyncMessage,
    WriteKind,
};
use crate::peer_tracker;
use crate::protocol::SyncRequest;
use crate::registry::TableRegistry;
use crate::shadow;

/// Commands sent from the application to the P2P engine.
#[derive(Debug)]
pub enum EngineCommand {
    /// App resumed from background — clear stale peers, restart mDNS, re-sync.
    Resume,
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
                let _ = event_tx.send(crate::network_status::NetworkEvent::EngineFailed {
                    reason,
                });
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
            Ok(builder
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
                .build())
        }
        Err(e) => {
            log::warn!(
                "System DNS resolver failed (expected on Android): {e}. \
                 Falling back to Google public DNS."
            );
            let mdns_cfg = mdns_config;
            Ok(SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_tcp(
                    Default::default(),
                    noise::Config::new,
                    yamux::Config::default,
                )?
                .with_quic()
                .with_dns_config(
                    dns::ResolverConfig::google(),
                    dns::ResolverOpts::default(),
                )
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
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

    let swarm = build_swarm(keypair.clone(), mdns_config)?;

    let local_peer_id = keypair.public().to_peer_id();

    // Load current db_version
    let local_db_version = shadow::get_db_version(&db).await?;

    let (snapshot_resp_tx, snapshot_resp_rx) = mpsc::channel::<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>(8);

    let effective_topic = match &group_key {
        Some(gk) => gk.derive_topic(&topic_name),
        None => topic_name.clone(),
    };

    // Determine rendezvous namespace (same as effective_topic — already PSK-derived)
    let rendezvous_namespace = effective_topic.clone();

    let push_token = config.push_token.clone();

    let mut engine = EngineRunner {
        swarm,
        peers: HashMap::new(),
        topic: gossipsub::IdentTopic::new(&effective_topic),
        db,
        change_tx,
        registry,
        local_peer_id,
        site_id,
        topic_name: effective_topic,
        config,
        local_db_version,
        peer_db_versions: HashMap::new(),
        snapshot_resp_tx,
        snapshot_resp_rx,
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
        pending_sync_peers: std::collections::HashSet::new(),
        push_token,
        push_registered: false,
        network_status,
        network_event_tx,
        resume_sync_deadline: None,
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
    /// TCP/QUIC connection established with relay peer.
    Connected { relay_peer_id: libp2p::PeerId },
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
    topic: IdentTopic,
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    local_peer_id: libp2p::PeerId,
    site_id: NodeId,
    topic_name: String,
    config: EngineConfig,
    local_db_version: u64,
    peer_db_versions: HashMap<libp2p::PeerId, u64>,
    snapshot_resp_tx: mpsc::Sender<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    snapshot_resp_rx: mpsc::Receiver<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
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
}

impl EngineRunner {
    /// Rebuild the full network status snapshot from internal state.
    fn update_network_status(&self) {
        use crate::network_status as ns;

        let connected_peers = self
            .peers
            .iter()
            .map(|(peer_id, addr)| ns::PeerInfo {
                peer_id: ns::PeerId(peer_id.to_string()),
                address: addr.to_string(),
                db_version: self.peer_db_versions.get(peer_id).copied(),
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

        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&self.topic)?;

        // If a relay server is configured, dial it
        if let Some(ref relay_addr) = self.config.relay_server.clone() {
            if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                log::warn!("Failed to dial relay server {}: {}", relay_addr, e);
            } else {
                log::info!("Dialing relay server: {}", relay_addr);
                self.relay_state = RelayState::Connecting { retry_count: 0 };
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RelayStatusChanged(
                        crate::network_status::RelayStatus::Connecting,
                    ),
                );
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
            tokio::time::Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
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
            EngineCommand::RequestFullSync => {
                log::info!("Full sync requested by user");
                // Reset peer versions to trigger full re-sync
                self.peer_db_versions.clear();
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
                if let RelayState::Connected { relay_peer_id }
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

        // Clear pending sync to allow fresh requests
        self.pending_sync_peers.clear();

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
        self.resume_sync_deadline =
            Some(tokio::time::Instant::now() + Duration::from_secs(2));
    }

    async fn handle_local_changeset(&mut self, changeset: SyncChangeset) {
        // Update local db_version
        self.local_db_version = self.local_db_version.max(changeset.db_version);
        self.update_network_status();

        let msg = SyncMessage::Changeset(changeset);
        let hmac = match &self.group_key {
            Some(gk) => {
                let inner_bytes = match serde_json::to_vec(&msg) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("Failed to serialize changeset: {}", e);
                        return;
                    }
                };
                Some(gk.mac(&inner_bytes))
            }
            None => None,
        };
        let wrapper = AuthenticatedMessage { inner: msg, hmac };
        match serde_json::to_vec(&wrapper) {
            Ok(data) => {
                const MAX_GOSSIPSUB_MSG_SIZE: usize = 1024 * 1024;
                if data.len() > MAX_GOSSIPSUB_MSG_SIZE {
                    log::warn!(
                        "Dropping oversized gossipsub message ({} bytes, max {})",
                        data.len(),
                        MAX_GOSSIPSUB_MSG_SIZE
                    );
                    return;
                }
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(self.topic.clone(), data)
                {
                    log::warn!("Failed to publish to gossipsub: {}", e);
                    self.trigger_rediscovery();
                    self.sync_all_known_peers().await;
                } else {
                    // Notify relay to send push notifications to sleeping mobile peers
                    self.notify_relay_topic();
                }
            }
            Err(e) => {
                log::error!("Failed to serialize changeset: {}", e);
            }
        }
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
                    if matches!(self.relay_state, RelayState::Connecting { .. }) {
                        if let Some(ref relay_addr) = self.config.relay_server {
                            log::info!(
                                "New listen address detected, attempting relay reconnection"
                            );
                            if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                                log::warn!("Relay redial on new address failed: {e}");
                            }
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
                log::debug!("Ping event with {peer:?} over {connection:?}: {result:?}");
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Mdns(event)) => {
                self.handle_mdns(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Gossipsub(event)) => {
                self.handle_gossipsub(event);
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
                    self.relay_state = RelayState::Connected {
                        relay_peer_id: peer_id,
                    };
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connected,
                        ),
                    );
                    self.update_network_status();
                    // Relay circuit listen is deferred until AutoNAT detects private NAT,
                    // to avoid a premature reservation failure killing gossipsub protocols.

                    // Register push token with relay if configured
                    self.maybe_register_push_token(peer_id);
                }

                // If this is a rendezvous server, register + discover
                if let Some(ref rendezvous_addr) = self.config.rendezvous_server
                    && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                        rendezvous_addr.iter().last()
                    && peer_id == rv_peer_id
                {
                    log::info!("Connected to rendezvous server {peer_id}");
                    self.rendezvous_register(peer_id);
                    self.rendezvous_discover();
                }

                // If this is a bootstrap peer, add to peers and initiate sync
                if self.bootstrap_peers.contains(&peer_id) {
                    let addr = endpoint.get_remote_address().clone();
                    self.peers.insert(peer_id, addr.clone());
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer_id);

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

                if self.peers.contains_key(&peer_id) && self.registry_is_ready {
                    // Refresh local_db_version in case spawned tasks updated it
                    self.local_db_version = shadow::get_db_version(&self.db)
                        .await
                        .unwrap_or(self.local_db_version);
                    self.initiate_sync_for_peer(peer_id);
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                log::debug!("Connection closed with {peer_id}");

                // Clean pending_sync_peers so the peer can be re-synced on reconnect
                self.pending_sync_peers.remove(&peer_id);

                // If relay server disconnected, update state
                if let RelayState::Connected { relay_peer_id }
                | RelayState::Listening { relay_peer_id } = &self.relay_state
                    && peer_id == *relay_peer_id
                {
                    log::warn!("Lost connection to relay server {peer_id}");
                    self.relay_state = RelayState::Connecting { retry_count: 0 };
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
                if let Some(ref rv_addr) = self.config.rendezvous_server
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
            }
            SwarmEvent::ListenerClosed { reason, .. } => {
                if let Err(ref e) = reason {
                    log::warn!("Listener closed with error: {e}");
                }
                // If relay was in Listening state, reset to Connected so the next
                // AutoNAT probe can re-trigger the reservation
                if let RelayState::Listening { relay_peer_id } = self.relay_state {
                    log::warn!("Relay listener closed, resetting to Connected state");
                    self.relay_state = RelayState::Connected { relay_peer_id };
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connected,
                        ),
                    );
                    self.update_network_status();
                }
                // Re-subscribe to gossipsub topic to restore protocol advertisement
                // in case the listener closure removed it
                if let Err(e) =
                    self.swarm.behaviour_mut().gossipsub.subscribe(&self.topic)
                {
                    log::warn!("Failed to re-subscribe gossipsub topic: {e}");
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                log::warn!("Listen address expired: {address}");
                // Re-subscribe gossipsub to restore protocol advertisement
                if let Err(e) =
                    self.swarm.behaviour_mut().gossipsub.subscribe(&self.topic)
                {
                    log::warn!("Failed to re-subscribe gossipsub topic: {e}");
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                log::warn!("Outgoing connection error to {peer_id:?}: {error}");
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

        let peer_ids: Vec<libp2p::PeerId> = self.peers.keys().cloned().collect();
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
                let SyncRequest::VersionVector { ref mut hmac, .. } = req;
                *hmac = Some(tag);
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
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer_id);

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
                    self.peer_db_versions.remove(&peer_id);
                    self.pending_sync_peers.remove(&peer_id);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                    self.emit_network_event(crate::network_status::NetworkEvent::PeerDisconnected(
                        crate::network_status::PeerId(peer_id.to_string()),
                    ));
                    self.update_network_status();
                }
            }
        }
    }

    fn handle_gossipsub(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                log::info!(
                    "Received message on topic {} from peer {propagation_source:?}: {message_id:?}",
                    message.topic
                );

                let auth_msg: AuthenticatedMessage = match serde_json::from_slice(&message.data) {
                    Ok(msg) => msg,
                    Err(e) => {
                        log::error!(
                            "Failed to deserialize message from peer {}: {}",
                            propagation_source,
                            e
                        );
                        return;
                    }
                };

                // Verify HMAC if group key is configured
                if let Some(ref gk) = self.group_key {
                    let tag = match auth_msg.hmac {
                        Some(t) => t,
                        None => {
                            log::debug!(
                                "Rejecting unauthenticated gossipsub message from peer {}",
                                propagation_source
                            );
                            return;
                        }
                    };
                    let inner_bytes = match serde_json::to_vec(&auth_msg.inner) {
                        Ok(b) => b,
                        Err(e) => {
                            log::error!("Failed to re-serialize inner message: {}", e);
                            return;
                        }
                    };
                    if !gk.verify(&inner_bytes, &tag) {
                        log::debug!(
                            "Rejecting gossipsub message with invalid HMAC from peer {}",
                            propagation_source
                        );
                        return;
                    }
                }

                match auth_msg.inner {
                    SyncMessage::Changeset(changeset) => {
                        log::info!(
                            "Received changeset from peer {} with {} changes at db_version {}",
                            propagation_source,
                            changeset.changes.len(),
                            changeset.db_version,
                        );

                        let db = self.db.clone();
                        let change_tx = self.change_tx.clone();
                        let registry = self.registry.clone();
                        tokio::spawn(async move {
                            apply_remote_changeset(&db, &change_tx, &registry, &changeset.changes)
                                .await;
                        });
                    }
                }
            }
            _ => {
                log::debug!("Other gossipsub event: {:?}", event);
            }
        }
    }

    fn handle_relay_client(&mut self, event: relay::client::Event) {
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } => {
                log::info!("Relay reservation accepted by {relay_peer_id}");
                self.relay_state = RelayState::Listening { relay_peer_id };
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RelayStatusChanged(
                        crate::network_status::RelayStatus::Listening,
                    ),
                );
                self.update_network_status();
            }
            _ => {
                log::debug!("Relay client event: {:?}", event);
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
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::NatStatusChanged(
                            crate::network_status::NatStatus::Public,
                        ),
                    );
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
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::NatStatusChanged(
                            crate::network_status::NatStatus::Private,
                        ),
                    );
                    self.update_network_status();
                }
                // If behind NAT and relay is configured but not yet listening, trigger reservation
                if matches!(self.relay_state, RelayState::Connected { .. })
                    && let RelayState::Connected { relay_peer_id } = self.relay_state
                    && let Some(ref relay_addr) = self.config.relay_server
                {
                    let circuit_addr = relay_addr
                        .clone()
                        .with(libp2p::multiaddr::Protocol::P2pCircuit);
                    if let Err(e) = self.swarm.listen_on(circuit_addr.clone()) {
                        log::warn!("Failed to listen on relay circuit: {}", e);
                    } else {
                        log::info!(
                            "NAT detected as private, listening on relay circuit: {}",
                            circuit_addr
                        );
                        self.relay_state = RelayState::Connected { relay_peer_id };
                    }
                }
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
                        if !self.swarm.is_connected(&peer_id)
                            && let Err(e) = self.swarm.dial(addr.clone())
                        {
                            log::warn!("Failed to dial rendezvous peer {peer_id}: {e}");
                        }
                        self.peers.insert(peer_id, addr.clone());
                        self.swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer_id);

                        self.emit_network_event(
                            crate::network_status::NetworkEvent::PeerConnected(
                                crate::network_status::PeerInfo {
                                    peer_id: crate::network_status::PeerId(peer_id.to_string()),
                                    address: addr.to_string(),
                                    db_version: None,
                                    is_bootstrap: false,
                                    is_group_member: true,
                                },
                            ),
                        );
                        self.update_network_status();

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
            Some(self.config.rendezvous_ttl),
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

        // Always re-register: handles TTL expiry (server forgets after rendezvous_ttl)
        // and stale rendezvous_registered state after silent disconnects
        self.rendezvous_register(server_peer_id);

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
                    log::info!("Attempting relay reconnection (attempt {})", count + 1);
                    if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                        log::warn!("Failed to redial relay server: {}", e);
                    }
                    self.relay_state = RelayState::Connecting {
                        retry_count: count + 1,
                    };
                }
            }
            RelayState::Disabled | RelayState::Connected { .. } | RelayState::Listening { .. } => {
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
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .remove_explicit_peer(&peer);
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
                                    let crate::protocol::SyncResponse::ChangesetResponse {
                                        ref mut hmac,
                                        ..
                                    } = resp;
                                    *hmac = Some(tag);
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
                                self.swarm
                                    .behaviour_mut()
                                    .gossipsub
                                    .remove_explicit_peer(&peer);
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

                                let db = self.db.clone();
                                let change_tx = self.change_tx.clone();
                                let registry = self.registry.clone();
                                let peer_str = peer.to_string();
                                tokio::spawn(async move {
                                    // Persist Lamport bump BEFORE applying changes so
                                    // increment_db_version reads the adjusted base value.
                                    if lamport_bump {
                                        let _ = shadow::set_db_version(&db, my_db_version).await;
                                    }

                                    apply_remote_changeset(&db, &change_tx, &registry, &changes)
                                        .await;

                                    // Persist peer version
                                    let _ = peer_tracker::upsert_peer_version(
                                        &db,
                                        &peer_str,
                                        &peer_site_id,
                                        my_db_version,
                                    )
                                    .await;
                                });
                            }
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

    /// Send a NotifyTopic request to the relay after successful gossipsub publish.
    fn notify_relay_topic(&mut self) {
        let relay_peer_id = match &self.relay_state {
            RelayState::Connected { relay_peer_id } | RelayState::Listening { relay_peer_id } => {
                *relay_peer_id
            }
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

            for change in row_changes {
                let local_cv = shadow::get_col_version(db, table, pk, &change.cid)
                    .await
                    .unwrap_or(0);

                let remote_val_bytes = serde_json::to_vec(&change.val).unwrap_or_default();
                let remote_site = change.site_id;

                let should_apply = local_cv == 0
                    || conflict::should_apply_column(
                        change.col_version,
                        &remote_val_bytes,
                        &remote_site,
                        local_cv,
                        &[], // empty = lose tiebreak (remote wins on equal cv)
                        &[0u8; 16],
                    );

                if should_apply {
                    winning_columns
                        .push((change.cid.clone(), json_to_sea_value(change.val.as_ref())));
                    changed_columns.push(change.cid.clone());

                    // Update shadow table
                    let _ = shadow::upsert_clock_entry(
                        db,
                        table,
                        pk,
                        &change.cid,
                        change.col_version,
                        local_db_version,
                        &remote_site,
                        change.seq,
                    )
                    .await;
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
                    any_applied = true;
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
        assert_eq!(title, "Local", "Lower version remote should not overwrite local");
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
        }];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        // Row should still exist
        let exists = row_exists(&db, "tasks", "id", "dlcl-1").await;
        assert!(exists, "Row should NOT be deleted when remote cl < local max cv");
        assert!(rx.try_recv().is_err(), "No notification for rejected delete");
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
            },
        ];
        apply_remote_changeset(&db, &tx, &registry, &insert_changes).await;

        assert!(row_exists(&db, "tasks", "id", "iad-1").await, "Row should reappear after re-insert");
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
            },
        ];

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

        assert!(row_exists(&db, "tasks", "id", "mr-1").await);
        assert!(row_exists(&db, "tasks", "id", "mr-2").await);
    }
}
