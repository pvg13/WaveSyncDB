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
pub(crate) mod command_handler;
pub(crate) mod identity_handler;
pub(crate) mod peer_manager;
pub(crate) mod push_protocol;
pub(crate) mod relay_manager;
pub(crate) mod snapshot_protocol;
pub(crate) mod sync_handler;

use sync_handler::apply_remote_changeset;

use std::collections::{HashMap, VecDeque};
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
    /// Set or clear the local application-level peer identity.
    SetPeerIdentity(Option<String>),
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
        verified_peers: std::collections::HashSet::new(),
        local_app_id: None,
        peer_identities: HashMap::new(),
        infrastructure_peers,
        pending_sync_peers: std::collections::HashSet::new(),
        dialing_peers: std::collections::HashSet::new(),
        pending_rendezvous_dials: VecDeque::new(),
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
pub(crate) enum RelayState {
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

use crate::network_status::NatStatus;

struct EngineRunner {
    pub(crate) swarm: libp2p::Swarm<WaveSyncBehaviour>,
    pub(crate) peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    pub(crate) db: DatabaseConnection,
    pub(crate) change_tx: broadcast::Sender<ChangeNotification>,
    pub(crate) registry: Arc<TableRegistry>,
    pub(crate) local_peer_id: libp2p::PeerId,
    pub(crate) site_id: NodeId,
    pub(crate) topic_name: String,
    pub(crate) config: EngineConfig,
    pub(crate) local_db_version: u64,
    pub(crate) peer_db_versions: HashMap<libp2p::PeerId, u64>,
    /// Display-only peer versions from incoming requests (NOT used for sync decisions).
    pub(crate) peer_reported_versions: HashMap<libp2p::PeerId, u64>,
    pub(crate) snapshot_resp_tx: mpsc::Sender<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    pub(crate) snapshot_resp_rx: mpsc::Receiver<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    /// Channel for queuing remote changesets to be applied sequentially.
    pub(crate) remote_changeset_tx: mpsc::Sender<Vec<ColumnChange>>,
    pub(crate) remote_changeset_rx: mpsc::Receiver<Vec<ColumnChange>>,
    pub(crate) registry_ready: Arc<Notify>,
    pub(crate) registry_is_ready: bool,
    pub(crate) cmd_rx: mpsc::Receiver<EngineCommand>,
    pub(crate) group_key: Option<GroupKey>,
    /// Relay connection state machine.
    pub(crate) relay_state: RelayState,
    /// Detected NAT status from AutoNAT probes.
    pub(crate) nat_status: NatStatus,
    /// Rendezvous namespace for peer discovery.
    pub(crate) rendezvous_namespace: String,
    /// Rendezvous discovery pagination cookie.
    pub(crate) rendezvous_cookie: Option<rendezvous::Cookie>,
    /// Whether we have an active rendezvous registration.
    pub(crate) rendezvous_registered: bool,
    /// Set of bootstrap peer IDs for tracking.
    pub(crate) bootstrap_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers rejected due to topic mismatch — never re-add via mDNS.
    pub(crate) rejected_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers verified via successful HMAC exchange — only these are group members.
    pub(crate) verified_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Application-level identity announced by the local peer (ephemeral, session-scoped).
    pub(crate) local_app_id: Option<String>,
    /// Application-level identities received from remote peers (ephemeral, session-scoped).
    pub(crate) peer_identities: HashMap<libp2p::PeerId, String>,
    /// Infrastructure peers (relay, rendezvous) — excluded from peer count and sync fan-out.
    pub(crate) infrastructure_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers with an in-flight sync request — prevents flooding request-response.
    pub(crate) pending_sync_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers currently being dialed (not yet connected). Prevents duplicate dials.
    pub(crate) dialing_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Queue of rendezvous-discovered peers waiting to be dialed (rate-limited).
    pub(crate) pending_rendezvous_dials: VecDeque<(libp2p::PeerId, libp2p::Multiaddr)>,
    /// Push notification token to register with relay: (platform, device_token).
    pub(crate) push_token: Option<(String, String)>,
    /// Whether push token has been registered with the relay.
    pub(crate) push_registered: bool,
    /// Shared network status snapshot, read by consumers.
    pub(crate) network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    /// Broadcast sender for network events.
    pub(crate) network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    /// Optional deadline for a post-resume sync retry (gives mDNS/rendezvous time to rediscover).
    pub(crate) resume_sync_deadline: Option<tokio::time::Instant>,
    /// API key for managed relay authentication.
    pub(crate) api_key: Option<String>,
    /// Keypair used for signing auth challenges (same identity as the swarm).
    pub(crate) keypair: identity::Keypair,
    /// Deadline after which we assume Private NAT if AutoNAT hasn't completed
    /// and the relay is connected. `None` once resolved or if no relay configured.
    pub(crate) nat_assumption_deadline: Option<tokio::time::Instant>,
    /// When the current relay circuit reservation was accepted (for proactive renewal).
    pub(crate) circuit_accepted_at: Option<tokio::time::Instant>,
    /// Number of circuit reservation retries while stuck in `Connected` state.
    pub(crate) circuit_retry_count: u32,
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
                is_group_member: self.verified_peers.contains(peer_id),
                app_id: self.peer_identities.get(peer_id).cloned(),
            })
            .collect();

        let relay_status = match &self.relay_state {
            RelayState::Disabled => ns::RelayStatus::Disabled,
            RelayState::Connecting { .. } => ns::RelayStatus::Connecting,
            RelayState::Connected { .. } => ns::RelayStatus::Connected,
            RelayState::Listening { .. } => ns::RelayStatus::Listening,
        };

        let nat_status = self.nat_status.clone();

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

    /// Handle a new connection: relay handshake, rendezvous discovery,
    /// bootstrap peer tracking, or regular peer sync initiation.
    async fn handle_connection_established(
        &mut self,
        peer_id: libp2p::PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
    ) {
        // If this is the relay server, transition state
        if let Some(ref relay_addr) = self.config.relay_server
            && let Some(libp2p::multiaddr::Protocol::P2p(relay_peer_id)) = relay_addr.iter().last()
            && peer_id == relay_peer_id
        {
            self.handle_relay_peer_connected(peer_id, relay_addr.clone());
        }

        // If this is a rendezvous server, discover peers
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
            self.handle_bootstrap_peer_connected(peer_id, endpoint);
        }

        if self.registry_is_ready
            && !self.rejected_peers.contains(&peer_id)
            && !self.infrastructure_peers.contains(&peer_id)
        {
            // Ensure reconnecting peer is tracked for future periodic syncs
            self.peers
                .entry(peer_id)
                .or_insert_with(|| endpoint.get_remote_address().clone());
            // Refresh local_db_version in case spawned tasks updated it
            self.local_db_version = shadow::get_db_version(&self.db)
                .await
                .unwrap_or(self.local_db_version);
            self.initiate_sync_for_peer(peer_id);
        }

        self.dialing_peers.remove(&peer_id);
        self.drain_pending_rendezvous_dials();
    }

    /// Set up relay circuit, external address, rendezvous registration, NAT timer.
    fn handle_relay_peer_connected(
        &mut self,
        peer_id: libp2p::PeerId,
        relay_addr: libp2p::Multiaddr,
    ) {
        log::info!("Connected to relay server {peer_id}");
        self.infrastructure_peers.insert(peer_id);
        self.circuit_retry_count = 0;
        self.relay_state = RelayState::Connected {
            relay_peer_id: peer_id,
            connected_at: tokio::time::Instant::now(),
        };
        self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
            crate::network_status::RelayStatus::Connected,
        ));
        self.update_network_status();

        // Eagerly listen on relay circuit
        let circuit_addr = relay_addr
            .clone()
            .with(libp2p::multiaddr::Protocol::P2pCircuit);
        if let Err(e) = self.swarm.listen_on(circuit_addr.clone()) {
            log::warn!("Failed to listen on relay circuit: {e}");
        } else {
            log::info!("Listening on relay circuit (eager): {circuit_addr}");
        }

        // Manually add circuit address as external
        let my_circuit_addr = relay_addr
            .clone()
            .with(libp2p::multiaddr::Protocol::P2pCircuit)
            .with(libp2p::multiaddr::Protocol::P2p(self.local_peer_id));
        self.swarm.add_external_address(my_circuit_addr.clone());
        log::info!("Added circuit address as external: {my_circuit_addr}");

        // Register with rendezvous immediately
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && self.swarm.is_connected(&rv_peer_id)
        {
            self.rendezvous_register(rv_peer_id);
        }

        // Start NAT assumption timer
        if self.nat_status == NatStatus::Unknown && self.nat_assumption_deadline.is_none() {
            self.nat_assumption_deadline =
                Some(tokio::time::Instant::now() + Duration::from_secs(30));
        }

        // Register push token with relay if configured
        self.maybe_register_push_token(peer_id);
        // Announce presence so the relay can introduce us to other peers
        // on the same topic (works for desktop too, no push token needed).
        self.announce_presence_to_relay(peer_id);
    }

    /// Track bootstrap peer, emit event, update last_seen.
    fn handle_bootstrap_peer_connected(
        &mut self,
        peer_id: libp2p::PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
    ) {
        let addr = endpoint.get_remote_address().clone();
        self.peers.insert(peer_id, addr.clone());

        self.emit_network_event(crate::network_status::NetworkEvent::PeerConnected(
            crate::network_status::PeerInfo {
                peer_id: crate::network_status::PeerId(peer_id.to_string()),
                address: addr.to_string(),
                db_version: None,
                is_bootstrap: true,
                is_group_member: false,
                app_id: None,
            },
        ));
        self.update_network_status();

        let db = self.db.clone();
        let peer_str = peer_id.to_string();
        tokio::spawn(async move {
            let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
        });
    }

    /// Handle peer disconnection: clean up tracking, reconnect relay/rendezvous.
    fn handle_connection_closed(&mut self, peer_id: libp2p::PeerId, num_established: u32) {
        self.pending_sync_peers.remove(&peer_id);
        self.verified_peers.remove(&peer_id);
        self.peer_identities.remove(&peer_id);

        if num_established > 0 {
            return;
        }

        // Handle relay server disconnect
        if let RelayState::Connected { relay_peer_id, .. } | RelayState::Listening { relay_peer_id } =
            &self.relay_state
            && peer_id == *relay_peer_id
        {
            self.handle_relay_peer_disconnected(peer_id);
        }

        // If rendezvous server disconnected (and is different from relay)
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && peer_id == rv_peer_id
            && !matches!(&self.relay_state, RelayState::Connecting { .. })
            && self.rendezvous_registered
        {
            log::warn!("Lost connection to rendezvous server {peer_id}");
            self.rendezvous_registered = false;
            self.emit_network_event(
                crate::network_status::NetworkEvent::RendezvousStatusChanged { registered: false },
            );
            self.update_network_status();
        }

        // Trigger rendezvous discover for disconnected sync peers
        if !self.infrastructure_peers.contains(&peer_id)
            && self.peer_db_versions.contains_key(&peer_id)
        {
            log::info!(
                "Peer {peer_id} disconnected with sync history, triggering rendezvous discover"
            );
            self.rendezvous_discover();
        }
    }

    /// Reset relay state, push registration, and attempt reconnection.
    fn handle_relay_peer_disconnected(&mut self, peer_id: libp2p::PeerId) {
        log::warn!("Lost connection to relay server {peer_id}");
        self.relay_state = RelayState::Connecting { retry_count: 0 };
        self.circuit_accepted_at = None;
        self.push_registered = false;
        self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
            crate::network_status::RelayStatus::Connecting,
        ));
        // If relay also serves rendezvous, reset that too
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && peer_id == rv_peer_id
        {
            log::warn!("Rendezvous server also disconnected (same as relay)");
            self.rendezvous_registered = false;
            self.emit_network_event(
                crate::network_status::NetworkEvent::RendezvousStatusChanged { registered: false },
            );
        }
        self.update_network_status();

        // Attempt immediate reconnection
        if let Some(ref relay_addr) = self.config.relay_server {
            log::info!("Attempting immediate relay reconnection");
            if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                log::warn!("Immediate relay redial failed: {e}");
            }
        }
    }

    async fn run(
        &mut self,
        sync_rx: &mut mpsc::Receiver<SyncChangeset>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        log::info!(
            "DIAG run() entered: relay_server={:?} rendezvous_server={:?} ipv6={} os={}",
            self.config.relay_server,
            self.config.rendezvous_server,
            self.config.ipv6,
            std::env::consts::OS
        );

        // iOS (including simulator): libp2p-tcp's `do_listen` instantiates an
        // `if_watch::tokio::IfWatcher` *only* when the bound IP is unspecified
        // (`0.0.0.0` / `::`). On iOS that watcher blocks indefinitely inside
        // `IfWatcher::new`. Binding to loopback skips the watcher path.
        #[cfg(target_os = "ios")]
        let (v4_tcp, v4_quic, v6_tcp, v6_quic) = (
            "/ip4/127.0.0.1/tcp/0",
            "/ip4/127.0.0.1/udp/0/quic-v1",
            "/ip6/::1/tcp/0",
            "/ip6/::1/udp/0/quic-v1",
        );
        #[cfg(not(target_os = "ios"))]
        let (v4_tcp, v4_quic, v6_tcp, v6_quic) = (
            "/ip4/0.0.0.0/tcp/0",
            "/ip4/0.0.0.0/udp/0/quic-v1",
            "/ip6/::/tcp/0",
            "/ip6/::/udp/0/quic-v1",
        );

        // IPv4 listen addresses — TCP is required, QUIC is best-effort
        log::info!("DIAG calling listen_on(TCP)={v4_tcp}");
        let t0 = std::time::Instant::now();
        let tcp_res = self.swarm.listen_on(v4_tcp.parse().unwrap());
        log::info!(
            "DIAG listen_on(TCP) returned in {:?}: {:?}",
            t0.elapsed(),
            tcp_res
        );
        tcp_res?;
        log::info!("DIAG calling listen_on(QUIC)={v4_quic}");
        let t1 = std::time::Instant::now();
        let quic_res = self.swarm.listen_on(v4_quic.parse().unwrap());
        log::info!(
            "DIAG listen_on(QUIC) returned in {:?}: {:?}",
            t1.elapsed(),
            quic_res
        );
        if let Err(e) = quic_res {
            log::warn!("QUIC IPv4 listen failed (non-fatal, TCP still active): {e}");
        }

        // IPv6 listen addresses (opt-in) — TCP is required, QUIC is best-effort
        if self.config.ipv6 {
            self.swarm.listen_on(v6_tcp.parse().unwrap())?;
            if let Err(e) = self.swarm.listen_on(v6_quic.parse().unwrap()) {
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
                                NatStatus::Private,
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
            log::debug!(
                "No directly-connected peers; relying on relay push to wake sleeping peers"
            );
        } else {
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
        }

        // Notify relay to send push notifications to sleeping mobile peers.
        // Must run even when peer_ids is empty — that's the case where both
        // peers are behind NAT with no direct connection, and push is the
        // only way to wake the other side.
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
                self.handle_connection_established(peer_id, &endpoint).await;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                log::debug!("Connection closed with {peer_id} ({num_established} remaining)");
                self.handle_connection_closed(peer_id, num_established);
            }
            SwarmEvent::ListenerClosed { reason, .. } => {
                if let Err(ref e) = reason {
                    let err_str = format!("{e:?}");
                    if err_str.contains("NoAddressesInReservation") {
                        log::error!(
                            "Relay circuit failed: NoAddressesInReservation. \
                             The relay server needs --external-address configured. \
                             Peers can still discover via rendezvous fallback."
                        );
                    } else {
                        log::warn!("Listener closed with error: {e}");
                    }
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
                if let Some(pid) = peer_id {
                    self.dialing_peers.remove(&pid);
                    if !self.swarm.is_connected(&pid) && self.peers.remove(&pid).is_some() {
                        self.pending_sync_peers.remove(&pid);
                        self.verified_peers.remove(&pid);
                        self.peer_identities.remove(&pid);
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::PeerDisconnected(
                                crate::network_status::PeerId(pid.to_string()),
                            ),
                        );
                        self.update_network_status();
                    }
                }
                self.drain_pending_rendezvous_dials();
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
                peer,
                message: request_response::Message::Response { response, .. },
                ..
            } => match response {
                push_protocol::PushResponse::Ok => {
                    log::debug!("Push request acknowledged by {peer}");
                }
                push_protocol::PushResponse::Error { message } => {
                    log::warn!("Push request error from {peer}: {message}");
                }
                push_protocol::PushResponse::PeerList { peers } => {
                    log::info!(
                        "Relay {peer} introduced {} peer(s) on our topic",
                        peers.len()
                    );
                    for addr in peers {
                        self.dial_introduced_peer(&addr);
                    }
                }
            },
            request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Request {
                        request, channel, ..
                    },
                ..
            } => {
                match &request {
                    push_protocol::PushRequest::PeerJoined { topic, peer_addrs } => {
                        // Only honour PeerJoined from our relay. Any other
                        // sender is unexpected — ignore with an error response.
                        let from_relay = matches!(
                            self.relay_state,
                            RelayState::Connected { relay_peer_id, .. }
                                | RelayState::Listening { relay_peer_id }
                                if relay_peer_id == peer
                        );
                        if !from_relay {
                            log::warn!("Ignoring PeerJoined from non-relay peer {peer}");
                            let _ = self.swarm.behaviour_mut().push.send_response(
                                channel,
                                push_protocol::PushResponse::Error {
                                    message: "not your relay".to_string(),
                                },
                            );
                            return;
                        }
                        if *topic != self.topic_name {
                            log::debug!(
                                "Ignoring PeerJoined for foreign topic (ours vs theirs hash mismatch)"
                            );
                            let _ = self
                                .swarm
                                .behaviour_mut()
                                .push
                                .send_response(channel, push_protocol::PushResponse::Ok);
                            return;
                        }
                        log::info!(
                            "Relay announced new peer on topic with {} address(es)",
                            peer_addrs.len()
                        );
                        for addr in peer_addrs {
                            self.dial_introduced_peer(addr);
                        }
                        let _ = self
                            .swarm
                            .behaviour_mut()
                            .push
                            .send_response(channel, push_protocol::PushResponse::Ok);
                    }
                    _ => {
                        // Peers shouldn't receive RegisterToken / NotifyTopic
                        // from anywhere. Reject politely.
                        let _ = self.swarm.behaviour_mut().push.send_response(
                            channel,
                            push_protocol::PushResponse::Error {
                                message: "unsupported request for a peer".to_string(),
                            },
                        );
                    }
                }
            }
            request_response::Event::OutboundFailure { error, peer, .. } => {
                log::warn!("Push request to {peer} failed: {error}");
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
}
