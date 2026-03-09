//! P2P sync engine powered by libp2p.
//!
//! The engine runs as a background tokio task, managing a libp2p swarm with:
//! - **gossipsub** for pub/sub message replication
//! - **mDNS** for local peer discovery
//! - **QUIC + TCP** transports with Noise encryption and Yamux multiplexing
//! - **relay + dcutr + autonat** for NAT traversal (WIP)
//!
//! Local write operations arrive via an mpsc channel from [`WaveSyncDb`](crate::WaveSyncDb),
//! are logged to [`sync_log`], and published to gossipsub.
//! Incoming remote operations are checked with [`conflict::should_apply`]
//! before being applied to the local database.

pub(crate) mod behaviour;
pub(crate) mod snapshot_protocol;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use behaviour::{WaveSyncBehaviour, WaveSyncBehaviourEvent};
use libp2p::{
    SwarmBuilder,
    futures::StreamExt,
    gossipsub::{self, IdentTopic},
    identify, identity, mdns, noise, ping, request_response,
    swarm::SwarmEvent,
    yamux,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use tokio::sync::{Notify, broadcast, mpsc};

use crate::conflict;
use crate::merkle::{MerkleTree, RowInfo};
use crate::messages::{ChangeNotification, NodeId, SyncMessage, SyncOperation, WriteKind};
use crate::peer_tracker;
use crate::protocol::{RowOpRequest, SyncRequest};
use crate::registry::TableRegistry;
use crate::sync_log;

/// Configuration for the sync engine.
pub struct EngineConfig {
    /// How long before a peer is considered stale and evicted (default: 7 days).
    pub eviction_timeout: Duration,
    /// How often to broadcast our watermark to peers (default: 5s).
    pub watermark_interval: Duration,
    /// How often to run log compaction (default: 5 min).
    pub compaction_interval: Duration,
    /// How often mDNS sends queries (default: 5s for fast LAN discovery).
    pub mdns_query_interval: Duration,
    /// How long mDNS records stay valid (default: 30s).
    pub mdns_ttl: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            eviction_timeout: Duration::from_secs(7 * 24 * 60 * 60),
            watermark_interval: Duration::from_secs(5),
            compaction_interval: Duration::from_secs(300),
            mdns_query_interval: Duration::from_secs(5),
            mdns_ttl: Duration::from_secs(30),
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
///
/// Spawns a new tokio task that runs the libp2p swarm event loop. The engine:
/// - Listens on a random TCP port for incoming connections
/// - Subscribes to the gossipsub topic for this application
/// - Reads local [`SyncOperation`]s from `sync_rx` and publishes them
/// - Receives remote operations from gossipsub and applies them (with LWW conflict resolution)
/// - Sends [`ChangeNotification`]s on `change_tx` when remote writes are applied
#[allow(clippy::too_many_arguments)]
pub fn start_engine(
    db: DatabaseConnection,
    sync_rx: mpsc::Receiver<SyncOperation>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    node_id: NodeId,
    topic: String,
    relay_server: Option<String>,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_engine(
            db,
            sync_rx,
            change_tx,
            registry,
            node_id,
            topic,
            relay_server,
            config,
            registry_ready,
        )
        .await
        {
            log::error!("Engine error: {}", e);
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn run_engine(
    db: DatabaseConnection,
    mut sync_rx: mpsc::Receiver<SyncOperation>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    node_id: NodeId,
    topic_name: String,
    relay_server: Option<String>,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let keypair = identity::Keypair::generate_ed25519();
    let mdns_config = config.mdns_config();

    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(move |key, relay_client| {
            WaveSyncBehaviour::new(key, relay_client, mdns_config.clone())
        })?
        .build();

    let local_peer_id = keypair.public().to_peer_id();

    // Check if we need a snapshot (empty sync log = new peer)
    let initial_max_hlc = sync_log::get_max_hlc(&db).await?;
    let needs_snapshot = Arc::new(AtomicBool::new(initial_max_hlc.is_none()));
    let snapshot_retries = Arc::new(AtomicU8::new(0));

    let (snapshot_resp_tx, snapshot_resp_rx) = mpsc::channel::<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>(8);

    // Build Merkle trees from the sync log
    let latest_rows = sync_log::get_latest_per_row(&db).await?;
    let mut merkle_trees: HashMap<String, MerkleTree> = HashMap::new();
    {
        let mut grouped: HashMap<String, Vec<RowInfo>> = HashMap::new();
        for row in latest_rows {
            let mut nid: NodeId = [0u8; 16];
            let len = row.node_id.len().min(16);
            nid[..len].copy_from_slice(&row.node_id[..len]);
            grouped
                .entry(row.table_name.clone())
                .or_default()
                .push(RowInfo {
                    primary_key: row.primary_key,
                    hlc_time: row.hlc_time as u64,
                    hlc_counter: row.hlc_counter as u32,
                    node_id: nid,
                });
        }
        for (table_name, rows) in grouped {
            merkle_trees.insert(
                table_name.clone(),
                MerkleTree::build(table_name, rows),
            );
        }
    }
    log::info!(
        "Built Merkle trees for {} tables at startup",
        merkle_trees.len()
    );

    let (merkle_update_tx, merkle_update_rx) = mpsc::channel::<SyncOperation>(256);
    let (deferred_req_tx, deferred_req_rx) = mpsc::channel::<(libp2p::PeerId, SyncRequest)>(8);
    let (watermark_tx, watermark_rx) = mpsc::channel::<u64>(8);
    let (snapshot_retry_tx, snapshot_retry_rx) = mpsc::channel::<bool>(4);

    let mut engine = EngineRunner {
        swarm,
        peers: HashMap::new(),
        topic: gossipsub::IdentTopic::new(&topic_name),
        db,
        change_tx,
        registry,
        relay_server,
        local_peer_id,
        node_id,
        config,
        local_watermark: initial_max_hlc.unwrap_or(0),
        needs_snapshot,
        snapshot_retries,
        snapshot_resp_tx,
        snapshot_resp_rx,
        merkle_trees,
        merkle_update_tx,
        merkle_update_rx,
        deferred_req_tx,
        deferred_req_rx,
        pending_requests: HashMap::new(),
        pending_full_sync: Arc::new(AtomicBool::new(false)),
        failed_snapshot_peers: HashSet::new(),
        pending_snapshot_peer: None,
        registry_ready,
        registry_is_ready: false,
        watermark_tx,
        watermark_rx,
        snapshot_retry_tx,
        snapshot_retry_rx,
    };

    engine.run(&mut sync_rx).await
}

/// Tracks what kind of request an outbound request ID corresponds to.
#[derive(Debug, Clone, Copy)]
enum SyncRequestKind {
    FullSync,
    MerkleRoots,
    MerkleTrees,
    MerkleRowOps,
}

struct EngineRunner {
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    topic: IdentTopic,
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    relay_server: Option<String>,
    local_peer_id: libp2p::PeerId,
    node_id: NodeId,
    config: EngineConfig,
    local_watermark: u64,
    needs_snapshot: Arc<AtomicBool>,
    snapshot_retries: Arc<AtomicU8>,
    snapshot_resp_tx: mpsc::Sender<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    snapshot_resp_rx: mpsc::Receiver<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    merkle_trees: HashMap<String, MerkleTree>,
    merkle_update_tx: mpsc::Sender<SyncOperation>,
    merkle_update_rx: mpsc::Receiver<SyncOperation>,
    deferred_req_tx: mpsc::Sender<(libp2p::PeerId, SyncRequest)>,
    deferred_req_rx: mpsc::Receiver<(libp2p::PeerId, SyncRequest)>,
    pending_requests: HashMap<request_response::OutboundRequestId, SyncRequestKind>,
    pending_full_sync: Arc<AtomicBool>,
    /// Peers that returned irrelevant or empty snapshots — skip on retry.
    failed_snapshot_peers: HashSet<libp2p::PeerId>,
    /// Which peer is currently handling our FullSync request.
    pending_snapshot_peer: Option<libp2p::PeerId>,
    registry_ready: Arc<Notify>,
    registry_is_ready: bool,
    watermark_tx: mpsc::Sender<u64>,
    watermark_rx: mpsc::Receiver<u64>,
    snapshot_retry_tx: mpsc::Sender<bool>,
    snapshot_retry_rx: mpsc::Receiver<bool>,
}

impl EngineRunner {
    async fn run(
        &mut self,
        sync_rx: &mut mpsc::Receiver<SyncOperation>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())?;

        self.swarm
            .listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap())?;

        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&self.topic)?;

        // If a relay server is configured, dial it
        if let Some(ref relay_addr) = self.relay_server
            && let Ok(addr) = relay_addr.parse::<libp2p::Multiaddr>()
        {
            if let Err(e) = self.swarm.dial(addr) {
                log::warn!("Failed to dial relay server {}: {}", relay_addr, e);
            } else {
                log::info!("Dialing relay server: {}", relay_addr);
            }
        }

        let mut compaction_interval = tokio::time::interval(self.config.compaction_interval);
        let mut watermark_interval = tokio::time::interval(self.config.watermark_interval);

        loop {
            tokio::select! {
                Some(op) = sync_rx.recv() => {
                    self.handle_local_op(op).await;
                },
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event);
                },
                _ = compaction_interval.tick() => {
                    let db = self.db.clone();
                    let eviction_timeout = self.config.eviction_timeout;
                    tokio::spawn(async move {
                        run_compaction(db, eviction_timeout).await;
                    });
                },
                _ = watermark_interval.tick() => {
                    self.broadcast_watermark();
                },
                Some((channel, response)) = self.snapshot_resp_rx.recv() => {
                    if let Err(resp) = self.swarm.behaviour_mut().snapshot.send_response(channel, response) {
                        log::error!("Failed to send snapshot response: {:?}", resp);
                    }
                },
                Some(op) = self.merkle_update_rx.recv() => {
                    self.update_merkle_tree(&op);
                },
                Some((peer_id, request)) = self.deferred_req_rx.recv() => {
                    let req_id = self.swarm.behaviour_mut().snapshot.send_request(&peer_id, request);
                    self.pending_requests.insert(req_id, SyncRequestKind::MerkleRowOps);
                },
                _ = self.registry_ready.notified(), if !self.registry_is_ready => {
                    self.registry_is_ready = true;
                    log::info!("Registry ready, syncing all known peers");
                    self.sync_all_known_peers();
                },
                Some(wm) = self.watermark_rx.recv() => {
                    self.local_watermark = wm;
                    self.pending_full_sync.store(false, Ordering::SeqCst);
                    self.failed_snapshot_peers.clear();
                    self.pending_snapshot_peer = None;
                },
                Some(irrelevant) = self.snapshot_retry_rx.recv() => {
                    // If the snapshot was irrelevant (wrong peer), remember
                    // that peer so we skip it next time.
                    if irrelevant
                        && let Some(failed_peer) = self.pending_snapshot_peer.take()
                    {
                        self.failed_snapshot_peers.insert(failed_peer);
                    }
                    log::info!("Snapshot retry requested, syncing all known peers");
                    self.sync_all_known_peers();
                },
            }
        }
    }

    async fn handle_local_op(&mut self, op: SyncOperation) {
        // Update local watermark
        self.local_watermark = self.local_watermark.max(op.hlc_time);

        // Log the operation to the sync log first
        if let Err(e) = sync_log::insert_op(&self.db, &op).await {
            log::error!("Failed to log local sync operation: {}", e);
        }

        // Update Merkle tree
        self.update_merkle_tree(&op);

        let msg = SyncMessage::Op(op);
        match postcard::to_allocvec(&msg) {
            Ok(data) => {
                // Gossipsub default max message size is ~1 MiB
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
                }
            }
            Err(e) => {
                log::error!("Failed to serialize operation: {}", e);
            }
        }
    }

    fn handle_swarm_event(&mut self, event: SwarmEvent<WaveSyncBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address:?}");
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
                log::debug!("Relay client event: {:?}", event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Dcutr(event)) => {
                log::debug!("DCUtR event: {:?}", event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Autonat(event)) => {
                log::debug!("AutoNAT event: {:?}", event);
            }
            _ => {}
        }
    }

    fn sync_all_known_peers(&mut self) {
        let peer_ids: Vec<libp2p::PeerId> = self.peers.keys().cloned().collect();
        for peer_id in peer_ids {
            self.initiate_sync_for_peer(peer_id);
        }
    }

    fn initiate_sync_for_peer(&mut self, peer_id: libp2p::PeerId) {
        let needs = self.needs_snapshot.load(Ordering::SeqCst);
        let pending = self.pending_full_sync.load(Ordering::SeqCst);

        if needs && !pending {
            if self.failed_snapshot_peers.contains(&peer_id) {
                log::debug!("Skipping previously failed snapshot peer {peer_id}");
                return;
            }
            self.pending_full_sync.store(true, Ordering::SeqCst);
            self.needs_snapshot.store(false, Ordering::SeqCst);
            self.pending_snapshot_peer = Some(peer_id);
            log::info!("Requesting full snapshot from peer {peer_id}");
            let req_id = self.swarm
                .behaviour_mut()
                .snapshot
                .send_request(&peer_id, SyncRequest::FullSync);
            self.pending_requests.insert(req_id, SyncRequestKind::FullSync);
        } else if !needs && self.local_watermark > 0 {
            log::info!("Requesting Merkle roots from peer {peer_id} for sync");
            let req_id = self.swarm.behaviour_mut().snapshot.send_request(
                &peer_id,
                SyncRequest::MerkleRoots,
            );
            self.pending_requests.insert(req_id, SyncRequestKind::MerkleRoots);
        } else if needs && pending {
            log::debug!("FullSync already pending, skipping peer {peer_id}");
        } else {
            // needs_snapshot=false, local_watermark=0: waiting for watermark after snapshot apply
            log::debug!("Waiting for watermark update, skipping peer {peer_id}");
        }
    }

    fn handle_mdns(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, multiaddr) in list {
                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                    let is_new = !self.peers.contains_key(&peer_id);
                    if is_new {
                        if let Err(e) = self.swarm.dial(multiaddr.clone()) {
                            log::warn!("Failed to dial discovered peer {peer_id}: {e}");
                        }
                        // Reset retry counter for each new peer
                        self.snapshot_retries.store(0, Ordering::SeqCst);
                    }
                    self.peers.insert(peer_id, multiaddr);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer_id);

                    // Register peer in tracker (use peer_id bytes as the node identifier)
                    let db = self.db.clone();
                    let peer_str = peer_id.to_string();
                    let peer_id_bytes = peer_id.to_bytes();
                    tokio::spawn(async move {
                        if peer_tracker::is_known_peer(&db, &peer_str).await.unwrap_or(false) {
                            let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
                        } else {
                            let _ = peer_tracker::upsert_peer(&db, &peer_str, &peer_id_bytes, 0).await;
                        }
                    });

                    // Sync decision — every discovery event, not just new peers
                    if self.registry_is_ready {
                        self.initiate_sync_for_peer(peer_id);
                    }
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, multiaddr) in list {
                    log::debug!("Expired peer {peer_id} at {multiaddr}");
                    self.peers.remove(&peer_id);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
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

                // Try SyncMessage envelope first, fall back to raw SyncOperation
                let sync_msg: SyncMessage = match postcard::from_bytes::<SyncMessage>(&message.data)
                {
                    Ok(msg) => msg,
                    Err(_) => {
                        // Backward compatibility: try raw SyncOperation
                        match postcard::from_bytes::<SyncOperation>(&message.data) {
                            Ok(op) => SyncMessage::Op(op),
                            Err(e) => {
                                log::error!(
                                    "Failed to deserialize message from peer {}: {}",
                                    propagation_source,
                                    e
                                );
                                return;
                            }
                        }
                    }
                };

                match sync_msg {
                    SyncMessage::Op(op) => {
                        log::info!(
                            "Deserialized operation from peer {}: {:?} on table {}",
                            propagation_source,
                            op.kind,
                            op.table
                        );

                        // Spawn the async DB work on a separate task to avoid
                        // borrowing self (which contains Swarm, not Sync) across await.
                        let db = self.db.clone();
                        let change_tx = self.change_tx.clone();
                        let registry = self.registry.clone();
                        let merkle_tx = self.merkle_update_tx.clone();
                        tokio::spawn(async move {
                            apply_remote_op(db, change_tx, registry, merkle_tx, op).await;
                        });
                    }
                    SyncMessage::Watermark { node_id, hlc_time } => {
                        log::debug!(
                            "Received watermark from peer {}: hlc_time={}",
                            propagation_source,
                            hlc_time
                        );
                        let db = self.db.clone();
                        let peer_str = propagation_source.to_string();
                        tokio::spawn(async move {
                            let _ = peer_tracker::update_watermark(&db, &peer_str, hlc_time).await;
                            let _ =
                                peer_tracker::upsert_peer(&db, &peer_str, &node_id, hlc_time).await;
                        });
                    }
                }
            }
            _ => {
                log::debug!("Other gossipsub event: {:?}", event);
            }
        }
    }

    /// Recreate the mDNS behaviour to restart the probe cycle.
    /// A fresh mDNS behaviour probes at 500ms, doubling until the query interval,
    /// giving rapid rediscovery when no peers are available.
    fn trigger_rediscovery(&mut self) {
        log::info!("No peers available, triggering mDNS rediscovery");
        match mdns::tokio::Behaviour::new(self.config.mdns_config(), self.local_peer_id) {
            Ok(new_mdns) => {
                self.swarm.behaviour_mut().mdns = new_mdns;
            }
            Err(e) => log::error!("Failed to restart mDNS: {e}"),
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
                    log::info!("Received snapshot request from peer {peer}: {request:?}");

                    match request {
                        SyncRequest::MerkleRoots => {
                            // Respond directly — no async DB needed
                            let roots: Vec<_> = self.merkle_trees.values().map(|t| t.root()).collect();
                            let current_hlc = self.local_watermark;
                            let resp = crate::protocol::SyncResponse::MerkleRootsResponse {
                                roots,
                                current_hlc,
                            };
                            if let Err(resp) = self.swarm.behaviour_mut().snapshot.send_response(channel, resp) {
                                log::error!("Failed to send MerkleRootsResponse: {:?}", resp);
                            }
                        }
                        SyncRequest::MerkleTrees { table_names } => {
                            let trees: Vec<_> = table_names
                                .iter()
                                .filter_map(|name| self.merkle_trees.get(name).cloned())
                                .collect();
                            let resp = crate::protocol::SyncResponse::MerkleTreesResponse { trees };
                            if let Err(resp) = self.swarm.behaviour_mut().snapshot.send_response(channel, resp) {
                                log::error!("Failed to send MerkleTreesResponse: {:?}", resp);
                            }
                        }
                        SyncRequest::MerkleRowOps { requests, initiator_ops } => {
                            let db = self.db.clone();
                            let resp_tx = self.snapshot_resp_tx.clone();
                            let change_tx = self.change_tx.clone();
                            let registry = self.registry.clone();
                            let merkle_tx = self.merkle_update_tx.clone();
                            tokio::spawn(async move {
                                let mut ops = Vec::new();
                                for req in &requests {
                                    match sync_log::get_latest_for_row(&db, &req.table_name, &req.primary_key).await {
                                        Ok(Some(op)) => ops.push(op),
                                        Ok(None) => {}
                                        Err(e) => log::error!("Failed to get op for {}/{}: {}", req.table_name, req.primary_key, e),
                                    }
                                }

                                // Apply the initiator's ops via LWW (bidirectional sync)
                                for op in initiator_ops {
                                    apply_remote_op(
                                        db.clone(),
                                        change_tx.clone(),
                                        registry.clone(),
                                        merkle_tx.clone(),
                                        op,
                                    ).await;
                                }

                                let resp = crate::protocol::SyncResponse::MerkleRowOpsResponse { ops };
                                if let Err(e) = resp_tx.send((channel, resp)).await {
                                    log::error!("Failed to queue MerkleRowOpsResponse: {}", e);
                                }
                            });
                        }
                        SyncRequest::FullSync => {
                            let db = self.db.clone();
                            let registry = self.registry.clone();
                            let resp_tx = self.snapshot_resp_tx.clone();
                            tokio::spawn(async move {
                                let current_hlc =
                                    sync_log::get_max_hlc(&db).await.ok().flatten().unwrap_or(0);
                                match crate::snapshot::generate_snapshot(&db, &registry, current_hlc).await {
                                    Ok(resp) => {
                                        if let Err(e) = resp_tx.send((channel, resp)).await {
                                            log::error!("Failed to queue snapshot response: {}", e);
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Failed to generate snapshot: {}", e);
                                    }
                                }
                            });
                        }
                    }
                }
                request_response::Message::Response { request_id, response } => {
                    log::info!("Received snapshot response from peer {peer}");
                    self.pending_requests.remove(&request_id);
                    match response {
                        crate::protocol::SyncResponse::FullSnapshot {
                            tables,
                            current_hlc,
                            ..
                        } => {
                            if tables.is_empty() {
                                log::warn!(
                                    "Received empty snapshot from peer {peer}, will retry"
                                );
                                self.needs_snapshot.store(true, Ordering::SeqCst);
                                self.pending_full_sync.store(false, Ordering::SeqCst);
                                self.snapshot_retries.fetch_add(1, Ordering::SeqCst);
                                return;
                            }

                            let db = self.db.clone();
                            let registry = self.registry.clone();
                            let change_tx = self.change_tx.clone();
                            let peer_str = peer.to_string();
                            let node_id = self.node_id;
                            let needs_snapshot = self.needs_snapshot.clone();
                            let pending_full_sync = self.pending_full_sync.clone();
                            let snapshot_retries = self.snapshot_retries.clone();
                            let watermark_tx = self.watermark_tx.clone();
                            let retry_tx = self.snapshot_retry_tx.clone();
                            tokio::spawn(async move {
                                match crate::snapshot::apply_snapshot(&db, &registry, &tables)
                                    .await
                                {
                                    Ok(true) => {
                                        log::info!(
                                            "Snapshot applied successfully, {} tables",
                                            tables.len()
                                        );
                                        snapshot_retries.store(0, Ordering::SeqCst);
                                        let _ = peer_tracker::upsert_peer(
                                            &db,
                                            &peer_str,
                                            &node_id,
                                            current_hlc,
                                        )
                                        .await;
                                        // Update watermark only after successful application
                                        let _ = watermark_tx.send(current_hlc).await;
                                        for table in &tables {
                                            let _ = change_tx.send(ChangeNotification {
                                                table: table.table_name.clone(),
                                                kind: crate::messages::WriteKind::Insert,
                                                primary_key: String::new(),
                                            });
                                        }
                                    }
                                    Ok(false) => {
                                        // Snapshot had no relevant data (wrong peer or empty tables).
                                        // Retry with a different peer.
                                        log::warn!("Snapshot had no relevant data, will retry");
                                        needs_snapshot.store(true, Ordering::SeqCst);
                                        pending_full_sync.store(false, Ordering::SeqCst);
                                        snapshot_retries.fetch_add(1, Ordering::SeqCst);
                                        // true = irrelevant peer, skip it on next retry
                                        let _ = retry_tx.send(true).await;
                                    }
                                    Err(e) => {
                                        log::error!("Failed to apply snapshot: {}", e);
                                        needs_snapshot.store(true, Ordering::SeqCst);
                                        pending_full_sync.store(false, Ordering::SeqCst);
                                        snapshot_retries.fetch_add(1, Ordering::SeqCst);
                                        // false = transient error, can retry same peer
                                        let _ = retry_tx.send(false).await;
                                    }
                                }
                            });
                        }
                        crate::protocol::SyncResponse::MerkleRootsResponse {
                            roots,
                            current_hlc,
                        } => {
                            // Compare roots — find tables that differ
                            let mut differing_tables = Vec::new();
                            for remote_root in &roots {
                                match self.merkle_trees.get(&remote_root.table_name) {
                                    Some(local_tree) => {
                                        if local_tree.root_hash() != remote_root.root_hash {
                                            differing_tables.push(remote_root.table_name.clone());
                                        }
                                    }
                                    None => {
                                        // We don't have this table at all
                                        differing_tables.push(remote_root.table_name.clone());
                                    }
                                }
                            }
                            // Also check tables we have but remote doesn't
                            for local_name in self.merkle_trees.keys() {
                                if !roots.iter().any(|r| &r.table_name == local_name)
                                    && !differing_tables.contains(local_name)
                                {
                                    differing_tables.push(local_name.clone());
                                }
                            }

                            if differing_tables.is_empty() {
                                log::info!("Merkle sync with peer {peer}: all tables match");
                                self.local_watermark = self.local_watermark.max(current_hlc);
                            } else {
                                log::info!(
                                    "Merkle sync with peer {peer}: {} tables differ, requesting trees",
                                    differing_tables.len()
                                );
                                let req_id = self.swarm.behaviour_mut().snapshot.send_request(
                                    &peer,
                                    SyncRequest::MerkleTrees {
                                        table_names: differing_tables,
                                    },
                                );
                                self.pending_requests.insert(req_id, SyncRequestKind::MerkleTrees);
                            }
                        }
                        crate::protocol::SyncResponse::MerkleTreesResponse { trees } => {
                            let mut row_requests: Vec<RowOpRequest> = Vec::new();
                            for remote_tree in &trees {
                                let differing_pks = match self.merkle_trees.get(&remote_tree.table_name) {
                                    Some(local_tree) => MerkleTree::diff(local_tree, remote_tree),
                                    None => remote_tree.keys.clone(),
                                };
                                for pk in differing_pks {
                                    row_requests.push(RowOpRequest {
                                        table_name: remote_tree.table_name.clone(),
                                        primary_key: pk,
                                    });
                                }
                            }

                            if row_requests.is_empty() {
                                log::info!("Merkle tree diff with peer {peer}: no differing rows");
                            } else {
                                log::info!(
                                    "Merkle tree diff with peer {peer}: {} rows differ, requesting ops",
                                    row_requests.len()
                                );
                                // Async lookup local ops for differing rows, then send via deferred channel
                                let db = self.db.clone();
                                let deferred_tx = self.deferred_req_tx.clone();
                                tokio::spawn(async move {
                                    let mut initiator_ops = Vec::new();
                                    for req in &row_requests {
                                        match sync_log::get_latest_for_row(&db, &req.table_name, &req.primary_key).await {
                                            Ok(Some(op)) => initiator_ops.push(op),
                                            Ok(None) => {}
                                            Err(e) => log::error!("Failed to get local op for {}/{}: {}", req.table_name, req.primary_key, e),
                                        }
                                    }
                                    let request = SyncRequest::MerkleRowOps {
                                        requests: row_requests,
                                        initiator_ops,
                                    };
                                    if let Err(e) = deferred_tx.send((peer, request)).await {
                                        log::error!("Failed to queue deferred MerkleRowOps request: {}", e);
                                    }
                                });
                            }
                        }
                        crate::protocol::SyncResponse::MerkleRowOpsResponse { ops } => {
                            if ops.is_empty() {
                                log::info!("Merkle row ops from peer {peer}: nothing to apply");
                                return;
                            }

                            log::info!(
                                "Received {} Merkle row ops from peer {peer}",
                                ops.len()
                            );

                            let db = self.db.clone();
                            let change_tx = self.change_tx.clone();
                            let registry = self.registry.clone();
                            let merkle_tx = self.merkle_update_tx.clone();
                            tokio::spawn(async move {
                                for op in ops {
                                    apply_remote_op(
                                        db.clone(),
                                        change_tx.clone(),
                                        registry.clone(),
                                        merkle_tx.clone(),
                                        op,
                                    )
                                    .await;
                                }
                            });
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, request_id, error, .. } => {
                log::warn!("Snapshot request to {peer} failed: {error}");
                match self.pending_requests.remove(&request_id) {
                    Some(SyncRequestKind::FullSync) => {
                        self.needs_snapshot.store(true, Ordering::SeqCst);
                        self.pending_full_sync.store(false, Ordering::SeqCst);
                        self.snapshot_retries.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {
                        // Merkle failures retry naturally on next mDNS cycle
                    }
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                log::warn!("Snapshot inbound from {peer} failed: {error}");
            }
            _ => {}
        }
    }

    fn update_merkle_tree(&mut self, op: &SyncOperation) {
        let tree = self.merkle_trees.entry(op.table.clone()).or_insert_with(|| {
            MerkleTree::build(op.table.clone(), vec![])
        });
        match op.kind {
            WriteKind::Delete => {
                tree.remove_leaf(&op.primary_key);
            }
            WriteKind::Insert | WriteKind::Update => {
                tree.update_leaf(
                    &op.primary_key,
                    op.hlc_time,
                    op.hlc_counter,
                    &op.node_id,
                );
            }
        }
    }

    fn broadcast_watermark(&mut self) {
        let msg = SyncMessage::Watermark {
            node_id: self.node_id,
            hlc_time: self.local_watermark,
        };
        match postcard::to_allocvec(&msg) {
            Ok(data) => {
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(self.topic.clone(), data)
                {
                    log::debug!("Failed to publish watermark: {}", e);
                }
            }
            Err(e) => {
                log::error!("Failed to serialize watermark: {}", e);
            }
        }
    }
}

async fn apply_remote_op(
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    merkle_update_tx: mpsc::Sender<SyncOperation>,
    op: SyncOperation,
) {
    // Reject ops for unregistered tables
    if !registry.is_registered(&op.table) {
        log::warn!(
            "Rejecting remote op for unregistered table: {}",
            op.table
        );
        return;
    }

    // Check LWW conflict resolution
    let local_latest = sync_log::get_latest_for_row(&db, &op.table, &op.primary_key)
        .await
        .ok()
        .flatten();

    if !conflict::should_apply(&op, local_latest.as_ref()) {
        log::debug!(
            "Skipping remote op (LWW): table={} pk={}",
            op.table,
            op.primary_key
        );
        return;
    }

    // Apply the operation to the local database
    let Some(ref sql_bytes) = op.data else {
        log::warn!(
            "Skipping remote op with no data: table={} pk={}",
            op.table,
            op.primary_key
        );
        return;
    };
    let Ok(sql) = std::str::from_utf8(sql_bytes) else {
        log::warn!(
            "Skipping remote op with invalid UTF-8 data: table={} pk={}",
            op.table,
            op.primary_key
        );
        return;
    };

    // Reject multi-statement SQL (potential injection)
    if sql.contains(';') {
        log::warn!(
            "Rejecting remote op with multi-statement SQL: table={} pk={}",
            op.table,
            op.primary_key
        );
        return;
    }

    // Strip RETURNING clause — remote nodes don't need it and it can cause
    // issues with execute_unprepared which doesn't return query results.
    let clean_sql = strip_returning(sql);

    // Use INSERT OR REPLACE for inserts to handle potential PK conflicts
    // between nodes (e.g., both nodes generating the same auto-increment ID).
    let final_sql = if op.kind == crate::messages::WriteKind::Insert {
        if clean_sql.contains("OR REPLACE") {
            clean_sql
        } else if clean_sql.contains("OR IGNORE") {
            clean_sql.replacen("INSERT OR IGNORE INTO", "INSERT OR REPLACE INTO", 1)
        } else {
            clean_sql.replacen("INSERT INTO", "INSERT OR REPLACE INTO", 1)
        }
    } else {
        clean_sql
    };

    // Validate that the SQL matches the declared operation kind and table
    match crate::connection::classify_write(&final_sql) {
        Some((kind, table)) => {
            if kind != op.kind || table != op.table {
                log::warn!(
                    "Rejecting remote op: SQL kind/table ({:?}/{}) doesn't match declared ({:?}/{})",
                    kind, table, op.kind, op.table
                );
                return;
            }
        }
        None => {
            log::warn!(
                "Rejecting remote op: SQL could not be classified: table={} pk={}",
                op.table,
                op.primary_key
            );
            return;
        }
    }

    match db.execute_unprepared(&final_sql).await {
        Ok(_) => {
            log::info!(
                "Applied remote op: table={} pk={}",
                op.table,
                op.primary_key
            );
        }
        Err(e) => {
            log::error!("Failed to apply remote op: {}", e);
            return;
        }
    }

    // Log the operation
    if let Err(e) = sync_log::insert_op(&db, &op).await {
        log::error!("Failed to log sync operation: {}", e);
    }

    // Update Merkle tree via channel back to engine loop
    if let Err(e) = merkle_update_tx.send(op.clone()).await {
        log::error!("Failed to send merkle update: {}", e);
    }

    // Notify change listeners
    let _ = change_tx.send(ChangeNotification {
        table: op.table.clone(),
        kind: op.kind.clone(),
        primary_key: op.primary_key.clone(),
    });
}

/// Strip the RETURNING clause from a SQL statement.
/// e.g., `INSERT INTO "tasks" (...) VALUES (...) RETURNING "id"` becomes
///        `INSERT INTO "tasks" (...) VALUES (...)`
fn strip_returning(sql: &str) -> String {
    if let Some(pos) = sql.to_ascii_uppercase().rfind(" RETURNING ") {
        sql[..pos].to_string()
    } else {
        sql.to_string()
    }
}

async fn run_compaction(db: DatabaseConnection, eviction_timeout: Duration) {
    let eviction_secs = eviction_timeout.as_secs();

    // Evict stale peers
    match peer_tracker::evict_stale_peers(&db, eviction_secs).await {
        Ok(evicted) => {
            if !evicted.is_empty() {
                log::info!("Evicted stale peers: {:?}", evicted);
            }
        }
        Err(e) => {
            log::error!("Failed to evict stale peers: {}", e);
        }
    }

    // Get minimum watermark across active peers
    let min_wm = match peer_tracker::get_min_watermark(&db).await {
        Ok(wm) => wm,
        Err(e) => {
            log::error!("Failed to get min watermark: {}", e);
            return;
        }
    };

    let cutoff = if let Some(min_wm) = min_wm {
        if min_wm > 0 {
            min_wm
        } else {
            return; // All peers at watermark 0, nothing safe to compact
        }
    } else {
        // No active peers — use safety fallback (7 days from latest HLC)
        match sync_log::get_max_hlc(&db).await {
            Ok(Some(max_hlc)) => {
                let seven_days_ns: u64 = 7 * 24 * 60 * 60 * 1_000_000_000;
                max_hlc.saturating_sub(seven_days_ns)
            }
            _ => return, // Nothing in log or error — skip compaction
        }
    };

    match sync_log::compact(&db, cutoff).await {
        Ok(result) => {
            log::info!("Sync log compaction complete: {:?}", result);
        }
        Err(e) => {
            log::error!("Sync log compaction failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ChangeNotification, WriteKind};
    use crate::registry::{TableMeta, TableRegistry};
    use crate::messages::SyncOperation;
    use sea_orm::Database;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn make_merkle_tx() -> mpsc::Sender<SyncOperation> {
        let (tx, _rx) = mpsc::channel::<SyncOperation>(16);
        tx
    }

    async fn setup_engine_test_db() -> (sea_orm::DatabaseConnection, Arc<TableRegistry>) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::sync_log::create_log_table(&db).await.unwrap();
        crate::peer_tracker::create_peers_table(&db).await.unwrap();
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)"
        ).await.unwrap();
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
        });
        (db, registry)
    }

    fn make_remote_op(table: &str, kind: WriteKind, pk: &str, sql: &str, hlc_time: u64) -> SyncOperation {
        SyncOperation {
            op_id: uuid::Uuid::new_v4().as_u128(),
            hlc_time,
            hlc_counter: 0,
            node_id: [2u8; 16],
            table: table.to_string(),
            kind,
            primary_key: pk.to_string(),
            data: Some(sql.as_bytes().to_vec()),
            columns: None,
        }
    }

    // ── strip_returning tests ──────────────────────────────────────────

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

    // ── apply_remote_op tests ──────────────────────────────────────────

    #[tokio::test]
    async fn test_apply_remote_op_unregistered_table() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "unknown",
            WriteKind::Insert,
            "1",
            r#"INSERT INTO "unknown" ("id") VALUES ('1')"#,
            100,
        );

        apply_remote_op(db, tx, registry, make_merkle_tx(), op).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification for unregistered table"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_lww_rejects_older() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert a newer op into the sync log first
        let newer_op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "pk-1",
            r#"INSERT INTO "tasks" ("id", "title", "done") VALUES ('pk-1', 'First', 0)"#,
            200,
        );
        sync_log::insert_op(&db, &newer_op).await.unwrap();

        // Now try to apply an older op for the same row
        let older_op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "pk-1",
            r#"INSERT INTO "tasks" ("id", "title", "done") VALUES ('pk-1', 'Older', 0)"#,
            100,
        );

        apply_remote_op(db, tx, registry, make_merkle_tx(), older_op).await;
        assert!(
            rx.try_recv().is_err(),
            "Older op should be rejected by LWW"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_no_data() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let mut op = make_remote_op("tasks", WriteKind::Insert, "1", "", 100);
        op.data = None;

        apply_remote_op(db, tx, registry, make_merkle_tx(), op).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification when data is None"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_invalid_utf8() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let mut op = make_remote_op("tasks", WriteKind::Insert, "1", "", 100);
        op.data = Some(vec![0xFF, 0xFE]);

        apply_remote_op(db, tx, registry, make_merkle_tx(), op).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification for invalid UTF-8"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_multi_statement_sql() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "1",
            r#"INSERT INTO "tasks" ("id") VALUES ('1'); DROP TABLE tasks"#,
            100,
        );

        apply_remote_op(db, tx, registry, make_merkle_tx(), op).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification for multi-statement SQL"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_sql_kind_mismatch() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "1",
            r#"UPDATE "tasks" SET "title" = 'Hacked' WHERE "id" = '1'"#,
            100,
        );

        apply_remote_op(db, tx, registry, make_merkle_tx(), op).await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification when SQL kind mismatches declared kind"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_success_insert() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "test-1",
            r#"INSERT INTO "tasks" ("id", "title", "done") VALUES ('test-1', 'Test Task', 0)"#,
            100,
        );

        apply_remote_op(db.clone(), tx, registry, make_merkle_tx(), op).await;

        // Should receive a notification
        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "test-1");

        // Row should exist in the database
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

        // Sync log entry should exist
        let ops = sync_log::get_ops_since(&db, 0).await.unwrap();
        assert!(
            ops.iter().any(|o| o.primary_key == "test-1"),
            "Sync log should contain the op"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_op_converts_insert_to_or_replace() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "conv-1",
            r#"INSERT INTO "tasks" ("id", "title", "done") VALUES ('conv-1', 'Converted', 0)"#,
            100,
        );

        apply_remote_op(db.clone(), tx, registry, make_merkle_tx(), op).await;

        // Verify the row was inserted
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'conv-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist after INSERT → OR REPLACE conversion");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Converted");
    }

    #[tokio::test]
    async fn test_apply_remote_op_preserves_or_replace() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "repl-1",
            r#"INSERT OR REPLACE INTO "tasks" ("id", "title", "done") VALUES ('repl-1', 'Replaced', 0)"#,
            100,
        );

        apply_remote_op(db.clone(), tx, registry, make_merkle_tx(), op).await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "repl-1");

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'repl-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Replaced");
    }

    #[tokio::test]
    async fn test_apply_remote_op_converts_or_ignore() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let op = make_remote_op(
            "tasks",
            WriteKind::Insert,
            "ign-1",
            r#"INSERT OR IGNORE INTO "tasks" ("id", "title", "done") VALUES ('ign-1', 'Was Ignore', 0)"#,
            100,
        );

        apply_remote_op(db.clone(), tx, registry, make_merkle_tx(), op).await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "ign-1");

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'ign-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist after OR IGNORE → OR REPLACE conversion");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Was Ignore");
    }

    // ── run_compaction tests ───────────────────────────────────────────

    #[tokio::test]
    async fn test_run_compaction_with_peers() {
        let (db, _registry) = setup_engine_test_db().await;

        // Add a peer with watermark=200
        peer_tracker::upsert_peer(&db, "peer-1", &[1u8; 16], 200).await.unwrap();

        // Add sync log ops at different HLC times
        let op100 = make_remote_op("tasks", WriteKind::Insert, "a", "sql", 100);
        let op150 = make_remote_op("tasks", WriteKind::Insert, "b", "sql", 150);
        let op300 = make_remote_op("tasks", WriteKind::Insert, "c", "sql", 300);
        sync_log::insert_op(&db, &op100).await.unwrap();
        sync_log::insert_op(&db, &op150).await.unwrap();
        sync_log::insert_op(&db, &op300).await.unwrap();

        run_compaction(db.clone(), Duration::from_secs(7 * 24 * 3600)).await;

        let remaining = sync_log::get_ops_since(&db, 0).await.unwrap();
        // Only op at hlc_time=300 should remain (100 and 150 are below cutoff=200)
        assert_eq!(remaining.len(), 1, "Expected only 1 op remaining, got {}", remaining.len());
        assert_eq!(remaining[0].hlc_time, 300);
    }

    #[tokio::test]
    async fn test_run_compaction_no_peers_fallback() {
        let (db, _registry) = setup_engine_test_db().await;

        // No peers added — fallback to max_hlc - 7 days
        let op100 = make_remote_op("tasks", WriteKind::Insert, "a", "sql", 100);
        let op200 = make_remote_op("tasks", WriteKind::Insert, "b", "sql", 200);
        let op300 = make_remote_op("tasks", WriteKind::Insert, "c", "sql", 300);
        sync_log::insert_op(&db, &op100).await.unwrap();
        sync_log::insert_op(&db, &op200).await.unwrap();
        sync_log::insert_op(&db, &op300).await.unwrap();

        run_compaction(db.clone(), Duration::from_secs(7 * 24 * 3600)).await;

        // 7 days in nanoseconds is huge (~6e14), max_hlc is 300, saturating_sub → cutoff=0
        // So nothing should be compacted (compact deletes where hlc_time < cutoff, cutoff=0 means nothing)
        let remaining = sync_log::get_ops_since(&db, 0).await.unwrap();
        assert_eq!(remaining.len(), 3, "No ops should be compacted with fallback, got {}", remaining.len());
    }

    #[tokio::test]
    async fn test_run_compaction_all_zero_watermark() {
        let (db, _registry) = setup_engine_test_db().await;

        // Add a peer with watermark=0
        peer_tracker::upsert_peer(&db, "peer-zero", &[3u8; 16], 0).await.unwrap();

        let op100 = make_remote_op("tasks", WriteKind::Insert, "a", "sql", 100);
        let op200 = make_remote_op("tasks", WriteKind::Insert, "b", "sql", 200);
        sync_log::insert_op(&db, &op100).await.unwrap();
        sync_log::insert_op(&db, &op200).await.unwrap();

        run_compaction(db.clone(), Duration::from_secs(7 * 24 * 3600)).await;

        // Watermark=0 means early return — nothing compacted
        let remaining = sync_log::get_ops_since(&db, 0).await.unwrap();
        assert_eq!(remaining.len(), 2, "No ops should be compacted when watermark is 0, got {}", remaining.len());
    }
}
