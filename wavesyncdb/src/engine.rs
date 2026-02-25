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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use behaviour::{WaveSyncBehaviour, WaveSyncBehaviourEvent};
use libp2p::{
    SwarmBuilder,
    futures::StreamExt,
    gossipsub::{self, IdentTopic},
    identify, identity, mdns, noise, ping,
    swarm::SwarmEvent,
    yamux,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use tokio::sync::{broadcast, mpsc};

use crate::conflict;
use crate::messages::{ChangeNotification, NodeId, SyncOperation};
use crate::registry::TableRegistry;
use crate::sync_log;

/// Start the P2P sync engine in a background tokio task.
///
/// Spawns a new tokio task that runs the libp2p swarm event loop. The engine:
/// - Listens on a random TCP port for incoming connections
/// - Subscribes to the gossipsub topic for this application
/// - Reads local [`SyncOperation`]s from `sync_rx` and publishes them
/// - Receives remote operations from gossipsub and applies them (with LWW conflict resolution)
/// - Sends [`ChangeNotification`]s on `change_tx` when remote writes are applied
pub fn start_engine(
    db: DatabaseConnection,
    sync_rx: mpsc::Receiver<SyncOperation>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    _node_id: NodeId,
    topic: String,
    relay_server: Option<String>,
) {
    tokio::spawn(async move {
        if let Err(e) = run_engine(db, sync_rx, change_tx, registry, topic, relay_server).await {
            log::error!("Engine error: {}", e);
        }
    });
}

async fn run_engine(
    db: DatabaseConnection,
    mut sync_rx: mpsc::Receiver<SyncOperation>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    topic_name: String,
    relay_server: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let keypair = identity::Keypair::generate_ed25519();

    let swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(|key, relay_client| {
            WaveSyncBehaviour::new(key, relay_client)
        })?
        .build();

    let mut engine = EngineRunner {
        swarm,
        peers: HashMap::new(),
        topic: gossipsub::IdentTopic::new(&topic_name),
        db,
        change_tx,
        registry,
        relay_server,
    };

    engine.run(&mut sync_rx).await
}

struct EngineRunner {
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    topic: IdentTopic,
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    #[allow(dead_code)] // Used in Phase 3 (full sync)
    registry: Arc<TableRegistry>,
    relay_server: Option<String>,
}

impl EngineRunner {
    async fn run(
        &mut self,
        sync_rx: &mut mpsc::Receiver<SyncOperation>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .unwrap();

        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&self.topic)
            .unwrap();

        // If a relay server is configured, dial it
        if let Some(ref relay_addr) = self.relay_server
            && let Ok(addr) = relay_addr.parse::<libp2p::Multiaddr>()
        {
            let _ = self.swarm.dial(addr);
            log::info!("Dialing relay server: {}", relay_addr);
        }

        let mut pending_full_sync = false;
        let mut full_sync_delay: Pin<Box<tokio::time::Sleep>> =
            Box::pin(tokio::time::sleep(std::time::Duration::from_secs(86400)));

        loop {
            tokio::select! {
                Some(op) = sync_rx.recv() => {
                    self.handle_local_op(op).await;
                },
                event = self.swarm.select_next_some() => {
                    if let Some(new_peer) = self.handle_swarm_event(event)
                        && new_peer
                    {
                        pending_full_sync = true;
                        full_sync_delay = Box::pin(tokio::time::sleep(
                            std::time::Duration::from_secs(2),
                        ));
                    }
                },
                () = &mut full_sync_delay, if pending_full_sync => {
                    pending_full_sync = false;
                    self.publish_full_state().await;
                }
            }
        }
    }

    async fn handle_local_op(&mut self, op: SyncOperation) {
        // Log the operation to the sync log first
        if let Err(e) = sync_log::insert_op(&self.db, &op).await {
            log::error!("Failed to log local sync operation: {}", e);
        }

        match postcard::to_allocvec(&op) {
            Ok(data) => {
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(self.topic.clone(), data)
                {
                    log::warn!("Failed to publish to gossipsub: {}", e);
                }
            }
            Err(e) => {
                log::error!("Failed to serialize operation: {}", e);
            }
        }
    }

    /// Returns `Some(true)` if a new peer was discovered (triggers full sync).
    fn handle_swarm_event(&mut self, event: SwarmEvent<WaveSyncBehaviourEvent>) -> Option<bool> {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address:?}");
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(
                identify::Event::Sent { peer_id, .. },
            )) => {
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
                let new_peer = self.handle_mdns(event);
                return Some(new_peer);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Gossipsub(event)) => {
                self.handle_gossipsub(event);
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
        None
    }

    /// Returns `true` if a truly new peer was discovered.
    fn handle_mdns(&mut self, event: mdns::Event) -> bool {
        let mut new_peer_found = false;
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, multiaddr) in list {
                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                    if !self.peers.contains_key(&peer_id) {
                        new_peer_found = true;
                    }
                    self.peers.insert(peer_id, multiaddr);
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer_id);
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
        new_peer_found
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

                let op: SyncOperation = match postcard::from_bytes(&message.data) {
                    Ok(op) => op,
                    Err(e) => {
                        log::error!(
                            "Failed to deserialize message from peer {}: {}",
                            propagation_source,
                            e
                        );
                        return;
                    }
                };

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
                tokio::spawn(async move {
                    apply_remote_op(db, change_tx, op).await;
                });
            }
            _ => {
                log::debug!("Other gossipsub event: {:?}", event);
            }
        }
    }

    async fn publish_full_state(&mut self) {
        let ops = match sync_log::get_ops_since(&self.db, 0).await {
            Ok(ops) => ops,
            Err(e) => {
                log::error!("Failed to read sync log for full state publish: {}", e);
                return;
            }
        };

        if ops.is_empty() {
            log::info!("Full sync: no operations to publish");
            return;
        }

        log::info!("Full sync: publishing {} operations", ops.len());

        for mut op in ops {
            // Assign a fresh op_id to avoid gossipsub message deduplication
            op.op_id = uuid::Uuid::new_v4().as_u128();

            match postcard::to_allocvec(&op) {
                Ok(data) => {
                    if let Err(e) = self
                        .swarm
                        .behaviour_mut()
                        .gossipsub
                        .publish(self.topic.clone(), data)
                    {
                        log::warn!("Failed to publish full sync op: {}", e);
                    }
                }
                Err(e) => {
                    log::error!("Failed to serialize full sync op: {}", e);
                }
            }
        }
    }
}

async fn apply_remote_op(
    db: DatabaseConnection,
    change_tx: broadcast::Sender<ChangeNotification>,
    op: SyncOperation,
) {
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
    if let Some(ref sql_bytes) = op.data
        && let Ok(sql) = std::str::from_utf8(sql_bytes)
    {
        // Strip RETURNING clause — remote nodes don't need it and it can cause
        // issues with execute_unprepared which doesn't return query results.
        let clean_sql = strip_returning(sql);

        // Use INSERT OR REPLACE for inserts to handle potential PK conflicts
        // between nodes (e.g., both nodes generating the same auto-increment ID).
        let final_sql = if op.kind == crate::messages::WriteKind::Insert {
            clean_sql.replacen("INSERT INTO", "INSERT OR REPLACE INTO", 1)
        } else {
            clean_sql
        };

        match db.execute_unprepared(&final_sql).await {
            Ok(_) => {
                log::info!("Applied remote op: table={} pk={}", op.table, op.primary_key);
            }
            Err(e) => {
                log::error!("Failed to apply remote op: {}", e);
                return;
            }
        }
    }

    // Log the operation
    if let Err(e) = sync_log::insert_op(&db, &op).await {
        log::error!("Failed to log sync operation: {}", e);
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
    if let Some(pos) = sql.to_uppercase().rfind(" RETURNING ") {
        sql[..pos].to_string()
    } else {
        sql.to_string()
    }
}
