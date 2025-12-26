mod behaviour;

use std::collections::HashMap;

use behaviour::WaveSyncBehaviour;
use diesel::Connection;
use libp2p::{
    SwarmBuilder,
    futures::StreamExt,
    gossipsub::{self, IdentTopic, Topic},
    identify, identity, mdns, noise, ping,
    swarm::SwarmEvent,
    yamux,
};
use tokio::{select, sync::mpsc};

use crate::{instrument::dialects::{DialectType}, sync::behaviour::WaveSyncBehaviourEvent, types::DbQuery};

// #[cfg(feature="async")]
// type WSConnection = AsyncConnection;
// #[cfg(not(feature="async"))]
// type WSConnection = Connection;

pub struct WaveSyncEngine<T: Connection> {
    _peer_id: identity::PeerId,
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    topic: IdentTopic,
    connection: T,
    rx: mpsc::Receiver<DbQuery>,
}

pub struct WaveSyncBuilder<T: Connection> {
    identity: Option<identity::Keypair>,
    connection: T,
    topic: String,
    tx: mpsc::Sender<DbQuery>,
    rx: mpsc::Receiver<DbQuery>,
}

impl<T: Connection> WaveSyncBuilder<T> {
    pub fn new(connection: T, topic: impl Into<String>) -> Self {

        let (tx, rx) = mpsc::channel(100);

        Self {
            identity: None,
            connection,
            topic: topic.into(),
            tx,
            rx
        }
    }

    pub fn with_identity(mut self, keypair: identity::Keypair) -> Self {
        self.identity = Some(keypair);
        self
    }

    pub fn connect(self, conn: &mut impl Connection, dialect: DialectType) -> Self {

        conn.set_instrumentation(crate::instrument::WaveSyncInstrument::new(
            self.tx.clone(),
            &self.topic,
            dialect,
        ));
        self
    }

    pub fn build(self) -> WaveSyncEngine<T> {
        let keypair = self.identity.unwrap_or_else(identity::Keypair::generate_ed25519);

        let builder = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_tcp(
                Default::default(),
                noise::Config::new,
                yamux::Config::default,
            )
            .unwrap()
            .with_quic()
            .with_behaviour(WaveSyncBehaviour::new)
            .unwrap();

        let swarm = builder.build();

        let peer_id = *swarm.local_peer_id();

        let topic = gossipsub::IdentTopic::new(self.topic);

        

        WaveSyncEngine {
            swarm,
            peers: HashMap::new(),
            _peer_id: peer_id,
            connection: self.connection,
            topic,
            rx: self.rx, 
        }
    }
}

impl<T: Connection> WaveSyncEngine<T> {

    pub async fn run(&mut self) {
        // Tell the swarm to listen on all interfaces and a random, OS-assigned
        // port.
        self.swarm
            .listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())
            .unwrap();

        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(&self.topic)
            .unwrap();

        loop {
            select! {
                Some(query) = self.rx.recv() => {
                    log::debug!("Received query to sync: {}", query.sql());
                    // Here you would handle the query, e.g., broadcast it to peers
                    // Build the topic
                    let topic: IdentTopic = Topic::new(query.topic().to_string());


                    // let topic_clone = topic.clone();
                    for peer_id in self.peers.keys() {
                        let message = gossipsub::Message {
                            data: query.sql().as_bytes().to_vec(),
                            sequence_number: None,
                            source: None,
                            topic: topic.hash(),
                        };
                        if let Err(e) = self.swarm.behaviour_mut().gossipsub.publish(topic.clone(), message.data) {
                            log::error!("Error publishing to peer {}: {}", peer_id, e);
                        } else {
                            log::info!("Published query to peer {}", peer_id);
                        }
                    }

                },
                event = self.swarm.select_next_some() => match event {
                    SwarmEvent::NewListenAddr { address, .. } => log::info!("Listening on {address:?}"),
                    // Prints peer id identify info is being sent to.
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. })) => {
                        log::info!("Sent identify info to {peer_id:?}")
                    }
                    // Prints out the info received via the identify event
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Received { info, .. })) => {
                        log::info!("Received {info:?}")
                    },
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Ping(ping::Event { peer, connection, result })) => {
                        log::info!("Ping event with {peer:?} over {connection:?}: {result:?}");
                    },
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Mdns(event)) => {
                        match event {
                            mdns::Event::Discovered(list) => {
                                for (peer_id, multiaddr) in list {
                                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                                    self.peers.insert(peer_id, multiaddr);
                                    // Subscribe to a topic to start receiving messages
                                    self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                                }
                            }
                            mdns::Event::Expired(list) => {
                                for (peer_id, multiaddr) in list {
                                    log::debug!("Expired peer {peer_id} at {multiaddr}");
                                    self.peers.remove(&peer_id);
                                    self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);


                                    // self.swarm.behaviour_mut().identify.remove_address(&peer_id, &multiaddr);
                                }
                            }
                        }
                    },
                    // GossipSub events
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Gossipsub(event)) => {
                        log::debug!("Gossipsub event: {event:?}");
                        match event {
                            gossipsub::Event::Message { propagation_source, message_id, message } => {
                                log::debug!("Got message: {} with id: {} from peer: {:?}", String::from_utf8_lossy(&message.data), message_id, propagation_source);
                                // Here you would handle the received message, e.g., apply the query to the local database
                                let sql = String::from_utf8_lossy(&message.data);
                                match self.connection.batch_execute(&sql) {
                                    Ok(_) => log::info!("Successfully executed query from peer {}: {}", propagation_source, sql),
                                    Err(e) => log::error!("Error executing query from peer {}: {}: {}", propagation_source, sql, e),
                                }
                            },
                            _ => {
                                log::debug!("Other gossipsub event: {:?}", event);
                            }
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}
