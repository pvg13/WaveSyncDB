mod behaviour;

use std::collections::HashMap;

use behaviour::WaveSyncBehaviour;
use libp2p::{
    SwarmBuilder,
    futures::StreamExt,
    gossipsub::{self, IdentTopic, Topic},
    identify, identity, mdns, noise, ping,
    swarm::SwarmEvent,
    yamux,
};
use tokio::{select, sync::{OnceCell, mpsc}};

use crate::{engine::behaviour::WaveSyncBehaviourEvent, messages::Operation};

pub(crate) static ENGINE_TX: OnceCell<mpsc::Sender<Operation>> = OnceCell::const_new();

pub struct WaveSyncEngine {
    _peer_id: identity::PeerId,
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    topic: IdentTopic,
    rx: mpsc::Receiver<Operation>,
}

pub struct WaveSyncBuilder {
    identity: Option<identity::Keypair>,
    topic: String,
    rx: mpsc::Receiver<Operation>,
    tx: mpsc::Sender<Operation>,
}

impl WaveSyncBuilder {
    pub fn new(topic: impl Into<String>) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            identity: None,
            topic: topic.into(),
            rx,
            tx,
        }
    }

    pub fn with_identity(mut self, keypair: identity::Keypair) -> Self {
        self.identity = Some(keypair);
        self
    }

    pub fn build(self) -> WaveSyncEngine {
        let keypair = self
            .identity
            .unwrap_or_else(identity::Keypair::generate_ed25519);

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

        // Set the global sender for the engine
        ENGINE_TX.set(self.tx).expect("Failed to set engine sender");

        // Start the database
        
        // Start the engine's main loop in a background task

        WaveSyncEngine {
            swarm,
            peers: HashMap::new(),
            _peer_id: peer_id,
            topic,
            rx: self.rx,
        }
    }
}

impl WaveSyncEngine {

    pub async fn submit(op: Operation) {
        if let Some(tx) = ENGINE_TX.get() {
            let _ = tx.send(op).await;
        } else {
            log::error!("Engine sender not initialized");
        }
    }

    pub fn start(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(&mut self) {
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
                Some(op) = self.rx.recv() => {
                    self.swarm.behaviour_mut().gossipsub.publish(self.topic.clone(), postcard::to_allocvec(&op).unwrap());
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

                                let topic = &message.topic;
                                log::info!("Received message on topic {topic} from peer {propagation_source:?}: {message_id:?}");

                                // Unpack the message and apply it to the local database
                                let op: Operation = match postcard::from_bytes(&message.data) {
                                    Ok(op) => op,
                                    Err(e) => {
                                        log::error!("Failed to deserialize message from peer {}: {}", propagation_source, e);
                                        return;
                                    }
                                };

                                log::info!("Deserialized operation from peer {}: {:?}", propagation_source, op);

                                let _ = op.execute().await;

                                
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
