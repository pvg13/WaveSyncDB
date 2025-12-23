mod behaviour;

use std::{collections::HashMap, string};

use diesel::Connection;
use diesel_async::AsyncConnection;
use libp2p::{SwarmBuilder, futures::StreamExt, gossipsub::{self, IdentTopic, Topic}, identify, identity, mdns, noise, ping, swarm::{self, SwarmEvent}, yamux};
use tokio::{select, sync::mpsc};
use behaviour::WaveSyncBehaviour;

use crate::{sync::behaviour::WaveSyncBehaviourEvent, types::DbQuery};

// #[cfg(feature="async")]
// type WSConnection = AsyncConnection;
// #[cfg(not(feature="async"))]
// type WSConnection = Connection;


pub struct WaveSyncEngine<T: Connection> {
    peer_id: identity::PeerId,
    query_rx: mpsc::Receiver<DbQuery>,
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    topic: IdentTopic,
    connection: T,
}

impl<T: Connection> WaveSyncEngine<T> {
    pub fn new(query_rx: mpsc::Receiver<DbQuery>, connection: T, topic: impl Into<String>) -> Self {

        let builder = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(Default::default(),
                noise::Config::new,
                yamux::Config::default).unwrap()
            .with_quic()
            .with_behaviour(|key| {
                WaveSyncBehaviour::new(key)
            }).unwrap();
        
        // #[cfg(feature="wasm")]
        // builder.with_wasm_bindgen();

        let swarm = builder.build();

            // .build();

        let peer_id = swarm.local_peer_id().clone();

        let topic = gossipsub::IdentTopic::new(topic.into());

        WaveSyncEngine { query_rx, swarm, peers: HashMap::new(), peer_id, connection, topic }
    }

    pub fn subscribe(&mut self, topic: impl Into<String>) {
        
    }

    pub async fn run(&mut self) {

        // Tell the swarm to listen on all interfaces and a random, OS-assigned
        // port.
        self.swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();

        self.swarm.behaviour_mut().gossipsub.subscribe(&self.topic).unwrap();

        loop {
            select! {
                Some(query) = self.query_rx.recv() => {
                    log::debug!("Received query to sync: {}", query.sql());
                    // Here you would handle the query, e.g., broadcast it to peers
                    // Build the topic
                    let topic: IdentTopic = Topic::new(query.topic().to_string());


                    // let topic_clone = topic.clone();
                    for (peer_id, _addr) in &self.peers {
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
                            _ => {}
                        }
                    },
                    _ => {}
                }
            }
            
        }
    }
}