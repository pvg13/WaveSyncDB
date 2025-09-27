mod behaviour;

use std::collections::HashMap;

use diesel::Connection;
use libp2p::{futures::StreamExt, gossipsub::{self, Topic}, identify, identity, mdns, noise, ping, swarm::SwarmEvent, yamux, SwarmBuilder};
use tokio::{select, sync::mpsc};
use behaviour::WaveSyncBehaviour;

use crate::{sync::behaviour::WaveSyncBehaviourEvent, types::DbQuery};

pub struct WaveSyncEngine<T: Connection> {
    peer_id: identity::PeerId,
    query_rx: mpsc::Receiver<DbQuery>,
    swarm: libp2p::Swarm<WaveSyncBehaviour>,
    peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    connection: T,
}

impl<T: Connection> WaveSyncEngine<T> {
    pub fn new(query_rx: mpsc::Receiver<DbQuery>, connection: T) -> Self {

        let swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(Default::default(),
                noise::Config::new,
                yamux::Config::default).unwrap()
            .with_quic()
            .with_behaviour(|key| {
                WaveSyncBehaviour::new(key)
            }).unwrap()
            .build();

        let peer_id = swarm.local_peer_id().clone();

        WaveSyncEngine { query_rx, swarm, peers: HashMap::new(), peer_id, connection }
    }



    pub async fn run(&mut self) {

        // Tell the swarm to listen on all interfaces and a random, OS-assigned
        // port.
        self.swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap()).unwrap();

        let peer_id_local = self.peer_id;
        

        let topic = gossipsub::IdentTopic::new(format!("wavesync-queries"));
        self.swarm.behaviour_mut().gossipsub.subscribe(&topic).unwrap();


        loop {
            select! {
                Some(query) = self.query_rx.recv() => {
                    log::debug!("Received query to sync: {}", query.sql());
                    // Here you would handle the query, e.g., broadcast it to peers
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
                    SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
                    // Prints peer id identify info is being sent to.
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Sent { peer_id, .. })) => {
                        println!("Sent identify info to {peer_id:?}")
                    }
                    // Prints out the info received via the identify event
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Received { info, .. })) => {
                        println!("Received {info:?}")
                    },
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Ping(ping::Event { peer, connection, result })) => {
                        println!("Ping event with {peer:?} over {connection:?}: {result:?}");
                    },
                    SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Mdns(event)) => {
                        match event {
                            mdns::Event::Discovered(list) => {
                                for (peer_id, multiaddr) in list {
                                    println!("Discovered peer {peer_id} at {multiaddr}");
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