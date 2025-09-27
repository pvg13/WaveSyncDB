use libp2p::{
    gossipsub, identify, mdns, ping
};
use libp2p_swarm_derive::NetworkBehaviour;


#[derive(NetworkBehaviour)]
pub struct WaveSyncBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

impl WaveSyncBehaviour {
    pub fn new(key: &libp2p::identity::Keypair) -> Self {

        let identify_behaviour = identify::Behaviour::new(identify::Config::new(
                    "/wavesync/1.0.0".into(),
                    key.public()
                ));

        let ping_behaviour = ping::Behaviour::default();

        let peer_id = key.public().to_peer_id();

        let mdns_behaviour = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id).unwrap();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(std::time::Duration::from_secs(10))
            .build()
            .expect("Valid config");

        Self {
            ping: ping_behaviour,
            identify: identify_behaviour,
            mdns: mdns_behaviour,
            gossipsub: gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Signed(key.clone()), gossipsub_config).unwrap(),
        }
    }
}


