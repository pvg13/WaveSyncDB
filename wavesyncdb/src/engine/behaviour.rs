use libp2p::{autonat, dcutr, gossipsub, identify, mdns, ping, relay};
use libp2p_swarm_derive::NetworkBehaviour;

#[derive(NetworkBehaviour)]
pub struct WaveSyncBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub relay_client: relay::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::v2::client::Behaviour,
}

impl WaveSyncBehaviour {
    pub fn new(
        key: &libp2p::identity::Keypair,
        relay_client: relay::client::Behaviour,
    ) -> Self {
        let identify_behaviour = identify::Behaviour::new(identify::Config::new(
            "/wavesync/2.0.0".into(),
            key.public(),
        ));

        let ping_behaviour = ping::Behaviour::default();

        let peer_id = key.public().to_peer_id();

        let mdns_behaviour =
            mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id).unwrap();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(std::time::Duration::from_secs(1))
            .build()
            .expect("Valid gossipsub config");

        let dcutr_behaviour = dcutr::Behaviour::new(peer_id);

        let autonat_behaviour = autonat::v2::client::Behaviour::default();

        Self {
            ping: ping_behaviour,
            identify: identify_behaviour,
            mdns: mdns_behaviour,
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .unwrap(),
            relay_client,
            dcutr: dcutr_behaviour,
            autonat: autonat_behaviour,
        }
    }
}
