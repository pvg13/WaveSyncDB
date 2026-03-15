use libp2p::{
    autonat, dcutr, gossipsub, identify, mdns, ping, relay, rendezvous, request_response,
};
use libp2p_swarm_derive::NetworkBehaviour;

use super::push_protocol::{PUSH_PROTOCOL, PushCodec};
use super::snapshot_protocol::{SNAPSHOT_PROTOCOL, SnapshotCodec};

#[derive(NetworkBehaviour)]
pub struct WaveSyncBehaviour {
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
    pub snapshot: request_response::Behaviour<SnapshotCodec>,
    pub push: request_response::Behaviour<PushCodec>,
    pub relay_client: relay::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::v2::client::Behaviour,
    pub rendezvous: rendezvous::client::Behaviour,
}

impl WaveSyncBehaviour {
    pub fn new(
        key: &libp2p::identity::Keypair,
        relay_client: relay::client::Behaviour,
        mdns_config: mdns::Config,
    ) -> Self {
        let identify_behaviour = identify::Behaviour::new(identify::Config::new(
            "/wavesync/2.0.0".into(),
            key.public(),
        ));

        let ping_behaviour = ping::Behaviour::default();

        let peer_id = key.public().to_peer_id();

        let mdns_behaviour = mdns::tokio::Behaviour::new(mdns_config, peer_id).unwrap();

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(std::time::Duration::from_secs(1))
            .mesh_n(3)
            .mesh_n_low(1)
            .mesh_n_high(6)
            .build()
            .expect("Valid gossipsub config");

        let dcutr_behaviour = dcutr::Behaviour::new(peer_id);

        let autonat_behaviour = autonat::v2::client::Behaviour::default();

        let rendezvous_behaviour = rendezvous::client::Behaviour::new(key.clone());

        let snapshot_behaviour = request_response::Behaviour::new(
            [(SNAPSHOT_PROTOCOL, request_response::ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        let push_behaviour = request_response::Behaviour::new(
            [(PUSH_PROTOCOL, request_response::ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        Self {
            ping: ping_behaviour,
            identify: identify_behaviour,
            mdns: mdns_behaviour,
            gossipsub: gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .unwrap(),
            snapshot: snapshot_behaviour,
            push: push_behaviour,
            relay_client,
            dcutr: dcutr_behaviour,
            autonat: autonat_behaviour,
            rendezvous: rendezvous_behaviour,
        }
    }
}
