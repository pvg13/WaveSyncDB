use std::time::Duration;

use libp2p::{
    autonat, connection_limits, dcutr, identify, mdns, ping, relay, rendezvous, request_response,
    swarm::behaviour::toggle::Toggle,
};
use libp2p_swarm_derive::NetworkBehaviour;

use super::auth_protocol::{
    AUTH_CHALLENGE_PROTOCOL, AUTH_RESULT_PROTOCOL, AuthChallengeCodec, AuthResultCodec,
};
use super::push_protocol::{PUSH_PROTOCOL, PushCodec};
use super::snapshot_protocol::{SNAPSHOT_PROTOCOL, SnapshotCodec};

#[derive(NetworkBehaviour)]
pub struct WaveSyncBehaviour {
    pub connection_limits: connection_limits::Behaviour,
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub mdns: Toggle<mdns::tokio::Behaviour>,
    pub snapshot: request_response::Behaviour<SnapshotCodec>,
    pub push: request_response::Behaviour<PushCodec>,
    pub relay_client: relay::client::Behaviour,
    pub dcutr: dcutr::Behaviour,
    pub autonat: autonat::v2::client::Behaviour,
    pub rendezvous: rendezvous::client::Behaviour,
    pub auth: request_response::Behaviour<AuthChallengeCodec>,
    pub auth_result: request_response::Behaviour<AuthResultCodec>,
}

impl WaveSyncBehaviour {
    pub fn new(
        key: &libp2p::identity::Keypair,
        relay_client: relay::client::Behaviour,
        mdns_config: mdns::Config,
        keep_alive_interval: Duration,
    ) -> Self {
        let identify_behaviour = identify::Behaviour::new(identify::Config::new(
            "/wavesync/2.0.0".into(),
            key.public(),
        ));

        let ping_behaviour =
            ping::Behaviour::new(ping::Config::new().with_interval(keep_alive_interval));

        let peer_id = key.public().to_peer_id();

        let mdns_behaviour = match mdns::tokio::Behaviour::new(mdns_config, peer_id) {
            Ok(mdns) => Toggle::from(Some(mdns)),
            Err(e) => {
                log::warn!("mDNS unavailable: {e}");
                Toggle::from(None)
            }
        };

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

        let auth_behaviour = request_response::Behaviour::new(
            [(
                AUTH_CHALLENGE_PROTOCOL,
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        let auth_result_behaviour = request_response::Behaviour::new(
            [(
                AUTH_RESULT_PROTOCOL,
                request_response::ProtocolSupport::Full,
            )],
            request_response::Config::default(),
        );

        let conn_limits =
            connection_limits::ConnectionLimits::default().with_max_established_per_peer(Some(1));

        Self {
            connection_limits: connection_limits::Behaviour::new(conn_limits),
            ping: ping_behaviour,
            identify: identify_behaviour,
            mdns: mdns_behaviour,
            snapshot: snapshot_behaviour,
            push: push_behaviour,
            relay_client,
            dcutr: dcutr_behaviour,
            autonat: autonat_behaviour,
            rendezvous: rendezvous_behaviour,
            auth: auth_behaviour,
            auth_result: auth_result_behaviour,
        }
    }
}
