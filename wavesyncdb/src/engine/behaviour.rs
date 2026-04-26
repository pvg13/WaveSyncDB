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
        let identify_behaviour = identify::Behaviour::new(
            identify::Config::new("/wavesync/2.0.0".into(), key.public())
                // Push identify updates to peers when our listen addresses
                // change (network switch, AutoNAT confirms a new external,
                // circuit-relay reservation succeeds). Without this, the
                // relay's `peer_addresses` snapshot for this peer is whatever
                // identify happened to send at first connection — and stays
                // stale until reconnect. With it on, network changes propagate
                // within seconds, so FCM payloads always contain the peer's
                // current reachable set.
                .with_push_listen_addr_updates(true),
        );

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
            request_response::Config::default().with_request_timeout(Duration::from_secs(30)),
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

        // Up to 2 simultaneous connections per peer.
        //
        // Why 2: DCUtR upgrades by dialing a *direct* connection to the same
        // peer while the circuit-relay one is still alive, then migrating
        // traffic to the direct connection and closing the relay one. With
        // max=1, libp2p's connection_limits behaviour rejects the upgrade
        // dial — DCUtR can never complete and sync is permanently stuck on
        // the relay path.
        //
        // Why this is safe now (it wasn't with TCP+QUIC enabled): when both
        // TCP and QUIC were active, every relay dial succeeded twice
        // (once per protocol), giving us two relay connections that
        // confused the relay-client behaviour and broke circuit-relay dials
        // with "Response from behaviour was canceled: oneshot canceled" on
        // cellular. With QUIC-only there's exactly one connection per peer,
        // so the second slot is reserved for a DCUtR-upgraded direct
        // connection without contention.
        let conn_limits = connection_limits::ConnectionLimits::default()
            .with_max_established_per_peer(Some(2))
            .with_max_pending_outgoing(Some(10));

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
