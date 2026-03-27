//! Network status types exposed to application developers.
//!
//! These types provide a snapshot of the P2P engine's internal state without
//! leaking any libp2p types. The engine is the sole writer of [`NetworkStatus`];
//! consumers get read-only snapshots via [`WaveSyncDb::network_status()`](crate::WaveSyncDb::network_status).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque peer identifier (wraps libp2p PeerId string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub String);

impl fmt::Display for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Information about a connected peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Opaque peer identifier.
    pub peer_id: PeerId,
    /// Multiaddr as a string.
    pub address: String,
    /// Last-known db_version from this peer (`None` if no sync yet).
    pub db_version: Option<u64>,
    /// Whether this peer was configured as a bootstrap peer.
    pub is_bootstrap: bool,
    /// Whether this peer is a group member (same topic/passphrase).
    /// Starts `false` and flips to `true` after a successful HMAC-verified exchange.
    pub is_group_member: bool,
    /// Application-defined identity announced by this peer (ephemeral, session-scoped).
    pub app_id: Option<String>,
}

/// Relay connection status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RelayStatus {
    /// No relay server configured.
    Disabled,
    /// Connecting to relay server.
    Connecting,
    /// TCP/QUIC connection established with relay peer.
    Connected,
    /// Listening on a relay circuit (reservation accepted).
    Listening,
}

/// NAT detection status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NatStatus {
    /// NAT status not yet determined.
    Unknown,
    /// Public address detected.
    Public,
    /// Behind NAT.
    Private,
}

/// Snapshot of the full network state.
///
/// Obtained via [`WaveSyncDb::network_status()`](crate::WaveSyncDb::network_status).
/// The engine is the sole writer; reads are cheap (RwLock).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStatus {
    /// This node's peer ID.
    pub local_peer_id: PeerId,
    /// All currently connected peers.
    pub connected_peers: Vec<PeerInfo>,
    /// The sync topic (effective, after PSK derivation).
    pub topic: String,
    /// Relay connection status.
    pub relay_status: RelayStatus,
    /// Detected NAT status.
    pub nat_status: NatStatus,
    /// Whether we have an active rendezvous registration.
    pub rendezvous_registered: bool,
    /// Whether push token has been registered with the relay.
    pub push_registered: bool,
    /// Current local db_version.
    pub local_db_version: u64,
    /// Whether the schema registry is ready.
    pub registry_ready: bool,
}

impl NetworkStatus {
    /// Returns only peers that are group members (same topic/passphrase).
    pub fn group_peers(&self) -> Vec<&PeerInfo> {
        self.connected_peers
            .iter()
            .filter(|p| p.is_group_member)
            .collect()
    }

    /// Number of group peers.
    pub fn group_peer_count(&self) -> usize {
        self.connected_peers
            .iter()
            .filter(|p| p.is_group_member)
            .count()
    }

    /// Total number of connected peers (including non-group).
    pub fn connected_peer_count(&self) -> usize {
        self.connected_peers.len()
    }
}

impl Default for NetworkStatus {
    fn default() -> Self {
        Self {
            local_peer_id: PeerId(String::new()),
            connected_peers: Vec::new(),
            topic: String::new(),
            relay_status: RelayStatus::Disabled,
            nat_status: NatStatus::Unknown,
            rendezvous_registered: false,
            push_registered: false,
            local_db_version: 0,
            registry_ready: false,
        }
    }
}

/// Events emitted when network state changes.
///
/// Subscribe via [`WaveSyncDb::network_event_rx()`](crate::WaveSyncDb::network_event_rx).
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// A new peer connected.
    PeerConnected(PeerInfo),
    /// A peer disconnected.
    PeerDisconnected(PeerId),
    /// A peer was rejected (topic/passphrase mismatch).
    PeerRejected(PeerId),
    /// A peer was verified via successful HMAC exchange.
    PeerVerified(PeerId),
    /// A peer announced its application-level identity.
    PeerIdentityReceived { peer_id: PeerId, app_id: String },
    /// Relay connection status changed.
    RelayStatusChanged(RelayStatus),
    /// NAT detection status changed.
    NatStatusChanged(NatStatus),
    /// Rendezvous registration status changed.
    RendezvousStatusChanged { registered: bool },
    /// Version vector sync completed with a peer.
    PeerSynced { peer_id: PeerId, db_version: u64 },
    /// Engine has started and initial network status is available.
    EngineStarted,
    /// Engine failed with an error or panic.
    EngineFailed { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_id_display() {
        let id = PeerId("12D3KooWTest".to_string());
        assert_eq!(format!("{id}"), "12D3KooWTest");
    }

    #[test]
    fn test_network_status_default() {
        let status = NetworkStatus::default();
        assert_eq!(status.connected_peer_count(), 0);
        assert_eq!(status.group_peer_count(), 0);
        assert!(status.group_peers().is_empty());
        assert_eq!(status.relay_status, RelayStatus::Disabled);
        assert_eq!(status.nat_status, NatStatus::Unknown);
    }

    #[test]
    fn test_group_peer_filtering() {
        let status = NetworkStatus {
            connected_peers: vec![
                PeerInfo {
                    peer_id: PeerId("a".into()),
                    address: "/ip4/1.2.3.4".into(),
                    db_version: Some(5),
                    is_bootstrap: false,
                    is_group_member: true,
                    app_id: None,
                },
                PeerInfo {
                    peer_id: PeerId("b".into()),
                    address: "/ip4/5.6.7.8".into(),
                    db_version: None,
                    is_bootstrap: false,
                    is_group_member: false,
                    app_id: None,
                },
                PeerInfo {
                    peer_id: PeerId("c".into()),
                    address: "/ip4/9.10.11.12".into(),
                    db_version: Some(10),
                    is_bootstrap: true,
                    is_group_member: true,
                    app_id: None,
                },
            ],
            ..Default::default()
        };
        assert_eq!(status.connected_peer_count(), 3);
        assert_eq!(status.group_peer_count(), 2);
        let group = status.group_peers();
        assert_eq!(group.len(), 2);
        assert_eq!(group[0].peer_id, PeerId("a".into()));
        assert_eq!(group[1].peer_id, PeerId("c".into()));
    }

    #[test]
    fn test_peer_id_serde_roundtrip() {
        let id = PeerId("12D3KooWTest".to_string());
        let json = serde_json::to_string(&id).unwrap();
        let back: PeerId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn test_network_status_serde_roundtrip() {
        let status = NetworkStatus {
            local_peer_id: PeerId("local".into()),
            connected_peers: vec![PeerInfo {
                peer_id: PeerId("remote".into()),
                address: "/ip4/1.2.3.4/tcp/1234".into(),
                db_version: Some(42),
                is_bootstrap: false,
                is_group_member: true,
                app_id: None,
            }],
            topic: "my-topic".into(),
            relay_status: RelayStatus::Connected,
            nat_status: NatStatus::Private,
            rendezvous_registered: true,
            push_registered: false,
            local_db_version: 100,
            registry_ready: true,
        };
        let json = serde_json::to_string(&status).unwrap();
        let back: NetworkStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back.local_peer_id, status.local_peer_id);
        assert_eq!(back.connected_peer_count(), 1);
        assert_eq!(back.relay_status, RelayStatus::Connected);
        assert_eq!(back.nat_status, NatStatus::Private);
    }

    #[test]
    fn test_relay_status_variants() {
        assert_ne!(RelayStatus::Disabled, RelayStatus::Connecting);
        assert_ne!(RelayStatus::Connected, RelayStatus::Listening);
    }

    #[test]
    fn test_nat_status_variants() {
        assert_ne!(NatStatus::Unknown, NatStatus::Public);
        assert_ne!(NatStatus::Public, NatStatus::Private);
    }
}
