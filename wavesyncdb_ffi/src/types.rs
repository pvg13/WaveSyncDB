use wavesyncdb::network_status::{
    NatStatus, NetworkStatus, PeerInfo, RelayStatus,
};
use wavesyncdb::messages::{ChangeNotification, WriteKind};

#[derive(uniffi::Record, Clone, Debug)]
pub struct FfiPeerInfo {
    pub peer_id: String,
    pub address: String,
    pub db_version: Option<u64>,
    pub is_bootstrap: bool,
    pub is_group_member: bool,
    pub app_id: Option<String>,
}

#[derive(uniffi::Enum, Clone, Debug)]
pub enum FfiRelayStatus {
    Disabled,
    Connecting,
    Connected,
    Listening,
}

#[derive(uniffi::Enum, Clone, Debug)]
pub enum FfiNatStatus {
    Unknown,
    Public,
    Private,
}

#[derive(uniffi::Record, Clone, Debug)]
pub struct FfiNetworkStatus {
    pub local_peer_id: String,
    pub peer_count: u32,
    pub group_peer_count: u32,
    pub relay_status: FfiRelayStatus,
    pub nat_status: FfiNatStatus,
    pub topic: String,
    pub rendezvous_registered: bool,
    pub push_registered: bool,
    pub local_db_version: u64,
    pub registry_ready: bool,
    pub peers: Vec<FfiPeerInfo>,
}

#[derive(uniffi::Enum, Clone, Debug)]
pub enum FfiWriteKind {
    Insert,
    Update,
    Delete,
}

#[derive(uniffi::Record, Clone, Debug)]
pub struct FfiChangeNotification {
    pub table: String,
    pub kind: FfiWriteKind,
    pub primary_key: String,
    pub changed_columns: Option<Vec<String>>,
}

impl From<&PeerInfo> for FfiPeerInfo {
    fn from(p: &PeerInfo) -> Self {
        Self {
            peer_id: p.peer_id.0.clone(),
            address: p.address.clone(),
            db_version: p.db_version,
            is_bootstrap: p.is_bootstrap,
            is_group_member: p.is_group_member,
            app_id: p.app_id.clone(),
        }
    }
}

impl From<&RelayStatus> for FfiRelayStatus {
    fn from(s: &RelayStatus) -> Self {
        match s {
            RelayStatus::Disabled => Self::Disabled,
            RelayStatus::Connecting => Self::Connecting,
            RelayStatus::Connected => Self::Connected,
            RelayStatus::Listening => Self::Listening,
        }
    }
}

impl From<&NatStatus> for FfiNatStatus {
    fn from(s: &NatStatus) -> Self {
        match s {
            NatStatus::Unknown => Self::Unknown,
            NatStatus::Public => Self::Public,
            NatStatus::Private => Self::Private,
        }
    }
}

impl From<NetworkStatus> for FfiNetworkStatus {
    fn from(s: NetworkStatus) -> Self {
        let peers: Vec<FfiPeerInfo> = s.connected_peers.iter().map(FfiPeerInfo::from).collect();
        let peer_count = s.connected_peer_count() as u32;
        let group_peer_count = s.group_peer_count() as u32;
        Self {
            local_peer_id: s.local_peer_id.0,
            peer_count,
            group_peer_count,
            relay_status: FfiRelayStatus::from(&s.relay_status),
            nat_status: FfiNatStatus::from(&s.nat_status),
            topic: s.topic,
            rendezvous_registered: s.rendezvous_registered,
            push_registered: s.push_registered,
            local_db_version: s.local_db_version,
            registry_ready: s.registry_ready,
            peers,
        }
    }
}

impl From<&WriteKind> for FfiWriteKind {
    fn from(k: &WriteKind) -> Self {
        match k {
            WriteKind::Insert => Self::Insert,
            WriteKind::Update => Self::Update,
            WriteKind::Delete => Self::Delete,
        }
    }
}

impl From<ChangeNotification> for FfiChangeNotification {
    fn from(n: ChangeNotification) -> Self {
        Self {
            table: n.table.0,
            kind: FfiWriteKind::from(&n.kind),
            primary_key: n.primary_key.0,
            changed_columns: n.changed_columns,
        }
    }
}

// ---------------------------------------------------------------------------
// Network events
// ---------------------------------------------------------------------------

use wavesyncdb::network_status::NetworkEvent;

/// FFI-safe representation of a network event.
#[derive(uniffi::Enum, Clone, Debug)]
pub enum FfiNetworkEvent {
    PeerConnected { peer: FfiPeerInfo },
    PeerDisconnected { peer_id: String },
    PeerRejected { peer_id: String },
    PeerVerified { peer_id: String },
    PeerIdentityReceived { peer_id: String, app_id: String },
    RelayStatusChanged { status: FfiRelayStatus },
    NatStatusChanged { status: FfiNatStatus },
    RendezvousStatusChanged { registered: bool },
    PeerSynced { peer_id: String, db_version: u64 },
    EngineStarted,
    EngineFailed { reason: String },
}

impl From<NetworkEvent> for FfiNetworkEvent {
    fn from(e: NetworkEvent) -> Self {
        match e {
            NetworkEvent::PeerConnected(p) => Self::PeerConnected {
                peer: FfiPeerInfo::from(&p),
            },
            NetworkEvent::PeerDisconnected(id) => Self::PeerDisconnected { peer_id: id.0 },
            NetworkEvent::PeerRejected(id) => Self::PeerRejected { peer_id: id.0 },
            NetworkEvent::PeerVerified(id) => Self::PeerVerified { peer_id: id.0 },
            NetworkEvent::PeerIdentityReceived { peer_id, app_id } => {
                Self::PeerIdentityReceived {
                    peer_id: peer_id.0,
                    app_id,
                }
            }
            NetworkEvent::RelayStatusChanged(s) => Self::RelayStatusChanged {
                status: FfiRelayStatus::from(&s),
            },
            NetworkEvent::NatStatusChanged(s) => Self::NatStatusChanged {
                status: FfiNatStatus::from(&s),
            },
            NetworkEvent::RendezvousStatusChanged { registered } => {
                Self::RendezvousStatusChanged { registered }
            }
            NetworkEvent::PeerSynced { peer_id, db_version } => Self::PeerSynced {
                peer_id: peer_id.0,
                db_version,
            },
            NetworkEvent::EngineStarted => Self::EngineStarted,
            NetworkEvent::EngineFailed { reason } => Self::EngineFailed { reason },
        }
    }
}
