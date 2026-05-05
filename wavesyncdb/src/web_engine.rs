//! Browser/wasm32 P2P engine — minimal real-time changeset fan-out with
//! optional IndexedDB-backed persistence.
//!
//! This is a parallel, browser-only sibling to [`crate::engine`]. It shares
//! the same wire format ([`SyncRequest::Push`] / [`SyncResponse::PushAck`],
//! protocol id `/wavesync/snapshot/3.0.0`, length-prefixed serde_json) so a
//! browser peer can talk to a native peer without protocol changes.
//!
//! ## Two flavours
//!
//! - [`WebSyncClient::connect`] — ephemeral. Identity, db_version, and
//!   shadow state live in memory only. A reload starts fresh and looks
//!   like a brand-new peer to the network.
//! - [`WebSyncClient::connect_persistent`] — IndexedDB-backed. Identity is
//!   restored on every reload (same `PeerId`), `db_version` is monotonic
//!   across reloads, and incoming changes are conflict-resolved against
//!   the persisted shadow table and only the winners are surfaced. This
//!   is what enables real local-first apps.
//!
//! ## What persistence covers
//!
//! - **Identity** — a single ed25519 keypair is generated on first load
//!   and persisted in the `meta` IndexedDB store. Subsequent reloads
//!   restore it and present the same `PeerId` to the network.
//! - **Local CRDT state** — every successful local write or accepted
//!   remote change is written to the `shadow` store, keyed by
//!   `(table, pk, cid)`. Conflict resolution on the next incoming change
//!   sees the persisted state.
//! - **Local db_version** — incremented on every local write and persisted
//!   so version-vector catch-up (when added) starts from the right place.
//!
//! ## What persistence does NOT cover (yet)
//!
//! - **Application data.** The browser app owns its own data store
//!   (IndexedDB rows, in-memory state, etc.). The engine emits resolved
//!   `ColumnChange` events via [`WebSyncClient::subscribe_resolved`]; the
//!   app applies them.
//! - **Version-vector catch-up.** Per-peer `last_db_version` is written
//!   but never read on this branch; a future revision will add the
//!   request/response side.
//! - **Tombstones / deletes.** Local writes go through `submit_local_write`,
//!   which is insert/update only. Apps that need deletes can build a
//!   `SyncChangeset` with `__deleted` columns by hand and call
//!   [`WebSyncClient::publish`].

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId as LibPeerId, Swarm, SwarmBuilder, identify, identity, noise, ping,
    request_response,
    swarm::{NetworkBehaviour, SwarmEvent},
    yamux,
};
use tokio::sync::{Mutex, Notify, broadcast, mpsc, oneshot, watch};

use crate::auth::GroupKey;
use crate::conflict;
use crate::messages::{ColumnChange, ColumnName, NodeId, PrimaryKey, SyncChangeset, TableName};
use crate::protocol::{SyncRequest, SyncResponse};
use crate::web_entity::BrowserEntity;
use crate::web_store::{BrowserStore, ShadowRow};

// Re-use the native engine's snapshot codec — same wire format, same
// protocol id. Cargo gates engine/* away from wasm32, so we cannot pull
// the codec from there. The codec is small (~80 LoC) and pure
// `futures::AsyncRead/Write`, so we duplicate it here behind the wasm32
// gate. Keeping the byte-for-byte protocol id `/wavesync/snapshot/3.0.0`
// is what guarantees a browser client can talk to a native peer.
mod snapshot_codec {
    use std::io;

    use async_trait::async_trait;
    use futures::prelude::*;
    use libp2p::StreamProtocol;
    use libp2p::request_response;

    use crate::protocol::{SyncRequest, SyncResponse};

    pub const SNAPSHOT_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/snapshot/3.0.0");

    #[derive(Debug, Clone, Default)]
    pub struct SnapshotCodec;

    #[async_trait]
    impl request_response::Codec for SnapshotCodec {
        type Protocol = StreamProtocol;
        type Request = SyncRequest;
        type Response = SyncResponse;

        async fn read_request<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Request>
        where
            T: AsyncRead + Unpin + Send,
        {
            let bytes = read_lp(io).await?;
            serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }

        async fn read_response<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Response>
        where
            T: AsyncRead + Unpin + Send,
        {
            let bytes = read_lp(io).await?;
            serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }

        async fn write_request<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
            req: Self::Request,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let bytes = serde_json::to_vec(&req)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            write_lp(io, &bytes).await
        }

        async fn write_response<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
            res: Self::Response,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let bytes = serde_json::to_vec(&res)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            write_lp(io, &bytes).await
        }
    }

    async fn read_lp<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 64 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("payload too large: {len}"),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn write_lp<T: AsyncWrite + Unpin>(io: &mut T, data: &[u8]) -> io::Result<()> {
        io.write_all(&(data.len() as u32).to_be_bytes()).await?;
        io.write_all(data).await?;
        io.flush().await
    }
}

use snapshot_codec::{SNAPSHOT_PROTOCOL, SnapshotCodec};

// Inline copy of `engine/push_protocol.rs` — needed here because
// `engine/*` is gated to `not(target_arch = "wasm32")`. Wire shape MUST
// stay byte-for-byte identical to the native version: protocol id
// `/wavesync/push/1.0.0`, length-prefixed serde_json with a 1 MiB cap.
// If `engine/push_protocol.rs` ever changes the wire format, this copy
// must move in lockstep.
mod push_codec {
    use std::io;

    use async_trait::async_trait;
    use futures::prelude::*;
    use libp2p::StreamProtocol;
    use libp2p::request_response;
    use serde::{Deserialize, Serialize};

    pub const PUSH_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/push/1.0.0");

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub enum PushPlatform {
        Fcm,
        Apns,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum PushRequest {
        RegisterToken {
            topic: String,
            platform: PushPlatform,
            token: String,
        },
        UnregisterToken {
            topic: String,
            token: String,
        },
        NotifyTopic {
            topic: String,
            sender_site_id: String,
        },
        AnnouncePresence {
            topic: String,
        },
        PeerJoined {
            topic: String,
            peer_addrs: Vec<String>,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum PushResponse {
        Ok,
        Error { message: String },
        PeerList { peers: Vec<String> },
    }

    #[derive(Debug, Clone, Default)]
    pub struct PushCodec;

    #[async_trait]
    impl request_response::Codec for PushCodec {
        type Protocol = StreamProtocol;
        type Request = PushRequest;
        type Response = PushResponse;

        async fn read_request<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Request>
        where
            T: AsyncRead + Unpin + Send,
        {
            let bytes = read_lp(io).await?;
            serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }

        async fn read_response<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
        ) -> io::Result<Self::Response>
        where
            T: AsyncRead + Unpin + Send,
        {
            let bytes = read_lp(io).await?;
            serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }

        async fn write_request<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
            req: Self::Request,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let bytes = serde_json::to_vec(&req)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            write_lp(io, &bytes).await
        }

        async fn write_response<T>(
            &mut self,
            _p: &Self::Protocol,
            io: &mut T,
            res: Self::Response,
        ) -> io::Result<()>
        where
            T: AsyncWrite + Unpin + Send,
        {
            let bytes = serde_json::to_vec(&res)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            write_lp(io, &bytes).await
        }
    }

    async fn read_lp<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<Vec<u8>> {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        // Push messages are small — 1 MiB cap matches native.
        if len > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("push payload too large: {len}"),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        Ok(buf)
    }

    async fn write_lp<T: AsyncWrite + Unpin>(io: &mut T, data: &[u8]) -> io::Result<()> {
        io.write_all(&(data.len() as u32).to_be_bytes()).await?;
        io.write_all(data).await?;
        io.flush().await
    }
}

use push_codec::{PUSH_PROTOCOL, PushCodec, PushRequest, PushResponse};

/// Errors surfaced from the browser sync client.
#[derive(Debug, thiserror::Error)]
pub enum WebSyncError {
    #[error("multiaddr parse failed: {0}")]
    InvalidMultiaddr(String),
    #[error("swarm setup failed: {0}")]
    Setup(String),
    #[error("dial failed: {0}")]
    Dial(String),
    #[error("client task is not running")]
    NotRunning,
    #[error("storage error: {0}")]
    Store(String),
    #[error("identity decode failed: {0}")]
    Identity(String),
}

#[derive(NetworkBehaviour)]
struct WebBehaviour {
    snapshot: request_response::Behaviour<SnapshotCodec>,
    push: request_response::Behaviour<PushCodec>,
    relay_client: libp2p::relay::client::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

enum Command {
    /// Caller-built changeset — fan-out as-is. Used by apps that want to
    /// build their own `SyncChangeset` (e.g. tombstones).
    Publish(SyncChangeset),
    /// High-level local write — engine bumps `db_version` and per-column
    /// `col_version`, persists to shadow, then broadcasts. The new
    /// `db_version` is delivered back via `ack`.
    SubmitLocal {
        table: String,
        pk: String,
        columns: Vec<(String, serde_json::Value)>,
        ack: oneshot::Sender<Result<u64, WebSyncError>>,
    },
}

/// Browser-side sync client.
///
/// Cheap to clone — internally a pair of channel senders. All clones share
/// the same underlying swarm task, which runs forever via
/// [`wasm_bindgen_futures::spawn_local`] until the last clone is dropped.
#[derive(Clone)]
pub struct WebSyncClient {
    cmd_tx: mpsc::UnboundedSender<Command>,
    inbound_tx: broadcast::Sender<SyncChangeset>,
    resolved_tx: broadcast::Sender<ColumnChange>,
    /// `None` for ephemeral clients — keeps the public type uniform.
    store: Option<Arc<BrowserStore>>,
    /// Live snapshot of "what does the engine see right now" — peer
    /// list, relay-connected flag. The engine task pushes a fresh
    /// snapshot on every connection lifecycle event. UIs subscribe via
    /// [`Self::subscribe_status`]. `watch` (not `broadcast`) because
    /// the latest value is what matters; subscribers that fall behind
    /// just get the most recent value next time they read.
    status_rx: watch::Receiver<WebSyncStatus>,
}

/// Live debug snapshot exposed by [`WebSyncClient::subscribe_status`].
///
/// Each field updates on its own rhythm:
/// - `connected_peer_ids` and `relay_connected` change on every
///   `ConnectionEstablished` / `ConnectionClosed` swarm event.
/// - `local_peer_id` is set once at startup.
/// - `relay_peer_id` is set once at startup if the client was built
///   via `connect_via_relay`.
#[derive(Debug, Clone, Default)]
pub struct WebSyncStatus {
    pub local_peer_id: String,
    pub relay_peer_id: Option<String>,
    pub relay_connected: bool,
    pub connected_peer_ids: Vec<String>,
}

impl WebSyncClient {
    /// Build an **ephemeral** client and dial `peer_multiaddr` over WebSocket.
    ///
    /// Identity, `db_version`, and shadow state live in memory only.
    /// Reload = fresh peer. Use [`Self::connect_persistent`] if you want
    /// a stable `PeerId` and CRDT continuity across reloads.
    pub fn connect(
        peer_multiaddr: &str,
        user_topic: &str,
        passphrase: Option<&str>,
    ) -> Result<Self, WebSyncError> {
        let keypair = identity::Keypair::generate_ed25519();
        let site_id = NodeId(rand_site_id());
        Self::start(
            peer_multiaddr,
            user_topic,
            passphrase,
            keypair,
            site_id,
            0,
            None,
            None, // not relay-mediated discovery
        )
    }

    /// Build a **persistent** client. Restores or initializes IndexedDB
    /// state in `wavesync-<store_name>`.
    ///
    /// Async because the IndexedDB open + read transactions are async.
    /// On first call, generates a fresh keypair and `site_id`, persists
    /// them, and starts at `db_version=0`. On subsequent calls (same
    /// `store_name`), restores the persisted identity so the network sees
    /// the same `PeerId` as before.
    pub async fn connect_persistent(
        peer_multiaddr: &str,
        user_topic: &str,
        passphrase: Option<&str>,
        store_name: &str,
    ) -> Result<Self, WebSyncError> {
        let store = BrowserStore::open(store_name)
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        let keypair = match store
            .get_keypair()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(bytes) => identity::Keypair::from_protobuf_encoding(&bytes)
                .map_err(|e| WebSyncError::Identity(e.to_string()))?,
            None => {
                let kp = identity::Keypair::generate_ed25519();
                let bytes = kp
                    .to_protobuf_encoding()
                    .map_err(|e| WebSyncError::Identity(e.to_string()))?;
                store
                    .put_keypair(&bytes)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                kp
            }
        };

        let site_id = match store
            .get_site_id()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(id) => id,
            None => {
                let id = NodeId(rand_site_id());
                store
                    .put_site_id(&id)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                id
            }
        };

        let db_version = store
            .get_db_version()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        Self::start(
            peer_multiaddr,
            user_topic,
            passphrase,
            keypair,
            site_id,
            db_version,
            Some(Arc::new(store)),
            None, // not relay-mediated discovery
        )
    }

    /// Build a persistent client that connects through a relay for
    /// peer discovery — the WhatsApp-Web-style topology.
    ///
    /// `relay_addr` must be a fully-qualified multiaddr including the
    /// `/p2p/<relay-peer-id>` suffix, e.g.
    /// `/ip4/127.0.0.1/tcp/4002/ws/p2p/12D3KooW…`. The client:
    ///
    /// 1. Dials the relay over WebSocket.
    /// 2. On connection establish, sends `PushRequest::AnnouncePresence
    ///    { topic }`. The relay responds with `PushResponse::PeerList`
    ///    of circuit-relay multiaddrs for every other peer on the same
    ///    topic. The client dials each.
    /// 3. Stays subscribed to inbound `PushRequest::PeerJoined` from
    ///    the relay so newly-arriving peers get dialed too.
    /// 4. Sync flows over the discovered peer connections (direct or
    ///    circuit-relayed depending on what's reachable).
    ///
    /// Identity (keypair + site_id + db_version) is persisted to
    /// IndexedDB at `wavesync-<store_name>` exactly like
    /// [`Self::connect_persistent`].
    ///
    /// Note on browser security: when the demo is served over HTTPS,
    /// browsers block plain `ws://` connections (mixed-content). The
    /// `relay_addr` must use `wss://` (terminated at a reverse proxy)
    /// or the demo must be served over HTTP for development.
    pub async fn connect_via_relay(
        relay_addr: &str,
        user_topic: &str,
        passphrase: Option<&str>,
        store_name: &str,
    ) -> Result<Self, WebSyncError> {
        // Parse the relay multiaddr early so we fail fast on malformed
        // input, and extract the peer-id so the engine can recognize
        // "this connection is to the relay" later.
        let parsed: Multiaddr = relay_addr
            .parse()
            .map_err(|e: libp2p::multiaddr::Error| WebSyncError::InvalidMultiaddr(e.to_string()))?;
        let relay_peer_id = peer_id_from_multiaddr(&parsed).ok_or_else(|| {
            WebSyncError::InvalidMultiaddr("relay multiaddr must end in /p2p/<peer-id>".into())
        })?;

        let store = BrowserStore::open(store_name)
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        let keypair = match store
            .get_keypair()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(bytes) => identity::Keypair::from_protobuf_encoding(&bytes)
                .map_err(|e| WebSyncError::Identity(e.to_string()))?,
            None => {
                let kp = identity::Keypair::generate_ed25519();
                let bytes = kp
                    .to_protobuf_encoding()
                    .map_err(|e| WebSyncError::Identity(e.to_string()))?;
                store
                    .put_keypair(&bytes)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                kp
            }
        };

        let site_id = match store
            .get_site_id()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(id) => id,
            None => {
                let id = NodeId(rand_site_id());
                store
                    .put_site_id(&id)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                id
            }
        };

        let db_version = store
            .get_db_version()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        Self::start(
            relay_addr,
            user_topic,
            passphrase,
            keypair,
            site_id,
            db_version,
            Some(Arc::new(store)),
            Some(relay_peer_id),
        )
    }

    fn start(
        peer_multiaddr: &str,
        user_topic: &str,
        passphrase: Option<&str>,
        keypair: identity::Keypair,
        site_id: NodeId,
        db_version: u64,
        store: Option<Arc<BrowserStore>>,
        // When `Some`, treat the dialed multiaddr as a relay rather
        // than a sync peer: send AnnouncePresence on connect, accept
        // PeerJoined inbounds from this peer-id only, and exclude this
        // peer from snapshot-Push fan-out. `None` keeps the legacy
        // single-peer-dial behavior used by `connect_persistent` and
        // `connect`.
        relay_peer_id: Option<LibPeerId>,
    ) -> Result<Self, WebSyncError> {
        let group_key = passphrase.map(GroupKey::from_passphrase);
        let effective_topic = group_key
            .as_ref()
            .map(|gk| gk.derive_topic(user_topic))
            .unwrap_or_else(|| user_topic.to_string());

        let target: Multiaddr = peer_multiaddr
            .parse()
            .map_err(|e: libp2p::multiaddr::Error| WebSyncError::InvalidMultiaddr(e.to_string()))?;

        let local_peer_id = keypair.public().to_peer_id();

        // `with_other_transport`'s closure must return either a bare
        // Transport or a `Result<T, Box<dyn Error + Send + Sync>>` — the
        // two `TryIntoTransport` impls in libp2p 0.56. Anything else
        // (incl. our own error type) compile-fails with a confusing
        // `Result<...>: Transport not satisfied` error. Box noise errors
        // explicitly to land in the documented happy path.
        // The `.with_relay_client(...)` step after `with_other_transport`
        // wraps our base WebSocket transport with libp2p's circuit-relay
        // client. After that, `swarm.dial(/p2p/<relay>/p2p-circuit/p2p/<peer>)`
        // is routable — the relay-client transport handles the inner hop.
        // Without this step those dials fail with an unrouted-address
        // error. The `with_behaviour` closure now receives `(key,
        // relay_client)` so the produced `relay_client::Behaviour` is
        // wired into our `WebBehaviour`.
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_wasm_bindgen()
            .with_other_transport(|key| {
                use libp2p::Transport;
                use libp2p::core::upgrade::Version;
                let ws = libp2p::websocket_websys::Transport::default();
                let auth = noise::Config::new(key)
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    ws.upgrade(Version::V1)
                        .authenticate(auth)
                        .multiplex(yamux::Config::default()),
                )
            })
            .map_err(|e| WebSyncError::Setup(format!("transport: {e}")))?
            .with_relay_client(noise::Config::new, yamux::Config::default)
            .map_err(|e| WebSyncError::Setup(format!("relay client: {e}")))?
            .with_behaviour(|key, relay_client| WebBehaviour {
                snapshot: request_response::Behaviour::new(
                    [(SNAPSHOT_PROTOCOL, request_response::ProtocolSupport::Full)],
                    request_response::Config::default(),
                ),
                push: request_response::Behaviour::new(
                    [(PUSH_PROTOCOL, request_response::ProtocolSupport::Full)],
                    request_response::Config::default(),
                ),
                relay_client,
                identify: identify::Behaviour::new(identify::Config::new(
                    "/wavesync/2.0.0".into(),
                    key.public(),
                )),
                ping: ping::Behaviour::default(),
            })
            .map_err(|e| WebSyncError::Setup(format!("behaviour: {e}")))?
            // Default in libp2p 0.55+ is 10 seconds — far too short for a
            // relay connection that may be idle between announces while no
            // peers exist on the topic. The browser would close the relay
            // connection 10s after the AnnouncePresence round-trip,
            // re-dial, repeat. Match the relay's 5-minute default so the
            // connection stays up across quiet periods. Ping
            // (`ping::Behaviour::default()` interval = 15s) keeps it
            // measurably alive within that window.
            .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
            .build();

        swarm
            .dial(target.clone())
            .map_err(|e| WebSyncError::Dial(e.to_string()))?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (inbound_tx, _) = broadcast::channel(64);
        let (resolved_tx, _) = broadcast::channel(256);
        let (status_tx, status_rx) = watch::channel(WebSyncStatus {
            local_peer_id: local_peer_id.to_string(),
            relay_peer_id: relay_peer_id.map(|p| p.to_string()),
            relay_connected: false,
            connected_peer_ids: Vec::new(),
        });

        log::info!(
            "WebSyncClient: peer {local_peer_id}, dialing {target}, topic={effective_topic}, persistent={}",
            store.is_some()
        );

        let state = EngineState {
            site_id,
            db_version: Arc::new(Mutex::new(db_version)),
            topic: effective_topic,
            group_key,
            store: store.clone(),
            inbound_tx: inbound_tx.clone(),
            resolved_tx: resolved_tx.clone(),
            relay_peer_id,
            status_tx,
        };

        wasm_bindgen_futures::spawn_local(run_swarm(swarm, cmd_rx, state));

        Ok(Self {
            cmd_tx,
            inbound_tx,
            resolved_tx,
            store,
            status_rx,
        })
    }

    /// Broadcast a caller-built changeset to all currently connected peers.
    ///
    /// This is the low-level escape hatch for apps that want to construct
    /// `SyncChangeset` themselves (e.g. for tombstones / deletes). For
    /// regular inserts and updates use [`Self::submit_local_write`].
    pub fn publish(&self, changeset: SyncChangeset) -> Result<(), WebSyncError> {
        self.cmd_tx
            .send(Command::Publish(changeset))
            .map_err(|_| WebSyncError::NotRunning)
    }

    /// High-level local write.
    ///
    /// Looks up the current shadow entry for each `(table, pk, column)`,
    /// bumps `col_version`, persists the new shadow row, increments
    /// `db_version`, and broadcasts the resulting `SyncChangeset` to all
    /// connected peers. Returns the new `db_version`.
    ///
    /// Persistent clients durably commit the new shadow rows and
    /// `db_version` to IndexedDB before the broadcast — a peer that
    /// receives this changeset and later sends back catch-up traffic
    /// will see consistent state. Ephemeral clients keep all of this in
    /// memory.
    pub async fn submit_local_write(
        &self,
        table: &str,
        pk: &str,
        columns: Vec<(String, serde_json::Value)>,
    ) -> Result<u64, WebSyncError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::SubmitLocal {
                table: table.to_string(),
                pk: pk.to_string(),
                columns,
                ack: tx,
            })
            .map_err(|_| WebSyncError::NotRunning)?;
        rx.await.map_err(|_| WebSyncError::NotRunning)?
    }

    /// Type-safe wrapper around [`Self::submit_local_write`].
    ///
    /// Apps implement [`BrowserEntity`] for their domain types and call
    /// `client.submit(&task).await` instead of building the column bag
    /// by hand. This is the path the reactive
    /// [`use_synced_table`](crate::dioxus::use_synced_table) hook takes
    /// internally; using it consistently means local edits flow through
    /// the same channel remote ones do, so reactive subscribers don't
    /// need a special optimistic-update path.
    pub async fn submit<E: BrowserEntity>(
        &self,
        table: &str,
        entity: &E,
    ) -> Result<u64, WebSyncError> {
        let pk = entity.pk();
        let columns = entity.to_columns();
        self.submit_local_write(table, &pk, columns).await
    }

    /// Subscribe to **raw** incoming changesets from peers.
    ///
    /// Each received message arrives whole, **before** local conflict
    /// resolution. Useful if the app wants to inspect the wire format
    /// directly or implement its own resolution. Most apps want
    /// [`Self::subscribe_resolved`] instead.
    pub fn subscribe(&self) -> broadcast::Receiver<SyncChangeset> {
        self.inbound_tx.subscribe()
    }

    /// Subscribe to per-column changes the engine has committed to its
    /// shadow store.
    ///
    /// Fires for **both** local writes (echoed after `submit_local_write`
    /// persists) and remote pushes (after they win conflict resolution).
    /// Reactive UIs can drive themselves entirely off this single stream
    /// without needing to special-case the optimistic-update path.
    /// Apps that genuinely want only-remote events can filter by
    /// `site_id != my_site_id`.
    pub fn subscribe_resolved(&self) -> broadcast::Receiver<ColumnChange> {
        self.resolved_tx.subscribe()
    }

    /// `true` if this client is backed by IndexedDB persistence.
    pub fn is_persistent(&self) -> bool {
        self.store.is_some()
    }

    /// Access the underlying [`BrowserStore`] for direct reads.
    ///
    /// `None` when the client was constructed via [`Self::connect`]
    /// (ephemeral). The store is what application UIs use to materialize
    /// initial state on mount via `list_table_rows`.
    pub fn store(&self) -> Option<Arc<BrowserStore>> {
        self.store.clone()
    }

    /// Connect via an in-process loopback channel. **Demo/test path only.**
    ///
    /// Two clients constructed with [`LoopbackPair::new`] and crossed
    /// channel ends exchange `SyncRequest::Push` envelopes directly,
    /// bypassing libp2p entirely. All the engine logic that matters —
    /// HMAC, topic check, conflict resolution against the persisted
    /// shadow, broadcast of resolved changes — still runs. What's
    /// skipped is the network: no swarm, no transport, no peer ID.
    ///
    /// This exists so the website can ship a self-contained demo that
    /// shows two independent peers with their own IndexedDB stores
    /// syncing in real time without the user needing to run a relay.
    /// Production apps should use [`Self::connect_persistent`] or
    /// [`Self::connect`].
    pub async fn connect_loopback(
        end: LoopbackEnd,
        user_topic: &str,
        passphrase: Option<&str>,
        store_name: &str,
    ) -> Result<Self, WebSyncError> {
        let store = BrowserStore::open(store_name)
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        // Same identity-restore logic as connect_persistent. The
        // loopback transport doesn't need the libp2p keypair (no peer
        // negotiation), but persisting one keeps `site_id` stable across
        // reloads and means swapping a loopback client for a real
        // network one later wouldn't change identity.
        let _kp = match store
            .get_keypair()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(bytes) => identity::Keypair::from_protobuf_encoding(&bytes)
                .map_err(|e| WebSyncError::Identity(e.to_string()))?,
            None => {
                let kp = identity::Keypair::generate_ed25519();
                let bytes = kp
                    .to_protobuf_encoding()
                    .map_err(|e| WebSyncError::Identity(e.to_string()))?;
                store
                    .put_keypair(&bytes)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                kp
            }
        };

        let site_id = match store
            .get_site_id()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?
        {
            Some(id) => id,
            None => {
                let id = NodeId(rand_site_id());
                store
                    .put_site_id(&id)
                    .await
                    .map_err(|e| WebSyncError::Store(e.to_string()))?;
                id
            }
        };

        let db_version = store
            .get_db_version()
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;

        let group_key = passphrase.map(GroupKey::from_passphrase);
        let effective_topic = group_key
            .as_ref()
            .map(|gk| gk.derive_topic(user_topic))
            .unwrap_or_else(|| user_topic.to_string());

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (inbound_tx, _) = broadcast::channel(64);
        let (resolved_tx, _) = broadcast::channel(256);
        let (status_tx, status_rx) = watch::channel(WebSyncStatus {
            local_peer_id: format!("loopback-{:02x?}", &site_id.0[..4]),
            relay_peer_id: None,
            relay_connected: false,
            connected_peer_ids: Vec::new(),
        });

        log::info!(
            "WebSyncClient (loopback): site_id={:02x?}, topic={effective_topic}, store={store_name}",
            &site_id.0[..4]
        );

        let store_arc = Arc::new(store);
        let state = EngineState {
            site_id,
            db_version: Arc::new(Mutex::new(db_version)),
            topic: effective_topic,
            group_key,
            store: Some(store_arc.clone()),
            inbound_tx: inbound_tx.clone(),
            resolved_tx: resolved_tx.clone(),
            relay_peer_id: None, // loopback transport has no notion of a relay
            status_tx,
        };

        wasm_bindgen_futures::spawn_local(run_loopback(end, cmd_rx, state));

        Ok(Self {
            cmd_tx,
            inbound_tx,
            resolved_tx,
            store: Some(store_arc),
            status_rx,
        })
    }

    /// Live-updating debug status: connected peer ids, relay
    /// connection flag. Subscribers see the most recent snapshot
    /// immediately and a fresh snapshot on every connection
    /// lifecycle event. UIs typically use a Dioxus
    /// `Signal<WebSyncStatus>` driven by this receiver.
    pub fn subscribe_status(&self) -> watch::Receiver<WebSyncStatus> {
        self.status_rx.clone()
    }
}

/// One end of an in-process loopback transport. See
/// [`LoopbackPair::new`] / [`WebSyncClient::connect_loopback`].
pub struct LoopbackEnd {
    out_tx: mpsc::UnboundedSender<SyncRequest>,
    in_rx: mpsc::UnboundedReceiver<SyncRequest>,
    link: LoopbackLink,
}

impl LoopbackEnd {
    /// Cloneable handle for toggling this side's online state from
    /// outside the engine task.
    pub fn link(&self) -> LoopbackLink {
        self.link.clone()
    }
}

/// External handle for an [`LoopbackEnd`]'s online state.
///
/// Cheap to clone — internally an `Arc<AtomicBool>` and an
/// `Arc<Notify>`. Calling [`Self::set_online`] toggles message delivery
/// for that side: while offline, the engine task buffers both
/// outgoing and incoming `SyncRequest`s. Going back online drains
/// both buffers in order, which is what makes "edit while disconnected,
/// reconnect, see it sync" work for free in the demo.
#[derive(Clone)]
pub struct LoopbackLink {
    online: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl LoopbackLink {
    fn new() -> Self {
        Self {
            online: Arc::new(AtomicBool::new(true)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Whether this side is currently delivering messages.
    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::Relaxed)
    }

    /// Toggle online state. Wakes the engine task so any backlog drains
    /// (when going online) or so subsequent traffic starts buffering
    /// (when going offline).
    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Relaxed);
        self.notify.notify_one();
    }
}

/// A pair of [`LoopbackEnd`]s wired together. Pass `pair.a` to one
/// client and `pair.b` to the other, and they will sync via in-process
/// channels. Grab `pair.a.link()` / `pair.b.link()` *before* moving
/// each end into [`WebSyncClient::connect_loopback`] if you want to
/// toggle either side offline later.
pub struct LoopbackPair {
    pub a: LoopbackEnd,
    pub b: LoopbackEnd,
}

impl Default for LoopbackPair {
    fn default() -> Self {
        Self::new()
    }
}

impl LoopbackPair {
    /// Create a fresh pair of crossed channels. Each end starts in the
    /// online state.
    pub fn new() -> Self {
        let (a_to_b, b_in) = mpsc::unbounded_channel::<SyncRequest>();
        let (b_to_a, a_in) = mpsc::unbounded_channel::<SyncRequest>();
        Self {
            a: LoopbackEnd {
                out_tx: a_to_b,
                in_rx: a_in,
                link: LoopbackLink::new(),
            },
            b: LoopbackEnd {
                out_tx: b_to_a,
                in_rx: b_in,
                link: LoopbackLink::new(),
            },
        }
    }
}

struct EngineState {
    site_id: NodeId,
    db_version: Arc<Mutex<u64>>,
    topic: String,
    group_key: Option<GroupKey>,
    store: Option<Arc<BrowserStore>>,
    inbound_tx: broadcast::Sender<SyncChangeset>,
    resolved_tx: broadcast::Sender<ColumnChange>,
    /// PeerId of the relay we dialed via `connect_via_relay`. Used to:
    /// (1) trigger an `AnnouncePresence` when this specific peer
    /// connects, (2) skip the relay when fan-outing snapshot Push
    /// messages — it's not a sync peer, and (3) validate inbound
    /// `PushRequest::PeerJoined` (only the relay is allowed to announce
    /// peer joins, otherwise any peer could trick us into dialing
    /// arbitrary addresses). `None` for `connect`/`connect_persistent`/
    /// `connect_loopback` clients that aren't using relay-mediated
    /// discovery.
    relay_peer_id: Option<LibPeerId>,
    /// Watch channel sender for live debug status. Engine pushes a
    /// fresh `WebSyncStatus` after every connection lifecycle event;
    /// UIs read it via [`WebSyncClient::subscribe_status`].
    status_tx: watch::Sender<WebSyncStatus>,
}

/// Recompute the watch-channel snapshot from the current connected
/// set + relay-known state. Cheap (a clone of a small Vec); called on
/// every `ConnectionEstablished`/`ConnectionClosed` so UIs see peer
/// list changes within one event loop tick.
///
/// `relay_connected` is tracked separately because the relay peer-id
/// is *not* inserted into `connected` (the relay isn't a sync peer
/// and shouldn't see snapshot fan-out) — so we can't derive its
/// connection state from the set.
fn push_status(state: &EngineState, connected: &HashSet<LibPeerId>, relay_connected: bool) {
    let connected_peer_ids = connected.iter().map(|p| p.to_string()).collect();
    // Snapshot the immutable fields (local + relay peer ids) into owned
    // values *before* calling `send_replace`. Inlining `borrow()` into
    // the `send_replace` argument list keeps the read-guard alive across
    // the write attempt, which races against the watch's internal lock.
    let (local_peer_id, relay_peer_id) = {
        let snap = state.status_tx.borrow();
        (snap.local_peer_id.clone(), snap.relay_peer_id.clone())
    };
    // `send_replace` doesn't error on no-subscribers (unlike
    // `broadcast::send`'s SendError) and it overwrites the latest
    // value, which is exactly what watch consumers want.
    state.status_tx.send_replace(WebSyncStatus {
        local_peer_id,
        relay_peer_id,
        relay_connected,
        connected_peer_ids,
    });
}

/// Loopback variant of [`run_swarm`]. No libp2p — `SyncRequest`s flow
/// through the `LoopbackEnd` channels directly. Used by the website
/// demo to show two independent engines syncing inside a single page.
///
/// Honors the [`LoopbackLink`] online flag. While offline:
/// - outgoing requests (from local writes) buffer in `pending_out`
/// - incoming requests (the peer can still write to the channel; we just
///   don't deliver them) buffer in `pending_in`
///
/// On the online → offline transition there's nothing to do beyond
/// flipping the flag. On offline → online, both buffers drain in FIFO
/// order: outgoing first (so the peer sees the local edits), then
/// incoming (so we see the peer's edits). The order doesn't actually
/// matter for convergence — CRDT resolution is order-independent — but
/// "show my own writes hitting the peer first" is the more intuitive
/// demo behavior.
/// Stable key under which each loopback engine records "what's the
/// highest db_version I've seen from my counterpart." Loopback always
/// has exactly one peer per pair, so we don't bother keying by site_id
/// (which would require knowing the peer's site_id ahead of time).
const LOOPBACK_PEER_KEY: &str = "loopback-peer";

async fn run_loopback(
    mut end: LoopbackEnd,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    state: EngineState,
) {
    let mut pending_out: VecDeque<SyncRequest> = VecDeque::new();
    let link = end.link.clone();
    // Force a catch-up on the very first iteration: even though the link
    // starts in the "online" state, our peer_versions store may have
    // entries from a previous session — or the peer may have made local
    // edits before we ran. Treating the first iteration as an offline →
    // online transition pulls in anything we missed.
    let mut was_online = false;

    loop {
        let now_online = link.is_online();
        if now_online && !was_online {
            // offline → online transition (or initial connect). Drain any
            // local edits we made while disconnected, then ask the peer
            // for everything they have past our last seen db_version.
            // This is the version-vector catch-up — same protocol shape
            // as the native engine, just routed via the loopback channel
            // here. On a real network the same VersionVector request goes
            // out via libp2p request-response.
            while let Some(req) = pending_out.pop_front() {
                if end.out_tx.send(req).is_err() {
                    log::info!("WebSyncClient (loopback): peer dropped while draining outbox");
                    return;
                }
            }
            send_version_vector(&state, &end.out_tx).await;
        }
        was_online = now_online;

        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Publish(changeset)) => {
                        let req = build_push_request(&state.topic, state.group_key.as_ref(), changeset);
                        if link.is_online() {
                            let _ = end.out_tx.send(req);
                        } else {
                            pending_out.push_back(req);
                        }
                    }
                    Some(Command::SubmitLocal { table, pk, columns, ack }) => {
                        let result = handle_submit_local_loopback(
                            &state,
                            &mut end,
                            &link,
                            &mut pending_out,
                            table,
                            pk,
                            columns,
                        )
                        .await;
                        let _ = ack.send(result);
                    }
                    None => {
                        log::info!("WebSyncClient (loopback): command channel closed");
                        return;
                    }
                }
            }
            incoming = end.in_rx.recv() => {
                match incoming {
                    Some(req) => {
                        if link.is_online() {
                            handle_loopback_request(req, &state, &end.out_tx).await;
                        } else {
                            // Honest to a real network: messages destined
                            // for an offline peer are dropped on the floor.
                            // The version-vector catch-up on reconnect
                            // (above) is what recovers them.
                            log::debug!("loopback: dropping incoming while offline");
                        }
                    }
                    None => {
                        log::info!("WebSyncClient (loopback): peer channel closed");
                        return;
                    }
                }
            }
            _ = link.notify.notified() => {
                // Online flag may have flipped — top of loop handles
                // the catch-up trigger when we come back online. Spurious
                // notifies cost an extra select tick; harmless.
            }
        }
    }
}

/// Send a `VersionVector` request over the loopback transport, using the
/// last `db_version` we recorded from our peer. The peer responds via
/// [`handle_loopback_request`] with a `Push` containing every shadow
/// change strictly newer than that.
async fn send_version_vector(state: &EngineState, out_tx: &mpsc::UnboundedSender<SyncRequest>) {
    let store = match &state.store {
        Some(s) => s,
        None => return, // ephemeral clients don't track peer versions
    };
    let last_seen = match store.get_peer_version(LOOPBACK_PEER_KEY).await {
        Ok(v) => v,
        Err(e) => {
            log::warn!("loopback: peer_version read failed: {e}");
            0
        }
    };
    let my_db_version = *state.db_version.lock().await;
    let mut req = SyncRequest::VersionVector {
        my_db_version,
        your_last_db_version: last_seen,
        site_id: state.site_id,
        topic: state.topic.clone(),
        hmac: None,
    };
    if let Some(gk) = &state.group_key {
        if let Ok(bytes) = serde_json::to_vec(&req) {
            let tag = gk.mac(&bytes);
            if let SyncRequest::VersionVector { ref mut hmac, .. } = req {
                *hmac = Some(tag);
            }
        }
    }
    log::debug!(
        "loopback: requesting catch-up since db_version={last_seen} (we are at {my_db_version})"
    );
    let _ = out_tx.send(req);
}

async fn handle_submit_local_loopback(
    state: &EngineState,
    end: &mut LoopbackEnd,
    link: &LoopbackLink,
    pending_out: &mut VecDeque<SyncRequest>,
    table: String,
    pk: String,
    columns: Vec<(String, serde_json::Value)>,
) -> Result<u64, WebSyncError> {
    // Mirrors handle_submit_local — bumps db_version, persists shadow per
    // column, then ships the changeset. The only difference is the
    // out-of-engine transport: a channel send instead of swarm.send_request.
    let mut dv = state.db_version.lock().await;
    *dv += 1;
    let new_db_version = *dv;
    if let Some(store) = &state.store {
        store
            .put_db_version(new_db_version)
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;
    }
    drop(dv);

    let mut changes: Vec<ColumnChange> = Vec::with_capacity(columns.len());
    for (seq, (cid, val)) in columns.into_iter().enumerate() {
        let prev = if let Some(store) = &state.store {
            store
                .get_shadow(&table, &pk, &cid)
                .await
                .map_err(|e| WebSyncError::Store(e.to_string()))?
        } else {
            None
        };
        let next_col_version = prev.as_ref().map(|r| r.col_version + 1).unwrap_or(1);
        let next_cl = prev
            .as_ref()
            .map(|r| r.cl.max(next_col_version))
            .unwrap_or(next_col_version);

        if let Some(store) = &state.store {
            let row = ShadowRow {
                val: Some(val.clone()),
                site_id: state.site_id.0,
                col_version: next_col_version,
                cl: next_cl,
                seq: seq as u32,
                db_version: new_db_version,
            };
            store
                .put_shadow(&table, &pk, &cid, &row)
                .await
                .map_err(|e| WebSyncError::Store(e.to_string()))?;
        }

        changes.push(ColumnChange {
            table: TableName(table.clone()),
            pk: PrimaryKey(pk.clone()),
            cid: ColumnName(cid),
            val: Some(val),
            site_id: state.site_id,
            col_version: next_col_version,
            cl: next_cl,
            seq: seq as u32,
            db_version: new_db_version,
        });
    }

    // Echo the local writes on resolved_tx so reactive subscribers
    // (e.g. `dioxus::use_synced_table`) update their in-memory views
    // for local edits the same way they do for remote ones. The shadow
    // is already durably persisted; this is purely a wake-up for
    // listeners. Lagged subscribers drop messages — fine, they'd
    // re-materialize from the store on next mount.
    for ch in &changes {
        let _ = state.resolved_tx.send(ch.clone());
    }

    let changeset = SyncChangeset {
        site_id: state.site_id,
        db_version: new_db_version,
        changes,
    };

    let req = build_push_request(&state.topic, state.group_key.as_ref(), changeset);
    if link.is_online() {
        let _ = end.out_tx.send(req);
    } else {
        // Offline: shadow + db_version are already durably persisted to
        // IndexedDB above, so the local write is "real". Buffer the
        // outbound changeset in pending_out — the run loop drains it
        // when the link transitions back to online.
        pending_out.push_back(req);
    }
    Ok(new_db_version)
}

/// Loopback equivalent of [`handle_snapshot_event`] for incoming
/// `SyncRequest`s.
///
/// In loopback there's no separate response channel — the libp2p
/// `request_response` round-trip is collapsed onto the same one-way
/// channel by sending catch-up data back as an unsolicited
/// [`SyncRequest::Push`]. The receiving side can't tell whether a Push
/// is real-time fan-out or catch-up reply, but it doesn't need to: the
/// merge logic is identical either way, courtesy of CRDT.
async fn handle_loopback_request(
    req: SyncRequest,
    state: &EngineState,
    out_tx: &mpsc::UnboundedSender<SyncRequest>,
) {
    match req {
        SyncRequest::Push {
            changeset,
            topic,
            hmac,
        } => {
            if topic != state.topic {
                log::debug!("loopback: dropping Push — topic mismatch");
                return;
            }
            if let Some(gk) = &state.group_key {
                let verify = SyncRequest::Push {
                    changeset: changeset.clone(),
                    topic: topic.clone(),
                    hmac: None,
                };
                let bytes = match serde_json::to_vec(&verify) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let tag = match hmac {
                    Some(t) => t,
                    None => {
                        log::debug!("loopback: dropping Push — missing HMAC");
                        return;
                    }
                };
                if !gk.verify(&bytes, &tag) {
                    log::debug!("loopback: dropping Push — bad HMAC");
                    return;
                }
            }
            let _ = state.inbound_tx.send(changeset.clone());
            apply_remote_changeset_loopback(state, LOOPBACK_PEER_KEY, &changeset).await;
        }
        SyncRequest::VersionVector {
            my_db_version: peer_db_version,
            your_last_db_version: since,
            site_id: peer_site,
            topic,
            hmac,
        } => {
            if topic != state.topic {
                log::debug!("loopback: dropping VersionVector — topic mismatch");
                return;
            }
            if let Some(gk) = &state.group_key {
                let verify = SyncRequest::VersionVector {
                    my_db_version: peer_db_version,
                    your_last_db_version: since,
                    site_id: peer_site,
                    topic: topic.clone(),
                    hmac: None,
                };
                let bytes = match serde_json::to_vec(&verify) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let tag = match hmac {
                    Some(t) => t,
                    None => {
                        log::debug!("loopback: dropping VersionVector — missing HMAC");
                        return;
                    }
                };
                if !gk.verify(&bytes, &tag) {
                    log::debug!("loopback: dropping VersionVector — bad HMAC");
                    return;
                }
            }

            // Build catch-up changeset from local shadow.
            let store = match &state.store {
                Some(s) => s,
                None => return, // ephemeral clients have nothing to send
            };
            let changes = match store.get_changes_since(since).await {
                Ok(c) => c,
                Err(e) => {
                    log::warn!("loopback: catch-up scan failed: {e}");
                    return;
                }
            };
            log::debug!(
                "loopback: VersionVector since={since} → returning {} changes",
                changes.len()
            );
            if !changes.is_empty() {
                let my_db_version = *state.db_version.lock().await;
                let changeset = SyncChangeset {
                    site_id: state.site_id,
                    db_version: my_db_version,
                    changes,
                };
                let push = build_push_request(&state.topic, state.group_key.as_ref(), changeset);
                let _ = out_tx.send(push);
            }
        }
        SyncRequest::IdentityAnnounce { .. } => {
            // Identity announce is a presence signal in the native
            // engine; loopback has no concept of presence beyond the
            // online flag, so we ignore.
        }
    }
}

/// Conflict-resolve every column change against persisted shadow state.
/// Same logic as `apply_remote_changeset` but takes a string peer id
/// (since loopback has no real `libp2p::PeerId`).
async fn apply_remote_changeset_loopback(
    state: &EngineState,
    peer: &str,
    changeset: &SyncChangeset,
) {
    let store = match &state.store {
        Some(s) => s,
        None => {
            for change in &changeset.changes {
                let _ = state.resolved_tx.send(change.clone());
            }
            return;
        }
    };

    for change in &changeset.changes {
        let local = match store
            .get_shadow(&change.table.0, &change.pk.0, &change.cid.0)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                log::warn!("loopback: shadow read failed: {e}");
                continue;
            }
        };

        let remote_val = match &change.val {
            Some(v) => serde_json::to_vec(v).unwrap_or_default(),
            None => Vec::new(),
        };
        let (local_col_version, local_val_bytes, local_site_id) = match &local {
            Some(r) => {
                let bytes = r
                    .val
                    .as_ref()
                    .map(|v| serde_json::to_vec(v).unwrap_or_default())
                    .unwrap_or_default();
                (r.col_version, bytes, NodeId(r.site_id))
            }
            None => (0, Vec::new(), NodeId([0u8; 16])),
        };

        let apply = conflict::should_apply_column(
            change.col_version,
            &remote_val,
            &change.site_id,
            local_col_version,
            &local_val_bytes,
            &local_site_id,
        );
        if !apply {
            continue;
        }

        let new_row = ShadowRow {
            val: change.val.clone(),
            site_id: change.site_id.0,
            col_version: change.col_version,
            cl: change.cl,
            seq: change.seq,
            db_version: change.db_version,
        };
        if let Err(e) = store
            .put_shadow(&change.table.0, &change.pk.0, &change.cid.0, &new_row)
            .await
        {
            log::warn!("loopback: shadow write failed: {e}");
            continue;
        }

        let _ = state.resolved_tx.send(change.clone());
    }

    if let Err(e) = store.set_peer_version(peer, changeset.db_version).await {
        log::debug!("loopback: peer_version write failed: {e}");
    }
}

async fn run_swarm(
    mut swarm: Swarm<WebBehaviour>,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
    state: EngineState,
) {
    let mut connected: HashSet<LibPeerId> = HashSet::new();
    // The relay peer-id is *not* inserted into `connected` (it isn't
    // a sync peer), so we track its link state on the side. The UI
    // status panel reads this through `WebSyncStatus.relay_connected`.
    let mut relay_connected = false;
    // Peers (currently only the relay) we want to send `AnnouncePresence`
    // to but couldn't synchronously inside the `ConnectionEstablished`
    // handler — see the comment in `handle_event` for the reentrancy
    // reason. Drained on every loop iteration before re-polling.
    let mut pending_announces: Vec<LibPeerId> = Vec::new();

    loop {
        for peer in pending_announces.drain(..) {
            log::info!("WebSyncClient: sending deferred AnnouncePresence to {peer}");
            let req = PushRequest::AnnouncePresence {
                topic: state.topic.clone(),
            };
            let _ = swarm.behaviour_mut().push.send_request(&peer, req);
        }

        tokio::select! {
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(Command::Publish(changeset)) => {
                        let req = build_push_request(&state.topic, state.group_key.as_ref(), changeset);
                        for peer in connected.iter().copied().collect::<Vec<_>>() {
                            swarm.behaviour_mut().snapshot.send_request(&peer, req.clone());
                        }
                    }
                    Some(Command::SubmitLocal { table, pk, columns, ack }) => {
                        let result = handle_submit_local(&state, &mut swarm, &connected, table, pk, columns).await;
                        let _ = ack.send(result);
                    }
                    None => {
                        log::info!("WebSyncClient: command channel closed, exiting");
                        return;
                    }
                }
            }
            event = swarm.select_next_some() => {
                handle_event(event, &mut connected, &mut relay_connected, &mut pending_announces, &state, &mut swarm).await;
            }
        }
    }
}

async fn handle_submit_local(
    state: &EngineState,
    swarm: &mut Swarm<WebBehaviour>,
    connected: &HashSet<LibPeerId>,
    table: String,
    pk: String,
    columns: Vec<(String, serde_json::Value)>,
) -> Result<u64, WebSyncError> {
    // Bump db_version once for the whole batch — matches native semantics
    // (every local write is exactly one `db_version` step).
    let mut dv = state.db_version.lock().await;
    *dv += 1;
    let new_db_version = *dv;
    if let Some(store) = &state.store {
        store
            .put_db_version(new_db_version)
            .await
            .map_err(|e| WebSyncError::Store(e.to_string()))?;
    }
    drop(dv);

    let mut changes: Vec<ColumnChange> = Vec::with_capacity(columns.len());
    for (seq, (cid, val)) in columns.into_iter().enumerate() {
        // Look up current shadow → next col_version.
        let prev = if let Some(store) = &state.store {
            store
                .get_shadow(&table, &pk, &cid)
                .await
                .map_err(|e| WebSyncError::Store(e.to_string()))?
        } else {
            None
        };
        let next_col_version = prev.as_ref().map(|r| r.col_version + 1).unwrap_or(1);
        let next_cl = prev
            .as_ref()
            .map(|r| r.cl.max(next_col_version))
            .unwrap_or(next_col_version);

        if let Some(store) = &state.store {
            let row = ShadowRow {
                val: Some(val.clone()),
                site_id: state.site_id.0,
                col_version: next_col_version,
                cl: next_cl,
                seq: seq as u32,
                db_version: new_db_version,
            };
            store
                .put_shadow(&table, &pk, &cid, &row)
                .await
                .map_err(|e| WebSyncError::Store(e.to_string()))?;
        }

        changes.push(ColumnChange {
            table: TableName(table.clone()),
            pk: PrimaryKey(pk.clone()),
            cid: ColumnName(cid),
            val: Some(val),
            site_id: state.site_id,
            col_version: next_col_version,
            cl: next_cl,
            seq: seq as u32,
            db_version: new_db_version,
        });
    }

    // Echo local writes on resolved_tx so reactive subscribers update
    // for local edits the same way they do for remote ones. See the
    // matching block in `handle_submit_local_loopback`.
    for ch in &changes {
        let _ = state.resolved_tx.send(ch.clone());
    }

    let changeset = SyncChangeset {
        site_id: state.site_id,
        db_version: new_db_version,
        changes,
    };

    let req = build_push_request(&state.topic, state.group_key.as_ref(), changeset);
    let peers: Vec<LibPeerId> = connected.iter().copied().collect();
    log::info!(
        "WebSyncClient: pushing changeset (db_v={new_db_version}) to {} peer(s): {:?}",
        peers.len(),
        peers.iter().map(|p| p.to_string()).collect::<Vec<_>>()
    );
    for peer in &peers {
        let id = swarm
            .behaviour_mut()
            .snapshot
            .send_request(peer, req.clone());
        log::debug!("WebSyncClient: send_request → peer {peer} req_id={id:?}");
    }
    Ok(new_db_version)
}

async fn handle_event(
    event: SwarmEvent<WebBehaviourEvent>,
    connected: &mut HashSet<LibPeerId>,
    relay_connected: &mut bool,
    pending_announces: &mut Vec<LibPeerId>,
    state: &EngineState,
    swarm: &mut Swarm<WebBehaviour>,
) {
    match event {
        SwarmEvent::ConnectionEstablished {
            peer_id, endpoint, ..
        } => {
            if Some(peer_id) == state.relay_peer_id {
                // Connected to the relay — register our topic and ask
                // for the current peer list. We deliberately do NOT add
                // it to `connected`: the relay isn't a sync peer and
                // shouldn't see snapshot Push fan-out.
                //
                // Defer the actual `send_request` to the next select
                // iteration. Calling it synchronously here triggers a
                // wasm_bindgen_futures executor reentrancy panic
                // ("RefCell already borrowed" in singlethread.rs:142):
                // the request_response substream open path can wake a
                // task whose poll re-enters `Inner::run` while we still
                // hold the outer borrow from `swarm.select_next_some()`.
                log::info!("WebSyncClient: connected to relay {peer_id}, queuing announce");
                log::debug!(
                    "WebSyncClient: relay connection endpoint = {} ({})",
                    if endpoint.is_dialer() {
                        "dialer"
                    } else {
                        "listener"
                    },
                    endpoint.get_remote_address()
                );
                *relay_connected = true;
                pending_announces.push(peer_id);
            } else {
                connected.insert(peer_id);
                log::info!(
                    "WebSyncClient: connected to peer {peer_id} (connected count={})",
                    connected.len()
                );
                log::debug!(
                    "WebSyncClient: peer connection endpoint = {} ({})",
                    if endpoint.is_dialer() {
                        "dialer"
                    } else {
                        "listener"
                    },
                    endpoint.get_remote_address()
                );
            }
            push_status(state, connected, *relay_connected);
        }
        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            if Some(peer_id) == state.relay_peer_id {
                log::info!("WebSyncClient: disconnected from relay {peer_id} (cause={cause:?})");
                *relay_connected = false;
            } else {
                connected.remove(&peer_id);
                log::info!(
                    "WebSyncClient: disconnected from peer {peer_id} (connected count={}, cause={cause:?})",
                    connected.len()
                );
            }
            push_status(state, connected, *relay_connected);
        }
        SwarmEvent::OutgoingConnectionError { error, peer_id, .. } => {
            log::warn!("WebSyncClient: dial failure ({peer_id:?}): {error}");
        }
        SwarmEvent::Behaviour(WebBehaviourEvent::Snapshot(ev)) => {
            handle_snapshot_event(ev, state, swarm).await;
        }
        SwarmEvent::Behaviour(WebBehaviourEvent::Push(ev)) => {
            handle_push_event(ev, state, swarm);
        }
        SwarmEvent::Behaviour(WebBehaviourEvent::Ping(_)) => {}
        SwarmEvent::Behaviour(WebBehaviourEvent::Identify(_)) => {}
        SwarmEvent::Behaviour(WebBehaviourEvent::RelayClient(_)) => {}
        _ => {}
    }
}

/// Handle inbound and outbound `push` request_response events.
///
/// Two cases that matter:
/// - **`Response`** to our own outbound `AnnouncePresence`: a
///   `PushResponse::PeerList` carrying multiaddrs. We dial each one;
///   they're already fully-qualified circuit-relay multiaddrs (the
///   relay's `build_peer_addrs` adds `/p2p/<relay>/p2p-circuit/p2p/<peer>`
///   suffix).
/// - **`Request`** of `PushRequest::PeerJoined`: the relay telling us
///   a new peer joined our topic. **Critical security check**: we
///   only accept this from `state.relay_peer_id` — otherwise any
///   connected peer could trick us into dialing arbitrary addresses.
fn handle_push_event(
    event: request_response::Event<PushRequest, PushResponse>,
    state: &EngineState,
    swarm: &mut Swarm<WebBehaviour>,
) {
    use request_response::{Event, Message};
    match event {
        Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } => match request {
            PushRequest::PeerJoined { topic, peer_addrs } => {
                if Some(peer) != state.relay_peer_id {
                    log::debug!("WebSyncClient: dropping PeerJoined from non-relay peer {peer}");
                    let _ = swarm.behaviour_mut().push.send_response(
                        channel,
                        PushResponse::Error {
                            message: "not authorized".into(),
                        },
                    );
                    return;
                }
                if topic != state.topic {
                    log::debug!(
                        "WebSyncClient: dropping PeerJoined — topic mismatch ({topic} vs {})",
                        state.topic
                    );
                    let _ = swarm
                        .behaviour_mut()
                        .push
                        .send_response(channel, PushResponse::Ok);
                    return;
                }
                log::info!(
                    "WebSyncClient: relay introduced new peer with {} addrs",
                    peer_addrs.len()
                );
                for addr_str in peer_addrs {
                    match addr_str.parse::<Multiaddr>() {
                        Ok(addr) => {
                            if let Err(e) = swarm.dial(addr.clone()) {
                                log::debug!(
                                    "WebSyncClient: dial of introduced peer {addr} failed: {e}"
                                );
                            }
                        }
                        Err(e) => log::debug!(
                            "WebSyncClient: invalid PeerJoined multiaddr {addr_str:?}: {e}"
                        ),
                    }
                }
                let _ = swarm
                    .behaviour_mut()
                    .push
                    .send_response(channel, PushResponse::Ok);
            }
            // Other PushRequest variants (RegisterToken, NotifyTopic,
            // etc.) are FCM-side concerns the browser doesn't speak.
            // Acknowledge generically.
            _ => {
                let _ = swarm
                    .behaviour_mut()
                    .push
                    .send_response(channel, PushResponse::Ok);
            }
        },
        Event::Message {
            peer,
            message: Message::Response { response, .. },
            ..
        } => match response {
            PushResponse::PeerList { peers } => {
                log::info!(
                    "WebSyncClient: relay {peer} returned {} known peers",
                    peers.len()
                );
                // Pick exactly ONE multiaddr per destination peer so we
                // don't kick off multiple parallel dials that libp2p
                // then immediately tears down via
                // `max_established_per_peer = 1`. Without this the
                // engine churns through connect→close→reconnect cycles
                // on every PeerList round-trip — the user-visible
                // symptom is that Add fires (`pushing changeset to N
                // peer(s)`) racing the only stable connection, and
                // most writes land on the floor.
                //
                // Preference order, highest first:
                //   1. /ws|/wss/.../p2p-circuit/...  — works directly:
                //      same outer transport as our existing relay
                //      connection, so the relay-client multiplexes the
                //      circuit substream onto the live WS conn.
                //   2. /tcp|/udp/.../p2p-circuit/... — also works
                //      because relay-client can reuse the existing
                //      relay conn regardless of the outer's transport
                //      hint, but kept second since it's slightly less
                //      direct and we want to bias toward the address
                //      that matches our actual transport stack.
                //   3. /ws|/wss/...                   — direct WS to
                //      another browser peer (rare, but valid).
                //   4. anything else                  — direct QUIC,
                //      TCP, etc. The browser has none of these so the
                //      dial fails fast; only useful as a last-resort
                //      placeholder so the dedup map has *something* to
                //      remember the peer-id we already considered.
                fn score_addr(addr: &Multiaddr) -> u8 {
                    use libp2p::multiaddr::Protocol;
                    let mut has_ws = false;
                    let mut has_circuit = false;
                    for p in addr.iter() {
                        match p {
                            Protocol::Ws(_) | Protocol::Wss(_) => has_ws = true,
                            Protocol::P2pCircuit => has_circuit = true,
                            _ => {}
                        }
                    }
                    match (has_ws, has_circuit) {
                        (true, true) => 4,
                        (false, true) => 3,
                        (true, false) => 2,
                        (false, false) => 1,
                    }
                }

                let mut best_per_peer: std::collections::HashMap<LibPeerId, Multiaddr> =
                    std::collections::HashMap::new();
                for addr_str in peers {
                    let addr: Multiaddr = match addr_str.parse() {
                        Ok(a) => a,
                        Err(e) => {
                            log::debug!(
                                "WebSyncClient: invalid PeerList multiaddr {addr_str:?}: {e}"
                            );
                            continue;
                        }
                    };
                    let target_peer = match peer_id_from_multiaddr(&addr) {
                        Some(p) => p,
                        None => continue,
                    };
                    if target_peer == *swarm.local_peer_id()
                        || Some(target_peer) == state.relay_peer_id
                    {
                        continue;
                    }
                    let new_score = score_addr(&addr);
                    best_per_peer
                        .entry(target_peer)
                        .and_modify(|cur| {
                            if score_addr(cur) < new_score {
                                *cur = addr.clone();
                            }
                        })
                        .or_insert(addr);
                }

                for (target, addr) in &best_per_peer {
                    log::debug!("WebSyncClient: dial(PeerList) → {addr} (peer {target})");
                    if let Err(e) = swarm.dial(addr.clone()) {
                        log::warn!(
                            "WebSyncClient: dial(PeerList) of {addr} returned Err synchronously: {e}"
                        );
                    }
                }
            }
            PushResponse::Error { message } => {
                log::warn!("WebSyncClient: push error from {peer}: {message}");
            }
            PushResponse::Ok => {}
        },
        Event::OutboundFailure { peer, error, .. } => {
            log::warn!("WebSyncClient: push outbound to {peer} failed: {error}");
        }
        Event::InboundFailure { peer, error, .. } => {
            log::warn!("WebSyncClient: push inbound from {peer} failed: {error}");
        }
        Event::ResponseSent { .. } => {}
    }
}

/// Extract the destination peer-id from a multiaddr.
///
/// We always want the **last** `/p2p/<id>` component, never the first.
/// For a direct multiaddr (`/ip4/.../tcp/.../p2p/<peer>`) there's only
/// one and it's the destination either way. For a circuit-relay
/// multiaddr (`/ip4/.../tcp/.../p2p/<relay>/p2p-circuit/p2p/<peer>`)
/// the FIRST `/p2p/` is the relay and the LAST is the actual target —
/// returning the first means treating every circuit address as if its
/// destination were the relay, which makes our "skip relay" filter
/// drop every circuit dial we get from the relay's PeerList. That
/// turned web→peer push into a no-op (db_v bumps fan out to 0
/// peers): the relay returned 9 addresses, the only ones that survived
/// the filter were the 3 direct QUIC ones, the browser has no QUIC
/// transport, all three failed with "Multiaddr is not supported", and
/// the engine never opened a connection to the destination peer.
fn peer_id_from_multiaddr(addr: &Multiaddr) -> Option<LibPeerId> {
    // `Multiaddr::iter()` is forward-only, so we walk it once and keep
    // the last `/p2p/<id>` we see.
    let mut last = None;
    for p in addr.iter() {
        if let libp2p::multiaddr::Protocol::P2p(id) = p {
            last = Some(id);
        }
    }
    last
}

async fn handle_snapshot_event(
    event: request_response::Event<SyncRequest, SyncResponse>,
    state: &EngineState,
    swarm: &mut Swarm<WebBehaviour>,
) {
    use request_response::{Event, Message};
    match event {
        Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } => match request {
            SyncRequest::Push {
                changeset,
                topic,
                hmac,
            } => {
                if topic != state.topic {
                    log::debug!("WebSyncClient: dropping Push from {peer} — topic mismatch");
                    return;
                }
                if let Some(gk) = &state.group_key {
                    let verify = SyncRequest::Push {
                        changeset: changeset.clone(),
                        topic: topic.clone(),
                        hmac: None,
                    };
                    let bytes = match serde_json::to_vec(&verify) {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    let tag = match hmac {
                        Some(t) => t,
                        None => {
                            log::debug!("WebSyncClient: dropping Push from {peer} — missing HMAC");
                            return;
                        }
                    };
                    if !gk.verify(&bytes, &tag) {
                        log::debug!("WebSyncClient: dropping Push from {peer} — bad HMAC");
                        return;
                    }
                }

                let _ = state.inbound_tx.send(changeset.clone());
                apply_remote_changeset(state, &peer, &changeset).await;

                let _ = swarm
                    .behaviour_mut()
                    .snapshot
                    .send_response(channel, SyncResponse::PushAck);
            }
            // Real-network VersionVector handler. Verify HMAC + topic,
            // scan persisted shadow for changes the peer hasn't seen,
            // and respond with a `ChangesetResponse`. Mirrors the
            // loopback handler's logic — same store, same query — just
            // routed through libp2p's request_response instead of
            // re-using the Push channel.
            SyncRequest::VersionVector {
                my_db_version: peer_db_version,
                your_last_db_version: since,
                site_id: peer_site,
                topic: req_topic,
                hmac,
            } => {
                if req_topic != state.topic {
                    log::debug!(
                        "WebSyncClient: dropping VersionVector from {peer} — topic mismatch"
                    );
                    return;
                }
                if let Some(gk) = &state.group_key {
                    let verify = SyncRequest::VersionVector {
                        my_db_version: peer_db_version,
                        your_last_db_version: since,
                        site_id: peer_site,
                        topic: req_topic.clone(),
                        hmac: None,
                    };
                    let bytes = match serde_json::to_vec(&verify) {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    let tag = match hmac {
                        Some(t) => t,
                        None => {
                            log::debug!(
                                "WebSyncClient: dropping VersionVector from {peer} — missing HMAC"
                            );
                            return;
                        }
                    };
                    if !gk.verify(&bytes, &tag) {
                        log::debug!("WebSyncClient: dropping VersionVector from {peer} — bad HMAC");
                        return;
                    }
                }

                let changes: Vec<ColumnChange> = match &state.store {
                    Some(store) => match store.get_changes_since(since).await {
                        Ok(c) => c,
                        Err(e) => {
                            log::warn!("WebSyncClient: catch-up scan failed: {e}");
                            Vec::new()
                        }
                    },
                    None => Vec::new(),
                };
                let my_db_version = *state.db_version.lock().await;
                let mut resp = SyncResponse::ChangesetResponse {
                    changes,
                    my_db_version,
                    your_last_db_version: peer_db_version,
                    site_id: state.site_id,
                    topic: state.topic.clone(),
                    hmac: None,
                };
                if let Some(gk) = &state.group_key {
                    let unsigned = match &resp {
                        SyncResponse::ChangesetResponse {
                            changes,
                            my_db_version,
                            your_last_db_version,
                            site_id,
                            topic,
                            ..
                        } => SyncResponse::ChangesetResponse {
                            changes: changes.clone(),
                            my_db_version: *my_db_version,
                            your_last_db_version: *your_last_db_version,
                            site_id: *site_id,
                            topic: topic.clone(),
                            hmac: None,
                        },
                        _ => resp.clone(),
                    };
                    if let Ok(bytes) = serde_json::to_vec(&unsigned) {
                        let tag = gk.mac(&bytes);
                        if let SyncResponse::ChangesetResponse { ref mut hmac, .. } = resp {
                            *hmac = Some(tag);
                        }
                    }
                }
                let _ = swarm.behaviour_mut().snapshot.send_response(channel, resp);
            }
            SyncRequest::IdentityAnnounce { .. } => {
                let _ = swarm
                    .behaviour_mut()
                    .snapshot
                    .send_response(channel, SyncResponse::IdentityAck);
            }
        },
        Event::Message {
            message: Message::Response { .. },
            ..
        } => {
            // PushAck etc. — no-op for now.
        }
        Event::OutboundFailure { peer, error, .. } => {
            log::warn!("WebSyncClient: outbound to {peer} failed: {error}");
        }
        Event::InboundFailure { peer, error, .. } => {
            log::warn!("WebSyncClient: inbound from {peer} failed: {error}");
        }
        Event::ResponseSent { .. } => {}
    }
}

/// Conflict-resolve every column change against persisted shadow state.
/// Winners are persisted and emitted on `resolved_tx`. Losers are dropped.
///
/// Mirrors the native engine's `apply_remote_changeset` minus the table
/// data side — browser apps own that and apply via `subscribe_resolved`.
/// On ephemeral clients (no store), every change is treated as a winner
/// and emitted unchanged, since there is no local state to compare.
async fn apply_remote_changeset(state: &EngineState, peer: &LibPeerId, changeset: &SyncChangeset) {
    let store = match &state.store {
        Some(s) => s,
        None => {
            // Ephemeral: surface every change as resolved.
            for change in &changeset.changes {
                let _ = state.resolved_tx.send(change.clone());
            }
            return;
        }
    };

    for change in &changeset.changes {
        let local = match store
            .get_shadow(&change.table.0, &change.pk.0, &change.cid.0)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                log::warn!("WebSyncClient: shadow read failed: {e}");
                continue;
            }
        };

        let remote_val = match &change.val {
            Some(v) => serde_json::to_vec(v).unwrap_or_default(),
            None => Vec::new(),
        };
        let (local_col_version, local_val_bytes, local_site_id) = match &local {
            Some(r) => {
                let bytes = r
                    .val
                    .as_ref()
                    .map(|v| serde_json::to_vec(v).unwrap_or_default())
                    .unwrap_or_default();
                (r.col_version, bytes, NodeId(r.site_id))
            }
            None => (0, Vec::new(), NodeId([0u8; 16])),
        };

        let apply = conflict::should_apply_column(
            change.col_version,
            &remote_val,
            &change.site_id,
            local_col_version,
            &local_val_bytes,
            &local_site_id,
        );
        if !apply {
            continue;
        }

        let new_row = ShadowRow {
            val: change.val.clone(),
            site_id: change.site_id.0,
            col_version: change.col_version,
            cl: change.cl,
            seq: change.seq,
            db_version: change.db_version,
        };
        if let Err(e) = store
            .put_shadow(&change.table.0, &change.pk.0, &change.cid.0, &new_row)
            .await
        {
            log::warn!("WebSyncClient: shadow write failed: {e}");
            continue;
        }

        let _ = state.resolved_tx.send(change.clone());
    }

    // Track the highest db_version seen from this peer for a future
    // version-vector catch-up. Failure here is non-fatal — it just means
    // catch-up will start from an earlier point next time.
    if let Err(e) = store
        .set_peer_version(&peer.to_string(), changeset.db_version)
        .await
    {
        log::debug!("WebSyncClient: peer_version write failed: {e}");
    }
}

fn build_push_request(
    topic: &str,
    group_key: Option<&GroupKey>,
    changeset: SyncChangeset,
) -> SyncRequest {
    let mut req = SyncRequest::Push {
        changeset,
        topic: topic.to_string(),
        hmac: None,
    };
    if let Some(gk) = group_key {
        // Serialize with hmac=None, MAC the canonical bytes, then attach.
        // Mirrors the native engine — keeps the verification path identical.
        if let Ok(bytes) = serde_json::to_vec(&req) {
            let tag = gk.mac(&bytes);
            if let SyncRequest::Push { ref mut hmac, .. } = req {
                *hmac = Some(tag);
            }
        }
    }
    req
}

/// Generate 16 cryptographically random bytes for a fresh `site_id`.
///
/// The browser provides this via `crypto.getRandomValues`; getrandom 0.2
/// (with the `js` feature) and 0.3 (with `wasm_js` feature + cfg) both
/// route to it.
fn rand_site_id() -> [u8; 16] {
    let mut buf = [0u8; 16];
    getrandom::getrandom(&mut buf).expect("crypto.getRandomValues failed");
    buf
}
