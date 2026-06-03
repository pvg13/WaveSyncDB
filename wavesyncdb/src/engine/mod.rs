//! P2P sync engine powered by libp2p.
//!
//! The engine runs as a background tokio task, managing a libp2p swarm with:
//! - **request-response** for real-time push (fan-out) and version vector catch-up sync
//! - **mDNS** for local peer discovery
//! - **QUIC + TCP** transports with Noise encryption and Yamux multiplexing
//! - **relay + dcutr + autonat** for NAT traversal (WIP)
//!
//! Local write operations arrive via an mpsc channel from [`WaveSyncDb`](crate::WaveSyncDb)
//! as [`SyncChangeset`]s and are pushed to all connected peers via request-response.
//! Incoming remote changesets are applied column-by-column using per-column
//! Lamport clocks for conflict resolution.

pub(crate) mod auth_protocol;
pub(crate) mod behaviour;
pub(crate) mod command_handler;
pub(crate) mod identity_handler;
pub(crate) mod peer_manager;
pub(crate) mod push_protocol;
pub(crate) mod reconcile;
pub(crate) mod relay_manager;
pub(crate) mod snapshot_protocol;
pub(crate) mod sync_handler;

use sync_handler::apply_remote_changeset;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use behaviour::{WaveSyncBehaviour, WaveSyncBehaviourEvent};
use futures::FutureExt;
use libp2p::{
    Multiaddr, SwarmBuilder, autonat, dcutr, dns, futures::StreamExt, identify, identity, mdns,
    noise, ping, relay, rendezvous, request_response, swarm::SwarmEvent, yamux,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use std::panic::AssertUnwindSafe;
use tokio::sync::{Notify, broadcast, mpsc};

use crate::auth::GroupKey;
use crate::conflict;
use crate::messages::{ChangeNotification, ColumnChange, NodeId, SyncChangeset, WriteKind};
use crate::peer_tracker;
use crate::protocol::SyncRequest;
use crate::registry::TableRegistry;
use crate::shadow;

/// A remote changeset queued for sequential application in the main event loop.
///
/// Carries the context needed to record our knowledge of the sender's
/// `db_version` *after* the changes durably commit — never before. Persisting
/// the peer version ahead of the commit risks claiming a version whose changes
/// we never actually applied (e.g. the process is torn down between queueing
/// and applying). On the next launch that peer would be told to send only
/// changes *above* the claimed version, silently skipping the dropped range.
pub(crate) struct RemoteChangeset {
    pub peer: libp2p::PeerId,
    pub peer_site: NodeId,
    /// `Some(version)` for version-vector responses: record this once the
    /// changes are applied. `None` for real-time `Push` frames, whose version
    /// is only tracked in-memory via `max()` and is not persisted from here.
    pub peer_db_version: Option<u64>,
    /// Effective (PSK-derived) topic identifying which group's DB to apply to.
    pub effective_topic: String,
    pub changes: Vec<ColumnChange>,
}

/// A local write tagged with the group it originated from.
///
/// Every local write funnels through the connection layer's single
/// `sync_tx.send(..)` site, which stamps the originating group's effective
/// (PSK-derived) topic onto the changeset. The engine routes on this tag so a
/// write to one group's DB only fans out on that group's topic / key. For a
/// single-group node the tag is always the default group's topic, so behaviour
/// is identical to the pre-multi-group path.
pub(crate) struct TaggedChangeset {
    pub effective_topic: String,
    pub changeset: SyncChangeset,
}

/// Everything the engine needs to stand up a new [`GroupState`] at runtime in
/// response to [`EngineCommand::JoinGroup`]. Built entirely on the connection
/// side (its own DB connection, broadcast channels, registry, hydrated peer
/// versions) so the engine handler just inserts the group and wires discovery.
pub struct GroupInit {
    pub db: DatabaseConnection,
    pub user_topic: String,
    pub effective_topic: String,
    pub group_key: Option<GroupKey>,
    pub site_id: NodeId,
    pub local_db_version: u64,
    pub db_version_cache: Arc<std::sync::atomic::AtomicU64>,
    pub registry: Arc<TableRegistry>,
    pub registry_ready: Arc<Notify>,
    pub change_tx: broadcast::Sender<ChangeNotification>,
    pub notification_tx: broadcast::Sender<crate::notify::Notification>,
    pub notification_registry: Arc<crate::registry::NotificationRegistry>,
    pub peer_db_versions: HashMap<libp2p::PeerId, u64>,
}

impl std::fmt::Debug for GroupInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupInit")
            .field("user_topic", &self.user_topic)
            .field("effective_topic", &self.effective_topic)
            .field("local_db_version", &self.local_db_version)
            .finish_non_exhaustive()
    }
}

/// Commands sent from the application to the P2P engine.
#[derive(Debug)]
pub enum EngineCommand {
    /// App resumed from background — clear stale peers, restart mDNS, re-sync.
    Resume,
    /// Network interface changed (WiFi ↔ cellular) — force-disconnect all
    /// connections and re-establish on the new interface.
    NetworkTransition,
    /// Request a full sync from peers (user-triggered).
    RequestFullSync,
    /// Register a push notification token with the relay server.
    RegisterPushToken { platform: String, token: String },
    /// Set or clear the local application-level peer identity.
    SetPeerIdentity(Option<String>),
    /// Enable or disable mDNS LAN discovery at runtime. When disabling,
    /// existing mDNS-discovered peer connections are kept; only future
    /// announcements and queries are silenced. When enabling, the mDNS
    /// behaviour is rebuilt and starts queries immediately.
    SetMdnsEnabled(bool),
    /// Graceful shutdown — stop the engine loop.
    Shutdown,
    /// Join a new sync group at runtime. The group's DB, channels, registry,
    /// and hydrated peer versions are built on the connection side and handed
    /// to the engine, which inserts the `GroupState` and wires up discovery
    /// (rendezvous namespace + sweeping connected peers for the new topic).
    JoinGroup(Box<GroupInit>),
    /// Leave a sync group at runtime. The `GroupState` is removed so the group
    /// stops syncing; the rendezvous namespace simply TTL-expires. The DB file
    /// is preserved (the connection side closes its handle separately).
    LeaveGroup { effective_topic: String },
    /// A runtime-joined (non-default) group has finished registering its schema.
    /// The default group signals readiness via its `registry_ready` Notify (which
    /// the engine awaits directly); joined groups have no such await, so their
    /// readiness arrives as this command. Flipping `registry_is_ready` lets the
    /// group participate in connect/discovery-time sync initiation (which only
    /// fires for ready groups) — without it a joined group only ever syncs via
    /// the periodic tick, making it slow and one-directional/asymmetric.
    GroupRegistryReady { effective_topic: String },
}

/// Configuration for the sync engine.
pub struct EngineConfig {
    /// How often to run periodic version vector sync (default: 30s).
    pub sync_interval: Duration,
    /// Whether mDNS LAN discovery is announced and queried (default: `true`).
    /// Apps that don't want to broadcast their presence on every LAN they
    /// touch can disable this and rely on rendezvous / relay discovery.
    /// Can be flipped at runtime via [`WaveSyncDb::set_mdns_enabled`].
    pub mdns_enabled: bool,
    /// How often mDNS sends queries (default: 5s for fast LAN discovery).
    pub mdns_query_interval: Duration,
    /// How long mDNS records stay valid (default: 30s).
    pub mdns_ttl: Duration,
    /// Static bootstrap peers to dial on startup.
    pub bootstrap_peers: Vec<Multiaddr>,
    /// Relay server multiaddr for NAT traversal (the primary).
    pub relay_server: Option<Multiaddr>,
    /// Fallback relay servers, tried in order if the primary fails
    /// repeatedly. Removes the single-point-of-failure for cold-start
    /// peer discovery without changing the connect-to-one-relay-at-a-time
    /// data path. See `relay_manager::rotate_to_next_relay`.
    pub relay_fallbacks: Vec<Multiaddr>,
    /// Rendezvous server multiaddr for WAN peer discovery.
    pub rendezvous_server: Option<Multiaddr>,
    /// How often to discover peers via rendezvous (default: 60s).
    pub rendezvous_discover_interval: Duration,
    /// TTL for rendezvous registration in seconds (default: 300s).
    pub rendezvous_ttl: u64,
    /// Whether to listen on IPv6 in addition to IPv4. Default: `true`.
    ///
    /// IPv6 sidesteps CGNAT entirely — when both peers have native v6
    /// (now the case on T-Mobile, Verizon, Jio, and most modern cellular
    /// carriers per Google's 2026 stats showing >50% global IPv6 traffic),
    /// the relay is no longer on the critical path for connection
    /// establishment. Falls back to IPv4 transparently via Happy Eyeballs.
    pub ipv6: bool,
    /// Push notification token: (platform, device_token).
    /// Platform is "Fcm" or "Apns".
    pub push_token: Option<(String, String)>,
    /// API key for managed relay authentication.
    pub api_key: Option<String>,
    /// Interval for libp2p ping keep-alives (default: 90s).
    /// Must be shorter than CGNAT mapping timeouts (typically 2–5 min for UDP).
    pub keep_alive_interval: Duration,
    /// Maximum relay circuit duration the server allows (default: 3600s).
    /// The engine proactively renews at 80% of this duration.
    pub circuit_max_duration: Duration,
    /// Opt-in TCP transport in addition to the default QUIC. Default: `false`.
    ///
    /// Enable for deployments where users hit networks that block UDP
    /// entirely (some corporate firewalls, captive-portal Wi-Fi). Costs
    /// ~1 extra RTT on cold start and can confuse circuit-relay on
    /// cellular (see `build_swarm` doc-comment); only flip on if the
    /// QUIC-only failure mode is actually hurting your users.
    pub tcp_enabled: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(30),
            mdns_enabled: true,
            mdns_query_interval: Duration::from_secs(5),
            mdns_ttl: Duration::from_secs(30),
            bootstrap_peers: Vec::new(),
            relay_server: None,
            relay_fallbacks: Vec::new(),
            rendezvous_server: None,
            rendezvous_discover_interval: Duration::from_secs(60),
            rendezvous_ttl: 300,
            ipv6: true,
            push_token: None,
            api_key: None,
            keep_alive_interval: Duration::from_secs(90),
            circuit_max_duration: Duration::from_secs(3600),
            tcp_enabled: false,
        }
    }
}

impl EngineConfig {
    /// Build the libp2p mDNS config from our settings.
    pub(crate) fn mdns_config(&self) -> mdns::Config {
        mdns::Config {
            query_interval: self.mdns_query_interval,
            ttl: self.mdns_ttl,
            ..Default::default()
        }
    }
}

/// Start the P2P sync engine in a background tokio task.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start_engine(
    db: DatabaseConnection,
    sync_rx: mpsc::Receiver<TaggedChangeset>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    site_id: NodeId,
    topic: String,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
    cmd_rx: mpsc::Receiver<EngineCommand>,
    group_key: Option<GroupKey>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    diagnostics: Arc<crate::diagnostics::Counters>,
    db_version_cache: Arc<std::sync::atomic::AtomicU64>,
    notification_tx: broadcast::Sender<crate::notify::Notification>,
    notification_registry: Arc<crate::registry::NotificationRegistry>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let event_tx = network_event_tx.clone();
        let result = AssertUnwindSafe(run_engine(
            db,
            sync_rx,
            change_tx,
            registry,
            site_id,
            topic,
            config,
            registry_ready,
            cmd_rx,
            group_key,
            network_status,
            network_event_tx,
            diagnostics,
            db_version_cache,
            notification_tx,
            notification_registry,
        ))
        .catch_unwind()
        .await;

        match result {
            Ok(Ok(())) => log::info!("Engine shut down cleanly"),
            Ok(Err(e)) => {
                let reason = format!("{e}");
                log::error!("Engine error: {reason}");
                let _ = event_tx.send(crate::network_status::NetworkEvent::EngineFailed { reason });
            }
            Err(panic) => {
                let msg = panic
                    .downcast_ref::<String>()
                    .map(|s| s.as_str())
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic");
                log::error!("Engine panicked: {msg}");
                let _ = event_tx.send(crate::network_status::NetworkEvent::EngineFailed {
                    reason: format!("panic: {msg}"),
                });
            }
        }
    })
}

/// Build the libp2p swarm with DNS resolution.
///
/// Tries system DNS first (`/etc/resolv.conf`). If that fails (e.g. on Android
/// where `/etc/resolv.conf` does not exist), falls back to Google public DNS
/// via `dns::ResolverConfig::google()`.
///
/// When `tcp_enabled = true`, adds TCP as a secondary transport. See
/// [`build_swarm_with_tcp`] and the `with_tcp_enabled` builder method.
/// iOS: enumerate the device's routable interface addresses for QUIC binding.
///
/// Returns every non-loopback, non-link-local address on an up interface
/// (Wi-Fi `en0`, cellular `pdp_ip0`, etc.), filtered to IPv4 unless `ipv6` is
/// enabled. These are the addresses a concrete-interface QUIC listener binds
/// to so that libp2p-quic sources outbound dials from a WAN-routable socket
/// instead of loopback (#72). An empty result means the device currently has
/// no usable interface (offline / between handoffs); the interface-watch tick
/// will bind once one appears.
#[cfg(target_os = "ios")]
fn routable_listen_ips(ipv6: bool) -> Vec<std::net::IpAddr> {
    // Prefer the single *default-route* interface address(es). A real iOS device
    // usually has several non-loopback interfaces up simultaneously — Wi-Fi
    // (`en0`), cellular kept warm under Wi-Fi (`pdp_ip0`), and VPN / iCloud
    // Private Relay (`utunN`). Binding a QUIC listener on every one of them lets
    // libp2p-quic's `PortUse::Reuse` dialer source an outbound dial from the
    // wrong interface (cellular / Private-Relay), which is unroutable — so
    // relay / rendezvous / direct dials silently fail and the device never
    // reaches the relay. The Simulator only ever has the Mac's single
    // interface, so it has no wrong choice to make — which is why WAN sync works
    // there but not on hardware. Asking the kernel which source address it would
    // use to reach the public internet pins QUIC to the one interface a WAN dial
    // must originate from.
    let mut ips = Vec::new();
    if let Some(v4) = default_route_ip(false) {
        ips.push(v4);
    }
    if ipv6 && let Some(v6) = default_route_ip(true) {
        ips.push(v6);
    }
    if !ips.is_empty() {
        return ips;
    }

    // Fallback (no default route — e.g. transiently offline / between handoffs):
    // enumerate routable interfaces so we still bind *something*. The
    // interface-watch tick re-runs this and converges onto the default-route
    // address once connectivity returns.
    let ifaces = match if_addrs::get_if_addrs() {
        Ok(i) => i,
        Err(e) => {
            log::warn!("getifaddrs failed; cannot enumerate interfaces for QUIC bind: {e}");
            return Vec::new();
        }
    };
    // `Interface::is_link_local` is platform-aware (IPv4 169.254.0.0/16 and
    // IPv6 fe80::/10). Link-local addresses are not WAN-routable and need a
    // scope id a bare multiaddr can't carry, so they're excluded.
    ifaces
        .into_iter()
        .filter(|iface| !iface.is_loopback() && !iface.is_link_local())
        .map(|iface| iface.ip())
        .filter(|ip| ipv6 || ip.is_ipv4())
        .collect()
}

/// iOS: the source IP the kernel would use to reach the public internet over
/// `ipv6`, discovered via a connected (but otherwise unused) UDP socket.
///
/// A connected UDP socket sends no packets — `connect` only fixes the route and
/// the local source address, which `local_addr` then reveals. That is exactly
/// the interface a relay / peer dial must originate from, so binding QUIC to it
/// keeps libp2p-quic's reuse-dialer on the routable interface. Returns `None`
/// when offline or when the resolved address is loopback / unspecified.
#[cfg(target_os = "ios")]
fn default_route_ip(ipv6: bool) -> Option<std::net::IpAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
    let (bind, dst): (SocketAddr, SocketAddr) = if ipv6 {
        (
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
            // Cloudflare public resolver (2606:4700:4700::1111); no packet sent.
            SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111)),
                53,
            ),
        )
    } else {
        (
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
        )
    };
    let sock = UdpSocket::bind(bind).ok()?;
    sock.connect(dst).ok()?;
    let ip = sock.local_addr().ok()?.ip();
    if ip.is_loopback() || ip.is_unspecified() {
        None
    } else {
        Some(ip)
    }
}

/// iOS: build a `/ip{4,6}/<addr>/udp/0/quic-v1` listen multiaddr for `ip`
/// (port 0 = let the OS pick an ephemeral UDP port).
#[cfg(target_os = "ios")]
fn quic_listen_multiaddr(ip: std::net::IpAddr) -> libp2p::Multiaddr {
    use libp2p::multiaddr::Protocol;
    let mut addr = libp2p::Multiaddr::empty();
    match ip {
        std::net::IpAddr::V4(v4) => addr.push(Protocol::Ip4(v4)),
        std::net::IpAddr::V6(v6) => addr.push(Protocol::Ip6(v6)),
    }
    addr.push(Protocol::Udp(0));
    addr.push(Protocol::QuicV1);
    addr
}

/// Whether a multiaddr is a circuit-relay path (`/.../p2p-circuit/...`), i.e.
/// a connection over it is carried by the relay server rather than direct.
/// Used for relay-cost telemetry (which peers / connections cost relay
/// bandwidth) — see [`crate::diagnostics`] and `PeerInfo::via_relay`.
fn addr_is_relayed(addr: &libp2p::Multiaddr) -> bool {
    addr.iter()
        .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2pCircuit))
}

/// Given a peer's introduced address set, drop circuit-relay addresses when a
/// direct path to that peer already exists (`prefers_direct`). Pure helper so
/// the storm-prevention rule is unit-testable without a swarm.
///
/// This is the core fix for the circuit-relay storm (#84 regression): the relay
/// re-introduces a peer's circuit address on every presence announce, and the
/// `#84` demotion immediately closes any circuit connection to a peer we already
/// reach directly. Re-dialing that circuit each time it is re-introduced is an
/// establish→close→redial loop that exhausts the relay's per-peer circuit cap
/// (`ResourceLimitExceeded`). Filtering circuit addresses here — keyed on the
/// stable `peer_via_relay == false` marker, which survives the brief
/// `is_connected` flicker right after a demotion close — breaks the loop while
/// still allowing the circuit path when no direct path exists.
fn dialable_addrs_preferring_direct(
    addrs: Vec<libp2p::Multiaddr>,
    prefers_direct: bool,
) -> Vec<libp2p::Multiaddr> {
    if prefers_direct {
        addrs.into_iter().filter(|a| !addr_is_relayed(a)).collect()
    } else {
        addrs
    }
}

/// Whether a multiaddr is on the local network — RFC1918 private IPv4, IPv4
/// link-local, loopback, or IPv6 ULA (`fc00::/7`) / link-local (`fe80::/10`).
/// Such a peer is reachable directly over the LAN, so the relay circuit to it
/// is both unnecessary and (with router hairpinning often unsupported) less
/// reliable — prefer the LAN path on the same-Wi-Fi case. Looks at the first IP
/// literal; non-IP (e.g. `/dns4`) and public addresses return false.
fn addr_is_lan(addr: &libp2p::Multiaddr) -> bool {
    use libp2p::multiaddr::Protocol;
    for p in addr.iter() {
        match p {
            Protocol::Ip4(ip) => {
                return ip.is_private() || ip.is_link_local() || ip.is_loopback();
            }
            Protocol::Ip6(ip) => {
                let seg = ip.segments();
                let ula = (seg[0] & 0xfe00) == 0xfc00;
                let link_local = (seg[0] & 0xffc0) == 0xfe80;
                return ip.is_loopback() || ula || link_local;
            }
            _ => continue,
        }
    }
    false
}

/// QUIC transport tuning shared by every swarm builder.
///
/// libp2p-quic's default `max_idle_timeout` is **10s** — far too aggressive for
/// relay-carried mobile connections. A brief Wi-Fi/radio/NAT interruption stops
/// packets for longer than 10s, the connection is declared dead with
/// `ConnectionError(TimedOut)`, and the whole stack reconnects (new reservation,
/// circuit re-dials, presence re-announce) — which on a flaky link produces a
/// continuous reconnect/circuit storm against the relay. Raise the idle timeout
/// to 30s so transient blips are tolerated, and keep a frequent keep-alive so
/// NAT UDP mappings stay open between heartbeats.
fn tune_quic(mut cfg: libp2p::quic::Config) -> libp2p::quic::Config {
    cfg.max_idle_timeout = 30_000;
    cfg.keep_alive_interval = Duration::from_secs(5);
    cfg
}

fn build_swarm(
    keypair: identity::Keypair,
    mdns_config: Option<mdns::Config>,
    keep_alive_interval: Duration,
    tcp_enabled: bool,
) -> Result<libp2p::Swarm<WaveSyncBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    if tcp_enabled {
        return build_swarm_with_tcp(keypair, mdns_config, keep_alive_interval);
    }
    // QUIC-only (no TCP). Two reasons:
    //
    // 1. **Cold-start latency.** QUIC is 1 RTT for fresh handshake and 0-RTT
    //    on resume; TCP+TLS+yamux is 2–3 RTT. On 100ms cellular, that's a
    //    ~200ms saving on every FCM-triggered sync — directly visible in
    //    bg_sync timing.
    //
    // 2. **Single transport per peer = single connection per peer.** With
    //    both TCP and QUIC enabled, every peer dial races both protocols and
    //    both succeed, leaving us with two simultaneous connections. That
    //    confused libp2p's relay-client behaviour: circuit-relay dials
    //    failed with "Response from behaviour was canceled: oneshot
    //    canceled" on cellular when there were two relay connections. With
    //    QUIC-only there's exactly one connection per peer, so we can raise
    //    `max_established_per_peer` to 2 (allowing DCUtR's direct upgrade)
    //    without breaking circuit-relay.
    //
    // The cost: networks that block UDP entirely (corporate firewalls,
    // captive-portal Wi-Fi) can't sync. In practice this is rare for the
    // mobile-sync target audience — but apps with users on UDP-hostile
    // networks can opt in via `WaveSyncDbBuilder::with_tcp_enabled(true)`.
    let system_result = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_quic_config(tune_quic)
        .with_dns();

    match system_result {
        Ok(builder) => {
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(builder
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
        Err(e) => {
            log::warn!(
                "System DNS resolver failed (expected on Android): {e}. \
                 Falling back to Google public DNS."
            );
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_quic_config(tune_quic)
                .with_dns_config(dns::ResolverConfig::google(), dns::ResolverOpts::default())
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
    }
}

/// Build the libp2p swarm with both TCP and QUIC transports.
///
/// Opt-in path for apps whose users may be on networks that block UDP
/// entirely (some corporate firewalls, captive-portal Wi-Fi). Pays the
/// cold-start RTT cost (TCP+TLS+yamux is 2–3 RTT vs QUIC's 1 RTT) and
/// allows the connection-per-peer limit of 2 to be filled by both
/// protocols, which the original docs at `build_swarm` flagged as
/// having broken circuit-relay on cellular. Apps that opt in are
/// accepting that trade-off.
fn build_swarm_with_tcp(
    keypair: identity::Keypair,
    mdns_config: Option<mdns::Config>,
    keep_alive_interval: Duration,
) -> Result<libp2p::Swarm<WaveSyncBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    use libp2p::tcp;

    let system_result = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic_config(tune_quic)
        .with_dns();

    match system_result {
        Ok(builder) => {
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(builder
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
        Err(e) => {
            log::warn!(
                "System DNS resolver failed (expected on Android): {e}. \
                 Falling back to Google public DNS."
            );
            let mdns_cfg = mdns_config;
            let ping_interval = keep_alive_interval;
            Ok(SwarmBuilder::with_existing_identity(keypair)
                .with_tokio()
                .with_tcp(
                    tcp::Config::default(),
                    noise::Config::new,
                    yamux::Config::default,
                )?
                .with_quic_config(tune_quic)
                .with_dns_config(dns::ResolverConfig::google(), dns::ResolverOpts::default())
                .with_relay_client(noise::Config::new, yamux::Config::default)?
                .with_behaviour(move |key, relay_client| {
                    WaveSyncBehaviour::new(key, relay_client, mdns_cfg, ping_interval)
                })?
                .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(300)))
                .build())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_engine(
    db: DatabaseConnection,
    mut sync_rx: mpsc::Receiver<TaggedChangeset>,
    change_tx: broadcast::Sender<ChangeNotification>,
    registry: Arc<TableRegistry>,
    site_id: NodeId,
    topic_name: String,
    config: EngineConfig,
    registry_ready: Arc<Notify>,
    cmd_rx: mpsc::Receiver<EngineCommand>,
    group_key: Option<GroupKey>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    diagnostics: Arc<crate::diagnostics::Counters>,
    db_version_cache: Arc<std::sync::atomic::AtomicU64>,
    notification_tx: broadcast::Sender<crate::notify::Notification>,
    notification_registry: Arc<crate::registry::NotificationRegistry>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load (or create on first launch) the persistent libp2p keypair from
    // _wavesync_meta. Stable PeerId across restarts is necessary so the
    // relay-side push-token store doesn't accumulate stale duplicates and
    // peer_versions tracking actually points at a stable identity.
    let keypair = shadow::get_or_create_libp2p_keypair(&db).await?;
    let mdns_config = if config.mdns_enabled {
        Some(config.mdns_config())
    } else {
        None
    };

    let swarm = build_swarm(
        keypair.clone(),
        mdns_config,
        config.keep_alive_interval,
        config.tcp_enabled,
    )?;

    let local_peer_id = keypair.public().to_peer_id();
    log::info!("Local libp2p PeerId (persistent): {local_peer_id}");

    // Load current db_version
    let local_db_version = shadow::get_db_version(&db).await?;

    // Hydrate our last-known db_version for each peer from the persistent
    // _wavesync_peer_versions table. Without this the map starts empty every
    // launch, so the first version-vector request to each peer would carry
    // your_last_db_version=0 and force a full re-sync of the entire database —
    // on every cold start and every push-triggered background sync. The
    // persisted value is always <= what we durably applied (it is written only
    // after a changeset commits), so requesting changes strictly above it never
    // skips data. Peer IDs are stable across restarts (persistent libp2p
    // keypair), so the rows still point at the right peers.
    let peer_db_versions: HashMap<libp2p::PeerId, u64> =
        match peer_tracker::get_all_peer_versions(&db).await {
            Ok(rows) => peer_tracker::parse_peer_versions(rows),
            Err(e) => {
                log::warn!("Failed to hydrate peer versions from disk: {e}");
                HashMap::new()
            }
        };

    let (snapshot_resp_tx, snapshot_resp_rx) = mpsc::channel::<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>(8);

    let (remote_changeset_tx, remote_changeset_rx) = mpsc::channel::<RemoteChangeset>(32);

    // Reconcile-digest requests are built by a spawned task (the digest is an
    // async DB scan) and handed back here so the event loop — the only owner of
    // the non-Sync swarm — actually sends them (Rule 2.10). Mirrors
    // `snapshot_resp_tx` for the response direction.
    let (reconcile_req_tx, reconcile_req_rx) =
        mpsc::channel::<(libp2p::PeerId, crate::protocol::SyncRequest)>(16);

    let effective_topic = match &group_key {
        Some(gk) => gk.derive_topic(&topic_name),
        None => topic_name.clone(),
    };

    // Determine rendezvous namespace (same as effective_topic — already PSK-derived)
    let rendezvous_namespace = effective_topic.clone();

    let push_token = config.push_token.clone();
    let api_key = config.api_key.clone();

    // Collect infrastructure peer IDs (relay + rendezvous) so they are excluded
    // from self.peers, peer counts, and sync fan-out.
    let mut infrastructure_peers = std::collections::HashSet::new();
    if let Some(ref addr) = config.relay_server
        && let Some(libp2p::multiaddr::Protocol::P2p(pid)) = addr.iter().last()
    {
        infrastructure_peers.insert(pid);
    }
    if let Some(ref addr) = config.rendezvous_server
        && let Some(libp2p::multiaddr::Protocol::P2p(pid)) = addr.iter().last()
    {
        infrastructure_peers.insert(pid);
    }

    let mdns_enabled = config.mdns_enabled;
    let default_effective_topic = effective_topic.clone();
    let default_group = GroupState {
        db,
        change_tx,
        registry,
        notification_tx,
        notification_registry,
        site_id,
        user_topic: topic_name,
        topic_name: effective_topic,
        local_db_version,
        db_version_cache,
        peer_db_versions,
        peer_reported_versions: HashMap::new(),
        registry_ready,
        registry_is_ready: false,
        group_key,
        rendezvous_namespace,
        rendezvous_registered: false,
        rejected_peers: std::collections::HashMap::new(),
        verified_peers: std::collections::HashSet::new(),
        pending_sync_peers: std::collections::HashSet::new(),
        pending_pushes: std::collections::BTreeMap::new(),
    };
    let mut groups = HashMap::new();
    groups.insert(default_effective_topic.clone(), default_group);
    let mut engine = EngineRunner {
        swarm,
        peers: HashMap::new(),
        groups,
        default_effective_topic,
        local_peer_id,
        config,
        mdns_enabled,
        snapshot_resp_tx,
        snapshot_resp_rx,
        remote_changeset_tx,
        remote_changeset_rx,
        reconcile_req_tx,
        reconcile_req_rx,
        cmd_rx,
        relay_state: RelayState::Disabled,
        nat_status: NatStatus::Unknown,
        bootstrap_peers: std::collections::HashSet::new(),
        local_app_id: None,
        peer_identities: HashMap::new(),
        infrastructure_peers,
        dialing_peers: std::collections::HashSet::new(),
        pending_rendezvous_dials: VecDeque::new(),
        push_token,
        push_registered_topics: std::collections::HashSet::new(),
        push_pending_registrations: std::collections::HashMap::new(),
        pending_push_reqs: std::collections::HashMap::new(),
        relayed_conn_ids: std::collections::HashMap::new(),
        network_status,
        network_event_tx,
        resume_sync_deadline: None,
        api_key,
        keypair,
        nat_assumption_deadline: None,
        circuit_accepted_at: None,
        circuit_retry_count: 0,
        circuit_listen_pending: false,
        relay_dial_pending: false,
        dcutr_retries: HashMap::new(),
        peer_dial_backoff: HashMap::new(),
        lan_peers: std::collections::HashSet::new(),
        pending_demotions: HashMap::new(),
        diagnostics,
        protocol_mismatch_peers: std::collections::HashSet::new(),
        peer_via_relay: std::collections::HashMap::new(),
        reconcile_capable: std::collections::HashSet::new(),
        #[cfg(target_os = "ios")]
        quic_listeners: std::collections::HashMap::new(),
    };

    // Set initial network status with local_peer_id and topic.
    engine.update_network_status();
    // Fire the local-data-ready signal BEFORE EngineStarted: by this
    // point the database connection is open, the keypair and db_version
    // are loaded, and the registry is wired up — i.e. the application
    // can already query its data. EngineStarted is reserved for "the
    // swarm is alive and the network status snapshot is meaningful",
    // which is a strictly later event.
    engine.emit_network_event(crate::network_status::NetworkEvent::LocalDataReady);
    engine.emit_network_event(crate::network_status::NetworkEvent::EngineStarted);

    engine.run(&mut sync_rx).await
}

/// State machine for relay server connection lifecycle.
#[derive(Debug)]
pub(crate) enum RelayState {
    /// No relay server configured.
    Disabled,
    /// Connecting to relay server (with retry count for backoff).
    Connecting { retry_count: u32 },
    /// TCP/QUIC connection established with relay peer but circuit not yet reserved.
    Connected {
        relay_peer_id: libp2p::PeerId,
        connected_at: tokio::time::Instant,
    },
    /// Listening on a relay circuit (reservation accepted).
    Listening { relay_peer_id: libp2p::PeerId },
}

use crate::network_status::NatStatus;

/// Per-group state: one per sync group the node has joined. Each group is backed
/// by its own SQLite database (own shadow tables, db_version, peer_versions,
/// registry, site_id). The shared libp2p swarm routes inbound messages to the
/// right group by effective (PSK-derived) topic.
pub(crate) struct GroupState {
    pub(crate) db: DatabaseConnection,
    pub(crate) change_tx: broadcast::Sender<ChangeNotification>,
    pub(crate) registry: Arc<TableRegistry>,
    pub(crate) notification_tx: broadcast::Sender<crate::notify::Notification>,
    pub(crate) notification_registry: Arc<crate::registry::NotificationRegistry>,
    pub(crate) site_id: NodeId,
    /// User-supplied topic (pre-derivation), kept for diagnostics / config.
    #[allow(dead_code)]
    pub(crate) user_topic: String,
    /// Effective (PSK-derived) topic — the on-the-wire routing key.
    pub(crate) topic_name: String,
    pub(crate) local_db_version: u64,
    pub(crate) db_version_cache: Arc<std::sync::atomic::AtomicU64>,
    pub(crate) peer_db_versions: HashMap<libp2p::PeerId, u64>,
    /// Display-only peer versions from incoming requests (NOT used for sync decisions).
    pub(crate) peer_reported_versions: HashMap<libp2p::PeerId, u64>,
    pub(crate) registry_ready: Arc<Notify>,
    pub(crate) registry_is_ready: bool,
    pub(crate) group_key: Option<GroupKey>,
    /// Rendezvous namespace for peer discovery (per group).
    pub(crate) rendezvous_namespace: String,
    pub(crate) rendezvous_registered: bool,
    /// Peers rejected for THIS group: a per-group HMAC failure for a topic we
    /// hold (a spoofed / incorrect-key request — an honest peer that derived
    /// the right topic necessarily has the right key). Time-boxed with
    /// exponential backoff rather than permanent (Rule 2.8 anti-storm intent),
    /// and removed when the peer later passes verification (recovery, N6). A
    /// peer rejected here may still be a valid member of another group.
    pub(crate) rejected_peers: std::collections::HashMap<libp2p::PeerId, RejectionState>,
    /// Peers verified via successful HMAC exchange for THIS group.
    pub(crate) verified_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers with an in-flight sync request for THIS group.
    pub(crate) pending_sync_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Recently fanned-out local changesets not yet confirmed delivered to every
    /// connected peer, keyed by their `db_version` (#81 Option A). A real-time
    /// push is fire-and-forget; if it's dropped to a still-connected peer the
    /// data otherwise waits up to a full `sync_interval` for the reconcile
    /// catch-up to redeliver it. Re-pushing these on a short cadence closes that
    /// latency gap. In-memory only — durability and *eventual* delivery are
    /// already guaranteed by the shadow tables + RBSR (#82); this just makes the
    /// common case fast. Bounded by [`MAX_PENDING_PUSHES`]; an entry is dropped
    /// once every connected peer has acked it (or proven converged), and the
    /// oldest is evicted to the reconcile backstop on overflow.
    pub(crate) pending_pushes: std::collections::BTreeMap<u64, PendingPush>,
}

/// A locally-originated changeset awaiting confirmed delivery. See
/// [`GroupState::pending_pushes`].
#[derive(Debug, Clone)]
pub(crate) struct PendingPush {
    /// The changeset to (re)push. Idempotent to re-apply (CRDT), so re-delivery
    /// is always safe.
    pub(crate) changeset: SyncChangeset,
    /// Peers that have acked this changeset (or are proven converged past it).
    pub(crate) acked_by: std::collections::HashSet<libp2p::PeerId>,
}

/// Cap on per-group un-acked pending pushes. Past this the oldest is evicted —
/// it is never lost (it's committed in the shadow tables and the reconcile
/// catch-up still carries it), it just stops getting the fast-path retry.
pub(crate) const MAX_PENDING_PUSHES: usize = 256;

/// Per-(group, peer) rejection backoff state. See [`GroupState::rejected_peers`].
#[derive(Debug, Clone)]
pub(crate) struct RejectionState {
    /// Consecutive rejections for this peer (1-based).
    pub(crate) attempts: u32,
    /// The peer is skipped (dialing / sync) until this instant. After it, the
    /// peer is eligible for one re-evaluation: a continued mismatch extends the
    /// backoff, a successful verify removes the entry.
    pub(crate) until: tokio::time::Instant,
}

/// Exponential rejection backoff: 30s, 60s, 120s, … capped at 1 hour. Bounds
/// how often a persistently-mismatching (e.g. spoofed-topic) peer is
/// re-evaluated — the Rule 2.8 anti-storm guarantee — while still letting a
/// transiently-misconfigured peer recover on a later attempt.
fn rejection_backoff(attempts: u32) -> Duration {
    const BASE_SECS: u64 = 30;
    const MAX_SECS: u64 = 3600;
    let shift = attempts.saturating_sub(1).min(20);
    let secs = BASE_SECS.saturating_mul(1u64 << shift).min(MAX_SECS);
    Duration::from_secs(secs)
}

/// Per-peer dial backoff schedule, lifted from go-libp2p's swarm: a peer is
/// re-dialable after `BASE + COEF * priorFailures²`, capped at `MAX`. With
/// `priorFailures` 1-based (1 = "one consecutive failure so far"), this yields
/// 6s, 9s, 14s, 21s, … capped at 5 minutes. Quadratic (not exponential) growth
/// keeps early retries responsive while still throttling a peer that keeps
/// failing — the anti-storm guard for redials on shared networks where mDNS /
/// the relay keep re-surfacing an unreachable peer. A successful connection
/// clears the entry entirely (`clear_dial_backoff`), so this never penalises a
/// peer that has recovered.
/// Anti-thrash dwell before a relay connection is closed once a direct path to
/// the peer comes up (#84 demotion). A DCUtR hole-punch can succeed and then
/// drop seconds later; holding the relay open for this window lets the direct
/// path prove it will hold before we pay to re-establish the relay. ~10s is
/// inside Tailscale's published "keep the fallback warm, switch only when the
/// new path is stable" guidance and well under a human-perceptible sync delay.
const DEMOTION_DWELL: Duration = Duration::from_secs(10);

fn peer_dial_backoff(prior_failures: u32) -> Duration {
    const BASE_SECS: u64 = 5;
    const COEF_SECS: u64 = 1;
    const MAX_SECS: u64 = 300;
    let n = u64::from(prior_failures);
    let secs = BASE_SECS
        .saturating_add(COEF_SECS.saturating_mul(n.saturating_mul(n)))
        .min(MAX_SECS);
    Duration::from_secs(secs)
}

impl GroupState {
    /// Whether `peer` is currently within an active rejection backoff window
    /// for this group (so it should be skipped for dialing / sync). An expired
    /// entry returns `false` so the peer gets one re-evaluation.
    pub(crate) fn is_rejected(&self, peer: &libp2p::PeerId) -> bool {
        self.rejected_peers
            .get(peer)
            .is_some_and(|r| r.until > tokio::time::Instant::now())
    }
}

struct EngineRunner {
    pub(crate) swarm: libp2p::Swarm<WaveSyncBehaviour>,
    pub(crate) peers: HashMap<libp2p::PeerId, libp2p::Multiaddr>,
    /// Sync groups keyed by effective (PSK-derived) topic.
    pub(crate) groups: HashMap<String, GroupState>,
    /// Effective topic of the build-time / single-group default group. Lets
    /// handlers without a per-message topic context reach the primary group.
    pub(crate) default_effective_topic: String,
    pub(crate) local_peer_id: libp2p::PeerId,
    pub(crate) config: EngineConfig,
    /// Mirrors `config.mdns_enabled` but is mutable at runtime via
    /// `EngineCommand::SetMdnsEnabled`. `trigger_rediscovery` and the
    /// runtime toggle handler both read this field.
    pub(crate) mdns_enabled: bool,
    pub(crate) snapshot_resp_tx: mpsc::Sender<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    pub(crate) snapshot_resp_rx: mpsc::Receiver<(
        request_response::ResponseChannel<crate::protocol::SyncResponse>,
        crate::protocol::SyncResponse,
    )>,
    /// Channel for queuing remote changesets to be applied sequentially.
    pub(crate) remote_changeset_tx: mpsc::Sender<RemoteChangeset>,
    pub(crate) remote_changeset_rx: mpsc::Receiver<RemoteChangeset>,
    /// Reconcile-digest requests built off-loop (digest is an async DB scan),
    /// drained by the event loop to send on the swarm. See `send_reconcile_digest`.
    pub(crate) reconcile_req_tx: mpsc::Sender<(libp2p::PeerId, crate::protocol::SyncRequest)>,
    pub(crate) reconcile_req_rx: mpsc::Receiver<(libp2p::PeerId, crate::protocol::SyncRequest)>,
    pub(crate) cmd_rx: mpsc::Receiver<EngineCommand>,
    /// Relay connection state machine.
    pub(crate) relay_state: RelayState,
    /// Detected NAT status from AutoNAT probes.
    pub(crate) nat_status: NatStatus,
    /// Set of bootstrap peer IDs for tracking.
    pub(crate) bootstrap_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Application-level identity announced by the local peer (ephemeral, session-scoped).
    pub(crate) local_app_id: Option<String>,
    /// Application-level identities received from remote peers (ephemeral, session-scoped).
    pub(crate) peer_identities: HashMap<libp2p::PeerId, String>,
    /// Infrastructure peers (relay, rendezvous) — excluded from peer count and sync fan-out.
    pub(crate) infrastructure_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Peers currently being dialed (not yet connected). Prevents duplicate dials.
    pub(crate) dialing_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Queue of rendezvous-discovered peers waiting to be dialed (rate-limited).
    pub(crate) pending_rendezvous_dials: VecDeque<(libp2p::PeerId, libp2p::Multiaddr)>,
    /// Push notification token to register with relay: (platform, device_token).
    pub(crate) push_token: Option<(String, String)>,
    /// Group topics whose push token has already been registered with the
    /// current relay connection. Tracked per-topic (not a single bool) so a
    /// group joined *after* the relay connect still gets a `RegisterToken`,
    /// and re-registration of an already-covered topic stays idempotent.
    /// Cleared on relay disconnect and on token rotation so every topic is
    /// re-registered against the new connection / token.
    pub(crate) push_registered_topics: std::collections::HashSet<String>,
    /// In-flight `RegisterToken` requests, keyed by their outbound request id,
    /// mapped to the topic they register. A topic moves into
    /// `push_registered_topics` only when the relay *acks* the request
    /// (`PushResponse::Ok`); an `OutboundFailure` / `Error` drops it from here
    /// without marking it registered, so the periodic reconcile retries. This
    /// is what makes registration survive a relay substream that isn't ready
    /// the instant a late-joined group fires its `RegisterToken`.
    pub(crate) push_pending_registrations:
        std::collections::HashMap<request_response::OutboundRequestId, String>,
    /// In-flight fan-out push requests, keyed by outbound request id, mapped to
    /// the `(effective_topic, db_version)` they carried (#81 Option A). A
    /// `PushAck` resolves the id to its group + changeset so that peer can be
    /// marked as having received it (`GroupState::pending_pushes`); an
    /// `OutboundFailure` just drops the id (the pending entry stays for the next
    /// redelivery tick). Correlation is local — nothing new goes on the wire.
    pub(crate) pending_push_reqs:
        std::collections::HashMap<request_response::OutboundRequestId, (String, u64)>,
    /// Shared network status snapshot, read by consumers.
    pub(crate) network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    /// Broadcast sender for network events.
    pub(crate) network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    /// Optional deadline for a post-resume sync retry (gives mDNS/rendezvous time to rediscover).
    pub(crate) resume_sync_deadline: Option<tokio::time::Instant>,
    /// API key for managed relay authentication.
    pub(crate) api_key: Option<String>,
    /// Keypair used for signing auth challenges (same identity as the swarm).
    pub(crate) keypair: identity::Keypair,
    /// Deadline after which we assume Private NAT if AutoNAT hasn't completed
    /// and the relay is connected. `None` once resolved or if no relay configured.
    pub(crate) nat_assumption_deadline: Option<tokio::time::Instant>,
    /// When the current relay circuit reservation was accepted (for proactive renewal).
    pub(crate) circuit_accepted_at: Option<tokio::time::Instant>,
    /// Number of circuit reservation retries while stuck in `Connected` state.
    pub(crate) circuit_retry_count: u32,
    /// `true` between calling `swarm.listen_on(circuit_addr)` and the relay
    /// acknowledging with `ReservationReqAccepted`. Used by
    /// [`EngineRunner::try_listen_on_circuit`] to dedup concurrent listen
    /// attempts from the four call sites (eager on first relay-connect,
    /// retry-while-stuck-in-Connected, listener-closed re-listen, and
    /// proactive 80%-of-max-duration renewal). Without this, a single
    /// relay-connect can queue ≥4 reservation requests within seconds —
    /// each one is one libp2p relay-client request — exhausting the
    /// relay's `--max-reservations-per-peer` cap and triggering a
    /// `ResourceLimitExceeded`-cascade that closes the relay connection
    /// ~15s later. See issue #21.
    pub(crate) circuit_listen_pending: bool,
    /// `true` between calling `swarm.dial(relay_addr)` and the resulting
    /// `ConnectionEstablished` / `OutgoingConnectionError` for the relay
    /// peer. Used by [`EngineRunner::try_dial_relay`] to prevent
    /// `NewListenAddr` from triggering N parallel dials to the same
    /// relay (one per local interface that comes up before the first
    /// dial completes — e.g. 3 QUIC binds, 3 redials, 3 connections,
    /// 3 reservation requests). Cleared on the connection-established or
    /// dial-failed event for the relay peer.
    pub(crate) relay_dial_pending: bool,
    /// Per-peer DCUtR retry state. Each entry is a peer whose most recent
    /// direct-connection-upgrade attempt failed; the engine retries with
    /// bounded exponential backoff so a single transient hole-punch
    /// failure (very common on cellular, where RTT jitter trips the
    /// synchronized punch) doesn't permanently strand the connection on
    /// the relay path. See [`relay_manager::DcutrRetryState`] and
    /// [`relay_manager::process_dcutr_retries`].
    pub(crate) dcutr_retries:
        HashMap<libp2p::PeerId, crate::engine::relay_manager::DcutrRetryState>,
    /// Per-peer dial backoff (anti-storm). Maps a peer to its consecutive dial
    /// failure count and the earliest instant it may be re-dialed. Consulted by
    /// the sync-peer dial paths (`dial_introduced_peer`, the `OutboundFailure`
    /// redial, mDNS re-dials) via [`EngineRunner::dial_backoff_ok`]; the next
    /// allowed instant grows per [`peer_dial_backoff`]. A successful
    /// `ConnectionEstablished` clears the entry. Infrastructure peers (relay /
    /// rendezvous) are exempt — their reconnect cadence is governed separately
    /// by `maybe_reconnect_relay`.
    pub(crate) peer_dial_backoff: HashMap<libp2p::PeerId, (u32, tokio::time::Instant)>,
    /// Peers we have discovered on the local network via mDNS (their announced
    /// address was RFC1918/link-local/ULA). For these we suppress relay-circuit
    /// dials entirely — the LAN path is closer and more reliable than a circuit
    /// (router hairpinning is often unsupported anyway). Populated on mDNS
    /// `Discovered`, cleared on mDNS `Expired` and on full disconnect. See
    /// [`EngineRunner::suppress_relay_dial`].
    pub(crate) lan_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Relay connections scheduled for demotion once a freshly-established
    /// direct path proves stable (anti-thrash dwell, #84). Maps a peer to the
    /// instant the relay connection(s) may be closed and the ids to close.
    /// Processed by [`EngineRunner::process_pending_demotions`] on the periodic
    /// tick: if the direct path is still up at the deadline the relay is closed;
    /// if it was lost in the meantime the demotion is cancelled and the relay
    /// kept — preventing a teardown before a flaky DCUtR upgrade proves it holds.
    pub(crate) pending_demotions: HashMap<
        libp2p::PeerId,
        (
            tokio::time::Instant,
            std::collections::HashSet<libp2p::swarm::ConnectionId>,
        ),
    >,
    /// Engine-wide diagnostics counters, shared with `WaveSyncDbInner`.
    /// All increments are `Relaxed` atomic ops on the hot path; readers
    /// (UI / debug panel / test assertions) snapshot via
    /// [`WaveSyncDb::diagnostics`]. See [`crate::diagnostics`].
    pub(crate) diagnostics: Arc<crate::diagnostics::Counters>,
    /// Peers that rejected our sync request-response protocol id
    /// (`OutboundFailure::UnsupportedProtocols`) — i.e. they are connected at
    /// the transport level but run an incompatible WaveSyncDB version. Tracked
    /// so the mismatch is logged/emitted once per peer instead of on every
    /// periodic sync retry; cleared when the peer fully disconnects so a later
    /// reconnect can re-surface it.
    pub(crate) protocol_mismatch_peers: std::collections::HashSet<libp2p::PeerId>,
    /// Per-peer current path classification for relay-cost telemetry: `true` if
    /// the best-known connection to the peer is carried by the relay (circuit),
    /// `false` if direct. Updated on `ConnectionEstablished` — a DCUtR upgrade
    /// arrives as a fresh direct connection that flips this to `false`; cleared
    /// on full disconnect. Surfaced as `PeerInfo.via_relay`.
    pub(crate) peer_via_relay: std::collections::HashMap<libp2p::PeerId, bool>,
    /// Relay-carried (circuit) connection ids per peer (#84 DERP demotion). When
    /// a direct connection to the same peer comes up (typically a DCUtR
    /// hole-punch), these are closed so steady-state data leaves the paid relay
    /// — the relay reverts to wake/fallback only. Entries are pruned as their
    /// connections close.
    pub(crate) relayed_conn_ids: std::collections::HashMap<
        libp2p::PeerId,
        std::collections::HashSet<libp2p::swarm::ConnectionId>,
    >,
    /// Peers that have answered a reconcile message (#82) — i.e. they speak the
    /// digest/bucket protocol. For these, the periodic version-vector catch-up
    /// is skipped (the digest exchange handles convergence + the bucket exchange
    /// handles the diff), so the relay carries no redundant whole-range resend.
    /// Peers NOT in this set (older builds, web) keep getting the version-vector
    /// catch-up as the fallback. Cleared on disconnect.
    pub(crate) reconcile_capable: std::collections::HashSet<libp2p::PeerId>,
    /// iOS only: QUIC listeners keyed by the concrete interface IP they are
    /// bound to. iOS binds QUIC to concrete routable addresses (not loopback,
    /// not unspecified — see the listen logic in `run`), and a mobile device's
    /// active interface changes (Wi-Fi↔cellular handoff, DHCP renew, NAT
    /// remap). The interface-watch tick diffs the current routable set against
    /// these keys to add listeners for new interfaces and `remove_listener`
    /// departed ones, keeping the node dialable across network changes.
    #[cfg(target_os = "ios")]
    pub(crate) quic_listeners:
        std::collections::HashMap<std::net::IpAddr, libp2p::core::transport::ListenerId>,
}

impl EngineRunner {
    /// The primary (build-time / single-group) group. Used by handlers that do
    /// not yet have a per-message topic context. Safe for the engine lifetime —
    /// the default group is always present.
    #[allow(dead_code)]
    pub(crate) fn default_group(&self) -> &GroupState {
        self.groups
            .get(&self.default_effective_topic)
            .expect("default group always present")
    }

    #[allow(dead_code)]
    pub(crate) fn default_group_mut(&mut self) -> &mut GroupState {
        let topic = self.default_effective_topic.clone();
        self.groups
            .get_mut(&topic)
            .expect("default group always present")
    }

    /// iOS: reconcile QUIC listeners against the current set of routable
    /// interface addresses. Adds a listener for each newly-appeared interface
    /// and removes the listener for each departed one. Called from the
    /// interface-watch tick so a concrete-interface bind survives a network
    /// change (Wi-Fi↔cellular handoff, DHCP renew, NAT remap). Adding a
    /// listener fires `NewListenAddr`, which the existing handler uses to
    /// redial the relay if it had dropped to `Connecting`.
    #[cfg(target_os = "ios")]
    fn reconcile_quic_listeners(&mut self) {
        use std::collections::HashSet;
        let current: HashSet<std::net::IpAddr> =
            routable_listen_ips(self.config.ipv6).into_iter().collect();
        let known: HashSet<std::net::IpAddr> = self.quic_listeners.keys().copied().collect();

        // Drop listeners for interfaces that have gone away.
        for ip in known.difference(&current).copied().collect::<Vec<_>>() {
            if let Some(id) = self.quic_listeners.remove(&ip) {
                log::info!("Network interface {ip} departed; removing QUIC listener {id:?}");
                self.swarm.remove_listener(id);
            }
        }
        // Bind listeners for interfaces that have just appeared.
        for ip in current.difference(&known).copied().collect::<Vec<_>>() {
            let addr = quic_listen_multiaddr(ip);
            log::info!("Network interface {ip} appeared; binding QUIC listener {addr}");
            match self.swarm.listen_on(addr.clone()) {
                Ok(id) => {
                    self.quic_listeners.insert(ip, id);
                }
                Err(e) => log::warn!("QUIC listen on {addr} failed (non-fatal): {e}"),
            }
        }
    }

    /// No-op on non-iOS targets: they bind QUIC to unspecified addresses and
    /// let libp2p enumerate and track interfaces itself.
    #[cfg(not(target_os = "ios"))]
    fn reconcile_quic_listeners(&mut self) {}

    /// Rebuild the full network status snapshot from internal state.
    fn update_network_status(&self) {
        use crate::network_status as ns;

        // Aggregate per-peer view across all groups (max known version; member
        // of the node if verified in any group). For a single-group node this
        // is identical to the pre-multi-group behaviour.
        let connected_peers = self
            .peers
            .iter()
            .filter(|(peer_id, _)| !self.infrastructure_peers.contains(peer_id))
            .map(|(peer_id, addr)| {
                let db_version = self
                    .groups
                    .values()
                    .filter_map(|g| {
                        g.peer_db_versions
                            .get(peer_id)
                            .copied()
                            .or_else(|| g.peer_reported_versions.get(peer_id).copied())
                    })
                    .max();
                let is_group_member = self
                    .groups
                    .values()
                    .any(|g| g.verified_peers.contains(peer_id));
                ns::PeerInfo {
                    peer_id: ns::PeerId(peer_id.to_string()),
                    address: addr.to_string(),
                    db_version,
                    is_bootstrap: self.bootstrap_peers.contains(peer_id),
                    is_group_member,
                    app_id: self.peer_identities.get(peer_id).cloned(),
                    via_relay: self.peer_via_relay.get(peer_id).copied().unwrap_or(false),
                }
            })
            .collect();

        let relay_status = match &self.relay_state {
            RelayState::Disabled => ns::RelayStatus::Disabled,
            RelayState::Connecting { .. } => ns::RelayStatus::Connecting,
            RelayState::Connected { .. } => ns::RelayStatus::Connected,
            RelayState::Listening { .. } => ns::RelayStatus::Listening,
        };

        let nat_status = self.nat_status.clone();

        let status = ns::NetworkStatus {
            local_peer_id: ns::PeerId(self.local_peer_id.to_string()),
            connected_peers,
            topic: self.default_group().topic_name.clone(),
            relay_status,
            nat_status,
            rendezvous_registered: self.default_group().rendezvous_registered,
            push_registered: !self.push_registered_topics.is_empty(),
            local_db_version: self.default_group().local_db_version,
            registry_ready: self.default_group().registry_is_ready,
        };

        *self.network_status.write().unwrap() = status;
    }

    /// Emit a network event on the broadcast channel, ignoring no-receiver errors.
    fn emit_network_event(&self, event: crate::network_status::NetworkEvent) {
        let _ = self.network_event_tx.send(event);
    }

    /// Idempotently issue `swarm.listen_on(<relay>/p2p-circuit)` against the
    /// configured relay. Skips the call when a previous request is still in
    /// flight (`circuit_listen_pending`) or when we're already listening on a
    /// circuit and `force_renewal` is false.
    ///
    /// `force_renewal = true` is the explicit 80%-of-`circuit_max_duration`
    /// proactive renewal path — it bypasses the "already listening" short-
    /// circuit so libp2p re-issues a reservation request to keep the entry
    /// fresh on the relay.
    ///
    /// Without this dedup, every site that wants the engine to listen on a
    /// circuit (eager on first relay-connect, retry-stuck-in-Connected,
    /// listener-closed re-listen, proactive renewal) issues its own
    /// `swarm.listen_on` and each one becomes a fresh reservation request to
    /// the relay. That collides with libp2p's own internal renewal logic and
    /// blows past the relay's `--max-reservations-per-peer` cap (stock 4)
    /// within seconds; the resulting `ResourceLimitExceeded` cascade closes
    /// the relay connection ~15s after pairing. See issue #21.
    fn try_listen_on_circuit(&mut self, force_renewal: bool) {
        let Some(ref relay_addr) = self.config.relay_server else {
            return;
        };
        if self.circuit_listen_pending {
            log::debug!("Skipping listen_on(circuit): a reservation request is already in flight");
            return;
        }
        if !force_renewal && matches!(self.relay_state, RelayState::Listening { .. }) {
            log::debug!(
                "Skipping listen_on(circuit): already in Listening state (set force_renewal to refresh the reservation)"
            );
            return;
        }
        let circuit_addr = relay_addr
            .clone()
            .with(libp2p::multiaddr::Protocol::P2pCircuit);
        match self.swarm.listen_on(circuit_addr.clone()) {
            Ok(_) => {
                self.circuit_listen_pending = true;
                self.diagnostics
                    .circuit_reservation_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                log::info!(
                    "Listening on relay circuit ({}): {circuit_addr}",
                    if force_renewal { "renewal" } else { "initial" }
                );
            }
            Err(e) => {
                log::warn!("listen_on(circuit) failed: {e}");
            }
        }
    }

    /// Idempotently dial the configured relay server. Skips the call when a
    /// previous dial is in flight (`relay_dial_pending`), when we already
    /// have a connection (`Connected` / `Listening`), or when no relay is
    /// configured.
    ///
    /// `NewListenAddr` fires once per local interface that comes up — on a
    /// QUIC-only Android device that's typically ≥3 events (LAN, cellular,
    /// loopback). Each one used to call `swarm.dial(relay_addr)` directly,
    /// producing parallel connections to the same relay peer that
    /// `max_established_per_peer = 1` then thrashes through. See issue #21.
    fn try_dial_relay(&mut self) {
        let Some(ref relay_addr) = self.config.relay_server.clone() else {
            return;
        };
        if matches!(
            self.relay_state,
            RelayState::Connected { .. } | RelayState::Listening { .. }
        ) {
            log::debug!("Skipping relay dial: already connected/listening");
            return;
        }
        if self.relay_dial_pending {
            log::debug!("Skipping relay dial: a previous dial is still in flight");
            return;
        }
        match self.swarm.dial(relay_addr.clone()) {
            Ok(_) => {
                self.relay_dial_pending = true;
                log::info!("Dialing relay server: {relay_addr}");
            }
            Err(e) => {
                log::warn!("Relay dial failed: {e}");
            }
        }
    }

    /// Handle a new connection: relay handshake, rendezvous discovery,
    /// bootstrap peer tracking, or regular peer sync initiation.
    async fn handle_connection_established(
        &mut self,
        peer_id: libp2p::PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
    ) {
        // If this is the relay server, transition state
        if let Some(ref relay_addr) = self.config.relay_server
            && let Some(libp2p::multiaddr::Protocol::P2p(relay_peer_id)) = relay_addr.iter().last()
            && peer_id == relay_peer_id
        {
            // Dial concluded — clear the in-flight guard so future
            // reconnect paths (after a disconnect) can re-arm.
            self.relay_dial_pending = false;
            self.handle_relay_peer_connected(peer_id, relay_addr.clone());
        }

        // If this is a rendezvous server, discover peers
        if let Some(ref rendezvous_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                rendezvous_addr.iter().last()
            && peer_id == rv_peer_id
        {
            log::info!("Connected to rendezvous server {peer_id}");
            self.infrastructure_peers.insert(peer_id);
            self.rendezvous_discover();
        }

        // If this is a bootstrap peer, add to peers and initiate sync
        if self.bootstrap_peers.contains(&peer_id) {
            self.handle_bootstrap_peer_connected(peer_id, endpoint);
        }

        if self.default_group().registry_is_ready
            && !self.default_group().is_rejected(&peer_id)
            && !self.infrastructure_peers.contains(&peer_id)
        {
            // Ensure reconnecting peer is tracked for future periodic syncs
            self.peers
                .entry(peer_id)
                .or_insert_with(|| endpoint.get_remote_address().clone());
            let effective_topic = self.default_effective_topic.clone();
            if let Some(g) = self.groups.get_mut(&effective_topic) {
                g.local_db_version = g
                    .db_version_cache
                    .load(std::sync::atomic::Ordering::Acquire);
            }
            self.initiate_sync_for_peer(peer_id, &effective_topic);
        }

        self.dialing_peers.remove(&peer_id);
        self.drain_pending_rendezvous_dials();
    }

    /// Set up relay circuit, external address, rendezvous registration, NAT timer.
    ///
    /// libp2p sometimes opens additional connections to the same relay
    /// peer-id after the first one — for example, the relay's AutoNAT v2
    /// dial-back can hit our QUIC listener and produce a fresh
    /// `ConnectionEstablished` event for the relay peer-id, even though
    /// we already have a healthy connection and an active reservation.
    /// This handler is the relay's "connected" entry point, so without an
    /// idempotency guard each extra connection re-runs the full setup
    /// (state reset to `Connected`, `try_listen_on_circuit`, external
    /// address registration, rendezvous registration, push-token
    /// re-register, presence re-announce) — and the listen_on triggers
    /// libp2p's relay-client to issue a fresh reservation request that
    /// the relay logs as another `ReservationReqAccepted` followed by a
    /// burst of internal renewals on the new connection. See issue #21.
    ///
    /// Skip the setup when we already have a `Connected` / `Listening`
    /// state for this same relay peer-id — the new connection is just an
    /// extra channel, not a fresh attach.
    fn handle_relay_peer_connected(
        &mut self,
        peer_id: libp2p::PeerId,
        relay_addr: libp2p::Multiaddr,
    ) {
        let already_attached = match &self.relay_state {
            RelayState::Connected {
                relay_peer_id: existing,
                ..
            }
            | RelayState::Listening {
                relay_peer_id: existing,
            } => *existing == peer_id,
            _ => false,
        };
        if already_attached {
            log::debug!(
                "ConnectionEstablished for relay {peer_id} ignored — state={:?} already attached, keeping existing reservation",
                self.relay_state
            );
            return;
        }
        log::info!("Connected to relay server {peer_id}");
        self.infrastructure_peers.insert(peer_id);
        self.circuit_retry_count = 0;
        self.relay_state = RelayState::Connected {
            relay_peer_id: peer_id,
            connected_at: tokio::time::Instant::now(),
        };
        self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
            crate::network_status::RelayStatus::Connected,
        ));
        self.update_network_status();

        // Eagerly request a circuit-relay reservation. Idempotent so
        // subsequent re-connects of the same relay (which fire this
        // handler again per `max_established_per_peer = 1` selection) don't
        // pile additional reservation requests onto an already-pending one.
        self.try_listen_on_circuit(false);

        // Manually add circuit address as external
        let my_circuit_addr = relay_addr
            .clone()
            .with(libp2p::multiaddr::Protocol::P2pCircuit)
            .with(libp2p::multiaddr::Protocol::P2p(self.local_peer_id));
        self.swarm.add_external_address(my_circuit_addr.clone());
        log::info!("Added circuit address as external: {my_circuit_addr}");

        // Register with rendezvous immediately
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && self.swarm.is_connected(&rv_peer_id)
        {
            self.rendezvous_register(rv_peer_id);
        }

        // Start NAT assumption timer
        if self.nat_status == NatStatus::Unknown && self.nat_assumption_deadline.is_none() {
            self.nat_assumption_deadline =
                Some(tokio::time::Instant::now() + Duration::from_secs(30));
        }

        // Register push token with relay if configured
        self.maybe_register_push_token(peer_id);
        // Announce presence so the relay can introduce us to other peers
        // on the same topic (works for desktop too, no push token needed).
        self.announce_presence_to_relay(peer_id);
    }

    /// Track bootstrap peer, emit event, update last_seen.
    fn handle_bootstrap_peer_connected(
        &mut self,
        peer_id: libp2p::PeerId,
        endpoint: &libp2p::core::ConnectedPoint,
    ) {
        let addr = endpoint.get_remote_address().clone();
        self.peers.insert(peer_id, addr.clone());

        self.emit_network_event(crate::network_status::NetworkEvent::PeerConnected(
            crate::network_status::PeerInfo {
                peer_id: crate::network_status::PeerId(peer_id.to_string()),
                address: addr.to_string(),
                db_version: None,
                is_bootstrap: true,
                is_group_member: false,
                app_id: None,
                via_relay: self.peer_via_relay.get(&peer_id).copied().unwrap_or(false),
            },
        ));
        self.update_network_status();

        let db = self.default_group().db.clone();
        let peer_str = peer_id.to_string();
        tokio::spawn(async move {
            let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
        });
    }

    /// Whether we currently prefer a direct path to `peer` — i.e. a direct
    /// connection has been established and not fully torn down. While true, the
    /// relay/circuit path to this peer is suppressed (see
    /// [`dialable_addrs_preferring_direct`] and the `OutboundFailure` redial
    /// guard) to prevent the demote→re-dial circuit storm. The marker survives
    /// the brief `is_connected` flicker after a demotion close because
    /// `peer_via_relay` is only cleared on a *full* disconnect, not on the close
    /// of one redundant connection.
    pub(crate) fn prefers_direct(&self, peer: &libp2p::PeerId) -> bool {
        self.peer_via_relay.get(peer) == Some(&false)
    }

    /// Whether to suppress relay-circuit dials to `peer`: either a direct
    /// connection is already up (`prefers_direct`) or the peer was discovered on
    /// the local network via mDNS (`lan_peers`) and should be reached over the
    /// LAN. The mDNS case proactively avoids opening a circuit during the brief
    /// window between LAN discovery and the direct connection establishing.
    pub(crate) fn suppress_relay_dial(&self, peer: &libp2p::PeerId) -> bool {
        self.prefers_direct(peer) || self.lan_peers.contains(peer)
    }

    /// Deterministic single-closer rule for relay demotion: only the peer with
    /// the numerically smaller PeerId actively closes a redundant relay
    /// connection. A relay circuit is one shared connection, so a single close
    /// tears it down for both ends; the other end prunes its tracking on the
    /// resulting `ConnectionClosed`. Picking one closer (rather than both
    /// closing in lockstep) avoids a close race and keeps the demotion decision
    /// symmetric and predictable across the two peers. Both ends compute the
    /// same comparison over the same pair of ids, so exactly one closes.
    fn should_demote_locally(&self, peer: &libp2p::PeerId) -> bool {
        self.local_peer_id.to_bytes() < peer.to_bytes()
    }

    /// Whether `peer` may be dialed now, or is still inside its backoff window
    /// after recent consecutive dial failures. Infrastructure peers are always
    /// allowed (their reconnect cadence is handled by `maybe_reconnect_relay`).
    /// See [`peer_dial_backoff`].
    pub(crate) fn dial_backoff_ok(&self, peer: &libp2p::PeerId) -> bool {
        if self.infrastructure_peers.contains(peer) {
            return true;
        }
        match self.peer_dial_backoff.get(peer) {
            Some((_, next_allowed)) => tokio::time::Instant::now() >= *next_allowed,
            None => true,
        }
    }

    /// Record a failed dial to `peer`, growing its backoff window. No-op for
    /// infrastructure peers.
    pub(crate) fn record_dial_failure(&mut self, peer: libp2p::PeerId) {
        if self.infrastructure_peers.contains(&peer) {
            return;
        }
        let entry = self
            .peer_dial_backoff
            .entry(peer)
            .or_insert((0, tokio::time::Instant::now()));
        entry.0 = entry.0.saturating_add(1);
        entry.1 = tokio::time::Instant::now() + peer_dial_backoff(entry.0);
    }

    /// Clear any dial backoff for `peer` — called on a successful connection so
    /// a recovered peer is never penalised by stale failure history.
    pub(crate) fn clear_dial_backoff(&mut self, peer: &libp2p::PeerId) {
        self.peer_dial_backoff.remove(peer);
    }

    /// Close relay connections whose anti-thrash demotion dwell has elapsed,
    /// provided the direct path that triggered the demotion is still up. If the
    /// direct path was lost during the dwell, cancel the demotion and keep the
    /// relay connection as the active path (restoring it to `relayed_conn_ids`
    /// so a later direct path can demote it again). Called from the periodic
    /// tick. See [`DEMOTION_DWELL`].
    fn process_pending_demotions(&mut self) {
        let now = tokio::time::Instant::now();
        let due: Vec<libp2p::PeerId> = self
            .pending_demotions
            .iter()
            .filter(|(_, (deadline, _))| *deadline <= now)
            .map(|(p, _)| *p)
            .collect();
        for peer in due {
            let Some((_, ids)) = self.pending_demotions.remove(&peer) else {
                continue;
            };
            if self.prefers_direct(&peer) && self.swarm.is_connected(&peer) {
                let mut closed = 0u64;
                for cid in &ids {
                    if self.swarm.close_connection(*cid) {
                        closed += 1;
                    }
                }
                if closed > 0 {
                    self.diagnostics
                        .relay_connections_demoted
                        .fetch_add(closed, std::sync::atomic::Ordering::Relaxed);
                    log::info!(
                        "Relay demotion: closed {closed} relay connection(s) to {peer} after dwell (direct path held)"
                    );
                }
            } else {
                // Direct path didn't hold — keep the relay as the active path and
                // restore tracking so a future direct path can demote it again.
                log::info!(
                    "Relay demotion cancelled for {peer}: direct path did not hold during dwell; keeping relay"
                );
                self.relayed_conn_ids.entry(peer).or_default().extend(ids);
            }
        }
    }

    /// Handle peer disconnection: clean up tracking, reconnect relay/rendezvous.
    fn handle_connection_closed(&mut self, peer_id: libp2p::PeerId, num_established: u32) {
        for g in self.groups.values_mut() {
            g.pending_sync_peers.remove(&peer_id);
            g.verified_peers.remove(&peer_id);
        }
        self.peer_identities.remove(&peer_id);

        if num_established > 0 {
            return;
        }

        // No more connections — drop any pending DCUtR retry for this
        // peer. Re-attempts only make sense while the relay-circuit
        // connection is still alive (DCUtR coordinates through it).
        self.dcutr_retries.remove(&peer_id);

        // Clear the path classification on a *full* disconnect so a later
        // reconnect can re-establish via whatever path wins (including the
        // relay). While ANY connection to the peer remains (num_established>0,
        // handled by the early return above) we deliberately keep
        // `peer_via_relay==false` as the "direct preferred, suppress circuit
        // re-dials" marker — see `prefers_direct` / `dialable_addrs_preferring_direct`.
        self.peer_via_relay.remove(&peer_id);
        self.relayed_conn_ids.remove(&peer_id);
        self.lan_peers.remove(&peer_id);
        self.pending_demotions.remove(&peer_id);

        // Handle relay server disconnect
        if let RelayState::Connected { relay_peer_id, .. } | RelayState::Listening { relay_peer_id } =
            &self.relay_state
            && peer_id == *relay_peer_id
        {
            self.handle_relay_peer_disconnected(peer_id);
        }

        // If rendezvous server disconnected (and is different from relay)
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && peer_id == rv_peer_id
            && !matches!(&self.relay_state, RelayState::Connecting { .. })
            && self.default_group().rendezvous_registered
        {
            log::warn!("Lost connection to rendezvous server {peer_id}");
            self.default_group_mut().rendezvous_registered = false;
            self.emit_network_event(
                crate::network_status::NetworkEvent::RendezvousStatusChanged { registered: false },
            );
            self.update_network_status();
        }

        // Trigger rendezvous discover for disconnected sync peers
        if !self.infrastructure_peers.contains(&peer_id)
            && self
                .groups
                .values()
                .any(|g| g.peer_db_versions.contains_key(&peer_id))
        {
            log::info!(
                "Peer {peer_id} disconnected with sync history, triggering rendezvous discover"
            );
            self.rendezvous_discover();
        }
    }

    /// Reset relay state, push registration, and attempt reconnection.
    fn handle_relay_peer_disconnected(&mut self, peer_id: libp2p::PeerId) {
        log::warn!("Lost connection to relay server {peer_id}");
        self.relay_state = RelayState::Connecting { retry_count: 0 };
        self.circuit_accepted_at = None;
        self.push_registered_topics.clear();
        // In-flight registrations die with the connection; drop them so the
        // post-reconnect sweep re-sends rather than waiting on acks that will
        // never arrive on the old substream.
        self.push_pending_registrations.clear();
        // The reservation died with the connection; clear both idempotency
        // flags so the immediate reconnect path below can re-arm.
        self.circuit_listen_pending = false;
        self.relay_dial_pending = false;
        self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
            crate::network_status::RelayStatus::Connecting,
        ));
        // If relay also serves rendezvous, reset that too
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && peer_id == rv_peer_id
        {
            log::warn!("Rendezvous server also disconnected (same as relay)");
            self.default_group_mut().rendezvous_registered = false;
            self.emit_network_event(
                crate::network_status::NetworkEvent::RendezvousStatusChanged { registered: false },
            );
        }
        self.update_network_status();

        // Attempt immediate reconnection (idempotent — won't pile dials on
        // top of one another).
        log::info!("Attempting immediate relay reconnection");
        self.try_dial_relay();
    }

    async fn run(
        &mut self,
        sync_rx: &mut mpsc::Receiver<TaggedChangeset>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        log::info!(
            "DIAG run() entered: relay_server={:?} rendezvous_server={:?} ipv6={} os={}",
            self.config.relay_server,
            self.config.rendezvous_server,
            self.config.ipv6,
            std::env::consts::OS
        );

        // QUIC-only listeners. See `build_swarm` for the rationale.
        //
        // iOS: bind to the device's *concrete* active-interface address(es),
        // not loopback and not unspecified. libp2p-quic 0.13 dials with
        // `PortUse::Reuse` by sourcing outbound dials from an eligible listener
        // socket; its listener selection does not exclude a loopback listener
        // for a public destination, so a loopback-bound listener makes every
        // relay/rendezvous/direct dial leave from 127.0.0.1 and fail with
        // EADDRNOTAVAIL (errno 49) — total WAN-sync failure (#72). A concrete
        // routable bind sources dials from the real interface. Concrete (non-
        // unspecified) binds also sidestep the unspecified-bind code path that
        // the previous loopback workaround was guarding against. The
        // interface-watch tick (below) re-binds when the active interface
        // changes (Wi-Fi↔cellular handoff, DHCP renew).
        #[cfg(target_os = "ios")]
        {
            let ips = routable_listen_ips(self.config.ipv6);
            if ips.is_empty() {
                log::warn!(
                    "No routable network interface at startup; QUIC listen deferred \
                     to the interface watch (will bind once an interface appears)"
                );
            }
            for ip in ips {
                let addr = quic_listen_multiaddr(ip);
                log::info!("DIAG calling listen_on(QUIC)={addr}");
                let t1 = std::time::Instant::now();
                match self.swarm.listen_on(addr.clone()) {
                    Ok(id) => {
                        self.quic_listeners.insert(ip, id);
                        log::info!("DIAG listen_on(QUIC) returned in {:?}", t1.elapsed());
                    }
                    Err(e) => log::warn!("QUIC listen on {addr} failed (non-fatal): {e}"),
                }
            }
        }
        #[cfg(not(target_os = "ios"))]
        {
            let (v4_quic, v6_quic) = ("/ip4/0.0.0.0/udp/0/quic-v1", "/ip6/::/udp/0/quic-v1");

            log::info!("DIAG calling listen_on(QUIC)={v4_quic}");
            let t1 = std::time::Instant::now();
            self.swarm.listen_on(v4_quic.parse().unwrap())?;
            log::info!("DIAG listen_on(QUIC) returned in {:?}", t1.elapsed());

            if self.config.ipv6
                && let Err(e) = self.swarm.listen_on(v6_quic.parse().unwrap())
            {
                log::warn!("QUIC IPv6 listen failed (non-fatal): {e}");
            }
        }

        // If a relay server is configured, dial it. Set state to Connecting
        // first so `try_dial_relay`'s "skip if already Connected/Listening"
        // guard lets the initial dial through but blocks NewListenAddr-driven
        // redials that would otherwise race with this one.
        if self.config.relay_server.is_some() {
            self.relay_state = RelayState::Connecting { retry_count: 0 };
            self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                crate::network_status::RelayStatus::Connecting,
            ));
            self.update_network_status();
            self.try_dial_relay();
        }

        // If a rendezvous server is configured, dial it (unless already dialing as relay)
        if let Some(ref rendezvous_addr) = self.config.rendezvous_server.clone() {
            let already_dialing = self.config.relay_server.as_ref().is_some_and(|relay_addr| {
                let relay_peer = relay_addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                    _ => None,
                });
                let rv_peer = rendezvous_addr.iter().find_map(|p| match p {
                    libp2p::multiaddr::Protocol::P2p(id) => Some(id),
                    _ => None,
                });
                relay_peer.is_some() && relay_peer == rv_peer
            });

            if already_dialing {
                log::info!("Rendezvous server is same peer as relay — skipping duplicate dial");
            } else if let Err(e) = self.swarm.dial(rendezvous_addr.clone()) {
                log::warn!(
                    "Failed to dial rendezvous server {}: {}",
                    rendezvous_addr,
                    e
                );
            } else {
                log::info!("Dialing rendezvous server: {}", rendezvous_addr);
            }
        }

        // Dial bootstrap peers, grouping by peer-id so that multiple
        // multiaddrs targeting the same peer (a common case when the
        // FCM payload supplies several candidate addresses for one
        // remote peer) race in a single libp2p dial via
        // `DialOpts::peer_id(p).addresses(addrs)`. Without grouping,
        // each address became a separate `swarm.dial` that libp2p
        // deduplicates against `PeerCondition::Disconnected` once one
        // connects — wasting handshake budget on the others.
        // Multiaddrs without a `/p2p/` suffix fall back to the
        // single-address dial path.
        let (grouped, suffixless) = group_bootstrap_addrs(self.config.bootstrap_peers.clone());
        for peer_id in grouped.keys() {
            self.bootstrap_peers.insert(*peer_id);
        }
        for (peer_id, addrs) in grouped {
            log::info!(
                "Dialing bootstrap peer {peer_id} with {} address(es): {addrs:?}",
                addrs.len()
            );
            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                .addresses(addrs)
                .build();
            if let Err(e) = self.swarm.dial(dial_opts) {
                log::warn!("Failed to dial bootstrap peer {peer_id}: {e}");
            }
        }
        for addr in suffixless {
            log::info!("Dialing bootstrap peer: {addr}");
            if let Err(e) = self.swarm.dial(addr.clone()) {
                log::warn!("Failed to dial bootstrap peer {addr}: {e}");
            }
        }

        // Cold-start: dial the addresses we successfully connected to last
        // run (#29). These dials race in parallel with mDNS / rendezvous
        // / relay-PeerList discovery; whichever produces a connection
        // first wins. On a warm cache this typically shaves the discovery
        // round-trip off the first sync.
        self.predial_cached_addrs().await;

        let mut sync_interval = tokio::time::interval(self.config.sync_interval);
        let mut rendezvous_interval =
            tokio::time::interval(self.config.rendezvous_discover_interval);
        let has_rendezvous = self.config.rendezvous_server.is_some();
        let registry_deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(registry_deadline);

        // Relay reconnection timer (only active when relay is configured but disconnected)
        // Use interval_at to avoid immediate first tick causing a duplicate dial
        let mut relay_reconnect = tokio::time::interval_at(
            tokio::time::Instant::now() + Duration::from_secs(5),
            Duration::from_secs(5),
        );
        let has_relay = self.config.relay_server.is_some();

        // iOS: poll the active network interfaces and re-bind QUIC listeners
        // when they change. A concrete-interface bind (see the listen logic
        // above) goes stale silently on a Wi-Fi↔cellular handoff or NAT remap,
        // so without this the node stops being dialable mid-session. Polling
        // (vs an NWPathMonitor callback) keeps the swarm mutation on the event
        // loop and adds no platform FFI; 3s is well inside human-perceptible
        // handoff recovery. Disabled on every other platform (those bind to
        // unspecified and let libp2p track interfaces itself).
        let watch_interfaces = cfg!(target_os = "ios");
        let mut interface_watch = tokio::time::interval(Duration::from_secs(3));

        // Fast-path redelivery of un-acked local pushes (#81 Option A). Short
        // cadence (well under `sync_interval`) so a dropped real-time push to a
        // still-connected peer is retried in seconds; a no-op whenever there is
        // nothing un-acked (the steady state).
        let mut redeliver_interval = tokio::time::interval(Duration::from_secs(3));

        // The registry-ready notifier is awaited inside the select! arm, which
        // cannot hold a borrow of `self` across the await. Clone the default
        // group's `Arc<Notify>` out here so the arm only touches the local.
        let registry_ready = self.default_group().registry_ready.clone();

        loop {
            tokio::select! {
                Some(tc) = sync_rx.recv() => {
                    self.handle_local_changeset(tc).await;
                },
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                },
                _ = sync_interval.tick() => {
                    // Periodic version vector sync with all known peers
                    if self.default_group().registry_is_ready {
                        self.sync_all_known_peers().await;
                        // Convergence verification (#82): alongside the
                        // version-vector catch-up above (the data path /
                        // backstop), exchange a value-inclusive digest with each
                        // peer to *prove* convergence. Additive — a peer that
                        // can't decode the digest fails it cleanly and the
                        // catch-up still does the work.
                        self.send_reconcile_digests();
                    }
                },
                _ = redeliver_interval.tick() => {
                    self.redeliver_pending_pushes();
                },
                _ = rendezvous_interval.tick(), if has_rendezvous => {
                    self.rendezvous_discover();
                },
                _ = interface_watch.tick(), if watch_interfaces => {
                    self.reconcile_quic_listeners();
                },
                _ = relay_reconnect.tick(), if has_relay => {
                    self.maybe_reconnect_relay();
                    // Same tick handles DCUtR retries. Both are
                    // infrastructure-paths book-keeping; aligning them on
                    // one 5s timer keeps the wakeup count low (saves a
                    // bit of mobile battery vs. running two timers).
                    self.process_dcutr_retries();
                    // Close relay connections whose demotion dwell has elapsed
                    // (and the direct path held). Relay-only book-keeping, so it
                    // belongs on the same has_relay tick.
                    self.process_pending_demotions();
                    // Reconcile push-token registration against the current
                    // group set. Registration is otherwise only attempted at
                    // discrete moments (relay-connect, token set, join-while-
                    // connected); if any of those raced the relay state or the
                    // token wasn't available yet, a group stays unregistered
                    // and its silent wake-pushes never fire. This is a no-op
                    // for topics already registered on the current connection.
                    if let RelayState::Connected { relay_peer_id, .. }
                    | RelayState::Listening { relay_peer_id } = self.relay_state
                    {
                        self.maybe_register_push_token(relay_peer_id);
                    }
                },
                Some((channel, response)) = self.snapshot_resp_rx.recv() => {
                    if let Err(resp) = self.swarm.behaviour_mut().snapshot.send_response(channel, response) {
                        log::error!("Failed to send sync response: {:?}", resp);
                    }
                },
                Some((peer, req)) = self.reconcile_req_rx.recv() => {
                    // Off-loop-built reconcile digest (#82) — send it on the swarm.
                    self.swarm.behaviour_mut().snapshot.send_request(&peer, req);
                },
                Some(rc) = self.remote_changeset_rx.recv() => {
                    // Route the changeset to the group it was received for.
                    // Borrow-split: pull the group's handles into locals before
                    // the await so the group borrow ends (the swarm is not held
                    // here, but we must not keep a `groups` borrow across it).
                    let Some(g) = self.groups.get(&rc.effective_topic) else {
                        log::debug!("remote changeset for unknown group {}; dropping", rc.effective_topic);
                        continue;
                    };
                    let db = g.db.clone();
                    let change_tx = g.change_tx.clone();
                    let registry = g.registry.clone();
                    let cache = g.db_version_cache.clone();
                    let notif_registry = g.notification_registry.clone();
                    let notif_tx = g.notification_tx.clone();

                    let change_source =
                        crate::messages::ChangeSource::Remote { peer_site: rc.peer_site };
                    let notify_ctx = sync_handler::NotifyCtx {
                        registry: &notif_registry,
                        tx: &notif_tx,
                    };
                    apply_remote_changeset(&db, &change_tx, &registry, &rc.changes, Some(&cache), change_source, Some(notify_ctx)).await;
                    // Record our knowledge of the sender's db_version only now,
                    // after the changes are durably committed — never at receive
                    // time. This guarantees the persisted peer version is always
                    // <= what we have actually applied, so a restart can hydrate
                    // it without risking a silently-skipped change range.
                    if let Some(version) = rc.peer_db_version {
                        if let Some(g) = self.groups.get_mut(&rc.effective_topic) {
                            g.peer_db_versions.insert(rc.peer, version);
                        }
                        let peer_str = rc.peer.to_string();
                        if let Err(e) =
                            peer_tracker::upsert_peer_version(&db, &peer_str, &rc.peer_site, version).await
                        {
                            log::warn!("Failed to persist peer version for {peer_str}: {e}");
                        }
                    }
                },
                _ = registry_ready.notified(), if !self.default_group().registry_is_ready => {
                    self.default_group_mut().registry_is_ready = true;
                    self.update_network_status();
                    log::info!("Registry ready, syncing all known peers");
                    self.sync_all_known_peers().await;
                },
                _ = &mut registry_deadline, if !self.default_group().registry_is_ready => {
                    log::error!("Schema registry not ready after 30s — proceeding without sync tables");
                    self.default_group_mut().registry_is_ready = true;
                    self.update_network_status();
                },
                _ = async {
                    match self.resume_sync_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if self.resume_sync_deadline.is_some() => {
                    log::info!("Post-resume sync retry");
                    self.resume_sync_deadline = None;
                    for g in self.groups.values_mut() {
                        g.pending_sync_peers.clear();
                    }
                    if self.default_group().registry_is_ready {
                        self.sync_all_known_peers().await;
                    }
                },
                _ = async {
                    match self.nat_assumption_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                }, if self.nat_assumption_deadline.is_some() => {
                    self.nat_assumption_deadline = None;
                    if self.nat_status == NatStatus::Unknown
                        && matches!(self.relay_state, RelayState::Connected { .. } | RelayState::Listening { .. })
                    {
                        log::info!("AutoNAT timeout — relay connected, assuming Private NAT");
                        self.nat_status = NatStatus::Private;
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::NatStatusChanged(
                                NatStatus::Private,
                            ),
                        );
                        self.update_network_status();
                    }
                },
                _ = async {
                    match self.circuit_accepted_at {
                        Some(at) => {
                            let renew_after = self.config.circuit_max_duration.mul_f64(0.8);
                            tokio::time::sleep_until(at + renew_after).await
                        }
                        None => std::future::pending().await,
                    }
                }, if self.circuit_accepted_at.is_some()
                    && matches!(self.relay_state, RelayState::Listening { .. }) => {
                    log::info!("Proactively renewing relay circuit (80% of max duration)");
                    self.circuit_accepted_at = None;
                    self.try_listen_on_circuit(true);
                },
                Some(cmd) = self.cmd_rx.recv() => {
                    if self.handle_command(cmd).await {
                        // Graceful shutdown: flush any writes still queued so
                        // the last edits reach peers before we stop polling.
                        self.drain_pending_changesets_on_shutdown(sync_rx).await;
                        break Ok(());
                    }
                },
            }
        }
    }

    async fn handle_local_changeset(&mut self, tc: TaggedChangeset) {
        // Route the local write to the group it originated from. Clone the
        // topic/group_key into locals up front so the group borrow ends before
        // we touch the swarm below (borrow-split). For a single-group node the
        // tag is always the default group's effective topic.
        let TaggedChangeset {
            effective_topic,
            changeset,
        } = tc;

        let (topic_name, group_key) = match self.groups.get(&effective_topic) {
            Some(g) => (g.topic_name.clone(), g.group_key.clone()),
            None => {
                log::debug!("local changeset for unknown group {effective_topic}; dropping");
                return;
            }
        };

        // Update local db_version for this group, and record this changeset as
        // pending confirmed delivery so a dropped push is redelivered promptly
        // (#81 Option A). Tracked even with zero peers connected — a peer that
        // joins later gets it on the next redelivery tick without waiting for a
        // full reconcile pass.
        if let Some(g) = self.groups.get_mut(&effective_topic) {
            g.local_db_version = g.local_db_version.max(changeset.db_version);
            g.pending_pushes.insert(
                changeset.db_version,
                PendingPush {
                    changeset: changeset.clone(),
                    acked_by: std::collections::HashSet::new(),
                },
            );
            while g.pending_pushes.len() > MAX_PENDING_PUSHES {
                if let Some(oldest) = g.pending_pushes.keys().next().copied() {
                    g.pending_pushes.remove(&oldest);
                    log::debug!(
                        "pending_pushes overflow for group {effective_topic}; \
                         evicting db_version={oldest} to the reconcile backstop"
                    );
                }
            }
        }
        self.update_network_status();

        // Fan-out: push changeset to all connected peers via request-response.
        // Exclude peers in an active rejection backoff *for this group* and
        // infrastructure peers.
        let now = tokio::time::Instant::now();
        let rejected: std::collections::HashSet<libp2p::PeerId> = self
            .groups
            .get(&effective_topic)
            .map(|g| {
                g.rejected_peers
                    .iter()
                    .filter(|(_, r)| r.until > now)
                    .map(|(p, _)| *p)
                    .collect()
            })
            .unwrap_or_default();
        let peer_ids: Vec<libp2p::PeerId> = self
            .peers
            .keys()
            .filter(|p| !rejected.contains(p))
            .filter(|p| !self.infrastructure_peers.contains(p))
            .cloned()
            .collect();

        if peer_ids.is_empty() {
            log::debug!(
                "No directly-connected peers; relying on relay push to wake sleeping peers"
            );
        } else {
            for peer_id in &peer_ids {
                let mut req = SyncRequest::Push {
                    changeset: changeset.clone(),
                    topic: topic_name.clone(),
                    hmac: None,
                };

                if let Some(ref gk) = group_key {
                    // Serialize with hmac: None, compute MAC, then set hmac
                    if let Ok(bytes) = serde_json::to_vec(&req) {
                        let tag = gk.mac(&bytes);
                        let SyncRequest::Push { ref mut hmac, .. } = req else {
                            unreachable!()
                        };
                        *hmac = Some(tag);
                    }
                }

                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .snapshot
                    .send_request(peer_id, req);
                // Correlate the ack back to this group + changeset (local only).
                self.pending_push_reqs
                    .insert(request_id, (effective_topic.clone(), changeset.db_version));
            }

            log::info!(
                "Pushed changeset (db_version={}) to {} peers",
                changeset.db_version,
                peer_ids.len()
            );
        }

        // Notify relay to send push notifications to sleeping mobile peers.
        // Must run even when peer_ids is empty — that's the case where both
        // peers are behind NAT with no direct connection, and push is the
        // only way to wake the other side.
        self.notify_relay_topic(&effective_topic);
    }

    /// Connected peers eligible for a fan-out push of `effective_topic`: every
    /// peer that isn't infrastructure and isn't in an active rejection backoff
    /// for this group. Mirrors the filter in `handle_local_changeset`.
    fn eligible_push_peers(
        &self,
        effective_topic: &str,
    ) -> std::collections::HashSet<libp2p::PeerId> {
        let now = tokio::time::Instant::now();
        let rejected: std::collections::HashSet<libp2p::PeerId> = self
            .groups
            .get(effective_topic)
            .map(|g| {
                g.rejected_peers
                    .iter()
                    .filter(|(_, r)| r.until > now)
                    .map(|(p, _)| *p)
                    .collect()
            })
            .unwrap_or_default();
        self.peers
            .keys()
            .filter(|p| !rejected.contains(p))
            .filter(|p| !self.infrastructure_peers.contains(p))
            .copied()
            .collect()
    }

    /// Record a `PushAck`: resolve the request id to its group + changeset and
    /// mark `peer` as having received it. Once every currently-connected
    /// eligible peer has acked a changeset it is dropped from `pending_pushes`
    /// (#81 Option A).
    fn note_push_ack(
        &mut self,
        request_id: request_response::OutboundRequestId,
        peer: libp2p::PeerId,
    ) {
        let Some((effective_topic, db_version)) = self.pending_push_reqs.remove(&request_id) else {
            return;
        };
        let eligible = self.eligible_push_peers(&effective_topic);
        if let Some(g) = self.groups.get_mut(&effective_topic)
            && let Some(p) = g.pending_pushes.get_mut(&db_version)
        {
            p.acked_by.insert(peer);
            if eligible.iter().all(|pid| p.acked_by.contains(pid)) {
                g.pending_pushes.remove(&db_version);
            }
        }
    }

    /// A reconcile digest proved `peer` holds all of `effective_topic`'s data
    /// (#82). That subsumes every pending push for the group as far as this peer
    /// is concerned — mark them acked by it and drop any now fully-delivered.
    fn note_peer_converged_pushes(&mut self, effective_topic: &str, peer: libp2p::PeerId) {
        let eligible = self.eligible_push_peers(effective_topic);
        if let Some(g) = self.groups.get_mut(effective_topic) {
            g.pending_pushes.retain(|_, p| {
                p.acked_by.insert(peer);
                !eligible.iter().all(|pid| p.acked_by.contains(pid))
            });
        }
    }

    /// Re-push every pending changeset to the connected peers that haven't acked
    /// it yet (#81 Option A). Runs on a short cadence so a dropped real-time
    /// push reaches a still-connected peer in seconds rather than waiting for
    /// the next reconcile pass. Re-application is idempotent (CRDT), so a
    /// redundant resend (e.g. an ack still in flight) is harmless.
    fn redeliver_pending_pushes(&mut self) {
        struct Redeliver {
            effective_topic: String,
            topic_name: String,
            group_key: Option<GroupKey>,
            db_version: u64,
            changeset: SyncChangeset,
            peers: Vec<libp2p::PeerId>,
        }

        // Collect the work first so no `groups` borrow is held across the swarm
        // sends below.
        let mut work: Vec<Redeliver> = Vec::new();
        let topics: Vec<String> = self.groups.keys().cloned().collect();
        for topic in topics {
            let eligible = self.eligible_push_peers(&topic);
            if eligible.is_empty() {
                continue;
            }
            let Some(g) = self.groups.get(&topic) else {
                continue;
            };
            if !g.registry_is_ready || g.pending_pushes.is_empty() {
                continue;
            }
            let topic_name = g.topic_name.clone();
            let group_key = g.group_key.clone();
            for (db_version, p) in &g.pending_pushes {
                let peers: Vec<libp2p::PeerId> = eligible
                    .iter()
                    .filter(|pid| !p.acked_by.contains(*pid))
                    .copied()
                    .collect();
                if peers.is_empty() {
                    continue;
                }
                work.push(Redeliver {
                    effective_topic: topic.clone(),
                    topic_name: topic_name.clone(),
                    group_key: group_key.clone(),
                    db_version: *db_version,
                    changeset: p.changeset.clone(),
                    peers,
                });
            }
        }

        let mut total = 0usize;
        for w in work {
            for peer_id in &w.peers {
                let mut req = SyncRequest::Push {
                    changeset: w.changeset.clone(),
                    topic: w.topic_name.clone(),
                    hmac: None,
                };
                if let Some(ref gk) = w.group_key
                    && let Ok(bytes) = serde_json::to_vec(&req)
                {
                    let tag = gk.mac(&bytes);
                    let SyncRequest::Push { ref mut hmac, .. } = req else {
                        unreachable!()
                    };
                    *hmac = Some(tag);
                }
                let request_id = self
                    .swarm
                    .behaviour_mut()
                    .snapshot
                    .send_request(peer_id, req);
                self.pending_push_reqs
                    .insert(request_id, (w.effective_topic.clone(), w.db_version));
                total += 1;
            }
        }
        if total > 0 {
            self.diagnostics
                .pending_pushes_redelivered
                .fetch_add(total as u64, std::sync::atomic::Ordering::Relaxed);
            log::debug!("Redelivered {total} pending push(es) to un-acked peers");
        }
    }

    /// Graceful-shutdown flush for queued local writes.
    ///
    /// A write made just before `shutdown()` has already been committed to
    /// SQLite — its shadow-table clocks and `db_version` are persisted in
    /// `dispatch_sync` *before* the changeset is enqueued — so no data is lost
    /// on shutdown. What can still be pending is that changeset's real-time
    /// Push fan-out to connected peers and the relay wake for sleeping peers,
    /// sitting in the bounded sync channel that the event loop would normally
    /// service. Drain the queue, dispatch each changeset, then give the swarm a
    /// brief, bounded grace period to actually flush the resulting outbound
    /// request-response traffic before the loop exits and the swarm is dropped.
    /// Without this, the last writes are delivered only on a peer's next
    /// version-vector catch-up (and if the shutting-down node is the only one
    /// holding them and never returns, not until it does). See PROBLEMS.md H6.
    ///
    /// Only runs on the explicit `shutdown()` path (which awaits the engine
    /// task); dropping a node without `shutdown()` aborts the task and skips it.
    async fn drain_pending_changesets_on_shutdown(
        &mut self,
        sync_rx: &mut mpsc::Receiver<TaggedChangeset>,
    ) {
        // Hard cap on how long shutdown waits to flush outbound traffic.
        const FLUSH_GRACE: Duration = Duration::from_secs(2);
        // If no swarm event arrives for this long, assume outbound has flushed
        // and stop early — keeps a clean shutdown snappy (and tests fast).
        const IDLE_GAP: Duration = Duration::from_millis(200);

        let mut drained = 0usize;
        while let Ok(tc) = sync_rx.try_recv() {
            self.handle_local_changeset(tc).await;
            drained += 1;
        }
        if drained == 0 {
            return;
        }
        log::info!(
            "Shutdown: drained {drained} pending changeset(s); flushing outbound (\u{2264}{FLUSH_GRACE:?})"
        );

        let deadline = tokio::time::Instant::now() + FLUSH_GRACE;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                // Re-armed each iteration: a quiet gap means outbound drained.
                _ = tokio::time::sleep(IDLE_GAP) => break,
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                },
            }
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<WaveSyncBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address:?}");
                // If this is a relay circuit address, add it as external so
                // rendezvous and identify advertise it to remote peers
                if address
                    .iter()
                    .any(|p| matches!(p, libp2p::multiaddr::Protocol::P2pCircuit))
                {
                    self.swarm.add_external_address(address.clone());
                    log::info!("Added relay circuit as external address: {address}");
                    // Re-register with rendezvous so the new circuit address is advertised
                    if let Some(ref rv_addr) = self.config.rendezvous_server
                        && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) =
                            rv_addr.iter().last()
                        && self.swarm.is_connected(&rv_peer_id)
                    {
                        self.rendezvous_register(rv_peer_id);
                    }
                } else {
                    // Non-circuit address (new network interface) — reconnect relay if
                    // needed. `try_dial_relay` is idempotent, so the typical Android
                    // pattern of 3 QUIC bind events firing back-to-back during
                    // startup no longer produces 3 parallel relay connections.
                    if matches!(self.relay_state, RelayState::Connecting { .. }) {
                        log::debug!(
                            "New listen address detected, redial relay if not already in flight"
                        );
                        self.try_dial_relay();
                    }
                }
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(identify::Event::Sent {
                peer_id,
                ..
            })) => {
                log::info!("Sent identify info to {peer_id:?}");
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Identify(
                identify::Event::Received { info, .. },
            )) => {
                log::info!("Received identify info: {:?}", info.protocol_version);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Ping(ping::Event {
                peer,
                connection,
                result,
            })) => {
                if let Err(ref e) = result {
                    log::warn!("Ping failed for {peer:?} over {connection:?}: {e}");
                    // If relay ping fails, log prominently — libp2p will close the
                    // connection, triggering ConnectionClosed → reconnect.
                    if matches!(
                        &self.relay_state,
                        RelayState::Connected { relay_peer_id, .. }
                        | RelayState::Listening { relay_peer_id }
                        if peer == *relay_peer_id
                    ) {
                        log::warn!("Ping to relay server failed — connection will be closed");
                    }
                } else {
                    log::debug!("Ping event with {peer:?} over {connection:?}: {result:?}");
                }
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Mdns(event)) => {
                self.handle_mdns(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Snapshot(event)) => {
                self.handle_snapshot(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::RelayClient(event)) => {
                self.handle_relay_client(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Dcutr(event)) => {
                self.handle_dcutr(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Autonat(event)) => {
                self.handle_autonat(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Rendezvous(event)) => {
                self.handle_rendezvous(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Push(event)) => {
                self.handle_push_event(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Auth(event)) => {
                self.handle_auth_challenge(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::AuthResult(event)) => {
                self.handle_auth_result(event);
            }
            SwarmEvent::Behaviour(WaveSyncBehaviourEvent::Upnp(event)) => {
                // libp2p auto-adds mapped addresses to the swarm's external
                // addresses; we just log for observability. Useful signal that
                // a residential router accepted our port-mapping request,
                // which means peers on other networks can now dial us
                // directly without DCUtR coordination.
                use libp2p::upnp::Event as UpnpEvent;
                match event {
                    UpnpEvent::NewExternalAddr(addr) => {
                        log::info!("UPnP: gateway mapped external address {addr}");
                    }
                    UpnpEvent::ExpiredExternalAddr(addr) => {
                        log::info!("UPnP: gateway expired external address {addr}");
                    }
                    UpnpEvent::GatewayNotFound => {
                        log::debug!(
                            "UPnP: no IGD-capable gateway on this network \
                             (expected on cellular/CGNAT/enterprise)"
                        );
                    }
                    UpnpEvent::NonRoutableGateway => {
                        log::debug!(
                            "UPnP: gateway is itself behind NAT (CGNAT or \
                             double-NAT); port mapping wouldn't help"
                        );
                    }
                }
            }
            SwarmEvent::ConnectionEstablished {
                peer_id,
                endpoint,
                connection_id,
                ..
            } => {
                log::info!("Connection established with {peer_id}");
                // Clear any pending DCUtR retry for this peer: a direct
                // connection just succeeded, so the hole-punch problem is
                // resolved (even if this connection happens to be via
                // circuit-relay; libp2p's DCUtR will upgrade later, and
                // we don't want stale retry timers firing redundantly).
                self.dcutr_retries.remove(&peer_id);
                // Count successful peer dials. Infrastructure peers (relay /
                // rendezvous) are excluded so the rate reflects sync-peer
                // discovery health, not infra plumbing.
                if !self.infrastructure_peers.contains(&peer_id) {
                    self.diagnostics
                        .peer_dial_successes
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // A successful connection clears any dial-failure backoff so a
                    // recovered peer is re-dialable at full speed.
                    self.clear_dial_backoff(&peer_id);

                    // Relay-cost telemetry: classify this connection as relayed
                    // (carried by the relay server) or direct. A DCUtR upgrade
                    // later arrives as a separate direct ConnectionEstablished,
                    // which flips the peer to direct below.
                    if addr_is_relayed(endpoint.get_remote_address()) {
                        self.diagnostics
                            .relayed_connections_established
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if self.peer_via_relay.get(&peer_id) == Some(&false) {
                            // A direct path to this peer already exists — this
                            // relay connection is redundant (#84 demotion). Only
                            // the lower-PeerId peer actively closes the shared
                            // connection; the other end tracks it and prunes on
                            // the resulting ConnectionClosed. See
                            // `should_demote_locally`.
                            if self.should_demote_locally(&peer_id) {
                                if self.swarm.close_connection(connection_id) {
                                    self.diagnostics
                                        .relay_connections_demoted
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                }
                                log::info!(
                                    "Relay demotion: dropped redundant relay connection to {peer_id} (direct already up)"
                                );
                            } else {
                                // Defer the close to the lower-id peer; keep the
                                // connection tracked so we prune it on close and
                                // clean it on full disconnect.
                                self.relayed_conn_ids
                                    .entry(peer_id)
                                    .or_default()
                                    .insert(connection_id);
                                log::debug!(
                                    "Relay demotion deferred to lower-PeerId peer for {peer_id} (direct already up)"
                                );
                            }
                        } else {
                            self.peer_via_relay.entry(peer_id).or_insert(true);
                            self.relayed_conn_ids
                                .entry(peer_id)
                                .or_default()
                                .insert(connection_id);
                        }
                    } else {
                        self.diagnostics
                            .direct_connections_established
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.peer_via_relay.insert(peer_id, false);
                        // Prefer the direct address for any future redial /
                        // status: `self.peers` may still hold the circuit
                        // address that was inserted when the relay first
                        // introduced this peer (`dial_introduced_peer`), and
                        // we must not redial that once a direct path is up.
                        self.peers
                            .insert(peer_id, endpoint.get_remote_address().clone());
                        // DERP demotion (#84): a direct path to this peer just
                        // came up (typically a DCUtR hole-punch). Schedule the
                        // relay-carried connection(s) for close after an
                        // anti-thrash dwell (process_pending_demotions) rather
                        // than closing immediately — a DCUtR upgrade can be flaky
                        // and drop right after establishing, and tearing the relay
                        // down instantly would force an immediate, costly
                        // re-reservation. If the direct path is still up at the
                        // deadline the relay is closed; otherwise the demotion is
                        // cancelled and the relay kept. Only the lower-PeerId peer
                        // schedules the active close (see `should_demote_locally`);
                        // the other end keeps the ids tracked and prunes on the
                        // resulting ConnectionClosed / full disconnect.
                        if self.should_demote_locally(&peer_id)
                            && let Some(ids) = self.relayed_conn_ids.remove(&peer_id)
                            && !ids.is_empty()
                        {
                            let deadline = tokio::time::Instant::now() + DEMOTION_DWELL;
                            self.pending_demotions.insert(peer_id, (deadline, ids));
                            log::debug!(
                                "Relay demotion scheduled for {peer_id} in ~{}s (direct path just came up)",
                                DEMOTION_DWELL.as_secs()
                            );
                        }
                    }

                    // Cache this (peer_id, multiaddr) so the next cold start
                    // can dial it directly before discovery (#29). Skip the
                    // relay so the cache stays a sync-peer set.
                    let db = self.default_group().db.clone();
                    let peer_str = peer_id.to_string();
                    let addr_str = endpoint.get_remote_address().to_string();
                    tokio::spawn(async move {
                        if let Err(e) =
                            crate::peer_addrs::record_success(&db, &peer_str, &addr_str).await
                        {
                            log::debug!("peer_addrs::record_success failed: {e}");
                        }
                    });
                }
                self.handle_connection_established(peer_id, &endpoint).await;
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                num_established,
                cause,
                ..
            } => {
                // Surface the close cause — essential for diagnosing relay churn
                // (idle timeout vs keep-alive vs transport reset vs remote close).
                // Logged at info for infrastructure peers (relay/rendezvous), whose
                // churn breaks circuit reservations, and at debug otherwise.
                if self.infrastructure_peers.contains(&peer_id) {
                    log::info!(
                        "Connection closed with infra peer {peer_id} ({num_established} remaining); cause: {cause:?}"
                    );
                } else {
                    log::debug!(
                        "Connection closed with {peer_id} ({num_established} remaining); cause: {cause:?}"
                    );
                }
                // Prune this connection from the relay-demotion tracking (#84).
                if let Some(ids) = self.relayed_conn_ids.get_mut(&peer_id) {
                    ids.remove(&connection_id);
                    if ids.is_empty() {
                        self.relayed_conn_ids.remove(&peer_id);
                    }
                }
                // Also prune it from any pending (dwelling) demotion so a relay
                // connection that closed on its own isn't "closed" again later.
                if let Some((_, ids)) = self.pending_demotions.get_mut(&peer_id) {
                    ids.remove(&connection_id);
                    if ids.is_empty() {
                        self.pending_demotions.remove(&peer_id);
                    }
                }
                self.handle_connection_closed(peer_id, num_established);
            }
            SwarmEvent::ListenerClosed { reason, .. } => {
                if let Err(ref e) = reason {
                    let err_str = format!("{e:?}");
                    if err_str.contains("NoAddressesInReservation") {
                        log::error!(
                            "Relay circuit failed: NoAddressesInReservation. \
                             The relay server needs --external-address configured. \
                             Peers can still discover via rendezvous fallback."
                        );
                    } else {
                        log::warn!("Listener closed with error: {e}");
                    }
                }
                // If relay was in Listening state, reset to Connected and re-request circuit
                if let RelayState::Listening { relay_peer_id } = self.relay_state {
                    log::warn!("Relay listener closed, re-requesting circuit reservation");
                    self.circuit_retry_count = 0;
                    self.relay_state = RelayState::Connected {
                        relay_peer_id,
                        connected_at: tokio::time::Instant::now(),
                    };
                    self.emit_network_event(
                        crate::network_status::NetworkEvent::RelayStatusChanged(
                            crate::network_status::RelayStatus::Connected,
                        ),
                    );
                    self.update_network_status();
                    // Re-request immediately (don't wait for AutoNAT). The
                    // listener that just closed cleared circuit_listen_pending
                    // already, so this issues a single fresh request.
                    self.circuit_listen_pending = false;
                    self.try_listen_on_circuit(false);
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                log::warn!("Listen address expired: {address}");
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                log::warn!("Outgoing connection error to {peer_id:?}: {error}");
                // If the failed dial was the relay, clear the in-flight guard
                // so the next reconnect-tick (or NewListenAddr trigger) can
                // re-arm. Without this, a single failed dial would lock out
                // every future relay-dial attempt for the engine's lifetime.
                if let Some(pid) = peer_id
                    && let Some(ref relay_addr) = self.config.relay_server
                    && let Some(libp2p::multiaddr::Protocol::P2p(relay_pid)) =
                        relay_addr.iter().last()
                    && pid == relay_pid
                {
                    self.relay_dial_pending = false;
                }
                // Count peer dial failures, excluding infrastructure peers so
                // the metric reflects sync-peer reachability — failed relay
                // redials are a separate (and noisier) signal.
                if let Some(pid) = peer_id
                    && !self.infrastructure_peers.contains(&pid)
                {
                    self.diagnostics
                        .peer_dial_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Grow this peer's dial backoff window (anti-storm). Cleared
                    // on the next successful connection.
                    self.record_dial_failure(pid);

                    // Bump fail_count on cached addresses for this peer
                    // (#29). No-op if the peer isn't cached yet — cache rows
                    // are only seeded on a successful connection.
                    let db = self.default_group().db.clone();
                    let peer_str = pid.to_string();
                    tokio::spawn(async move {
                        if let Err(e) =
                            crate::peer_addrs::record_failure_for_peer(&db, &peer_str).await
                        {
                            log::debug!("peer_addrs::record_failure_for_peer failed: {e}");
                        }
                    });
                }
                if let Some(pid) = peer_id {
                    self.dialing_peers.remove(&pid);
                    if !self.swarm.is_connected(&pid) && self.peers.remove(&pid).is_some() {
                        for g in self.groups.values_mut() {
                            g.pending_sync_peers.remove(&pid);
                            g.verified_peers.remove(&pid);
                        }
                        self.peer_identities.remove(&pid);
                        self.protocol_mismatch_peers.remove(&pid);
                        self.peer_via_relay.remove(&pid);
                        self.relayed_conn_ids.remove(&pid);
                        self.reconcile_capable.remove(&pid);
                        self.emit_network_event(
                            crate::network_status::NetworkEvent::PeerDisconnected(
                                crate::network_status::PeerId(pid.to_string()),
                            ),
                        );
                        self.update_network_status();
                    }
                }
                self.drain_pending_rendezvous_dials();
            }
            _ => {}
        }
    }

    fn handle_push_event(
        &mut self,
        event: request_response::Event<push_protocol::PushRequest, push_protocol::PushResponse>,
    ) {
        match event {
            request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Response {
                        response,
                        request_id,
                    },
                ..
            } => match response {
                push_protocol::PushResponse::Ok => {
                    // If this acks an in-flight RegisterToken, the relay now
                    // holds our token for that topic — mark it registered so
                    // the reconcile stops resending. Only confirmed-on-ack
                    // registration is trusted (issue #65).
                    if let Some(topic) = self.push_pending_registrations.remove(&request_id) {
                        self.push_registered_topics.insert(topic.clone());
                        self.update_network_status();
                        log::info!("Push token registration confirmed for topic {topic} by {peer}");
                    } else {
                        log::debug!("Push request acknowledged by {peer}");
                    }
                }
                push_protocol::PushResponse::Error { message } => {
                    // A rejected RegisterToken must not count as registered —
                    // drop it from the in-flight set so the reconcile retries.
                    if let Some(topic) = self.push_pending_registrations.remove(&request_id) {
                        log::warn!(
                            "Push token registration for topic {topic} rejected by {peer}: \
                             {message} — will retry"
                        );
                    } else {
                        log::warn!("Push request error from {peer}: {message}");
                    }
                }
                push_protocol::PushResponse::PeerList { peers } => {
                    // PeerList is a flat list of addresses for every existing
                    // peer on the topic. Group by the /p2p/ suffix so we hand
                    // each peer's full address set to the dialer at once —
                    // otherwise libp2p would race only one address per peer
                    // and a NAT'd peer's direct address would lose to its
                    // circuit fallback that never got tried.
                    //
                    // Use the **last** `/p2p/` component, not the first.
                    // For circuit-relay addresses
                    // (`/.../p2p/<relay>/p2p-circuit/p2p/<dest>`) the first
                    // is the relay and the last is the actual destination;
                    // grouping by the first lumps every circuit address
                    // for every dest into the relay's bucket, and
                    // `dial_introduced_peer` then sees the relay peer-id,
                    // observes we're already connected to it, and silently
                    // drops the dial — circuit-relay addresses from
                    // PeerList never reach libp2p.
                    let mut by_peer: std::collections::HashMap<libp2p::PeerId, Vec<String>> =
                        std::collections::HashMap::new();
                    for addr_str in &peers {
                        let Ok(addr) = addr_str.parse::<libp2p::Multiaddr>() else {
                            continue;
                        };
                        let mut last_pid = None;
                        for p in addr.iter() {
                            if let libp2p::multiaddr::Protocol::P2p(pid) = p {
                                last_pid = Some(pid);
                            }
                        }
                        if let Some(pid) = last_pid {
                            by_peer.entry(pid).or_default().push(addr_str.clone());
                        }
                    }
                    log::info!(
                        "Relay {peer} introduced {} peer(s) on our topic",
                        by_peer.len()
                    );
                    self.diagnostics
                        .peerlist_introductions
                        .fetch_add(by_peer.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    for (_pid, addrs) in by_peer {
                        self.dial_introduced_peer(&addrs);
                    }
                }
            },
            request_response::Event::Message {
                peer,
                message:
                    request_response::Message::Request {
                        request, channel, ..
                    },
                ..
            } => {
                match &request {
                    push_protocol::PushRequest::PeerJoined { topic, peer_addrs } => {
                        // Only honour PeerJoined from our relay. Any other
                        // sender is unexpected — ignore with an error response.
                        let from_relay = matches!(
                            self.relay_state,
                            RelayState::Connected { relay_peer_id, .. }
                                | RelayState::Listening { relay_peer_id }
                                if relay_peer_id == peer
                        );
                        if !from_relay {
                            log::warn!("Ignoring PeerJoined from non-relay peer {peer}");
                            let _ = self.swarm.behaviour_mut().push.send_response(
                                channel,
                                push_protocol::PushResponse::Error {
                                    message: "not your relay".to_string(),
                                },
                            );
                            return;
                        }
                        if *topic != self.default_group().topic_name {
                            log::debug!(
                                "Ignoring PeerJoined for foreign topic (ours vs theirs hash mismatch)"
                            );
                            let _ = self
                                .swarm
                                .behaviour_mut()
                                .push
                                .send_response(channel, push_protocol::PushResponse::Ok);
                            return;
                        }
                        log::info!(
                            "Relay announced new peer on topic with {} address(es)",
                            peer_addrs.len()
                        );
                        self.diagnostics
                            .peerjoined_introductions
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        // peer_addrs is already a single peer's address set —
                        // pass them all to the dialer so libp2p races them.
                        self.dial_introduced_peer(peer_addrs);
                        let _ = self
                            .swarm
                            .behaviour_mut()
                            .push
                            .send_response(channel, push_protocol::PushResponse::Ok);
                    }
                    _ => {
                        // Peers shouldn't receive RegisterToken / NotifyTopic
                        // from anywhere. Reject politely.
                        let _ = self.swarm.behaviour_mut().push.send_response(
                            channel,
                            push_protocol::PushResponse::Error {
                                message: "unsupported request for a peer".to_string(),
                            },
                        );
                    }
                }
            }
            request_response::Event::OutboundFailure {
                error,
                peer,
                request_id,
                ..
            } => {
                // A RegisterToken that never reached the relay (e.g. the
                // relayed substream wasn't ready at join time) must not stay
                // marked in-flight, or the reconcile would skip it forever.
                // Dropping it here lets the 5s reconcile resend (issue #65).
                if let Some(topic) = self.push_pending_registrations.remove(&request_id) {
                    log::warn!(
                        "Push token registration for topic {topic} to {peer} failed: \
                         {error} — will retry"
                    );
                } else {
                    log::warn!("Push request to {peer} failed: {error}");
                }
            }
            _ => {}
        }
    }

    fn handle_auth_challenge(
        &mut self,
        event: request_response::Event<auth_protocol::AuthChallenge, auth_protocol::AuthResponse>,
    ) {
        use request_response::{Event, Message};
        if let Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } = event
        {
            let api_key = match &self.api_key {
                Some(k) => k.clone(),
                None => {
                    log::warn!("Received auth challenge from {peer} but no API key configured");
                    return;
                }
            };

            let nonce_sig = match self.keypair.sign(&request.nonce) {
                Ok(sig) => sig,
                Err(e) => {
                    log::error!("Failed to sign auth nonce: {e}");
                    return;
                }
            };

            let response = auth_protocol::AuthResponse { api_key, nonce_sig };
            if let Err(e) = self
                .swarm
                .behaviour_mut()
                .auth
                .send_response(channel, response)
            {
                log::error!("Failed to send auth response to relay {peer}: {e:?}");
            } else {
                log::info!("Auth response sent to relay {peer}");
            }
        }
    }

    fn handle_auth_result(
        &mut self,
        event: request_response::Event<auth_protocol::AuthResult, ()>,
    ) {
        use request_response::{Event, Message};
        if let Event::Message {
            peer,
            message: Message::Request {
                request, channel, ..
            },
            ..
        } = event
        {
            // Ack immediately
            let _ = self
                .swarm
                .behaviour_mut()
                .auth_result
                .send_response(channel, ());

            if request.accepted {
                log::info!("Relay {peer} accepted auth — managed relay active");
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Connected,
                ));
            } else {
                let reason = request.reason.as_deref().unwrap_or("invalid API key");
                log::warn!("Relay {peer} rejected auth: {reason}");
                self.emit_network_event(crate::network_status::NetworkEvent::EngineFailed {
                    reason: format!("Relay auth rejected: {reason}"),
                });
            }
        }
    }
}

/// Partition a flat list of bootstrap multiaddrs into two buckets:
///
///   * Multiaddrs that end in `/p2p/<peer-id>` are grouped by peer-id so a
///     single `DialOpts::peer_id(p).addresses(addrs)` call can race them.
///     This matters most when the FCM payload supplies several candidate
///     addresses for one remote peer.
///   * Multiaddrs without a `/p2p/` suffix are returned as-is for the
///     legacy single-address `swarm.dial(addr)` path.
fn group_bootstrap_addrs(
    addrs: Vec<libp2p::Multiaddr>,
) -> (
    std::collections::HashMap<libp2p::PeerId, Vec<libp2p::Multiaddr>>,
    Vec<libp2p::Multiaddr>,
) {
    let mut grouped: std::collections::HashMap<libp2p::PeerId, Vec<libp2p::Multiaddr>> =
        std::collections::HashMap::new();
    let mut suffixless: Vec<libp2p::Multiaddr> = Vec::new();
    for addr in addrs {
        if let Some(libp2p::multiaddr::Protocol::P2p(peer_id)) = addr.iter().last() {
            grouped.entry(peer_id).or_default().push(addr);
        } else {
            suffixless.push(addr);
        }
    }
    (grouped, suffixless)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_backoff_grows_exponentially_and_caps() {
        // 1-based attempts double from 30s and cap at 1 hour (Rule 2.8
        // anti-storm: a persistently-mismatching peer is re-evaluated ever less
        // often, never faster than the base and never more than hourly).
        assert_eq!(rejection_backoff(1), Duration::from_secs(30));
        assert_eq!(rejection_backoff(2), Duration::from_secs(60));
        assert_eq!(rejection_backoff(3), Duration::from_secs(120));
        assert_eq!(rejection_backoff(4), Duration::from_secs(240));
        assert_eq!(rejection_backoff(7), Duration::from_secs(1920));
        // Caps at 1 hour and never overflows for large attempt counts.
        assert_eq!(rejection_backoff(8), Duration::from_secs(3600));
        assert_eq!(rejection_backoff(1000), Duration::from_secs(3600));
        // Non-decreasing.
        let mut prev = Duration::ZERO;
        for a in 1..=12 {
            let d = rejection_backoff(a);
            assert!(d >= prev, "backoff must be non-decreasing");
            prev = d;
        }
    }

    #[test]
    fn addr_is_lan_classifies_local_vs_public() {
        let lan = [
            "/ip4/192.168.1.150/udp/4001/quic-v1",
            "/ip4/10.0.0.5/tcp/4001",
            "/ip4/172.16.3.4/tcp/4001",
            "/ip4/169.254.1.1/tcp/4001",        // link-local
            "/ip4/127.0.0.1/tcp/4001",          // loopback
            "/ip6/fd00::abcd/udp/4001/quic-v1", // ULA
            "/ip6/fe80::1/tcp/4001",            // link-local
            "/ip6/::1/tcp/4001",                // loopback
        ];
        for s in lan {
            assert!(
                addr_is_lan(&s.parse().unwrap()),
                "{s} should be classified LAN"
            );
        }
        let public = [
            "/ip4/77.37.125.212/udp/4001/quic-v1",
            "/ip4/8.8.8.8/tcp/4001",
            "/ip6/2606:4700::1/tcp/4001",
            // A circuit address's first IP literal is the relay's public IP →
            // not LAN (and circuit addrs are filtered separately anyway).
            "/ip4/77.37.125.212/tcp/4001/p2p/12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3/p2p-circuit",
        ];
        for s in public {
            assert!(
                !addr_is_lan(&s.parse().unwrap()),
                "{s} should NOT be classified LAN"
            );
        }
    }

    #[test]
    fn peer_dial_backoff_grows_quadratically_and_caps() {
        // 6s, 9s, 14s, 21s, … (5 + n²), capped at 5 minutes.
        assert_eq!(peer_dial_backoff(1), Duration::from_secs(6));
        assert_eq!(peer_dial_backoff(2), Duration::from_secs(9));
        assert_eq!(peer_dial_backoff(3), Duration::from_secs(14));
        assert_eq!(peer_dial_backoff(4), Duration::from_secs(21));
        // Caps at 300s and never overflows for large failure counts.
        assert_eq!(peer_dial_backoff(100), Duration::from_secs(300));
        assert_eq!(peer_dial_backoff(u32::MAX), Duration::from_secs(300));
        // Non-decreasing.
        let mut prev = Duration::ZERO;
        for n in 0..=40 {
            let d = peer_dial_backoff(n);
            assert!(d >= prev, "dial backoff must be non-decreasing");
            prev = d;
        }
    }

    // Regression: the circuit-relay storm (#84). When a direct path to a peer
    // already exists, the relay's repeated re-introduction of that peer's
    // circuit address must NOT be re-dialed — otherwise each re-dial re-opens a
    // circuit the demotion logic immediately closes, exhausting the relay's
    // per-peer circuit cap (ResourceLimitExceeded). The pure filter below is the
    // decision that breaks the loop; here we pin its behaviour both ways.
    #[test]
    fn dialable_addrs_drops_circuit_when_direct_preferred() {
        let pid: libp2p::PeerId = "12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3"
            .parse()
            .unwrap();
        let direct: libp2p::Multiaddr = format!("/ip4/192.168.1.150/udp/39981/quic-v1/p2p/{pid}")
            .parse()
            .unwrap();
        let circuit: libp2p::Multiaddr =
            format!("/ip4/77.37.125.212/udp/4001/quic-v1/p2p/{pid}/p2p-circuit/p2p/{pid}")
                .parse()
                .unwrap();

        // Direct preferred: circuit address is dropped, direct kept.
        let kept = dialable_addrs_preferring_direct(
            vec![direct.clone(), circuit.clone()],
            /* prefers_direct */ true,
        );
        assert_eq!(kept, vec![direct.clone()], "circuit must be filtered out");

        // Direct preferred but ONLY a circuit address available: result is
        // empty so the caller skips the dial entirely (no storm).
        let none = dialable_addrs_preferring_direct(vec![circuit.clone()], true);
        assert!(
            none.is_empty(),
            "a circuit-only set must yield no dialable address when direct is preferred"
        );

        // No direct path yet: nothing is filtered — the circuit is the only way
        // to reach the peer and must still be dialable.
        let all = dialable_addrs_preferring_direct(
            vec![direct.clone(), circuit.clone()],
            /* prefers_direct */ false,
        );
        assert_eq!(
            all,
            vec![direct, circuit],
            "without a direct path, keep all"
        );
    }

    #[test]
    fn group_bootstrap_addrs_collapses_addrs_for_same_peer() {
        // Same peer_id, three different transport addresses — they should
        // end up in one HashMap entry that DialOpts can race together.
        let pid: libp2p::PeerId = "12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3"
            .parse()
            .unwrap();
        let a1: libp2p::Multiaddr = format!("/ip4/79.112.10.59/tcp/42674/p2p/{pid}")
            .parse()
            .unwrap();
        let a2: libp2p::Multiaddr = format!("/ip4/192.168.1.150/tcp/39981/p2p/{pid}")
            .parse()
            .unwrap();
        let a3: libp2p::Multiaddr =
            format!("/ip4/77.37.125.212/tcp/4001/p2p/{pid}/p2p-circuit/p2p/{pid}")
                .parse()
                .unwrap();

        let (grouped, suffixless) = group_bootstrap_addrs(vec![a1.clone(), a2.clone(), a3.clone()]);

        assert!(suffixless.is_empty());
        assert_eq!(grouped.len(), 1);
        let group = grouped.get(&pid).expect("peer_id should be present");
        assert_eq!(group.len(), 3);
        assert!(group.contains(&a1));
        assert!(group.contains(&a2));
        assert!(group.contains(&a3));
    }

    #[test]
    fn group_bootstrap_addrs_separates_distinct_peers() {
        let p1: libp2p::PeerId = "12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3"
            .parse()
            .unwrap();
        let p2: libp2p::PeerId = "12D3KooWQTV2REAJX77iesp2Qjax5tiK7Zt65FA7tUL6Ch47BJc6"
            .parse()
            .unwrap();
        let a1: libp2p::Multiaddr = format!("/ip4/1.2.3.4/tcp/1234/p2p/{p1}").parse().unwrap();
        let a2: libp2p::Multiaddr = format!("/ip4/5.6.7.8/tcp/5678/p2p/{p2}").parse().unwrap();

        let (grouped, suffixless) = group_bootstrap_addrs(vec![a1.clone(), a2.clone()]);

        assert!(suffixless.is_empty());
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[&p1], vec![a1]);
        assert_eq!(grouped[&p2], vec![a2]);
    }

    #[test]
    fn group_bootstrap_addrs_passes_suffixless_through() {
        // A multiaddr without a /p2p/ suffix can't be batched by peer-id —
        // it must go through the legacy single-address swarm.dial path.
        let bare: libp2p::Multiaddr = "/ip4/1.2.3.4/tcp/1234".parse().unwrap();
        let pid: libp2p::PeerId = "12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3"
            .parse()
            .unwrap();
        let with_pid: libp2p::Multiaddr =
            format!("/ip4/5.6.7.8/tcp/5678/p2p/{pid}").parse().unwrap();

        let (grouped, suffixless) = group_bootstrap_addrs(vec![bare.clone(), with_pid.clone()]);

        assert_eq!(suffixless, vec![bare]);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&pid], vec![with_pid]);
    }

    #[test]
    fn group_bootstrap_addrs_empty_input() {
        let (grouped, suffixless) = group_bootstrap_addrs(vec![]);
        assert!(grouped.is_empty());
        assert!(suffixless.is_empty());
    }
}
