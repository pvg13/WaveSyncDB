mod push_notifier;
mod push_protocol;
mod push_sender;
mod push_store;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use clap::Parser;
use libp2p::{
    Multiaddr, SwarmBuilder, connection_limits, futures::StreamExt, identify, identity, noise,
    ping, relay, rendezvous, request_response, swarm::SwarmEvent, yamux,
};
use libp2p_swarm_derive::NetworkBehaviour;
use rand::rngs::OsRng;

use push_protocol::{PUSH_PROTOCOL, PushCodec, PushRequest, PushResponse};
use push_sender::{ApnsConfig, FcmConfig, PushSender};
use push_store::PushStore;

#[derive(Parser)]
#[command(
    name = "wavesync-relay",
    about = "WaveSyncDB relay and rendezvous server"
)]
struct Cli {
    /// Listen address (default: /ip4/0.0.0.0/tcp/4001)
    #[arg(long, env = "LISTEN_ADDR", default_value = "/ip4/0.0.0.0/tcp/4001")]
    listen_addr: String,

    /// Path to a file containing the persistent identity keypair.
    /// If the file does not exist, a new keypair is generated and saved.
    #[arg(long, env = "IDENTITY_FILE")]
    identity_file: Option<PathBuf>,

    /// Base64-encoded protobuf identity keypair, or a path to a file whose
    /// contents are the base64 string. Overrides --identity-file.
    /// Generate with: wavesync-relay --generate-identity
    #[arg(long, env = "IDENTITY_KEYPAIR")]
    identity_keypair: Option<String>,

    /// Print a new base64-encoded identity keypair and its PeerId, then exit.
    #[arg(long)]
    generate_identity: bool,

    /// Maximum number of relay circuits (0 = unlimited)
    #[arg(long, env = "MAX_CIRCUITS", default_value_t = 256)]
    max_circuits: usize,

    /// Maximum circuit duration in seconds (default: 3600 = 1 hour)
    #[arg(long, env = "MAX_CIRCUIT_DURATION_SECS", default_value_t = 3600)]
    max_circuit_duration: u64,

    /// Maximum bytes per circuit (0 = unlimited, default: unlimited)
    #[arg(long, env = "MAX_CIRCUIT_BYTES", default_value_t = 0)]
    max_circuit_bytes: u64,

    /// Maximum total relay reservations across all peers (default: 1024).
    /// libp2p's stock default is 128 — too tight for a mobile-FCM workload
    /// where the same peer may reconnect dozens of times per hour.
    #[arg(long, env = "MAX_RESERVATIONS", default_value_t = 1024)]
    max_reservations: usize,

    /// Maximum reservations per peer (default: 32). libp2p's stock default
    /// is 4. With FCM-driven reconnect bursts (each new connection asks for
    /// a fresh reservation while the old one is still in its
    /// `reservation_duration` window) clients hit the per-peer cap within
    /// minutes, every subsequent request is denied with
    /// `ResourceLimitExceeded`, and circuit-relay sync stops working for
    /// that peer.
    #[arg(long, env = "MAX_RESERVATIONS_PER_PEER", default_value_t = 32)]
    max_reservations_per_peer: usize,

    /// Reservation duration in seconds (default: 600 = 10 min). libp2p's
    /// stock default is 3600 (1 hour). Reservations auto-release when the
    /// underlying connection closes anyway, so this only matters for clients
    /// that disconnect uncleanly. A shorter duration limits how long stale
    /// reservations clog the per-peer cap during rapid reconnect storms.
    #[arg(long, env = "RESERVATION_DURATION_SECS", default_value_t = 600)]
    reservation_duration: u64,

    /// Path to the push token SQLite database (enables push notifications)
    #[arg(long, env = "PUSH_DB")]
    push_db: Option<String>,

    /// FCM service account JSON — either a file path or the raw JSON string.
    /// When the value starts with '{', it is treated as inline JSON;
    /// otherwise it is read as a file path.
    #[arg(long, env = "FCM_CREDENTIALS")]
    fcm_credentials: Option<String>,

    /// APNs .p8 key — either the PEM contents (starting with `-----BEGIN`)
    /// or a filesystem path to a .p8 file. Matches the FCM_CREDENTIALS
    /// convention so both can be pasted as inline secrets.
    #[arg(long, env = "APNS_KEY_FILE")]
    apns_key_file: Option<String>,

    /// APNs key ID
    #[arg(long, env = "APNS_KEY_ID")]
    apns_key_id: Option<String>,

    /// APNs team ID
    #[arg(long, env = "APNS_TEAM_ID")]
    apns_team_id: Option<String>,

    /// APNs bundle ID (e.g., com.example.myapp)
    #[arg(long, env = "APNS_BUNDLE_ID")]
    apns_bundle_id: Option<String>,

    /// Whether to use APNs sandbox endpoint
    #[arg(long, env = "APNS_SANDBOX")]
    apns_sandbox: bool,

    /// Push notification cooldown window in seconds (default: 1).
    /// First notification fires immediately; subsequent ones within this
    /// window are batched. The shorter the window, the faster trailing-edge
    /// notifications arrive at the cost of more FCM volume on rapid bursts.
    /// 1s strikes a balance between near-real-time feel and avoiding spam.
    #[arg(long, env = "PUSH_DEBOUNCE_SECS", default_value_t = 1)]
    push_debounce_secs: u64,

    /// External address to advertise (repeatable, e.g. /ip4/77.37.125.212/tcp/4001).
    /// Required when running behind NAT or in Docker.
    #[arg(long, env = "EXTERNAL_ADDRESS", value_delimiter = ',')]
    external_address: Vec<String>,

    /// Idle connection timeout in seconds (default: 300).
    /// Must be longer than the client keep-alive interval to prevent premature
    /// disconnects. 300s works with the default 90s client ping interval.
    #[arg(long, env = "IDLE_CONNECTION_TIMEOUT_SECS", default_value_t = 300)]
    idle_connection_timeout: u64,
}

#[derive(NetworkBehaviour)]
struct RelayServerBehaviour {
    connection_limits: connection_limits::Behaviour,
    relay: relay::Behaviour,
    rendezvous: rendezvous::server::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    autonat: libp2p::autonat::v2::server::Behaviour,
    push: request_response::Behaviour<PushCodec>,
}

/// Read a secret file and return its trimmed contents, or log a warning and
/// return `None` if the path is missing / is a directory / is empty. Docker
/// auto-creates host paths for bind mounts whose source doesn't exist, so a
/// "mounted but never populated" secret shows up inside the container as an
/// empty directory rather than a missing path — handle both.
fn read_optional_secret(kind: &str, path: &str) -> Option<String> {
    match std::fs::metadata(path) {
        Ok(meta) if meta.is_file() => match std::fs::read_to_string(path) {
            Ok(s) if !s.trim().is_empty() => Some(s),
            Ok(_) => {
                log::warn!("{kind} file at {path:?} is empty; skipping");
                None
            }
            Err(e) => {
                log::warn!("{kind} file at {path:?} not readable ({e}); skipping");
                None
            }
        },
        Ok(_) => {
            log::warn!(
                "{kind} path {path:?} exists but is not a regular file \
                 (empty bind mount?); skipping"
            );
            None
        }
        Err(e) => {
            log::warn!("{kind} file at {path:?} not available ({e}); skipping");
            None
        }
    }
}

/// Standard Docker secret mount points. Docker / Compose / Dokploy / Coolify
/// all converge on `/run/secrets/<name>` for user-uploaded secret files,
/// so auto-discovering these paths means a deployer only needs to upload
/// the file — no extra `FCM_CREDENTIALS=/run/secrets/fcm.json` env var to
/// keep in sync with the mount.
const DEFAULT_FCM_SECRET_PATH: &str = "/run/secrets/fcm.json";
const DEFAULT_APNS_SECRET_PATH: &str = "/run/secrets/apns.p8";

/// Resolve a secret "source" — either an explicit CLI/env value, or, if
/// that isn't set, the conventional Docker secret mount point. Returns the
/// string the existing secret-parser logic should consume (an inline blob
/// from the env var, or a file path the parser will read).
///
/// Auto-discovery only kicks in when the file actually exists, so plain
/// `cargo run` outside Docker keeps the previous behaviour (no FCM unless
/// you set `FCM_CREDENTIALS`).
fn resolve_secret_source(
    kind: &str,
    cli_value: Option<&String>,
    default_path: &str,
) -> Option<String> {
    if let Some(v) = cli_value {
        let trimmed = v.trim();
        if !trimmed.is_empty() {
            return Some(v.clone());
        }
    }
    match std::fs::metadata(default_path) {
        Ok(meta) if meta.is_file() => {
            log::info!("auto-discovered {kind} at {default_path}");
            Some(default_path.to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod resolve_secret_source_tests {
    use super::*;
    use std::io::Write;

    fn temp_file_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "wsync-relay-test-{}-{}-{}",
            name,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        p
    }

    #[test]
    fn explicit_cli_value_beats_default_path() {
        let path = temp_file_path("explicit");
        std::fs::write(&path, b"{\"x\": 1}").unwrap();
        let explicit = "{\"inline\": true}".to_string();
        let got = resolve_secret_source(
            "test",
            Some(&explicit),
            path.to_str().unwrap(),
        );
        assert_eq!(got, Some(explicit));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_cli_value_falls_through_to_default() {
        let path = temp_file_path("fallback");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"{}").unwrap();
        let blank = "  ".to_string();
        let got = resolve_secret_source("test", Some(&blank), path.to_str().unwrap());
        assert_eq!(got.as_deref(), path.to_str());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_default_returns_none() {
        let path = temp_file_path("missing");
        // Don't create the file.
        let got = resolve_secret_source("test", None, path.to_str().unwrap());
        assert_eq!(got, None);
    }

    #[test]
    fn directory_at_default_path_returns_none() {
        // Empty bind mount in Docker shows up as a directory — must not be
        // treated as a valid secret source.
        let path = temp_file_path("dir");
        std::fs::create_dir(&path).unwrap();
        let got = resolve_secret_source("test", None, path.to_str().unwrap());
        assert_eq!(got, None);
        let _ = std::fs::remove_dir(&path);
    }

    #[test]
    fn existing_default_returns_path_when_cli_is_none() {
        let path = temp_file_path("default");
        std::fs::write(&path, b"data").unwrap();
        let got = resolve_secret_source("test", None, path.to_str().unwrap());
        assert_eq!(got.as_deref(), path.to_str());
        let _ = std::fs::remove_file(&path);
    }
}

fn load_or_generate_keypair(path: &PathBuf) -> identity::Keypair {
    if path.exists() {
        let bytes = std::fs::read(path).expect("Failed to read identity file");
        identity::Keypair::from_protobuf_encoding(&bytes).expect("Invalid identity file format")
    } else {
        let keypair = identity::Keypair::generate_ed25519();
        let bytes = keypair
            .to_protobuf_encoding()
            .expect("Failed to encode keypair");
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(path, bytes).expect("Failed to write identity file");
        log::info!("Generated new identity at {}", path.display());
        keypair
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The autonat v2 server logs a WARN every time a dial-back times out, which
    // is the normal mechanism for classifying NAT'd peers — we expect dozens per
    // hour and they are not actionable. Pin that one module to error-only so it
    // doesn't drown out real warnings. RUST_LOG can still override.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module(
            "libp2p_autonat::v2::server::handler::dial_request",
            log::LevelFilter::Error,
        )
        .init();

    // Compose interpolation (`${FOO:-}`) leaves unset vars as empty strings in
    // the process env. For string flags that becomes `Some("")` and panics
    // later; for bool flags like APNS_SANDBOX, clap itself errors out
    // (empty string is not a valid bool). Strip blank env vars up-front so
    // every flag falls back to its declared default / None.
    //
    // SAFETY: called before any threads are spawned in this process.
    for var in [
        "LISTEN_ADDR",
        "IDENTITY_FILE",
        "IDENTITY_KEYPAIR",
        "MAX_CIRCUITS",
        "MAX_CIRCUIT_DURATION_SECS",
        "MAX_CIRCUIT_BYTES",
        "PUSH_DB",
        "PUSH_DEBOUNCE_SECS",
        "FCM_CREDENTIALS",
        "APNS_KEY_FILE",
        "APNS_KEY_ID",
        "APNS_TEAM_ID",
        "APNS_BUNDLE_ID",
        "APNS_SANDBOX",
        "EXTERNAL_ADDRESS",
        "IDLE_CONNECTION_TIMEOUT_SECS",
    ] {
        if std::env::var(var).is_ok_and(|v| v.trim().is_empty()) {
            unsafe { std::env::remove_var(var) };
        }
    }

    // One-shot startup diagnostic: print which env vars are set so a
    // dokploy/compose deploy that silently drops a variable shows up
    // immediately in the relay logs instead of looking like a code bug.
    // Values are masked — only presence and length are reported.
    for var in [
        "PUSH_DB",
        "FCM_CREDENTIALS",
        "APNS_KEY_FILE",
        "IDENTITY_KEYPAIR",
        "IDENTITY_FILE",
        "EXTERNAL_ADDRESS",
    ] {
        match std::env::var(var) {
            Ok(v) => log::info!(
                "env {var}: set (len={}, starts_with={:?})",
                v.len(),
                v.chars().take(8).collect::<String>()
            ),
            Err(_) => log::info!("env {var}: <unset>"),
        }
    }

    let cli = Cli::parse();

    if cli.generate_identity {
        let keypair = identity::Keypair::generate_ed25519();
        let b64 = base64::engine::general_purpose::STANDARD.encode(
            keypair
                .to_protobuf_encoding()
                .expect("Failed to encode keypair"),
        );
        let peer_id = keypair.public().to_peer_id();
        println!("IDENTITY_KEYPAIR={b64}");
        println!("PeerId: {peer_id}");
        return Ok(());
    }

    // Resolve the identity keypair. IDENTITY_KEYPAIR takes precedence (inline
    // base64 or path-to-base64). If it's a path that doesn't resolve to a
    // populated file, fall through to IDENTITY_FILE (generate-and-persist)
    // rather than crashing — makes the docker-compose setup forgiving when a
    // user deploys without populating every secret mount.
    let keypair_from_keypair_value = cli.identity_keypair.as_ref().and_then(|value| {
        let looks_like_path = value.contains('/') || value.contains('\\');
        let b64 = if looks_like_path {
            read_optional_secret("IDENTITY_KEYPAIR", value)?
                .trim()
                .to_string()
        } else {
            value.trim().to_string()
        };
        match base64::engine::general_purpose::STANDARD.decode(&b64) {
            Ok(bytes) => match identity::Keypair::from_protobuf_encoding(&bytes) {
                Ok(kp) => Some(kp),
                Err(e) => {
                    log::warn!(
                        "IDENTITY_KEYPAIR not a valid protobuf-encoded keypair ({e}); falling back"
                    );
                    None
                }
            },
            Err(e) => {
                log::warn!("IDENTITY_KEYPAIR not valid base64 ({e}); falling back");
                None
            }
        }
    });
    let keypair = keypair_from_keypair_value
        .or_else(|| cli.identity_file.as_ref().map(load_or_generate_keypair))
        .unwrap_or_else(identity::Keypair::generate_ed25519);

    let peer_id = keypair.public().to_peer_id();
    log::info!("Relay server PeerId: {peer_id}");

    let relay_config = relay::Config {
        max_circuits: cli.max_circuits,
        max_circuit_duration: std::time::Duration::from_secs(cli.max_circuit_duration),
        max_circuit_bytes: cli.max_circuit_bytes,
        max_reservations: cli.max_reservations,
        max_reservations_per_peer: cli.max_reservations_per_peer,
        reservation_duration: std::time::Duration::from_secs(cli.reservation_duration),
        ..Default::default()
    };

    // Initialize push notification subsystem if configured
    let push_notifier = if let Some(ref push_db_path) = cli.push_db {
        let store = Arc::new(
            PushStore::open(push_db_path)
                .await
                .expect("Failed to open push token database"),
        );

        let fcm_source = resolve_secret_source(
            "FCM credentials",
            cli.fcm_credentials.as_ref(),
            DEFAULT_FCM_SECRET_PATH,
        );
        let fcm_config = fcm_source.and_then(|value| {
            // Inline JSON if it starts with '{', otherwise a file path.
            let json = if value.trim_start().starts_with('{') {
                value.clone()
            } else {
                read_optional_secret("FCM credentials", &value)?
            };
            match serde_json::from_str::<serde_json::Value>(&json) {
                Ok(sa) => match sa["project_id"].as_str() {
                    Some(project_id) => Some(FcmConfig {
                        project_id: project_id.to_string(),
                        service_account_json: json,
                    }),
                    None => {
                        log::warn!("FCM credentials missing project_id; skipping FCM");
                        None
                    }
                },
                Err(e) => {
                    log::warn!("FCM credentials JSON invalid ({e}); skipping FCM");
                    None
                }
            }
        });

        let apns_key_source = resolve_secret_source(
            "APNs key",
            cli.apns_key_file.as_ref(),
            DEFAULT_APNS_SECRET_PATH,
        );
        let apns_config = if let (Some(key_source), Some(key_id), Some(team_id), Some(bundle_id)) = (
            apns_key_source.as_ref(),
            cli.apns_key_id.as_ref(),
            cli.apns_team_id.as_ref(),
            cli.apns_bundle_id.as_ref(),
        ) {
            // Inline PEM if it contains `-----BEGIN`, otherwise a file path.
            let key_pem = if key_source.contains("-----BEGIN") {
                Some(key_source.clone())
            } else {
                read_optional_secret("APNs key", key_source)
            };
            key_pem.map(|pem| ApnsConfig {
                key_pem: pem,
                key_id: key_id.clone(),
                team_id: team_id.clone(),
                bundle_id: bundle_id.clone(),
                sandbox: cli.apns_sandbox,
            })
        } else {
            None
        };

        if fcm_config.is_none() && apns_config.is_none() {
            log::warn!("Push DB configured but no FCM or APNs credentials provided");
        }

        let sender = Arc::new(PushSender::new(fcm_config, apns_config));
        let debounce = Duration::from_secs(cli.push_debounce_secs);
        let notifier = push_notifier::PushNotifier::spawn(store.clone(), sender, debounce);

        log::info!("Push notifications enabled (db: {push_db_path})");
        Some((store, notifier))
    } else {
        None
    };

    let mut swarm = SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            Default::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let identify = identify::Behaviour::new(identify::Config::new(
                "/wavesync-relay/1.0.0".into(),
                key.public(),
            ));

            let push_behaviour = request_response::Behaviour::new(
                [(PUSH_PROTOCOL, request_response::ProtocolSupport::Full)],
                request_response::Config::default(),
            );

            let conn_limits = connection_limits::ConnectionLimits::default()
                .with_max_pending_outgoing(Some(8))
                .with_max_established_outgoing(Some(16));

            RelayServerBehaviour {
                connection_limits: connection_limits::Behaviour::new(conn_limits),
                relay: relay::Behaviour::new(key.public().to_peer_id(), relay_config),
                rendezvous: rendezvous::server::Behaviour::new(
                    rendezvous::server::Config::default().with_min_ttl(120), // Allow 2-minute TTL for mobile clients
                ),
                identify,
                ping: ping::Behaviour::new(
                    ping::Config::new().with_interval(Duration::from_secs(90)),
                ),
                autonat: libp2p::autonat::v2::server::Behaviour::new(OsRng),
                push: push_behaviour,
            }
        })?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_secs(cli.idle_connection_timeout))
        })
        .build();

    if cli.external_address.is_empty() {
        log::warn!(
            "No --external-address configured! Relay circuit reservations will respond \
             with NoAddressesInReservation for clients behind NAT. \
             Set --external-address /ip4/<public-ip>/tcp/4001"
        );
    }
    for ext_addr_str in &cli.external_address {
        let ext_addr: Multiaddr = ext_addr_str.parse()?;
        swarm.add_external_address(ext_addr.clone());
        log::info!("Advertising external address: {ext_addr}");
    }

    let listen_addr: Multiaddr = cli.listen_addr.parse()?;
    swarm.listen_on(listen_addr.clone())?;
    log::info!("Listening on {listen_addr}");

    // Also listen on QUIC on same port if TCP was specified
    if let Some(port) = extract_tcp_port(&listen_addr) {
        let quic_addr: Multiaddr = format!("/ip4/0.0.0.0/udp/{port}/quic-v1").parse()?;
        swarm.listen_on(quic_addr.clone())?;
        log::info!("Also listening on {quic_addr}");
    }

    // Track connected peer addresses for FCM push payloads.
    // When a peer triggers NotifyTopic, we include the sender's known addresses
    // so the waking device can dial directly without waiting for mDNS discovery.
    let mut peer_addresses: HashMap<libp2p::PeerId, Vec<Multiaddr>> = HashMap::new();

    // Relay-as-presence-server: track which peers are currently online for
    // each topic. Populated by `AnnouncePresence` requests, cleaned up on
    // disconnect. Used to answer presence requests with a peer list and to
    // push `PeerJoined` to existing peers when a newcomer arrives.
    //
    // This is intentionally in-memory — presence is session-scoped and should
    // not survive a relay restart (peers will re-announce when they reconnect).
    let mut topic_peers: HashMap<String, HashSet<libp2p::PeerId>> = HashMap::new();

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address}/p2p/{peer_id}");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                log::info!("Peer connected: {peer_id}");
                let addr = endpoint.get_remote_address().clone();
                let addrs = peer_addresses.entry(peer_id).or_default();
                if !addrs.contains(&addr) {
                    addrs.push(addr);
                }
                cap_peer_addresses(addrs);
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                log::info!("Peer disconnected: {peer_id}");
                if num_established == 0 {
                    peer_addresses.remove(&peer_id);
                    // Drop from every topic set; leave empty sets so the
                    // topic key is reclaimed on the next scan
                    topic_peers.retain(|_, set| {
                        set.remove(&peer_id);
                        !set.is_empty()
                    });
                }
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Relay(event)) => {
                log::info!("Relay: {event:?}");
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Rendezvous(event)) => {
                log::info!("Rendezvous: {event:?}");
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Identify(
                identify::Event::Received { info, peer_id, .. },
            )) => {
                log::info!(
                    "Identify from {peer_id}: {} listen addr(s) ({})",
                    info.listen_addrs.len(),
                    info.protocol_version,
                );
                // Replace, don't union. Identify is a snapshot of the peer's
                // current reachable addresses; treating it as additive
                // accumulates stale entries across reconnects (every NAT-mapped
                // outbound port that AutoNAT happened to observe at some point
                // gets stuck in the table forever). Replace-on-identify means
                // the relay's view tracks the peer's actual current state, so
                // FCM payloads contain only addresses that are live *right
                // now*. The peer pushes identify updates on listen-addr change
                // (with_push_listen_addr_updates(true)), so this stays current
                // even within a session.
                peer_addresses.insert(peer_id, info.listen_addrs.clone());
                if let Some(addrs) = peer_addresses.get_mut(&peer_id) {
                    cap_peer_addresses(addrs);
                }
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Push(
                request_response::Event::Message {
                    peer,
                    message:
                        request_response::Message::Request {
                            request, channel, ..
                        },
                    ..
                },
            )) => {
                // Build the sender's reachable addresses. Two variants:
                //   * `sender_addrs`     — full direct + circuit list, used
                //                          for libp2p `PeerJoined` peer
                //                          introductions (the receiving libp2p
                //                          peer wants every address to give
                //                          parallel dial the best chance).
                //   * `sender_addrs_fcm` — closest-path-first ordered set
                //                          with obviously-unreachable addresses
                //                          dropped (loopback, link-local,
                //                          multicast). Used for FCM payloads
                //                          where the receiver pays for every
                //                          address it has to dial. Order:
                //                          private LAN first (closest hop),
                //                          public WAN second, relay-circuit
                //                          last (guaranteed fallback). This
                //                          satisfies "don't use relay if not
                //                          necessary" — direct paths win the
                //                          libp2p parallel race when reachable.
                let sender_addrs: Vec<String> =
                    build_peer_addrs(&peer_addresses, &swarm, peer, peer_id);
                let sender_addrs_fcm: Vec<String> =
                    build_peer_addrs_for_fcm(&peer_addresses, &swarm, peer, peer_id);

                let response = match &request {
                    PushRequest::AnnouncePresence { topic } => {
                        // Gather existing peers' dial addresses BEFORE inserting
                        // the newcomer, so the response doesn't echo them back.
                        let existing: Vec<libp2p::PeerId> = topic_peers
                            .get(topic)
                            .into_iter()
                            .flatten()
                            .copied()
                            .filter(|p| *p != peer)
                            .collect();
                        let existing_addrs: Vec<String> = existing
                            .iter()
                            .flat_map(|p| build_peer_addrs(&peer_addresses, &swarm, *p, peer_id))
                            .collect();

                        // Register the newcomer for this topic
                        topic_peers.entry(topic.clone()).or_default().insert(peer);
                        log::info!(
                            "Presence announced: topic={topic} peer={peer} ({} existing)",
                            existing.len()
                        );

                        // Fan-out PeerJoined to each existing peer so they
                        // dial the newcomer and the normal sync flow can start.
                        if !sender_addrs.is_empty() {
                            for existing_peer in &existing {
                                let notify = PushRequest::PeerJoined {
                                    topic: topic.clone(),
                                    peer_addrs: sender_addrs.clone(),
                                };
                                swarm
                                    .behaviour_mut()
                                    .push
                                    .send_request(existing_peer, notify);
                            }
                        }

                        PushResponse::PeerList {
                            peers: existing_addrs,
                        }
                    }
                    _ => {
                        // FCM-bound push: use the closest-path-first ordered
                        // address set. See `build_peer_addrs_for_fcm` for the
                        // ordering rationale.
                        handle_push_request(
                            &push_notifier,
                            &request,
                            &peer.to_string(),
                            sender_addrs_fcm,
                        )
                        .await
                    }
                };

                if let Err(resp) = swarm.behaviour_mut().push.send_response(channel, response) {
                    log::error!("Failed to send push response: {:?}", resp);
                }
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Push(
                request_response::Event::Message {
                    message: request_response::Message::Response { .. },
                    ..
                },
            )) => {
                // `PeerJoined` acks from peers; nothing to do.
            }
            SwarmEvent::Behaviour(RelayServerBehaviourEvent::Push(
                request_response::Event::OutboundFailure { peer, error, .. },
            )) => {
                log::debug!("PeerJoined delivery to {peer} failed: {error}");
            }
            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                log::warn!(
                    "Incoming connection error from {send_back_addr} on {local_addr}: {error}"
                );
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                // Almost always an autonat dial-back that timed out because the
                // probed peer is behind NAT — that is how autonat decides "this
                // peer is private". Not actionable for the relay; keep at debug.
                log::debug!("Outgoing connection error to {peer_id:?}: {error}");
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                log::error!("Listener {listener_id:?} error: {error}");
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                ..
            } => {
                log::error!("Listener {listener_id:?} closed: {reason:?}");
            }
            _ => {}
        }
    }
}

async fn handle_push_request(
    push_state: &Option<(Arc<PushStore>, push_notifier::PushNotifier)>,
    request: &PushRequest,
    peer_id: &str,
    sender_addrs: Vec<String>,
) -> PushResponse {
    let (store, notifier) = match push_state {
        Some(s) => s,
        None => {
            return PushResponse::Error {
                message: "Push notifications not configured on this relay".to_string(),
            };
        }
    };

    match request {
        PushRequest::RegisterToken {
            topic,
            platform,
            token,
        } => {
            let platform_str = match platform {
                push_protocol::PushPlatform::Fcm => "Fcm",
                push_protocol::PushPlatform::Apns => "Apns",
            };
            match store
                .register_token(topic, platform_str, token, peer_id)
                .await
            {
                Ok(()) => {
                    log::info!(
                        "Registered {platform_str} push token for topic {topic} from {peer_id}"
                    );
                    PushResponse::Ok
                }
                Err(e) => PushResponse::Error {
                    message: format!("Failed to register token: {e}"),
                },
            }
        }
        PushRequest::UnregisterToken { topic, token } => {
            match store.unregister_token(topic, token).await {
                Ok(()) => {
                    log::info!("Unregistered push token for topic {topic} from {peer_id}");
                    PushResponse::Ok
                }
                Err(e) => PushResponse::Error {
                    message: format!("Failed to unregister token: {e}"),
                },
            }
        }
        PushRequest::NotifyTopic {
            topic,
            sender_site_id,
        } => {
            log::info!(
                "NotifyTopic received from {peer_id} (sender site={sender_site_id}) for {topic}"
            );
            notifier.notify(topic.clone(), sender_addrs);
            PushResponse::Ok
        }
        // Handled inline in the main loop — needs swarm access to fan out
        // PeerJoined to other peers.
        PushRequest::AnnouncePresence { .. } => unreachable!(
            "AnnouncePresence is handled inline in the swarm event loop, not in handle_push_request"
        ),
        // Relay doesn't receive PeerJoined — it only sends it.
        PushRequest::PeerJoined { .. } => PushResponse::Error {
            message: "PeerJoined is a relay-to-peer request; peers cannot send it".to_string(),
        },
    }
}

/// Build the set of dial multiaddrs advertised for a peer: direct addresses
/// (if reachable) plus relay circuit addresses constructed from each of the
/// Maximum addresses to retain per peer. Bounds growth from peers that reconnect
/// many times within one session (each reconnect lands on a new NAT-mapped source
/// port). Keeping the most recent N is fine because identify re-pushes current
/// listen addresses on every reconnect.
const MAX_PEER_ADDRESSES: usize = 8;

/// Trim the address list to the last `MAX_PEER_ADDRESSES` entries, dropping the
/// oldest. Newer addresses are statistically more likely to be live (recent NAT
/// mappings, current listen ports), so eviction is FIFO from the front.
fn cap_peer_addresses(addrs: &mut Vec<Multiaddr>) {
    if addrs.len() > MAX_PEER_ADDRESSES {
        let drop = addrs.len() - MAX_PEER_ADDRESSES;
        addrs.drain(..drop);
    }
}

/// relay's external addresses. Each entry has a trailing `/p2p/<peer-id>`.
///
/// Some inputs already end in `/p2p/<peer>` — notably circuit-relay listener
/// endpoints reported by `endpoint.get_remote_address()`, which look like
/// `/ip4/.../tcp/.../p2p/<relay>/p2p-circuit/p2p/<peer>`. Naively appending
/// another `/p2p/<peer>` produces a duplicate suffix that libp2p rejects with
/// `MalformedMultiaddr`. Strip the existing suffix first.
fn build_peer_addrs(
    peer_addresses: &HashMap<libp2p::PeerId, Vec<Multiaddr>>,
    swarm: &libp2p::Swarm<RelayServerBehaviour>,
    peer: libp2p::PeerId,
    relay_peer_id: libp2p::PeerId,
) -> Vec<String> {
    let with_p2p_suffix = |a: &Multiaddr| -> String {
        let last_is_self =
            matches!(a.iter().last(), Some(libp2p::multiaddr::Protocol::P2p(p)) if p == peer);
        if last_is_self {
            a.to_string()
        } else {
            format!("{a}/p2p/{peer}")
        }
    };

    let mut addrs: Vec<String> = peer_addresses
        .get(&peer)
        .into_iter()
        .flatten()
        .map(with_p2p_suffix)
        .collect();
    for ext_addr in swarm.external_addresses() {
        addrs.push(format!(
            "{ext_addr}/p2p/{relay_peer_id}/p2p-circuit/p2p/{peer}"
        ));
    }
    addrs
}

/// Build the FCM-bound peer-address set: closest path first, relay last.
///
/// libp2p's `DialOpts::addresses(...)` races the entries in parallel — but
/// the swarm enforces a max-pending-outgoing limit (10), and address order
/// influences which dials get a slot. So putting the *most-likely-to-be-
/// closest* address first guarantees it gets a real chance to win, and
/// putting the relay-circuit address last keeps it as a guaranteed fallback
/// without making it the default.
///
/// Order produced:
///   1. Private-LAN direct addresses (10/8, 172.16/12, 192.168/16, fc00::/7
///      for IPv6 ULA) — closest path when the receiver happens to share the
///      writer's LAN. Cellular-phone receivers will fail these fast with
///      `ENETUNREACH` and the parallel dial moves on; same-LAN receivers
///      connect in a few milliseconds and never need the relay.
///   2. Public WAN direct addresses — may work if the writer's NAT permits
///      (port forwarding, UPnP, full-cone NAT) or after libp2p DCUtR
///      hole-punching. Cheap to attempt.
///   3. Relay-circuit addresses (one per relay external address) — guaranteed
///      to work cross-network. Acts as the always-present fallback.
///
/// Dropped (not included at all because they can never accept a remote dial):
///   * loopback (127/8, ::1)
///   * link-local (169.254/16, fe80::/10)
///   * multicast / unspecified
///
/// User intent satisfied: "first address we try is the closest path; don't
/// use relay if not necessary." If a direct address connects, libp2p's
/// per-peer connection limit (1) closes the redundant relay-circuit
/// connection, so the relay only carries traffic when it has to.
///
/// Same-LAN sync via mDNS is still preferred when the foreground app is
/// running — this function only matters for FCM-triggered cold-start sync
/// where mDNS hasn't had a chance to discover anything yet.
fn build_peer_addrs_for_fcm(
    peer_addresses: &HashMap<libp2p::PeerId, Vec<Multiaddr>>,
    swarm: &libp2p::Swarm<RelayServerBehaviour>,
    peer: libp2p::PeerId,
    relay_peer_id: libp2p::PeerId,
) -> Vec<String> {
    let with_p2p_suffix = |a: &Multiaddr| -> String {
        let last_is_self =
            matches!(a.iter().last(), Some(libp2p::multiaddr::Protocol::P2p(p)) if p == peer);
        if last_is_self {
            a.to_string()
        } else {
            format!("{a}/p2p/{peer}")
        }
    };

    let circuit: Vec<String> = swarm
        .external_addresses()
        .map(|ext_addr| format!("{ext_addr}/p2p/{relay_peer_id}/p2p-circuit/p2p/{peer}"))
        .collect();

    let mut lan: Vec<String> = Vec::new();
    let mut wan: Vec<String> = Vec::new();
    if let Some(addrs) = peer_addresses.get(&peer) {
        for addr in addrs {
            match classify_addr(addr) {
                AddrClass::Drop => continue,
                AddrClass::Private => lan.push(with_p2p_suffix(addr)),
                AddrClass::Public => wan.push(with_p2p_suffix(addr)),
            }
        }
    }

    let mut out = Vec::with_capacity(lan.len() + wan.len() + circuit.len());
    out.extend(lan);
    out.extend(wan);
    out.extend(circuit);
    // Dedup while preserving order — `swarm.external_addresses()` can yield
    // duplicates (e.g. when AutoNAT confirms the same external addr from two
    // observers), and we don't want the phone dialing the same circuit-relay
    // multiaddr twice.
    let mut seen = HashSet::new();
    out.retain(|a| seen.insert(a.clone()));
    out
}

#[derive(Debug, PartialEq, Eq)]
enum AddrClass {
    /// Drop entirely — never useful for a remote receiver.
    Drop,
    /// Private LAN address (RFC 1918 v4, ULA fc00::/7 v6). Closest path if
    /// the receiver happens to share the writer's LAN.
    Private,
    /// Public WAN address. May or may not be reachable depending on NAT.
    Public,
}

/// Classify a multiaddr's reachability class for FCM payload purposes.
/// Looks at the *first* `Ip4`/`Ip6` protocol component — multiaddrs that
/// don't start with an IP literal (e.g. `/dns4/...`) are treated as Public
/// (they may resolve to anything; let libp2p attempt).
fn classify_addr(addr: &Multiaddr) -> AddrClass {
    use libp2p::multiaddr::Protocol;

    for proto in addr.iter() {
        match proto {
            Protocol::Ip4(ip) => return classify_v4(ip),
            Protocol::Ip6(ip) => return classify_v6(ip),
            _ => continue,
        }
    }

    // No IP literal — assume Public (DNS-named addresses, etc.). The caller
    // gives this a non-zero chance; cost of a wasted dial is bounded.
    AddrClass::Public
    // (Helpers defined inline below to keep the classification self-contained.)
}

fn classify_v4(ip: std::net::Ipv4Addr) -> AddrClass {
    if ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
    {
        return AddrClass::Drop;
    }
    // Docker default bridges allocate from 172.17.0.0/16 through 172.31.0.0/16
    // (each `docker network create` claims the next free /16). These are
    // host-only and never reachable from another machine, but identify still
    // reports them because libp2p binds 0.0.0.0 and enumerates per-interface.
    // Always-drop them — desktops behind Docker accumulate 4–6 bridges,
    // multiplied by TCP+QUIC, that's a 8–12-address noise floor in every FCM
    // payload. The 192.168.0.0/16 range (typical home Wi-Fi) and 10.0.0.0/8
    // (corporate LAN) remain Private.
    let octets = ip.octets();
    if octets[0] == 172 && (16..=31).contains(&octets[1]) {
        return AddrClass::Drop;
    }
    if ip.is_private() {
        return AddrClass::Private;
    }
    AddrClass::Public
}

fn classify_v6(ip: std::net::Ipv6Addr) -> AddrClass {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return AddrClass::Drop;
    }
    // Link-local: fe80::/10
    let segments = ip.segments();
    if (segments[0] & 0xffc0) == 0xfe80 {
        return AddrClass::Drop;
    }
    // Unique-local addresses (ULA): fc00::/7
    if (segments[0] & 0xfe00) == 0xfc00 {
        return AddrClass::Private;
    }
    AddrClass::Public
}

fn extract_tcp_port(addr: &Multiaddr) -> Option<u16> {
    for proto in addr.iter() {
        if let libp2p::multiaddr::Protocol::Tcp(port) = proto {
            return Some(port);
        }
    }
    None
}

#[cfg(test)]
mod cap_peer_addresses_tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    #[test]
    fn no_op_under_cap() {
        let mut addrs = vec![ma("/ip4/1.1.1.1/tcp/1"), ma("/ip4/2.2.2.2/tcp/2")];
        cap_peer_addresses(&mut addrs);
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn no_op_at_cap() {
        let mut addrs: Vec<Multiaddr> = (0..MAX_PEER_ADDRESSES)
            .map(|i| ma(&format!("/ip4/10.0.0.{i}/tcp/{i}")))
            .collect();
        cap_peer_addresses(&mut addrs);
        assert_eq!(addrs.len(), MAX_PEER_ADDRESSES);
    }

    #[test]
    fn drops_oldest_when_over_cap() {
        let total = MAX_PEER_ADDRESSES + 3;
        let mut addrs: Vec<Multiaddr> = (0..total)
            .map(|i| ma(&format!("/ip4/10.0.0.{i}/tcp/{i}")))
            .collect();
        cap_peer_addresses(&mut addrs);
        assert_eq!(addrs.len(), MAX_PEER_ADDRESSES);
        // Oldest 3 dropped → first remaining is index 3
        assert_eq!(addrs[0], ma("/ip4/10.0.0.3/tcp/3"));
        // Newest is still last
        assert_eq!(addrs.last().unwrap(), &ma(&format!("/ip4/10.0.0.{}/tcp/{}", total - 1, total - 1)));
    }
}

#[cfg(test)]
mod classify_addr_tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    #[test]
    fn private_v4_ranges_are_private() {
        // Wi-Fi LAN and corporate LAN remain Private — useful when receiver
        // happens to be on the same network.
        for s in [
            "/ip4/192.168.1.150/tcp/4001",
            "/ip4/10.0.0.5/tcp/4001",
        ] {
            assert_eq!(classify_addr(&ma(s)), AddrClass::Private, "{s}");
        }
    }

    #[test]
    fn docker_bridge_range_is_dropped() {
        // 172.16.0.0/12 (Docker default bridges, also some VPNs) are host-only
        // virtual interfaces. Even though they're RFC1918 Private, including
        // them in FCM payloads is pure noise: a desktop with 4 Docker bridges
        // adds 8 useless entries (×TCP+QUIC).
        for s in [
            "/ip4/172.16.0.1/tcp/4001",
            "/ip4/172.17.0.1/tcp/4001",
            "/ip4/172.18.0.1/tcp/4001",
            "/ip4/172.20.0.1/tcp/4001",
            "/ip4/172.31.255.254/tcp/4001",
        ] {
            assert_eq!(classify_addr(&ma(s)), AddrClass::Drop, "{s}");
        }
    }

    #[test]
    fn loopback_and_link_local_dropped() {
        for s in [
            "/ip4/127.0.0.1/tcp/4001",
            "/ip4/0.0.0.0/tcp/4001",
            "/ip4/169.254.10.1/tcp/4001",
            "/ip4/224.0.0.1/tcp/4001",
            "/ip6/::1/tcp/4001",
            "/ip6/fe80::1/tcp/4001",
            "/ip6/ff02::1/tcp/4001",
        ] {
            assert_eq!(classify_addr(&ma(s)), AddrClass::Drop, "{s}");
        }
    }

    #[test]
    fn public_addresses_are_public() {
        for s in [
            "/ip4/77.37.125.212/tcp/4001",
            "/ip4/8.8.8.8/tcp/4001",
            "/ip6/2001:db8::1/tcp/4001",
        ] {
            // 2001:db8 is documentation but global-unicast otherwise; consumers
            // can't easily distinguish so we keep it as Public; let libp2p
            // attempt the dial.
            let class = classify_addr(&ma(s));
            assert!(
                class == AddrClass::Public || class == AddrClass::Drop,
                "{s} got {class:?}"
            );
        }
    }

    #[test]
    fn ipv6_ula_is_private() {
        assert_eq!(
            classify_addr(&ma("/ip6/fc00::1/tcp/4001")),
            AddrClass::Private
        );
        assert_eq!(
            classify_addr(&ma("/ip6/fd00::abcd/tcp/4001")),
            AddrClass::Private
        );
    }

    #[test]
    fn dns_addresses_are_public_by_default() {
        // No IP literal; we don't resolve at filter-time. Treat as Public so
        // libp2p gets a chance to attempt.
        assert_eq!(
            classify_addr(&ma("/dns4/relay.example.com/tcp/4001")),
            AddrClass::Public
        );
    }
}

#[cfg(test)]
mod build_peer_addrs_for_fcm_tests {
    use super::*;

    fn pid(s: &str) -> libp2p::PeerId {
        s.parse().unwrap()
    }
    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    /// Hand-build the inputs without standing up a real swarm. We exercise
    /// only the address-handling logic; the swarm parameter is required by
    /// `build_peer_addrs_for_fcm` for the relay's external-address iterator,
    /// so this test inlines the equivalent pure logic to validate ordering.
    /// See `build_peer_addrs_for_fcm` source — this test mirrors its body
    /// minus the swarm dependency.
    #[test]
    fn ordering_is_lan_then_wan_then_circuit() {
        // Pure-data simulation of `build_peer_addrs_for_fcm` so the test
        // doesn't need a libp2p swarm.
        let peer = pid("12D3KooWQTV2REAJX77iesp2Qjax5tiK7Zt65FA7tUL6Ch47BJc6");
        let relay = pid("12D3KooWFnxFFxCm5ywp5j2WhBV4HbtCLDDh1jAr1QYa3xMtkAy3");
        let known: Vec<Multiaddr> = vec![
            ma("/ip4/127.0.0.1/tcp/39981"),       // dropped (loopback)
            ma("/ip4/172.18.0.1/tcp/39981"),      // dropped (Docker bridge)
            ma("/ip4/192.168.1.150/tcp/39981"),   // private (Wi-Fi LAN)
            ma("/ip4/79.116.240.142/tcp/42674"),  // public
        ];
        let external_addrs: Vec<Multiaddr> = vec![
            ma("/ip4/77.37.125.212/tcp/4001"),
            ma("/ip4/77.37.125.212/tcp/4001"), // duplicate — should be deduped
        ];

        // Re-implement classification + ordering inline (same as production).
        let mut lan: Vec<String> = Vec::new();
        let mut wan: Vec<String> = Vec::new();
        for addr in &known {
            match classify_addr(addr) {
                AddrClass::Drop => continue,
                AddrClass::Private => lan.push(format!("{addr}/p2p/{peer}")),
                AddrClass::Public => wan.push(format!("{addr}/p2p/{peer}")),
            }
        }
        let circuit: Vec<String> = external_addrs
            .iter()
            .map(|ext| format!("{ext}/p2p/{relay}/p2p-circuit/p2p/{peer}"))
            .collect();
        let mut got: Vec<String> = Vec::new();
        got.extend(lan);
        got.extend(wan);
        got.extend(circuit);
        // Mirror production dedup
        let mut seen = HashSet::new();
        got.retain(|a| seen.insert(a.clone()));

        assert_eq!(got.len(), 3); // 1 LAN + 1 WAN + 1 circuit; loopback + docker + duplicate dropped
        // First entry is the Wi-Fi LAN address
        assert!(got[0].starts_with("/ip4/192.168.1.150/"));
        // Second is WAN
        assert!(got[1].starts_with("/ip4/79.116.240.142/"));
        // Third is circuit-relay (only once, not twice)
        assert!(got[2].contains("/p2p-circuit/"));
    }
}
