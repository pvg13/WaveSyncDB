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

    /// Push notification cooldown window in seconds (default: 2).
    /// First notification fires immediately; subsequent ones within this window are batched.
    #[arg(long, env = "PUSH_DEBOUNCE_SECS", default_value_t = 2)]
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
                peer_addresses.entry(peer_id).or_default().push(addr);
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
                log::debug!("Identify from {peer_id}: {}", info.protocol_version);
                // Update peer addresses from identify info (includes listen addresses)
                let addrs = peer_addresses.entry(peer_id).or_default();
                for addr in &info.listen_addrs {
                    if !addrs.contains(addr) {
                        addrs.push(addr.clone());
                    }
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
                // Build the sender's reachable addresses (used in both FCM
                // payloads and presence introductions).
                let sender_addrs: Vec<String> =
                    build_peer_addrs(&peer_addresses, &swarm, peer, peer_id);

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
                        handle_push_request(
                            &push_notifier,
                            &request,
                            &peer.to_string(),
                            sender_addrs,
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
/// relay's external addresses. Each entry has a trailing `/p2p/<peer-id>`.
fn build_peer_addrs(
    peer_addresses: &HashMap<libp2p::PeerId, Vec<Multiaddr>>,
    swarm: &libp2p::Swarm<RelayServerBehaviour>,
    peer: libp2p::PeerId,
    relay_peer_id: libp2p::PeerId,
) -> Vec<String> {
    let mut addrs: Vec<String> = peer_addresses
        .get(&peer)
        .into_iter()
        .flatten()
        .map(|a| format!("{a}/p2p/{peer}"))
        .collect();
    for ext_addr in swarm.external_addresses() {
        addrs.push(format!(
            "{ext_addr}/p2p/{relay_peer_id}/p2p-circuit/p2p/{peer}"
        ));
    }
    addrs
}

fn extract_tcp_port(addr: &Multiaddr) -> Option<u16> {
    for proto in addr.iter() {
        if let libp2p::multiaddr::Protocol::Tcp(port) = proto {
            return Some(port);
        }
    }
    None
}
