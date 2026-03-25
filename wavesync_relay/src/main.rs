mod push_notifier;
mod push_protocol;
mod push_sender;
mod push_store;

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
    #[arg(long, default_value = "/ip4/0.0.0.0/tcp/4001")]
    listen_addr: String,

    /// Path to a file containing the persistent identity keypair.
    /// If the file does not exist, a new keypair is generated and saved.
    #[arg(long)]
    identity_file: Option<PathBuf>,

    /// Base64-encoded protobuf identity keypair. Overrides --identity-file.
    /// Generate with: wavesync-relay --generate-identity
    #[arg(long, env = "IDENTITY_KEYPAIR")]
    identity_keypair: Option<String>,

    /// Print a new base64-encoded identity keypair and its PeerId, then exit.
    #[arg(long)]
    generate_identity: bool,

    /// Maximum number of relay circuits (0 = unlimited)
    #[arg(long, default_value_t = 256)]
    max_circuits: usize,

    /// Maximum circuit duration in seconds (default: 3600 = 1 hour)
    #[arg(long, default_value_t = 3600)]
    max_circuit_duration: u64,

    /// Maximum bytes per circuit (0 = unlimited, default: unlimited)
    #[arg(long, default_value_t = 0)]
    max_circuit_bytes: u64,

    /// Path to the push token SQLite database (enables push notifications)
    #[arg(long, env = "PUSH_DB")]
    push_db: Option<String>,

    /// Path to FCM service account JSON file
    #[arg(long, env = "FCM_CREDENTIALS")]
    fcm_credentials: Option<PathBuf>,

    /// Path to APNs .p8 key file
    #[arg(long, env = "APNS_KEY_FILE")]
    apns_key_file: Option<PathBuf>,

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
    #[arg(long)]
    apns_sandbox: bool,

    /// Push notification debounce window in seconds (default: 5)
    #[arg(long, default_value_t = 5)]
    push_debounce_secs: u64,

    /// External address to advertise (repeatable, e.g. /ip4/77.37.125.212/tcp/4001).
    /// Required when running behind NAT or in Docker.
    #[arg(long, env = "EXTERNAL_ADDRESS")]
    external_address: Vec<String>,
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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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

    let keypair = if let Some(ref b64) = cli.identity_keypair {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .expect("IDENTITY_KEYPAIR is not valid base64");
        identity::Keypair::from_protobuf_encoding(&bytes)
            .expect("IDENTITY_KEYPAIR is not a valid protobuf-encoded keypair")
    } else if let Some(ref path) = cli.identity_file {
        load_or_generate_keypair(path)
    } else {
        identity::Keypair::generate_ed25519()
    };

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

        let fcm_config = cli.fcm_credentials.as_ref().map(|path| {
            let json = std::fs::read_to_string(path).expect("Failed to read FCM credentials file");
            // Extract project_id from the service account JSON
            let sa: serde_json::Value =
                serde_json::from_str(&json).expect("Invalid FCM credentials JSON");
            let project_id = sa["project_id"]
                .as_str()
                .expect("Missing project_id in FCM credentials")
                .to_string();
            FcmConfig {
                project_id,
                service_account_json: json,
            }
        });

        let apns_config = if let (Some(key_file), Some(key_id), Some(team_id), Some(bundle_id)) = (
            &cli.apns_key_file,
            &cli.apns_key_id,
            &cli.apns_team_id,
            &cli.apns_bundle_id,
        ) {
            let key_pem = std::fs::read_to_string(key_file).expect("Failed to read APNs key file");
            Some(ApnsConfig {
                key_pem,
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
                ping: ping::Behaviour::default(),
                autonat: libp2p::autonat::v2::server::Behaviour::new(OsRng),
                push: push_behaviour,
            }
        })?
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(std::time::Duration::from_secs(60))
        })
        .build();

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

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                log::info!("Listening on {address}/p2p/{peer_id}");
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                log::info!("Peer connected: {peer_id}");
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                log::info!("Peer disconnected: {peer_id}");
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
                let response =
                    handle_push_request(&push_notifier, &request, &peer.to_string()).await;
                if let Err(resp) = swarm.behaviour_mut().push.send_response(channel, response) {
                    log::error!("Failed to send push response: {:?}", resp);
                }
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
                log::warn!("Outgoing connection error to {peer_id:?}: {error}");
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
            log::debug!("Topic notification from {sender_site_id} for {topic}");
            notifier.notify(topic.clone());
            PushResponse::Ok
        }
    }
}

fn extract_tcp_port(addr: &Multiaddr) -> Option<u16> {
    for proto in addr.iter() {
        if let libp2p::multiaddr::Protocol::Tcp(port) = proto {
            return Some(port);
        }
    }
    None
}
