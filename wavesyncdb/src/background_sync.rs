//! One-shot background sync for push notification wake-up services.
//!
//! When a mobile app is fully closed and receives an FCM/APNs silent push,
//! the OS wakes a native service (Android `FirebaseMessagingService` or iOS
//! background notification handler). That service calls [`background_sync()`]
//! to start the sync engine, pull changes from peers, and shut down.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use wavesyncdb::background_sync::background_sync;
//!
//! let result = background_sync("sqlite:///data/data/com.app/app.db?mode=rwc", Duration::from_secs(30)).await?;
//! match result {
//!     BackgroundSyncResult::Synced { peers_synced } => log::info!("Synced with {peers_synced} peers"),
//!     BackgroundSyncResult::TimedOut { peers_synced } => log::warn!("Timeout, synced with {peers_synced}"),
//!     BackgroundSyncResult::NoPeers => log::warn!("No peers found"),
//! }
//! ```

use std::collections::HashSet;
use std::time::Duration;

use crate::WaveSyncDbBuilder;
use crate::connection::SyncConfig;
use crate::network_status::NetworkEvent;

/// Result of a background sync operation.
#[derive(Debug)]
pub enum BackgroundSyncResult {
    /// Successfully synced with at least one peer.
    Synced { peers_synced: usize },
    /// No peers were found within the timeout.
    NoPeers,
    /// Timed out before all peers finished syncing.
    TimedOut { peers_synced: usize },
}

/// Errors that can occur during background sync.
#[derive(Debug)]
pub enum BackgroundSyncError {
    /// No config file found — the app must have been built with [`WaveSyncDbBuilder`] at least once.
    ConfigNotFound(String),
    /// Config file is invalid or corrupted.
    ConfigInvalid(String),
    /// Database connection or setup error.
    DatabaseError(String),
    /// Schema registry could not be initialized.
    RegistryError(String),
}

impl std::fmt::Display for BackgroundSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigNotFound(msg) => write!(f, "Config not found: {msg}"),
            Self::ConfigInvalid(msg) => write!(f, "Invalid config: {msg}"),
            Self::DatabaseError(msg) => write!(f, "Database error: {msg}"),
            Self::RegistryError(msg) => write!(f, "Registry error: {msg}"),
        }
    }
}

impl std::error::Error for BackgroundSyncError {}

/// Performs a one-shot background sync.
///
/// Reads the saved config from the database directory, builds a [`WaveSyncDb`](crate::WaveSyncDb),
/// requests a full sync, waits for peer sync events (or timeout), then gracefully shuts down.
///
/// The config file is written automatically by [`WaveSyncDbBuilder::build()`] — the app must
/// have been started at least once before background sync can work.
///
/// # Arguments
///
/// * `database_url` — SQLite URL (e.g., `"sqlite:///data/data/com.app/app.db?mode=rwc"`)
/// * `timeout` — Maximum time to wait for sync to complete
pub async fn background_sync(
    database_url: &str,
    timeout: Duration,
) -> Result<BackgroundSyncResult, BackgroundSyncError> {
    background_sync_with_peers(database_url, timeout, &[]).await
}

/// Performs a one-shot background sync with optional direct peer addresses.
///
/// When `peer_addrs` is non-empty, these are added as bootstrap peers so the
/// engine dials them directly without waiting for mDNS or relay discovery.
/// This is used by the FCM push handler which receives the sender's addresses
/// from the relay server.
pub async fn background_sync_with_peers(
    database_url: &str,
    timeout: Duration,
    peer_addrs: &[String],
) -> Result<BackgroundSyncResult, BackgroundSyncError> {
    // 1. Load saved config
    let config = SyncConfig::load(database_url).map_err(|e| {
        if e.contains("Failed to read") {
            BackgroundSyncError::ConfigNotFound(e)
        } else {
            BackgroundSyncError::ConfigInvalid(e)
        }
    })?;

    // 2. Reconstruct the builder
    let mut builder = WaveSyncDbBuilder::new(database_url, &config.topic);

    if let Some(ref relay) = config.relay_server {
        builder = builder.with_relay_server(relay);
    }
    if let Some(ref passphrase) = config.passphrase {
        builder = builder.with_passphrase(passphrase);
    }
    if let Some(ref rendezvous) = config.rendezvous_server {
        builder = builder.with_rendezvous_server(rendezvous);
    }
    for peer in &config.bootstrap_peers {
        builder = builder.with_bootstrap_peer(peer);
    }
    if let Some(ref api_key) = config.api_key
        && let Some(ref relay) = config.relay_server
    {
        builder = WaveSyncDbBuilder::new(database_url, &config.topic).managed_relay(relay, api_key);
        // Re-apply other settings
        if let Some(ref passphrase) = config.passphrase {
            builder = builder.with_passphrase(passphrase);
        }
        if let Some(ref rendezvous) = config.rendezvous_server {
            builder = builder.with_rendezvous_server(rendezvous);
        }
        for peer in &config.bootstrap_peers {
            builder = builder.with_bootstrap_peer(peer);
        }
    }
    if config.ipv6 {
        builder = builder.with_ipv6(true);
    }

    // Add dynamic peer addresses from FCM payload (direct dial, skips discovery)
    for addr in peer_addrs {
        builder = builder.with_bootstrap_peer(addr);
    }

    // Read push token from config so the engine registers it with the relay.
    if let (Some(platform), Some(token)) = (&config.push_platform, &config.push_token) {
        builder = builder.with_push_token(platform, token);
    }

    // 3. Build the DB (starts engine)
    log::info!("background_sync: building engine for topic={}", config.topic);
    let db = builder
        .build()
        .await
        .map_err(|e| BackgroundSyncError::DatabaseError(e.to_string()))?;

    // Subscribe to events BEFORE registry_ready so we don't miss PeerConnected
    // events that fire immediately when sync_all_known_peers() runs.
    let mut events = db.network_event_rx();

    // 4. Initialize schema registry (tables already exist, but registry needs populating)
    if let Some(ref crate_name) = config.crate_name {
        // Dioxus path: auto-discover entities via inventory
        log::info!("background_sync: using crate_name={crate_name} for schema discovery");
        db.get_schema_registry(crate_name)
            .sync()
            .await
            .map_err(|e| BackgroundSyncError::RegistryError(e.to_string()))?;
    } else if let Some(ref tables) = config.registered_tables {
        // FFI/RN path: re-register tables from persisted metadata
        log::info!("background_sync: re-registering {} table(s) from config", tables.len());
        for meta in tables {
            log::debug!("background_sync: registering table={}", meta.table_name);
            db.register_table(meta.clone());
        }
        db.registry_ready();
    } else {
        // No schema info — signal registry ready anyway so engine can proceed
        log::warn!("background_sync: no crate_name or registered_tables in config — registry is empty");
        db.registry_ready();
    }

    // 5. Wait for peer discovery, then sync.
    // Don't call request_full_sync() immediately — peers haven't been discovered yet.
    // Instead, wait for PeerConnected events and trigger sync when peers appear.
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    let mut synced_peers = HashSet::new();
    let mut saw_any_peer = false;
    let mut sync_requested = false;

    loop {
        tokio::select! {
            _ = &mut deadline => {
                break;
            }
            event = events.recv() => {
                match event {
                    Ok(NetworkEvent::PeerConnected(_)) => {
                        saw_any_peer = true;
                        // A peer connected — now request sync if we haven't yet
                        if !sync_requested {
                            db.request_full_sync();
                            sync_requested = true;
                        }
                    }
                    Ok(NetworkEvent::RelayStatusChanged(crate::RelayStatus::Listening)) => {
                        // Relay is ready — request sync to discover relay-connected peers
                        if !sync_requested {
                            db.request_full_sync();
                            sync_requested = true;
                        }
                    }
                    Ok(NetworkEvent::PeerSynced { peer_id, .. }) => {
                        synced_peers.insert(peer_id);
                        // Give a brief window for additional peers to sync
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    _ => continue,
                }
            }
        }
    }

    // 7. Graceful shutdown
    db.shutdown().await;

    // 8. Return result
    let peers_synced = synced_peers.len();
    if peers_synced > 0 {
        Ok(BackgroundSyncResult::Synced { peers_synced })
    } else if saw_any_peer {
        Ok(BackgroundSyncResult::TimedOut { peers_synced: 0 })
    } else {
        Ok(BackgroundSyncResult::NoPeers)
    }
}
