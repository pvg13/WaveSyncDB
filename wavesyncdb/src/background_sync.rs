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
    // Per-stage timing. When a sync round is slow (sometimes hits the 25s
    // timeout while typical runs are 2–3s), the question is always "where
    // did the time go". These markers let logcat show the answer:
    //
    //   bg_sync stage=config_loaded elapsed_ms=N
    //   bg_sync stage=engine_built elapsed_ms=N
    //   bg_sync stage=registry_ready elapsed_ms=N
    //   bg_sync stage=relay_listening elapsed_ms=N      (first time)
    //   bg_sync stage=first_peer elapsed_ms=N           (first time)
    //   bg_sync stage=sync_requested elapsed_ms=N       (first time)
    //   bg_sync stage=first_peer_synced elapsed_ms=N    (first time)
    //   bg_sync stage=shutdown_started elapsed_ms=N
    //   bg_sync stage=done elapsed_ms=N result=…
    //
    // Each is at info so they're visible without a debug RUST_LOG override.
    let t_start = std::time::Instant::now();
    let log_stage = |stage: &str| {
        log::info!(
            "bg_sync stage={stage} elapsed_ms={}",
            t_start.elapsed().as_millis()
        );
    };

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
    if !config.relay_fallbacks.is_empty() {
        builder = builder.with_relay_fallbacks(&config.relay_fallbacks);
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
        if !config.relay_fallbacks.is_empty() {
            builder = builder.with_relay_fallbacks(&config.relay_fallbacks);
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
    }
    if config.ipv6 {
        builder = builder.with_ipv6(true);
    }

    // Add dynamic peer addresses from FCM payload (direct dial, skips discovery)
    for addr in peer_addrs {
        builder = builder.with_bootstrap_peer(addr);
    }
    log_stage("config_loaded");

    // 3. Build the DB (starts engine)
    let db = builder
        .build()
        .await
        .map_err(|e| BackgroundSyncError::DatabaseError(e.to_string()))?;
    log_stage("engine_built");

    // 4. Initialize schema registry (tables already exist, but registry needs populating)
    if let Some(ref crate_name) = config.crate_name {
        db.get_schema_registry(crate_name)
            .sync()
            .await
            .map_err(|e| BackgroundSyncError::RegistryError(e.to_string()))?;
    } else {
        // No crate name saved — signal registry ready anyway so engine can proceed
        db.registry_ready();
    }
    log_stage("registry_ready");

    // 4b. Rejoin every additional group recorded in the config so the wake
    // syncs ALL of this device's groups, not just the default one. Each runs on
    // the same engine/swarm; each needs its own schema registered to sync.
    // A single group's failure must not abort the others or the default sync.
    // The handles are held in `_group_handles` for the rest of the function so
    // the groups stay joined while we wait for sync below.
    let mut _group_handles: Vec<crate::WaveSyncDb> = Vec::new();
    for group in &config.groups {
        match db.node().join_group(&group.user_topic, &group.passphrase).await {
            Ok(group_db) => {
                if let Some(ref crate_name) = config.crate_name {
                    if let Err(e) = group_db.get_schema_registry(crate_name).sync().await {
                        log::warn!(
                            "bg_sync: schema sync failed for group '{}': {e}",
                            group.user_topic
                        );
                    }
                } else {
                    group_db.registry_ready();
                }
                _group_handles.push(group_db);
            }
            Err(e) => {
                log::warn!("bg_sync: failed to rejoin group '{}': {e}", group.user_topic);
            }
        }
    }
    if !config.groups.is_empty() {
        log_stage("groups_rejoined");
    }

    // 5. Wait for peer discovery and let the engine sync on its own.
    //
    // We deliberately do NOT call request_full_sync() here. With peer versions
    // hydrated from disk, the automatic version-vector sync the engine fires on
    // ConnectionEstablished / registry-ready pulls only the delta since our last
    // sync — fast, and the whole point of waking incrementally. Forcing a full
    // sync would re-pull the entire database on every wake.
    //
    // Two timers bound the wait:
    //   * COMPLETION_GRACE — after the first PeerSynced, linger briefly so a
    //     second/third peer can also finish before we tear down.
    //   * FALLBACK_AFTER — if a peer connected but no incremental sync has
    //     completed by then (first-ever contact, or our persisted view of the
    //     peer was somehow ahead), ask for a full sync once. Preserves new-peer
    //     onboarding (db_version=0 semantics) without making it the default.
    // After the first peer syncs, linger for additional peers/groups before we
    // tear down. With extra groups joined, give their (fast, incremental)
    // version-vector round-trips room to land too — they share the connections
    // but emit their own PeerSynced events.
    let completion_grace = if config.groups.is_empty() {
        Duration::from_millis(500)
    } else {
        Duration::from_millis(1500)
    };
    const FALLBACK_AFTER: Duration = Duration::from_secs(5);

    let mut events = db.network_event_rx();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let fallback = tokio::time::sleep(FALLBACK_AFTER);
    tokio::pin!(fallback);

    let mut synced_peers = HashSet::new();
    let mut saw_any_peer = false;
    let mut full_sync_fallback_done = false;
    let mut logged_relay_listening = false;
    let mut logged_first_peer = false;

    loop {
        tokio::select! {
            _ = &mut deadline => {
                log_stage("timeout");
                break;
            }
            _ = &mut fallback,
                if saw_any_peer && synced_peers.is_empty() && !full_sync_fallback_done =>
            {
                // A peer connected but no incremental sync completed in time —
                // fall back to a one-shot full sync before the hard timeout.
                log_stage("full_sync_fallback");
                db.request_full_sync();
                full_sync_fallback_done = true;
            }
            event = events.recv() => {
                match event {
                    Ok(NetworkEvent::PeerConnected(_)) => {
                        if !logged_first_peer {
                            log_stage("first_peer");
                            logged_first_peer = true;
                        }
                        saw_any_peer = true;
                        // No explicit sync trigger — the engine initiates an
                        // incremental version-vector sync for this peer on its
                        // own, seeded with the hydrated peer version.
                    }
                    Ok(NetworkEvent::RelayStatusChanged(crate::RelayStatus::Listening)) => {
                        if !logged_relay_listening {
                            log_stage("relay_listening");
                            logged_relay_listening = true;
                        }
                    }
                    Ok(NetworkEvent::PeerSynced { peer_id, .. }) => {
                        // Always the first time we hit this branch (we break out of
                        // the loop after a brief grace window), so log unconditionally.
                        log_stage("first_peer_synced");
                        synced_peers.insert(peer_id);
                        // Give a brief window for additional peers to sync
                        tokio::time::sleep(completion_grace).await;
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
    log_stage("shutdown_started");
    db.shutdown().await;

    // 8. Return result
    let peers_synced = synced_peers.len();
    let result = if peers_synced > 0 {
        BackgroundSyncResult::Synced { peers_synced }
    } else if saw_any_peer {
        BackgroundSyncResult::TimedOut { peers_synced: 0 }
    } else {
        BackgroundSyncResult::NoPeers
    };
    log::info!(
        "bg_sync stage=done elapsed_ms={} result={result:?}",
        t_start.elapsed().as_millis()
    );
    Ok(result)
}
