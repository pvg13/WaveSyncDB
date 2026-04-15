use crate::error::WaveSyncError;

/// Result of a one-shot background sync.
#[derive(uniffi::Enum, Clone, Debug)]
pub enum FfiBackgroundSyncResult {
    /// Successfully synced with at least one peer.
    Synced { peers_synced: u32 },
    /// No peers found within the timeout.
    NoPeers,
    /// Timed out before all peers finished syncing.
    TimedOut { peers_synced: u32 },
}

/// Perform a one-shot background sync for push notification wake-up.
///
/// Call this from a `FirebaseMessagingService` when a `sync_available` push
/// arrives while the app is killed or in the background. It:
///   1. Reads `.wavesync_config.json` from the database directory
///   2. Reconstructs the sync engine with saved settings
///   3. Waits for peer discovery and pulls changes
///   4. Shuts down cleanly
///
/// The app must have been started at least once (to create the config file).
///
/// `peer_addrs_json` is an optional JSON array of multiaddr strings from the
/// FCM payload (e.g. `["/ip4/192.168.1.5/tcp/36189/p2p/12D3Koo..."]`).
/// These are dialed directly, bypassing slow mDNS/relay discovery.
#[uniffi::export(async_runtime = "tokio")]
pub async fn background_sync(
    database_url: String,
    timeout_secs: u32,
    peer_addrs_json: Option<String>,
) -> Result<FfiBackgroundSyncResult, WaveSyncError> {
    crate::builder::init_logging();
    tracing::info!(
        "background_sync started: db={}, timeout={}s, peer_addrs={:?}",
        database_url,
        timeout_secs,
        peer_addrs_json.as_deref().unwrap_or("none")
    );

    let timeout = std::time::Duration::from_secs(timeout_secs.into());

    let peer_addrs: Vec<String> = peer_addrs_json
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok())
        .unwrap_or_default();

    let result = wavesyncdb::background_sync::background_sync_with_peers(
        &database_url,
        timeout,
        &peer_addrs,
    )
    .await
    .map_err(|e| {
        tracing::error!("background_sync failed: {e}");
        WaveSyncError::Database {
            msg: e.to_string(),
        }
    })?;

    tracing::info!("background_sync result: {:?}", result);

    Ok(match result {
        wavesyncdb::background_sync::BackgroundSyncResult::Synced { peers_synced } => {
            FfiBackgroundSyncResult::Synced {
                peers_synced: peers_synced as u32,
            }
        }
        wavesyncdb::background_sync::BackgroundSyncResult::NoPeers => {
            FfiBackgroundSyncResult::NoPeers
        }
        wavesyncdb::background_sync::BackgroundSyncResult::TimedOut { peers_synced } => {
            FfiBackgroundSyncResult::TimedOut {
                peers_synced: peers_synced as u32,
            }
        }
    })
}
