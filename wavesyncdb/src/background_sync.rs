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
//!     BackgroundSyncResult::Synced { peers_synced } => tracing::info!("Synced with {peers_synced} peers"),
//!     BackgroundSyncResult::TimedOut { peers_synced } => tracing::warn!("Timeout, synced with {peers_synced}"),
//!     BackgroundSyncResult::NoPeers => tracing::warn!("No peers found"),
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
    background_sync_with_peers_for_topic(database_url, timeout, peer_addrs, None).await
}

/// Performs a one-shot background sync, optionally **targeting a single group**.
///
/// `target_effective_topic` is the effective (PSK-derived) topic carried in the
/// FCM/APNs `data` payload by the relay's `NotifyTopic` push — it names the one
/// group whose data changed. When `Some`:
///
/// * if it is the **default** group's effective topic → only the default group
///   is synced (no extra groups are rejoined);
/// * if it matches a **configured extra** group → the engine is built on the
///   default group (the node identity) and only that one extra group is rejoined;
/// * if it matches **nothing** we know → fall back to rejoining all groups.
///
/// When `None`, every configured group is rejoined (the conservative wake used by
/// callers that can't tell which group changed).
pub async fn background_sync_with_peers_for_topic(
    database_url: &str,
    timeout: Duration,
    peer_addrs: &[String],
    target_effective_topic: Option<&str>,
) -> Result<BackgroundSyncResult, BackgroundSyncError> {
    background_sync_core(
        database_url,
        timeout,
        peer_addrs,
        target_effective_topic,
        false,
        false,
    )
    .await
    .map(|(result, _)| result)
}

/// Same as [`background_sync_with_peers_for_topic`], but also captures every
/// user-facing [`Notification`](crate::notify::Notification) surfaced while
/// applying remote changes during this sync window.
///
/// This is what the iOS Notification Service Extension (NSE) uses: it has no
/// foreground `use_sync_notifications` hook to render a notification for it,
/// so it must derive one itself from whatever this sync actually applied.
/// The returned `Vec` is ordered oldest → newest and already de-duplicated /
/// coalesced exactly as a foreground session would see it (same registry,
/// same gate) — most callers only need `.last()`. Empty on timeout, no
/// matching `SyncNotify` policy, or nothing that policy considered
/// notify-worthy — exactly like a foreground session seeing nothing.
///
/// `key_cache_load_only`, when `true`, forbids every group-key derivation this
/// call makes (default group and every rejoined extra group) from running the
/// KDF on a cache miss — the miss is surfaced as a sync failure for that group
/// instead. `wavesync_nse_handle_push` always passes `true`: this is what
/// keeps the ~19 MiB Argon2id derivation out of the NSE's ~24 MB budget.
pub async fn background_sync_with_capture(
    database_url: &str,
    timeout: Duration,
    peer_addrs: &[String],
    target_effective_topic: Option<&str>,
    key_cache_load_only: bool,
) -> Result<(BackgroundSyncResult, Vec<crate::notify::Notification>), BackgroundSyncError> {
    background_sync_core(
        database_url,
        timeout,
        peer_addrs,
        target_effective_topic,
        true,
        key_cache_load_only,
    )
    .await
}

/// Shared implementation behind [`background_sync_with_peers_for_topic`] and
/// [`background_sync_with_capture`]. `capture`, when true, additionally
/// collects every [`Notification`](crate::notify::Notification) produced
/// while syncing (see [`background_sync_with_capture`]'s docs); when false
/// the collection is skipped entirely, so the non-capturing FFI entry point
/// (`run_background_sync`) pays no extra cost. `key_cache_load_only` is
/// documented on [`background_sync_with_capture`]; every non-NSE caller in
/// this file passes `false`, under which this function's group-key handling
/// is byte-for-byte what it was before load-only mode existed.
async fn background_sync_core(
    database_url: &str,
    timeout: Duration,
    peer_addrs: &[String],
    target_effective_topic: Option<&str>,
    capture: bool,
    key_cache_load_only: bool,
) -> Result<(BackgroundSyncResult, Vec<crate::notify::Notification>), BackgroundSyncError> {
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
        tracing::info!(
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
    let mut builder = WaveSyncDbBuilder::new(database_url, &config.topic)
        .with_group_key_cache(config.group_key_cache_enabled)
        .with_key_cache_load_only(key_cache_load_only);

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
        builder = WaveSyncDbBuilder::new(database_url, &config.topic)
            .with_group_key_cache(config.group_key_cache_enabled)
            .with_key_cache_load_only(key_cache_load_only)
            .managed_relay(relay, api_key);
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

    // Restore FCM credentials persisted by the foreground `build()` so a
    // background build round-trips them back into the saved config rather than
    // overwriting them with `None`. The token-file read in `build()` no longer
    // depends on these (the relay sends with its own service account, so a
    // device only needs its token registered), but keeping the persisted creds
    // intact avoids churning `.wavesync_config.json` across wakes.
    #[cfg(feature = "push-sync")]
    if let (Some(project_id), Some(app_id), Some(api_key)) = (
        config.fcm_project_id.as_deref(),
        config.fcm_app_id.as_deref(),
        config.fcm_api_key.as_deref(),
    ) {
        builder = builder.with_fcm(project_id, app_id, api_key);
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
    let crate_name: Option<String> = config.crate_name.clone();
    let extra_groups: Vec<crate::connection::GroupConfig> = config.groups.clone();
    // Same cache dir `with_passphrase`/`join_group` use — on iOS, the default
    // group's key was almost certainly just cached a moment ago inside
    // `builder.build()` above, so this recomputation (and each extra group's,
    // in `groups_to_rejoin` below) is a cache hit rather than a redundant
    // Argon2id derivation. Matters most for the NSE's targeted wake, which
    // may need to check several extra groups' effective topics against the
    // push payload's topic before finding (or failing to find) a match.
    let cache_dir = crate::connection::key_cache_dir(database_url);
    // Falls back to the plaintext topic if this can't be derived (only
    // possible in load-only mode, and only if the default group's key
    // somehow wasn't cached — which `builder.build()` above would already
    // have failed on, so this is unreachable in practice; the fallback is
    // just defensive). It's only ever used for topic-string comparison
    // below, never for anything cryptographic.
    let default_effective = derive_effective_topic(
        &config.topic,
        config.passphrase.as_deref(),
        cache_dir.as_deref(),
        config.group_key_cache_enabled,
        key_cache_load_only,
    )
    .unwrap_or_else(|| config.topic.clone());
    let selected = groups_to_rejoin(
        target_effective_topic,
        &default_effective,
        &extra_groups,
        cache_dir.as_deref(),
        config.group_key_cache_enabled,
        key_cache_load_only,
    );
    if let Some(target) = target_effective_topic {
        tracing::info!(
            "bg_sync: targeted wake for topic {target} → rejoining {} of {} extra group(s)",
            selected.len(),
            extra_groups.len()
        );
    }
    let mut _group_handles: Vec<crate::WaveSyncDb> = Vec::new();
    for &idx in &selected {
        let group = &extra_groups[idx];
        // `join_group_load_only` never derives on a cache miss (NSE path);
        // `join_group` is the normal, KDF-allowed path every other caller
        // uses.
        let join_result = if key_cache_load_only {
            db.node()
                .join_group_load_only(&group.user_topic, &group.passphrase, group.kind.as_deref())
                .await
        } else {
            db.node()
                .join_group(&group.user_topic, &group.passphrase, group.kind.as_deref())
                .await
        };
        match join_result {
            Ok(group_db) => {
                if let Some(ref cn) = crate_name {
                    if let Err(e) = group_db.get_schema_registry(cn).sync().await {
                        tracing::warn!(
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
                tracing::warn!(
                    "bg_sync: failed to rejoin group '{}': {e}",
                    group.user_topic
                );
            }
        }
    }
    let joined_any_extra = !_group_handles.is_empty();
    if joined_any_extra {
        log_stage("groups_rejoined");
    }

    // 4c. Pump user-facing notifications. The foreground `use_sync_notifications`
    // Dioxus hook isn't running in this (FCM/APNs service, or NSE) process, so
    // the SyncNotify policies the engine evaluates while we sync would fire
    // but never reach anywhere — unless we pump them ourselves. Each group's
    // `notification_rx()` is a broadcast channel (fan-out, not exclusive), but
    // the two consumers below are mutually exclusive on `capture`, not both
    // active whenever `push-sync` is compiled in:
    //   * non-capture `push-sync` builds post each one as a native OS
    //     notification (the "app with no NSE" path — see `notify_display`).
    //   * a `capture` request (the NSE's own path) instead collects them, so
    //     the caller builds ITS OWN notification content — the rewritten
    //     banner — from the last one. Also gating the first branch on
    //     `!capture` matters here: a consumer that links `push-sync` INTO the
    //     NSE (as the README instructs, so `wavesync_nse_handle_push` has a
    //     notify registry to walk) would otherwise get both — the pump's
    //     native notification AND the rewritten banner — for the same event.
    // Subscribed *before* the wait loop (broadcast only delivers to current
    // subscribers); every pump is aborted after shutdown below.
    let captured: std::sync::Arc<std::sync::Mutex<Vec<crate::notify::Notification>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut notif_pumps: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    #[cfg(feature = "push-sync")]
    if !capture {
        let mut rxs = vec![db.notification_rx()];
        for g in &_group_handles {
            rxs.push(g.notification_rx());
        }
        for mut rx in rxs {
            notif_pumps.push(tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(n) => crate::notify_display::show_background(&n),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }));
        }
    }
    if capture {
        let mut rxs = vec![db.notification_rx()];
        for g in &_group_handles {
            rxs.push(g.notification_rx());
        }
        for mut rx in rxs {
            let captured = captured.clone();
            notif_pumps.push(tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(n) => captured.lock().unwrap().push(n),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }));
        }
    }

    // 5. Wait for peer discovery and let the engine sync on its own.
    //
    // We deliberately do NOT call request_full_sync() here. With peer versions
    // hydrated from disk, the automatic version-vector sync the engine fires on
    // ConnectionEstablished / registry-ready pulls only the delta since our last
    // sync — fast, and the whole point of waking incrementally. Forcing a full
    // sync would re-pull the entire database on every wake.
    //
    // Two timers bound the wait, both scaled to the caller's `timeout` (the
    // OS-granted background-execution budget — see `scaled_completion_grace`
    // / `scaled_fallback_after`) so a short grant still leaves the fallback
    // and linger window room to run before the hard deadline. At the
    // historical fixed 25s grant both scale to their original fixed values:
    //   * completion_grace — after the first PeerSynced, linger briefly so a
    //     second/third peer can also finish before we tear down.
    //   * fallback_after — if a peer connected but no incremental sync has
    //     completed by then (first-ever contact, or our persisted view of the
    //     peer was somehow ahead), ask for a full sync once. Preserves new-peer
    //     onboarding (db_version=0 semantics) without making it the default.
    // After the first peer syncs, linger for additional peers/groups before we
    // tear down. With extra groups joined, give their (fast, incremental)
    // version-vector round-trips room to land too — they share the connections
    // but emit their own PeerSynced events.
    let completion_grace_base = if joined_any_extra {
        Duration::from_millis(1500)
    } else {
        Duration::from_millis(500)
    };
    let completion_grace = scaled_completion_grace(completion_grace_base, timeout);
    let fallback_after = scaled_fallback_after(timeout);

    let mut events = db.network_event_rx();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let fallback = tokio::time::sleep(fallback_after);
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
    // Stop the notification pumps before tearing down the engine — any
    // notification for a change applied during the sync window has already
    // been posted (or captured) by now.
    for h in notif_pumps {
        h.abort();
    }
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
    tracing::info!(
        "bg_sync stage=done elapsed_ms={} result={result:?}",
        t_start.elapsed().as_millis()
    );
    let notifications = std::mem::take(&mut *captured.lock().unwrap());
    Ok((result, notifications))
}

/// Scale the "connected-but-no-incremental-sync" fallback timer (see the
/// `FALLBACK_AFTER` doc comment above) to the granted background-execution
/// budget `timeout`, so the fallback still has room to complete before the
/// hard deadline on a short grant. At the historical 25s grant this returns
/// the original fixed 5s value unchanged.
fn scaled_fallback_after(timeout: Duration) -> Duration {
    Duration::from_secs(5).min(timeout / 3)
}

/// Scale the post-first-sync linger window (`base`, see `COMPLETION_GRACE` in
/// the doc comment above) to the granted background-execution budget
/// `timeout`, floored at 200ms so even a very short grant leaves a moment for
/// a second peer to finish. At the historical 25s grant this returns `base`
/// unchanged.
fn scaled_completion_grace(base: Duration, timeout: Duration) -> Duration {
    base.min(timeout / 10).max(Duration::from_millis(200))
}

/// Derive the effective (PSK-derived) topic for a group, mirroring the engine:
/// with a passphrase it is `BLAKE3(user_topic, group_key)`; without one the
/// effective topic is the user topic verbatim. Keep in lockstep with
/// `engine::run_engine`'s `effective_topic` computation.
///
/// `cache_dir`/`cache_enabled` route the passphrase branch through the same
/// on-disk group-key cache `with_passphrase`/`join_group` use
/// (`connection::group_key_for_dir`) — iOS only; every other platform always
/// derives fresh. Passing `None`/`true` (or `false`) is always safe — it's
/// exactly "no cache available", which any non-iOS caller (including every
/// test in this module) already is.
///
/// Returns `None` only when `load_only` forbade deriving and the cache
/// missed — the caller can't know this group's effective topic without
/// running the KDF, and load-only mode means it must not. Off iOS this can
/// never happen (`group_key_for_dir` ignores `load_only` there).
fn derive_effective_topic(
    user_topic: &str,
    passphrase: Option<&str>,
    cache_dir: Option<&std::path::Path>,
    cache_enabled: bool,
    load_only: bool,
) -> Option<String> {
    match passphrase {
        Some(p) => {
            crate::connection::group_key_for_dir(p, user_topic, cache_dir, cache_enabled, load_only)
                .ok()
                .map(|k| k.derive_topic(user_topic))
        }
        None => Some(user_topic.to_string()),
    }
}

/// Decide which configured extra groups to rejoin for a wake, returning indices
/// into `groups`.
///
/// * `None` target → all groups (conservative wake).
/// * target == the default group's effective topic → none (default covers it).
/// * target matches one or more extra groups → just those.
/// * target matches nothing known → all groups (safe fallback; the relay should
///   only ever push a topic we registered for, so this is belt-and-suspenders).
///
/// `cache_dir`/`cache_enabled`/`load_only` are forwarded to
/// [`derive_effective_topic`] for each group checked — see that function's
/// docs. This matters most for a targeted (NSE) wake with several extra
/// groups configured: without a cache hit, checking each one against the
/// payload's topic would otherwise re-run Argon2id once per group just to
/// find (or rule out) a match — `load_only` forbids that; a group whose
/// effective topic can't be determined without deriving is simply treated as
/// not matching (its actual rejoin attempt below will independently, and
/// harmlessly, fail the same cache-miss check).
fn groups_to_rejoin(
    target_effective_topic: Option<&str>,
    default_effective: &str,
    groups: &[crate::connection::GroupConfig],
    cache_dir: Option<&std::path::Path>,
    cache_enabled: bool,
    load_only: bool,
) -> Vec<usize> {
    let Some(target) = target_effective_topic else {
        return (0..groups.len()).collect();
    };
    if target == default_effective {
        return Vec::new();
    }
    let matched: Vec<usize> = groups
        .iter()
        .enumerate()
        .filter(|(_, g)| {
            derive_effective_topic(
                &g.user_topic,
                Some(&g.passphrase),
                cache_dir,
                cache_enabled,
                load_only,
            )
            .is_some_and(|eff| eff == target)
        })
        .map(|(i, _)| i)
        .collect();
    if matched.is_empty() {
        // Unknown target — don't silently sync nothing; bring up everything.
        (0..groups.len()).collect()
    } else {
        matched
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::GroupConfig;

    fn group(user_topic: &str, passphrase: &str) -> GroupConfig {
        GroupConfig {
            user_topic: user_topic.to_string(),
            passphrase: passphrase.to_string(),
            database_url: format!("sqlite:/tmp/{user_topic}.db"),
            kind: None,
        }
    }

    /// Convenience for tests: the non-load-only path never returns `None`.
    fn eff(
        user_topic: &str,
        passphrase: Option<&str>,
        cache_dir: Option<&std::path::Path>,
        cache_enabled: bool,
    ) -> String {
        derive_effective_topic(user_topic, passphrase, cache_dir, cache_enabled, false)
            .expect("non-load-only derivation never misses")
    }

    #[test]
    fn no_target_rejoins_all_groups() {
        let groups = vec![group("beta", "pb"), group("gamma", "pg")];
        let sel = groups_to_rejoin(None, "wavesync-default", &groups, None, true, false);
        assert_eq!(sel, vec![0, 1]);
    }

    #[test]
    fn target_default_rejoins_nothing() {
        let groups = vec![group("beta", "pb")];
        let default_eff = eff("alpha", Some("pa"), None, true);
        let sel = groups_to_rejoin(Some(&default_eff), &default_eff, &groups, None, true, false);
        assert!(sel.is_empty(), "default-targeted wake skips extra groups");
    }

    #[test]
    fn target_extra_rejoins_only_that_group() {
        let groups = vec![group("beta", "pb"), group("gamma", "pg")];
        let beta_eff = eff("beta", Some("pb"), None, true);
        let sel = groups_to_rejoin(
            Some(&beta_eff),
            "wavesync-default",
            &groups,
            None,
            true,
            false,
        );
        assert_eq!(sel, vec![0], "only the targeted extra group is rejoined");
    }

    #[test]
    fn unknown_target_falls_back_to_all() {
        let groups = vec![group("beta", "pb"), group("gamma", "pg")];
        let sel = groups_to_rejoin(
            Some("wavesync-stranger"),
            "wavesync-default",
            &groups,
            None,
            true,
            false,
        );
        assert_eq!(sel, vec![0, 1], "unknown target falls back to all groups");
    }

    #[test]
    fn effective_topic_matches_engine_semantics() {
        // No passphrase → effective == user topic (mirrors engine::run_engine).
        assert_eq!(eff("plain", None, None, true), "plain");
        // With passphrase → derived hash, stable and != the raw topic.
        let topic = eff("plain", Some("secret"), None, true);
        assert!(topic.starts_with("wavesync2-"));
        assert_ne!(topic, "plain");
        // Same inputs → same output; different passphrase → different topic.
        assert_eq!(topic, eff("plain", Some("secret"), None, true));
        assert_ne!(topic, eff("plain", Some("other"), None, true));
    }

    #[test]
    fn no_cache_dir_or_disabled_cache_behave_the_same_off_ios() {
        // On every non-iOS target `group_key_for_dir` always derives fresh
        // regardless of `cache_enabled`/`cache_dir` — this pins that down so
        // a future iOS-only behavior change can't silently regress host
        // (Linux/macOS dev, CI) derivation.
        let with_cache_flag = eff("plain", Some("secret"), None, true);
        let without_cache_flag = eff("plain", Some("secret"), None, false);
        assert_eq!(with_cache_flag, without_cache_flag);
    }

    #[test]
    fn load_only_cache_miss_yields_none_and_does_not_derive() {
        // A cache dir that exists but has never been warmed for this topic.
        let dir = std::env::temp_dir().join(format!(
            "wavesync_bg_sync_load_only_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let result = derive_effective_topic("plain", Some("secret"), Some(&dir), true, true);
        assert_eq!(
            result, None,
            "load-only mode must not derive on a cache miss"
        );
    }

    #[test]
    fn load_only_cache_hit_still_resolves() {
        let dir = std::env::temp_dir().join(format!(
            "wavesync_bg_sync_load_only_hit_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        // Warm the cache the way a normal (non-load-only) derivation would.
        let warmed = derive_effective_topic("plain", Some("secret"), Some(&dir), true, false)
            .expect("warm derivation must succeed");
        let loaded = derive_effective_topic("plain", Some("secret"), Some(&dir), true, true)
            .expect("load-only must resolve from a warm cache");
        assert_eq!(warmed, loaded);
    }

    #[test]
    fn groups_to_rejoin_load_only_miss_treats_group_as_non_matching() {
        // With load_only and an unwarmed cache, an extra group's effective
        // topic can't be determined — it must be filtered out of `matched`
        // rather than derived. Here that means the (only) group doesn't
        // match, so the "unknown target" fallback (sync everything) applies —
        // the important assertion is that this never panics or derives.
        let dir = std::env::temp_dir().join(format!(
            "wavesync_bg_sync_groups_to_rejoin_load_only_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let groups = vec![group("beta", "pb")];
        let sel = groups_to_rejoin(
            Some("wavesync2-somehash"),
            "wavesync-default",
            &groups,
            Some(&dir),
            true,
            true,
        );
        assert_eq!(
            sel,
            vec![0],
            "cache-miss group falls back via the unknown-target path"
        );
    }

    #[test]
    fn scaled_timers_preserve_current_values_at_25s_grant() {
        // The historical fixed grant (background_sync's default `timeout`).
        // Both scaled functions must reproduce the exact pre-scaling
        // constants at this timeout — zero behavior change at defaults.
        let timeout = Duration::from_secs(25);
        assert_eq!(scaled_fallback_after(timeout), Duration::from_secs(5));
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(1500), timeout),
            Duration::from_millis(1500)
        );
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(500), timeout),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn scaled_timers_shrink_on_a_short_grant() {
        // timeout=6s: fallback = min(5s, 6s/3=2s) = 2s.
        let timeout = Duration::from_secs(6);
        assert_eq!(scaled_fallback_after(timeout), Duration::from_secs(2));
        // completion_grace(base=1500ms) = min(1500ms, 6s/10=600ms).max(200ms) = 600ms.
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(1500), timeout),
            Duration::from_millis(600)
        );
        // completion_grace(base=500ms) = min(500ms, 600ms).max(200ms) = 500ms
        // (base is already below the scaled cap, so it passes through).
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(500), timeout),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn scaled_timers_floor_on_a_very_short_grant() {
        // timeout=1s: fallback = min(5s, 1s/3) = 1s/3 (~333ms; Duration
        // division truncates, so compare against the same expression rather
        // than a rounded millisecond literal).
        let timeout = Duration::from_secs(1);
        assert_eq!(scaled_fallback_after(timeout), timeout / 3);
        // completion_grace: timeout/10=100ms, below the 200ms floor regardless
        // of base, so the floor wins for any base.
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(1500), timeout),
            Duration::from_millis(200)
        );
        assert_eq!(
            scaled_completion_grace(Duration::from_millis(500), timeout),
            Duration::from_millis(200)
        );
    }
}
