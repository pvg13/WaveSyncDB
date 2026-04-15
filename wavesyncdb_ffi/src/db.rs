use std::sync::{Arc, Mutex};

use sea_orm::{ConnectionTrait, FromQueryResult, Statement, DatabaseBackend};
use tokio::task::JoinHandle;
use wavesyncdb::WaveSyncDb;
use wavesyncdb::messages::DeletePolicy;
use wavesyncdb::registry::TableMeta;

use crate::callbacks::{ChangeListener, NetworkEventListener};
use crate::error::WaveSyncError;
use crate::types::{FfiChangeNotification, FfiNetworkEvent, FfiNetworkStatus, FfiPeerInfo};

#[derive(uniffi::Object)]
pub struct WaveSyncFfi {
    db: WaveSyncDb,
    rt: tokio::runtime::Handle,
    /// Handle to the change-listener tokio task so we can abort it on
    /// re-subscribe or shutdown, preventing zombie listener accumulation.
    change_task: Mutex<Option<JoinHandle<()>>>,
    /// Handle to the network-event listener task.
    network_event_task: Mutex<Option<JoinHandle<()>>>,
}

impl WaveSyncFfi {
    pub(crate) fn new(db: WaveSyncDb, rt: tokio::runtime::Handle) -> Self {
        Self {
            db,
            rt,
            change_task: Mutex::new(None),
            network_event_task: Mutex::new(None),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl WaveSyncFfi {
    // -----------------------------------------------------------------------
    // SQL operations
    // -----------------------------------------------------------------------

    /// Execute a SQL write (INSERT/UPDATE/DELETE).
    /// Routes through WaveSyncDb write interceptor — CRDT bookkeeping applies.
    pub async fn execute(&self, sql: String) -> Result<u64, WaveSyncError> {
        let result = self.db.execute_unprepared(&sql).await?;
        Ok(result.rows_affected())
    }

    /// Execute a SQL read (SELECT).
    /// Returns a JSON array of objects.
    pub async fn query(&self, sql: String) -> Result<String, WaveSyncError> {
        let stmt = Statement::from_string(DatabaseBackend::Sqlite, sql);
        let rows = serde_json::Value::find_by_statement(stmt)
            .all(self.db.inner())
            .await?;
        Ok(serde_json::to_string(&rows)?)
    }

    // -----------------------------------------------------------------------
    // Change subscriptions
    // -----------------------------------------------------------------------

    /// Subscribe to data change notifications.
    /// Spawns a background task — callback is invoked from a tokio task thread.
    /// Calling this again aborts the previous listener task to prevent zombies.
    pub fn subscribe_changes(&self, listener: Box<dyn ChangeListener>) {
        // Abort previous listener task if any.
        if let Some(prev) = self.change_task.lock().unwrap().take() {
            prev.abort();
        }

        let mut rx = self.db.change_rx();
        let handle = self.rt.spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(notif) => {
                        listener.on_change(FfiChangeNotification::from(notif));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("change_rx lagged, skipped {n} notifications");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        *self.change_task.lock().unwrap() = Some(handle);
    }

    /// Subscribe to network events (peer connect/disconnect, relay changes, etc.).
    /// Spawns a background task — callback is invoked from a tokio task thread.
    /// Calling this again aborts the previous listener task to prevent zombies.
    pub fn subscribe_network_events(&self, listener: Box<dyn NetworkEventListener>) {
        if let Some(prev) = self.network_event_task.lock().unwrap().take() {
            prev.abort();
        }

        let mut rx = self.db.network_event_rx();
        let handle = self.rt.spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        listener.on_event(FfiNetworkEvent::from(event));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("network_event_rx lagged, skipped {n} events");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        *self.network_event_task.lock().unwrap() = Some(handle);
    }

    // -----------------------------------------------------------------------
    // Network status
    // -----------------------------------------------------------------------

    /// Get current network status snapshot.
    pub fn network_status(&self) -> FfiNetworkStatus {
        self.db.network_status().into()
    }

    // -----------------------------------------------------------------------
    // Table registration
    // -----------------------------------------------------------------------

    /// Register a table for sync (call before registry_ready).
    pub async fn register_synced_table(
        &self,
        table_name: String,
        pk_column: String,
        columns: Vec<String>,
        create_sql: String,
    ) -> Result<(), WaveSyncError> {
        self.db.execute_unprepared(&create_sql).await?;

        // Create the shadow clock table for CRDT version tracking.
        wavesyncdb::shadow::create_shadow_table(self.db.inner(), &table_name).await?;

        let meta = TableMeta {
            table_name,
            primary_key_column: pk_column,
            columns,
            delete_policy: DeletePolicy::DeleteWins,
        };
        self.db.register_table(meta);
        Ok(())
    }

    /// Signal that all tables are registered.
    /// Also persists table metadata to SyncConfig so background sync can
    /// reconstruct the registry without the app running.
    pub fn registry_ready(&self) {
        // Persist registered tables to SyncConfig for cold background sync.
        let tables = self.db.registry().all_tables();
        if tables.is_empty() {
            tracing::warn!("registry_ready called with zero registered tables — background sync will have no schema");
        } else {
            match wavesyncdb::connection::SyncConfig::load(self.db.database_url()) {
                Ok(mut config) => {
                    let count = tables.len();
                    config.registered_tables = Some(tables);
                    if let Err(e) = config.save() {
                        tracing::error!("Failed to persist registered_tables to SyncConfig: {e}");
                    } else {
                        tracing::info!("Persisted {count} table(s) to SyncConfig for background sync");
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to load SyncConfig for table persistence: {e}");
                }
            }
        }
        self.db.registry_ready();
    }

    // -----------------------------------------------------------------------
    // Lifecycle & sync control
    // -----------------------------------------------------------------------

    /// Signal foreground resume.
    pub fn resume(&self) {
        self.db.resume();
    }

    /// Signal network change (e.g., WiFi to cellular).
    pub fn network_transition(&self) {
        self.db.network_transition();
    }

    /// Request a full sync from connected peers.
    pub fn request_full_sync(&self) {
        self.db.request_full_sync();
    }

    /// Register FCM/APNs push token.
    pub fn register_push_token(&self, platform: String, token: String) {
        self.db.register_push_token(&platform, &token);
    }

    // -----------------------------------------------------------------------
    // Peer identity
    // -----------------------------------------------------------------------

    /// Set the application-level identity for this peer.
    /// The identity is announced to all verified peers.
    pub fn set_peer_identity(&self, app_id: String) {
        self.db.set_peer_identity(&app_id);
    }

    /// Clear the application-level identity for this peer.
    pub fn clear_peer_identity(&self) {
        self.db.clear_peer_identity();
    }

    /// Get peers grouped by application-level identity.
    /// Returns a list of (app_id, peers) pairs.
    pub fn peers_by_identity(&self) -> Vec<FfiIdentityGroup> {
        self.db
            .peers_by_identity()
            .into_iter()
            .map(|(app_id, peers)| FfiIdentityGroup {
                app_id,
                peers: peers.iter().map(FfiPeerInfo::from).collect(),
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Database metadata
    // -----------------------------------------------------------------------

    /// Get this node's ephemeral node ID (hex-encoded).
    pub fn node_id(&self) -> String {
        self.db.node_id().0.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// Get this node's persistent site ID (hex-encoded).
    pub fn site_id(&self) -> String {
        self.db.site_id().0.iter().map(|b| format!("{b:02x}")).collect()
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Graceful shutdown.
    pub async fn shutdown(self: Arc<Self>) -> Result<(), WaveSyncError> {
        // Abort listener tasks so they don't outlive the engine.
        if let Some(handle) = self.change_task.lock().unwrap().take() {
            handle.abort();
        }
        if let Some(handle) = self.network_event_task.lock().unwrap().take() {
            handle.abort();
        }
        self.db.shutdown().await;
        Ok(())
    }

    /// Check if the engine background task is still running.
    pub fn is_engine_alive(&self) -> bool {
        self.db.is_engine_alive()
    }

    /// Read the SyncConfig JSON for diagnostic purposes.
    /// Returns the raw JSON content of `.wavesync_config.json`.
    pub fn read_sync_config(&self) -> Result<String, WaveSyncError> {
        let config = wavesyncdb::connection::SyncConfig::load(self.db.database_url())
            .map_err(|e| WaveSyncError::Database { msg: e })?;
        serde_json::to_string_pretty(&config)
            .map_err(|e| WaveSyncError::Serialization { msg: e.to_string() })
    }
}

/// Group of peers sharing an application-level identity.
#[derive(uniffi::Record, Clone, Debug)]
pub struct FfiIdentityGroup {
    pub app_id: String,
    pub peers: Vec<FfiPeerInfo>,
}
