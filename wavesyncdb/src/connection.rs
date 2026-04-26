use std::path::PathBuf;
use std::sync::Arc;

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, ExecResult, Iterable, PrimaryKeyToColumn, QueryResult, Schema, Statement,
    sea_query::SqliteQueryBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, broadcast, mpsc};

use crate::messages::{
    ChangeNotification, ColumnChange, DeletePolicy, NodeId, SyncChangeset, WriteKind,
};
use crate::registry::{SyncEntityInfo, TableMeta, TableRegistry};

/// Try to classify a SQL statement as a write and extract relevant info.
///
/// Uses keyword-anchor parsing to handle variants like `INSERT OR REPLACE INTO`,
/// multi-line SQL, and leading whitespace/comments.
pub(crate) fn classify_write(sql: &str) -> Option<(WriteKind, String)> {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("INSERT") {
        // INSERT [OR REPLACE|OR IGNORE] INTO <table> ...
        let into_pos = upper.find("INTO ")?;
        let after_into = &trimmed[into_pos + 5..];
        let table = after_into.split_whitespace().next()?;
        let table = table.trim_matches('"').trim_matches('`').to_string();
        Some((WriteKind::Insert, table))
    } else if upper.starts_with("UPDATE") {
        // UPDATE <table> SET ...
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        // Find the token just before SET
        let set_idx = parts.iter().position(|p| p.eq_ignore_ascii_case("SET"))?;
        if set_idx == 0 {
            return None;
        }
        let table = parts[set_idx - 1]
            .trim_matches('"')
            .trim_matches('`')
            .to_string();
        Some((WriteKind::Update, table))
    } else if upper.starts_with("DELETE") {
        // DELETE FROM <table> ...
        let from_pos = upper.find("FROM ")?;
        let after_from = &trimmed[from_pos + 5..];
        let table = after_from.split_whitespace().next()?;
        let table = table.trim_matches('"').trim_matches('`').to_string();
        Some((WriteKind::Delete, table))
    } else {
        None
    }
}

/// Parsed write information with column-value pairs.
pub(crate) struct ParsedWrite {
    pub primary_key: String,
    pub columns: Vec<(String, serde_json::Value)>,
}

/// Parse a SQL write statement to extract column names and values.
///
/// For INSERT: pairs column list with VALUES list positionally.
/// For UPDATE: parses `SET col = val` pairs.
/// For DELETE: returns empty columns (tombstone handles it).
pub(crate) fn parse_write_full(sql: &str, pk_column: &str) -> Option<ParsedWrite> {
    let (kind, _table) = classify_write(sql)?;

    match kind {
        WriteKind::Insert => {
            let primary_key = extract_pk_from_insert(sql, pk_column);
            let columns = extract_column_values_insert(sql);
            Some(ParsedWrite {
                primary_key,
                columns,
            })
        }
        WriteKind::Update => {
            let primary_key = extract_pk_from_where(sql, pk_column);
            let columns = extract_column_values_update(sql);
            Some(ParsedWrite {
                primary_key,
                columns,
            })
        }
        WriteKind::Delete => {
            let primary_key = extract_pk_from_where(sql, pk_column);
            Some(ParsedWrite {
                primary_key,
                columns: vec![],
            })
        }
    }
}

/// Extract column-value pairs from an INSERT statement.
fn extract_column_values_insert(sql: &str) -> Vec<(String, serde_json::Value)> {
    // Find column list between first ( and )
    let col_start = match sql.find('(') {
        Some(i) => i + 1,
        None => return vec![],
    };
    let col_end = match sql[col_start..].find(')') {
        Some(i) => col_start + i,
        None => return vec![],
    };
    let columns: Vec<String> = sql[col_start..col_end]
        .split(',')
        .map(|c| c.trim().trim_matches('"').trim_matches('`').to_string())
        .collect();

    // Find VALUES list
    let upper = sql.to_uppercase();
    let values_pos = match upper.find("VALUES") {
        Some(i) => i,
        None => return vec![],
    };
    let val_start = match sql[values_pos..].find('(') {
        Some(i) => values_pos + i + 1,
        None => return vec![],
    };
    let val_end = match sql[val_start..].rfind(')') {
        Some(i) => val_start + i,
        None => return vec![],
    };

    let values = split_sql_values(&sql[val_start..val_end]);

    columns
        .into_iter()
        .zip(values)
        .map(|(col, val)| {
            let val = val.trim();
            let json_val = sql_value_to_json(val);
            (col, json_val)
        })
        .collect()
}

/// Extract column-value pairs from an UPDATE SET clause.
fn extract_column_values_update(sql: &str) -> Vec<(String, serde_json::Value)> {
    let upper = sql.to_uppercase();
    let set_pos = match upper.find("SET ") {
        Some(i) => i + 4,
        None => return vec![],
    };
    let end_pos = upper[set_pos..]
        .find("WHERE")
        .map(|i| set_pos + i)
        .unwrap_or(sql.len());
    let set_clause = &sql[set_pos..end_pos];

    set_clause
        .split(',')
        .filter_map(|part| {
            let eq = part.find('=')?;
            let col = part[..eq]
                .trim()
                .trim_matches('"')
                .trim_matches('`')
                .to_string();
            let val = part[eq + 1..].trim();
            let json_val = sql_value_to_json(val);
            Some((col, json_val))
        })
        .collect()
}

/// Convert a SQL literal value to a JSON value.
fn sql_value_to_json(val: &str) -> serde_json::Value {
    let val = val.trim();
    if val.eq_ignore_ascii_case("NULL") {
        serde_json::Value::Null
    } else if val.starts_with('\'') && val.ends_with('\'') {
        // String literal
        serde_json::Value::String(val[1..val.len() - 1].replace("''", "'"))
    } else if let Ok(i) = val.parse::<i64>() {
        serde_json::Value::Number(i.into())
    } else if let Ok(f) = val.parse::<f64>() {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::String(val.to_string()))
    } else if val.eq_ignore_ascii_case("TRUE") {
        serde_json::Value::Bool(true)
    } else if val.eq_ignore_ascii_case("FALSE") {
        serde_json::Value::Bool(false)
    } else {
        serde_json::Value::String(val.to_string())
    }
}

/// Internal shared state for [`WaveSyncDb`].
struct WaveSyncDbInner {
    inner: DatabaseConnection,
    database_url: String,
    sync_tx: mpsc::Sender<SyncChangeset>,
    change_tx: broadcast::Sender<ChangeNotification>,
    site_id: NodeId,
    db_version: Mutex<u64>,
    node_id: NodeId,
    registry: Arc<TableRegistry>,
    registry_ready: Arc<Notify>,
    cmd_tx: mpsc::Sender<crate::engine::EngineCommand>,
    engine_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
}

/// A SeaORM connection wrapper that transparently intercepts write operations
/// and dispatches them to the sync engine via column-level CRDT changesets.
///
/// `WaveSyncDb` is cheap to clone (internally Arc-based), matching the
/// ergonomics of SeaORM's `DatabaseConnection`.
#[derive(Clone)]
pub struct WaveSyncDb {
    inner: Arc<WaveSyncDbInner>,
}

impl Drop for WaveSyncDbInner {
    fn drop(&mut self) {
        // Abort the engine task to prevent zombie swarms (e.g. mDNS cross-talk between tests).
        // Use get_mut() instead of lock() — since we have &mut self, no other thread
        // can hold a reference, so we can access the mutex data without locking.
        // This avoids the "pthread_mutex_lock called on a destroyed mutex" crash
        // on Android when the app process is killed.
        if let Some(handle) = self.engine_handle.get_mut().ok().and_then(|h| h.take()) {
            handle.abort();
        }
    }
}

impl PartialEq for WaveSyncDb {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl std::fmt::Debug for WaveSyncDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaveSyncDb")
            .field("site_id", &self.inner.site_id)
            .finish_non_exhaustive()
    }
}

impl WaveSyncDb {
    /// Get a reference to the underlying SeaORM connection.
    pub fn inner(&self) -> &DatabaseConnection {
        &self.inner.inner
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.inner.node_id
    }

    /// Get the persistent site_id.
    pub fn site_id(&self) -> &NodeId {
        &self.inner.site_id
    }

    /// Get a handle to the change notification broadcast channel.
    pub fn change_rx(&self) -> broadcast::Receiver<ChangeNotification> {
        self.inner.change_tx.subscribe()
    }

    /// Get a reference to the change notification sender.
    pub fn change_tx(&self) -> &broadcast::Sender<ChangeNotification> {
        &self.inner.change_tx
    }

    /// Get a snapshot of the current network status.
    ///
    /// This is a cheap read from shared memory — no network round-trip.
    pub fn network_status(&self) -> crate::network_status::NetworkStatus {
        self.inner.network_status.read().unwrap().clone()
    }

    /// Subscribe to network events (peer connect/disconnect, relay changes, etc.).
    pub fn network_event_rx(&self) -> broadcast::Receiver<crate::network_status::NetworkEvent> {
        self.inner.network_event_tx.subscribe()
    }

    /// Get a reference to the sync changeset sender.
    pub fn sync_tx(&self) -> &mpsc::Sender<SyncChangeset> {
        &self.inner.sync_tx
    }

    /// Get a reference to the table registry.
    pub fn registry(&self) -> &Arc<TableRegistry> {
        &self.inner.registry
    }

    /// Gracefully shut down the engine and close the database connection.
    pub async fn shutdown(&self) {
        let _ = self
            .inner
            .cmd_tx
            .send(crate::engine::EngineCommand::Shutdown)
            .await;

        let handle = {
            self.inner
                .engine_handle
                .lock()
                .ok()
                .and_then(|mut h| h.take())
        };
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        self.inner.inner.clone().close().await.ok();
    }

    /// Check if the engine background task is still running.
    pub fn is_engine_alive(&self) -> bool {
        self.inner
            .engine_handle
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }

    /// Signal the engine to resync after the app resumes from background.
    pub fn resume(&self) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::Resume);
    }

    /// Notify the engine that the network interface changed (e.g., WiFi to cellular).
    ///
    /// This force-disconnects all connections (including the relay) and
    /// re-establishes them on the new network interface. More aggressive than
    /// `resume()` — use when you know the network path has changed.
    pub fn network_transition(&self) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::NetworkTransition);
    }

    /// Request a full sync from connected peers.
    pub fn request_full_sync(&self) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::RequestFullSync);
    }

    /// Register or update the push notification token with the relay server.
    ///
    /// Call this when the app receives a new FCM/APNs device token, or when
    /// the token rotates. The engine will send a `RegisterToken` request to
    /// the relay on the next connection (or immediately if already connected).
    /// `platform` should be `"Fcm"` or `"Apns"`.
    pub fn register_push_token(&self, platform: &str, token: &str) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::RegisterPushToken {
                platform: platform.to_string(),
                token: token.to_string(),
            });
    }

    /// Returns the parent directory of the database file.
    ///
    /// This is where push token files (`wavesync_apns_token`, `wavesync_fcm_token`)
    /// and the sync config (`.wavesync_config.json`) are stored.
    pub fn database_directory(&self) -> Option<std::path::PathBuf> {
        crate::push::extract_db_path(&self.inner.database_url)
            .and_then(|p| std::path::Path::new(&p).parent().map(|p| p.to_path_buf()))
    }

    /// Set the application-level identity for this peer.
    ///
    /// The identity is an opaque string — WaveSyncDB does not interpret it.
    /// It is announced to all currently verified peers and to any peer that
    /// becomes verified in the future. Identities are ephemeral (session-scoped)
    /// and cleared on disconnect.
    pub fn set_peer_identity(&self, app_id: &str) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::SetPeerIdentity(Some(
                app_id.to_string(),
            )));
    }

    /// Clear the application-level identity for this peer.
    pub fn clear_peer_identity(&self) {
        let _ = self
            .inner
            .cmd_tx
            .try_send(crate::engine::EngineCommand::SetPeerIdentity(None));
    }

    /// Get all connected peers grouped by their application-level identity.
    ///
    /// Returns only peers that have announced an identity. Peers without
    /// an identity are excluded.
    pub fn peers_by_identity(
        &self,
    ) -> std::collections::HashMap<String, Vec<crate::network_status::PeerInfo>> {
        let status = self.inner.network_status.read().unwrap();
        let mut map: std::collections::HashMap<String, Vec<crate::network_status::PeerInfo>> =
            std::collections::HashMap::new();
        for peer in &status.connected_peers {
            if let Some(ref app_id) = peer.app_id {
                map.entry(app_id.clone()).or_default().push(peer.clone());
            }
        }
        map
    }

    /// Register a table for sync.
    pub fn register_table(&self, meta: TableMeta) {
        self.inner.registry.register(meta);
    }

    /// Signal the engine that all tables have been registered and sync can begin.
    ///
    /// This is called automatically by [`SchemaBuilder::sync()`]. You only need
    /// to call it manually when registering tables via [`register_table()`](Self::register_table)
    /// without using the schema builder.
    pub fn registry_ready(&self) {
        self.inner.registry_ready.notify_one();
    }

    /// Start building the sync schema.
    pub fn schema(&self) -> SchemaBuilder<'_> {
        SchemaBuilder {
            db: self,
            entries: Vec::new(),
            crate_name: None,
        }
    }

    /// Auto-discover entities registered via `#[derive(SyncEntity)]` and build a
    /// [`SchemaBuilder`] populated with all matching entities.
    pub fn get_schema_registry(&self, prefix: &str) -> SchemaBuilder<'_> {
        let mut builder = self.schema();
        builder.crate_name = Some(prefix.to_string());
        let backend = self.get_database_backend();

        // Normalize: trim trailing ::* or ::, convert hyphens to underscores in crate name
        let owned;
        let prefix = {
            let p = prefix.trim_end_matches('*').trim_end_matches("::");
            if let Some((left, right)) = p.split_once("::") {
                if left.contains('-') {
                    owned = format!("{}::{}", left.replace('-', "_"), right);
                    &owned
                } else {
                    p
                }
            } else if p.contains('-') {
                owned = p.replace('-', "_");
                &owned
            } else {
                p
            }
        };

        for info in inventory::iter::<SyncEntityInfo> {
            if info.module_path.starts_with(prefix) {
                let (create_sql, meta) = (info.schema_fn)(backend);
                builder.entries.push(EntityEntry {
                    create_sql,
                    meta,
                    synced: true,
                });
            }
        }
        builder
    }

    /// Create the table (if not exists) and register it for sync, using SeaORM entity metadata.
    pub async fn sync_entity<E>(&self) -> Result<(), DbErr>
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let backend = self.get_database_backend();
        let schema = Schema::new(backend);
        let create_stmt = schema
            .create_table_from_entity(E::default())
            .if_not_exists()
            .to_owned();
        self.inner
            .inner
            .execute_unprepared(&create_stmt.to_string(SqliteQueryBuilder))
            .await?;

        let entity = E::default();
        let table_name = entity.table_name().to_string();

        let columns: Vec<String> = E::Column::iter()
            .map(|c| sea_orm::IdenStatic::as_str(&c).to_string())
            .collect();

        let primary_key_column = E::PrimaryKey::iter()
            .next()
            .map(|pk| {
                let col = pk.into_column();
                sea_orm::IdenStatic::as_str(&col).to_string()
            })
            .unwrap_or_default();

        // Create shadow table
        crate::shadow::create_shadow_table(&self.inner.inner, &table_name).await?;

        self.register_table(TableMeta {
            table_name,
            primary_key_column,
            columns,
            delete_policy: DeletePolicy::default(),
        });

        Ok(())
    }

    /// Broadcast a change notification (used by the engine for remote changes).
    pub fn notify_change(&self, notification: ChangeNotification) {
        let _ = self.inner.change_tx.send(notification);
    }

    /// After a successful write, create and dispatch column-level CRDT changes.
    ///
    /// Returns `Err` if the `db_version` persist to `_wavesync_meta` fails.
    /// The in-memory counter is rolled back on failure so it stays in sync
    /// with the persisted value.
    async fn dispatch_sync(
        &self,
        kind: WriteKind,
        table: &str,
        resolved_sql: &str,
    ) -> Result<(), DbErr> {
        if table.starts_with("_wavesync") {
            return Ok(()); // Don't sync internal tables
        }
        if !self.inner.registry.is_registered(table) {
            return Ok(()); // Don't sync unregistered tables
        }

        let pk_column = self
            .inner
            .registry
            .get(table)
            .map(|m| m.primary_key_column.clone());
        let Some(pk_col) = pk_column else {
            return Ok(());
        };

        let parsed = match parse_write_full(resolved_sql, &pk_col) {
            Some(p) => p,
            None => {
                let truncated: String = resolved_sql.chars().take(200).collect();
                log::warn!(
                    "Sync skipped: parse_write_full returned None for registered table \"{}\". SQL: {}",
                    table,
                    truncated
                );
                return Ok(());
            }
        };

        if parsed.primary_key.is_empty() {
            return Ok(());
        }

        // Send change notification IMMEDIATELY — data is already committed.
        // This ensures the notification is sent on the caller's runtime (Dioxus),
        // not from a tokio::spawn which can't reliably wake the Dioxus event loop on mobile.
        let changed_columns = if matches!(kind, WriteKind::Delete) {
            None
        } else {
            Some(parsed.columns.iter().map(|(col, _)| col.clone()).collect())
        };
        let _ = self.inner.change_tx.send(ChangeNotification {
            table: table.to_string().into(),
            kind: kind.clone(),
            primary_key: parsed.primary_key.clone().into(),
            changed_columns,
        });

        let site_id = self.inner.site_id;
        let shared = self.inner.clone();
        let inner = self.inner.inner.clone();
        let table_owned = table.to_string();
        let kind_clone = kind.clone();

        // Oneshot channel to report db_version persist result back to the caller.
        // The spawned task holds the db_version lock for the full operation (shadow
        // serialization) but signals persist success/failure immediately so the
        // ConnectionTrait method can propagate errors.
        let (persist_tx, persist_rx) = tokio::sync::oneshot::channel::<Result<(), DbErr>>();

        // Async CRDT bookkeeping + P2P sync
        // The db_version lock is held for the entire operation to serialize
        // shadow reads/writes. Without this, two rapid writes (INSERT then UPDATE)
        // can race: the UPDATE task reads shadow before INSERT's task writes it,
        // producing incorrect col_versions and out-of-order changeset delivery.
        tokio::spawn(async move {
            let mut ver = shared.db_version.lock().await;
            *ver += 1;
            let new_db_version = *ver;
            // Persist — if this fails, roll back and report error
            if let Err(e) = crate::shadow::set_db_version(&inner, new_db_version).await {
                *ver -= 1;
                let _ = persist_tx.send(Err(e));
                return;
            }
            let _ = persist_tx.send(Ok(()));

            let mut changes = Vec::new();

            match kind_clone {
                WriteKind::Delete => {
                    // For deletes, find max col_version for this row and create tombstone
                    let entries = crate::shadow::get_clock_entries_for_row(
                        &inner,
                        &table_owned,
                        &parsed.primary_key,
                    )
                    .await
                    .unwrap_or_default();

                    let max_cv = entries.iter().map(|e| e.col_version).max().unwrap_or(0);
                    let tombstone_cv = max_cv + 1;

                    if let Err(e) = crate::shadow::insert_tombstone(
                        &inner,
                        &table_owned,
                        &parsed.primary_key,
                        tombstone_cv,
                        new_db_version,
                        &site_id,
                    )
                    .await
                    {
                        log::error!("Failed to insert tombstone: {}", e);
                    }

                    changes.push(ColumnChange {
                        table: table_owned.clone().into(),
                        pk: parsed.primary_key.clone().into(),
                        cid: "__deleted".into(),
                        val: None,
                        site_id,
                        col_version: tombstone_cv,
                        cl: tombstone_cv,
                        seq: 0,
                        db_version: new_db_version,
                    });
                }
                WriteKind::Insert | WriteKind::Update => {
                    // Clear any tombstone for this row (it's alive again),
                    // but preserve per-column clock entries so col_versions
                    // continue from their previous values.
                    let _ =
                        crate::shadow::clear_tombstone(&inner, &table_owned, &parsed.primary_key)
                            .await;

                    for (seq, (col, val)) in parsed.columns.iter().enumerate() {
                        // Get current col_version and increment
                        let current_cv = crate::shadow::get_col_version(
                            &inner,
                            &table_owned,
                            &parsed.primary_key,
                            col,
                        )
                        .await
                        .unwrap_or(0);
                        let new_cv = current_cv + 1;

                        if let Err(e) = crate::shadow::upsert_clock_entry(
                            &inner,
                            &table_owned,
                            &parsed.primary_key,
                            col,
                            new_cv,
                            new_db_version,
                            &site_id,
                            seq as u32,
                        )
                        .await
                        {
                            log::error!("Failed to upsert clock entry: {}", e);
                        }

                        changes.push(ColumnChange {
                            table: table_owned.clone().into(),
                            pk: parsed.primary_key.clone().into(),
                            cid: col.clone().into(),
                            val: Some(val.clone()),
                            site_id,
                            col_version: new_cv,
                            cl: new_cv,
                            seq: seq as u32,
                            db_version: new_db_version,
                        });
                    }
                }
            }

            let changeset = SyncChangeset {
                site_id,
                db_version: new_db_version,
                changes,
            };

            let _ = shared.sync_tx.send(changeset).await;
            drop(ver); // Release lock after changeset is sent
        });

        match persist_rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(DbErr::Custom("db_version persist task panicked".into())),
        }
    }
}

/// Extract the primary key value from an INSERT statement.
fn extract_pk_from_insert(sql: &str, pk_column: &str) -> String {
    let col_start = match sql.find('(') {
        Some(i) => i + 1,
        None => return String::new(),
    };
    let col_end = match sql[col_start..].find(')') {
        Some(i) => col_start + i,
        None => return String::new(),
    };
    let columns: Vec<&str> = sql[col_start..col_end]
        .split(',')
        .map(|c| c.trim().trim_matches('"').trim_matches('`'))
        .collect();

    let pk_idx = match columns.iter().position(|c| *c == pk_column) {
        Some(i) => i,
        None => return String::new(),
    };

    let upper = sql.to_uppercase();
    let values_pos = match upper.find("VALUES") {
        Some(i) => i,
        None => return String::new(),
    };
    let val_start = match sql[values_pos..].find('(') {
        Some(i) => values_pos + i + 1,
        None => return String::new(),
    };
    let val_end = match sql[val_start..].rfind(')') {
        Some(i) => val_start + i,
        None => return String::new(),
    };

    let values = split_sql_values(&sql[val_start..val_end]);
    values
        .get(pk_idx)
        .map(|v| v.trim().trim_matches('\'').to_string())
        .unwrap_or_default()
}

fn extract_pk_from_where(sql: &str, pk_column: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    let where_pos = match upper.find("WHERE") {
        Some(i) => i + 5,
        None => return String::new(),
    };
    let after_where = &sql[where_pos..];
    let search = after_where.to_ascii_uppercase();
    let pk_upper = pk_column.to_ascii_uppercase();

    let needle_quoted = format!("\"{}\"", pk_upper);
    let col_pos =
        find_whole_word(&search, &needle_quoted).or_else(|| find_whole_word(&search, &pk_upper));

    let col_pos = match col_pos {
        Some(i) => i,
        None => return String::new(),
    };

    let after_col = &after_where[col_pos..];
    let eq_pos = match after_col.find('=') {
        Some(i) => i + 1,
        None => return String::new(),
    };

    let value_part = after_col[eq_pos..].trim_start();
    let end = value_part
        .find([' ', '\n', '\r'])
        .unwrap_or(value_part.len());
    let value = &value_part[..end];
    value.trim().trim_matches('\'').to_string()
}

/// Find `needle` in `haystack` only at word boundaries.
fn find_whole_word(haystack: &str, needle: &str) -> Option<usize> {
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let abs_pos = start + pos;
        let before_ok = abs_pos == 0
            || !haystack.as_bytes()[abs_pos - 1].is_ascii_alphanumeric()
                && haystack.as_bytes()[abs_pos - 1] != b'_';
        let after_pos = abs_pos + needle.len();
        let after_ok = after_pos >= haystack.len()
            || !haystack.as_bytes()[after_pos].is_ascii_alphanumeric()
                && haystack.as_bytes()[after_pos] != b'_';
        if before_ok && after_ok {
            return Some(abs_pos);
        }
        start = abs_pos + 1;
    }
    None
}

/// Split a SQL VALUES list respecting single-quoted strings.
fn split_sql_values(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            ',' if !in_quote => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Extract column names from a SQL statement.
#[cfg(test)]
fn extract_columns(sql: &str, kind: &WriteKind) -> Option<Vec<String>> {
    match kind {
        WriteKind::Insert => {
            let col_start = sql.find('(')?;
            let col_end = col_start + 1 + sql[col_start + 1..].find(')')?;
            let cols = sql[col_start + 1..col_end]
                .split(',')
                .map(|c| c.trim().trim_matches('"').trim_matches('`').to_string())
                .collect();
            Some(cols)
        }
        WriteKind::Update => {
            let upper = sql.to_uppercase();
            let set_pos = upper.find("SET ")? + 4;
            let end_pos = upper[set_pos..]
                .find("WHERE")
                .map(|i| set_pos + i)
                .unwrap_or(sql.len());
            let set_clause = &sql[set_pos..end_pos];
            let cols = set_clause
                .split(',')
                .filter_map(|part| {
                    let eq = part.find('=')?;
                    Some(
                        part[..eq]
                            .trim()
                            .trim_matches('"')
                            .trim_matches('`')
                            .to_string(),
                    )
                })
                .collect();
            Some(cols)
        }
        WriteKind::Delete => None,
    }
}

impl ConnectionTrait for WaveSyncDb {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.inner.get_database_backend()
    }

    fn execute_raw<'life0, 'async_trait>(
        &'life0 self,
        stmt: Statement,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ExecResult, DbErr>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let resolved_sql = stmt.to_string();
        Box::pin(async move {
            let result = self.inner.inner.execute_raw(stmt).await?;
            if let Some((kind, table)) = classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql).await?;
            }
            Ok(result)
        })
    }

    fn execute_unprepared<'life0, 'life1, 'async_trait>(
        &'life0 self,
        sql: &'life1 str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ExecResult, DbErr>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let sql_owned = sql.to_string();
        Box::pin(async move {
            let result = self.inner.inner.execute_unprepared(&sql_owned).await?;
            if let Some((kind, table)) = classify_write(&sql_owned) {
                self.dispatch_sync(kind, &table, &sql_owned).await?;
            }
            Ok(result)
        })
    }

    fn query_one_raw<'life0, 'async_trait>(
        &'life0 self,
        stmt: Statement,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Option<QueryResult>, DbErr>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let resolved_sql = stmt.to_string();
        Box::pin(async move {
            let result = self.inner.inner.query_one_raw(stmt).await?;
            if let Some((kind, table)) = classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql).await?;
            }
            Ok(result)
        })
    }

    fn query_all_raw<'life0, 'async_trait>(
        &'life0 self,
        stmt: Statement,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<QueryResult>, DbErr>> + Send + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let resolved_sql = stmt.to_string();
        Box::pin(async move {
            let result = self.inner.inner.query_all_raw(stmt).await?;
            if let Some((kind, table)) = classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql).await?;
            }
            Ok(result)
        })
    }
}

/// Builder for declaring which entities participate in sync.
pub struct SchemaBuilder<'a> {
    db: &'a WaveSyncDb,
    entries: Vec<EntityEntry>,
    crate_name: Option<String>,
}

struct EntityEntry {
    create_sql: String,
    meta: TableMeta,
    synced: bool,
}

impl<'a> SchemaBuilder<'a> {
    /// Register a SeaORM entity for sync.
    pub fn register<E>(mut self, _entity: E) -> Self
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        self.push_entity::<E>(true);
        self
    }

    /// Register a SeaORM entity as local-only.
    pub fn register_local<E>(mut self, _entity: E) -> Self
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        self.push_entity::<E>(false);
        self
    }

    /// Create all registered tables and register synced ones for P2P replication.
    pub async fn sync(self) -> Result<(), DbErr> {
        for entry in &self.entries {
            self.db
                .inner
                .inner
                .execute_unprepared(&entry.create_sql)
                .await?;
            if entry.synced {
                // Create shadow table for synced entities
                crate::shadow::create_shadow_table(&self.db.inner.inner, &entry.meta.table_name)
                    .await?;
                self.db.register_table(entry.meta.clone());
            }
        }
        // Persist the crate name so background sync can reconstruct the registry
        if let Some(crate_name) = &self.crate_name
            && let Some(config_path) = SyncConfig::config_path(&self.db.inner.database_url)
            && let Ok(json) = std::fs::read_to_string(&config_path)
            && let Ok(mut config) = serde_json::from_str::<SyncConfig>(&json)
        {
            config.crate_name = Some(crate_name.clone());
            if let Ok(updated) = serde_json::to_string_pretty(&config) {
                let _ = std::fs::write(&config_path, updated);
            }
        }
        // Signal the engine that tables are registered and sync can begin
        self.db.inner.registry_ready.notify_one();
        Ok(())
    }

    fn push_entity<E>(&mut self, synced: bool)
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let backend = self.db.get_database_backend();
        let schema = Schema::new(backend);
        let create_sql = schema
            .create_table_from_entity(E::default())
            .if_not_exists()
            .to_owned()
            .to_string(SqliteQueryBuilder);

        let entity = E::default();
        let table_name = entity.table_name().to_string();
        let columns: Vec<String> = E::Column::iter()
            .map(|c| sea_orm::IdenStatic::as_str(&c).to_string())
            .collect();
        let primary_key_column = E::PrimaryKey::iter()
            .next()
            .map(|pk| sea_orm::IdenStatic::as_str(&pk.into_column()).to_string())
            .unwrap_or_default();

        self.entries.push(EntityEntry {
            create_sql,
            meta: TableMeta {
                table_name,
                primary_key_column,
                columns,
                delete_policy: DeletePolicy::default(),
            },
            synced,
        });
    }
}

/// Parse a multiaddr string and replace its first `/dns4/` or `/dns6/` hop
/// with the corresponding `/ip4/` or `/ip6/` hop, resolved via the OS
/// resolver (`getaddrinfo` underneath `tokio::net::lookup_host`).
///
/// Why this exists: libp2p's `dns::Transport` delegates to `hickory-resolver`
/// which on iOS cannot load the system DNS configuration and ends up
/// unable to resolve anything. Pre-resolving in the builder sidesteps that
/// entirely while leaving behaviour on desktop and Android unchanged —
/// the OS resolver is what libp2p would have used eventually.
///
/// If resolution fails, the original multiaddr is returned unchanged and a
/// warning is logged. That way hosts that are temporarily unreachable do
/// not prevent the engine from starting; libp2p will surface a dial error
/// later with its own diagnostics.
async fn parse_and_resolve_multiaddr(addr_str: &str) -> Result<libp2p::Multiaddr, String> {
    use libp2p::multiaddr::Protocol;

    let original: libp2p::Multiaddr = addr_str
        .parse()
        .map_err(|e| format!("bad multiaddr '{addr_str}': {e}"))?;

    let mut protos: Vec<Protocol<'static>> = original.iter().map(|p| p.acquire()).collect();
    let mut resolved_once = false;

    for slot in protos.iter_mut() {
        if resolved_once {
            break;
        }
        match slot {
            Protocol::Dns4(host) | Protocol::Dns(host) => {
                let host_str = host.to_string();
                match lookup_first_addr(&host_str).await {
                    Ok(std::net::IpAddr::V4(v4)) => {
                        *slot = Protocol::Ip4(v4);
                        resolved_once = true;
                    }
                    Ok(std::net::IpAddr::V6(v6)) => {
                        *slot = Protocol::Ip6(v6);
                        resolved_once = true;
                    }
                    Err(e) => {
                        log::warn!(
                            "DNS resolution failed for '{host_str}' in '{addr_str}': {e}; \
                             passing multiaddr through to libp2p unchanged"
                        );
                        return Ok(original);
                    }
                }
            }
            Protocol::Dns6(host) => {
                let host_str = host.to_string();
                match lookup_first_addr(&host_str).await {
                    Ok(std::net::IpAddr::V6(v6)) => {
                        *slot = Protocol::Ip6(v6);
                        resolved_once = true;
                    }
                    Ok(std::net::IpAddr::V4(v4)) => {
                        // Unusual (AAAA requested, A returned); use it anyway.
                        *slot = Protocol::Ip4(v4);
                        resolved_once = true;
                    }
                    Err(e) => {
                        log::warn!(
                            "DNS resolution failed for '{host_str}' in '{addr_str}': {e}; \
                             passing multiaddr through to libp2p unchanged"
                        );
                        return Ok(original);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(protos.into_iter().collect())
}

async fn lookup_first_addr(host: &str) -> std::io::Result<std::net::IpAddr> {
    // `tokio::net::lookup_host` expects `host:port`; we use port 0 because
    // only the IP matters here.
    tokio::net::lookup_host(format!("{host}:0"))
        .await?
        .map(|sa| sa.ip())
        .next()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no A/AAAA for {host}"),
            )
        })
}

/// Persisted sync configuration for background sync services.
///
/// Written to `{db_directory}/.wavesync_config.json` during [`WaveSyncDbBuilder::build()`].
/// Read by [`background_sync()`](crate::background_sync::background_sync) to reconstruct
/// the builder without the app developer passing any configuration.
///
/// **Security note**: The passphrase is stored in plaintext. On Android/iOS the app's
/// data directory is sandboxed (same protection as the SQLite database itself).
#[derive(Serialize, Deserialize)]
pub struct SyncConfig {
    pub database_url: String,
    pub topic: String,
    pub relay_server: Option<String>,
    pub passphrase: Option<String>,
    pub rendezvous_server: Option<String>,
    pub bootstrap_peers: Vec<String>,
    pub api_key: Option<String>,
    pub ipv6: bool,
    pub crate_name: Option<String>,
    /// Firebase project ID for background service cold-start init.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fcm_project_id: Option<String>,
    /// Firebase application ID for background service cold-start init.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fcm_app_id: Option<String>,
    /// Firebase API key for background service cold-start init.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fcm_api_key: Option<String>,
}

impl SyncConfig {
    /// Derive the config file path from a SQLite database URL.
    ///
    /// The config is stored alongside the database file as `.wavesync_config.json`.
    pub fn config_path(database_url: &str) -> Option<PathBuf> {
        // Strip the "sqlite:" or "sqlite://" prefix and query parameters
        let path_str = database_url
            .strip_prefix("sqlite://")
            .or_else(|| database_url.strip_prefix("sqlite:"))
            .unwrap_or(database_url);
        let path_str = path_str.split('?').next().unwrap_or(path_str);
        let db_path = PathBuf::from(path_str);
        db_path
            .parent()
            .map(|dir| dir.join(".wavesync_config.json"))
    }

    /// Read a previously saved config from the database directory.
    pub fn load(database_url: &str) -> Result<Self, String> {
        let path = Self::config_path(database_url)
            .ok_or_else(|| "Cannot derive config path from database URL".to_string())?;
        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read config at {}: {e}", path.display()))?;
        serde_json::from_str(&json)
            .map_err(|e| format!("Invalid config JSON at {}: {e}", path.display()))
    }

    /// Save this config to the database directory.
    fn save(&self) -> Result<(), String> {
        let path = Self::config_path(&self.database_url)
            .ok_or_else(|| "Cannot derive config path from database URL".to_string())?;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize config: {e}"))?;
        std::fs::write(&path, json)
            .map_err(|e| format!("Failed to write config to {}: {e}", path.display()))
    }
}

/// Builder for `WaveSyncDb`.
pub struct WaveSyncDbBuilder {
    database_url: String,
    node_id: Option<NodeId>,
    relay_server: Option<String>,
    topic: String,
    sync_interval: std::time::Duration,
    mdns_query_interval: std::time::Duration,
    mdns_ttl: std::time::Duration,
    group_key: Option<crate::auth::GroupKey>,
    passphrase: Option<String>,
    bootstrap_peers: Vec<String>,
    rendezvous_server: Option<String>,
    rendezvous_discover_interval: std::time::Duration,
    rendezvous_ttl: u64,
    ipv6: bool,
    push_token: Option<(String, String)>,
    #[cfg(feature = "push-sync")]
    fcm_credentials: Option<crate::push::FcmCredentials>,
    api_key: Option<String>,
    keep_alive_interval: std::time::Duration,
    circuit_max_duration: std::time::Duration,
}

impl WaveSyncDbBuilder {
    pub fn new(url: &str, topic: &str) -> Self {
        let defaults = crate::engine::EngineConfig::default();
        Self {
            database_url: url.to_string(),
            node_id: None,
            relay_server: None,
            topic: topic.to_string(),
            sync_interval: defaults.sync_interval,
            mdns_query_interval: defaults.mdns_query_interval,
            mdns_ttl: defaults.mdns_ttl,
            group_key: None,
            passphrase: None,
            bootstrap_peers: Vec::new(),
            rendezvous_server: None,
            rendezvous_discover_interval: defaults.rendezvous_discover_interval,
            rendezvous_ttl: defaults.rendezvous_ttl,
            ipv6: false,
            push_token: None,
            #[cfg(feature = "push-sync")]
            fcm_credentials: None,
            api_key: None,
            keep_alive_interval: defaults.keep_alive_interval,
            circuit_max_duration: defaults.circuit_max_duration,
        }
    }

    pub fn with_node_id(mut self, id: NodeId) -> Self {
        self.node_id = Some(id);
        self
    }

    /// Configure a relay server for NAT traversal.
    ///
    /// The address should include the server's peer ID, e.g.:
    /// `/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...`
    pub fn with_relay_server(mut self, addr: &str) -> Self {
        self.relay_server = Some(addr.to_string());
        self
    }

    /// Connect to WaveSync Cloud managed relay.
    ///
    /// `addr`: full multiaddr including peer ID, e.g.
    ///   `/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...`
    /// `api_key`: raw API key from WaveSync Cloud, e.g. `"wsc_live_xxx"`
    pub fn managed_relay(mut self, addr: &str, api_key: &str) -> Self {
        self.relay_server = Some(addr.to_string());
        self.api_key = Some(api_key.to_string());
        self
    }

    /// Add a static bootstrap peer to dial on startup.
    ///
    /// Bootstrap peers are dialed immediately and treated like mDNS-discovered
    /// peers for sync.
    pub fn with_bootstrap_peer(mut self, addr: &str) -> Self {
        self.bootstrap_peers.push(addr.to_string());
        self
    }

    /// Configure a rendezvous server for WAN peer discovery.
    ///
    /// Peers register under a namespace derived from the passphrase/topic,
    /// enabling discovery without a public DHT. The address should include
    /// the server's peer ID, e.g.: `/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...`
    pub fn with_rendezvous_server(mut self, addr: &str) -> Self {
        self.rendezvous_server = Some(addr.to_string());
        self
    }

    /// Set the interval for rendezvous discovery queries (default: 60s).
    pub fn with_rendezvous_discover_interval(mut self, interval: std::time::Duration) -> Self {
        self.rendezvous_discover_interval = interval;
        self
    }

    /// Set the TTL for rendezvous registration in seconds (default: 300s).
    pub fn with_rendezvous_ttl(mut self, ttl: u64) -> Self {
        self.rendezvous_ttl = ttl;
        self
    }

    /// Enable IPv6 listen addresses in addition to IPv4.
    pub fn with_ipv6(mut self, enabled: bool) -> Self {
        self.ipv6 = enabled;
        self
    }

    pub fn with_sync_interval(mut self, interval: std::time::Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    pub fn with_mdns_query_interval(mut self, interval: std::time::Duration) -> Self {
        self.mdns_query_interval = interval;
        self
    }

    pub fn with_mdns_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.mdns_ttl = ttl;
        self
    }

    pub fn with_passphrase(mut self, passphrase: &str) -> Self {
        self.group_key = Some(crate::auth::GroupKey::from_passphrase(passphrase));
        self.passphrase = Some(passphrase.to_string());
        self
    }

    /// Register a push notification token for mobile wake-up via the relay server.
    ///
    /// When connected to a relay, the engine will send a `RegisterToken` request
    /// so the relay can send silent push notifications when other peers publish changes.
    /// `platform` should be `"Fcm"` or `"Apns"`.
    pub fn with_push_token(mut self, platform: &str, token: &str) -> Self {
        self.push_token = Some((platform.to_string(), token.to_string()));
        self
    }

    /// Configure FCM from a `google-services.json` file for push-based background sync.
    ///
    /// Pass the contents of your Firebase `google-services.json` file (use `include_str!`
    /// to embed it at compile time). WaveSyncDB extracts the Firebase credentials and
    /// handles initialization + token retrieval via JNI automatically.
    ///
    /// On non-Android platforms, the credentials are parsed (to catch errors early)
    /// but initialization is skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let db = WaveSyncDbBuilder::new(db_url, "my-topic")
    ///     .with_relay_server("/dns4/relay.example.com/tcp/4001/p2p/12D3Koo...")
    ///     .with_google_services(include_str!("../google-services.json"))
    ///     .build()
    ///     .await?;
    /// ```
    #[cfg(feature = "push-sync")]
    pub fn with_google_services(mut self, google_services_json: &str) -> Self {
        match crate::push::FcmCredentials::from_google_services_json(google_services_json) {
            Ok(creds) => {
                self.fcm_credentials = Some(creds);
            }
            Err(e) => {
                log::error!("Failed to parse google-services.json: {e}");
            }
        }
        self
    }

    /// Configure FCM with explicit Firebase credentials.
    ///
    /// Use this if you prefer not to use `google-services.json`. Get these values
    /// from Firebase Console → Project Settings → General.
    ///
    /// See [`with_google_services()`](Self::with_google_services) for the simpler approach.
    #[cfg(feature = "push-sync")]
    pub fn with_fcm(mut self, project_id: &str, app_id: &str, api_key: &str) -> Self {
        self.fcm_credentials = Some(crate::push::FcmCredentials {
            project_id: project_id.to_string(),
            app_id: app_id.to_string(),
            api_key: api_key.to_string(),
        });
        self
    }

    /// Set the ping keep-alive interval (default: 90s).
    ///
    /// This should be shorter than the shortest CGNAT mapping timeout in the
    /// network path (typically 2–5 min for UDP). Keeping connections alive
    /// prevents CGNAT from silently dropping relay circuits.
    pub fn with_keep_alive_interval(mut self, interval: std::time::Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// Set the maximum relay circuit duration (default: 3600s).
    ///
    /// Must match the relay server's `--max-circuit-duration` setting.
    /// The engine proactively renews the circuit at 80% of this duration.
    pub fn with_circuit_max_duration(mut self, duration: std::time::Duration) -> Self {
        self.circuit_max_duration = duration;
        self
    }

    #[allow(unused_mut)]
    pub async fn build(mut self) -> Result<WaveSyncDb, DbErr> {
        // Auto-read FCM token from file written by WaveSyncInitProvider / WaveSyncService.
        // The ContentProvider writes the token on a background thread at app startup,
        // so we retry a few times with a short delay to handle the race.
        // Only runs on Android — desktop has no FCM service to write the token file.
        #[cfg(all(feature = "push-sync", target_os = "android"))]
        if self.fcm_credentials.is_some() && self.push_token.is_none() {
            for attempt in 0..5 {
                if let Some(token) = crate::push::read_token_file(&self.database_url) {
                    self.push_token = Some(("Fcm".to_string(), token));
                    break;
                }
                if attempt < 4 {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
            if self.push_token.is_none() {
                log::info!("No FCM token file found — push will be registered on next launch");
            }
        }

        // On iOS the APNs token is written by the Swift `WaveSyncPush`
        // package's `didRegisterForRemoteNotificationsWithDeviceToken:`
        // swizzle (installed at image load by `WaveSyncAppDelegateProxy+load`).
        // The Swift side discovers where to write the token by locating the
        // `.wavesync_config.json` file that `SyncConfig::save` writes below.
        //
        // First, force-load the framework: dx embeds it in `.app/Frameworks/`
        // but does not add an `LC_LOAD_DYLIB` entry on the Rust binary, so
        // dyld would otherwise never load it and `+load` would never run.
        // We dlopen it here, early in `build()`, so the observer registers
        // before `UIApplicationDidFinishLaunchingNotification` fires.
        //
        // Then poll briefly for a token file left by a previous run.
        // First-ever launch produces no file; Swift will write it on the
        // *next* launch once APNs responds, and we pick it up then.
        #[cfg(all(feature = "push-sync", target_os = "ios"))]
        {
            crate::push::load_ios_push_framework();
            if self.push_token.is_none() {
                for attempt in 0..5 {
                    if let Some(token) = crate::push::read_apns_token_file(&self.database_url) {
                        self.push_token = Some(("Apns".to_string(), token));
                        break;
                    }
                    if attempt < 4 {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
                if self.push_token.is_none() {
                    log::info!("No APNs token file found — push will be registered on next launch");
                }
            }
        }

        let mut opts = ConnectOptions::new(&self.database_url);
        // Silence sqlx's per-query INFO logs (one line per SELECT/INSERT/DELETE
        // — and a sync round can be hundreds of queries). Tracing them at this
        // verbosity drowns out engine-level events on logcat. Bump to debug if
        // actually diagnosing slow queries.
        opts.sqlx_logging_level(log::LevelFilter::Debug);
        let inner = Database::connect(opts).await?;

        // Create meta table and get/generate persistent site_id
        crate::shadow::create_meta_table(&inner).await?;
        let site_id = crate::shadow::get_site_id(&inner).await?;

        let node_id = self.node_id.unwrap_or(site_id);

        let db_version = crate::shadow::get_db_version(&inner).await?;

        let (sync_tx, sync_rx) = mpsc::channel::<SyncChangeset>(256);
        let (change_tx, _) = broadcast::channel::<ChangeNotification>(1024);

        let registry = Arc::new(TableRegistry::new());
        let registry_ready = Arc::new(Notify::new());

        // Create peer versions table
        crate::peer_tracker::create_peer_versions_table(&inner).await?;

        let (cmd_tx, cmd_rx) = mpsc::channel::<crate::engine::EngineCommand>(4);

        let network_status = Arc::new(std::sync::RwLock::new(
            crate::network_status::NetworkStatus::default(),
        ));
        let (network_event_tx, _) = broadcast::channel::<crate::network_status::NetworkEvent>(256);

        // Parse multiaddrs for WAN config, pre-resolving any `/dns4/` or
        // `/dns6/` hops against the OS resolver (`getaddrinfo`). libp2p's
        // built-in `dns::Transport` uses `hickory-resolver`, which on iOS
        // cannot read the DNS configuration (there is no `/etc/resolv.conf`
        // and it does not currently consume `SCDynamicStore`), so DNS
        // multiaddrs silently hang. Pre-resolving here lets the system
        // resolver do its job and keeps desktop / Android behaviour
        // unchanged — on those platforms libp2p-dns would have worked
        // anyway, and resolving once up front is indistinguishable.
        let relay_server = match self.relay_server.as_deref() {
            Some(s) => Some(
                parse_and_resolve_multiaddr(s)
                    .await
                    .map_err(|e| DbErr::Custom(format!("Invalid relay server address: {e}")))?,
            ),
            None => None,
        };

        let rendezvous_server =
            match self.rendezvous_server.as_deref() {
                Some(s) => Some(parse_and_resolve_multiaddr(s).await.map_err(|e| {
                    DbErr::Custom(format!("Invalid rendezvous server address: {e}"))
                })?),
                None => None,
            };

        let mut bootstrap_peers: Vec<libp2p::Multiaddr> = Vec::new();
        for s in &self.bootstrap_peers {
            match parse_and_resolve_multiaddr(s).await {
                Ok(addr) => bootstrap_peers.push(addr),
                Err(e) => log::warn!("Skipping invalid bootstrap peer address '{s}': {e}"),
            }
        }

        // Persist config for background sync services (before moving fields)
        // Extract FCM credentials for config persistence (behind feature gate)
        #[cfg(feature = "push-sync")]
        let (fcm_project_id, fcm_app_id, fcm_api_key) = self
            .fcm_credentials
            .as_ref()
            .map(|c| {
                (
                    Some(c.project_id.clone()),
                    Some(c.app_id.clone()),
                    Some(c.api_key.clone()),
                )
            })
            .unwrap_or((None, None, None));
        #[cfg(not(feature = "push-sync"))]
        let (fcm_project_id, fcm_app_id, fcm_api_key) = (None, None, None);

        let sync_config = SyncConfig {
            database_url: self.database_url.clone(),
            topic: self.topic.clone(),
            relay_server: self.relay_server.clone(),
            passphrase: self.passphrase,
            rendezvous_server: self.rendezvous_server.clone(),
            bootstrap_peers: self.bootstrap_peers.clone(),
            api_key: self.api_key.clone(),
            ipv6: self.ipv6,
            crate_name: None, // Set by SchemaBuilder::sync()
            fcm_project_id,
            fcm_app_id,
            fcm_api_key,
        };
        if let Err(e) = sync_config.save() {
            log::warn!("Failed to save sync config for background services: {e}");
        }

        let engine_config = crate::engine::EngineConfig {
            sync_interval: self.sync_interval,
            mdns_query_interval: self.mdns_query_interval,
            mdns_ttl: self.mdns_ttl,
            bootstrap_peers,
            relay_server,
            rendezvous_server,
            rendezvous_discover_interval: self.rendezvous_discover_interval,
            rendezvous_ttl: self.rendezvous_ttl,
            ipv6: self.ipv6,
            push_token: self.push_token,
            api_key: self.api_key,
            keep_alive_interval: self.keep_alive_interval,
            circuit_max_duration: self.circuit_max_duration,
        };

        // Start the P2P engine in a background task
        let engine_handle = crate::engine::start_engine(
            inner.clone(),
            sync_rx,
            change_tx.clone(),
            registry.clone(),
            site_id,
            self.topic,
            engine_config,
            registry_ready.clone(),
            cmd_rx,
            self.group_key,
            network_status.clone(),
            network_event_tx.clone(),
        );

        let db = WaveSyncDb {
            inner: Arc::new(WaveSyncDbInner {
                inner,
                database_url: self.database_url,
                sync_tx,
                change_tx,
                site_id,
                db_version: Mutex::new(db_version),
                node_id,
                registry,
                registry_ready,
                cmd_tx,
                engine_handle: std::sync::Mutex::new(Some(engine_handle)),
                network_status,
                network_event_tx,
            }),
        };

        Ok(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // classify_write tests

    #[test]
    fn test_classify_insert_basic() {
        let result = classify_write(r#"INSERT INTO "tasks" ("id", "name") VALUES ('1', 'foo')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_insert_or_replace() {
        let result = classify_write(r#"INSERT OR REPLACE INTO "tasks" ("id") VALUES ('1')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_insert_or_ignore() {
        let result = classify_write(r#"INSERT OR IGNORE INTO "tasks" ("id") VALUES ('1')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_update() {
        let result = classify_write(r#"UPDATE "tasks" SET "name" = 'bar' WHERE "id" = '1'"#);
        assert_eq!(result, Some((WriteKind::Update, "tasks".to_string())));
    }

    #[test]
    fn test_classify_delete() {
        let result = classify_write(r#"DELETE FROM "tasks" WHERE "id" = '1'"#);
        assert_eq!(result, Some((WriteKind::Delete, "tasks".to_string())));
    }

    #[test]
    fn test_classify_select_returns_none() {
        let result = classify_write(r#"SELECT * FROM "tasks""#);
        assert_eq!(result, None);
    }

    // parse_write_full tests

    #[test]
    fn test_parse_write_insert() {
        let sql = r#"INSERT INTO "tasks" ("id", "title", "done") VALUES ('abc', 'My Task', 0)"#;
        let (kind, table) = classify_write(sql).unwrap();
        assert_eq!(kind, WriteKind::Insert);
        assert_eq!(table, "tasks");
        let parsed = parse_write_full(sql, "id").unwrap();
        assert_eq!(parsed.primary_key, "abc");
        assert_eq!(parsed.columns.len(), 3);
        assert_eq!(parsed.columns[0].0, "id");
        assert_eq!(parsed.columns[0].1, serde_json::json!("abc"));
        assert_eq!(parsed.columns[1].0, "title");
        assert_eq!(parsed.columns[1].1, serde_json::json!("My Task"));
        assert_eq!(parsed.columns[2].0, "done");
        assert_eq!(parsed.columns[2].1, serde_json::json!(0));
    }

    #[test]
    fn test_parse_write_update() {
        let sql = r#"UPDATE "tasks" SET "title" = 'New Title', "done" = 1 WHERE "id" = 'abc'"#;
        let (kind, table) = classify_write(sql).unwrap();
        assert_eq!(kind, WriteKind::Update);
        assert_eq!(table, "tasks");
        let parsed = parse_write_full(sql, "id").unwrap();
        assert_eq!(parsed.primary_key, "abc");
        assert_eq!(parsed.columns.len(), 2);
        assert_eq!(parsed.columns[0].0, "title");
        assert_eq!(parsed.columns[0].1, serde_json::json!("New Title"));
        assert_eq!(parsed.columns[1].0, "done");
        assert_eq!(parsed.columns[1].1, serde_json::json!(1));
    }

    #[test]
    fn test_parse_write_delete() {
        let sql = r#"DELETE FROM "tasks" WHERE "id" = 'abc'"#;
        let (kind, table) = classify_write(sql).unwrap();
        assert_eq!(kind, WriteKind::Delete);
        assert_eq!(table, "tasks");
        let parsed = parse_write_full(sql, "id").unwrap();
        assert_eq!(parsed.primary_key, "abc");
        assert!(parsed.columns.is_empty());
    }

    #[test]
    fn test_sql_value_to_json_null() {
        assert_eq!(sql_value_to_json("NULL"), serde_json::Value::Null);
    }

    #[test]
    fn test_sql_value_to_json_string() {
        assert_eq!(
            sql_value_to_json("'hello'"),
            serde_json::Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_sql_value_to_json_integer() {
        assert_eq!(sql_value_to_json("42"), serde_json::json!(42));
    }

    #[test]
    fn test_sql_value_to_json_bool() {
        assert_eq!(sql_value_to_json("TRUE"), serde_json::Value::Bool(true));
        assert_eq!(sql_value_to_json("FALSE"), serde_json::Value::Bool(false));
    }

    // extract_primary_key tests

    #[test]
    fn test_extract_pk_from_insert() {
        let sql = r#"INSERT INTO "tasks" ("id", "name") VALUES ('abc-123', 'my task')"#;
        let pk = extract_pk_from_insert(sql, "id");
        assert_eq!(pk, "abc-123");
    }

    #[test]
    fn test_extract_pk_from_where_update() {
        let sql = r#"UPDATE "tasks" SET "name" = 'bar' WHERE "id" = 'abc-123'"#;
        let pk = extract_pk_from_where(sql, "id");
        assert_eq!(pk, "abc-123");
    }

    // split_sql_values tests

    #[test]
    fn test_split_sql_values_with_quotes() {
        let values = split_sql_values("'hello, world', 42, 'foo'");
        assert_eq!(values, vec!["'hello, world'", " 42", " 'foo'"]);
    }

    // classify_write edge cases

    #[test]
    fn test_classify_leading_whitespace() {
        let result = classify_write(r#"   INSERT INTO "tasks" ("id") VALUES ('1')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_multiline_insert() {
        let result = classify_write("INSERT\n  INTO \"tasks\" (\"id\") VALUES ('1')");
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_backtick_table() {
        let result = classify_write("INSERT INTO `tasks` (\"id\") VALUES ('1')");
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_update_no_set() {
        let result = classify_write("UPDATE tasks");
        assert_eq!(result, None);
    }

    #[test]
    fn test_extract_pk_delete_from_where() {
        let sql = r#"DELETE FROM "tasks" WHERE "id" = 'abc'"#;
        let pk = extract_pk_from_where(sql, "id");
        assert_eq!(pk, "abc");
    }

    #[test]
    fn test_extract_pk_insert_no_parens() {
        let sql = "INSERT INTO tasks";
        let pk = extract_pk_from_insert(sql, "id");
        assert_eq!(pk, "");
    }

    #[test]
    fn test_extract_pk_insert_no_values() {
        let sql = r#"INSERT INTO "tasks" ("id")"#;
        let pk = extract_pk_from_insert(sql, "id");
        assert_eq!(pk, "");
    }

    #[test]
    fn test_extract_columns_insert() {
        let sql = r#"INSERT INTO "tasks" ("id", "name", "done") VALUES ('1', 'foo', 0)"#;
        let cols = extract_columns(sql, &WriteKind::Insert);
        assert_eq!(
            cols,
            Some(vec![
                "id".to_string(),
                "name".to_string(),
                "done".to_string()
            ])
        );
    }

    #[test]
    fn test_extract_columns_update() {
        let sql = r#"UPDATE "tasks" SET "name" = 'bar', "done" = 1 WHERE "id" = '1'"#;
        let cols = extract_columns(sql, &WriteKind::Update);
        assert_eq!(cols, Some(vec!["name".to_string(), "done".to_string()]));
    }

    #[test]
    fn test_extract_columns_delete() {
        let sql = "DELETE FROM tasks WHERE id = 1";
        let cols = extract_columns(sql, &WriteKind::Delete);
        assert_eq!(cols, None);
    }

    #[test]
    fn test_split_sql_values_empty() {
        let values = split_sql_values("");
        assert_eq!(values, vec![""]);
    }

    #[test]
    fn test_split_sql_values_single() {
        let values = split_sql_values("42");
        assert_eq!(values, vec!["42"]);
    }

    #[test]
    fn test_split_sql_values_no_quotes() {
        let values = split_sql_values("1, 2, 3");
        assert_eq!(values, vec!["1", " 2", " 3"]);
    }

    // --- Bug regression tests ---

    /// H3: multi-row INSERT — extract_pk_from_insert only returns first PK
    #[test]
    fn test_h3_multi_row_insert_first_pk_only() {
        let sql = r#"INSERT INTO "tasks" ("id", "title") VALUES ('a', 'x'), ('b', 'y')"#;
        let pk = extract_pk_from_insert(sql, "id");
        // Documents H3 bug: only first row's PK is returned
        assert_eq!(pk, "a", "H3: only first PK extracted from multi-row INSERT");
    }

    /// H5: PK with spaces is truncated by extract_pk_from_where
    #[test]
    fn test_h5_pk_with_spaces_truncated() {
        let sql = r#"UPDATE "tasks" SET "title" = 'x' WHERE "id" = 'hello world'"#;
        let pk = extract_pk_from_where(sql, "id");
        // H5 bug: PK is truncated at the space
        assert!(
            pk == "hello" || pk == "hello world",
            "H5: PK '{}' — expected 'hello world' (fixed) or 'hello' (bug)",
            pk
        );
    }

    /// H5: comparison operator >= confuses the parser
    #[test]
    fn test_h5_pk_with_comparison_operators() {
        let sql = r#"DELETE FROM "tasks" WHERE "id" >= 10"#;
        let pk = extract_pk_from_where(sql, "id");
        // The parser finds '=' inside '>=' — the PK extracted may be wrong
        // This documents the known limitation
        assert!(
            !pk.is_empty(),
            "H5: parser should extract something even with >="
        );
    }

    /// M12: non-ASCII in SET clause
    #[test]
    fn test_m12_unicode_set_clause() {
        let sql = r#"UPDATE "tasks" SET "title" = 'café' WHERE "id" = 'pk1'"#;
        let parsed = parse_write_full(sql, "id").unwrap();
        assert_eq!(parsed.columns.len(), 1);
        assert_eq!(parsed.columns[0].0, "title");
        assert_eq!(parsed.columns[0].1, serde_json::json!("café"));
    }

    /// M5: escaped quotes in VALUES
    #[test]
    fn test_m5_escaped_quotes_edge() {
        let sql = r#"INSERT INTO "tasks" ("id", "title", "completed") VALUES ('pk1', 'it''s', 0)"#;
        let parsed = parse_write_full(sql, "id").unwrap();
        // The split_sql_values should handle the escaped quote
        assert_eq!(parsed.primary_key, "pk1");
        assert_eq!(parsed.columns[1].0, "title");
        // The value should have the unescaped quote
        assert_eq!(parsed.columns[1].1, serde_json::json!("it's"));
    }

    /// H4: unparseable SQL returns None
    #[test]
    fn test_parse_write_full_returns_none_for_gibberish() {
        assert!(parse_write_full("THIS IS NOT SQL", "id").is_none());
        assert!(parse_write_full("SELECT * FROM tasks", "id").is_none());
    }

    /// Internal table prefix still classifies (but dispatch_sync skips them)
    #[test]
    fn test_classify_write_internal_table_prefix() {
        let result =
            classify_write(r#"INSERT INTO "_wavesync_meta" ("key", "value") VALUES ('x', 'y')"#);
        assert!(
            result.is_some(),
            "classify_write should still parse _wavesync tables"
        );
        let (_, table) = result.unwrap();
        assert!(table.starts_with("_wavesync"));
    }

    /// sql_value_to_json for float
    #[test]
    fn test_sql_value_to_json_float() {
        let val = sql_value_to_json("3.14");
        assert!(val.is_number());
        assert!((val.as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    /// sql_value_to_json for hex blob — falls back to string
    #[test]
    fn test_sql_value_to_json_hex_blob() {
        let val = sql_value_to_json("X'DEADBEEF'");
        assert!(val.is_string(), "Hex blob should fall back to string");
    }
}
