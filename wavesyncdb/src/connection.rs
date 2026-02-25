use std::sync::{Arc, Mutex};

use sea_orm::{
    sea_query::SqliteQueryBuilder, ConnectOptions, ConnectionTrait, Database, DatabaseBackend,
    DatabaseConnection, DbErr, EntityTrait, ExecResult, Iterable, PrimaryKeyToColumn, QueryResult,
    Schema, Statement,
};
use tokio::sync::{broadcast, mpsc};

use crate::messages::{ChangeNotification, NodeId, SyncOperation, WriteKind};
use crate::registry::{SyncEntityInfo, TableMeta, TableRegistry};

/// A SeaORM connection wrapper that transparently intercepts write operations
/// and dispatches them to the sync engine.
pub struct WaveSyncDb {
    inner: DatabaseConnection,
    sync_tx: mpsc::Sender<SyncOperation>,
    change_tx: broadcast::Sender<ChangeNotification>,
    hlc: Arc<Mutex<uhlc::HLC>>,
    node_id: NodeId,
    registry: Arc<TableRegistry>,
}

impl WaveSyncDb {
    /// Get a reference to the underlying SeaORM connection.
    /// Use this when you need to bypass sync interception (e.g., applying remote ops).
    pub fn inner(&self) -> &DatabaseConnection {
        &self.inner
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get a handle to the change notification broadcast channel.
    pub fn change_rx(&self) -> broadcast::Receiver<ChangeNotification> {
        self.change_tx.subscribe()
    }

    /// Get a reference to the change notification sender.
    pub fn change_tx(&self) -> &broadcast::Sender<ChangeNotification> {
        &self.change_tx
    }

    /// Get a reference to the sync operation sender.
    pub fn sync_tx(&self) -> &mpsc::Sender<SyncOperation> {
        &self.sync_tx
    }

    /// Get a reference to the HLC.
    pub fn hlc(&self) -> &Arc<Mutex<uhlc::HLC>> {
        &self.hlc
    }

    /// Get a reference to the table registry.
    pub fn registry(&self) -> &Arc<TableRegistry> {
        &self.registry
    }

    /// Register a table for sync.
    pub fn register_table(&self, meta: TableMeta) {
        self.registry.register(meta);
    }

    /// Start building the sync schema.
    ///
    /// Returns a [`SchemaBuilder`] that lets you register multiple entities
    /// and then apply them all at once with `.sync().await`.
    pub fn schema(&self) -> SchemaBuilder<'_> {
        SchemaBuilder {
            db: self,
            entries: Vec::new(),
        }
    }

    /// Auto-discover entities registered via `#[derive(SyncEntity)]` and build a
    /// [`SchemaBuilder`] populated with all matching entities.
    ///
    /// `prefix` is matched against each entity's `module_path!()`. Typically you
    /// pass your crate name:
    ///
    /// ```ignore
    /// db.get_schema_registry(module_path!().split("::").next().unwrap())
    ///     .sync()
    ///     .await?;
    /// ```
    pub fn get_schema_registry(&self, prefix: &str) -> SchemaBuilder<'_> {
        let mut builder = self.schema();
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
    ///
    /// This replaces the manual pattern of `Schema::create_table_from_entity` +
    /// `register_table(TableMeta { … })` with a single call.
    pub async fn sync_entity<E>(&self) -> Result<(), DbErr>
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        // 1. Create the table if it doesn't exist
        let backend = self.get_database_backend();
        let schema = Schema::new(backend);
        let create_stmt = schema
            .create_table_from_entity(E::default())
            .if_not_exists()
            .to_owned();
        self.inner
            .execute_unprepared(&create_stmt.to_string(SqliteQueryBuilder))
            .await?;

        // 2. Extract metadata from the entity
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

        // 3. Register for sync
        self.register_table(TableMeta {
            table_name,
            primary_key_column,
            columns,
        });

        Ok(())
    }

    /// Broadcast a change notification (used by the engine for remote changes).
    pub fn notify_change(&self, notification: ChangeNotification) {
        let _ = self.change_tx.send(notification);
    }

    /// Generate a new HLC timestamp, returning (physical_time, counter).
    fn next_hlc(&self) -> (u64, u32) {
        let hlc = self.hlc.lock().unwrap();
        let ts = hlc.new_timestamp();
        let ntp = ts.get_time();
        (ntp.as_u64(), ts.get_id().to_le_bytes()[0] as u32)
    }

    /// Try to classify a SQL statement as a write and extract relevant info.
    fn classify_write(sql: &str) -> Option<(WriteKind, String)> {
        let trimmed = sql.trim_start().to_uppercase();
        if trimmed.starts_with("INSERT") {
            // Extract table name: INSERT INTO <table> ...
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 3 {
                let table = parts[2].trim_matches('"').trim_matches('`').to_string();
                return Some((WriteKind::Insert, table));
            }
        } else if trimmed.starts_with("UPDATE") {
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 2 {
                let table = parts[1].trim_matches('"').trim_matches('`').to_string();
                return Some((WriteKind::Update, table));
            }
        } else if trimmed.starts_with("DELETE") {
            // DELETE FROM <table> ...
            let parts: Vec<&str> = sql.split_whitespace().collect();
            if parts.len() >= 3 {
                let table = parts[2].trim_matches('"').trim_matches('`').to_string();
                return Some((WriteKind::Delete, table));
            }
        }
        None
    }

    /// After a successful write, create and dispatch a sync operation.
    /// `resolved_sql` must be a fully-resolved SQL string with parameter values
    /// inlined (no `?` placeholders), so it can be executed on remote nodes.
    fn dispatch_sync(&self, kind: WriteKind, table: &str, resolved_sql: &str) {
        if table.starts_with("_wavesync") {
            return; // Don't sync internal tables
        }
        if !self.registry.is_registered(table) {
            return; // Don't sync unregistered tables
        }

        let (hlc_time, hlc_counter) = self.next_hlc();
        let op = SyncOperation {
            op_id: rand_op_id(),
            hlc_time,
            hlc_counter,
            node_id: self.node_id,
            table: table.to_string(),
            kind: kind.clone(),
            primary_key: String::new(), // Will be enriched by caller or from RETURNING
            data: Some(resolved_sql.as_bytes().to_vec()),
            columns: None,
        };

        let sync_tx = self.sync_tx.clone();
        let change_tx = self.change_tx.clone();
        let table_owned = table.to_string();

        tokio::spawn(async move {
            let _ = sync_tx.send(op).await;
            let _ = change_tx.send(ChangeNotification {
                table: table_owned,
                kind,
                primary_key: String::new(),
            });
        });
    }
}

fn rand_op_id() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    nanos ^ (std::process::id() as u128) << 64
}

impl ConnectionTrait for WaveSyncDb {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.inner.get_database_backend()
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
            let result = self.inner.execute_raw(stmt).await?;
            if let Some((kind, table)) = Self::classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql);
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
            let result = self.inner.execute_unprepared(&sql_owned).await?;
            if let Some((kind, table)) = Self::classify_write(&sql_owned) {
                self.dispatch_sync(kind, &table, &sql_owned);
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
        // SeaORM uses query_one_raw for INSERT ... RETURNING, so we intercept writes here too
        let resolved_sql = stmt.to_string();
        Box::pin(async move {
            let result = self.inner.query_one_raw(stmt).await?;
            if let Some((kind, table)) = Self::classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql);
            }
            Ok(result)
        })
    }

    fn query_all_raw<'life0, 'async_trait>(
        &'life0 self,
        stmt: Statement,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<QueryResult>, DbErr>>
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
            let result = self.inner.query_all_raw(stmt).await?;
            if let Some((kind, table)) = Self::classify_write(&resolved_sql) {
                self.dispatch_sync(kind, &table, &resolved_sql);
            }
            Ok(result)
        })
    }
}

/// Builder for declaring which entities participate in sync.
///
/// Created via [`WaveSyncDb::schema()`]. Collects entity registrations,
/// then applies them all in [`sync()`](SchemaBuilder::sync).
///
/// # Example
///
/// ```ignore
/// db.schema()
///     .register(task::Entity)
///     .register(user::Entity)
///     .register_local(cache::Entity)
///     .sync()
///     .await?;
/// ```
pub struct SchemaBuilder<'a> {
    db: &'a WaveSyncDb,
    entries: Vec<EntityEntry>,
}

struct EntityEntry {
    create_sql: String,
    meta: TableMeta,
    synced: bool,
}

impl<'a> SchemaBuilder<'a> {
    /// Register a SeaORM entity for sync.
    ///
    /// The entity's table will be created (if not exists) and registered
    /// for P2P sync when [`sync()`](SchemaBuilder::sync) is called.
    pub fn register<E>(mut self, _entity: E) -> Self
    where
        E: EntityTrait,
        <E::Column as std::str::FromStr>::Err: std::fmt::Debug,
    {
        self.push_entity::<E>(true);
        self
    }

    /// Register a SeaORM entity as local-only.
    ///
    /// The entity's table will be created (if not exists) but will **not**
    /// participate in P2P sync.
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
        for entry in self.entries {
            self.db
                .inner
                .execute_unprepared(&entry.create_sql)
                .await?;
            if entry.synced {
                self.db.register_table(entry.meta);
            }
        }
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
            },
            synced,
        });
    }
}

/// Builder for `WaveSyncDb`.
pub struct WaveSyncDbBuilder {
    database_url: String,
    node_id: Option<NodeId>,
    relay_server: Option<String>,
    topic: String,
}

impl WaveSyncDbBuilder {
    pub fn new(url: &str, topic: &str) -> Self {
        Self {
            database_url: url.to_string(),
            node_id: None,
            relay_server: None,
            topic: topic.to_string(),
        }
    }

    pub fn with_node_id(mut self, id: NodeId) -> Self {
        self.node_id = Some(id);
        self
    }

    pub fn with_relay_server(mut self, addr: &str) -> Self {
        self.relay_server = Some(addr.to_string());
        self
    }

    pub async fn build(self) -> Result<WaveSyncDb, DbErr> {
        let opts = ConnectOptions::new(&self.database_url);
        let inner = Database::connect(opts).await?;

        let node_id = self.node_id.unwrap_or_else(|| {
            let mut id = [0u8; 16];
            let pid = std::process::id().to_le_bytes();
            id[..4].copy_from_slice(&pid);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .to_le_bytes();
            id[4..].copy_from_slice(&now[..12]);
            id
        });

        let node_u128 = u128::from_le_bytes(node_id);
        // uhlc::ID requires NonZero, ensure we have a non-zero value
        let nz = std::num::NonZeroU128::new(node_u128)
            .unwrap_or(std::num::NonZeroU128::new(1).unwrap());
        let hlc_id = uhlc::ID::from(nz);
        let hlc = uhlc::HLCBuilder::new().with_id(hlc_id).build();

        let (sync_tx, sync_rx) = mpsc::channel::<SyncOperation>(256);
        let (change_tx, _) = broadcast::channel::<ChangeNotification>(256);

        let registry = Arc::new(TableRegistry::new());

        // Create the sync log table
        crate::sync_log::create_log_table(&inner).await?;

        let db = WaveSyncDb {
            inner: inner.clone(),
            sync_tx,
            change_tx: change_tx.clone(),
            hlc: Arc::new(Mutex::new(hlc)),
            node_id,
            registry: registry.clone(),
        };

        // Start the P2P engine in a background task
        crate::engine::start_engine(
            inner,
            sync_rx,
            change_tx,
            registry,
            node_id,
            self.topic,
            self.relay_server,
        );

        Ok(db)
    }
}
