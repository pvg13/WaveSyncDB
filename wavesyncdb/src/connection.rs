use std::sync::{Arc, Mutex};

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, ExecResult, Iterable, PrimaryKeyToColumn, QueryResult, Schema, Statement,
    sea_query::SqliteQueryBuilder,
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

impl PartialEq for WaveSyncDb {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
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
        let raw = ts.get_time().as_u64();
        // In uhlc, the counter is embedded in the lower 4 bits of the NTP64 timestamp
        (raw, (raw & 0xF) as u32)
    }

    /// Try to classify a SQL statement as a write and extract relevant info.
    ///
    /// Uses keyword-anchor parsing to handle variants like `INSERT OR REPLACE INTO`,
    /// multi-line SQL, and leading whitespace/comments.
    fn classify_write(sql: &str) -> Option<(WriteKind, String)> {
        let upper = sql.trim_start().to_uppercase();
        if upper.starts_with("INSERT") {
            // INSERT [OR REPLACE|OR IGNORE] INTO <table> ...
            let into_pos = upper.find("INTO ")?;
            let after_into = &sql[into_pos + 5..];
            let table = after_into.split_whitespace().next()?;
            let table = table.trim_matches('"').trim_matches('`').to_string();
            Some((WriteKind::Insert, table))
        } else if upper.starts_with("UPDATE") {
            // UPDATE <table> SET ...
            let parts: Vec<&str> = sql.split_whitespace().collect();
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
            let after_from = &sql[from_pos + 5..];
            let table = after_from.split_whitespace().next()?;
            let table = table.trim_matches('"').trim_matches('`').to_string();
            Some((WriteKind::Delete, table))
        } else {
            None
        }
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

        let pk_column = self.registry.get(table).map(|m| m.primary_key_column);
        let primary_key = pk_column
            .as_deref()
            .map(|pk_col| extract_primary_key(resolved_sql, &kind, pk_col))
            .unwrap_or_default();
        let columns = extract_columns(resolved_sql, &kind);

        let op = SyncOperation {
            op_id: uuid::Uuid::new_v4().as_u128(),
            hlc_time,
            hlc_counter,
            node_id: self.node_id,
            table: table.to_string(),
            kind: kind.clone(),
            primary_key: primary_key.clone(),
            data: Some(resolved_sql.as_bytes().to_vec()),
            columns,
        };

        let sync_tx = self.sync_tx.clone();
        let change_tx = self.change_tx.clone();
        let table_owned = table.to_string();

        tokio::spawn(async move {
            let _ = sync_tx.send(op).await;
            let _ = change_tx.send(ChangeNotification {
                table: table_owned,
                kind,
                primary_key,
            });
        });
    }
}

/// Extract the primary key value from a SQL statement.
///
/// - For INSERT: finds the PK column in the column list and extracts the corresponding value.
/// - For UPDATE/DELETE: extracts from the WHERE clause (`WHERE "pk_col" = value`).
fn extract_primary_key(sql: &str, kind: &WriteKind, pk_column: &str) -> String {
    match kind {
        WriteKind::Insert => extract_pk_from_insert(sql, pk_column),
        WriteKind::Update | WriteKind::Delete => extract_pk_from_where(sql, pk_column),
    }
}

fn extract_pk_from_insert(sql: &str, pk_column: &str) -> String {
    // Find column list between first ( and )
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

    // Find VALUES list
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
    let upper = sql.to_uppercase();
    let where_pos = match upper.find("WHERE") {
        Some(i) => i + 5,
        None => return String::new(),
    };
    let after_where = &sql[where_pos..];

    // Look for patterns like: "pk_col" = value  or  pk_col = value
    let needle_quoted = format!("\"{}\"", pk_column);
    let search = after_where.to_uppercase();

    let col_pos = search.find(&needle_quoted.to_uppercase()).or_else(|| {
        // Also try unquoted
        search.find(&pk_column.to_uppercase())
    });

    let col_pos = match col_pos {
        Some(i) => i,
        None => return String::new(),
    };

    // Find the = sign after the column name
    let after_col = &after_where[col_pos..];
    let eq_pos = match after_col.find('=') {
        Some(i) => i + 1,
        None => return String::new(),
    };

    let value_part = after_col[eq_pos..].trim_start();
    // Take until whitespace, AND, OR, or end
    let end = value_part
        .find([' ', '\n', '\r'])
        .unwrap_or(value_part.len());
    let value = &value_part[..end];
    // Check if the remaining part starts with AND/OR and trim accordingly
    value.trim().trim_matches('\'').to_string()
}

/// Split a SQL VALUES list respecting single-quoted strings.
/// e.g., `'hello, world', 42, 'foo'` -> `["'hello, world'", "42", "'foo'"]`
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
///
/// - For INSERT: parses the column list from `(col1, col2, ...)` before VALUES.
/// - For UPDATE: parses column names from `SET col1 = ..., col2 = ...`.
/// - For DELETE: returns `None`.
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
            dyn std::future::Future<Output = Result<Vec<QueryResult>, DbErr>> + Send + 'async_trait,
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
            self.db.inner.execute_unprepared(&entry.create_sql).await?;
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
        let nz =
            std::num::NonZeroU128::new(node_u128).unwrap_or(std::num::NonZeroU128::new(1).unwrap());
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

#[cfg(test)]
mod tests {
    use super::*;

    // classify_write tests

    #[test]
    fn test_classify_insert_basic() {
        let result =
            WaveSyncDb::classify_write(r#"INSERT INTO "tasks" ("id", "name") VALUES ('1', 'foo')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_insert_or_replace() {
        let result =
            WaveSyncDb::classify_write(r#"INSERT OR REPLACE INTO "tasks" ("id") VALUES ('1')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_insert_or_ignore() {
        let result =
            WaveSyncDb::classify_write(r#"INSERT OR IGNORE INTO "tasks" ("id") VALUES ('1')"#);
        assert_eq!(result, Some((WriteKind::Insert, "tasks".to_string())));
    }

    #[test]
    fn test_classify_update() {
        let result =
            WaveSyncDb::classify_write(r#"UPDATE "tasks" SET "name" = 'bar' WHERE "id" = '1'"#);
        assert_eq!(result, Some((WriteKind::Update, "tasks".to_string())));
    }

    #[test]
    fn test_classify_delete() {
        let result = WaveSyncDb::classify_write(r#"DELETE FROM "tasks" WHERE "id" = '1'"#);
        assert_eq!(result, Some((WriteKind::Delete, "tasks".to_string())));
    }

    #[test]
    fn test_classify_select_returns_none() {
        let result = WaveSyncDb::classify_write(r#"SELECT * FROM "tasks""#);
        assert_eq!(result, None);
    }

    // extract_primary_key tests

    #[test]
    fn test_extract_pk_from_insert() {
        let sql = r#"INSERT INTO "tasks" ("id", "name") VALUES ('abc-123', 'my task')"#;
        let pk = extract_primary_key(sql, &WriteKind::Insert, "id");
        assert_eq!(pk, "abc-123");
    }

    #[test]
    fn test_extract_pk_from_insert_integer() {
        let sql = r#"INSERT INTO "tasks" ("id", "name") VALUES (42, 'my task')"#;
        let pk = extract_primary_key(sql, &WriteKind::Insert, "id");
        assert_eq!(pk, "42");
    }

    #[test]
    fn test_extract_pk_from_where() {
        let sql = r#"UPDATE "tasks" SET "name" = 'bar' WHERE "id" = 'abc-123'"#;
        let pk = extract_primary_key(sql, &WriteKind::Update, "id");
        assert_eq!(pk, "abc-123");
    }

    // extract_columns tests

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

    // split_sql_values tests

    #[test]
    fn test_split_sql_values_with_quotes() {
        let values = split_sql_values("'hello, world', 42, 'foo'");
        assert_eq!(values, vec!["'hello, world'", " 42", " 'foo'"]);
    }
}
