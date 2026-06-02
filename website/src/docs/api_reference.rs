use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "API Reference" }

        p {
            "Complete reference for all public APIs across Rust, React Native, and "
            "WatermelonDB. For usage examples and guides, see the framework-specific "
            "documentation pages."
        }

        // ── WaveSyncDbBuilder ──

        H2 { id: "builder", text: "WaveSyncDbBuilder" }

        p {
            "Entry point for creating a synced database connection. All configuration "
            "is set via builder methods before calling build()."
        }

        CodeBlock { html: CODE_BUILDER }

        // ── WaveSyncDb ──

        H2 { id: "wavesyncdb", text: "WaveSyncDb" }

        p {
            "The synced database connection. Implements SeaORM's ConnectionTrait so it "
            "can be used as a drop-in replacement for DatabaseConnection. All write "
            "methods are intercepted for sync."
        }

        H3 { id: "db-core", text: "Core Methods" }

        CodeBlock { html: CODE_DB_CORE }

        H3 { id: "db-network", text: "Network Methods" }

        CodeBlock { html: CODE_DB_NETWORK }

        H3 { id: "db-push", text: "Push and Identity" }

        CodeBlock { html: CODE_DB_PUSH }

        H3 { id: "db-schema", text: "Schema Registration" }

        CodeBlock { html: CODE_DB_SCHEMA }

        // ── SchemaBuilder ──

        H2 { id: "schema-builder", text: "SchemaBuilder" }

        p {
            "Fluent API for registering SeaORM entities and starting the sync engine. "
            "Obtained via db.schema()."
        }

        CodeBlock { html: CODE_SCHEMA_BUILDER }

        // ── React Native ──

        H2 { id: "react-native", text: "React Native API" }

        H3 { id: "rn-core", text: "Core Functions" }

        CodeBlock { html: CODE_RN_CORE }

        H3 { id: "rn-network", text: "Network Functions" }

        CodeBlock { html: CODE_RN_NETWORK }

        H3 { id: "rn-push", text: "Push Functions" }

        CodeBlock { html: CODE_RN_PUSH }

        // ── WatermelonDB ──

        H2 { id: "watermelondb", text: "WatermelonDB Adapter" }

        CodeBlock { html: CODE_WATERMELON }

        // ── Types ──

        H2 { id: "types", text: "Types" }

        H3 { id: "type-node-id", text: "NodeId" }

        CodeBlock { html: CODE_NODE_ID }

        H3 { id: "type-table-meta", text: "TableMeta" }

        CodeBlock { html: CODE_TABLE_META }

        H3 { id: "type-delete-policy", text: "DeletePolicy" }

        CodeBlock { html: CODE_DELETE_POLICY }

        H3 { id: "type-change-notification", text: "ChangeNotification" }

        CodeBlock { html: CODE_CHANGE_NOTIF }

        H3 { id: "type-write-kind", text: "WriteKind" }

        CodeBlock { html: CODE_WRITE_KIND }

        H3 { id: "type-sync-changeset", text: "SyncChangeset" }

        CodeBlock { html: CODE_SYNC_CHANGESET }

        H3 { id: "type-column-change", text: "ColumnChange" }

        CodeBlock { html: CODE_COLUMN_CHANGE }

        H3 { id: "type-network", text: "Network Types" }

        p {
            "See the Network Status and Events page for full definitions of "
            "NetworkStatus, NetworkEvent, PeerInfo, PeerId, RelayStatus, and NatStatus."
        }

        // ── Background Sync ──

        H2 { id: "background-sync", text: "Background Sync" }

        CodeBlock { html: CODE_BG_SYNC }
    }
}

const CODE_BUILDER: &str = r##"<span class="kw">impl</span> <span class="fn">WaveSyncDbBuilder</span> {
    <span class="cmt">/// Create a new builder with database URL and sync topic.</span>
    <span class="kw">pub fn</span> <span class="fn">new</span>(url: &amp;str, topic: &amp;str) -&gt; Self

    <span class="cmt">/// Set a fixed node ID (default: random on first run, persisted).</span>
    <span class="kw">pub fn</span> <span class="fn">with_node_id</span>(self, id: NodeId) -&gt; Self

    <span class="cmt">/// Set the relay server multiaddr for NAT traversal.</span>
    <span class="kw">pub fn</span> <span class="fn">with_relay_server</span>(self, addr: &amp;str) -&gt; Self

    <span class="cmt">/// Set a managed relay with API key authentication.</span>
    <span class="kw">pub fn</span> <span class="fn">managed_relay</span>(self, addr: &amp;str, api_key: &amp;str) -&gt; Self

    <span class="cmt">/// Add a bootstrap peer for direct connection.</span>
    <span class="kw">pub fn</span> <span class="fn">with_bootstrap_peer</span>(self, addr: &amp;str) -&gt; Self

    <span class="cmt">/// Set the rendezvous server multiaddr for WAN peer discovery.</span>
    <span class="kw">pub fn</span> <span class="fn">with_rendezvous_server</span>(self, addr: &amp;str) -&gt; Self

    <span class="cmt">/// Set rendezvous discover interval (default: 30s).</span>
    <span class="kw">pub fn</span> <span class="fn">with_rendezvous_discover_interval</span>(self, interval: Duration) -&gt; Self

    <span class="cmt">/// Set rendezvous registration TTL in seconds.</span>
    <span class="kw">pub fn</span> <span class="fn">with_rendezvous_ttl</span>(self, ttl: u64) -&gt; Self

    <span class="cmt">/// Enable or disable IPv6 (default: true).</span>
    <span class="kw">pub fn</span> <span class="fn">with_ipv6</span>(self, enabled: bool) -&gt; Self

    <span class="cmt">/// Set the version vector sync interval (default: 30s).</span>
    <span class="kw">pub fn</span> <span class="fn">with_sync_interval</span>(self, interval: Duration) -&gt; Self

    <span class="cmt">/// Set the mDNS query interval for LAN discovery.</span>
    <span class="kw">pub fn</span> <span class="fn">with_mdns_query_interval</span>(self, interval: Duration) -&gt; Self

    <span class="cmt">/// Set the mDNS record TTL.</span>
    <span class="kw">pub fn</span> <span class="fn">with_mdns_ttl</span>(self, ttl: Duration) -&gt; Self

    <span class="cmt">/// Set a passphrase for HMAC authentication and topic derivation.</span>
    <span class="kw">pub fn</span> <span class="fn">with_passphrase</span>(self, passphrase: &amp;str) -&gt; Self

    <span class="cmt">/// Pre-register a push token (platform: "Fcm" or "Apns").</span>
    <span class="kw">pub fn</span> <span class="fn">with_push_token</span>(self, platform: &amp;str, token: &amp;str) -&gt; Self

    <span class="cmt">/// Parse google-services.json for FCM project configuration.</span>
    <span class="kw">pub fn</span> <span class="fn">with_google_services</span>(self, json: &amp;str) -&gt; Self

    <span class="cmt">/// Set FCM credentials manually (project_id, app_id, api_key).</span>
    <span class="kw">pub fn</span> <span class="fn">with_fcm</span>(self, project_id: &amp;str, app_id: &amp;str, api_key: &amp;str) -&gt; Self

    <span class="cmt">/// Set the keep-alive ping interval (default: 90s).</span>
    <span class="kw">pub fn</span> <span class="fn">with_keep_alive_interval</span>(self, interval: Duration) -&gt; Self

    <span class="cmt">/// Set maximum circuit relay duration.</span>
    <span class="kw">pub fn</span> <span class="fn">with_circuit_max_duration</span>(self, duration: Duration) -&gt; Self

    <span class="cmt">/// Build the synced database connection and start the P2P engine.</span>
    <span class="kw">pub async fn</span> <span class="fn">build</span>(self) -&gt; Result&lt;WaveSyncDb, DbErr&gt;
}"##;

const CODE_DB_CORE: &str = r##"<span class="kw">impl</span> <span class="fn">WaveSyncDb</span> {
    <span class="cmt">/// Access the underlying SeaORM DatabaseConnection.</span>
    <span class="kw">pub fn</span> <span class="fn">inner</span>(&amp;self) -&gt; &amp;DatabaseConnection

    <span class="cmt">/// This node's unique identifier.</span>
    <span class="kw">pub fn</span> <span class="fn">node_id</span>(&amp;self) -&gt; &amp;NodeId
    <span class="kw">pub fn</span> <span class="fn">site_id</span>(&amp;self) -&gt; &amp;NodeId  <span class="cmt">// alias</span>

    <span class="cmt">/// The database URL used to create this connection.</span>
    <span class="kw">pub fn</span> <span class="fn">database_url</span>(&amp;self) -&gt; &amp;str

    <span class="cmt">/// Subscribe to change notifications (local + remote writes).</span>
    <span class="kw">pub fn</span> <span class="fn">change_rx</span>(&amp;self) -&gt; broadcast::Receiver&lt;ChangeNotification&gt;

    <span class="cmt">/// Get the change notification sender (for manual notifications).</span>
    <span class="kw">pub fn</span> <span class="fn">change_tx</span>(&amp;self) -&gt; &amp;broadcast::Sender&lt;ChangeNotification&gt;

    <span class="cmt">/// Access the sync changeset sender channel.</span>
    <span class="kw">pub fn</span> <span class="fn">sync_tx</span>(&amp;self) -&gt; &amp;mpsc::Sender&lt;SyncChangeset&gt;

    <span class="cmt">/// Access the table registry.</span>
    <span class="kw">pub fn</span> <span class="fn">registry</span>(&amp;self) -&gt; &amp;Arc&lt;TableRegistry&gt;

    <span class="cmt">/// Gracefully shut down the P2P engine.</span>
    <span class="kw">pub async fn</span> <span class="fn">shutdown</span>(&amp;self)

    <span class="cmt">/// Check if the engine task is still alive.</span>
    <span class="kw">pub fn</span> <span class="fn">is_engine_alive</span>(&amp;self) -&gt; bool

    <span class="cmt">/// Get the filesystem directory containing the database.</span>
    <span class="kw">pub fn</span> <span class="fn">database_directory</span>(&amp;self) -&gt; Option&lt;PathBuf&gt;

    <span class="cmt">/// Manually emit a change notification.</span>
    <span class="kw">pub fn</span> <span class="fn">notify_change</span>(&amp;self, notification: ChangeNotification)
}"##;

const CODE_DB_NETWORK: &str = r##"<span class="kw">impl</span> <span class="fn">WaveSyncDb</span> {
    <span class="cmt">/// Get a snapshot of current network state.</span>
    <span class="kw">pub fn</span> <span class="fn">network_status</span>(&amp;self) -&gt; NetworkStatus

    <span class="cmt">/// Subscribe to network events.</span>
    <span class="kw">pub fn</span> <span class="fn">network_event_rx</span>(&amp;self) -&gt; broadcast::Receiver&lt;NetworkEvent&gt;

    <span class="cmt">/// Signal the engine to resume sync (after app foregrounding).</span>
    <span class="kw">pub fn</span> <span class="fn">resume</span>(&amp;self)

    <span class="cmt">/// Notify the engine of a network transition (WiFi/cellular switch).</span>
    <span class="kw">pub fn</span> <span class="fn">network_transition</span>(&amp;self)

    <span class="cmt">/// Request an immediate full sync with all peers.</span>
    <span class="kw">pub fn</span> <span class="fn">request_full_sync</span>(&amp;self)
}"##;

const CODE_DB_PUSH: &str = r##"<span class="kw">impl</span> <span class="fn">WaveSyncDb</span> {
    <span class="cmt">/// Register a push notification token.</span>
    <span class="cmt">/// platform: "Fcm" (Android) or "Apns" (iOS)</span>
    <span class="kw">pub fn</span> <span class="fn">register_push_token</span>(&amp;self, platform: &amp;str, token: &amp;str)

    <span class="cmt">/// Set an application-level peer identity (announced to peers).</span>
    <span class="kw">pub fn</span> <span class="fn">set_peer_identity</span>(&amp;self, app_id: &amp;str)

    <span class="cmt">/// Clear the peer identity.</span>
    <span class="kw">pub fn</span> <span class="fn">clear_peer_identity</span>(&amp;self)

    <span class="cmt">/// Get peers grouped by their announced identity.</span>
    <span class="kw">pub fn</span> <span class="fn">peers_by_identity</span>(&amp;self) -&gt; HashMap&lt;String, Vec&lt;PeerId&gt;&gt;
}"##;

const CODE_DB_SCHEMA: &str = r##"<span class="kw">impl</span> <span class="fn">WaveSyncDb</span> {
    <span class="cmt">/// Get a schema builder for registering entities.</span>
    <span class="kw">pub fn</span> <span class="fn">schema</span>(&amp;self) -&gt; SchemaBuilder

    <span class="cmt">/// Get a schema builder with a table name prefix.</span>
    <span class="kw">pub fn</span> <span class="fn">get_schema_registry</span>(&amp;self, prefix: &amp;str) -&gt; SchemaBuilder

    <span class="cmt">/// Register a table for sync manually (no ORM).</span>
    <span class="kw">pub fn</span> <span class="fn">register_table</span>(&amp;self, meta: TableMeta)

    <span class="cmt">/// Signal that all tables have been registered.</span>
    <span class="kw">pub fn</span> <span class="fn">registry_ready</span>(&amp;self)

    <span class="cmt">/// Register a SeaORM entity and create its shadow table.</span>
    <span class="kw">pub async fn</span> <span class="fn">sync_entity</span>&lt;E: EntityTrait&gt;(&amp;self) -&gt; Result&lt;(), DbErr&gt;
}"##;

const CODE_SCHEMA_BUILDER: &str = r##"<span class="kw">impl</span> <span class="fn">SchemaBuilder</span> {
    <span class="cmt">/// Register a SeaORM entity for sync. Creates the table and</span>
    <span class="cmt">/// shadow table when sync() is called.</span>
    <span class="kw">pub fn</span> <span class="fn">register</span>&lt;E: EntityTrait&gt;(self, entity: E) -&gt; Self

    <span class="cmt">/// Register a local-only entity (table created but not synced).</span>
    <span class="kw">pub fn</span> <span class="fn">register_local</span>&lt;E: EntityTrait&gt;(self, entity: E) -&gt; Self

    <span class="cmt">/// Execute registration: create tables, shadow tables, and</span>
    <span class="cmt">/// signal registry_ready.</span>
    <span class="kw">pub async fn</span> <span class="fn">sync</span>(self) -&gt; Result&lt;(), DbErr&gt;
}

<span class="cmt">// Usage:</span>
db.<span class="fn">schema</span>()
  .<span class="fn">register</span>(Task)      <span class="cmt">// synced across peers</span>
  .<span class="fn">register</span>(Expense)   <span class="cmt">// synced across peers</span>
  .<span class="fn">register_local</span>(Settings)  <span class="cmt">// local only</span>
  .<span class="fn">sync</span>().<span class="kw">await</span>?;"##;

const CODE_RN_CORE: &str = r##"<span class="cmt">// Initialize the WaveSyncDB native module</span>
<span class="kw">function</span> <span class="fn">initialize</span>(
  dbPath: string,
  topic: string,
  passphrase: string,
  options?: {
    relayAddr?: string,
    rendezvousAddr?: string,
    bootstrapPeer?: string,
  }
): Promise&lt;void&gt;

<span class="cmt">// Register a table for sync</span>
<span class="kw">function</span> <span class="fn">registerSyncedTable</span>(
  tableName: string,
  columns: string[]
): void

<span class="cmt">// Signal that all tables have been registered</span>
<span class="kw">function</span> <span class="fn">registryReady</span>(): Promise&lt;void&gt;

<span class="cmt">// Execute a write statement (synced)</span>
<span class="kw">function</span> <span class="fn">execute</span>(sql: string, params?: any[]): Promise&lt;void&gt;

<span class="cmt">// Execute a read query</span>
<span class="kw">function</span> <span class="fn">query</span>(sql: string, params?: any[]): Promise&lt;Row[]&gt;

<span class="cmt">// Shut down the sync engine</span>
<span class="kw">function</span> <span class="fn">shutdown</span>(): Promise&lt;void&gt;"##;

const CODE_RN_NETWORK: &str = r##"<span class="cmt">// Get current network status snapshot</span>
<span class="kw">function</span> <span class="fn">networkStatus</span>(): Promise&lt;NetworkStatus&gt;

<span class="cmt">// Subscribe to all network events</span>
<span class="kw">function</span> <span class="fn">subscribeNetworkEvents</span>(
  callback: (event: NetworkEvent) =&gt; void
): () =&gt; void  <span class="cmt">// returns unsubscribe</span>

<span class="cmt">// Subscribe to a specific event type</span>
<span class="kw">function</span> <span class="fn">onNetworkEvent</span>(
  eventType: string,
  callback: (data: any) =&gt; void
): () =&gt; void  <span class="cmt">// returns unsubscribe</span>"##;

const CODE_RN_PUSH: &str = r##"<span class="cmt">// Initialize FCM (Android) or APNs via Firebase (iOS)</span>
<span class="kw">function</span> <span class="fn">initWaveSyncFCM</span>(): Promise&lt;void&gt;

<span class="cmt">// Register background push handler (call in index.js)</span>
<span class="kw">function</span> <span class="fn">registerWaveSyncBackgroundHandler</span>(): void"##;

const CODE_WATERMELON: &str = r##"<span class="kw">class</span> <span class="fn">WaveSyncAdapter</span> {
  <span class="cmt">/// Create a new WaveSyncDB-backed WatermelonDB adapter.</span>
  <span class="kw">constructor</span>(options: {
    schema: AppSchema,
    topic: string,
    passphrase: string,
    relayAddr?: string,
    rendezvousAddr?: string,
  })

  <span class="cmt">/// CRITICAL: Call after new Database({ adapter }).</span>
  <span class="cmt">/// Connects the adapter to WatermelonDB's cache for</span>
  <span class="cmt">/// remote change propagation.</span>
  <span class="fn">setDatabase</span>(database: Database): void

  <span class="cmt">/// Shut down the sync engine and close the database.</span>
  <span class="fn">tearDown</span>(): Promise&lt;void&gt;
}

<span class="cmt">/// Helper to create column definitions from [name, type] pairs</span>
<span class="kw">function</span> <span class="fn">columnArray</span>(
  columns: Array&lt;[string, <span class="str">'string'</span> | <span class="str">'number'</span> | <span class="str">'boolean'</span>]&gt;
): ColumnSchema[]"##;

const CODE_NODE_ID: &str = r##"<span class="cmt">/// 16-byte unique node identifier.</span>
<span class="cmt">/// Persisted in _wavesync_meta; stable across restarts.</span>
<span class="cmt">/// Used as the final tiebreaker in conflict resolution.</span>
<span class="kw">pub struct</span> <span class="fn">NodeId</span>(<span class="kw">pub</span> [u8; <span class="num">16</span>]);"##;

const CODE_TABLE_META: &str = r##"<span class="cmt">/// Metadata for a synced table.</span>
<span class="kw">pub struct</span> <span class="fn">TableMeta</span> {
    <span class="cmt">/// Table name in the database</span>
    <span class="kw">pub</span> table_name: TableName,
    <span class="cmt">/// Name of the primary key column</span>
    <span class="kw">pub</span> pk_column: String,
    <span class="cmt">/// All column names (including pk)</span>
    <span class="kw">pub</span> columns: Vec&lt;ColumnName&gt;,
    <span class="cmt">/// How to resolve delete vs non-delete conflicts</span>
    <span class="kw">pub</span> delete_policy: DeletePolicy,
}"##;

const CODE_DELETE_POLICY: &str = r##"<span class="kw">pub enum</span> <span class="fn">DeletePolicy</span> {
    <span class="cmt">/// Delete wins over concurrent non-delete (default)</span>
    DeleteWins,
    <span class="cmt">/// Non-delete wins over concurrent delete</span>
    AddWins,
}"##;

const CODE_CHANGE_NOTIF: &str = r##"<span class="cmt">/// Emitted after every local or remote write.</span>
<span class="kw">pub struct</span> <span class="fn">ChangeNotification</span> {
    <span class="cmt">/// Name of the modified table</span>
    <span class="kw">pub</span> table: TableName,
    <span class="cmt">/// Type of write operation</span>
    <span class="kw">pub</span> kind: WriteKind,
    <span class="cmt">/// Primary key of the affected row</span>
    <span class="kw">pub</span> primary_key: PrimaryKey,
    <span class="cmt">/// Columns that were changed (if known)</span>
    <span class="kw">pub</span> changed_columns: Option&lt;Vec&lt;String&gt;&gt;,
}"##;

const CODE_WRITE_KIND: &str = r##"<span class="kw">pub enum</span> <span class="fn">WriteKind</span> {
    Insert,
    Update,
    Delete,
}"##;

const CODE_SYNC_CHANGESET: &str = r##"<span class="cmt">/// A batch of column changes from a single write.</span>
<span class="kw">pub struct</span> <span class="fn">SyncChangeset</span> {
    <span class="cmt">/// Originating node's ID</span>
    <span class="kw">pub</span> site_id: NodeId,
    <span class="cmt">/// db_version at which this was created</span>
    <span class="kw">pub</span> db_version: u64,
    <span class="cmt">/// Individual column changes</span>
    <span class="kw">pub</span> changes: Vec&lt;ColumnChange&gt;,
}"##;

const CODE_COLUMN_CHANGE: &str = r##"<span class="cmt">/// A single column-level CRDT change.</span>
<span class="kw">pub struct</span> <span class="fn">ColumnChange</span> {
    <span class="kw">pub</span> table: TableName,        <span class="cmt">// Table name</span>
    <span class="kw">pub</span> pk: PrimaryKey,          <span class="cmt">// Row primary key</span>
    <span class="kw">pub</span> cid: ColumnName,         <span class="cmt">// Column name (or "__deleted")</span>
    <span class="kw">pub</span> val: Option&lt;serde_json::Value&gt;, <span class="cmt">// New value (None for deletes)</span>
    <span class="kw">pub</span> site_id: NodeId,         <span class="cmt">// Originating node</span>
    <span class="kw">pub</span> col_version: u64,        <span class="cmt">// Per-column Lamport clock</span>
    <span class="kw">pub</span> cl: u64,                 <span class="cmt">// Causal length (for deletes)</span>
    <span class="kw">pub</span> seq: u32,                <span class="cmt">// Order within db_version batch</span>
    <span class="kw">pub</span> db_version: u64,         <span class="cmt">// db_version when created</span>
}"##;

const CODE_BG_SYNC: &str = r##"<span class="cmt">/// One-shot sync for push notification wake-up.</span>
<span class="kw">pub async fn</span> <span class="fn">background_sync</span>(
    database_url: &amp;str,
    timeout: Duration,
) -&gt; Result&lt;BackgroundSyncResult, BackgroundSyncError&gt;

<span class="kw">pub enum</span> <span class="fn">BackgroundSyncResult</span> {
    <span class="cmt">/// Synced with at least one peer</span>
    Synced { peers_synced: usize },
    <span class="cmt">/// No peers found within timeout</span>
    NoPeers,
    <span class="cmt">/// Timed out before all peers finished</span>
    TimedOut { peers_synced: usize },
}

<span class="kw">pub enum</span> <span class="fn">BackgroundSyncError</span> {
    <span class="cmt">/// No .wavesync_config.json found</span>
    ConfigNotFound(String),
    <span class="cmt">/// Config file invalid or corrupted</span>
    ConfigInvalid(String),
    <span class="cmt">/// Database connection error</span>
    DatabaseError(String),
    <span class="cmt">/// Schema registry initialization failed</span>
    RegistryError(String),
}"##;
