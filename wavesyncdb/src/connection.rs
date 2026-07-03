use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseBackend, DatabaseConnection, DbErr,
    EntityTrait, ExecResult, Iterable, PrimaryKeyToColumn, QueryResult, Schema, Statement,
    TransactionTrait, sea_query::SqliteQueryBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify, broadcast, mpsc};

use crate::auth::GroupKey;
use crate::engine::{EngineCommand, GroupInit, TaggedChangeset};
use crate::messages::{
    ChangeNotification, ColumnChange, DeletePolicy, NodeId, SyncChangeset, WriteKind,
};
use crate::registry::{SyncEntityInfo, TableMeta, TableRegistry};

/// Cheap post-execution filter: could this SQL have fired a capture trigger?
///
/// Case-insensitive substring scan. False positives are fine — the drain
/// just finds an empty capture table. False negatives are NOT: this must
/// stay a strict superset of every statement shape that can write a
/// registered table, which is why it is a substring scan (CTEs like
/// `WITH … INSERT`, `REPLACE INTO`, and multi-statement scripts all match)
/// rather than a first-token classifier. Row data itself comes from the
/// capture triggers, never from parsing this SQL text.
fn may_write(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    ["INSERT", "UPDATE", "DELETE", "REPLACE"]
        .iter()
        .any(|kw| upper.contains(kw))
}

/// Node-level shared state: the single libp2p engine and everything that is
/// shared across all of its sync groups.
///
/// One [`WaveSyncNode`] owns one engine task serving N groups, each backed by
/// its own SQLite DB and surfaced as a [`WaveSyncDb`] handle. The engine task
/// is aborted exactly once, when the last reference to this inner struct drops
/// (see the `Drop` impl). Every [`WaveSyncDbInner`] holds an
/// `Arc<WaveSyncNodeInner>`, so the engine outlives any individual group
/// handle: a single-group app that only keeps the returned `WaveSyncDb` keeps
/// the engine alive transitively, and dropping that last handle tears the
/// engine down — preserving the original single-group lifecycle.
pub(crate) struct WaveSyncNodeInner {
    cmd_tx: mpsc::Sender<EngineCommand>,
    /// Local-write channel into the engine. Each group's handle clones this and
    /// stamps its own effective topic onto every changeset (see
    /// [`TaggedChangeset`]).
    tagged_sync_tx: mpsc::Sender<TaggedChangeset>,
    engine_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    network_status: Arc<std::sync::RwLock<crate::network_status::NetworkStatus>>,
    network_event_tx: broadcast::Sender<crate::network_status::NetworkEvent>,
    diagnostics: Arc<crate::diagnostics::Counters>,
    /// Engine-shared per-peer health store (byte counters, last-synced /
    /// last-converged timestamps, catch-up RTT). This node holds one clone of
    /// the `Arc` and the engine holds another; the engine's clone is what
    /// actually feeds `PeerInfo`'s health fields today, via
    /// `update_network_status` reading `PeerHealthStore::snapshot_for`. This
    /// field's own clone isn't read anywhere yet — it exists so a future
    /// `WaveSyncDb`-level accessor can reach the store directly without going
    /// through `NetworkStatus`.
    #[allow(dead_code)]
    peer_health: Arc<crate::diagnostics::PeerHealthStore>,
    /// Node-level user-notification channel, shared by **every** group on this
    /// node. Each group's engine-side `GroupState.notification_tx` is a clone of
    /// this one sender, so `WaveSyncDb::notification_rx()` on any handle yields
    /// notifications for *all* groups — a single `use_sync_notifications` covers
    /// the whole node (default + every joined group), instead of silently
    /// missing groups whose per-group channel had no subscriber.
    notification_tx: broadcast::Sender<crate::notify::Notification>,
    /// Fires on [`WaveSyncDb::resume`] to tell foreground table/row hooks to
    /// re-query the DB from scratch. A push-triggered background sync runs in a
    /// **separate process** (the FCM service) and writes the shared SQLite file
    /// directly, so it emits no in-process `ChangeNotification` — the foreground
    /// hooks' in-memory signals would otherwise stay stale until the next write
    /// they happen to observe. Carries no payload: it just means "the DB may
    /// have changed underneath you; reload."
    refresh_tx: broadcast::Sender<()>,
    /// Active group handles keyed by *user* topic (pre-derivation). Lets
    /// `join_group` be idempotent and `leave_group` resolve handles.
    ///
    /// Stored as `Weak` to avoid a reference cycle: every `WaveSyncDbInner`
    /// holds `Arc<WaveSyncNodeInner>` (this struct), so a *strong* handle here
    /// would keep the node — and thus the engine task and its DB connection
    /// pool — alive forever. That zombie engine holds the group's SQLite file
    /// open across a handle drop / reopen (rolling restart, partition → merge),
    /// producing "database is locked" and cross-talking zombie swarms. With
    /// `Weak`, dropping the last external `WaveSyncDb` lets `WaveSyncDbInner`'s
    /// `Drop` run and tear the engine down, exactly as the single-group `Drop`
    /// did before the node/engine split.
    groups: std::sync::Mutex<HashMap<String, std::sync::Weak<WaveSyncDbInner>>>,
    /// Base for deriving per-group SQLite URLs. The default group keeps the
    /// original URL; joined groups derive a sibling file. See `join_group`.
    base_database_url: String,
    /// Capacity for each group's ChangeNotification broadcast channel,
    /// carried from the builder so runtime-joined groups match the
    /// default group's setting.
    change_channel_capacity: usize,
    /// Tombstone retention override from the builder, applied to every
    /// group DB this node opens (None = builder untouched; the DB's
    /// persisted value or the 7-day default governs).
    tombstone_retention: Option<Option<std::time::Duration>>,
}

impl Drop for WaveSyncNodeInner {
    fn drop(&mut self) {
        // Abort the engine task to prevent zombie swarms (e.g. mDNS cross-talk
        // between tests). Use get_mut() instead of lock() — since we have
        // &mut self, no other thread can hold a reference, so we can access the
        // mutex data without locking. This avoids the
        // "pthread_mutex_lock called on a destroyed mutex" crash on Android
        // when the app process is killed.
        if let Some(handle) = self.engine_handle.get_mut().ok().and_then(|h| h.take()) {
            handle.abort();
        }
    }
}

/// A handle to the libp2p engine that serves one or more sync groups.
///
/// Cheap to clone (internally `Arc`-based). Holding a `WaveSyncNode` keeps the
/// engine alive even if every per-group [`WaveSyncDb`] is dropped, and lets an
/// app `join_group` / `leave_group` additional groups at runtime.
#[derive(Clone)]
pub struct WaveSyncNode {
    inner: Arc<WaveSyncNodeInner>,
}

/// Internal shared state for [`WaveSyncDb`] — a single sync group's handle.
struct WaveSyncDbInner {
    inner: DatabaseConnection,
    #[allow(dead_code)]
    database_url: String,
    /// Clone of the node's local-write channel. Every local write funnels
    /// through `dispatch_sync`'s single `send` site, which stamps
    /// [`effective_topic`](Self::effective_topic) onto the changeset.
    sync_tx: mpsc::Sender<TaggedChangeset>,
    /// Effective (PSK-derived) topic of this group — the routing tag the engine
    /// uses to find this group's [`GroupState`].
    effective_topic: String,
    /// Whether this is the node's default group (created by `build()`). Drives
    /// `EntityScope::Private` registration — private entities live here only.
    is_default_group: bool,
    /// Stable kind label for this group (from `join_group(.., kind)`); `None`
    /// for the default group. Matched against `EntityScope::Groups`.
    group_kind: Option<String>,
    change_tx: broadcast::Sender<ChangeNotification>,
    /// User-facing notifications produced by `#[derive(SyncNotify)]` policies on
    /// incoming remote changes. Drained by `use_sync_notifications`. The
    /// matching registry of policies lives in the engine task.
    notification_tx: broadcast::Sender<crate::notify::Notification>,
    site_id: NodeId,
    db_version: Mutex<u64>,
    db_version_cache: Arc<AtomicU64>,
    node_id: NodeId,
    registry: Arc<TableRegistry>,
    registry_ready: Arc<Notify>,
    /// The node that owns the engine. Keeping this `Arc` here makes the engine
    /// outlive any *single* `WaveSyncDb` clone (so multi-group handles share one
    /// engine), while `WaveSyncDbInner`'s `Drop` still aborts the engine
    /// eagerly when this is the last group handle — preserving the back-compat
    /// single-group teardown (drop the handle → engine stops, DB file released).
    node: Arc<WaveSyncNodeInner>,
    table_cache: std::sync::RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
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
        // When this is the last live group handle for the node and no
        // `WaveSyncNode` is being held, abort the engine task *eagerly* here
        // rather than waiting for `WaveSyncNodeInner`'s own `Drop`. A running
        // engine keeps this group's SQLite connection pool open and keeps mDNS
        // announcing the topic; tests (and apps) that drop a peer and then
        // immediately reopen the same DB file — rolling restart, partition →
        // merge — depend on the file being released synchronously on drop, the
        // way the pre-split single-group `WaveSyncDbInner::drop` did.
        //
        // `strong_count == 1` means the only remaining strong reference to the
        // node is the one held by this `WaveSyncDbInner` (about to drop), i.e.
        // no other group handle and no held `WaveSyncNode` exist. A genuine
        // multi-group node with other live handles has count > 1, so the engine
        // stays up and `WaveSyncNodeInner::drop` tears it down once the last
        // reference goes away.
        if Arc::strong_count(&self.node) == 1
            && let Ok(mut guard) = self.node.engine_handle.lock()
            && let Some(handle) = guard.take()
        {
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
    /// The effective (PSK-derived) topic of this group handle. Uniquely
    /// identifies the group on the wire; distinct passphrases/topics yield
    /// distinct effective topics.
    pub fn effective_topic(&self) -> &str {
        &self.inner.effective_topic
    }

    /// The [`WaveSyncNode`] that owns this group's engine. Use it to
    /// `join_group` / `leave_group` additional groups at runtime.
    pub fn node(&self) -> WaveSyncNode {
        WaveSyncNode {
            inner: self.inner.node.clone(),
        }
    }
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

    /// Subscribe to user-facing sync notifications.
    ///
    /// Emits a [`Notification`](crate::Notification) for each *incoming remote*
    /// change whose entity declares a policy via `#[derive(SyncNotify)]` and
    /// whose `on_sync` returns `Some`, after de-duplication/coalescing. Never
    /// fires for the local user's own writes. Drain this with the
    /// `use_sync_notifications` Dioxus hook, or directly to render OS toasts.
    pub fn notification_rx(&self) -> broadcast::Receiver<crate::notify::Notification> {
        self.inner.notification_tx.subscribe()
    }

    /// Get a snapshot of the current network status.
    ///
    /// This is a cheap read from shared memory — no network round-trip.
    pub fn network_status(&self) -> crate::network_status::NetworkStatus {
        // Recover from a poisoned lock rather than cascading the panic: the
        // status is plain readable data, and one writer panicking must not make
        // every later status read panic too.
        self.inner
            .node
            .network_status
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Subscribe to network events (peer connect/disconnect, relay changes, etc.).
    pub fn network_event_rx(&self) -> broadcast::Receiver<crate::network_status::NetworkEvent> {
        self.inner.node.network_event_tx.subscribe()
    }

    /// Snapshot the engine's diagnostics counters.
    ///
    /// Cheap (a handful of `Relaxed` atomic loads). Counters reset to
    /// zero on engine restart — see [`crate::diagnostics`] for what each
    /// counter measures and why this exists.
    pub fn diagnostics(&self) -> crate::diagnostics::Snapshot {
        self.inner.node.diagnostics.snapshot()
    }

    /// Get a reference to the table registry.
    pub fn registry(&self) -> &Arc<TableRegistry> {
        &self.inner.registry
    }

    /// Current `db_version` from the in-memory cache.
    ///
    /// Lock-free: reads an `AtomicU64` that is kept in sync with the
    /// Mutex-guarded counter by `dispatch_sync` and
    /// `apply_remote_changeset`. No database query is performed.
    pub fn db_version(&self) -> u64 {
        self.inner.db_version_cache.load(Ordering::Acquire)
    }

    /// Return a cached copy of a table's rows if one exists.
    ///
    /// Keyed by `TypeId` so different entity types have independent caches.
    /// Returns `None` on first load (cache miss). The Dioxus hooks call
    /// this before hitting the database — a cache hit means instant data
    /// on page navigation.
    pub fn get_table_cache<T: Clone + Send + Sync + 'static>(&self) -> Option<T> {
        self.inner
            .table_cache
            .read()
            .unwrap()
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref::<T>())
            .cloned()
    }

    /// Store a snapshot of a table's rows in the in-memory cache.
    pub fn set_table_cache<T: Send + Sync + 'static>(&self, data: T) {
        self.inner
            .table_cache
            .write()
            .unwrap()
            .insert(TypeId::of::<T>(), Box::new(data));
    }

    /// Gracefully shut down the engine and close the database connection.
    pub async fn shutdown(&self) {
        let _ = self
            .inner
            .node
            .cmd_tx
            .send(crate::engine::EngineCommand::Shutdown)
            .await;

        let handle = {
            self.inner
                .node
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
            .node
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
            .node
            .cmd_tx
            .try_send(crate::engine::EngineCommand::Resume);
        // Tell foreground table/row hooks to re-query: a background-sync wake
        // (a separate process) may have written the shared DB while we were
        // backgrounded, with no in-process ChangeNotification to observe.
        let _ = self.inner.node.refresh_tx.send(());
    }

    /// Subscribe to resume/refresh ticks (fired by [`resume`](Self::resume)).
    /// Foreground hooks reload the DB on each tick to pick up writes made by a
    /// background-sync process. Node-level: shared across every group handle.
    pub fn refresh_rx(&self) -> broadcast::Receiver<()> {
        self.inner.node.refresh_tx.subscribe()
    }

    /// A cheap, `Send + Clone` callable that performs [`resume`](Self::resume)
    /// when invoked. Captures only the engine command + refresh channel senders
    /// — **not** the node `Arc` — so a long-lived lifecycle listener holding it
    /// does not keep the engine alive past the last `WaveSyncDb` drop (which
    /// would resurrect the zombie-swarm / "database is locked" problems the
    /// `Weak` group map avoids). Used to auto-drive resume on app foreground.
    #[cfg(feature = "dioxus")]
    pub(crate) fn resume_trigger(&self) -> impl Fn() + Clone + Send + 'static {
        let cmd_tx = self.inner.node.cmd_tx.clone();
        let refresh_tx = self.inner.node.refresh_tx.clone();
        move || {
            let _ = cmd_tx.try_send(crate::engine::EngineCommand::Resume);
            let _ = refresh_tx.send(());
        }
    }

    /// Notify the engine that the network interface changed (e.g., WiFi to cellular).
    ///
    /// This force-disconnects all connections (including the relay) and
    /// re-establishes them on the new network interface. More aggressive than
    /// `resume()` — use when you know the network path has changed.
    pub fn network_transition(&self) {
        let _ = self
            .inner
            .node
            .cmd_tx
            .try_send(crate::engine::EngineCommand::NetworkTransition);
    }

    /// Request a full sync from connected peers.
    pub fn request_full_sync(&self) {
        let _ = self
            .inner
            .node
            .cmd_tx
            .try_send(crate::engine::EngineCommand::RequestFullSync);
    }

    /// Enable or disable mDNS LAN discovery at runtime.
    ///
    /// Idempotent. When disabling, existing mDNS-discovered peer connections
    /// are kept — only future announcements and queries are silenced. When
    /// re-enabling, a fresh mDNS behaviour is built and queries resume on
    /// the next mDNS tick.
    ///
    /// Useful for apps that want to opt into LAN announcements only when
    /// the user is actively pairing or syncing, instead of broadcasting
    /// continuously on every network the device joins.
    pub fn set_mdns_enabled(&self, enabled: bool) {
        let _ = self
            .inner
            .node
            .cmd_tx
            .try_send(crate::engine::EngineCommand::SetMdnsEnabled(enabled));
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
            .node
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
            .node
            .cmd_tx
            .try_send(crate::engine::EngineCommand::SetPeerIdentity(Some(
                app_id.to_string(),
            )));
    }

    /// Clear the application-level identity for this peer.
    pub fn clear_peer_identity(&self) {
        let _ = self
            .inner
            .node
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
        let status = self
            .inner
            .node
            .network_status
            .read()
            .unwrap_or_else(|e| e.into_inner());
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
    ///
    /// Installs the change-capture triggers for the table before adding it to
    /// the registry — a registered table must never have a capture gap, or
    /// its writes are silently never synced. The table (and its shadow table)
    /// must already exist; use [`SchemaBuilder`](Self::schema) or
    /// [`sync_entity`](Self::sync_entity) for the full create-and-register
    /// flow.
    pub async fn register_table(&self, meta: TableMeta) -> Result<(), DbErr> {
        // Ensure the shadow schema exists AND is current (idempotent create
        // + in-place migration) — a manually created shadow table from an
        // older layout would otherwise break the retention predicates on
        // first use.
        crate::shadow::create_shadow_table(&self.inner.inner, &meta.table_name).await?;
        crate::capture::ensure_triggers(&self.inner.inner, &meta).await?;
        self.inner.registry.register(meta);
        Ok(())
    }

    /// Signal the engine that all tables have been registered and sync can begin.
    ///
    /// This is called automatically by [`SchemaBuilder::sync()`]. You only need
    /// to call it manually when registering tables via [`register_table()`](Self::register_table)
    /// without using the schema builder.
    pub fn registry_ready(&self) {
        self.inner.registry_ready.notify_one();
        // The engine awaits only the *default* group's `registry_ready` Notify.
        // A runtime-joined group has no such await, so signal it explicitly —
        // otherwise it's excluded from connect/discovery-time sync initiation
        // (which only fires for `registry_is_ready` groups) and syncs only via
        // the slow periodic tick, leaving it one-directional/asymmetric.
        if !self.inner.is_default_group {
            let _ =
                self.inner
                    .node
                    .cmd_tx
                    .try_send(crate::engine::EngineCommand::GroupRegistryReady {
                        effective_topic: self.inner.effective_topic.clone(),
                    });
        }
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

        // Only auto-register entities whose declared scope includes THIS group.
        // Private → default group only; All → every group; Groups("kind") → a
        // group joined with a matching kind label. Explicit `.register(E)` is an
        // unconditional override and bypasses this gate.
        let is_default = self.inner.is_default_group;
        let group_kind = self.inner.group_kind.as_deref();
        for info in inventory::iter::<SyncEntityInfo> {
            if info.module_path.starts_with(prefix) && info.scope.matches(is_default, group_kind) {
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

        // register_table installs (or refreshes after schema change) the
        // capture triggers — from here on every write to this table is
        // recorded for the drain.
        self.register_table(TableMeta {
            table_name,
            primary_key_column,
            columns,
            delete_policy: DeletePolicy::default(),
        })
        .await?;

        // Drain anything already captured: leftovers from a crash between a
        // user write and its bookkeeping, or writes made by a separate
        // process sharing this DB file (iOS background sync). Best-effort —
        // rows persist for the next drain if this one fails.
        if let Err(e) = self.drain_and_dispatch().await {
            log::warn!("Startup capture drain failed (will retry on next write): {e}");
        }

        Ok(())
    }

    /// Broadcast a change notification (used by the engine for remote changes).
    pub fn notify_change(&self, notification: ChangeNotification) {
        let _ = self.inner.change_tx.send(notification);
    }

    /// Drain the trigger-capture table and publish the pending changes.
    ///
    /// Runs after every intercepted write statement (and once at startup):
    /// reads `_wavesync_changes` in write order, plans logical row ops,
    /// performs the CRDT bookkeeping for all of them in ONE transaction
    /// (single fsync) serialized by the db_version mutex, purges the drained
    /// rows in that same transaction, and only then notifies subscribers and
    /// hands the changeset to the engine.
    ///
    /// Returns `Err` when the bookkeeping cannot be committed. The capture
    /// rows stay in place on failure — the change is retried on the next
    /// drain (or the startup drain) instead of being lost. The in-memory
    /// db_version rolls back on every failure path so it stays in sync with
    /// persisted state.
    async fn drain_and_dispatch(&self) -> Result<(), DbErr> {
        // The mutex serializes concurrent drains for this handle — two rapid
        // writes would otherwise race on the shadow read steps and produce
        // incorrect col_versions.
        let mut ver = self.inner.db_version.lock().await;

        let rows = crate::capture::fetch_capture_rows(&self.inner.inner).await?;
        let Some(max_seq) = rows.last().map(|r| r.seq) else {
            return Ok(());
        };

        let ops = crate::capture::plan_logical_ops(&rows, &self.inner.registry);
        if ops.is_empty() {
            // Only unregistered/no-op rows: discard without spending a
            // db_version. (Rows past max_seq, added meanwhile by another
            // process, are untouched.)
            crate::capture::purge_capture_rows(&self.inner.inner, max_seq).await?;
            return Ok(());
        }

        let site_id = self.inner.site_id;
        let inner = &self.inner.inner;

        // One drain = one db_version increment shared by every logical op in
        // it — normally one statement's rows, occasionally a backlog (startup
        // recovery, writes from a separate process sharing the DB file).
        // Receivers apply the changeset as a single batch either way.
        *ver += 1;
        let new_db_version = *ver;
        self.inner
            .db_version_cache
            .store(new_db_version, Ordering::Release);

        // Open the bookkeeping transaction. Roll back the in-memory counter
        // if we can't even start a tx — keeps it in sync with the persisted
        // state.
        let txn = match inner.begin().await {
            Ok(t) => t,
            Err(e) => {
                *ver -= 1;
                self.inner.db_version_cache.store(*ver, Ordering::Release);
                return Err(e);
            }
        };

        // No `_wavesync_meta.db_version` write here — the shadow upsert(s)
        // below land with the new db_version in the same tx, and
        // `shadow::get_db_version` recovers via `MAX(meta, MAX_shadow)` on
        // engine startup.

        let mut changes = Vec::new();

        for op in &ops {
            match op {
                crate::capture::LogicalOp::Delete { table, pk } => {
                    // Find max col_version for this row and create tombstone.
                    let entries =
                        match crate::shadow::get_clock_entries_for_row(&txn, table, pk).await {
                            Ok(entries) => entries,
                            Err(e) => {
                                // Fail-closed: a read failure here would yield a
                                // wrong tombstone col_version. Roll back rather
                                // than publish a delete the local shadow can't
                                // back.
                                log::error!("Failed to read clock entries for delete: {e}");
                                *ver -= 1;
                                self.inner.db_version_cache.store(*ver, Ordering::Release);
                                let _ = txn.rollback().await;
                                return Err(e);
                            }
                        };

                    let max_cv = entries.iter().map(|e| e.col_version).max().unwrap_or(0);
                    let tombstone_cv = max_cv + 1;
                    // The deleter's clock, stamped once and carried on the
                    // wire so every replica ages this tombstone identically.
                    let deleted_ts = crate::shadow::unix_now_secs();

                    if let Err(e) = crate::shadow::insert_tombstone(
                        &txn,
                        table,
                        pk,
                        tombstone_cv,
                        new_db_version,
                        &site_id,
                        deleted_ts,
                    )
                    .await
                    {
                        log::error!("Failed to insert tombstone: {e}");
                        *ver -= 1;
                        self.inner.db_version_cache.store(*ver, Ordering::Release);
                        let _ = txn.rollback().await;
                        return Err(e);
                    }

                    changes.push(ColumnChange {
                        table: table.clone().into(),
                        pk: pk.clone().into(),
                        cid: "__deleted".to_string().into(),
                        val: None,
                        site_id,
                        col_version: tombstone_cv,
                        cl: tombstone_cv,
                        seq: 0,
                        db_version: new_db_version,
                        deleted_ts: Some(deleted_ts),
                    });
                }
                crate::capture::LogicalOp::Insert { table, pk, cols }
                | crate::capture::LogicalOp::Update { table, pk, cols } => {
                    // If this row is resurrecting after a local delete, its cells
                    // must outrank the tombstone: read the tombstone's causal
                    // length first and floor the revived col_versions at cl+1, so
                    // a DeleteWins peer still holding that tombstone applies the
                    // re-insert instead of letting the (equal-cl) delete win. 0 =
                    // no tombstone = normal write.
                    let floor = match crate::shadow::get_tombstone_cl(&txn, table, pk).await {
                        Ok(Some(cl)) => cl + 1,
                        Ok(None) => 0,
                        Err(e) => {
                            log::error!("Failed to read tombstone for resurrection floor: {e}");
                            *ver -= 1;
                            self.inner.db_version_cache.store(*ver, Ordering::Release);
                            let _ = txn.rollback().await;
                            return Err(e);
                        }
                    };

                    // Clear any tombstone for this row (it's alive again),
                    // but preserve per-column clock entries so col_versions
                    // continue from their previous values.
                    if let Err(e) = crate::shadow::clear_tombstone(&txn, table, pk).await {
                        // Fail-closed: a stale tombstone left behind could let a
                        // delete win over this resurrection on a peer. Roll back
                        // instead of committing inconsistent shadow state.
                        log::error!("Failed to clear tombstone: {e}");
                        *ver -= 1;
                        self.inner.db_version_cache.store(*ver, Ordering::Release);
                        let _ = txn.rollback().await;
                        return Err(e);
                    }

                    // Single batched upsert across every changed column.
                    // SQLite's ON CONFLICT DO UPDATE … RETURNING gives us
                    // the resolved col_version per cid in one round trip.
                    let batch_input: Vec<(String, u32)> = cols
                        .iter()
                        .enumerate()
                        .map(|(seq, (col, _))| (col.clone(), seq as u32))
                        .collect();
                    let resolved = match crate::shadow::upsert_clock_entries_batch(
                        &txn,
                        table,
                        pk,
                        &batch_input,
                        new_db_version,
                        &site_id,
                        floor,
                    )
                    .await
                    {
                        Ok(map) => map,
                        Err(e) => {
                            log::error!("Failed to batch-upsert clock entries: {e}");
                            *ver -= 1;
                            self.inner.db_version_cache.store(*ver, Ordering::Release);
                            let _ = txn.rollback().await;
                            return Err(e);
                        }
                    };

                    for (seq, (col, val)) in cols.iter().enumerate() {
                        let new_cv = resolved.get(col).copied().unwrap_or(1);

                        changes.push(ColumnChange {
                            table: table.clone().into(),
                            pk: pk.clone().into(),
                            cid: col.clone().into(),
                            val: Some(val.clone()),
                            site_id,
                            col_version: new_cv,
                            cl: new_cv,
                            seq: seq as u32,
                            db_version: new_db_version,
                            deleted_ts: None,
                        });
                    }
                }
            }
        }

        // Purge the drained capture rows INSIDE the bookkeeping transaction:
        // either the bookkeeping and the purge commit together, or neither
        // does — an undrained capture row can never be lost, and a bookkept
        // row can never be drained twice.
        if let Err(e) = crate::capture::purge_capture_rows(&txn, max_seq).await {
            *ver -= 1;
            self.inner.db_version_cache.store(*ver, Ordering::Release);
            let _ = txn.rollback().await;
            return Err(e);
        }

        // Commit the whole bookkeeping batch with a single fsync.
        if let Err(e) = txn.commit().await {
            *ver -= 1;
            self.inner.db_version_cache.store(*ver, Ordering::Release);
            return Err(e);
        }

        // Release the lock before sending on sync_tx — we no longer touch
        // shadow tables, so further writes can proceed concurrently with
        // the engine consuming this changeset.
        drop(ver);

        // Emit change notifications only now that the shadow-table transaction
        // has committed. The user-table data was already committed before the
        // drain ran, but a subscriber that re-reads version/shadow state on
        // notification must not observe a pre-commit — or since-rolled-back —
        // shadow snapshot. One notification per logical op so reactive hooks
        // (`use_synced_table`) wake exactly once per affected primary key; a
        // pk-changing UPDATE yields two (delete + insert), which is what the
        // hooks need to move the row in place.
        for op in &ops {
            let (kind, pk, column_values) = match op {
                crate::capture::LogicalOp::Insert { pk, cols, .. } => {
                    (WriteKind::Insert, pk, Some(cols))
                }
                crate::capture::LogicalOp::Update { pk, cols, .. } => {
                    (WriteKind::Update, pk, Some(cols))
                }
                crate::capture::LogicalOp::Delete { pk, .. } => (WriteKind::Delete, pk, None),
            };
            let table = match op {
                crate::capture::LogicalOp::Insert { table, .. }
                | crate::capture::LogicalOp::Update { table, .. }
                | crate::capture::LogicalOp::Delete { table, .. } => table,
            };
            let column_values: Option<Vec<(crate::ColumnName, serde_json::Value)>> = column_values
                .map(|cols| {
                    cols.iter()
                        .map(|(col, val)| (crate::ColumnName(col.clone()), val.clone()))
                        .collect()
                });
            let changed_columns = column_values
                .as_ref()
                .map(|cv| cv.iter().map(|(c, _)| c.0.clone()).collect());
            let _ = self.inner.change_tx.send(ChangeNotification {
                table: table.clone().into(),
                kind,
                source: crate::messages::ChangeSource::Local,
                primary_key: pk.clone().into(),
                changed_columns,
                column_values,
            });
        }

        let changeset = SyncChangeset {
            site_id,
            db_version: new_db_version,
            changes,
        };

        // sync_tx is a bounded mpsc; if the engine is slow we wait. That's
        // intentional — backpressure into the user write loop is preferable
        // to dropping changesets. The changeset is tagged with this handle's
        // effective topic so the engine routes it to the correct group — the
        // single point where a local write enters the engine.
        let _ = self
            .inner
            .sync_tx
            .send(TaggedChangeset {
                effective_topic: self.inner.effective_topic.clone(),
                changeset,
            })
            .await;

        Ok(())
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
        // Raw statement text only — values are never inlined or parsed; the
        // capture triggers record the actual row data.
        let might_write = may_write(&stmt.sql);
        Box::pin(async move {
            let result = self.inner.inner.execute_raw(stmt).await?;
            if might_write {
                self.drain_and_dispatch().await?;
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
        let might_write = may_write(sql);
        Box::pin(async move {
            let result = self.inner.inner.execute_unprepared(sql).await?;
            if might_write {
                self.drain_and_dispatch().await?;
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
        // SeaORM routes `INSERT … RETURNING` through this method — the drain
        // must run here exactly as on the execute paths.
        let might_write = may_write(&stmt.sql);
        Box::pin(async move {
            let result = self.inner.inner.query_one_raw(stmt).await?;
            if might_write {
                self.drain_and_dispatch().await?;
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
        let might_write = may_write(&stmt.sql);
        Box::pin(async move {
            let result = self.inner.inner.query_all_raw(stmt).await?;
            if might_write {
                self.drain_and_dispatch().await?;
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
                // register_table installs the capture triggers before
                // registration — a registered table must never have a
                // capture gap.
                self.db.register_table(entry.meta.clone()).await?;
            }
        }
        // Drain anything already captured: crash-window leftovers (a user
        // write committed but its bookkeeping never ran), writes from a
        // separate process sharing this DB file (iOS background sync), and
        // pre-sync() writes recorded by triggers from a previous run.
        // Best-effort — rows persist for the next drain if this one fails.
        if let Err(e) = self.db.drain_and_dispatch().await {
            log::warn!("Startup capture drain failed (will retry on next write): {e}");
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
        // One-time N8 repair: clear any defeated tombstone still sitting on a live
        // row (from a losing delete applied before the tombstone-clear fix). This
        // lets reconciliation reconverge such rows. Best-effort — a failure here
        // must not block startup, only leave the (data-correct) digest mismatch.
        if let Err(e) =
            crate::shadow::heal_lost_tombstones(&self.db.inner.inner, self.db.registry()).await
        {
            log::warn!("N8 tombstone heal sweep failed (non-fatal): {e}");
        }

        // Physically collect aged tombstones off the startup path. Exclusion
        // already hides them from every sync/reconcile/conflict surface, so
        // this sweep only reclaims storage — its timing is a purely local
        // concern and a failure costs nothing but disk.
        let gc_db = self.db.inner.inner.clone();
        let gc_registry = self.db.registry().clone();
        tokio::spawn(async move {
            match crate::shadow::gc_aged_tombstones(&gc_db, &gc_registry).await {
                Ok(0) => {}
                Ok(n) => log::info!("Tombstone GC collected {n} aged tombstones"),
                Err(e) => log::warn!("Tombstone GC sweep failed (non-fatal): {e}"),
            }
        });

        // Signal the engine that tables are registered and sync can begin. Via
        // `registry_ready()` so runtime-joined groups also notify the engine
        // (GroupRegistryReady), not just the default group's Notify.
        self.db.registry_ready();
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
/// A single additional sync group joined at runtime via
/// [`WaveSyncNode::join_group`], persisted so a background wake can rejoin it.
///
/// The node's *default* group is described by the top-level [`SyncConfig`]
/// fields; these entries are the extra groups. `database_url` is the group's
/// own sibling SQLite file (derived from the node's base URL at join time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    pub user_topic: String,
    pub passphrase: String,
    pub database_url: String,
    /// Stable kind label (from `join_group(.., kind)`), used to resolve
    /// `EntityScope::Groups` when a background wake rejoins this group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// **Security note**: The passphrase is stored in plaintext. On Android/iOS the app's
/// data directory is sandboxed (same protection as the SQLite database itself).
#[derive(Serialize, Deserialize)]
pub struct SyncConfig {
    pub database_url: String,
    pub topic: String,
    pub relay_server: Option<String>,
    /// Additional relay servers tried in order if the primary fails.
    /// Empty for backward compatibility with single-relay configs written
    /// before multi-relay fallback shipped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relay_fallbacks: Vec<String>,
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
    /// Additional groups joined at runtime via [`WaveSyncNode::join_group`].
    /// A background wake rebuilds the default group from the top-level fields,
    /// then rejoins each of these. Empty for single-group / back-compat configs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<GroupConfig>,
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
    ///
    /// The file holds the group passphrase(s) in cleartext. On mobile the app
    /// sandbox already isolates it, but on a shared desktop the default umask can
    /// leave it world-readable, so restrict it to owner-only (`0600`) on Unix.
    fn save(&self) -> Result<(), String> {
        let path = Self::config_path(&self.database_url)
            .ok_or_else(|| "Cannot derive config path from database URL".to_string())?;
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize config: {e}"))?;
        std::fs::write(&path, json)
            .map_err(|e| format!("Failed to write config to {}: {e}", path.display()))?;
        restrict_file_permissions(&path);
        Ok(())
    }

    /// Record a runtime-joined group in the persisted config (load → upsert by
    /// `user_topic` → save). Keyed off `base_database_url` (the default group's),
    /// which is where the config file lives. Best-effort: errors (e.g. no config
    /// written yet, as with in-memory test DBs) leave the live join unaffected.
    fn persist_group_joined(base_database_url: &str, group: GroupConfig) -> Result<(), String> {
        let mut config = Self::load(base_database_url)?;
        config.groups.retain(|g| g.user_topic != group.user_topic);
        config.groups.push(group);
        config.save()
    }

    /// Remove a runtime-joined group from the persisted config (load → drop by
    /// `user_topic` → save). Best-effort, same as [`Self::persist_group_joined`].
    fn persist_group_left(base_database_url: &str, user_topic: &str) -> Result<(), String> {
        let mut config = Self::load(base_database_url)?;
        let before = config.groups.len();
        config.groups.retain(|g| g.user_topic != user_topic);
        if config.groups.len() == before {
            return Ok(());
        }
        config.save()
    }
}

/// Restrict a file to owner read/write (`0600`) on Unix. No-op elsewhere and
/// best-effort — a failure to tighten permissions must not fail the write that
/// already succeeded, but it is logged so a misconfigured environment is visible.
fn restrict_file_permissions(path: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            log::warn!("Could not restrict permissions on {}: {e}", path.display());
        }
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Builder for `WaveSyncDb`.
pub struct WaveSyncDbBuilder {
    database_url: String,
    node_id: Option<NodeId>,
    relay_server: Option<String>,
    relay_fallbacks: Vec<String>,
    topic: String,
    sync_interval: std::time::Duration,
    mdns_enabled: bool,
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
    tcp_enabled: bool,
    change_channel_capacity: usize,
    tombstone_retention: Option<Option<std::time::Duration>>,
}

impl WaveSyncDbBuilder {
    pub fn new(url: &str, topic: &str) -> Self {
        let defaults = crate::engine::EngineConfig::default();
        Self {
            database_url: url.to_string(),
            node_id: None,
            relay_server: None,
            relay_fallbacks: Vec::new(),
            topic: topic.to_string(),
            sync_interval: defaults.sync_interval,
            mdns_enabled: defaults.mdns_enabled,
            mdns_query_interval: defaults.mdns_query_interval,
            mdns_ttl: defaults.mdns_ttl,
            group_key: None,
            passphrase: None,
            bootstrap_peers: Vec::new(),
            rendezvous_server: None,
            rendezvous_discover_interval: defaults.rendezvous_discover_interval,
            rendezvous_ttl: defaults.rendezvous_ttl,
            ipv6: defaults.ipv6,
            push_token: None,
            #[cfg(feature = "push-sync")]
            fcm_credentials: None,
            api_key: None,
            keep_alive_interval: defaults.keep_alive_interval,
            circuit_max_duration: defaults.circuit_max_duration,
            tcp_enabled: defaults.tcp_enabled,
            change_channel_capacity: 1024,
            tombstone_retention: None,
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
    ///
    /// To configure fallback relays (recommended for production to remove
    /// the single-point-of-failure), call [`Self::with_relay_fallback`] one
    /// or more times after this, or use [`Self::with_relay_fallbacks`].
    pub fn with_relay_server(mut self, addr: &str) -> Self {
        self.relay_server = Some(addr.to_string());
        self
    }

    /// Add a fallback relay server. Tried after the primary
    /// ([`Self::with_relay_server`]) has failed repeatedly. Can be called
    /// multiple times to add several fallbacks in priority order.
    pub fn with_relay_fallback(mut self, addr: &str) -> Self {
        self.relay_fallbacks.push(addr.to_string());
        self
    }

    /// Add multiple fallback relays in one call. Preserves order.
    pub fn with_relay_fallbacks<I, S>(mut self, addrs: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.relay_fallbacks
            .extend(addrs.into_iter().map(|s| s.as_ref().to_string()));
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

    /// Enable or disable IPv6 listen addresses in addition to IPv4.
    /// Default: `true`.
    ///
    /// IPv6 sidesteps CGNAT — most modern cellular carriers (T-Mobile,
    /// Verizon, Jio, etc.) ship IPv6 by default, eliminating the need for
    /// hole-punching or circuit relay for peers on those networks. Only
    /// disable this if your deployment environment has known-broken v6.
    pub fn with_ipv6(mut self, enabled: bool) -> Self {
        self.ipv6 = enabled;
        self
    }

    /// Enable or disable TCP as a secondary transport alongside QUIC.
    /// Default: `false`.
    ///
    /// QUIC-only is the recommended path for ~95% of deployments — it
    /// has a faster cold start (1 RTT vs 2-3 for TCP+TLS+yamux) and
    /// avoids a known issue where dual TCP+QUIC dials confused
    /// circuit-relay on cellular. Enable this only if your users hit
    /// networks that block UDP entirely (some corporate firewalls,
    /// captive-portal Wi-Fi). Accepts the cold-start trade-off and
    /// the cellular-circuit-relay risk; we recommend measuring before
    /// flipping it on.
    pub fn with_tcp_enabled(mut self, enabled: bool) -> Self {
        self.tcp_enabled = enabled;
        self
    }

    pub fn with_sync_interval(mut self, interval: std::time::Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    /// Capacity of the per-group `ChangeNotification` broadcast channel
    /// (default 1024, minimum 16). A subscriber that falls more than this
    /// many notifications behind sees `Lagged` and the reactive hooks fall
    /// back to a debounced full-table reload — raise this for bursty
    /// writers whose subscribers must keep per-row deltas.
    /// Tombstone retention window (default 7 days). Deleted rows'
    /// tombstones older than this are excluded from sync/reconciliation and
    /// eventually garbage-collected. Tradeoff: a peer offline LONGER than
    /// this window can resurrect rows deleted in its absence. Persisted in
    /// the database so a background-sync process ages tombstones by the
    /// same rule.
    pub fn with_tombstone_retention(mut self, retention: std::time::Duration) -> Self {
        self.tombstone_retention = Some(Some(retention));
        self
    }

    /// Disable tombstone garbage collection entirely: tombstones are kept
    /// (and synced) forever. Storage grows with all-time deletes.
    pub fn without_tombstone_gc(mut self) -> Self {
        self.tombstone_retention = Some(None);
        self
    }

    pub fn with_change_channel_capacity(mut self, capacity: usize) -> Self {
        self.change_channel_capacity = capacity.max(16);
        self
    }

    /// Enable or disable mDNS LAN discovery at startup. Default: `true`.
    ///
    /// When `false`, the engine never announces itself on the LAN and never
    /// queries for other peers — useful for apps that want peer discovery
    /// to go through a private rendezvous server or relay only, without
    /// broadcasting on every network the device joins. Can be flipped at
    /// runtime via [`WaveSyncDb::set_mdns_enabled`].
    pub fn with_mdns_enabled(mut self, enabled: bool) -> Self {
        self.mdns_enabled = enabled;
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
        // Argon2id derivation, salted with the user topic (fixed at
        // `WaveSyncDbBuilder::new`) — intentionally slow, runs once here.
        self.group_key = Some(crate::auth::GroupKey::from_passphrase(
            passphrase,
            &self.topic,
        ));
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
        //
        // This is intentionally NOT gated on `fcm_credentials.is_some()`. The
        // device's FCM credentials (project/app/api key) are never transmitted:
        // `RegisterToken` carries only `(topic, platform, token)`, and the relay
        // sends the push with its *own* service account. So a device only needs
        // its token to be woken — requiring the app to also embed sender
        // credentials just to register left Android silently unable to wake
        // whenever the consumer didn't ship google-services.json, while iOS
        // (which reads its APNs token unconditionally, below) worked fine.
        #[cfg(all(feature = "push-sync", target_os = "android"))]
        if self.push_token.is_none() {
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
        // Capture + guard tables must exist before any registered table's
        // triggers can fire — a trigger referencing a missing table fails
        // the user's write outright.
        crate::capture::ensure_capture_tables(&inner).await?;
        // Persist the retention window in the DB so every process sharing
        // this file (foreground + background sync) ages tombstones by the
        // same rule. Absent key = the 7-day default.
        if let Some(retention) = self.tombstone_retention {
            crate::shadow::set_tombstone_retention(&inner, retention).await?;
        }
        let site_id = crate::shadow::get_site_id(&inner).await?;

        let node_id = self.node_id.unwrap_or(site_id);

        let db_version = crate::shadow::get_db_version(&inner).await?;

        let (sync_tx, sync_rx) = mpsc::channel::<TaggedChangeset>(256);
        let (change_tx, _) = broadcast::channel::<ChangeNotification>(self.change_channel_capacity);
        // Smaller than change_tx: notifications are post-policy + post-coalesce,
        // so far fewer than raw change events.
        let (notification_tx, _) = broadcast::channel::<crate::notify::Notification>(256);
        // Resume/refresh signal for foreground hooks (see `refresh_tx` doc).
        let (refresh_tx, _) = broadcast::channel::<()>(16);

        let registry = Arc::new(TableRegistry::new());
        // Populate per-table notification policies from every #[derive(SyncNotify)]
        // entity linked into the binary (mirrors SyncEntityInfo discovery). Keyed
        // by table name; only fires for tables that actually emit changes.
        let notification_registry = Arc::new(crate::registry::NotificationRegistry::new());
        let mut notify_tables: Vec<String> = Vec::new();
        for info in inventory::iter::<crate::notify::NotifyEntityInfo> {
            let (table_name, dispatch) = (info.make)();
            notify_tables.push(table_name.clone());
            notification_registry.register(table_name, dispatch);
        }
        // Diagnostic: 0 here means no entity derives `SyncNotify`, so no user
        // notification will ever fire regardless of sync working. A populated
        // list but no "notification: generated" logs means `on_sync` returned
        // None for the changes that arrived.
        log::info!(
            "SyncNotify: {} notification polic{} registered: [{}]",
            notify_tables.len(),
            if notify_tables.len() == 1 { "y" } else { "ies" },
            notify_tables.join(", "),
        );
        let registry_ready = Arc::new(Notify::new());

        // Create peer versions table
        crate::peer_tracker::create_peer_versions_table(&inner).await?;

        // Create cached peer-addresses table (issue #29). Used by the
        // engine to pre-dial known good peers at startup before discovery
        // has had time to find them.
        crate::peer_addrs::create_peer_addrs_table(&inner).await?;

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

        // Resolve fallback relays. Invalid ones are skipped with a warning
        // rather than failing the whole build — a typo in a fallback
        // shouldn't take down the primary path.
        let mut relay_fallbacks: Vec<libp2p::Multiaddr> = Vec::new();
        for s in &self.relay_fallbacks {
            match parse_and_resolve_multiaddr(s).await {
                Ok(addr) => relay_fallbacks.push(addr),
                Err(e) => log::warn!("Skipping invalid relay fallback address '{s}': {e}"),
            }
        }

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

        // Preserve any runtime-joined groups recorded by a previous launch —
        // build() runs every startup and would otherwise wipe them.
        let preserved_groups = SyncConfig::load(&self.database_url)
            .map(|c| c.groups)
            .unwrap_or_default();
        let sync_config = SyncConfig {
            database_url: self.database_url.clone(),
            topic: self.topic.clone(),
            relay_server: self.relay_server.clone(),
            relay_fallbacks: self.relay_fallbacks.clone(),
            passphrase: self.passphrase,
            rendezvous_server: self.rendezvous_server.clone(),
            bootstrap_peers: self.bootstrap_peers.clone(),
            api_key: self.api_key.clone(),
            ipv6: self.ipv6,
            crate_name: None, // Set by SchemaBuilder::sync()
            fcm_project_id,
            fcm_app_id,
            fcm_api_key,
            groups: preserved_groups,
        };
        if let Err(e) = sync_config.save() {
            log::warn!("Failed to save sync config for background services: {e}");
        }

        let engine_config = crate::engine::EngineConfig {
            sync_interval: self.sync_interval,
            mdns_enabled: self.mdns_enabled,
            mdns_query_interval: self.mdns_query_interval,
            mdns_ttl: self.mdns_ttl,
            bootstrap_peers,
            relay_server,
            relay_fallbacks,
            rendezvous_server,
            rendezvous_discover_interval: self.rendezvous_discover_interval,
            rendezvous_ttl: self.rendezvous_ttl,
            ipv6: self.ipv6,
            push_token: self.push_token,
            api_key: self.api_key,
            keep_alive_interval: self.keep_alive_interval,
            circuit_max_duration: self.circuit_max_duration,
            tcp_enabled: self.tcp_enabled,
        };

        // Diagnostics counters are owned jointly by the engine task (writer)
        // and `WaveSyncDbInner` (reader via `WaveSyncDb::diagnostics`). The
        // engine clones the Arc, increments through atomic operations, and
        // never blocks on it.
        let diagnostics = Arc::new(crate::diagnostics::Counters::default());
        // Per-peer byte/health bookkeeping, threaded the same way as
        // `diagnostics` above so a future `WaveSyncDb`-level accessor has it
        // ready — see `crate::diagnostics::PeerHealthStore`.
        let peer_health = Arc::new(crate::diagnostics::PeerHealthStore::new());

        let db_version_cache = Arc::new(AtomicU64::new(db_version));

        // Effective (PSK-derived) topic for the default group. Computed before
        // `self.topic` / `self.group_key` are consumed by `start_engine`, and
        // used both as the default group's routing tag (the changeset stamp in
        // `dispatch_sync`) and to register the default group in the node map.
        let default_user_topic = self.topic.clone();
        let effective_topic = match &self.group_key {
            Some(gk) => gk.derive_topic(&self.topic),
            None => self.topic.clone(),
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
            diagnostics.clone(),
            peer_health.clone(),
            db_version_cache.clone(),
            notification_tx.clone(),
            notification_registry,
        );

        // The node owns the engine and everything shared across groups. Its
        // `Arc` is held by every group handle, so the engine is aborted only
        // when the last handle (and any held `WaveSyncNode`) drops — preserving
        // the original single-group teardown behaviour.
        let node = Arc::new(WaveSyncNodeInner {
            cmd_tx,
            tagged_sync_tx: sync_tx,
            engine_handle: std::sync::Mutex::new(Some(engine_handle)),
            network_status,
            network_event_tx,
            diagnostics,
            peer_health,
            // The default group's channel is the node-level channel; joined
            // groups clone this same sender so all notifications merge here.
            notification_tx: notification_tx.clone(),
            refresh_tx,
            groups: std::sync::Mutex::new(HashMap::new()),
            base_database_url: self.database_url.clone(),
            change_channel_capacity: self.change_channel_capacity,
            tombstone_retention: self.tombstone_retention,
        });

        let db = WaveSyncDb {
            inner: Arc::new(WaveSyncDbInner {
                inner,
                database_url: self.database_url,
                sync_tx: node.tagged_sync_tx.clone(),
                effective_topic,
                is_default_group: true,
                group_kind: None,
                change_tx,
                notification_tx,
                site_id,
                db_version: Mutex::new(db_version),
                db_version_cache,
                node_id,
                registry,
                registry_ready,
                node: node.clone(),
                table_cache: std::sync::RwLock::new(HashMap::new()),
            }),
        };

        // Register the default group under its user topic so a later
        // `join_group` for the same topic is idempotent.
        node.groups
            .lock()
            .unwrap()
            .insert(default_user_topic, Arc::downgrade(&db.inner));

        Ok(db)
    }
}

impl WaveSyncNode {
    /// Join an additional sync group at runtime, served by this node's existing
    /// libp2p engine and peer identity.
    ///
    /// Idempotent: if `user_topic` is already joined, the existing handle is
    /// returned. Each group is backed by its own SQLite file derived from the
    /// node's base URL (the default group keeps the original URL). The returned
    /// [`WaveSyncDb`] must have its schema registered
    /// (`.schema().register(..).sync()` / `register_table`) before it syncs,
    /// exactly like the handle returned by [`WaveSyncDbBuilder::build`].
    pub async fn join_group(
        &self,
        user_topic: &str,
        passphrase: &str,
        kind: Option<&str>,
    ) -> Result<WaveSyncDb, DbErr> {
        // Idempotency: return the existing handle if still alive. A dead `Weak`
        // (handle already dropped) falls through to a fresh join.
        if let Some(inner) = self
            .inner
            .groups
            .lock()
            .unwrap()
            .get(user_topic)
            .and_then(|w| w.upgrade())
        {
            return Ok(WaveSyncDb { inner });
        }

        let group_key = GroupKey::from_passphrase(passphrase, user_topic);
        let effective_topic = group_key.derive_topic(user_topic);

        // Per-group DB file derived from the node's base URL. The effective
        // topic is already `wavesync2-<hex>`, so it is filesystem-safe.
        let group_url = derive_group_database_url(&self.inner.base_database_url, &effective_topic);

        let mut opts = ConnectOptions::new(&group_url);
        opts.sqlx_logging_level(log::LevelFilter::Debug);
        let db = Database::connect(opts).await?;

        // Same per-DB setup that `build()` performs for the default group.
        crate::shadow::create_meta_table(&db).await?;
        crate::capture::ensure_capture_tables(&db).await?;
        if let Some(retention) = self.inner.tombstone_retention {
            crate::shadow::set_tombstone_retention(&db, retention).await?;
        }
        let site_id = crate::shadow::get_site_id(&db).await?;
        let db_version = crate::shadow::get_db_version(&db).await?;
        let db_version_cache = Arc::new(AtomicU64::new(db_version));
        crate::peer_tracker::create_peer_versions_table(&db).await?;
        crate::peer_addrs::create_peer_addrs_table(&db).await?;

        let registry = Arc::new(TableRegistry::new());
        let registry_ready = Arc::new(Notify::new());
        let (change_tx, _) =
            broadcast::channel::<ChangeNotification>(self.inner.change_channel_capacity);
        // Reuse the node-level notification channel (not a fresh per-group one)
        // so this group's notifications reach the same `notification_rx()` every
        // other group does — one `use_sync_notifications` covers the whole node.
        let notification_tx = self.inner.notification_tx.clone();
        let notification_registry = Arc::new(crate::registry::NotificationRegistry::new());
        let mut notify_tables: Vec<String> = Vec::new();
        for info in inventory::iter::<crate::notify::NotifyEntityInfo> {
            let (table_name, dispatch) = (info.make)();
            notify_tables.push(table_name.clone());
            notification_registry.register(table_name, dispatch);
        }
        log::info!(
            "SyncNotify (group): {} notification polic{} registered: [{}]",
            notify_tables.len(),
            if notify_tables.len() == 1 { "y" } else { "ies" },
            notify_tables.join(", "),
        );

        let node_id = site_id;

        // Hydrate this group's last-known peer versions from its own DB.
        let peer_db_versions = match crate::peer_tracker::get_all_peer_versions(&db).await {
            Ok(rows) => crate::peer_tracker::parse_peer_versions(rows),
            Err(e) => {
                log::warn!("Failed to hydrate peer versions for joined group: {e}");
                HashMap::new()
            }
        };

        let db_handle = WaveSyncDb {
            inner: Arc::new(WaveSyncDbInner {
                inner: db.clone(),
                database_url: group_url.clone(),
                sync_tx: self.inner.tagged_sync_tx.clone(),
                effective_topic: effective_topic.clone(),
                is_default_group: false,
                group_kind: kind.map(|k| k.to_string()),
                change_tx: change_tx.clone(),
                notification_tx: notification_tx.clone(),
                site_id,
                db_version: Mutex::new(db_version),
                db_version_cache: db_version_cache.clone(),
                node_id,
                registry: registry.clone(),
                registry_ready: registry_ready.clone(),
                node: self.inner.clone(),
                table_cache: std::sync::RwLock::new(HashMap::new()),
            }),
        };

        self.inner
            .groups
            .lock()
            .unwrap()
            .insert(user_topic.to_string(), Arc::downgrade(&db_handle.inner));

        // Hand the group to the engine. The engine inserts the GroupState and
        // wires discovery (rendezvous namespace + connected-peer sweep).
        let init = GroupInit {
            db,
            user_topic: user_topic.to_string(),
            effective_topic,
            group_key: Some(group_key),
            site_id,
            local_db_version: db_version,
            db_version_cache,
            registry,
            registry_ready,
            change_tx,
            notification_tx,
            notification_registry,
            peer_db_versions,
        };
        let _ = self
            .inner
            .cmd_tx
            .send(EngineCommand::JoinGroup(Box::new(init)))
            .await;

        // Persist the group so a background wake (which only has the default
        // group's config on disk) can rejoin it. Best-effort: a missing config
        // file (e.g. in-memory test DB) is not fatal to the live join.
        if let Err(e) = SyncConfig::persist_group_joined(
            &self.inner.base_database_url,
            GroupConfig {
                user_topic: user_topic.to_string(),
                passphrase: passphrase.to_string(),
                database_url: group_url,
                kind: kind.map(|k| k.to_string()),
            },
        ) {
            log::debug!("Could not persist joined group '{user_topic}' to config: {e}");
        }

        Ok(db_handle)
    }

    /// Leave a sync group: the engine stops syncing it and the rendezvous
    /// namespace TTL-expires. The DB file is preserved. Leaving the node's
    /// default group is a no-op.
    pub async fn leave_group(&self, db: &WaveSyncDb) {
        let effective_topic = db.inner.effective_topic.clone();
        let _ = self
            .inner
            .cmd_tx
            .send(EngineCommand::LeaveGroup {
                effective_topic: effective_topic.clone(),
            })
            .await;
        // Drop the group from the node map, capturing its user topic(s) so we
        // can also remove it from the persisted config.
        let mut left_user_topics: Vec<String> = Vec::new();
        self.inner
            .groups
            .lock()
            .unwrap()
            .retain(|user_topic, weak| {
                let keep = weak
                    .upgrade()
                    .map(|inner| inner.effective_topic != effective_topic)
                    .unwrap_or(false);
                if !keep {
                    left_user_topics.push(user_topic.clone());
                }
                keep
            });
        for user_topic in left_user_topics {
            if let Err(e) =
                SyncConfig::persist_group_left(&self.inner.base_database_url, &user_topic)
            {
                log::debug!("Could not remove left group '{user_topic}' from config: {e}");
            }
        }
    }
}

/// Derive a per-group SQLite URL from the node's base URL.
///
/// `sqlite:/path/app.db` → `sqlite:/path/app__<effective_topic>.db?mode=rwc`.
/// Query strings on the base URL are preserved on the derived URL. Non-sqlite
/// or unrecognized URLs fall back to suffixing the whole URL, which is enough
/// for the file-backed sqlite URLs WaveSyncDB targets.
fn derive_group_database_url(base: &str, effective_topic: &str) -> String {
    // Split off any existing query string.
    let (path_part, query_part) = match base.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (base, None),
    };

    let suffixed = if let Some(stripped) = path_part.strip_suffix(".db") {
        format!("{stripped}__{effective_topic}.db")
    } else {
        format!("{path_part}__{effective_topic}.db")
    };

    match query_part {
        Some(q) => format!("{suffixed}?{q}"),
        // Ensure the file is created if it doesn't exist yet.
        None => format!("{suffixed}?mode=rwc"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // may_write is the drain pre-filter: false positives are harmless (the
    // drain peeks an empty capture table), false negatives lose sync — it
    // must match every statement shape that can fire a capture trigger.

    #[test]
    fn test_may_write_plain_statements() {
        assert!(may_write("INSERT INTO t VALUES (1)"));
        assert!(may_write("UPDATE t SET a = 1"));
        assert!(may_write("DELETE FROM t"));
        assert!(may_write("REPLACE INTO t VALUES (1)"));
        assert!(may_write("insert into t values (1)"));
    }

    #[test]
    fn test_may_write_wrapped_statements() {
        assert!(may_write(
            "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"
        ));
        assert!(may_write("INSERT OR REPLACE INTO t VALUES (1)"));
        assert!(may_write(
            "CREATE TABLE a (id TEXT); INSERT INTO a VALUES ('x');"
        ));
        assert!(may_write("  \n\tUPDATE t SET a = 1"));
    }

    #[test]
    fn test_may_write_reads_skipped() {
        assert!(!may_write("SELECT * FROM tasks"));
        assert!(!may_write("PRAGMA table_info(tasks)"));
        assert!(!may_write("CREATE TABLE t (id TEXT)"));
    }
}
