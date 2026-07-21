//! HTTP-wrapped WaveSyncDB peer binary, run inside a container by the
//! E2E harness. Reads its config from environment variables, opens a
//! `WaveSyncDb`, and exposes a small REST API the harness uses to
//! script writes / reads / state queries.
//!
//! Environment variables:
//!
//! | Var | Required | Description |
//! |---|---|---|
//! | `BIND_ADDR` | yes | `host:port` to listen on, e.g. `0.0.0.0:8080` |
//! | `DB_URL` | yes | SeaORM-compatible SQLite URL |
//! | `TOPIC` | yes | WaveSyncDB topic |
//! | `RELAY_ADDR` | no | libp2p multiaddr of the relay |
//! | `PASSPHRASE` | no | enable HMAC + topic derivation |
//! | `MDNS_ENABLED` | no | `false` disables mDNS (forces relay-only discovery) |
//! | `SECONDARY_TOPIC` | no | join a second (non-default) group on this topic |
//! | `SECONDARY_PASSPHRASE` | no | passphrase for the secondary group |
//! | `PUSH_TOKEN` | no | register this (dummy) push token with the relay so NotifyTopic is accepted |
//! | `RUST_LOG` | no | log level filter |
//!
//! When `SECONDARY_TOPIC` is set the peer joins a second group at runtime via
//! `WaveSyncNode::join_group` and exposes a parallel `/g2/tasks` route set
//! backed by that group's own database. This exercises the multi-group sync
//! path (each group is a separate rendezvous namespace).

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use sea_orm::{ActiveModelTrait, EntityTrait, IntoActiveModel, Set};
use serde::{Deserialize, Serialize};
use wavesyncdb::{WaveSyncDb, WaveSyncDbBuilder};
use wavesyncdb_e2e::task_entity as task;

#[derive(Clone)]
struct AppState {
    db: WaveSyncDb,
    /// Secondary (non-default) group, present only when `SECONDARY_TOPIC`
    /// is configured. Backs the `/g2/...` routes.
    db2: Option<WaveSyncDb>,
    /// The default group's database URL, kept for `/push_wake`: the
    /// background-sync entry point is keyed by URL exactly like the mobile
    /// FFI layer is.
    db_url: String,
}

impl AppState {
    /// Resolve the secondary group's handle or return a 404-equivalent
    /// error so scenarios that hit `/g2/...` without configuring a
    /// secondary group fail loudly instead of silently using the default.
    fn group2(&self) -> Result<&WaveSyncDb, AppError> {
        self.db2.as_ref().ok_or(AppError::NotFound)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Task {
    id: String,
    title: String,
    completed: bool,
}

impl From<task::Model> for Task {
    fn from(m: task::Model) -> Self {
        Self {
            id: m.id,
            title: m.title,
            completed: m.completed,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let bind: SocketAddr = std::env::var("BIND_ADDR")
        .context("BIND_ADDR is required")?
        .parse()
        .context("BIND_ADDR is not a valid socket address")?;
    let db_url = std::env::var("DB_URL").context("DB_URL is required")?;
    let topic = std::env::var("TOPIC").context("TOPIC is required")?;

    let mut builder =
        WaveSyncDbBuilder::new(&db_url, &topic).with_sync_interval(Duration::from_secs(2));

    if let Ok(p) = std::env::var("PASSPHRASE")
        && !p.is_empty()
    {
        builder = builder.with_passphrase(&p);
    }

    if let Ok(addr) = std::env::var("RELAY_ADDR")
        && !addr.is_empty()
    {
        builder = builder.with_relay_server(&addr);
    }

    // `MDNS_ENABLED=false` forces relay/rendezvous-only discovery — the
    // condition under which the secondary-group cookie bug surfaces (LAN
    // mDNS otherwise masks it by discovering peers directly).
    if matches!(std::env::var("MDNS_ENABLED").as_deref(), Ok("false")) {
        builder = builder.with_mdns_enabled(false);
    }

    // Ack-threshold mailbox dial (#107), in seconds. Unset/empty keeps the
    // default always-append behavior.
    if let Ok(secs) = std::env::var("MAILBOX_APPEND_AFTER_SECS")
        && let Ok(secs) = secs.parse::<u64>()
    {
        builder = builder.with_mailbox_append_after(Duration::from_secs(secs));
    }

    let db = builder.build().await.context("build WaveSyncDb")?;
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .context("register task entity")?;

    // Optionally join a secondary (non-default) group at runtime. This is
    // the multi-group path: a second rendezvous namespace served over the
    // same swarm.
    let db2 = if let Ok(secondary_topic) = std::env::var("SECONDARY_TOPIC")
        && !secondary_topic.is_empty()
    {
        let secondary_pass = std::env::var("SECONDARY_PASSPHRASE").unwrap_or_default();
        let g2 = db
            .node()
            .join_group(&secondary_topic, &secondary_pass, Some("household"))
            .await
            .context("join secondary group")?;
        g2.schema()
            .register(task::Entity)
            .sync()
            .await
            .context("register task entity on secondary group")?;
        Some(g2)
    } else {
        None
    };

    // PUSH_TOKEN: register a (dummy) push token with the relay. The relay
    // only accepts NotifyTopic from peers that registered a token for the
    // topic (anti-wake-spam), so a host-side writer that should trigger
    // FCM wakes on real devices registers this placeholder — the relay's
    // fan-out excludes the sender's own token, so nothing is sent to it.
    if let Ok(token) = std::env::var("PUSH_TOKEN")
        && !token.is_empty()
    {
        db.register_push_token("Fcm", &token);
    }

    let state = AppState { db, db2, db_url };
    let router = Router::new()
        .route("/health", get(health))
        .route("/peers", get(peers))
        .route("/diagnostics", get(diagnostics))
        .route("/push_wake", post(push_wake))
        .route("/register_push", post(register_push))
        .route("/tasks", get(list_tasks).post(insert_task))
        .route("/tasks/:id", get(get_task).put(update_task))
        .route("/g2/tasks", get(list_tasks_g2).post(insert_task_g2))
        .route("/g2/tasks/:id", get(get_task_g2).put(update_task_g2))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind {bind}"))?;
    println!("test-peer ready on {bind}");
    axum::serve(listener, router).await.context("axum serve")?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn peers(State(s): State<AppState>) -> impl IntoResponse {
    let n = s.db.network_status().connected_peers.len();
    Json(serde_json::json!({"connected": n}))
}

async fn diagnostics(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.db.diagnostics())
}

/// (Re-)register the `PUSH_TOKEN` dummy token with the relay. Needed by
/// scenarios where another peer's write pushes to this dummy token first:
/// FCM rejects it as invalid and the relay (correctly) evicts the row,
/// which would then make this peer's own `NotifyTopic` be rejected as
/// unregistered. The harness calls this right before the write whose push
/// it actually wants delivered.
async fn register_push(State(s): State<AppState>) -> impl IntoResponse {
    match std::env::var("PUSH_TOKEN") {
        Ok(t) if !t.is_empty() => {
            s.db.register_push_token("Fcm", &t);
            StatusCode::OK
        }
        _ => StatusCode::PRECONDITION_FAILED,
    }
}

#[derive(Debug, Deserialize, Default)]
struct PushWakeRequest {
    /// Background-execution budget, mirroring the OS-granted push window.
    timeout_secs: Option<u64>,
}

/// Simulate a not-killed push wake: run the same shared background-sync
/// entry point the mobile FFI layer calls (`run_background_sync`), on a
/// fresh tokio runtime in a blocking thread, against this peer's own live
/// database. With the engine alive in this process, the call must reuse it
/// (node registry) rather than build a duplicate-identity second engine.
async fn push_wake(
    State(s): State<AppState>,
    body: Option<Json<PushWakeRequest>>,
) -> impl IntoResponse {
    let timeout = Duration::from_secs(body.and_then(|Json(b)| b.timeout_secs).unwrap_or(20));
    let db_url = s.db_url.clone();
    let started = std::time::Instant::now();
    let outcome = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        rt.block_on(
            wavesyncdb::background_sync::background_sync_with_peers_for_topic(
                &db_url,
                timeout,
                &[],
                None,
            ),
        )
        .map_err(|e| e.to_string())
    })
    .await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    use wavesyncdb::background_sync::BackgroundSyncResult;
    let (result, peers_synced) = match outcome {
        Ok(Ok(BackgroundSyncResult::Synced { peers_synced })) => ("synced", peers_synced),
        Ok(Ok(BackgroundSyncResult::NoPeers)) => ("no_peers", 0),
        Ok(Ok(BackgroundSyncResult::TimedOut { peers_synced })) => ("timed_out", peers_synced),
        Ok(Err(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e, "elapsed_ms": elapsed_ms})),
            );
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("join: {e}"), "elapsed_ms": elapsed_ms})),
            );
        }
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "result": result,
            "peers_synced": peers_synced,
            "elapsed_ms": elapsed_ms,
        })),
    )
}

// --- Core CRUD, parameterised by which group's db to use. The route
// handlers below are thin wrappers that pick the default or secondary
// group and delegate here, so both groups share identical semantics. ---

async fn db_list_tasks(db: &WaveSyncDb) -> Result<Json<Vec<Task>>, AppError> {
    let rows = task::Entity::find().all(db).await?;
    Ok(Json(rows.into_iter().map(Task::from).collect()))
}

async fn db_get_task(db: &WaveSyncDb, id: String) -> Result<Json<Task>, AppError> {
    let row = task::Entity::find_by_id(id)
        .one(db)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(row.into()))
}

async fn db_insert_task(db: &WaveSyncDb, t: Task) -> Result<StatusCode, AppError> {
    task::ActiveModel {
        id: Set(t.id),
        title: Set(t.title),
        completed: Set(t.completed),
    }
    .insert(db)
    .await?;
    Ok(StatusCode::CREATED)
}

async fn db_update_task(db: &WaveSyncDb, id: String, t: Task) -> Result<StatusCode, AppError> {
    // Match the SeaORM idiom: load + into_active_model + set + update.
    let existing = task::Entity::find_by_id(&id)
        .one(db)
        .await?
        .ok_or(AppError::NotFound)?;
    let mut m = existing.into_active_model();
    m.title = Set(t.title);
    m.completed = Set(t.completed);
    m.update(db).await?;
    Ok(StatusCode::OK)
}

async fn list_tasks(State(s): State<AppState>) -> Result<Json<Vec<Task>>, AppError> {
    db_list_tasks(&s.db).await
}

async fn get_task(
    Path(id): Path<String>,
    State(s): State<AppState>,
) -> Result<Json<Task>, AppError> {
    db_get_task(&s.db, id).await
}

async fn insert_task(
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    db_insert_task(&s.db, t).await
}

async fn update_task(
    Path(id): Path<String>,
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    db_update_task(&s.db, id, t).await
}

async fn list_tasks_g2(State(s): State<AppState>) -> Result<Json<Vec<Task>>, AppError> {
    db_list_tasks(s.group2()?).await
}

async fn get_task_g2(
    Path(id): Path<String>,
    State(s): State<AppState>,
) -> Result<Json<Task>, AppError> {
    db_get_task(s.group2()?, id).await
}

async fn insert_task_g2(
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    db_insert_task(s.group2()?, t).await
}

async fn update_task_g2(
    Path(id): Path<String>,
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    db_update_task(s.group2()?, id, t).await
}

#[derive(Debug)]
enum AppError {
    NotFound,
    Db(sea_orm::DbErr),
}

impl From<sea_orm::DbErr> for AppError {
    fn from(e: sea_orm::DbErr) -> Self {
        AppError::Db(e)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found").into_response(),
            AppError::Db(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("db error: {e}")).into_response()
            }
        }
    }
}
