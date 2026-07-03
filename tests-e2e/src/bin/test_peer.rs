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
    routing::get,
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

    let state = AppState { db, db2 };
    let router = Router::new()
        .route("/health", get(health))
        .route("/peers", get(peers))
        .route("/diagnostics", get(diagnostics))
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
