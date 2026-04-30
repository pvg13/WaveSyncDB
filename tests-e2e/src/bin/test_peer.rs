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
//! | `RUST_LOG` | no | log level filter |

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
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .parse_env("RUST_LOG")
        .init();

    let bind: SocketAddr = std::env::var("BIND_ADDR")
        .context("BIND_ADDR is required")?
        .parse()
        .context("BIND_ADDR is not a valid socket address")?;
    let db_url = std::env::var("DB_URL").context("DB_URL is required")?;
    let topic = std::env::var("TOPIC").context("TOPIC is required")?;

    let mut builder = WaveSyncDbBuilder::new(&db_url, &topic)
        .with_sync_interval(Duration::from_secs(2));

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

    let db = builder.build().await.context("build WaveSyncDb")?;
    db.schema()
        .register(task::Entity)
        .sync()
        .await
        .context("register task entity")?;

    let state = AppState { db };
    let router = Router::new()
        .route("/health", get(health))
        .route("/peers", get(peers))
        .route("/tasks", get(list_tasks).post(insert_task))
        .route("/tasks/:id", get(get_task).put(update_task))
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

async fn list_tasks(State(s): State<AppState>) -> Result<Json<Vec<Task>>, AppError> {
    let rows = task::Entity::find().all(&s.db).await?;
    Ok(Json(rows.into_iter().map(Task::from).collect()))
}

async fn get_task(
    Path(id): Path<String>,
    State(s): State<AppState>,
) -> Result<Json<Task>, AppError> {
    let row = task::Entity::find_by_id(id)
        .one(&s.db)
        .await?
        .ok_or(AppError::NotFound)?;
    Ok(Json(row.into()))
}

async fn insert_task(
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    task::ActiveModel {
        id: Set(t.id),
        title: Set(t.title),
        completed: Set(t.completed),
    }
    .insert(&s.db)
    .await?;
    Ok(StatusCode::CREATED)
}

async fn update_task(
    Path(id): Path<String>,
    State(s): State<AppState>,
    Json(t): Json<Task>,
) -> Result<StatusCode, AppError> {
    // Match the SeaORM idiom: load + into_active_model + set + update.
    let existing = task::Entity::find_by_id(&id)
        .one(&s.db)
        .await?
        .ok_or(AppError::NotFound)?;
    let mut m = existing.into_active_model();
    m.title = Set(t.title);
    m.completed = Set(t.completed);
    m.update(&s.db).await?;
    Ok(StatusCode::OK)
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
            AppError::Db(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("db error: {e}"),
            )
                .into_response(),
        }
    }
}
