//! Headless native WaveSyncDB peer for the browser e2e harness.
//!
//! It joins the same relay/topic/passphrase as the browser page, seeds one
//! row (`id = "from-native"`), and echoes every remote insert it observes by
//! writing back `id = "native-saw-<pk>"`. The orchestrator drives the browser
//! and asserts on the resulting cross-peer state.
//!
//! Environment:
//!
//! | Var | Required | Description |
//! |---|---|---|
//! | `RELAY_ADDR`  | yes | libp2p multiaddr of the relay's TCP listener |
//! | `TOPIC`       | yes | user topic (must match the browser) |
//! | `PASSPHRASE`  | yes | shared passphrase (HMAC + topic derivation) |
//! | `DB_PATH`     | yes | filesystem path for the SQLite database |
//!
//! Prints `READY` on stdout (flushed) once the engine is up, so the
//! orchestrator can gate on it.

use std::time::Duration;

use anyhow::{Context, Result};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use tokio::sync::broadcast::error::RecvError;
use wavesyncdb::{ChangeSource, WaveSyncDbBuilder, WriteKind};

mod e2e_items {
    use sea_orm::entity::prelude::*;
    use wavesyncdb_derive::SyncEntity;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
    #[sea_orm(table_name = "e2e_items")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub body: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let relay_addr = std::env::var("RELAY_ADDR").context("RELAY_ADDR is required")?;
    let topic = std::env::var("TOPIC").context("TOPIC is required")?;
    let passphrase = std::env::var("PASSPHRASE").context("PASSPHRASE is required")?;
    let db_path = std::env::var("DB_PATH").context("DB_PATH is required")?;
    let db_url = format!("sqlite://{db_path}?mode=rwc");

    let db = WaveSyncDbBuilder::new(&db_url, &topic)
        .with_passphrase(&passphrase)
        .with_relay_server(&relay_addr)
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .context("build WaveSyncDb")?;

    db.schema()
        .register(e2e_items::Entity)
        .sync()
        .await
        .context("register e2e_items entity")?;

    // Subscribe before seeding so we never miss a notification.
    let mut changes = db.change_rx();

    e2e_items::ActiveModel {
        id: Set("from-native".to_string()),
        body: Set("hello from native".to_string()),
    }
    .insert(&db)
    .await
    .context("seed from-native row")?;

    println!("READY");
    use std::io::Write as _;
    std::io::stdout().flush().ok();

    loop {
        match changes.recv().await {
            Ok(n) => {
                // Only echo remote inserts/updates on our table, and never
                // echo an echo (or the native seed) — that would loop forever.
                let is_remote = matches!(n.source, ChangeSource::Remote { .. });
                let is_write = matches!(n.kind, WriteKind::Insert | WriteKind::Update);
                if !is_remote || !is_write || n.table != "e2e_items" {
                    continue;
                }
                let pk = n.primary_key.0;
                if pk.starts_with("native-saw-") || pk.starts_with("from-native") {
                    continue;
                }
                let echo_pk = format!("native-saw-{pk}");
                // Idempotent upsert: only insert if not already present.
                if e2e_items::Entity::find_by_id(&echo_pk)
                    .one(&db)
                    .await
                    .context("lookup echo row")?
                    .is_none()
                {
                    e2e_items::ActiveModel {
                        id: Set(echo_pk),
                        body: Set(format!("echo of {pk}")),
                    }
                    .insert(&db)
                    .await
                    .context("insert echo row")?;
                }
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => break,
        }
    }

    Ok(())
}
