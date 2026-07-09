//! Headless native WaveSyncDB peer for the browser e2e harness.
//!
//! Default group: it joins the same relay/topic/passphrase as the browser page,
//! seeds one row (`id = "from-native"`), and echoes every remote insert it
//! observes by writing back `id = "native-saw-<pk>"`.
//!
//! Second group (multi-group #93): when `GROUP2_TOPIC`/`GROUP2_PASS` are set, it
//! also `join_group`s a `kind="e2e"` group over the SAME engine, registers the
//! `e2e_group_items` entity there (scope `groups("e2e")`), seeds
//! `id = "g2-from-native"`, and echoes remote inserts as
//! `id = "g2-native-saw-<pk>"`. This is what the browser's scenario C asserts
//! cross-group isolation against.
//!
//! The orchestrator drives the browser and asserts on the resulting cross-peer
//! state.
//!
//! Environment:
//!
//! | Var | Required | Description |
//! |---|---|---|
//! | `RELAY_ADDR`   | yes | libp2p multiaddr of the relay (QUIC listener) |
//! | `TOPIC`        | yes | default-group user topic (must match the browser) |
//! | `PASSPHRASE`   | yes | default-group shared passphrase (HMAC + topic) |
//! | `DB_PATH`      | yes | filesystem path for the default-group SQLite database |
//! | `GROUP2_TOPIC` | no  | second-group user topic; joins the group when set |
//! | `GROUP2_PASS`  | no  | second-group passphrase; joins the group when set |
//!
//! Prints `READY` on stdout (flushed) once the engine (and the second group,
//! when configured) is up, so the orchestrator can gate on it.

use std::time::Duration;

use anyhow::{Context, Result};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use tokio::sync::broadcast::error::RecvError;
use wavesyncdb::{ChangeSource, WaveSyncDb, WaveSyncDbBuilder, WriteKind};

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

// Second-group entity. Scope `groups("e2e")` means it is only auto-registered
// into a group joined with `kind = "e2e"` (never the default group) — the
// same registration-time policy the browser's `scoped_web_config` mirrors.
mod e2e_group_items {
    use sea_orm::entity::prelude::*;
    use wavesyncdb_derive::SyncEntity;

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
    #[sea_orm(table_name = "e2e_group_items")]
    #[wavesync(scope = groups("e2e"))]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub body: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/// Echo loop for the DEFAULT group's `e2e_items` table: for every remote
/// insert/update whose pk is neither an echo (`native-saw-`) nor the native
/// seed (`from-native`), upsert `id = "native-saw-<pk>"`. Idempotent
/// find-then-insert, so it never echoes an echo (no loop).
async fn run_default_echo(db: WaveSyncDb) -> Result<()> {
    let mut changes = db.change_rx();
    loop {
        match changes.recv().await {
            Ok(n) => {
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

/// Echo loop for the SECOND group's `e2e_group_items` table. Same anti-loop
/// guards as the default group, with the group-scoped `g2-` naming: skips
/// `g2-native-saw-` (its own echoes) and `g2-from-native` (its own seed),
/// upserts `id = "g2-native-saw-<pk>"` for everything else.
async fn run_group2_echo(db: WaveSyncDb) -> Result<()> {
    let mut changes = db.change_rx();
    loop {
        match changes.recv().await {
            Ok(n) => {
                let is_remote = matches!(n.source, ChangeSource::Remote { .. });
                let is_write = matches!(n.kind, WriteKind::Insert | WriteKind::Update);
                if !is_remote || !is_write || n.table != "e2e_group_items" {
                    continue;
                }
                let pk = n.primary_key.0;
                if pk.starts_with("g2-native-saw-") || pk.starts_with("g2-from-native") {
                    continue;
                }
                let echo_pk = format!("g2-native-saw-{pk}");
                if e2e_group_items::Entity::find_by_id(&echo_pk)
                    .one(&db)
                    .await
                    .context("lookup group2 echo row")?
                    .is_none()
                {
                    e2e_group_items::ActiveModel {
                        id: Set(echo_pk),
                        body: Set(format!("echo of {pk}")),
                    }
                    .insert(&db)
                    .await
                    .context("insert group2 echo row")?;
                }
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => break,
        }
    }
    Ok(())
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

    // Seed the default-group row.
    e2e_items::ActiveModel {
        id: Set("from-native".to_string()),
        body: Set("hello from native".to_string()),
    }
    .insert(&db)
    .await
    .context("seed from-native row")?;

    // Second group (multi-group #93): joined only when both env vars are set.
    // Uses the SAME engine (`db.node()`) — one swarm serving two groups.
    if let (Ok(group2_topic), Ok(group2_pass)) =
        (std::env::var("GROUP2_TOPIC"), std::env::var("GROUP2_PASS"))
    {
        let group_db = db
            .node()
            .join_group(&group2_topic, &group2_pass, Some("e2e"))
            .await
            .context("join second group")?;

        // Auto-register scoped entities into the group by scope: only
        // `e2e_group_items` (scope groups("e2e")) lands here; `e2e_items`
        // (private) does not.
        let prefix = module_path!().split("::").next().unwrap();
        group_db
            .get_schema_registry(prefix)
            .sync()
            .await
            .context("register e2e_group_items into second group")?;

        e2e_group_items::ActiveModel {
            id: Set("g2-from-native".to_string()),
            body: Set("hello from native group2".to_string()),
        }
        .insert(&group_db)
        .await
        .context("seed g2-from-native row")?;

        tokio::spawn(async move {
            if let Err(e) = run_group2_echo(group_db).await {
                eprintln!("group2 echo loop ended with error: {e:#}");
            }
        });
    }

    println!("READY");
    use std::io::Write as _;
    std::io::stdout().flush().ok();

    run_default_echo(db).await
}
