//! WAN sync example — two peers syncing through a relay/rendezvous server.
//!
//! # Setup
//!
//! 1. Start the relay server (persistent identity so the PeerId is stable):
//!
//!    ```sh
//!    cargo run -p wavesync_relay -- \
//!        --identity-file relay-key.bin \
//!        --listen-addr /ip4/0.0.0.0/tcp/4001
//!    ```
//!
//!    The server prints its PeerId on startup, e.g.:
//!    `Relay server PeerId: 12D3KooWAbCdEf...`
//!
//! 2. Run Peer A (on any machine that can reach the relay):
//!
//!    ```sh
//!    cargo run -p example-wan -- \
//!        --relay /ip4/<RELAY_IP>/tcp/4001/p2p/<RELAY_PEER_ID> \
//!        --passphrase "my-secret" \
//!        --db ./peer_a.db
//!    ```
//!
//! 3. Run Peer B (on a different machine or terminal):
//!
//!    ```sh
//!    cargo run -p example-wan -- \
//!        --relay /ip4/<RELAY_IP>/tcp/4001/p2p/<RELAY_PEER_ID> \
//!        --passphrase "my-secret" \
//!        --db ./peer_b.db
//!    ```
//!
//! Both peers share the same passphrase and relay address. The relay handles:
//! - **Rendezvous** — peers register and discover each other by namespace
//! - **Relay circuits** — NAT traversal so peers behind different NATs can connect
//!
//! Once connected, create a task on Peer A and it will appear on Peer B.

mod entity;

use clap::Parser;
use entity::task;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use std::time::Duration;
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

#[derive(Parser)]
#[command(name = "wan-example", about = "WaveSyncDB WAN sync example")]
struct Cli {
    /// Relay server multiaddr (e.g., /ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...)
    #[arg(long)]
    relay: String,

    /// Shared passphrase for peer group authentication
    #[arg(long, default_value = "demo-secret")]
    passphrase: String,

    /// SQLite database path
    #[arg(long, default_value = "sqlite:./wan_peer.db?mode=rwc")]
    db: String,

    /// Sync interval in seconds
    #[arg(long, default_value_t = 10)]
    sync_interval: u64,

    /// Rendezvous discovery interval in seconds
    #[arg(long, default_value_t = 15)]
    discover_interval: u64,

    /// Push notification token (format: "Fcm:`<token>`" or "Apns:`<token>`")
    #[arg(long)]
    push_token: Option<String>,
}

fn menu() {
    println!();
    println!("=== WAN Sync Demo ===");
    println!("1. Create Task");
    println!("2. Update Task");
    println!("3. Delete Task");
    println!("4. List Tasks");
    println!("5. Exit");
    println!("=====================");
    print!("> ");
    use std::io::Write;
    std::io::stdout().flush().ok();
}

#[tokio::main]
pub async fn main() {
    let mut log_builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    for (module, level) in wavesyncdb::recommended_log_filters() {
        log_builder.filter_module(module, level);
    }
    log_builder.init();

    let cli = Cli::parse();

    // The relay address is used for both relay circuits and rendezvous discovery.
    // In a production deployment these could be separate servers, but the
    // wavesync-relay binary serves both roles.
    let relay_addr = &cli.relay;

    let mut builder = WaveSyncDbBuilder::new(&cli.db, "wan-tasks")
        .with_passphrase(&cli.passphrase)
        .with_relay_server(relay_addr)
        .with_rendezvous_server(relay_addr)
        .with_sync_interval(Duration::from_secs(cli.sync_interval))
        .with_rendezvous_discover_interval(Duration::from_secs(cli.discover_interval));

    // Register push token if provided (for mobile wake-up via relay)
    if let Some(ref token_str) = cli.push_token {
        if let Some((platform, token)) = token_str.split_once(':') {
            builder = builder.with_push_token(platform, token);
            log::info!("Push notifications enabled (platform: {platform})");
        } else {
            eprintln!("Invalid --push-token format. Expected 'Fcm:<token>' or 'Apns:<token>'");
            std::process::exit(1);
        }
    }

    let db = builder.build().await.expect("Failed to create WaveSyncDb");

    // Auto-discover entities annotated with #[derive(SyncEntity)]
    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await
        .expect("Failed to sync schema");

    println!("Connected! Relay: {relay_addr}");
    println!("Passphrase group: {}", cli.passphrase);

    loop {
        menu();
        let mut choice = String::new();
        std::io::stdin()
            .read_line(&mut choice)
            .expect("Failed to read input");
        let choice = choice.trim().parse::<u32>().unwrap_or(0);

        match choice {
            1 => {
                println!("Enter task title:");
                let mut title = String::new();
                std::io::stdin()
                    .read_line(&mut title)
                    .expect("Failed to read input");
                let title = title.trim().to_string();

                let new_task = task::ActiveModel {
                    id: Set(Uuid::new_v4().to_string()),
                    title: Set(title),
                    completed: Set(false),
                    ..Default::default()
                };
                match new_task.insert(&db).await {
                    Ok(t) => println!("Created task: {} (id={})", t.title, t.id),
                    Err(e) => println!("Error creating task: {e}"),
                }
            }
            2 => {
                println!("Enter task ID to update:");
                let mut id = String::new();
                std::io::stdin()
                    .read_line(&mut id)
                    .expect("Failed to read input");
                let id = id.trim().to_string();

                match task::Entity::find_by_id(&id).one(&db).await {
                    Ok(Some(t)) => {
                        println!("Enter new title (current: {}):", t.title);
                        let mut title = String::new();
                        std::io::stdin()
                            .read_line(&mut title)
                            .expect("Failed to read input");

                        let mut active: task::ActiveModel = t.into();
                        active.title = Set(title.trim().to_string());
                        match active.update(&db).await {
                            Ok(t) => println!("Updated task: {}", t.title),
                            Err(e) => println!("Error updating task: {e}"),
                        }
                    }
                    Ok(None) => println!("Task not found"),
                    Err(e) => println!("Error: {e}"),
                }
            }
            3 => {
                println!("Enter task ID to delete:");
                let mut id = String::new();
                std::io::stdin()
                    .read_line(&mut id)
                    .expect("Failed to read input");
                let id = id.trim().to_string();

                match task::Entity::delete_by_id(&id).exec(&db).await {
                    Ok(res) => println!("Deleted {} task(s)", res.rows_affected),
                    Err(e) => println!("Error deleting task: {e}"),
                }
            }
            4 => match task::Entity::find().all(&db).await {
                Ok(tasks) => {
                    if tasks.is_empty() {
                        println!("No tasks found.");
                    } else {
                        for t in tasks {
                            println!("  [{}] {} (completed: {})", t.id, t.title, t.completed);
                        }
                    }
                }
                Err(e) => println!("Error listing tasks: {e}"),
            },
            5 => {
                println!("Shutting down...");
                db.shutdown().await;
                break;
            }
            _ => println!("Invalid choice"),
        }
    }
}
