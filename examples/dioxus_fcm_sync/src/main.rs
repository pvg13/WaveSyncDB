//! # WaveSyncDB Push-Sync Example
//!
//! Demonstrates push-based background sync on mobile. When a peer writes
//! data, sleeping devices receive a silent push (FCM on Android, APNs on
//! iOS), wake a short-lived background task, sync with peers, and shut down
//! — like WhatsApp.
//!
//! Works on **Android, iOS, desktop, and any platform** — push is only used
//! for wake-up on mobile; LAN peers sync via mDNS regardless.
//!
//! ## Running (desktop, for testing)
//!
//! ```sh
//! # Two peers on the same LAN:
//! cargo run -p example-dioxus-fcm-sync    # Terminal 1
//! cargo run -p example-dioxus-fcm-sync    # Terminal 2
//! ```
//!
//! ## Running (Android)
//!
//! ```sh
//! cd examples/dioxus_fcm_sync
//! dx serve --platform android
//! ```
//!
//! ## Running (iOS)
//!
//! ```sh
//! cd examples/dioxus_fcm_sync
//! dx serve --platform ios --device   # physical device required for real APNs
//! ```
//!
//! ## Setup
//!
//! 1. Android: place `google-services.json` in your project root (from Firebase Console).
//! 2. iOS: enable Push Notifications + Background Modes ("Remote notifications")
//!    on your app target; ensure your provisioning profile has the
//!    `aps-environment` entitlement (see `entitlements.plist`).
//! 3. Add `wavesyncdb` with `push-sync` feature — the Kotlin AAR and Swift
//!    Package are bundled automatically via manganis.
//! 4. Set `RELAY_SERVER` to your relay's multiaddr for WAN + push.
//! 5. On Android, in Rust:
//!    `.with_google_services(include_str!("../google-services.json"))`.
//!    iOS needs no equivalent call — APNs integration is zero-config.

mod entity;

use std::sync::OnceLock;
use std::time::Duration;

use entity::task;
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::dioxus::{
    use_auto_lifecycle, use_network_status, use_synced_table, use_wavesync,
    use_wavesync_provider,
};
use wavesyncdb::{WaveSyncDb, WaveSyncDbBuilder};

use dioxus::prelude::*;

const STYLE: &str = r#"
    body { font-family: sans-serif; max-width: 600px; margin: 40px auto; padding: 0 20px; }
    h1 { color: #333; }
    .status-bar { background: #f5f5f5; border-radius: 6px; padding: 12px; margin-bottom: 20px; font-size: 13px; }
    .status-bar .row { display: flex; justify-content: space-between; margin: 4px 0; }
    .status-bar .label { color: #666; }
    .status-bar .value { font-weight: 600; }
    .status-bar .online { color: #27ae60; }
    .status-bar .offline { color: #e74c3c; }
    .status-bar .pending { color: #f39c12; }
    .add-form { display: flex; gap: 8px; margin-bottom: 20px; }
    .add-form input { flex: 1; padding: 8px; font-size: 14px; border: 1px solid #ccc; border-radius: 4px; }
    .add-form button { padding: 8px 16px; background: #4a90d9; color: white; border: none; border-radius: 4px; cursor: pointer; }
    .task-list { list-style: none; padding: 0; }
    .task-item { display: flex; align-items: center; gap: 8px; padding: 8px 0; border-bottom: 1px solid #eee; }
    .task-item label { flex: 1; cursor: pointer; }
    .task-item label.completed { text-decoration: line-through; color: #999; }
    .task-item button { background: #e74c3c; color: white; border: none; border-radius: 4px; padding: 4px 10px; cursor: pointer; }
    .empty { color: #999; font-style: italic; }
    .bg-sync-btn { margin-top: 12px; padding: 8px 16px; background: #8e44ad; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; }
"#;

/// Relay server address. Set this to your relay's multiaddr for WAN sync.
/// Example: "/ip4/your-server-ip/tcp/4001/p2p/12D3KooW..."
/// Leave as None for LAN-only mDNS sync (desktop testing).
const RELAY_SERVER: Option<&str> =
    Some("/dns4/relay.roommatesapp.es/tcp/4001/p2p/12D3KooWSH8G4zDwzK2srm8u6DWxvFiY6emuJkKSKswvGiG2i8qh");

static DB: OnceLock<WaveSyncDb> = OnceLock::new();

fn main() {
    // Set up logging with noisy libp2p modules filtered out
    let mut log_builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    for (module, level) in wavesyncdb::recommended_log_filters() {
        log_builder.filter_module(module, level);
    }
    log_builder.init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
    let _guard = rt.enter();

    let db = rt.block_on(async {
        // Build the DB with all mobile features configured

        let data_directory = dioxus_sdk_storage::data_directory().join("mobile_tasks.db");

        // Ensure the parent directory exists (iOS sandbox may not pre-create it)
        if let Some(parent) = data_directory.parent() {
            std::fs::create_dir_all(parent).expect("Failed to create data directory");
        }

        let mut builder = WaveSyncDbBuilder::new(
            &format!("sqlite:{}?mode=rwc", data_directory.display()),
            "mobile-tasks-demo",
        )
        .with_passphrase("demo-shared-secret");

        // Configure relay for NAT traversal (WAN sync)
        if let Some(relay) = RELAY_SERVER {
            builder = builder.with_relay_server(relay);
        }

        // Configure FCM for background sync push notifications (Android only).
        // Set GOOGLE_SERVICES_JSON to the path of your google-services.json before building.
        // On iOS, push is handled via APNs — no google-services.json needed.
        #[cfg(target_os = "android")]
        {
            builder = builder.with_google_services(include_str!(env!("GOOGLE_SERVICES_JSON")));
        }

        let db = builder.build().await.expect("Failed to create WaveSyncDb");

        // Register entities — this also persists the crate name in the config
        // so that background_sync() can reconstruct the registry
        db.get_schema_registry(module_path!().split("::").next().unwrap())
            .sync()
            .await
            .expect("Schema sync failed");

        db
    });

    DB.set(db).expect("DB already initialized");
    dioxus::launch(App);
}

#[allow(non_snake_case)]
fn App() -> Element {
    let db = DB.get().expect("DB not initialized");
    use_wavesync_provider(db.clone());

    // Auto lifecycle: on mobile, this detects app resume and triggers sync.
    // On desktop, this is a no-op — peers sync via mDNS anyway.
    use_auto_lifecycle(db.clone());

    rsx! {
        style { {STYLE} }
        h1 { "WaveSyncDB Mobile Demo" }
        NetworkStatusBar {}
        AddTaskForm {}
        TaskList {}
        BackgroundSyncDemo {}
    }
}

/// Shows live network status: relay connection, NAT type, connected peers, push registration.
#[component]
fn NetworkStatusBar() -> Element {
    let db = use_wavesync();
    let status = use_network_status(db);

    let relay_class = match status().relay_status {
        wavesyncdb::RelayStatus::Listening => "online",
        wavesyncdb::RelayStatus::Connected => "pending",
        wavesyncdb::RelayStatus::Connecting => "pending",
        wavesyncdb::RelayStatus::Disabled => "offline",
    };

    let peer_count = status().connected_peers.len();

    rsx! {
        div { class: "status-bar",
            div { class: "row",
                span { class: "label", "Relay" }
                span { class: "value {relay_class}", "{status().relay_status:?}" }
            }
            div { class: "row",
                span { class: "label", "NAT" }
                span { class: "value", "{status().nat_status:?}" }
            }
            div { class: "row",
                span { class: "label", "Peers" }
                span { class: if peer_count > 0 { "value online" } else { "value offline" },
                    "{peer_count} connected"
                }
            }
            div { class: "row",
                span { class: "label", "Push registered" }
                span { class: if status().push_registered { "value online" } else { "value offline" },
                    if status().push_registered {
                        "Yes"
                    } else {
                        "No"
                    }
                }
            }
            div { class: "row",
                span { class: "label", "DB version" }
                span { class: "value", "{status().local_db_version}" }
            }
        }
    }
}

/// Demonstrates calling background_sync() directly — simulates what the native
/// push service does when the app is killed and an FCM arrives.
#[component]
fn BackgroundSyncDemo() -> Element {
    let mut result_text = use_signal(String::new);
    let mut running = use_signal(|| false);

    let run_sync = move |_| {
        running.set(true);
        result_text.set("Running background sync...".into());
        spawn(async move {
            let db_path = dioxus_sdk_storage::data_directory().join("mobile_tasks.db");
            let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
            let outcome =
                wavesyncdb::background_sync::background_sync(&db_url, Duration::from_secs(15))
                    .await;

            let msg = match outcome {
                Ok(wavesyncdb::background_sync::BackgroundSyncResult::Synced { peers_synced }) => {
                    format!("Synced with {peers_synced} peer(s)")
                }
                Ok(wavesyncdb::background_sync::BackgroundSyncResult::NoPeers) => {
                    "No peers found".into()
                }
                Ok(wavesyncdb::background_sync::BackgroundSyncResult::TimedOut {
                    peers_synced,
                }) => format!("Timed out ({peers_synced} peer(s) synced)"),
                Err(e) => format!("Error: {e}"),
            };

            result_text.set(msg);
            running.set(false);
        });
    };

    rsx! {
        div { style: "margin-top: 24px; padding: 12px; background: #faf3ff; border-radius: 6px;",
            p { style: "font-size: 13px; color: #666; margin: 0 0 8px;",
                "Simulate what happens when a push notification wakes the app:"
            }
            button {
                class: "bg-sync-btn",
                disabled: running(),
                onclick: run_sync,
                if running() {
                    "Syncing..."
                } else {
                    "Run Background Sync"
                }
            }
            if !result_text().is_empty() {
                p { style: "margin-top: 8px; font-size: 13px;", "{result_text}" }
            }
        }
    }
}

#[component]
fn AddTaskForm() -> Element {
    let db = use_wavesync();
    let mut input = use_signal(String::new);

    let on_submit = move |evt: FormEvent| {
        evt.prevent_default();
        let title = input.read().trim().to_string();
        if title.is_empty() {
            return;
        }
        input.set(String::new());
        let db = db.clone();
        spawn(async move {
            let new_task = task::ActiveModel {
                id: Set(Uuid::new_v4().to_string()),
                title: Set(title),
                completed: Set(false),
                ..Default::default()
            };
            if let Err(e) = new_task.insert(&db).await {
                log::error!("Failed to insert task: {}", e);
            }
        });
    };

    rsx! {
        form { class: "add-form", onsubmit: on_submit,
            input {
                r#type: "text",
                placeholder: "What needs to be done?",
                value: "{input}",
                oninput: move |evt| input.set(evt.value()),
            }
            button { r#type: "submit", "Add" }
        }
    }
}

#[component]
fn TaskList() -> Element {
    let db = use_wavesync();
    let tasks = use_synced_table::<task::Entity>(db);

    rsx! {
        ul { class: "task-list",
            if tasks.read().is_empty() {
                li { class: "empty", "No tasks yet. Add one above!" }
            }
            for t in tasks.read().iter() {
                TaskItem { key: "{t.id}", task: t.clone() }
            }
        }
    }
}

#[component]
fn TaskItem(task: task::Model) -> Element {
    let db = use_wavesync();
    let id = task.id.clone();
    let completed = task.completed;

    let toggle_id = id.clone();
    let db2 = db.clone();
    let toggle = move |_| {
        let id = toggle_id.clone();
        let db = db2.clone();
        spawn(async move {
            let active = task::ActiveModel {
                id: Set(id),
                completed: Set(!completed),
                ..Default::default()
            };
            if let Err(e) = active.update(&db).await {
                log::error!("Failed to toggle task: {}", e);
            }
        });
    };

    let delete_id = id.clone();
    let delete = move |_| {
        let id = delete_id.clone();
        let db = db.clone();
        spawn(async move {
            if let Err(e) = task::Entity::delete_by_id(id).exec(&db).await {
                log::error!("Failed to delete task: {}", e);
            }
        });
    };

    rsx! {
        li { class: "task-item",
            input {
                r#type: "checkbox",
                checked: task.completed,
                onchange: toggle,
            }
            label { class: if task.completed { "completed" } else { "" }, "{task.title}" }
            button { onclick: delete, "Delete" }
        }
    }
}
