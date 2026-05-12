//! Browser side of the QR-pairing example.
//!
//! Renders a small pairing form (topic + passphrase — no relay; the
//! relay multiaddr is hardcoded per platform, same way an app bakes
//! in a STUN server). Connects the browser via
//! `WebSyncClient::connect_via_relay`, then once it knows its own
//! peer-id (read from the engine's status watch channel) generates a
//! `wavesync://?peer=…&topic=…&pass=…` QR. The phone scans it, joins
//! the same topic via *its* hardcoded relay, and the two peers
//! discover each other through the relay's topic mesh.

use dioxus::prelude::*;
use wavesyncdb::{SyncEntity, WebSyncClient, WebSyncStatus, dioxus::use_synced_table};

use crate::pairing::{PairingParams, build_pairing_url};

const TASK_TABLE: &str = "tasks";
const STORE_NAME: &str = "qr-pairing-web";

/// PeerId of the default relay (the one the README's
/// `--identity-keypair` arg pins). Used to build the WebSocket
/// multiaddr the browser dials.
const DEFAULT_RELAY_PEER_ID: &str = "12D3KooWP6oyorVZmZvTRPHdxsEyE2V8mR1dTiAZJYoNsMNX19KW";

/// Build the relay multiaddr the *browser* dials. The phone uses its
/// own hardcoded TCP/4001 multiaddr and never sees this one — the QR
/// only carries peer-id + topic + pass. `window.location.hostname`
/// is the dev machine's address as the browser sees it
/// (`localhost` when serving locally; LAN IP when a phone loads the
/// page over wifi to inspect the QR).
fn relay_addr() -> String {
    let hostname = web_sys::window()
        .and_then(|w| w.location().hostname().ok())
        .unwrap_or_else(|| "127.0.0.1".to_string());
    format!("/ip4/{hostname}/tcp/4002/ws/p2p/{DEFAULT_RELAY_PEER_ID}")
}

const DEFAULT_TOPIC: &str = "qr-pairing-demo";
const DEFAULT_PASS: &str = "demo-shared-secret";

#[derive(Clone, Debug, Default, SyncEntity)]
struct Task {
    #[sea_orm(primary_key)]
    id: String,
    title: String,
    done: bool,
    created_at: u64,
}

#[component]
pub fn App() -> Element {
    let topic = use_signal(|| DEFAULT_TOPIC.to_string());
    let pass = use_signal(|| DEFAULT_PASS.to_string());
    let mut client = use_signal(|| None::<WebSyncClient>);
    let mut error_msg = use_signal(|| None::<String>);

    // wasmlogger forwards `log::*!` to the browser console. The engine
    // uses the `log` crate; Dioxus's own logger only captures
    // `tracing::*!`, so without this the engine's diagnostic
    // `WebSyncClient: …` lines silently disappear and there's no way
    // to debug a live tab. `init` is a no-op on its second call so
    // running this on every App mount is fine.
    use_hook(|| {
        wasm_logger::init(wasm_logger::Config::new(log::Level::Info));
    });

    // Connect on mount. The browser persists its libp2p keypair in
    // IndexedDB (see `WebSyncClient::connect_via_relay`), so the
    // peer-id stays stable across reloads — and so a QR generated
    // on one load remains valid after a refresh.
    use_hook(move || {
        let topic_v = topic.peek().clone();
        let pass_v = pass.peek().clone();
        let pass_opt = if pass_v.is_empty() {
            None
        } else {
            Some(pass_v)
        };
        let addr = relay_addr();
        spawn(async move {
            match WebSyncClient::connect_via_relay(&addr, &topic_v, pass_opt.as_deref(), STORE_NAME)
                .await
            {
                Ok(c) => client.set(Some(c)),
                Err(e) => error_msg.set(Some(format!("connect_via_relay failed: {e}"))),
            }
        });
    });

    rsx! {
        style { {STYLE} }
        div { class: "app",
            h1 { "WaveSyncDB · QR pairing" }
            p { class: "blurb",
                "This browser is now a peer. Once it's connected to the relay, the "
                "QR below carries this tab's libp2p peer-id plus the topic + "
                "passphrase. Scan it with the phone build of this example — the "
                "phone joins the same topic via its own hardcoded relay and the "
                "two sides discover each other through the relay's topic mesh."
            }

            if let Some(err) = error_msg() {
                div { class: "panel error-panel",
                    p { class: "error", "{err}" }
                }
            }

            if client().is_some() {
                PairingPanel { client: client, topic: topic(), pass: pass() }
                DebugPanel { client: client }
                TaskList { client: client }
            } else {
                div { class: "panel",
                    h2 { "Pair a phone" }
                    p { class: "hint", "Connecting to the relay…" }
                }
            }
        }
    }
}

/// Renders the QR once we know the local peer-id. Subscribes to the
/// engine's status watch channel and rebuilds the QR if the peer-id
/// changes (it shouldn't, post-IndexedDB-keypair, but if a future
/// refactor invalidates that assumption the QR stays correct).
#[component]
fn PairingPanel(client: Signal<Option<WebSyncClient>>, topic: String, pass: String) -> Element {
    let status = use_sync_status(client);
    let s = status();

    let qr_svg = use_memo(move || {
        let s = status();
        if s.local_peer_id.is_empty() {
            return None;
        }
        let pass_opt = if pass.is_empty() {
            None
        } else {
            Some(pass.clone())
        };
        let url = build_pairing_url(&PairingParams {
            peer_id: s.local_peer_id.clone(),
            topic: topic.clone(),
            pass: pass_opt,
        });
        match qrcode::QrCode::new(&url) {
            Ok(code) => Some(
                code.render::<qrcode::render::svg::Color>()
                    .min_dimensions(280, 280)
                    .build(),
            ),
            Err(_) => None,
        }
    });

    rsx! {
        div { class: "panel",
            h2 { "Pair a phone" }
            p { class: "kv", strong { "This peer-id " }
                code { class: "wrap", "{s.local_peer_id}" }
            }
            if let Some(svg) = qr_svg() {
                div { class: "qr-wrap",
                    div { class: "qr", dangerous_inner_html: "{svg}" }
                    p { class: "hint",
                        if s.relay_connected {
                            "Relay online — scan the QR with the phone app."
                        } else {
                            "Waiting for relay…"
                        }
                    }
                }
            } else {
                p { class: "hint", "Generating QR…" }
            }
        }
    }
}

#[component]
fn TaskList(client: Signal<Option<WebSyncClient>>) -> Element {
    let tasks = use_synced_table::<Task>(client, TASK_TABLE);
    let mut new_title = use_signal(String::new);

    let do_add = use_callback(move |_: ()| {
        let title = new_title().trim().to_string();
        if title.is_empty() {
            return;
        }
        let Some(c) = client() else { return };
        let task = Task {
            id: uuid_v4_string(),
            title,
            done: false,
            created_at: now_ms(),
        };
        new_title.set(String::new());
        spawn(async move {
            let _ = c.submit::<Task>(TASK_TABLE, &task).await;
        });
    });

    let toggle = move |t: Task| {
        let Some(c) = client() else { return };
        let updated = Task { done: !t.done, ..t };
        spawn(async move {
            let _ = c.submit::<Task>(TASK_TABLE, &updated).await;
        });
    };

    let mut visible: Vec<Task> = tasks();
    visible.sort_by_key(|t| std::cmp::Reverse(t.created_at));

    rsx! {
        div { class: "panel",
            h2 { "Tasks" }
            div { class: "add-row",
                input {
                    placeholder: "Add a task…",
                    value: "{new_title}",
                    oninput: move |e| new_title.set(e.value()),
                    onkeydown: move |e| {
                        if e.key() == Key::Enter { do_add(()); }
                    },
                }
                button {
                    class: "primary",
                    onclick: move |_| do_add(()),
                    "Add"
                }
            }
            ul { class: "task-list",
                if visible.is_empty() {
                    li { class: "empty", "No tasks yet — add one or wait for the phone." }
                }
                for t in visible.into_iter() {
                    {
                        let toggle_t = t.clone();
                        rsx! {
                            li {
                                key: "{t.id}",
                                class: if t.done { "task done" } else { "task" },
                                input {
                                    r#type: "checkbox",
                                    checked: t.done,
                                    onchange: move |_| toggle(toggle_t.clone()),
                                }
                                span { class: "title", "{t.title}" }
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Reactive Signal<WebSyncStatus> bound to the engine's watch
/// channel. On mount, takes a snapshot from `client.subscribe_status()`
/// and then spawns a future that pumps subsequent updates into the
/// signal. Cheap — `watch::Receiver::changed()` is push-driven, no
/// polling.
fn use_sync_status(client: Signal<Option<WebSyncClient>>) -> Signal<WebSyncStatus> {
    let mut sig = use_signal(WebSyncStatus::default);
    use_hook(move || {
        spawn(async move {
            // Wait for the client to be ready, then subscribe. Once
            // subscribed, the loop only exits when the engine drops
            // (client cleared) — at which point recv()? returns Err.
            let Some(c) = client.read().clone() else {
                return;
            };
            let mut rx = c.subscribe_status();
            sig.set(rx.borrow().clone());
            while rx.changed().await.is_ok() {
                sig.set(rx.borrow().clone());
            }
        });
    });
    sig
}

#[component]
fn DebugPanel(client: Signal<Option<WebSyncClient>>) -> Element {
    let status = use_sync_status(client);
    let s = status();
    let relay_label = if s.relay_connected {
        "connected"
    } else if s.relay_peer_id.is_some() {
        "connecting"
    } else {
        "—"
    };
    let relay_class = if s.relay_connected {
        "value online"
    } else if s.relay_peer_id.is_some() {
        "value pending"
    } else {
        "value offline"
    };

    rsx! {
        div { class: "panel",
            h2 { "Network" }
            p { class: "kv", strong { "Local peer-id " }
                code { class: "wrap", "{s.local_peer_id}" }
            }
            if let Some(rid) = s.relay_peer_id.clone() {
                p { class: "kv", strong { "Relay peer-id " } code { class: "wrap", "{rid}" } }
            }
            p { class: "kv", strong { "Relay " }
                span { class: "{relay_class}", "{relay_label}" }
            }
            p { class: "kv", strong { "Peers " }
                span {
                    class: if s.connected_peer_ids.is_empty() {
                        "value offline"
                    } else {
                        "value online"
                    },
                    "{s.connected_peer_ids.len()} connected"
                }
            }
            if !s.connected_peer_ids.is_empty() {
                ul { class: "peer-list",
                    for id in s.connected_peer_ids.iter() {
                        li { key: "{id}", class: "peer",
                            code { class: "peer-id-only", "{id}" }
                        }
                    }
                }
            }
        }
    }
}

fn uuid_v4_string() -> String {
    let mut bytes = [0u8; 16];
    getrandom::getrandom(&mut bytes).expect("crypto.getRandomValues failed");
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

fn now_ms() -> u64 {
    js_sys::Date::now() as u64
}

const STYLE: &str = r#"
body { background: #0a0d12; color: #e6ecf2; font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif; margin: 0; }
.app { max-width: 760px; margin: 0 auto; padding: 40px 24px; }
.app h1 { font-size: 1.8rem; margin: 0 0 8px; }
.blurb { color: #9aa6b6; line-height: 1.55; margin: 0 0 32px; }
.panel { background: #11151c; border: 1px solid #232a36; border-radius: 12px; padding: 24px; margin-bottom: 24px; }
.panel h2 { margin: 0 0 16px; font-size: 1.1rem; }
.panel label { display: block; color: #9aa6b6; font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 14px; margin-bottom: 4px; }
.panel input[type="text"], .panel input[type="password"] { width: 100%; padding: 9px 12px; background: #0a0d12; border: 1px solid #303949; border-radius: 6px; color: #e6ecf2; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.9rem; box-sizing: border-box; }
.panel input:focus { outline: none; border-color: #6ce0c9; }
button.primary { margin-top: 18px; padding: 10px 18px; background: #6ce0c9; color: #06231f; border: none; border-radius: 6px; font-weight: 600; cursor: pointer; }
button.primary:hover { background: #8ff0d8; }
.error { margin-top: 14px; padding: 10px; background: rgba(247, 200, 124, 0.08); border: 1px solid #f7c87c; color: #f7c87c; border-radius: 6px; font-size: 0.85rem; }
.qr-wrap { margin-top: 18px; display: flex; flex-direction: column; align-items: center; gap: 10px; }
.qr { width: 280px; height: 280px; background: white; padding: 12px; border-radius: 8px; }
.qr svg { width: 100%; height: 100%; display: block; }
.hint { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.82rem; color: #9aa6b6; margin: 0; }
.add-row { display: flex; gap: 8px; }
.add-row input { flex: 1; padding: 9px 12px; background: #0a0d12; border: 1px solid #303949; border-radius: 6px; color: #e6ecf2; font-size: 0.95rem; }
.add-row button { margin-top: 0; }
.task-list { list-style: none; margin: 16px 0 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.task { display: flex; align-items: center; gap: 12px; padding: 10px 12px; background: #0a0d12; border: 1px solid #232a36; border-radius: 6px; }
.task.done .title { text-decoration: line-through; color: #6f7c8e; }
.task input[type="checkbox"] { accent-color: #6ce0c9; width: 17px; height: 17px; cursor: pointer; }
.empty { text-align: center; color: #6f7c8e; font-style: italic; padding: 18px; }
.kv { font-size: 0.85rem; color: #e6ecf2; margin: 4px 0; }
.kv strong { color: #9aa6b6; font-weight: 500; margin-right: 6px; }
.kv code, code.wrap { font-size: 0.8rem; word-break: break-all; }
.value.online { color: #6ce0c9; }
.value.offline { color: #e74c3c; }
.value.pending { color: #f39c12; }
.peer-list { list-style: none; margin: 12px 0 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
.peer { padding: 10px 12px; background: #0a0d12; border: 1px solid #232a36; border-radius: 6px; }
.peer-id-only { font-size: 0.75rem; word-break: break-all; }
"#;
