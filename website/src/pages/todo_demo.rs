//! Self-contained two-device sync demo, WhatsApp-Web-style.
//!
//! Each device runs an independent [`WebSyncClient`] with its own
//! IndexedDB store. The two clients are wired together via
//! [`LoopbackPair`] — crossed in-process channels that swap
//! `SyncRequest::Push` envelopes directly, so no relay or network is
//! required.
//!
//! What's deliberately *not* in this file:
//! - manual `subscribe_resolved` plumbing
//! - a parallel `HashMap<String, Task>` of in-memory app state
//! - optimistic-merge logic in the add/toggle/delete handlers
//!
//! All of that is now handled by [`use_synced_table`] in wavesyncdb.
//! IndexedDB is the single source of truth; the component is a thin
//! renderer over `Signal<Vec<Task>>`.

use std::collections::HashMap;

use dioxus::prelude::*;
use wavesyncdb::{
    BrowserEntity, LoopbackLink, LoopbackPair, WebSyncClient, dioxus::use_synced_table, serde_json,
};

const TOPIC: &str = "wavesync-local-demo";
const STORE_LAPTOP: &str = "local-demo-laptop";
const STORE_PHONE: &str = "local-demo-phone";
const TASK_TABLE: &str = "tasks";

#[derive(Clone, Debug, Default)]
struct Task {
    id: String,
    title: String,
    done: bool,
    deleted: bool,
    created_at: u64,
}

impl BrowserEntity for Task {
    fn from_columns(pk: &str, cols: &HashMap<String, serde_json::Value>) -> Self {
        Task {
            id: pk.to_string(),
            title: cols
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("(untitled)")
                .to_string(),
            done: cols.get("done").and_then(|v| v.as_bool()).unwrap_or(false),
            deleted: cols
                .get("deleted")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            created_at: cols.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0),
        }
    }

    fn to_columns(&self) -> Vec<(String, serde_json::Value)> {
        vec![
            (
                "title".to_string(),
                serde_json::Value::String(self.title.clone()),
            ),
            ("done".to_string(), serde_json::Value::Bool(self.done)),
            ("deleted".to_string(), serde_json::Value::Bool(self.deleted)),
            (
                "created_at".to_string(),
                serde_json::Value::Number(serde_json::Number::from(self.created_at)),
            ),
        ]
    }

    fn pk(&self) -> String {
        self.id.clone()
    }
}

#[component]
pub fn TodoDemo() -> Element {
    // Two clients, one per device, wired through a LoopbackPair.
    // Everything else lives in the children via `use_synced_table`.
    let mut laptop = use_signal(|| None::<WebSyncClient>);
    let mut phone = use_signal(|| None::<WebSyncClient>);
    let mut laptop_link = use_signal(|| None::<LoopbackLink>);
    let mut phone_link = use_signal(|| None::<LoopbackLink>);
    let laptop_online = use_signal(|| true);
    let phone_online = use_signal(|| true);
    let mut error_msg = use_signal(|| None::<String>);

    use_hook(move || {
        spawn(async move {
            let pair = LoopbackPair::new();
            laptop_link.set(Some(pair.a.link()));
            phone_link.set(Some(pair.b.link()));

            match WebSyncClient::connect_loopback(pair.a, TOPIC, None, STORE_LAPTOP).await {
                Ok(c) => laptop.set(Some(c)),
                Err(e) => {
                    error_msg.set(Some(format!("laptop init failed: {e}")));
                    return;
                }
            }
            match WebSyncClient::connect_loopback(pair.b, TOPIC, None, STORE_PHONE).await {
                Ok(c) => phone.set(Some(c)),
                Err(e) => {
                    error_msg.set(Some(format!("phone init failed: {e}")));
                }
            }
        });
    });

    rsx! {
        section { class: "page-header",
            div { class: "section-inner",
                h1 { class: "page-title", "Live demo · Two devices, one page" }
                p { class: "page-subtitle",
                    "Two independent WaveSyncDB peers running side-by-side in this tab. "
                    "Each one has its own identity, its own IndexedDB, and its own copy of "
                    "the task list. They're wired together by an in-process channel — "
                    "no relay, no network — so you can see the engine doing real work "
                    "without any setup."
                }
                p { class: "page-subtitle",
                    "Take a device offline to break the link, edit on either side, then "
                    "bring it back online. The local device replays its buffered edits "
                    "and asks the peer for everything it missed via a version-vector "
                    "catch-up. Same protocol the native engine uses over the network."
                }
                if let Some(err) = error_msg() {
                    p { class: "demo-error", "{err}" }
                }
            }
        }
        section { class: "examples-grid-section",
            div { class: "section-inner",
                div { class: "local-demo-grid",
                    DevicePhone {
                        client: phone,
                        link: phone_link,
                        online: phone_online,
                    }
                    DeviceLaptop {
                        client: laptop,
                        link: laptop_link,
                        online: laptop_online,
                    }
                }
            }
        }
    }
}

#[component]
fn DeviceLaptop(
    client: Signal<Option<WebSyncClient>>,
    link: Signal<Option<LoopbackLink>>,
    online: Signal<bool>,
) -> Element {
    rsx! {
        div { class: "device-laptop",
            div { class: "device-label", "Laptop · " code { "{STORE_LAPTOP}" } }
            div {
                class: if online() { "device-laptop-screen" } else { "device-laptop-screen offline" },
                DeviceBody {
                    client,
                    link,
                    online,
                    store_name: STORE_LAPTOP.to_string(),
                }
            }
            div { class: "device-laptop-stand" }
            div { class: "device-laptop-base" }
        }
    }
}

#[component]
fn DevicePhone(
    client: Signal<Option<WebSyncClient>>,
    link: Signal<Option<LoopbackLink>>,
    online: Signal<bool>,
) -> Element {
    rsx! {
        div { class: "device-phone",
            div { class: "device-label", "Phone · " code { "{STORE_PHONE}" } }
            div {
                class: if online() { "device-phone-screen" } else { "device-phone-screen offline" },
                DeviceBody {
                    client,
                    link,
                    online,
                    store_name: STORE_PHONE.to_string(),
                }
            }
        }
    }
}

#[component]
fn DeviceBody(
    client: Signal<Option<WebSyncClient>>,
    link: Signal<Option<LoopbackLink>>,
    mut online: Signal<bool>,
    store_name: String,
) -> Element {
    // The reactive view: tasks materialize from IndexedDB on first
    // client-ready, then auto-update on every local or remote change
    // via subscribe_resolved. No HashMap, no manual merge.
    let tasks = use_synced_table::<Task>(client, TASK_TABLE);
    let mut new_title = use_signal(String::new);
    let connected = client().is_some();
    let is_online = online();

    let toggle_online = move |_| {
        if let Some(l) = link() {
            let next = !online();
            l.set_online(next);
            online.set(next);
        }
    };

    let do_add = move || {
        let title = new_title().trim().to_string();
        if title.is_empty() {
            return;
        }
        let Some(c) = client() else { return };
        let task = Task {
            id: uuid_v4_string(),
            title,
            done: false,
            deleted: false,
            created_at: now_ms(),
        };
        new_title.set(String::new());
        spawn(async move {
            let _ = c.submit::<Task>(TASK_TABLE, &task).await;
        });
    };

    let toggle_done = move |t: Task| {
        let Some(c) = client() else { return };
        let updated = Task { done: !t.done, ..t };
        spawn(async move {
            let _ = c.submit::<Task>(TASK_TABLE, &updated).await;
        });
    };

    let delete_task = move |t: Task| {
        let Some(c) = client() else { return };
        let updated = Task { deleted: true, ..t };
        spawn(async move {
            let _ = c.submit::<Task>(TASK_TABLE, &updated).await;
        });
    };

    let visible: Vec<Task> = {
        let mut v: Vec<Task> = tasks().into_iter().filter(|t| !t.deleted).collect();
        v.sort_by_key(|t| std::cmp::Reverse(t.created_at));
        v
    };

    rsx! {
        div { class: "device-status",
            span {
                class: if connected && is_online {
                    "device-status-dot on"
                } else if connected && !is_online {
                    "device-status-dot offline"
                } else {
                    "device-status-dot"
                }
            }
            span { style: "flex: 1;",
                if !connected {
                    "Starting…"
                } else if is_online {
                    "Online"
                } else {
                    "Offline"
                }
            }
            if connected {
                button {
                    class: "device-toggle",
                    onclick: toggle_online,
                    title: if is_online { "Take this device offline" } else { "Bring this device online" },
                    if is_online { "Go offline" } else { "Go online" }
                }
            }
        }
        ul { class: "device-tasks",
            if visible.is_empty() {
                li { class: "device-empty",
                    "No tasks yet."
                }
            }
            for t in visible.into_iter() {
                {
                    let t_for_toggle = t.clone();
                    let t_for_delete = t.clone();
                    let tog = toggle_done;
                    let del = delete_task;
                    rsx! {
                        li {
                            key: "{t.id}",
                            class: if t.done { "device-task done" } else { "device-task" },
                            input {
                                r#type: "checkbox",
                                checked: t.done,
                                onchange: move |_| tog(t_for_toggle.clone()),
                            }
                            span { class: "device-task-title", "{t.title}" }
                            button {
                                class: "device-task-delete",
                                onclick: move |_| del(t_for_delete.clone()),
                                "title": "Delete",
                                "×"
                            }
                        }
                    }
                }
            }
        }
        div { class: "device-add-row",
            input {
                placeholder: "Add a task…",
                disabled: !connected,
                value: "{new_title}",
                oninput: move |e| new_title.set(e.value()),
                onkeydown: {
                    let mut do_add = do_add;
                    move |e| {
                        if e.key() == Key::Enter {
                            do_add();
                        }
                    }
                },
            }
            button {
                disabled: !connected,
                onclick: {
                    let mut do_add = do_add;
                    move |_| do_add()
                },
                "Add"
            }
        }
        div { class: "device-meta",
            "store=" code { "{store_name}" }
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
