//! Browser P2P sync demo page.
//!
//! Wires `wavesyncdb::WebSyncClient` into a Dioxus form so a visitor can
//! point the demo at any libp2p WebSocket-listening peer (typically a
//! `wavesync-relay` instance running with `--ws-listen-addr`) and see
//! authenticated CRDT changesets stream in. Doubles as the smoke test
//! that the `web` feature genuinely links and runs end-to-end in the
//! browser.

use dioxus::prelude::*;
use wavesyncdb::{ColumnChange, WebSyncClient, serde_json};

const DEFAULT_RELAY_ADDR: &str = "/dns4/relay.example.com/tcp/443/wss";
const DEFAULT_TOPIC: &str = "wavesync-web-demo";
const DEFAULT_STORE_NAME: &str = "demo";
const DEFAULT_TABLE: &str = "tasks";

#[component]
pub fn SyncDemo() -> Element {
    let mut relay_addr = use_signal(|| DEFAULT_RELAY_ADDR.to_string());
    let mut topic = use_signal(|| DEFAULT_TOPIC.to_string());
    let mut passphrase = use_signal(String::new);
    let mut persist = use_signal(|| true);
    let mut store_name = use_signal(|| DEFAULT_STORE_NAME.to_string());

    let mut status = use_signal(|| "Not connected".to_string());
    let mut received = use_signal(Vec::<String>::new);
    let mut client_handle = use_signal(|| None::<WebSyncClient>);

    let mut write_table = use_signal(|| DEFAULT_TABLE.to_string());
    let mut write_pk = use_signal(|| "task-1".to_string());
    let mut write_col = use_signal(|| "title".to_string());
    let mut write_val = use_signal(|| "Buy milk".to_string());

    let on_connect = move |_| {
        let pp = passphrase();
        let pp_opt = if pp.is_empty() { None } else { Some(pp) };
        let addr = relay_addr();
        let topic_v = topic();
        let store_n = store_name();
        let persistent = persist();

        spawn(async move {
            let result = if persistent {
                WebSyncClient::connect_persistent(&addr, &topic_v, pp_opt.as_deref(), &store_n)
                    .await
            } else {
                WebSyncClient::connect(&addr, &topic_v, pp_opt.as_deref())
            };

            match result {
                Ok(client) => {
                    status.set(format!(
                        "Dialing {addr} ({})",
                        if persistent {
                            "persistent"
                        } else {
                            "ephemeral"
                        }
                    ));
                    let mut rx = client.subscribe_resolved();
                    spawn(async move {
                        while let Ok(change) = rx.recv().await {
                            received.with_mut(|v| v.push(format_change(&change)));
                        }
                    });
                    client_handle.set(Some(client));
                }
                Err(e) => status.set(format!("Connect failed: {e}")),
            }
        });
    };

    let on_submit_write = move |_| {
        if let Some(client) = client_handle() {
            let table = write_table();
            let pk = write_pk();
            let cid = write_col();
            let val = serde_json::Value::String(write_val());
            spawn(async move {
                match client
                    .submit_local_write(&table, &pk, vec![(cid, val)])
                    .await
                {
                    Ok(dv) => status.set(format!("Wrote, new db_version={dv}")),
                    Err(e) => status.set(format!("Write failed: {e}")),
                }
            });
        }
    };

    rsx! {
        section { class: "page-header",
            div { class: "section-inner",
                h1 { class: "page-title", "Browser sync demo" }
                p { class: "page-subtitle",
                    "Connects to a libp2p WebSocket peer (typically a wavesync-relay running with "
                    code { "--ws-listen-addr" }
                    ") and exchanges authenticated SyncChangesets in real time. "
                    "When persistence is enabled, identity, db_version, and CRDT shadow state "
                    "survive page reload via IndexedDB."
                }
            }
        }
        section { class: "examples-grid-section",
            div { class: "section-inner",
                form {
                    style: "display: grid; gap: 0.75rem; max-width: 640px;",
                    onsubmit: move |e| e.prevent_default(),

                    h3 { "Connection" }
                    label { "Peer multiaddr" }
                    input {
                        value: "{relay_addr}",
                        oninput: move |e| relay_addr.set(e.value()),
                    }
                    label { "Topic" }
                    input {
                        value: "{topic}",
                        oninput: move |e| topic.set(e.value()),
                    }
                    label { "Passphrase (optional)" }
                    input {
                        r#type: "password",
                        value: "{passphrase}",
                        oninput: move |e| passphrase.set(e.value()),
                    }
                    label {
                        input {
                            r#type: "checkbox",
                            checked: persist(),
                            oninput: move |e| persist.set(e.value() == "true"),
                        }
                        " Persist identity + shadow tables in IndexedDB"
                    }
                    if persist() {
                        label { "Store name" }
                        input {
                            value: "{store_name}",
                            oninput: move |e| store_name.set(e.value()),
                        }
                    }
                    button {
                        r#type: "button",
                        onclick: on_connect,
                        "Connect"
                    }

                    h3 { "Submit local write" }
                    div { style: "display: grid; gap: 0.5rem; grid-template-columns: 1fr 1fr;",
                        div {
                            label { "Table" }
                            input {
                                value: "{write_table}",
                                oninput: move |e| write_table.set(e.value()),
                            }
                        }
                        div {
                            label { "Primary key" }
                            input {
                                value: "{write_pk}",
                                oninput: move |e| write_pk.set(e.value()),
                            }
                        }
                        div {
                            label { "Column" }
                            input {
                                value: "{write_col}",
                                oninput: move |e| write_col.set(e.value()),
                            }
                        }
                        div {
                            label { "Value" }
                            input {
                                value: "{write_val}",
                                oninput: move |e| write_val.set(e.value()),
                            }
                        }
                    }
                    button {
                        r#type: "button",
                        onclick: on_submit_write,
                        disabled: client_handle().is_none(),
                        "Submit local write (bumps versions, persists, broadcasts)"
                    }

                    p { "Status: " strong { "{status}" } }
                    div {
                        h3 { "Resolved remote changes" }
                        if received().is_empty() {
                            p { em { "(none yet)" } }
                        } else {
                            ul {
                                for line in received().iter() {
                                    li { "{line}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn format_change(c: &ColumnChange) -> String {
    format!(
        "{}.{} pk={} col_v={} val={}",
        c.table.0,
        c.cid.0,
        c.pk.0,
        c.col_version,
        c.val
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|| "<deleted>".into()),
    )
}
