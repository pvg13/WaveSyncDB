//! Minimal browser harness for the puppeteer e2e suite. Exposes a tiny
//! window-level API; all assertions live in the JS driver.
//!
//! The exported functions map onto the real [`WebSyncClient`] surface:
//! `e2e_init` -> `connect_via_relay_with_config`, `e2e_submit` ->
//! `submit_local_write`, `e2e_rows` -> the persistent store's
//! `list_table_rows`, and `e2e_status` -> a JSON snapshot of the latest
//! `WebSyncStatus` watch value (including `relay_reconnect_attempts`,
//! which the relay-restart scenario depends on).
//!
//! Multi-group (#93) additions mirror the same surface against a
//! [`WebGroupHandle`]: `e2e_join_group` / `e2e_leave_group` manage a
//! non-default group, `e2e_group_rows` / `e2e_group_submit` read and write it,
//! and `e2e_force_resync` triggers the client's manual resync.

use std::cell::RefCell;
use std::collections::HashMap;

use wasm_bindgen::prelude::*;
use wavesyncdb::web_sync_core::{WebSyncConfig, WebTableConfig};
use wavesyncdb::{DeletePolicy, WebGroupHandle, WebSyncClient};

// `WebSyncClient` is `Clone` (a pair of channel senders behind an `Arc`),
// so a single stored handle is enough — we clone it out before every
// `.await` so the `RefCell` is never held across a suspension point.
thread_local! {
    static CLIENT: RefCell<Option<WebSyncClient>> = const { RefCell::new(None) };
    // Non-default group handles keyed by user topic. Populated by
    // `e2e_join_group`, and lazily re-populated after a reload from the
    // client's auto-rejoin registry (see `resolve_group`).
    static GROUPS: RefCell<HashMap<String, WebGroupHandle>> = RefCell::new(HashMap::new());
}

// Serialize a list of `{ pk, columns }` rows to the JSON string the driver
// parses. Shared by `e2e_rows` and `e2e_group_rows`.
fn rows_to_json(rows: Vec<wavesyncdb::web_store::ResolvedRow>) -> Result<String, JsValue> {
    let arr: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            let cols: serde_json::Map<String, serde_json::Value> = r.columns.into_iter().collect();
            serde_json::json!({ "pk": r.pk, "columns": cols })
        })
        .collect();
    serde_json::to_string(&serde_json::Value::Array(arr))
        .map_err(|e| JsValue::from_str(&format!("{e}")))
}

// Resolve a non-default group handle by user topic. Checks the local `GROUPS`
// map first, then falls back to the client's registry (which includes groups
// auto-rejoined from a previous session on construction — the path scenario D
// exercises after a page reload), caching the handle for subsequent calls.
fn resolve_group(user_topic: &str) -> Option<WebGroupHandle> {
    if let Some(h) = GROUPS.with(|g| g.borrow().get(user_topic).cloned()) {
        return Some(h);
    }
    let client = CLIENT.with(|c| c.borrow().clone())?;
    let handle = client.group(user_topic)?;
    GROUPS.with(|g| {
        g.borrow_mut()
            .insert(user_topic.to_string(), handle.clone())
    });
    Some(handle)
}

/// Connect to the relay and register the `e2e_items` table.
///
/// Parameter order mirrors `WebSyncClient::connect_via_relay_with_config`
/// (`relay_addr`, `user_topic`, `passphrase`, `store_name`, `config`).
#[wasm_bindgen]
pub async fn e2e_init(
    relay_addr: String,
    topic: String,
    passphrase: String,
    store_name: String,
) -> Result<(), JsValue> {
    let config = WebSyncConfig::default().with_table(
        "e2e_items",
        WebTableConfig {
            delete_policy: DeletePolicy::default(),
            primary_key_column: Some("id".into()),
        },
    );
    let client = WebSyncClient::connect_via_relay_with_config(
        &relay_addr,
        &topic,
        Some(&passphrase),
        &store_name,
        config,
    )
    .await
    .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    CLIENT.with(|c| *c.borrow_mut() = Some(client));
    Ok(())
}

/// Perform a local write. `columns_json` is a JSON array of
/// `[name, value]` pairs, e.g. `[["body", "hello"]]`.
#[wasm_bindgen]
pub async fn e2e_submit(table: String, pk: String, columns_json: String) -> Result<(), JsValue> {
    let columns: Vec<(String, serde_json::Value)> =
        serde_json::from_str(&columns_json).map_err(|e| JsValue::from_str(&format!("{e}")))?;
    let client = CLIENT
        .with(|c| c.borrow().clone())
        .ok_or_else(|| JsValue::from_str("not initialized"))?;
    client
        .submit_local_write(&table, &pk, columns)
        .await
        .map(|_db_version| ())
        .map_err(|e| JsValue::from_str(&format!("{e}")))
}

/// Return the materialized rows of `table` (default group) as a JSON array of
/// `{ "pk": ..., "columns": { ... } }` objects.
#[wasm_bindgen]
pub async fn e2e_rows(table: String) -> Result<String, JsValue> {
    let client = CLIENT
        .with(|c| c.borrow().clone())
        .ok_or_else(|| JsValue::from_str("not initialized"))?;
    let store = client
        .store()
        .ok_or_else(|| JsValue::from_str("client has no persistent store"))?;
    let rows = store
        .list_table_rows(&table)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    rows_to_json(rows)
}

/// Join a non-default group (#93). `kind` is the scope label ("" → `None`).
/// Runs Argon2id — seconds on wasm; the driver awaits it with a generous
/// timeout. Idempotent per topic (a repeat join fast-paths the KDF).
#[wasm_bindgen]
pub async fn e2e_join_group(
    user_topic: String,
    passphrase: String,
    kind: String,
) -> Result<(), JsValue> {
    let client = CLIENT
        .with(|c| c.borrow().clone())
        .ok_or_else(|| JsValue::from_str("not initialized"))?;
    let kind_opt = if kind.is_empty() {
        None
    } else {
        Some(kind.as_str())
    };
    let handle = client
        .join_group(&user_topic, &passphrase, kind_opt)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    GROUPS.with(|g| g.borrow_mut().insert(user_topic, handle));
    Ok(())
}

/// Return the materialized rows of `table` in the joined group `user_topic`.
#[wasm_bindgen]
pub async fn e2e_group_rows(user_topic: String, table: String) -> Result<String, JsValue> {
    let handle = resolve_group(&user_topic).ok_or_else(|| JsValue::from_str("group not joined"))?;
    let store = handle
        .store()
        .ok_or_else(|| JsValue::from_str("group has no persistent store"))?;
    let rows = store
        .list_table_rows(&table)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    rows_to_json(rows)
}

/// Perform a local write into the joined group `user_topic`.
#[wasm_bindgen]
pub async fn e2e_group_submit(
    user_topic: String,
    table: String,
    pk: String,
    columns_json: String,
) -> Result<(), JsValue> {
    let columns: Vec<(String, serde_json::Value)> =
        serde_json::from_str(&columns_json).map_err(|e| JsValue::from_str(&format!("{e}")))?;
    let handle = resolve_group(&user_topic).ok_or_else(|| JsValue::from_str("group not joined"))?;
    handle
        .submit_local_write(&table, &pk, columns)
        .await
        .map(|_db_version| ())
        .map_err(|e| JsValue::from_str(&format!("{e}")))
}

/// Leave a joined group and forget its handle.
#[wasm_bindgen]
pub async fn e2e_leave_group(user_topic: String) -> Result<(), JsValue> {
    let client = CLIENT
        .with(|c| c.borrow().clone())
        .ok_or_else(|| JsValue::from_str("not initialized"))?;
    let handle = resolve_group(&user_topic).ok_or_else(|| JsValue::from_str("group not joined"))?;
    client
        .leave_group(&handle)
        .await
        .map_err(|e| JsValue::from_str(&format!("{e}")))?;
    GROUPS.with(|g| g.borrow_mut().remove(&user_topic));
    Ok(())
}

/// Trigger a manual resync (re-announce + catch-up on every group, and an
/// immediate relay redial if the link is down). Fire-and-forget.
#[wasm_bindgen]
pub fn e2e_force_resync() -> Result<(), JsValue> {
    let client = CLIENT
        .with(|c| c.borrow().clone())
        .ok_or_else(|| JsValue::from_str("not initialized"))?;
    client.force_resync();
    Ok(())
}

/// JSON snapshot of the current engine status. Shape:
/// `{ "initialized": bool, "localReady": bool, "relayConnected": bool,
///    "connectedPeers": number, "reconnectAttempts": number,
///    "localPeerId": string, "relayPeerId": string|null,
///    "joinedTopics": string[] }`.
#[wasm_bindgen]
pub fn e2e_status() -> String {
    let client = CLIENT.with(|c| c.borrow().clone());
    match client {
        None => r#"{"initialized":false}"#.to_string(),
        Some(c) => {
            let s = c.subscribe_status().borrow().clone();
            serde_json::json!({
                "initialized": true,
                "localReady": s.local_ready,
                "relayConnected": s.relay_connected,
                "connectedPeers": s.connected_peer_ids.len(),
                "reconnectAttempts": s.relay_reconnect_attempts,
                "localPeerId": s.local_peer_id,
                "relayPeerId": s.relay_peer_id,
                "joinedTopics": s.joined_topics,
            })
            .to_string()
        }
    }
}
