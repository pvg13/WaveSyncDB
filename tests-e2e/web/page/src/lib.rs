//! Minimal browser harness for the puppeteer e2e suite. Exposes a tiny
//! window-level API; all assertions live in the JS driver.
//!
//! The exported functions map onto the real [`WebSyncClient`] surface:
//! `e2e_init` -> `connect_via_relay_with_config`, `e2e_submit` ->
//! `submit_local_write`, `e2e_rows` -> the persistent store's
//! `list_table_rows`, and `e2e_status` -> a JSON snapshot of the latest
//! `WebSyncStatus` watch value (including `relay_reconnect_attempts`,
//! which the relay-restart scenario depends on).

use std::cell::RefCell;

use wasm_bindgen::prelude::*;
use wavesyncdb::web_sync_core::{WebSyncConfig, WebTableConfig};
use wavesyncdb::{DeletePolicy, WebSyncClient};

// `WebSyncClient` is `Clone` (a pair of channel senders behind an `Arc`),
// so a single stored handle is enough — we clone it out before every
// `.await` so the `RefCell` is never held across a suspension point.
thread_local! {
    static CLIENT: RefCell<Option<WebSyncClient>> = const { RefCell::new(None) };
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

/// Return the materialized rows of `table` as a JSON array of
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

/// JSON snapshot of the current engine status. Shape:
/// `{ "initialized": bool, "localReady": bool, "relayConnected": bool,
///    "connectedPeers": number, "reconnectAttempts": number,
///    "localPeerId": string, "relayPeerId": string|null }`.
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
            })
            .to_string()
        }
    }
}
