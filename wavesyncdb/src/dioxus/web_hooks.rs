//! Dioxus reactive hooks for the browser sync engine.
//!
//! Mirrors what the WhatsApp-Web-style local-first apps do: IndexedDB
//! is the single source of truth for application state, and components
//! drive themselves off a reactive stream of changes.
//!
//! **Most apps should use the cross-target
//! [`super::use_synced_table`] instead** — it takes a
//! [`SyncHandle`](super::SyncHandle) and works identically on native
//! and wasm32. The hook below is the web-only escape hatch for code
//! that already holds a `Signal<Option<WebGroupHandle>>` directly.
//!
//! [`use_synced_table_client::<E>(client, table)`] returns a
//! `Signal<Vec<E>>` that:
//!
//! 1. On the first render where `client` is `Some`, materializes the
//!    full table from [`BrowserStore::list_table_rows`] — so reload
//!    starts with whatever is persisted.
//! 2. Subscribes to [`WebGroupHandle::subscribe_resolved`] and folds
//!    each `ColumnChange` into the in-memory `Vec<E>`. The engine
//!    echoes local writes onto the same channel after `submit_local_write`
//!    persists them, so a single subscription drives both local and
//!    remote updates without the component needing optimistic-merge code.
//! 3. Filters by table name — sufficient because a group handle's
//!    `subscribe_resolved` stream is already scoped to that one group
//!    (multi-group #93): a single group can still hold multiple synced
//!    tables, so the filter still guards against cross-table noise, it
//!    just no longer also has to guard against cross-group noise.
//!
//! The companion [`WebGroupHandle::submit`](crate::WebGroupHandle::submit)
//! is the writer: it takes a `&E`, serializes via
//! [`BrowserEntity::to_columns`], and goes through `submit_local_write`.
//! Components call `group.submit(table, &task).await` and get
//! reactivity for free.

use std::collections::HashMap;

use dioxus::prelude::*;

use crate::web_engine::WebGroupHandle;
use crate::web_entity::BrowserEntity;

/// Reactive `Vec<E>` materialized from a synced table.
///
/// `client` is a `Signal<Option<WebGroupHandle>>` rather than a
/// `WebGroupHandle` directly because the typical setup is to construct
/// the client/group async and store it in a parent signal — `None`
/// until the initial connect (or join) resolves. The hook waits
/// internally for the first `Some`, then materializes from
/// [`BrowserStore::list_table_rows`] and stays subscribed to changes
/// for the rest of the component's lifetime.
///
/// `table` is captured by value (`String`) so the hook can use it from
/// the spawned subscription task without lifetime gymnastics.
pub fn use_synced_table_client<E: BrowserEntity>(
    client: Signal<Option<WebGroupHandle>>,
    table: &'static str,
) -> Signal<Vec<E>> {
    let entities = use_signal(Vec::<E>::new);

    use_effect({
        let table = table.to_string();
        let mut entities = entities;
        move || {
            let Some(c) = client.read().clone() else {
                return;
            };
            let table = table.clone();
            spawn(async move {
                // The in-memory cache is a FIRST-PAINT HINT, never a substitute
                // for re-reading the store: paint it immediately (instant page
                // re-navigation), then always re-materialize below.
                //
                // Only a *live* subscriber writes that cache, so a write that
                // lands while no subscriber for this table is mounted — an
                // editor route that navigates away on save — leaves it holding
                // the pre-write snapshot with nothing to correct it, and every
                // later mount would republish the stale list indefinitely.
                let hint = c.get_table_cache::<Vec<E>>();
                if let Some(hint) = hint.clone() {
                    entities.set(hint);
                }
                let mut rows: Vec<E> = if let Some(store) = c.store() {
                    match store.list_table_rows(&table).await {
                        Ok(stored) => stored
                            .into_iter()
                            .map(|r| E::from_columns(&r.pk, &r.columns))
                            .collect(),
                        Err(e) => {
                            tracing::warn!(
                                "use_synced_table({table}): list_table_rows failed: {e}"
                            );
                            // Keep the hint: a transient read failure degrades
                            // to "possibly stale", never to a blanked list.
                            hint.unwrap_or_default()
                        }
                    }
                } else {
                    // Ephemeral client (no store): the cache is the only
                    // source there is.
                    hint.unwrap_or_default()
                };
                let mut pk_index: HashMap<String, usize> = HashMap::new();
                for (i, e) in rows.iter().enumerate() {
                    pk_index.insert(e.pk().to_string(), i);
                }
                c.set_table_cache(rows.clone());
                entities.set(rows.clone());

                let mut rx = c.subscribe_resolved();
                loop {
                    let first = match rx.recv().await {
                        Ok(change) => change,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!(
                                "use_synced_table({table}): lagged {n}, re-materializing"
                            );
                            if let Some(store) = c.store() {
                                if let Ok(stored) = store.list_table_rows(&table).await {
                                    rows = stored
                                        .into_iter()
                                        .map(|r| E::from_columns(&r.pk, &r.columns))
                                        .collect();
                                    pk_index.clear();
                                    for (i, e) in rows.iter().enumerate() {
                                        pk_index.insert(e.pk().to_string(), i);
                                    }
                                    c.set_table_cache(rows.clone());
                                    entities.set(rows.clone());
                                }
                            }
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            tracing::info!("use_synced_table({table}): client dropped");
                            return;
                        }
                    };

                    // Drain all immediately available notifications.
                    let mut batch = vec![first];
                    loop {
                        match rx.try_recv() {
                            Ok(n) => batch.push(n),
                            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                                tracing::warn!(
                                    "use_synced_table({table}): lagged {n}, re-materializing"
                                );
                                batch.clear();
                                if let Some(store) = c.store() {
                                    if let Ok(stored) = store.list_table_rows(&table).await {
                                        rows = stored
                                            .into_iter()
                                            .map(|r| E::from_columns(&r.pk, &r.columns))
                                            .collect();
                                        pk_index.clear();
                                        for (i, e) in rows.iter().enumerate() {
                                            pk_index.insert(e.pk().to_string(), i);
                                        }
                                        c.set_table_cache(rows.clone());
                                        entities.set(rows.clone());
                                    }
                                }
                                break;
                            }
                            Err(_) => break,
                        }
                    }

                    if batch.is_empty() {
                        continue;
                    }

                    // Group column changes by PK to minimize ser/deser.
                    let mut changed = false;
                    let mut grouped_updates: HashMap<
                        String,
                        HashMap<String, Option<serde_json::Value>>,
                    > = HashMap::new();

                    for change in batch {
                        if change.table.0 != table {
                            continue;
                        }
                        let pk = change.pk.0;
                        let cid = change.cid.0;

                        if cid == "__deleted" {
                            grouped_updates.remove(&pk);
                            if let Some(idx) = pk_index.remove(&pk) {
                                rows.swap_remove(idx);
                                if idx < rows.len() {
                                    let moved_pk = rows[idx].pk().to_string();
                                    pk_index.insert(moved_pk, idx);
                                }
                            }
                            changed = true;
                            continue;
                        }

                        grouped_updates
                            .entry(pk)
                            .or_default()
                            .insert(cid, change.val);
                        changed = true;
                    }

                    for (pk, col_updates) in grouped_updates {
                        if let Some(&idx) = pk_index.get(&pk) {
                            let mut cols: HashMap<String, serde_json::Value> =
                                rows[idx].to_columns().into_iter().collect();
                            for (cid, val) in col_updates {
                                match val {
                                    Some(v) => {
                                        cols.insert(cid, v);
                                    }
                                    None => {
                                        cols.remove(&cid);
                                    }
                                }
                            }
                            rows[idx] = E::from_columns(&pk, &cols);
                        } else {
                            let cols: HashMap<String, serde_json::Value> = col_updates
                                .into_iter()
                                .filter_map(|(k, v)| v.map(|v| (k, v)))
                                .collect();
                            pk_index.insert(pk.clone(), rows.len());
                            rows.push(E::from_columns(&pk, &cols));
                        }
                    }

                    if changed {
                        c.set_table_cache(rows.clone());
                        entities.set(rows.clone());
                    }
                }
            });
        }
    });

    entities
}
