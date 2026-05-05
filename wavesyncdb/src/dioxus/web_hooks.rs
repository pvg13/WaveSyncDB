//! Dioxus reactive hooks for the browser sync engine.
//!
//! Mirrors what the WhatsApp-Web-style local-first apps do: IndexedDB
//! is the single source of truth for application state, and components
//! drive themselves off a reactive stream of changes.
//!
//! [`use_synced_table::<E>(client, table)`] returns a
//! `Signal<Vec<E>>` that:
//!
//! 1. On the first render where `client` is `Some`, materializes the
//!    full table from [`BrowserStore::list_table_rows`] — so reload
//!    starts with whatever is persisted.
//! 2. Subscribes to [`WebSyncClient::subscribe_resolved`] and folds
//!    each `ColumnChange` into the in-memory `Vec<E>`. The engine
//!    echoes local writes onto the same channel after `submit_local_write`
//!    persists them, so a single subscription drives both local and
//!    remote updates without the component needing optimistic-merge code.
//! 3. Filters by table name so a single client can power multiple
//!    independent table hooks.
//!
//! The companion [`WebSyncClient::submit`](crate::WebSyncClient::submit)
//! is the writer: it takes a `&E`, serializes via
//! [`BrowserEntity::to_columns`], and goes through `submit_local_write`.
//! Components call `client.submit(&task).await` and get reactivity for
//! free.

use std::collections::HashMap;

use dioxus::prelude::*;

use crate::messages::ColumnChange;
use crate::web_engine::WebSyncClient;
use crate::web_entity::BrowserEntity;

/// Reactive `Vec<E>` materialized from a synced table.
///
/// `client` is a `Signal<Option<WebSyncClient>>` rather than a
/// `WebSyncClient` directly because the typical setup is to construct
/// the client async and store it in a parent signal — `None` until the
/// initial connect resolves. The hook waits internally for the first
/// `Some`, then materializes from [`BrowserStore::list_table_rows`] and
/// stays subscribed to changes for the rest of the component's
/// lifetime.
///
/// `table` is captured by value (`String`) so the hook can use it from
/// the spawned subscription task without lifetime gymnastics.
pub fn use_synced_table<E: BrowserEntity>(
    client: Signal<Option<WebSyncClient>>,
    table: &'static str,
) -> Signal<Vec<E>> {
    let entities = use_signal(Vec::<E>::new);

    use_effect({
        let table = table.to_string();
        let mut entities = entities;
        move || {
            // `use_effect` re-runs whenever any signal it reads changes.
            // The only signal we read is `client`, which transitions
            // None → Some exactly once in the typical setup, so the
            // effect body fires once with a Some value. If something
            // re-set client to None and back later, the effect would
            // re-run and re-spawn the subscriber — that's fine; the old
            // subscriber's task ends when the broadcast Receiver drops.
            let Some(c) = client.read().clone() else {
                return;
            };
            let table = table.clone();
            spawn(async move {
                // Materialize current state from the persisted shadow.
                if let Some(store) = c.store() {
                    match store.list_table_rows(&table).await {
                        Ok(rows) => {
                            let materialized: Vec<E> = rows
                                .into_iter()
                                .map(|r| E::from_columns(&r.pk, &r.columns))
                                .collect();
                            entities.set(materialized);
                        }
                        Err(e) => {
                            log::warn!("use_synced_table({table}): list_table_rows failed: {e}");
                        }
                    }
                }

                // Stream subsequent changes into the same signal. The
                // engine echoes local writes onto resolved_tx after
                // persistence, so this single loop handles both local
                // and remote updates uniformly.
                let mut rx = c.subscribe_resolved();
                loop {
                    match rx.recv().await {
                        Ok(change) => {
                            if change.table.0 == table {
                                apply_change_to::<E>(&mut entities, change);
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // Slow subscriber dropped messages. Recover
                            // by re-materializing from the store, which
                            // is authoritative.
                            log::warn!("use_synced_table({table}): lagged {n}, re-materializing");
                            if let Some(store) = c.store() {
                                if let Ok(rows) = store.list_table_rows(&table).await {
                                    entities.set(
                                        rows.into_iter()
                                            .map(|r| E::from_columns(&r.pk, &r.columns))
                                            .collect(),
                                    );
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            log::info!("use_synced_table({table}): client dropped");
                            return;
                        }
                    }
                }
            });
        }
    });

    entities
}

/// Fold a single `ColumnChange` into the materialized entity list.
///
/// Strategy: find the entity whose `pk` matches; if found, rebuild it
/// by re-serializing the existing entity (`to_columns`), patching the
/// affected column, and re-deserializing (`from_columns`). If not
/// found, the change refers to a row this client hasn't seen before —
/// build a fresh entity from the single column we just received. The
/// missing-column slots get the entity's default values via
/// `from_columns`'s `unwrap_or_default` pattern.
///
/// This handles the partial-update case (a single column edit on an
/// existing row) and the new-row case (first column write for a
/// previously unseen pk) without the caller having to special-case
/// either. The cost is one round-trip through `to_columns` /
/// `from_columns` per change — fine for interactive write rates.
fn apply_change_to<E: BrowserEntity>(entities: &mut Signal<Vec<E>>, change: ColumnChange) {
    let pk = change.pk.0;
    let cid = change.cid.0;
    let val = change.val;

    entities.with_mut(|vec| {
        if let Some(idx) = vec.iter().position(|e| e.pk() == pk) {
            let mut cols: HashMap<String, serde_json::Value> =
                vec[idx].to_columns().into_iter().collect();
            match val {
                Some(v) => {
                    cols.insert(cid, v);
                }
                None => {
                    cols.remove(&cid);
                }
            }
            vec[idx] = E::from_columns(&pk, &cols);
        } else {
            let mut cols = HashMap::new();
            if let Some(v) = val {
                cols.insert(cid, v);
            }
            vec.push(E::from_columns(&pk, &cols));
        }
    });
}
