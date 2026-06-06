//! Native-run tests for the browser sync core (`web_sync_core`).
//!
//! The browser engine's convergence-critical logic is target-independent and
//! written against the `ShadowStore` trait, so its semantics — conflict
//! resolution, delete policies, fail-closed atomic persistence — are asserted
//! here in plain `cargo test` with an in-memory store, no browser needed.
//!
//! Run with: `cargo test -p wavesyncdb --test web_core --features web`

#![cfg(feature = "web")]

mod web_common;

use wavesyncdb::messages::{
    ColumnChange, ColumnName, NodeId, PrimaryKey, SyncChangeset, TableName,
};
use wavesyncdb::web_sync_core::{
    ShadowStore, WebSyncConfig, apply_remote_changeset_core, submit_local_write_core,
};

use web_common::MemoryStore;

const SITE_A: NodeId = NodeId([1u8; 16]);
const SITE_B: NodeId = NodeId([2u8; 16]);

fn change(
    table: &str,
    pk: &str,
    cid: &str,
    val: serde_json::Value,
    site: NodeId,
    cv: u64,
) -> ColumnChange {
    ColumnChange {
        table: TableName(table.into()),
        pk: PrimaryKey(pk.into()),
        cid: ColumnName(cid.into()),
        val: Some(val),
        site_id: site,
        col_version: cv,
        cl: cv,
        seq: 0,
        db_version: 0,
    }
}

fn changeset(site: NodeId, db_version: u64, changes: Vec<ColumnChange>) -> SyncChangeset {
    SyncChangeset {
        site_id: site,
        db_version,
        changes,
    }
}

// ── column conflict resolution (mirrors native should_apply_column) ──────

#[tokio::test]
async fn remote_higher_col_version_wins() {
    let store = MemoryStore::new();
    let cfg = WebSyncConfig::default();

    // Local state at cv=1 via a local write.
    let written = submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("local"))],
        1,
    )
    .await
    .unwrap();
    assert_eq!(written.len(), 1);
    assert_eq!(written[0].col_version, 1);

    // Remote cv=2 must win.
    let cs = changeset(
        SITE_B,
        10,
        vec![change(
            "tasks",
            "p1",
            "title",
            serde_json::json!("remote"),
            SITE_B,
            2,
        )],
    );
    let applied = apply_remote_changeset_core(&store, &cfg, &cs, 2, Some("peer-b"))
        .await
        .unwrap();
    assert_eq!(applied.len(), 1);

    let row = store
        .get_shadow("tasks", "p1", "title")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.col_version, 2);
    assert_eq!(row.val, Some(serde_json::json!("remote")));
    assert_eq!(row.site_id, SITE_B.0);
}

#[tokio::test]
async fn remote_lower_col_version_loses() {
    let store = MemoryStore::new();
    let cfg = WebSyncConfig::default();

    // Local at cv=2 (two writes).
    submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("v1"))],
        1,
    )
    .await
    .unwrap();
    submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("v2"))],
        2,
    )
    .await
    .unwrap();

    let cs = changeset(
        SITE_B,
        5,
        vec![change(
            "tasks",
            "p1",
            "title",
            serde_json::json!("stale"),
            SITE_B,
            1,
        )],
    );
    let applied = apply_remote_changeset_core(&store, &cfg, &cs, 3, Some("peer-b"))
        .await
        .unwrap();
    assert!(applied.is_empty(), "stale remote change must not apply");

    let row = store
        .get_shadow("tasks", "p1", "title")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.val, Some(serde_json::json!("v2")));
    assert_eq!(row.col_version, 2);

    // Even when nothing applies, the peer cursor advances (we processed the
    // changeset) — in the same committed batch.
    assert_eq!(store.peer_version("peer-b"), 5);
}

#[tokio::test]
async fn equal_col_version_ties_break_deterministically() {
    // Same cv, different values: value-bytes then site_id ordering decides —
    // both replicas must pick the same winner regardless of arrival order.
    let store_x = MemoryStore::new();
    let store_y = MemoryStore::new();
    let cfg = WebSyncConfig::default();

    let a = change("tasks", "p1", "title", serde_json::json!("aaa"), SITE_A, 3);
    let b = change("tasks", "p1", "title", serde_json::json!("zzz"), SITE_B, 3);

    // X sees a then b; Y sees b then a.
    apply_remote_changeset_core(
        &store_x,
        &cfg,
        &changeset(SITE_A, 1, vec![a.clone()]),
        1,
        None,
    )
    .await
    .unwrap();
    apply_remote_changeset_core(
        &store_x,
        &cfg,
        &changeset(SITE_B, 1, vec![b.clone()]),
        2,
        None,
    )
    .await
    .unwrap();
    apply_remote_changeset_core(&store_y, &cfg, &changeset(SITE_B, 1, vec![b]), 1, None)
        .await
        .unwrap();
    apply_remote_changeset_core(&store_y, &cfg, &changeset(SITE_A, 1, vec![a]), 2, None)
        .await
        .unwrap();

    let row_x = store_x
        .get_shadow("tasks", "p1", "title")
        .await
        .unwrap()
        .unwrap();
    let row_y = store_y
        .get_shadow("tasks", "p1", "title")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row_x.val, row_y.val, "order-independent convergence");
    assert_eq!(row_x.site_id, row_y.site_id);
}

// ── fail-closed atomic persistence ────────────────────────────────────────

#[tokio::test]
async fn failed_batch_persists_nothing_and_returns_err() {
    let store = MemoryStore::new();
    let cfg = WebSyncConfig::default();

    store.fail_next_batch();
    let cs = changeset(
        SITE_B,
        7,
        vec![change(
            "tasks",
            "p1",
            "title",
            serde_json::json!("x"),
            SITE_B,
            1,
        )],
    );
    let res = apply_remote_changeset_core(&store, &cfg, &cs, 1, Some("peer-b")).await;
    assert!(res.is_err(), "store failure must surface as Err");

    // Fail-closed: NOTHING persisted — no shadow row, no db_version, no
    // peer cursor. The change will be re-requested by the next catch-up.
    assert!(
        store
            .get_shadow("tasks", "p1", "title")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.db_version(), 0);
    assert_eq!(store.peer_version("peer-b"), 0);

    // The same changeset applies cleanly afterwards (no poisoned state).
    let applied = apply_remote_changeset_core(&store, &cfg, &cs, 1, Some("peer-b"))
        .await
        .unwrap();
    assert_eq!(applied.len(), 1);
    assert_eq!(store.db_version(), 1);
    assert_eq!(store.peer_version("peer-b"), 7);
}

#[tokio::test]
async fn failed_local_write_persists_nothing() {
    let store = MemoryStore::new();

    store.fail_next_batch();
    let res = submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("x"))],
        1,
    )
    .await;
    assert!(res.is_err());
    assert!(
        store
            .get_shadow("tasks", "p1", "title")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.db_version(), 0);
}

// ── local write clock semantics ───────────────────────────────────────────

#[tokio::test]
async fn local_write_increments_col_version_per_column() {
    let store = MemoryStore::new();

    submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![
            ("title".into(), serde_json::json!("t")),
            ("done".into(), serde_json::json!(false)),
        ],
        1,
    )
    .await
    .unwrap();
    let written = submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("t2"))],
        2,
    )
    .await
    .unwrap();

    assert_eq!(written[0].col_version, 2, "title bumped 1 → 2");
    let title = store
        .get_shadow("tasks", "p1", "title")
        .await
        .unwrap()
        .unwrap();
    let done = store
        .get_shadow("tasks", "p1", "done")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(title.col_version, 2);
    assert_eq!(done.col_version, 1, "untouched column keeps its clock");
    assert_eq!(title.db_version, 2);
    assert_eq!(done.db_version, 1);
}

#[tokio::test]
async fn applied_changes_round_trip_through_catch_up() {
    // What we persist must come back out of get_changes_since for catch-up.
    let store = MemoryStore::new();
    let cfg = WebSyncConfig::default();

    let cs = changeset(
        SITE_B,
        3,
        vec![
            change("tasks", "p1", "title", serde_json::json!("a"), SITE_B, 1),
            change("tasks", "p2", "title", serde_json::json!("b"), SITE_B, 1),
        ],
    );
    apply_remote_changeset_core(&store, &cfg, &cs, 1, Some("peer-b"))
        .await
        .unwrap();

    let out = store.get_changes_since(0).await.unwrap();
    assert_eq!(out.len(), 2);
    // Stamped with OUR db_version (the local Lamport batch), not the peer's.
    assert!(out.iter().all(|c| c.db_version == 1));

    let none = store.get_changes_since(1).await.unwrap();
    assert!(none.is_empty());
}
