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

// ── delete semantics (mirrors native should_apply_delete + tombstones) ───

use wavesyncdb::messages::DeletePolicy;
use wavesyncdb::web_sync_core::{DELETED_COLUMN, WebTableConfig, submit_local_delete_core};

fn delete_change(table: &str, pk: &str, site: NodeId, cl: u64) -> ColumnChange {
    ColumnChange {
        table: TableName(table.into()),
        pk: PrimaryKey(pk.into()),
        cid: ColumnName(DELETED_COLUMN.into()),
        val: None,
        site_id: site,
        col_version: cl,
        cl,
        seq: 0,
        db_version: 0,
    }
}

fn cfg_with_policy(policy: DeletePolicy) -> WebSyncConfig {
    WebSyncConfig::default().with_table(
        "tasks",
        WebTableConfig {
            delete_policy: policy,
            primary_key_column: Some("id".into()),
        },
    )
}

/// Seed a row with two columns at cv=1 (local max cv = 1).
async fn seed_row(store: &MemoryStore) {
    submit_local_write_core(
        store,
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
}

#[tokio::test]
async fn remote_delete_with_higher_cl_wins_and_tombstones() {
    let store = MemoryStore::new();
    let cfg = cfg_with_policy(DeletePolicy::DeleteWins);
    seed_row(&store).await; // local max cv = 1

    let cs = changeset(SITE_B, 9, vec![delete_change("tasks", "p1", SITE_B, 2)]);
    let applied = apply_remote_changeset_core(&store, &cfg, &cs, 2, Some("peer-b"))
        .await
        .unwrap();
    assert_eq!(
        applied.len(),
        1,
        "winning delete is surfaced to subscribers"
    );
    assert_eq!(applied[0].cid.0, DELETED_COLUMN);

    // Per-column clock entries are gone; only the tombstone remains —
    // mirrors native delete_clock_entries + insert_tombstone.
    assert_eq!(store.row_entry_count("tasks", "p1"), 1);
    let tomb = store
        .get_shadow("tasks", "p1", DELETED_COLUMN)
        .await
        .unwrap()
        .expect("tombstone written");
    assert_eq!(tomb.val, None);
    assert_eq!(tomb.col_version, 2);
    assert_eq!(tomb.seq, 0);
    assert_eq!(tomb.site_id, SITE_B.0);
}

#[tokio::test]
async fn remote_delete_with_lower_cl_is_a_noop() {
    let store = MemoryStore::new();
    let cfg = cfg_with_policy(DeletePolicy::DeleteWins);
    seed_row(&store).await;
    // Bump title to cv=3 (local max cv = 3).
    submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("t2"))],
        2,
    )
    .await
    .unwrap();
    submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("t3"))],
        3,
    )
    .await
    .unwrap();

    let cs = changeset(SITE_B, 9, vec![delete_change("tasks", "p1", SITE_B, 2)]);
    let applied = apply_remote_changeset_core(&store, &cfg, &cs, 4, Some("peer-b"))
        .await
        .unwrap();
    assert!(applied.is_empty(), "stale delete must not apply");
    assert_eq!(store.row_entry_count("tasks", "p1"), 2, "row untouched");
    assert!(
        store
            .get_shadow("tasks", "p1", DELETED_COLUMN)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_tie_respects_policy() {
    // Tie: delete cl == local max cv. DeleteWins applies it; AddWins keeps
    // the row — mirrors native sync_handler AddWins-tie test.
    for (policy, expect_deleted) in [
        (DeletePolicy::DeleteWins, true),
        (DeletePolicy::AddWins, false),
    ] {
        let store = MemoryStore::new();
        let cfg = cfg_with_policy(policy.clone());
        seed_row(&store).await; // local max cv = 1

        let cs = changeset(SITE_B, 9, vec![delete_change("tasks", "p1", SITE_B, 1)]);
        let applied = apply_remote_changeset_core(&store, &cfg, &cs, 2, Some("peer-b"))
            .await
            .unwrap();
        if expect_deleted {
            assert_eq!(applied.len(), 1, "{policy:?}: tie should delete");
            assert_eq!(store.row_entry_count("tasks", "p1"), 1, "tombstone only");
        } else {
            assert!(applied.is_empty(), "{policy:?}: tie should keep the row");
            assert_eq!(store.row_entry_count("tasks", "p1"), 2, "row intact");
        }
    }
}

#[tokio::test]
async fn delete_branch_skips_sibling_column_changes() {
    // A changeset carrying both a delete and column changes for the same
    // pk: only the delete branch runs (native sync_handler.rs:1332-1347).
    let store = MemoryStore::new();
    let cfg = cfg_with_policy(DeletePolicy::DeleteWins);
    seed_row(&store).await;

    let cs = changeset(
        SITE_B,
        9,
        vec![
            change(
                "tasks",
                "p1",
                "title",
                serde_json::json!("zombie"),
                SITE_B,
                5,
            ),
            delete_change("tasks", "p1", SITE_B, 5),
        ],
    );
    apply_remote_changeset_core(&store, &cfg, &cs, 2, Some("peer-b"))
        .await
        .unwrap();

    assert_eq!(store.row_entry_count("tasks", "p1"), 1, "tombstone only");
    assert!(
        store
            .get_shadow("tasks", "p1", "title")
            .await
            .unwrap()
            .is_none(),
        "sibling column change must NOT survive a winning delete"
    );
}

#[tokio::test]
async fn local_delete_emits_native_shaped_tombstone() {
    let store = MemoryStore::new();
    seed_row(&store).await; // max cv = 1

    let changes = submit_local_delete_core(&store, &SITE_A, "tasks", "p1", 2)
        .await
        .unwrap();
    assert_eq!(changes.len(), 1);
    let del = &changes[0];
    assert_eq!(del.cid.0, DELETED_COLUMN);
    assert_eq!(del.val, None);
    assert_eq!(del.col_version, 2, "tombstone cv = max_cv + 1");
    assert_eq!(del.cl, 2, "delete: cl == col_version");
    assert_eq!(del.seq, 0);

    // Native local delete leaves per-column clock entries in place (only
    // the receiving side clears them when the delete wins there).
    assert_eq!(store.row_entry_count("tasks", "p1"), 3);
    assert!(
        store
            .get_shadow("tasks", "p1", DELETED_COLUMN)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn local_write_resurrects_tombstoned_row() {
    let store = MemoryStore::new();
    seed_row(&store).await;
    submit_local_delete_core(&store, &SITE_A, "tasks", "p1", 2)
        .await
        .unwrap();

    // Re-insert: tombstone cleared, per-column clocks continue (not reset).
    let written = submit_local_write_core(
        &store,
        &SITE_A,
        "tasks",
        "p1",
        vec![("title".into(), serde_json::json!("back"))],
        3,
    )
    .await
    .unwrap();
    assert!(
        store
            .get_shadow("tasks", "p1", DELETED_COLUMN)
            .await
            .unwrap()
            .is_none(),
        "tombstone cleared on resurrection"
    );
    assert_eq!(
        written[0].col_version, 2,
        "col_version continues from the pre-delete value"
    );
}
