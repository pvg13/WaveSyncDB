//! Web↔native convergence proof.
//!
//! One REAL native peer (full `WaveSyncDb`: SeaORM write interception,
//! SQLite shadow tables, real apply path) exchanges changesets with a
//! browser-core peer (`web_sync_core` over the in-memory `ShadowStore` —
//! the exact code the wasm engine runs). After every scenario the test
//! asserts the literal convergence proof:
//!
//! `engine::convergence::compute_group_digest(native) ==
//!  web_sync_core::compute_store_digest(web, web_common::test_now())`
//!
//! Equal digests mean the two replicas are byte-identical under the shared
//! cell-fingerprint definition — the same check the #82 reconciliation
//! exchange performs over the wire.
//!
//! The exchange is function-level (no libp2p): codec parity is covered by
//! the duplicated-codec unit structure and the qr_pairing e2e; what THIS
//! suite proves is that the two engines' apply/delete/visibility semantics
//! produce identical state from identical inputs.
//!
//! Node ID seeds: 80–89 (registered in CLAUDE.md §6).
//! Run single-threaded (a real engine instance runs mDNS):
//! `cargo test -p wavesyncdb --test web_native_convergence --features web -- --test-threads=1`

// Native-only (real WaveSyncDb: sea-orm, SQLite, tokio runtime). `wasm-pack
// test` runs `cargo build --tests`, which builds every integration test
// binary in the crate regardless of which one is named to run, so this
// file must be excluded from wasm32 test builds.
#![cfg(all(feature = "web", not(target_arch = "wasm32")))]

#[allow(dead_code)]
mod common;
mod web_common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;

use common::{make_node_id, make_peer, mem_db, task};
use wavesyncdb::WaveSyncDb;
use wavesyncdb::engine::convergence::{apply_remote_changeset, compute_group_digest};
use wavesyncdb::messages::{ChangeSource, ColumnChange, DeletePolicy, NodeId, SyncChangeset};
use wavesyncdb::registry::TableMeta;
use wavesyncdb::shadow;
use wavesyncdb::web_sync_core::{
    DELETED_COLUMN, ShadowStore, WebSyncConfig, WebTableConfig, apply_remote_changeset_core,
    changes_since_core, compute_store_digest, submit_local_delete_core, submit_local_write_core,
};
use web_common::MemoryStore;

// ── the web-core peer ─────────────────────────────────────────────────────

struct WebPeer {
    store: MemoryStore,
    cfg: WebSyncConfig,
    site: NodeId,
    dv: u64,
}

impl WebPeer {
    fn new(seed: u8, policy: DeletePolicy) -> Self {
        Self {
            store: MemoryStore::new(),
            cfg: WebSyncConfig::default().with_table(
                "tasks",
                WebTableConfig {
                    delete_policy: policy,
                    // Must match the native registry so the PK column's
                    // clock cell is excluded from both digests alike.
                    primary_key_column: Some("id".into()),
                },
            ),
            site: make_node_id(seed),
            dv: 0,
        }
    }

    /// Local write mirroring what `BrowserEntity::to_columns` produces for
    /// the `tasks` entity. `completed` is written as a JSON NUMBER (0/1) —
    /// SQLite stores the native bool as INTEGER and `json_object()` reads
    /// it back as a number, so this is the representation that fingerprints
    /// identically across targets (see the value-byte parity note in
    /// `web_sync_core`).
    async fn write(&mut self, pk: &str, title: &str, completed: i64) {
        self.dv += 1;
        submit_local_write_core(
            &self.store,
            &self.cfg,
            &self.site,
            "tasks",
            pk,
            vec![
                ("id".into(), serde_json::json!(pk)),
                ("title".into(), serde_json::json!(title)),
                ("completed".into(), serde_json::json!(completed)),
            ],
            self.dv,
            web_common::test_now(),
        )
        .await
        .unwrap();
    }

    async fn delete(&mut self, pk: &str) {
        self.dv += 1;
        submit_local_delete_core(
            &self.store,
            &self.cfg,
            &self.site,
            "tasks",
            pk,
            self.dv,
            web_common::test_now(),
        )
        .await
        .unwrap();
    }

    async fn apply(&mut self, changes: Vec<ColumnChange>, from_site: NodeId) {
        if changes.is_empty() {
            return;
        }
        self.dv += 1;
        let cs = SyncChangeset {
            site_id: from_site,
            db_version: 0,
            changes,
        };
        apply_remote_changeset_core(
            &self.store,
            &self.cfg,
            &cs,
            self.dv,
            None,
            web_common::test_now(),
        )
        .await
        .unwrap();
    }

    /// The sync-visible state (tombstoned rows expose only their tombstone).
    async fn state(&self) -> Vec<ColumnChange> {
        changes_since_core(&self.store, &self.cfg, 0, web_common::test_now())
            .await
            .unwrap()
    }

    async fn digest(&self) -> [u8; 32] {
        compute_store_digest(&self.store, &self.cfg, web_common::test_now())
            .await
            .unwrap()
    }
}

// ── the native peer helpers ───────────────────────────────────────────────

async fn native_state(peer: &WaveSyncDb) -> Vec<ColumnChange> {
    shadow::get_changes_since(peer.inner(), peer.registry(), 0)
        .await
        .unwrap()
}

async fn native_digest(peer: &WaveSyncDb) -> [u8; 32] {
    compute_group_digest(peer.inner(), peer.registry()).await
}

async fn apply_to_native(peer: &WaveSyncDb, changes: Vec<ColumnChange>, from_site: NodeId) {
    if changes.is_empty() {
        return;
    }
    let committed = apply_remote_changeset(
        peer.inner(),
        peer.change_tx(),
        peer.registry(),
        &changes,
        None,
        ChangeSource::Remote {
            peer_site: from_site,
        },
        None,
    )
    .await;
    assert!(committed, "native apply must commit");
}

/// Full-state exchange in both directions, then assert the digests match.
/// Re-applying already-held changes is a no-op (CRDT idempotence), so
/// full-state exchange is the simplest complete delivery.
async fn exchange_and_prove(native: &WaveSyncDb, web: &mut WebPeer, scenario: &str) {
    let native_site = NodeId([0u8; 16]); // changeset site label only; cells carry their own
    apply_to_native(native, web.state().await, web.site).await;
    web.apply(native_state(native).await, native_site).await;
    // Second native pass: changes web held that native rejected on the
    // first pass (e.g. ordering) — full state is idempotent.
    apply_to_native(native, web.state().await, web.site).await;

    let nd = native_digest(native).await;
    let wd = web.digest().await;
    assert_eq!(
        nd, wd,
        "{scenario}: native and web digests must be identical — convergence proof failed"
    );
}

fn unique_topic(name: &str) -> String {
    format!("convergence-{name}-{}", Uuid::new_v4())
}

// ── scenarios ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn inserts_and_updates_converge_bidirectionally() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_a"), &unique_topic("ins"), 80).await;
    let mut web = WebPeer::new(81, DeletePolicy::DeleteWins);

    // Native writes through the REAL SeaORM interception path.
    for (id, title) in [("n1", "native one"), ("n2", "native two")] {
        task::ActiveModel {
            id: Set(id.into()),
            title: Set(title.into()),
            completed: Set(false),
        }
        .insert(&native)
        .await
        .unwrap();
    }
    // ...including an update (per-column clock bump via UPDATE interception).
    let mut n1: task::ActiveModel = task::Entity::find_by_id("n1")
        .one(&native)
        .await
        .unwrap()
        .unwrap()
        .into();
    n1.title = Set("native one v2".into());
    n1.update(&native).await.unwrap();

    // Web writes through the real browser core.
    web.write("w1", "web one", 0).await;
    web.write("w2", "web two", 1).await;
    web.write("w1", "web one v2", 0).await; // col_version bump

    exchange_and_prove(&native, &mut web, "inserts+updates").await;

    // Cross-checks beyond the digest: both replicas materialize all rows.
    let rows = task::Entity::find().all(&native).await.unwrap();
    assert_eq!(rows.len(), 4);
    let w1 = rows.iter().find(|r| r.id == "w1").unwrap();
    assert_eq!(w1.title, "web one v2");
}

#[tokio::test]
async fn native_delete_propagates_to_web() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_b"), &unique_topic("ndel"), 82).await;
    let mut web = WebPeer::new(83, DeletePolicy::DeleteWins);

    task::ActiveModel {
        id: Set("del1".into()),
        title: Set("to delete".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "pre-delete seed").await;

    // Real native delete (tombstone cv = max+1 via DELETE interception).
    task::Entity::delete_by_id("del1")
        .exec(&native)
        .await
        .unwrap();

    exchange_and_prove(&native, &mut web, "native delete").await;

    // Web's replica holds exactly the tombstone for del1.
    let visible = web.state().await;
    let del1: Vec<_> = visible.iter().filter(|c| c.pk.0 == "del1").collect();
    assert_eq!(del1.len(), 1);
    assert_eq!(del1[0].cid.0, DELETED_COLUMN);
}

#[tokio::test]
async fn web_delete_propagates_to_native() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_c"), &unique_topic("wdel"), 84).await;
    let mut web = WebPeer::new(85, DeletePolicy::DeleteWins);

    task::ActiveModel {
        id: Set("wd1".into()),
        title: Set("web will delete".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "pre-delete seed").await;

    web.delete("wd1").await;

    exchange_and_prove(&native, &mut web, "web delete").await;
    assert!(
        task::Entity::find_by_id("wd1")
            .one(&native)
            .await
            .unwrap()
            .is_none(),
        "native must have deleted the row web tombstoned"
    );
}

#[tokio::test]
async fn concurrent_edit_vs_delete_tie_delete_wins() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_d"), &unique_topic("tie-dw"), 86).await;
    let mut web = WebPeer::new(87, DeletePolicy::DeleteWins);

    task::ActiveModel {
        id: Set("c1".into()),
        title: Set("contested".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "seed").await;

    // Concurrent: native edits (title cv 1→2) while web deletes
    // (tombstone cl = max_cv+1 = 2) — a TIE, resolved by policy on both
    // sides. This is exactly the divergence vector the delete-parity work
    // closed: before it, web resolved this through the generic column
    // comparator and could disagree with native.
    let mut c1: task::ActiveModel = task::Entity::find_by_id("c1")
        .one(&native)
        .await
        .unwrap()
        .unwrap()
        .into();
    c1.title = Set("edited concurrently".into());
    c1.update(&native).await.unwrap();
    web.delete("c1").await;

    exchange_and_prove(&native, &mut web, "DeleteWins tie").await;
    assert!(
        task::Entity::find_by_id("c1")
            .one(&native)
            .await
            .unwrap()
            .is_none(),
        "DeleteWins: the delete must win the tie on the native side too"
    );
}

#[tokio::test]
async fn concurrent_edit_vs_delete_tie_add_wins_keeps_row_on_both() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_e"), &unique_topic("tie-aw"), 88).await;
    // Flip the native registry to AddWins for this scenario (upsert) and
    // configure web identically — mismatched policies are themselves a
    // divergence vector.
    native.registry().register(TableMeta {
        table_name: "tasks".into(),
        primary_key_column: "id".into(),
        columns: vec!["id".into(), "title".into(), "completed".into()],
        delete_policy: DeletePolicy::AddWins,
    });
    let mut web = WebPeer::new(89, DeletePolicy::AddWins);

    task::ActiveModel {
        id: Set("c2".into()),
        title: Set("contested".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "seed").await;

    let mut c2: task::ActiveModel = task::Entity::find_by_id("c2")
        .one(&native)
        .await
        .unwrap()
        .unwrap()
        .into();
    c2.title = Set("edited concurrently".into());
    c2.update(&native).await.unwrap();
    web.delete("c2").await;

    // Cross-apply and prove convergence. AddWins: the delete loses the tie on
    // both engines, and — with the N8 fix — the losing deleter (web) clears its
    // now-defeated tombstone when the winning edit applies, so the two replicas'
    // cell sets become identical rather than diverging permanently.
    exchange_and_prove(&native, &mut web, "AddWins tie").await;

    // The native row survives — same outcome a native AddWins receiver produces.
    let row = task::Entity::find_by_id("c2").one(&native).await.unwrap();
    assert_eq!(
        row.map(|r| r.title),
        Some("edited concurrently".to_string()),
        "AddWins: the concurrent edit must survive the tie on native"
    );
    // Web applied native's winning edit and cleared its tombstone.
    let title = web
        .store
        .get_shadow("tasks", "c2", "title")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(title.val, Some(serde_json::json!("edited concurrently")));
    assert_eq!(title.col_version, 2);
    assert!(
        web.store
            .get_shadow("tasks", "c2", DELETED_COLUMN)
            .await
            .unwrap()
            .is_none(),
        "N8: the losing deleter must clear its defeated tombstone"
    );
}

#[tokio::test]
async fn dominant_delete_is_not_resurrected_by_stale_edit() {
    // The N8 gate must not over-correct: a delete that provably DOMINATES an
    // incoming edit (its causal length exceeds the edit's col_version) keeps the
    // row deleted — the stale edit must not resurrect it.
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_dom"), &unique_topic("dom"), 86).await;
    let mut web = WebPeer::new(87, DeletePolicy::AddWins);
    native.registry().register(TableMeta {
        table_name: "tasks".into(),
        primary_key_column: "id".into(),
        columns: vec!["id".into(), "title".into(), "completed".into()],
        delete_policy: DeletePolicy::AddWins,
    });

    // Seed and sync a row.
    task::ActiveModel {
        id: Set("d1".into()),
        title: Set("v1".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "seed").await;

    // Native advances the row's clock (two edits → title cv 3) then deletes it,
    // so the tombstone's causal length (4) dominates any single stale edit.
    for t in ["v2", "v3"] {
        let mut m: task::ActiveModel = task::Entity::find_by_id("d1")
            .one(&native)
            .await
            .unwrap()
            .unwrap()
            .into();
        m.title = Set(t.into());
        m.update(&native).await.unwrap();
    }
    // Web makes a single stale concurrent edit (title cv 2) before it hears the
    // native churn, then native deletes.
    web.write("d1", "stale-edit", 0).await;
    task::Entity::delete_by_id("d1")
        .exec(&native)
        .await
        .unwrap();

    // Converge. The dominant delete wins on both sides; the stale edit never
    // resurrects the row, and the digests match.
    exchange_and_prove(&native, &mut web, "dominant delete").await;
    assert!(
        task::Entity::find_by_id("d1")
            .one(&native)
            .await
            .unwrap()
            .is_none(),
        "a delete that dominates a stale edit must keep the row deleted"
    );
}

#[tokio::test]
async fn reinsert_after_won_delete_converges_deletewins() {
    // Resurrection floor (N8 part A): a row deleted then re-inserted on the SAME
    // node must revive above its own tombstone, or a DeleteWins peer that still
    // holds the tombstone rejects the re-insert (equal cl → delete wins the tie)
    // and the two replicas diverge permanently.
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_res"), &unique_topic("res"), 84).await;
    let mut web = WebPeer::new(85, DeletePolicy::DeleteWins);

    // Seed + sync a row so the web peer holds it (and will hold the tombstone).
    task::ActiveModel {
        id: Set("r1".into()),
        title: Set("orig".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "seed").await;

    // Native deletes, then immediately re-inserts the same pk locally. Without
    // the floor the revived title lands at col_version == tombstone.cl.
    task::Entity::delete_by_id("r1")
        .exec(&native)
        .await
        .unwrap();
    task::ActiveModel {
        id: Set("r1".into()),
        title: Set("revived".into()),
        completed: Set(true),
    }
    .insert(&native)
    .await
    .unwrap();

    // Converge. The revived row must survive on both sides and the digests match.
    exchange_and_prove(&native, &mut web, "reinsert after won delete").await;
    let row = task::Entity::find_by_id("r1").one(&native).await.unwrap();
    assert_eq!(
        row.map(|r| r.title),
        Some("revived".to_string()),
        "the re-inserted row must survive against a peer's stale tombstone"
    );
}

#[tokio::test]
async fn interleaved_batches_converge() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_f"), &unique_topic("mix"), 80).await;
    let mut web = WebPeer::new(81, DeletePolicy::DeleteWins);

    // Interleave writes and partial exchanges — order independence is the
    // point of the CRDT; the digest must come out identical regardless.
    for round in 0..3u64 {
        task::ActiveModel {
            id: Set(format!("n-{round}")),
            title: Set(format!("native round {round}")),
            completed: Set(round % 2 == 0),
        }
        .insert(&native)
        .await
        .unwrap();
        web.write(
            &format!("w-{round}"),
            &format!("web round {round}"),
            (round % 2) as i64,
        )
        .await;

        // Partial, one-directional exchange mid-stream.
        if round == 1 {
            web.apply(native_state(&native).await, NodeId([0u8; 16]))
                .await;
        }
    }
    // Contested cell: both edit the same column of the same row.
    web.apply(native_state(&native).await, NodeId([0u8; 16]))
        .await;
    web.write("n-0", "web overwrote native", 1).await; // cv 1→2
    let mut n0: task::ActiveModel = task::Entity::find_by_id("n-0")
        .one(&native)
        .await
        .unwrap()
        .unwrap()
        .into();
    n0.title = Set("native overwrote native".into()); // cv 1→2: TIE with web's
    n0.update(&native).await.unwrap();

    exchange_and_prove(&native, &mut web, "interleaved batches").await;
}

// ---------------------------------------------------------------------------
// Retention: aged tombstones vanish from both engines by the SAME shared
// timestamp rule, so digests stay equal no matter when (or whether) each
// side physically garbage-collects — the central claim of the retention
// design. Aging is injected (never slept): both replicas hold the same
// wire-carried deleted_ts, so rewriting it on both sides is exactly what
// the passage of time would do.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn aged_tombstones_stay_convergent_across_gc_timing() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_ret"), &unique_topic("ret"), 178).await;
    let mut web = WebPeer::new(179, DeletePolicy::DeleteWins);

    // Same retention window on both sides (100s).
    wavesyncdb::shadow::set_tombstone_retention(
        native.inner(),
        Some(std::time::Duration::from_secs(100)),
    )
    .await
    .unwrap();
    web.cfg.tombstone_retention_secs = Some(100);

    // A row lives on both, then web deletes it; the tombstone propagates.
    web.write("r1", "doomed", 0).await;
    exchange_and_prove(&native, &mut web, "retention: row propagated").await;
    web.delete("r1").await;
    exchange_and_prove(&native, &mut web, "retention: delete propagated").await;
    assert!(
        native_state(&native)
            .await
            .iter()
            .any(|c| c.cid == DELETED_COLUMN && c.pk == "r1"),
        "fresh tombstone visible on native"
    );

    // Age the SAME tombstone on both sides (simulates elapsed time).
    let aged = web_common::test_now() - 1000;
    native
        .inner()
        .execute_unprepared(&format!(
            "UPDATE \"_wavesync_tasks_clock\" SET deleted_ts = {aged} \
             WHERE pk = 'r1' AND cid = '__deleted'"
        ))
        .await
        .unwrap();
    web.store.age_tombstone("tasks", "r1", aged);

    // Both exclude it — digests equal, tombstone gone from every surface.
    exchange_and_prove(&native, &mut web, "retention: aged out on both").await;
    assert!(
        !native_state(&native)
            .await
            .iter()
            .any(|c| c.cid == DELETED_COLUMN),
        "aged tombstone must not travel from native"
    );
    assert!(
        !web.state().await.iter().any(|c| c.cid.0 == DELETED_COLUMN),
        "aged tombstone must not travel from web"
    );

    // Asymmetric physical GC: native deletes the row from disk, web only
    // excludes. Digest equality must be unaffected — physical timing is a
    // local concern.
    let collected = wavesyncdb::shadow::gc_aged_tombstones(native.inner(), native.registry())
        .await
        .unwrap();
    assert_eq!(collected, 1, "native physically collected the tombstone");
    exchange_and_prove(&native, &mut web, "retention: asymmetric physical GC").await;
}

/// Post-GC: unlike the asymmetric case above (native reaps, web only
/// excludes), here BOTH sides physically reap the aged tombstone —
/// native via `shadow::gc_aged_tombstones`, web via
/// `web_sync_core::gc_aged_tombstones_core` (the browser-core GC path).
/// Exclusion already hides an aged tombstone from every surface before
/// either side reaps; this proves that actually reclaiming its storage —
/// on one side, the other, or both — changes nothing a peer can observe.
/// Same node ID seeds as the retention scenario above (178–179):
/// reusing them here is safe (each test builds its own unique topic /
/// mem_db, matching the reuse pattern already established elsewhere in
/// this file), and no new libp2p exchange is involved — this suite stays
/// function-level.
#[tokio::test]
async fn post_gc_digests_stay_equal() {
    common::init_test_tracing();
    let native = make_peer(&mem_db("conv_gc"), &unique_topic("gc"), 178).await;
    let mut web = WebPeer::new(179, DeletePolicy::DeleteWins);

    // Same retention window on both sides.
    wavesyncdb::shadow::set_tombstone_retention(
        native.inner(),
        Some(std::time::Duration::from_secs(100)),
    )
    .await
    .unwrap();
    web.cfg.tombstone_retention_secs = Some(100);

    // Insert + delete on native, then propagate so both replicas hold the
    // (live) tombstone.
    task::ActiveModel {
        id: Set("gc1".into()),
        title: Set("will be gc'd".into()),
        completed: Set(false),
    }
    .insert(&native)
    .await
    .unwrap();
    exchange_and_prove(&native, &mut web, "post-gc: row propagated").await;

    task::Entity::delete_by_id("gc1")
        .exec(&native)
        .await
        .unwrap();
    exchange_and_prove(&native, &mut web, "post-gc: delete propagated").await;
    assert!(
        web.state()
            .await
            .iter()
            .any(|c| c.cid.0 == DELETED_COLUMN && c.pk.0 == "gc1"),
        "web must hold the live tombstone before aging"
    );

    // Age the SAME tombstone on both sides — injected, never slept, exactly
    // like the retention scenario above.
    let aged = web_common::test_now() - 1000;
    native
        .inner()
        .execute_unprepared(&format!(
            "UPDATE \"_wavesync_tasks_clock\" SET deleted_ts = {aged} \
             WHERE pk = 'gc1' AND cid = '__deleted'"
        ))
        .await
        .unwrap();
    web.store.age_tombstone("tasks", "gc1", aged);

    // Both exclude it — digests equal before either side has physically
    // reaped anything (sanity: exclusion, not GC timing, drives this).
    exchange_and_prove(&native, &mut web, "post-gc: aged out on both").await;

    // Now physically reap on BOTH sides.
    let native_collected =
        wavesyncdb::shadow::gc_aged_tombstones(native.inner(), native.registry())
            .await
            .unwrap();
    assert_eq!(
        native_collected, 1,
        "native must physically collect the tombstone"
    );

    let web_collected = wavesyncdb::web_sync_core::gc_aged_tombstones_core(
        &web.cfg,
        &web.store,
        web_common::test_now(),
    )
    .await
    .unwrap();
    assert!(
        web_collected > 0,
        "web must physically reap the aged tombstone"
    );

    // Post-GC: digests must still match — physical reaping, on either side
    // or both, is a local storage concern invisible to convergence.
    exchange_and_prove(&native, &mut web, "post-gc: physical GC on both").await;
}
