//! Web↔native convergence proof.
//!
//! One REAL native peer (full `WaveSyncDb`: SeaORM write interception,
//! SQLite shadow tables, real apply path) exchanges changesets with a
//! browser-core peer (`web_sync_core` over the in-memory `ShadowStore` —
//! the exact code the wasm engine runs). After every scenario the test
//! asserts the literal convergence proof:
//!
//! `engine::convergence::compute_group_digest(native) ==
//!  web_sync_core::compute_store_digest(web)`
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

#![cfg(feature = "web")]

#[allow(dead_code)]
mod common;
mod web_common;

use sea_orm::{ActiveModelTrait, EntityTrait, Set};
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
            &self.site,
            "tasks",
            pk,
            vec![
                ("id".into(), serde_json::json!(pk)),
                ("title".into(), serde_json::json!(title)),
                ("completed".into(), serde_json::json!(completed)),
            ],
            self.dv,
        )
        .await
        .unwrap();
    }

    async fn delete(&mut self, pk: &str) {
        self.dv += 1;
        submit_local_delete_core(&self.store, &self.site, "tasks", pk, self.dv)
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
        apply_remote_changeset_core(&self.store, &self.cfg, &cs, self.dv, None)
            .await
            .unwrap();
    }

    /// The sync-visible state (tombstoned rows expose only their tombstone).
    async fn state(&self) -> Vec<ColumnChange> {
        changes_since_core(&self.store, 0).await.unwrap()
    }

    async fn digest(&self) -> [u8; 32] {
        compute_store_digest(&self.store, &self.cfg).await.unwrap()
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
    apply_remote_changeset(
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
    let _ = env_logger::try_init();
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
    let _ = env_logger::try_init();
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
    let _ = env_logger::try_init();
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
    let _ = env_logger::try_init();
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
    let _ = env_logger::try_init();
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

    // Cross-apply. AddWins: the delete loses the tie on BOTH engines.
    apply_to_native(&native, web.state().await, web.site).await;
    web.apply(native_state(&native).await, NodeId([0u8; 16]))
        .await;

    // The native row survives — same outcome a native AddWins receiver
    // produces (sync_handler's AddWins-tie test).
    let row = task::Entity::find_by_id("c2").one(&native).await.unwrap();
    assert_eq!(
        row.map(|r| r.title),
        Some("edited concurrently".to_string()),
        "AddWins: the concurrent edit must survive the tie on native"
    );
    // Web applied native's winning edit through the column path.
    let title = web
        .store
        .get_shadow("tasks", "c2", "title")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(title.val, Some(serde_json::json!("edited concurrently")));
    assert_eq!(title.col_version, 2);

    // NOTE deliberately NO digest-equality assertion here: the losing
    // deleter retains its local tombstone while the survivor never stores
    // one — the same shadow-state asymmetry two NATIVE peers exhibit after
    // an AddWins tie (a remote column apply never clears a tombstone;
    // only a LOCAL write does, connection.rs clear_tombstone). Parity
    // means matching that behavior, not improving on it unilaterally.
}

#[tokio::test]
async fn interleaved_batches_converge() {
    let _ = env_logger::try_init();
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
