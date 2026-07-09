//! Browser-only IndexedDB tests, run via `wasm-pack test --headless --chrome`.
//! These exercise the wasm-only storage layer (`BrowserStore`) that native
//! `cargo test` can never execute — until this suite, `web_store.rs` and
//! `web_engine.rs` only ever got `cargo check`ed for wasm32, never run.
//!
//! Run with:
//! `cd wavesyncdb && wasm-pack test --headless --chrome --features web --test wasm_store`
#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;
wasm_bindgen_test_configure!(run_in_browser);

use wavesyncdb::web_sync_core::{ShadowStore, WriteBatch};
use wavesyncdb::{BrowserStore, ShadowRow};

/// Build a shadow row with fixed clock fields; only `deleted_ts` and
/// `db_version` vary per call site. `val` is left `None` — none of these
/// tests assert on the stored value, only on presence/absence of the row.
fn row(deleted_ts: Option<u64>, db_version: u64) -> ShadowRow {
    ShadowRow {
        val: None,
        site_id: [7u8; 16],
        col_version: 1,
        cl: 1,
        seq: 0,
        db_version,
        deleted_ts,
    }
}

#[wasm_bindgen_test]
async fn batch_is_atomic() {
    // Distinct store name per test — IndexedDB DBs persist for the whole
    // browser session, so reusing a name would leak state across tests.
    let store = BrowserStore::open("wasmtest-atomic").await.unwrap();
    let mut batch = WriteBatch::default();
    batch.db_version = Some(3);
    batch
        .shadow_puts
        .push(("t".into(), "pk1".into(), "col".into(), row(None, 3)));

    store.apply_batch(batch).await.unwrap();

    assert!(
        store.get_shadow("t", "pk1", "col").await.unwrap().is_some(),
        "a committed batch must persist its shadow put"
    );
}

#[wasm_bindgen_test]
async fn tombstone_reaper_respects_cutoff() {
    let store = BrowserStore::open("wasmtest-gc").await.unwrap();
    let mut batch = WriteBatch::default();
    // Aged: stamped well before the cutoff -> reaped.
    batch.shadow_puts.push((
        "t".into(),
        "aged".into(),
        "__deleted".into(),
        row(Some(100), 1),
    ));
    // Live: stamped after the cutoff -> spared.
    batch.shadow_puts.push((
        "t".into(),
        "live".into(),
        "__deleted".into(),
        row(Some(10_000), 2),
    ));
    // Unstamped: no deleted_ts -> never ages, always spared.
    batch.shadow_puts.push((
        "t".into(),
        "nostamp".into(),
        "__deleted".into(),
        row(None, 3),
    ));
    store.apply_batch(batch).await.unwrap();

    let reaped = store.gc_aged_tombstones(5_000).await.unwrap();
    assert_eq!(reaped, 1, "exactly the aged stamped tombstone is reaped");

    assert!(
        store
            .get_shadow("t", "aged", "__deleted")
            .await
            .unwrap()
            .is_none(),
        "aged tombstone must be physically deleted"
    );
    assert!(
        store
            .get_shadow("t", "live", "__deleted")
            .await
            .unwrap()
            .is_some(),
        "live tombstone (stamped after cutoff) must be spared"
    );
    assert!(
        store
            .get_shadow("t", "nostamp", "__deleted")
            .await
            .unwrap()
            .is_some(),
        "unstamped tombstone must never age out"
    );
}

#[wasm_bindgen_test]
async fn joined_groups_roundtrip_and_v3_upgrade() {
    // opening is itself the v2->v3 upgrade path (idb machinery is additive)
    let store = BrowserStore::open("wasmtest-groups").await.unwrap();
    let rec = wavesyncdb::web_store::JoinedGroupRecord {
        user_topic: "house".into(),
        effective_topic: "wavesync2-abc".into(),
        derived_key: [7u8; 32],
        kind: Some("household".into()),
    };
    store.record_joined_group(&rec).await.unwrap();
    // upsert: same user_topic replaces, not duplicates
    store.record_joined_group(&rec).await.unwrap();
    let loaded = store.load_joined_groups().await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].derived_key, [7u8; 32]);
    assert_eq!(loaded[0].kind.as_deref(), Some("household"));
    store.remove_joined_group("house").await.unwrap();
    assert!(store.load_joined_groups().await.unwrap().is_empty());
}

#[wasm_bindgen_test]
async fn group_store_names_are_isolated() {
    use wavesyncdb::web_store::group_store_name;
    let a = BrowserStore::open(&group_store_name("iso", "wavesync2-aaa"))
        .await
        .unwrap();
    let b = BrowserStore::open(&group_store_name("iso", "wavesync2-bbb"))
        .await
        .unwrap();
    // write a shadow row in a; assert absent in b
    let mut batch = WriteBatch::default();
    batch
        .shadow_puts
        .push(("t".into(), "p".into(), "c".into(), row(None, 1)));
    a.apply_batch(batch).await.unwrap();
    assert!(a.get_shadow("t", "p", "c").await.unwrap().is_some());
    assert!(b.get_shadow("t", "p", "c").await.unwrap().is_none());
}

#[wasm_bindgen_test]
async fn peer_addr_roundtrip() {
    let store = BrowserStore::open("wasmtest-addrs").await.unwrap();
    store
        .record_peer_address_success("12D3KooWpeer", "/dns4/x/tcp/443/wss")
        .await
        .unwrap();

    // Generous age/fail-count bounds — this test only asserts the
    // just-written record round-trips, not the filter's edge behavior.
    let loaded = store.load_recent_peer_addresses(3600, 5).await.unwrap();
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].peer_id, "12D3KooWpeer");
    assert_eq!(loaded[0].multiaddr, "/dns4/x/tcp/443/wss");
    assert_eq!(loaded[0].fail_count, 0);
}
