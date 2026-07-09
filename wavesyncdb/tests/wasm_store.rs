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

/// Multi-group #93 / issue-linked persistence: round-trip a `JoinedGroupRecord`
/// and rebuild the `GroupKey` from its stored bytes — proves the persisted
/// record reconstructs the SAME effective topic, which is the rejoin
/// invariant `web_engine::connect_persistent_with_config`'s rejoin path
/// depends on. Uses `GroupKey::from_raw` (not `from_passphrase`) as the
/// source key: a real Argon2id derivation costs seconds in a browser test,
/// and `from_raw` exercises the identical `to_bytes`/`derive_topic` round
/// trip without paying it.
#[wasm_bindgen_test]
async fn joined_group_record_shape_supports_rejoin() {
    let key = wavesyncdb::GroupKey::from_raw([9u8; 32]);
    let effective = key.derive_topic("house");
    let store = BrowserStore::open("wasmtest-rejoin").await.unwrap();
    store
        .record_joined_group(&wavesyncdb::web_store::JoinedGroupRecord {
            user_topic: "house".into(),
            effective_topic: effective.clone(),
            derived_key: key.to_bytes(),
            kind: None,
        })
        .await
        .unwrap();
    let loaded = store.load_joined_groups().await.unwrap();
    let rec = &loaded[0];
    let rebuilt = wavesyncdb::GroupKey::from_raw(rec.derived_key);
    assert_eq!(rebuilt.derive_topic(&rec.user_topic), rec.effective_topic);
    assert_eq!(rec.effective_topic, effective);
}

/// Multi-group (#93): a loopback client cannot host a second group — the
/// single-pair demo transport has no swarm, so `join_group` must fail fast
/// with `Unsupported`. This is the cheapest real-engine assertion available
/// without standing up a relay in the browser test; full join coverage is
/// the node e2e suite (Task 11).
#[wasm_bindgen_test]
async fn loopback_join_group_is_unsupported() {
    let pair = wavesyncdb::LoopbackPair::new();
    let client =
        wavesyncdb::WebSyncClient::connect_loopback(pair.a, "topic-x", None, "wasmtest-lb-join")
            .await
            .unwrap();
    // `WebGroupHandle` intentionally has no `Debug` (it holds a `Box<dyn Any>`
    // table cache), so match the `Result` directly rather than `unwrap_err`.
    let result = client.join_group("other-group", "pw", None).await;
    assert!(
        matches!(result, Err(wavesyncdb::WebSyncError::Unsupported)),
        "loopback join_group must be Unsupported"
    );
}
