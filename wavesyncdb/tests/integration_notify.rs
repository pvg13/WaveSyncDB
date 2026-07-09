// Native-only (real engine, SQLite, libp2p). `wasm-pack test` runs `cargo
// build --tests`, which builds every integration test binary in the crate
// regardless of which one is named to run — without this gate, this file's
// native-only imports would fail a wasm32 test build.
#![cfg(not(target_arch = "wasm32"))]

mod common;

use std::time::Duration;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, Set};
use uuid::Uuid;
use wavesyncdb::{Notification, WaveSyncDb, WaveSyncDbBuilder};

use common::{assert_eventually, make_node_id, mem_db};

// ---------------------------------------------------------------------------
// Per-table notification policy (seeds 200-219).
//
// A `msg` entity declares — via `#[derive(SyncNotify)]` + `impl SyncNotify` —
// that only *inserts* should notify. We verify the three guarantees the feature
// makes:
//   1. A remote insert produces exactly one Notification on the receiver.
//   2. The receiver's OWN local insert produces zero notifications (remote-only).
//   3. A remote update produces zero notifications (the policy is insert-only),
//      even though a raw change still flows on `change_rx`.
// ---------------------------------------------------------------------------
mod msg {
    use sea_orm::entity::prelude::*;
    use wavesyncdb::{Notification, SyncEvent, WriteKind};
    use wavesyncdb_derive::{SyncEntity, SyncNotify};

    #[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity, SyncNotify)]
    #[sea_orm(table_name = "msgs")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: String,
        pub text: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}

    // Insert-only policy: surface a notification when a new message arrives,
    // stay silent on edits and deletes.
    impl wavesyncdb::SyncNotify for Model {
        fn on_sync(ev: &SyncEvent<Self>) -> Option<Notification> {
            match ev.op {
                WriteKind::Insert => ev
                    .row
                    .as_ref()
                    .map(|m| Notification::new("New message", &m.text)),
                _ => None,
            }
        }
    }
}

async fn make_msg_peer(db_url: &str, topic: &str, seed: u8) -> WaveSyncDb {
    let peer = WaveSyncDbBuilder::new(db_url, topic)
        .with_node_id(make_node_id(seed))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .expect("failed to build peer");
    peer.schema()
        .register(msg::Entity)
        .sync()
        .await
        .expect("failed to sync schema");
    peer
}

/// Drain all currently-available notifications from a receiver (non-blocking).
fn drain(rx: &mut tokio::sync::broadcast::Receiver<Notification>) -> Vec<Notification> {
    let mut out = Vec::new();
    while let Ok(n) = rx.try_recv() {
        out.push(n);
    }
    out
}

#[tokio::test]
async fn test_remote_insert_notifies_local_does_not_and_update_is_silent() {
    common::init_test_tracing();
    let topic = format!("test-notify-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(20);

    let peer_a = make_msg_peer(&mem_db("notify_a"), &topic, 200).await;
    let peer_b = make_msg_peer(&mem_db("notify_b"), &topic, 201).await;

    // B watches for user notifications.
    let mut notif_rx = peer_b.notification_rx();

    // (1) A inserts a message → B should get exactly one "New message".
    msg::ActiveModel {
        id: Set("a1".to_string()),
        text: Set("hello from A".to_string()),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B has the row from A", timeout, || async {
        msg::Entity::find_by_id("a1".to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // Give the notification a beat to propagate after the row applied.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let got = drain(&mut notif_rx);
    assert_eq!(
        got.len(),
        1,
        "expected exactly one notification, got {got:?}"
    );
    assert_eq!(got[0].title, "New message");
    assert_eq!(got[0].body, "hello from A");
    assert_eq!(got[0].table, "msgs");
    assert_eq!(got[0].primary_key, "a1");

    // (2) B's OWN local insert must NOT notify B (remote-only policy).
    msg::ActiveModel {
        id: Set("b1".to_string()),
        text: Set("typed locally".to_string()),
        ..Default::default()
    }
    .insert(&peer_b)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let local = drain(&mut notif_rx);
    assert!(
        local.is_empty(),
        "local write must not notify the local user, got {local:?}"
    );

    // (3) A updates the existing row → policy is insert-only → no notification,
    //     even though the raw change still flows through change_rx.
    peer_a
        .execute_unprepared("UPDATE \"msgs\" SET \"text\" = 'edited by A' WHERE \"id\" = 'a1'")
        .await
        .unwrap();

    assert_eventually("B sees the edit", timeout, || async {
        msg::Entity::find_by_id("a1".to_string())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|m| m.text == "edited by A")
    })
    .await;

    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_update = drain(&mut notif_rx);
    assert!(
        after_update.is_empty(),
        "an insert-only policy must stay silent on updates, got {after_update:?}"
    );
}
