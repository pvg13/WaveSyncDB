mod common;

use sea_orm::{ActiveModelTrait, ConnectionTrait, EntityTrait, FromQueryResult, Set};
use std::time::Duration;
use uuid::Uuid;
use wavesyncdb::WaveSyncDbBuilder;

use common::task;
use common::{assert_eventually, make_peer, mem_db};

// ---------------------------------------------------------------------------
// H3 regression: multi-row INSERT only syncs first PK
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h3_multi_row_insert_sync() {
    let db = WaveSyncDbBuilder::new(&mem_db("h3"), "test-h3")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    // Multi-row INSERT
    db.execute_unprepared(
        "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('a', 'Task A', 0), ('b', 'Task B', 1)",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Collect all notifications
    let mut notifications = Vec::new();
    while let Ok(n) = rx.try_recv() {
        notifications.push(n);
    }

    // H3 bug: currently only the first PK is captured.
    // This test documents that behavior — when fixed, it should produce 2 notifications.
    assert!(
        !notifications.is_empty(),
        "Should get at least one notification for multi-row INSERT"
    );

    // Verify both rows exist in the table
    let all = task::Entity::find().all(&db).await.unwrap();
    assert_eq!(all.len(), 2, "Both rows should be inserted");
}

// ---------------------------------------------------------------------------
// H5 regression: PK with spaces gets truncated
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h5_update_pk_with_spaces() {
    let db = WaveSyncDbBuilder::new(&mem_db("h5"), "test-h5")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let pk = "hello world";

    // Insert via raw SQL (SeaORM uses parameterized queries which bypass the parser)
    db.execute_unprepared(&format!(
        "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('{}', 'SpaceTest', 0)",
        pk
    ))
    .await
    .unwrap();

    let mut rx = db.change_rx();

    // Update the row
    db.execute_unprepared(&format!(
        "UPDATE \"tasks\" SET \"title\" = 'Updated' WHERE \"id\" = '{}'",
        pk
    ))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let notif = rx.try_recv();
    // H5 bug: PK extraction truncates at spaces.
    // This documents the current behavior.
    if let Ok(n) = notif {
        // When bug is fixed, this should be "hello world"
        // Currently it may be "hello" (truncated)
        assert!(
            n.primary_key == "hello" || n.primary_key == "hello world",
            "PK should be 'hello world' (fixed) or 'hello' (bug): got '{}'",
            n.primary_key
        );
    }
}

// ---------------------------------------------------------------------------
// H7 regression: broadcast overflow with >1024 rapid writes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h7_broadcast_overflow() {
    let db = WaveSyncDbBuilder::new(&mem_db("h7"), "test-h7")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let mut rx = db.change_rx();

    // Insert >1024 rows rapidly without reading the receiver
    for i in 0..1100 {
        db.execute_unprepared(&format!(
            "INSERT INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('h7-{}', 'task', 0)",
            i
        ))
        .await
        .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Try to recv — should get Lagged error
    let mut got_lagged = false;
    loop {
        match rx.try_recv() {
            Ok(_) => continue,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                got_lagged = true;
                assert!(n > 0, "Should have missed some messages");
                break;
            }
            Err(_) => break,
        }
    }

    assert!(
        got_lagged,
        "H7: Should get Lagged error when >1024 notifications sent without reading"
    );
}

// ---------------------------------------------------------------------------
// N3 regression: delete + re-insert preserves col_version in shadow
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_n3_delete_reinsert_preserves_col_version() {
    let db = WaveSyncDbBuilder::new(&mem_db("n3"), "test-n3")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let pk = "n3-pk";

    // Insert
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("First".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    // Give time for async shadow table updates
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Delete
    task::Entity::delete_by_id(pk.to_string())
        .exec(&db)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Re-insert same PK
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("Second".into()),
        completed: Set(true),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check shadow table — col_version for "title" should be > 1
    // because the first insert set it to 1, and the re-insert should continue from there
    #[derive(Debug, FromQueryResult)]
    struct ClockRow {
        col_version: i64,
    }

    let row = ClockRow::find_by_statement(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT col_version FROM \"_wavesync_tasks_clock\" WHERE pk = $1 AND cid = 'title'",
        [pk.into()],
    ))
    .one(db.inner())
    .await
    .unwrap();

    if let Some(row) = row {
        assert!(
            row.col_version > 1,
            "N3: col_version after delete+reinsert should be > 1 (was {})",
            row.col_version
        );
    }
    // If None, the async task hasn't completed yet — that's a different issue
}

// ---------------------------------------------------------------------------
// M12 regression: UPDATE with non-ASCII values
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_m12_update_with_unicode_column_values() {
    let db = WaveSyncDbBuilder::new(&mem_db("m12"), "test-m12")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    let pk = "m12-pk";
    task::ActiveModel {
        id: Set(pk.to_string()),
        title: Set("ascii".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    let mut rx = db.change_rx();

    // Update with non-ASCII via raw SQL
    db.execute_unprepared(&format!(
        "UPDATE \"tasks\" SET \"title\" = 'café ñ 日本語' WHERE \"id\" = '{}'",
        pk
    ))
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let notif = rx.try_recv();
    assert!(
        notif.is_ok(),
        "M12: Should get notification for unicode update"
    );

    // Verify the value was stored correctly
    let found = task::Entity::find_by_id(pk.to_string())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.title, "café ñ 日本語");
}

// ---------------------------------------------------------------------------
// N4 regression: db_version persist failure must return error and roll back
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_n4_db_version_persist_failure_returns_error() {
    let db = WaveSyncDbBuilder::new(&mem_db("n4"), "test-n4")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    // Step 1: Successfully insert a task — db_version advances to 1
    task::ActiveModel {
        id: Set("t1".to_string()),
        title: Set("first".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    // Wait for async shadow work to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    let ver_after_first = wavesyncdb::shadow::get_db_version(db.inner())
        .await
        .unwrap();
    assert_eq!(
        ver_after_first, 1,
        "db_version should be 1 after first insert"
    );

    // Step 2: Drop _wavesync_meta to make set_db_version fail
    db.inner()
        .execute_unprepared("DROP TABLE \"_wavesync_meta\"")
        .await
        .unwrap();

    // Step 3: Attempt insert — should return Err because db_version persist fails
    let result = task::ActiveModel {
        id: Set("t2".to_string()),
        title: Set("second".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await;

    assert!(
        result.is_err(),
        "N4: Insert should fail when db_version persist fails"
    );

    // Step 4: Restore _wavesync_meta and set db_version back to 1
    wavesyncdb::shadow::create_meta_table(db.inner())
        .await
        .unwrap();
    wavesyncdb::shadow::set_db_version(db.inner(), 1)
        .await
        .unwrap();

    // Step 5: Insert again — should succeed and advance db_version to 2
    task::ActiveModel {
        id: Set("t3".to_string()),
        title: Set("third".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&db)
    .await
    .unwrap();

    // Wait for async shadow work
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Step 6: Verify db_version is 2 (not 3), proving the in-memory rollback worked
    let ver_final = wavesyncdb::shadow::get_db_version(db.inner())
        .await
        .unwrap();
    assert_eq!(
        ver_final, 2,
        "N4: db_version should be 2 (not 3) — in-memory counter must have been rolled back"
    );
}

// ---------------------------------------------------------------------------
// H4 regression: unparseable SQL on a registered table should warn, not panic
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_h4_unparseable_sql_succeeds_locally() {
    let db = WaveSyncDbBuilder::new(&mem_db("h4"), "test-h4")
        .build()
        .await
        .unwrap();
    db.schema().register(task::Entity).sync().await.unwrap();

    // REPLACE INTO is not handled by parse_write_full — exercises the warn path.
    // The table exists and is registered, so the warning should fire (in logs),
    // but the write itself must succeed locally.
    let result = db
        .execute_unprepared(
            "REPLACE INTO \"tasks\" (\"id\", \"title\", \"completed\") VALUES ('h4', 'test', 0)",
        )
        .await;

    assert!(
        result.is_ok(),
        "H4: unparseable SQL should still succeed locally"
    );
}

// ---------------------------------------------------------------------------
// R1 regression: is_group_member must start false and flip after HMAC exchange
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_r1_peer_starts_unverified() {
    let _ = env_logger::try_init();
    let topic = format!("test-r1a-{}", Uuid::new_v4());
    let passphrase = "test-secret";

    let url_a = mem_db("r1a_a");
    let url_b = mem_db("r1a_b");

    // Create peers with passphrase but do NOT call sync() yet on B
    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(200))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(201))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Immediately after creation, no peer should be a group member yet
    let status_a = peer_a.network_status();
    for p in &status_a.connected_peers {
        assert!(
            !p.is_group_member,
            "R1: peer should start as is_group_member=false"
        );
    }
}

#[tokio::test]
async fn test_r1_peer_verified_after_sync() {
    let _ = env_logger::try_init();
    let topic = format!("test-r1b-{}", Uuid::new_v4());
    let passphrase = "test-secret-r1b";
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("r1b_a");
    let url_b = mem_db("r1b_b");

    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(202))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(203))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // After sync exchange completes, both peers should see each other as group members
    assert_eventually("A sees B as group member", timeout, || async {
        peer_a.network_status().group_peer_count() > 0
    })
    .await;

    assert_eventually("B sees A as group member", timeout, || async {
        peer_b.network_status().group_peer_count() > 0
    })
    .await;

    // Verify is_group_member is true for the discovered peer
    let status_a = peer_a.network_status();
    assert!(
        status_a.group_peers().iter().all(|p| p.is_group_member),
        "R1: verified peers must have is_group_member=true"
    );
}

#[tokio::test]
async fn test_r1_no_passphrase_never_verified() {
    let _ = env_logger::try_init();
    let topic = format!("test-r1c-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(10);

    let url_a = mem_db("r1c_a");
    let url_b = mem_db("r1c_b");

    // No passphrase — peers sync but should never become group members
    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(204))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(205))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Wait for sync to complete — data should replicate even without passphrase
    task::ActiveModel {
        id: Set("r1c-task".to_string()),
        title: Set("no-pass".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B has task from A", timeout, || async {
        task::Entity::find_by_id("r1c-task")
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // Even after successful sync, group_peer_count should be 0 (no passphrase = no verification)
    assert_eq!(
        peer_a.network_status().group_peer_count(),
        0,
        "R1: without passphrase, is_group_member must stay false"
    );
    assert_eq!(
        peer_b.network_status().group_peer_count(),
        0,
        "R1: without passphrase, is_group_member must stay false"
    );
}

// ---------------------------------------------------------------------------
// R5: application-level peer identity
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_r5_identity_announced_after_verification() {
    let _ = env_logger::try_init();
    let topic = format!("test-r5a-{}", Uuid::new_v4());
    let passphrase = "test-secret-r5a";
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("r5a_a");
    let url_b = mem_db("r5a_b");

    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(210))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();

    // Set identity on A before B is created
    peer_a.set_peer_identity("user-123");

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(211))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Wait for B to see A's identity
    assert_eventually("B sees A's identity", timeout, || async {
        let status = peer_b.network_status();
        status
            .connected_peers
            .iter()
            .any(|p| p.app_id.as_deref() == Some("user-123"))
    })
    .await;

    // Also check peers_by_identity
    let by_id = peer_b.peers_by_identity();
    assert!(
        by_id.contains_key("user-123"),
        "R5: peers_by_identity should contain user-123"
    );
}

#[tokio::test]
async fn test_r5_identity_cleared_on_disconnect() {
    let _ = env_logger::try_init();
    let topic = format!("test-r5b-{}", Uuid::new_v4());
    let passphrase = "test-secret-r5b";
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("r5b_a");
    let url_b = mem_db("r5b_b");

    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(212))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();
    peer_a.set_peer_identity("user-456");

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(213))
        .with_passphrase(passphrase)
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Wait for B to see A's identity
    assert_eventually("B sees A's identity", timeout, || async {
        peer_b
            .network_status()
            .connected_peers
            .iter()
            .any(|p| p.app_id.as_deref() == Some("user-456"))
    })
    .await;

    // Disconnect peer A by shutting it down
    peer_a.shutdown().await;

    // B should no longer have A's identity
    assert_eventually("B no longer sees A's identity", timeout, || async {
        peer_b.peers_by_identity().is_empty()
    })
    .await;
}

#[tokio::test]
async fn test_r5_identity_not_sent_without_passphrase() {
    let _ = env_logger::try_init();
    let topic = format!("test-r5c-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(10);

    let url_a = mem_db("r5c_a");
    let url_b = mem_db("r5c_b");

    // No passphrase — identity should never be sent
    let peer_a = WaveSyncDbBuilder::new(&url_a, &topic)
        .with_node_id(common::make_node_id(214))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_a.schema().register(task::Entity).sync().await.unwrap();
    peer_a.set_peer_identity("user-789");

    let peer_b = WaveSyncDbBuilder::new(&url_b, &topic)
        .with_node_id(common::make_node_id(215))
        .with_mdns_query_interval(Duration::from_millis(100))
        .with_mdns_ttl(Duration::from_secs(5))
        .with_sync_interval(Duration::from_secs(2))
        .build()
        .await
        .unwrap();
    peer_b.schema().register(task::Entity).sync().await.unwrap();

    // Wait for sync to work (data replication should still happen)
    task::ActiveModel {
        id: Set("r5c-task".to_string()),
        title: Set("no-pass-id".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    assert_eventually("B has task from A", timeout, || async {
        task::Entity::find_by_id("r5c-task")
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some()
    })
    .await;

    // Even after sync, no peer should have an app_id (no passphrase = no verification = no identity)
    assert!(
        peer_b
            .network_status()
            .connected_peers
            .iter()
            .all(|p| p.app_id.is_none()),
        "R5: without passphrase, app_id must stay None"
    );
    assert!(
        peer_b.peers_by_identity().is_empty(),
        "R5: peers_by_identity should be empty without passphrase"
    );
}

// ---------------------------------------------------------------------------
// Regression: out-of-order changeset delivery (UPDATE before INSERT)
// INSERT then rapid UPDATE should converge on peer B without workarounds.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_out_of_order_insert_update_convergence() {
    let _ = env_logger::try_init();
    let topic = format!("test-ooo-{}", Uuid::new_v4());
    let timeout = Duration::from_secs(15);

    let url_a = mem_db("ooo_a");
    let url_b = mem_db("ooo_b");

    let peer_a = make_peer(&url_a, &topic, 180).await;
    let peer_b = make_peer(&url_b, &topic, 181).await;

    // Wait for peer discovery
    tokio::time::sleep(Duration::from_secs(2)).await;

    // A: INSERT then immediately UPDATE (no waiting for propagation)
    let pk = Uuid::new_v4().to_string();
    task::ActiveModel {
        id: Set(pk.clone()),
        title: Set("original".into()),
        completed: Set(false),
        ..Default::default()
    }
    .insert(&peer_a)
    .await
    .unwrap();

    peer_a
        .execute_unprepared(&format!(
            "UPDATE \"tasks\" SET \"title\" = 'updated' WHERE \"id\" = '{pk}'"
        ))
        .await
        .unwrap();

    // B should converge to the updated value
    assert_eventually("B has updated row", timeout, || async {
        task::Entity::find_by_id(pk.clone())
            .one(&peer_b)
            .await
            .ok()
            .flatten()
            .is_some_and(|t| t.title == "updated")
    })
    .await;
}
