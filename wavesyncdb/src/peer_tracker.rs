//! Peer tracking for watermark-based log compaction.
//!
//! Maintains a `_wavesync_peers` table that tracks known peers, their last-seen
//! timestamps, and their sync watermarks (the HLC time up to which they have
//! received all operations). The minimum watermark across all active peers
//! determines the safe compaction cutoff.

use sea_orm::{ConnectionTrait, DbErr, ExecResult, FromQueryResult, Statement};

/// Create the `_wavesync_peers` table if it does not already exist.
pub async fn create_peers_table(db: &impl ConnectionTrait) -> Result<ExecResult, DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS _wavesync_peers (
            peer_id     TEXT PRIMARY KEY,
            node_id     BLOB,
            watermark   INTEGER NOT NULL DEFAULT 0,
            last_seen   INTEGER NOT NULL,
            evicted     INTEGER NOT NULL DEFAULT 0
        )",
    )
    .await
}

/// Insert or update a peer record.
pub async fn upsert_peer(
    db: &impl ConnectionTrait,
    peer_id: &str,
    node_id: &[u8],
    watermark: u64,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO _wavesync_peers (peer_id, node_id, watermark, last_seen, evicted)
         VALUES ($1, $2, $3, $4, 0)
         ON CONFLICT(peer_id) DO UPDATE SET
            node_id = excluded.node_id,
            watermark = excluded.watermark,
            last_seen = excluded.last_seen,
            evicted = 0",
        [
            peer_id.into(),
            node_id.to_vec().into(),
            (watermark as i64).into(),
            (now as i64).into(),
        ],
    ))
    .await
}

/// Update a peer's watermark and last_seen timestamp.
pub async fn update_watermark(
    db: &impl ConnectionTrait,
    peer_id: &str,
    watermark: u64,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE _wavesync_peers SET watermark = $1, last_seen = $2 WHERE peer_id = $3",
        [
            (watermark as i64).into(),
            (now as i64).into(),
            peer_id.into(),
        ],
    ))
    .await
}

/// Update a peer's last_seen timestamp.
pub async fn update_last_seen(
    db: &impl ConnectionTrait,
    peer_id: &str,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE _wavesync_peers SET last_seen = $1 WHERE peer_id = $2",
        [(now as i64).into(), peer_id.into()],
    ))
    .await
}

#[derive(Debug, FromQueryResult)]
struct MinWatermark {
    min_wm: Option<i64>,
}

/// Get the minimum watermark across all non-evicted peers.
pub async fn get_min_watermark(db: &impl ConnectionTrait) -> Result<Option<u64>, DbErr> {
    let row = MinWatermark::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT MIN(watermark) as min_wm FROM _wavesync_peers WHERE evicted = 0",
        [],
    ))
    .one(db)
    .await?;

    Ok(row.and_then(|r| r.min_wm).map(|v| v as u64))
}

#[derive(Debug, FromQueryResult)]
struct PeerIdRow {
    peer_id: String,
}

/// Mark peers not seen in `timeout_secs` as evicted. Returns evicted peer IDs.
pub async fn evict_stale_peers(
    db: &impl ConnectionTrait,
    timeout_secs: u64,
) -> Result<Vec<String>, DbErr> {
    let cutoff = now_secs().saturating_sub(timeout_secs);

    // Find peers to evict
    let rows = PeerIdRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT peer_id FROM _wavesync_peers WHERE evicted = 0 AND last_seen < $1",
        [(cutoff as i64).into()],
    ))
    .all(db)
    .await?;

    let evicted: Vec<String> = rows.into_iter().map(|r| r.peer_id).collect();

    if !evicted.is_empty() {
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE _wavesync_peers SET evicted = 1 WHERE evicted = 0 AND last_seen < $1",
            [(cutoff as i64).into()],
        ))
        .await?;
    }

    Ok(evicted)
}

#[derive(Debug, FromQueryResult)]
struct CountRow {
    cnt: i64,
}

/// Check whether a peer exists and is not evicted.
pub async fn is_known_peer(db: &impl ConnectionTrait, peer_id: &str) -> Result<bool, DbErr> {
    let row = CountRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT COUNT(*) as cnt FROM _wavesync_peers WHERE peer_id = $1 AND evicted = 0",
        [peer_id.into()],
    ))
    .one(db)
    .await?;

    Ok(row.is_some_and(|r| r.cnt > 0))
}

/// Hard delete a peer record (for rejoining evicted peers).
pub async fn remove_peer(db: &impl ConnectionTrait, peer_id: &str) -> Result<ExecResult, DbErr> {
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "DELETE FROM _wavesync_peers WHERE peer_id = $1",
        [peer_id.into()],
    ))
    .await
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::Database;

    async fn setup_db() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        create_peers_table(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_upsert_and_query_peer() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 100).await.unwrap();
        assert!(is_known_peer(&db, "peer-1").await.unwrap());
        assert!(!is_known_peer(&db, "peer-2").await.unwrap());
    }

    #[tokio::test]
    async fn test_min_watermark() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 100).await.unwrap();
        upsert_peer(&db, "peer-2", &[2u8; 16], 50).await.unwrap();
        upsert_peer(&db, "peer-3", &[3u8; 16], 200).await.unwrap();
        let min = get_min_watermark(&db).await.unwrap();
        assert_eq!(min, Some(50));
    }

    #[tokio::test]
    async fn test_min_watermark_excludes_evicted() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 10).await.unwrap();
        upsert_peer(&db, "peer-2", &[2u8; 16], 100).await.unwrap();
        // Evict peer-1 by setting last_seen far in the past
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE _wavesync_peers SET last_seen = 0 WHERE peer_id = $1",
            ["peer-1".into()],
        ))
        .await
        .unwrap();
        evict_stale_peers(&db, 1).await.unwrap();
        let min = get_min_watermark(&db).await.unwrap();
        assert_eq!(min, Some(100));
    }

    #[tokio::test]
    async fn test_no_peers_returns_none() {
        let db = setup_db().await;
        let min = get_min_watermark(&db).await.unwrap();
        assert_eq!(min, None);
    }

    #[tokio::test]
    async fn test_update_watermark() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 50).await.unwrap();
        update_watermark(&db, "peer-1", 200).await.unwrap();
        let min = get_min_watermark(&db).await.unwrap();
        assert_eq!(min, Some(200));
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 50).await.unwrap();
        remove_peer(&db, "peer-1").await.unwrap();
        assert!(!is_known_peer(&db, "peer-1").await.unwrap());
    }

    #[tokio::test]
    async fn test_update_last_seen() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 100).await.unwrap();
        update_last_seen(&db, "peer-1").await.unwrap();
        assert!(is_known_peer(&db, "peer-1").await.unwrap());
    }

    #[tokio::test]
    async fn test_is_known_peer_evicted() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 100).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE _wavesync_peers SET last_seen = 0 WHERE peer_id = $1",
            ["peer-1".into()],
        ))
        .await
        .unwrap();
        evict_stale_peers(&db, 1).await.unwrap();
        assert!(!is_known_peer(&db, "peer-1").await.unwrap());
    }

    #[tokio::test]
    async fn test_evict_stale_peers_returns_ids() {
        let db = setup_db().await;
        upsert_peer(&db, "peer-1", &[1u8; 16], 100).await.unwrap();
        upsert_peer(&db, "peer-2", &[2u8; 16], 200).await.unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE _wavesync_peers SET last_seen = 0 WHERE peer_id = $1",
            ["peer-1".into()],
        ))
        .await
        .unwrap();
        db.execute_raw(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            "UPDATE _wavesync_peers SET last_seen = 0 WHERE peer_id = $1",
            ["peer-2".into()],
        ))
        .await
        .unwrap();
        let evicted = evict_stale_peers(&db, 1).await.unwrap();
        assert!(evicted.contains(&"peer-1".to_string()));
        assert!(evicted.contains(&"peer-2".to_string()));
        assert_eq!(evicted.len(), 2);
    }
}
