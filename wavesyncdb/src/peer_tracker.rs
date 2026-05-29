//! Peer version tracking for db_version-based sync.
//!
//! Maintains a `_wavesync_peer_versions` table that tracks known peers and
//! their last-known `db_version`. This allows efficient incremental sync:
//! when a peer reconnects, we only send changes since their last known version.

use std::collections::HashMap;

use sea_orm::{ConnectionTrait, DbErr, ExecResult, FromQueryResult, Statement};

use crate::messages::NodeId;

/// Create the `_wavesync_peer_versions` table if it does not already exist.
pub async fn create_peer_versions_table(db: &impl ConnectionTrait) -> Result<ExecResult, DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS _wavesync_peer_versions (
            peer_id     TEXT PRIMARY KEY,
            site_id     BLOB,
            db_version  INTEGER NOT NULL DEFAULT 0,
            last_seen   INTEGER NOT NULL
        )",
    )
    .await
}

/// Insert or update a peer's version information.
pub async fn upsert_peer_version(
    db: &impl ConnectionTrait,
    peer_id: &str,
    site_id: &NodeId,
    db_version: u64,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO _wavesync_peer_versions (peer_id, site_id, db_version, last_seen)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT(peer_id) DO UPDATE SET
            site_id = excluded.site_id,
            db_version = excluded.db_version,
            last_seen = excluded.last_seen",
        [
            peer_id.into(),
            site_id.0.to_vec().into(),
            (db_version as i64).into(),
            (now as i64).into(),
        ],
    ))
    .await
}

/// Get the last known db_version for a specific peer.
pub async fn get_peer_version(
    db: &impl ConnectionTrait,
    peer_id: &str,
) -> Result<Option<u64>, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct VersionRow {
        db_version: i64,
    }

    let row = VersionRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT db_version FROM _wavesync_peer_versions WHERE peer_id = $1",
        [peer_id.into()],
    ))
    .one(db)
    .await?;

    Ok(row.map(|r| r.db_version as u64))
}

/// Get all peer versions as a map.
pub async fn get_all_peer_versions(
    db: &impl ConnectionTrait,
) -> Result<HashMap<String, u64>, DbErr> {
    #[derive(Debug, FromQueryResult)]
    struct PeerVersionRow {
        peer_id: String,
        db_version: i64,
    }

    let rows = PeerVersionRow::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT peer_id, db_version FROM _wavesync_peer_versions",
        [],
    ))
    .all(db)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| (r.peer_id, r.db_version as u64))
        .collect())
}

/// Parse a raw `(peer_id string -> db_version)` map (as returned by
/// [`get_all_peer_versions`]) into libp2p [`PeerId`](libp2p::PeerId)s, used to
/// hydrate the engine's in-memory peer-version map at startup. Rows whose
/// `peer_id` does not parse (e.g. left over from before persistent keypairs)
/// are skipped with a debug log rather than failing the whole hydration.
pub fn parse_peer_versions(raw: HashMap<String, u64>) -> HashMap<libp2p::PeerId, u64> {
    raw.into_iter()
        .filter_map(
            |(peer_id, version)| match peer_id.parse::<libp2p::PeerId>() {
                Ok(pid) => Some((pid, version)),
                Err(e) => {
                    log::debug!(
                        "Skipping unparseable peer_id '{peer_id}' during version hydration: {e}"
                    );
                    None
                }
            },
        )
        .collect()
}

/// Update the last_seen timestamp for a peer.
pub async fn update_last_seen(
    db: &impl ConnectionTrait,
    peer_id: &str,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE _wavesync_peer_versions SET last_seen = $1 WHERE peer_id = $2",
        [(now as i64).into(), peer_id.into()],
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
    use crate::messages::NodeId;
    use sea_orm::Database;

    async fn setup_db() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        create_peer_versions_table(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn test_upsert_and_get_peer_version() {
        let db = setup_db().await;
        let site_id = NodeId([1u8; 16]);
        upsert_peer_version(&db, "peer-1", &site_id, 42)
            .await
            .unwrap();
        let version = get_peer_version(&db, "peer-1").await.unwrap();
        assert_eq!(version, Some(42));
    }

    #[tokio::test]
    async fn test_get_peer_version_missing() {
        let db = setup_db().await;
        let version = get_peer_version(&db, "nonexistent").await.unwrap();
        assert_eq!(version, None);
    }

    #[tokio::test]
    async fn test_upsert_updates_version() {
        let db = setup_db().await;
        let site_id = NodeId([1u8; 16]);
        upsert_peer_version(&db, "peer-1", &site_id, 10)
            .await
            .unwrap();
        upsert_peer_version(&db, "peer-1", &site_id, 50)
            .await
            .unwrap();
        let version = get_peer_version(&db, "peer-1").await.unwrap();
        assert_eq!(version, Some(50));
    }

    #[tokio::test]
    async fn test_get_all_peer_versions() {
        let db = setup_db().await;
        let site_a = NodeId([1u8; 16]);
        let site_b = NodeId([2u8; 16]);
        upsert_peer_version(&db, "peer-1", &site_a, 10)
            .await
            .unwrap();
        upsert_peer_version(&db, "peer-2", &site_b, 20)
            .await
            .unwrap();

        let versions = get_all_peer_versions(&db).await.unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions["peer-1"], 10);
        assert_eq!(versions["peer-2"], 20);
    }

    #[test]
    fn test_parse_peer_versions_skips_unparseable() {
        // A real libp2p PeerId string parses; junk is skipped, not fatal.
        let real = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
            .to_string();
        let mut raw = HashMap::new();
        raw.insert(real.clone(), 7u64);
        raw.insert("not-a-peer-id".to_string(), 99u64);

        let parsed = parse_peer_versions(raw);

        assert_eq!(parsed.len(), 1, "the unparseable row must be dropped");
        let pid: libp2p::PeerId = real.parse().unwrap();
        assert_eq!(parsed[&pid], 7);
    }

    #[tokio::test]
    async fn test_get_all_peer_versions_roundtrips_into_parsed_map() {
        // End-to-end: write a real PeerId's version, read it back, and confirm
        // it survives the String -> PeerId hydration mapping.
        let db = setup_db().await;
        let pid = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        upsert_peer_version(&db, &pid.to_string(), &NodeId([3u8; 16]), 123)
            .await
            .unwrap();

        let parsed = parse_peer_versions(get_all_peer_versions(&db).await.unwrap());
        assert_eq!(parsed[&pid], 123);
    }

    #[tokio::test]
    async fn test_update_last_seen() {
        let db = setup_db().await;
        let site_id = NodeId([1u8; 16]);
        upsert_peer_version(&db, "peer-1", &site_id, 42)
            .await
            .unwrap();
        update_last_seen(&db, "peer-1").await.unwrap();
        // Just verify it doesn't error; timestamp is wall-clock
        let version = get_peer_version(&db, "peer-1").await.unwrap();
        assert_eq!(version, Some(42));
    }
}
