//! Successful-peer-address cache for fast cold-start dialing.
//!
//! Persists `(peer_id, multiaddr)` pairs the engine has talked to before
//! into `_wavesync_peer_addrs`, so the next process start can dial them
//! directly instead of waiting on the full mDNS / rendezvous /
//! relay-PeerList discovery cycle. Closes [issue #29].
//!
//! ## Why this exists
//!
//! Cold-start sync today: launch → dial relay → `AnnouncePresence` →
//! `PeerList` round-trip → dial each → first sync. On cellular this is
//! 5–30s. The peers we sync with are almost always the same ones we
//! synced with last time, so caching their addresses lets us start the
//! dial *in parallel with* discovery and finish typically <1s after
//! launch when the cache is fresh.
//!
//! Universal pattern in production P2P: Bitcoin `peers.dat`, Syncthing
//! `addressbook`, Tailscale `state.json`, Tor cached consensus. The
//! Syncthing team's published experience is that this single change is
//! worth more than every other discovery improvement combined for
//! day-2 user-perceived latency.
//!
//! ## What we cache (and what we don't)
//!
//! * **One row per `(peer_id, multiaddr)` pair.** A peer with multiple
//!   reachable addresses (LAN + circuit-relay + DCUtR-upgraded direct)
//!   has multiple rows; the engine fires them all in parallel at next
//!   startup and lets libp2p race them. Only *dialable* multiaddrs are
//!   stored — a bare `/p2p/<id>` remote address (as surfaced by some
//!   upgraded connections) is rejected at [`record_success`].
//! * **Last-success / last-attempt timestamps + consecutive-failure
//!   counter.** Enough to drive a recency filter and a future per-peer
//!   circuit breaker (#34) without retaining a full history.
//!   `fail_count` grows **per address**, and the engine only records a
//!   failure when the peer is simultaneously connected via another path
//!   — the one signal that distinguishes "this address is stale" from
//!   "the peer is asleep". A sleeping phone fails on ALL its addresses;
//!   counting that would erase its whole cache within ~10 push wakes
//!   (the normal mobile rhythm). Rows that never accumulate failures
//!   leave via the 7-day age GC instead.
//! * **No reputation / scoring beyond age and failure count.** Bitcoin's
//!   AddrMan tries / new buckets are overkill for our scale (typically
//!   under 20 peers per topic). If we ever get there, the schema is
//!   forward-compatible.
//! * **No infrastructure peers.** The engine's `infrastructure_peers`
//!   set (relay, rendezvous) writes go through different paths and the
//!   relay multiaddr is already in `EngineConfig::relay_server`.
//! * **No cross-process coordination.** Each engine instance owns its
//!   own SQLite; no shared cache between processes.
//!
//! [issue #29]: https://github.com/pvg13/WaveSyncDB/issues/29

use sea_orm::{ConnectionTrait, DbErr, ExecResult, FromQueryResult, Statement};

/// Create the `_wavesync_peer_addrs` table if it does not already exist.
///
/// Called once at engine startup, alongside `_wavesync_meta` and
/// `_wavesync_peer_versions`.
pub async fn create_peer_addrs_table(db: &impl ConnectionTrait) -> Result<ExecResult, DbErr> {
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS _wavesync_peer_addrs (
            peer_id     TEXT NOT NULL,
            multiaddr   TEXT NOT NULL,
            last_ok_at  INTEGER NOT NULL,
            last_try_at INTEGER NOT NULL,
            fail_count  INTEGER NOT NULL,
            PRIMARY KEY (peer_id, multiaddr)
        )",
    )
    .await?;
    db.execute_unprepared(
        "CREATE INDEX IF NOT EXISTS _wavesync_peer_addrs_ok \
            ON _wavesync_peer_addrs(last_ok_at DESC)",
    )
    .await
}

/// A multiaddr worth caching: it must parse and contain at least one
/// non-`/p2p/` component (a transport we can dial later). Hole-punched /
/// upgraded connections can surface bare `/p2p/<id>` remote addresses;
/// caching those wastes a cold-start pre-dial on a guaranteed
/// "Unsupported resolved address" failure — and pre-dial failures are
/// exactly what start the dial backoff that suppresses introduction
/// dials (PROBLEMS N14).
pub fn is_dialable_multiaddr(addr: &str) -> bool {
    let Ok(ma) = addr.parse::<libp2p::Multiaddr>() else {
        return false;
    };
    ma.iter()
        .any(|p| !matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
}

/// Record a successful connection / message exchange for `(peer_id, multiaddr)`.
///
/// Sets both `last_ok_at` and `last_try_at` to now, and resets the
/// consecutive-failure counter to 0. Insert if the row doesn't exist
/// (a peer/address pair we haven't seen before). Silently skips
/// addresses that could never be re-dialed (see
/// [`is_dialable_multiaddr`]).
pub async fn record_success(
    db: &impl ConnectionTrait,
    peer_id: &str,
    multiaddr: &str,
) -> Result<(), DbErr> {
    if !is_dialable_multiaddr(multiaddr) {
        tracing::debug!("peer_addrs: skipping non-dialable multiaddr {multiaddr}");
        return Ok(());
    }
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO _wavesync_peer_addrs (peer_id, multiaddr, last_ok_at, last_try_at, fail_count)
         VALUES ($1, $2, $3, $3, 0)
         ON CONFLICT(peer_id, multiaddr) DO UPDATE SET
            last_ok_at  = excluded.last_ok_at,
            last_try_at = excluded.last_try_at,
            fail_count  = 0",
        [peer_id.into(), multiaddr.into(), (now as i64).into()],
    ))
    .await?;
    Ok(())
}

/// Record a failed dial attempt for `(peer_id, multiaddr)`.
///
/// Updates `last_try_at` to now and increments `fail_count`. Does not
/// touch `last_ok_at` — the recency filter still treats a peer as
/// usable if it was seen recently and is now temporarily unreachable.
/// A no-op if the row doesn't exist (we only learn an address by
/// dialing it once; if the first dial fails, there's nothing to record).
pub async fn record_failure(
    db: &impl ConnectionTrait,
    peer_id: &str,
    multiaddr: &str,
) -> Result<ExecResult, DbErr> {
    let now = now_secs();
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "UPDATE _wavesync_peer_addrs
            SET last_try_at = $3, fail_count = fail_count + 1
            WHERE peer_id = $1 AND multiaddr = $2",
        [peer_id.into(), multiaddr.into(), (now as i64).into()],
    ))
    .await
}

/// Load addresses we've successfully connected to within the last
/// `max_age_secs` seconds and have fewer than `max_fail_count`
/// consecutive failures.
///
/// Returned rows are sorted by `last_ok_at DESC` so the freshest entries
/// dial first. The caller fans them out via `swarm.dial(...)` and lets
/// libp2p race them; failures are recorded via [`record_failure`].
pub async fn load_recent(
    db: &impl ConnectionTrait,
    max_age_secs: u64,
    max_fail_count: u32,
) -> Result<Vec<CachedAddr>, DbErr> {
    let cutoff = now_secs().saturating_sub(max_age_secs);
    CachedAddr::find_by_statement(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "SELECT peer_id, multiaddr, last_ok_at, last_try_at, fail_count
            FROM _wavesync_peer_addrs
            WHERE last_ok_at >= $1 AND fail_count < $2
            ORDER BY last_ok_at DESC",
        [(cutoff as i64).into(), (max_fail_count as i64).into()],
    ))
    .all(db)
    .await
}

/// Drop rows that have aged out (`last_ok_at` older than `max_age_secs`)
/// or have hit the failure cap. Called opportunistically at engine
/// startup to keep the table from growing without bound.
pub async fn gc(
    db: &impl ConnectionTrait,
    max_age_secs: u64,
    max_fail_count: u32,
) -> Result<ExecResult, DbErr> {
    let cutoff = now_secs().saturating_sub(max_age_secs);
    db.execute_raw(Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "DELETE FROM _wavesync_peer_addrs
            WHERE last_ok_at < $1 OR fail_count >= $2",
        [(cutoff as i64).into(), (max_fail_count as i64).into()],
    ))
    .await
}

/// One row of the cache, surfaced to the engine for fan-out dialing.
#[derive(Debug, Clone, FromQueryResult)]
pub struct CachedAddr {
    pub peer_id: String,
    pub multiaddr: String,
    pub last_ok_at: i64,
    pub last_try_at: i64,
    pub fail_count: i64,
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{Database, DatabaseConnection};

    async fn fresh_db() -> DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        create_peer_addrs_table(&db).await.unwrap();
        db
    }

    #[tokio::test]
    async fn record_success_inserts_new_row_with_fail_count_zero() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();

        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].peer_id, "peer-A");
        assert_eq!(rows[0].fail_count, 0);
    }

    #[tokio::test]
    async fn record_failure_increments_fail_count_and_preserves_last_ok_at() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();
        let initial_ok_at = load_recent(&db, 3600, 100).await.unwrap()[0].last_ok_at;

        // Sleep one tick so the timestamp would otherwise change.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        record_failure(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();
        record_failure(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();

        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fail_count, 2);
        assert_eq!(
            rows[0].last_ok_at, initial_ok_at,
            "last_ok_at must NOT move on failure — it's the recency anchor"
        );
        assert!(
            rows[0].last_try_at > initial_ok_at,
            "last_try_at must move forward on failure"
        );
    }

    #[tokio::test]
    async fn record_success_after_failure_resets_fail_count() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();
        record_failure(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();
        record_failure(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();
        record_success(&db, "peer-A", "/ip4/1.2.3.4/udp/4001/quic-v1")
            .await
            .unwrap();

        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows[0].fail_count, 0);
    }

    #[tokio::test]
    async fn load_recent_filters_out_too_many_failures() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        // 5 consecutive failures
        for _ in 0..5 {
            record_failure(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        }

        // With max_fail_count = 5, peer-A is filtered out.
        let rows = load_recent(&db, 3600, 5).await.unwrap();
        assert!(rows.is_empty(), "fail_count >= max should be excluded");

        // With max_fail_count = 100, peer-A is included.
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn record_success_skips_transport_less_addr() {
        let db = fresh_db().await;
        // A bare /p2p/<id> multiaddr has no transport component and can
        // never be dialed — caching it wastes a cold-start pre-dial on a
        // guaranteed "Unsupported resolved address" failure.
        record_success(
            &db,
            "12D3KooWQoYdYSkm4Ypb7TA9qVW4PFQNAHhN3FiHEaBpt7DxRWfC",
            "/p2p/12D3KooWQoYdYSkm4Ypb7TA9qVW4PFQNAHhN3FiHEaBpt7DxRWfC",
        )
        .await
        .unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert!(rows.is_empty(), "transport-less addr was cached: {rows:?}");
    }

    #[tokio::test]
    async fn record_success_skips_unparseable_addr() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "not a multiaddr")
            .await
            .unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert!(rows.is_empty(), "unparseable addr was cached: {rows:?}");
    }

    #[tokio::test]
    async fn record_success_keeps_circuit_and_direct_addrs() {
        let db = fresh_db().await;
        record_success(
            &db,
            "12D3KooWQoYdYSkm4Ypb7TA9qVW4PFQNAHhN3FiHEaBpt7DxRWfC",
            "/ip4/10.0.0.2/udp/4001/quic-v1/p2p/12D3KooWJdeCPk9kiQN1L19ESVcUrcEfC1d1pNnpkAyk8spniCiX/p2p-circuit/p2p/12D3KooWQoYdYSkm4Ypb7TA9qVW4PFQNAHhN3FiHEaBpt7DxRWfC",
        )
        .await
        .unwrap();
        record_success(
            &db,
            "12D3KooWQoYdYSkm4Ypb7TA9qVW4PFQNAHhN3FiHEaBpt7DxRWfC",
            "/ip4/10.0.0.3/udp/9999/quic-v1",
        )
        .await
        .unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 2, "both dialable addrs must be cached");
    }

    #[tokio::test]
    async fn record_failure_on_unknown_pair_is_noop() {
        // We learn addresses by succeeding first; record_failure on a row
        // that doesn't exist must not insert one (we'd be polluting the
        // cache with addresses we've never reached).
        let db = fresh_db().await;
        record_failure(&db, "peer-A", "/ip4/9.9.9.9").await.unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn gc_deletes_aged_out_rows() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        // Sleep 1s, then GC with max_age=0. Cutoff = now (T+1), row's
        // last_ok_at = T → row is strictly older than cutoff and gets
        // dropped. (Equality wouldn't drop, by design — see
        // `gc_keeps_fresh_rows`.)
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        gc(&db, 0, 100).await.unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert!(rows.is_empty(), "row past max_age should have been GC'd");
    }

    #[tokio::test]
    async fn gc_keeps_fresh_rows() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        gc(&db, 3600, 100).await.unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 1, "fresh row must survive GC");
    }

    #[tokio::test]
    async fn gc_drops_rows_at_or_above_fail_cap() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        for _ in 0..5 {
            record_failure(&db, "peer-A", "/ip4/1.2.3.4").await.unwrap();
        }
        gc(&db, 3600, 5).await.unwrap();
        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert!(rows.is_empty(), "row past fail cap should have been GC'd");
    }

    #[tokio::test]
    async fn load_recent_orders_by_last_ok_at_desc() {
        let db = fresh_db().await;
        record_success(&db, "peer-A", "/ip4/1.1.1.1").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        record_success(&db, "peer-B", "/ip4/2.2.2.2").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        record_success(&db, "peer-C", "/ip4/3.3.3.3").await.unwrap();

        let rows = load_recent(&db, 3600, 100).await.unwrap();
        assert_eq!(rows.len(), 3);
        // Most-recent success first.
        assert_eq!(rows[0].peer_id, "peer-C");
        assert_eq!(rows[1].peer_id, "peer-B");
        assert_eq!(rows[2].peer_id, "peer-A");
    }
}
