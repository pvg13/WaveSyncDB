//! SQLite-backed storage for push notification tokens using sqlx.

use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

/// A registered push token entry.
#[derive(Debug, Clone)]
pub struct PushToken {
    pub topic: String,
    pub platform: String,
    pub token: String,
    pub peer_id: String,
}

/// Async wrapper around an sqlx SQLite pool for push token storage.
pub struct PushStore {
    pool: SqlitePool,
}

impl PushStore {
    /// Open (or create) the push token database at the given path.
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        let url = if path == ":memory:" {
            "sqlite::memory:".to_string()
        } else {
            format!("sqlite:{path}?mode=rwc")
        };

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS push_tokens (
                topic TEXT NOT NULL,
                platform TEXT NOT NULL,
                token TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                registered_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (topic, token)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Self { pool })
    }

    /// Register a push token for a topic.
    pub async fn register_token(
        &self,
        topic: &str,
        platform: &str,
        token: &str,
        peer_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT OR REPLACE INTO push_tokens (topic, platform, token, peer_id, registered_at)
             VALUES (?1, ?2, ?3, ?4, strftime('%s', 'now'))",
        )
        .bind(topic)
        .bind(platform)
        .bind(token)
        .bind(peer_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Unregister a specific token from a topic.
    pub async fn unregister_token(&self, topic: &str, token: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM push_tokens WHERE topic = ?1 AND token = ?2")
            .bind(topic)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get all tokens registered for a given topic.
    pub async fn get_tokens_for_topic(&self, topic: &str) -> Result<Vec<PushToken>, sqlx::Error> {
        let rows =
            sqlx::query("SELECT topic, platform, token, peer_id FROM push_tokens WHERE topic = ?1")
                .bind(topic)
                .fetch_all(&self.pool)
                .await?;

        Ok(rows
            .iter()
            .map(|row| PushToken {
                topic: row.get("topic"),
                platform: row.get("platform"),
                token: row.get("token"),
                peer_id: row.get("peer_id"),
            })
            .collect())
    }

    /// Remove all tokens registered by a specific peer.
    pub async fn remove_tokens_for_peer(&self, peer_id: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_tokens WHERE peer_id = ?1")
            .bind(peer_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Remove tokens older than `max_age_secs` seconds.
    pub async fn cleanup_stale(&self, max_age_secs: u64) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "DELETE FROM push_tokens WHERE registered_at < (strftime('%s', 'now') - ?1)",
        )
        .bind(max_age_secs as i64)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    /// Remove a specific token across all topics (used when push provider reports invalid).
    pub async fn remove_token(&self, token: &str) -> Result<u64, sqlx::Error> {
        let result = sqlx::query("DELETE FROM push_tokens WHERE token = ?1")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem_store() -> PushStore {
        PushStore::open(":memory:").await.unwrap()
    }

    #[tokio::test]
    async fn test_register_and_get() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        store
            .register_token("topic1", "Apns", "token-b", "peer-2")
            .await
            .unwrap();
        store
            .register_token("topic2", "Fcm", "token-c", "peer-1")
            .await
            .unwrap();

        let tokens = store.get_tokens_for_topic("topic1").await.unwrap();
        assert_eq!(tokens.len(), 2);

        let tokens = store.get_tokens_for_topic("topic2").await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].token, "token-c");
    }

    #[tokio::test]
    async fn test_unregister() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        store.unregister_token("topic1", "token-a").await.unwrap();
        let tokens = store.get_tokens_for_topic("topic1").await.unwrap();
        assert!(tokens.is_empty());
    }

    #[tokio::test]
    async fn test_remove_tokens_for_peer() {
        let store = mem_store().await;
        store
            .register_token("t1", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        store
            .register_token("t2", "Fcm", "tok2", "peer-1")
            .await
            .unwrap();
        store
            .register_token("t1", "Apns", "tok3", "peer-2")
            .await
            .unwrap();

        let removed = store.remove_tokens_for_peer("peer-1").await.unwrap();
        assert_eq!(removed, 2);

        let tokens = store.get_tokens_for_topic("t1").await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].peer_id, "peer-2");
    }

    #[tokio::test]
    async fn test_upsert_on_duplicate() {
        let store = mem_store().await;
        store
            .register_token("topic1", "Fcm", "token-a", "peer-1")
            .await
            .unwrap();
        // Re-register same (topic, token) with different peer — should upsert
        store
            .register_token("topic1", "Fcm", "token-a", "peer-2")
            .await
            .unwrap();
        let tokens = store.get_tokens_for_topic("topic1").await.unwrap();
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].peer_id, "peer-2");
    }

    #[tokio::test]
    async fn test_remove_token() {
        let store = mem_store().await;
        store
            .register_token("t1", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        store
            .register_token("t2", "Fcm", "tok1", "peer-1")
            .await
            .unwrap();
        let removed = store.remove_token("tok1").await.unwrap();
        assert_eq!(removed, 2);
    }
}
