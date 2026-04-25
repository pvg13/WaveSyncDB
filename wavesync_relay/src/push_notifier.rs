//! Leading-edge debounce coordinator for push notifications.
//!
//! Fires immediately on the first notification for a topic, then suppresses
//! duplicates during a cooldown window. If notifications arrive during cooldown,
//! the latest peer addresses are kept and a final notification fires when the
//! cooldown expires. This ensures sub-second delivery for single writes while
//! still batching rapid burst writes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::push_sender::{PushResult, PushSender};
use crate::push_store::PushStore;

/// A notification request for a topic.
pub struct TopicNotification {
    pub topic: String,
    /// Addresses of the peer that triggered the notification.
    pub peer_addrs: Vec<String>,
}

/// Background task that debounces topic notifications and fans out push messages.
pub struct PushNotifier {
    tx: mpsc::Sender<TopicNotification>,
}

impl PushNotifier {
    /// Spawn the notifier background task and return a handle for sending notifications.
    pub fn spawn(
        store: Arc<PushStore>,
        sender: Arc<PushSender>,
        cooldown_duration: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<TopicNotification>(256);
        tokio::spawn(notifier_loop(rx, store, sender, cooldown_duration));
        Self { tx }
    }

    /// Queue a topic for notification (non-blocking, drops if channel full).
    pub fn notify(&self, topic: String, peer_addrs: Vec<String>) {
        let _ = self.tx.try_send(TopicNotification { topic, peer_addrs });
    }
}

/// Per-topic cooldown state.
struct CooldownState {
    /// When the cooldown expires (next fire allowed).
    expires_at: Instant,
    /// If a notification arrived during cooldown, store its addresses here.
    /// When cooldown expires, this fires as a trailing-edge notification.
    pending: Option<Vec<String>>,
}

async fn notifier_loop(
    mut rx: mpsc::Receiver<TopicNotification>,
    store: Arc<PushStore>,
    sender: Arc<PushSender>,
    cooldown_duration: Duration,
) {
    let mut cooldowns: HashMap<String, CooldownState> = HashMap::new();

    loop {
        // Find the next cooldown expiry to check for trailing-edge fires
        let next_expiry = cooldowns
            .values()
            .filter(|s| s.pending.is_some())
            .map(|s| s.expires_at)
            .min();

        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(notification) => {
                        let now = Instant::now();
                        let topic = notification.topic;

                        if let Some(state) = cooldowns.get_mut(&topic) {
                            if now >= state.expires_at {
                                // Cooldown expired — fire immediately (leading edge)
                                fire_notifications(&store, &sender, &topic, &notification.peer_addrs).await;
                                state.expires_at = now + cooldown_duration;
                                state.pending = None;
                            } else {
                                // During cooldown — suppress, but save for trailing edge
                                state.pending = Some(notification.peer_addrs);
                            }
                        } else {
                            // First notification for this topic — fire immediately
                            fire_notifications(&store, &sender, &topic, &notification.peer_addrs).await;
                            cooldowns.insert(topic, CooldownState {
                                expires_at: now + cooldown_duration,
                                pending: None,
                            });
                        }
                    }
                    None => break, // Channel closed
                }
            }
            _ = async {
                match next_expiry {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                // Check for expired cooldowns with pending notifications
                let now = Instant::now();
                let expired: Vec<(String, Vec<String>)> = cooldowns
                    .iter()
                    .filter(|(_, state)| state.pending.is_some() && state.expires_at <= now)
                    .map(|(topic, state)| (topic.clone(), state.pending.clone().unwrap()))
                    .collect();

                for (topic, peer_addrs) in expired {
                    fire_notifications(&store, &sender, &topic, &peer_addrs).await;
                    if let Some(state) = cooldowns.get_mut(&topic) {
                        state.expires_at = now + cooldown_duration;
                        state.pending = None;
                    }
                }

                // Clean up stale cooldowns (no pending, expired > 60s ago)
                let stale_threshold = now - Duration::from_secs(60);
                cooldowns.retain(|_, state| {
                    state.pending.is_some() || state.expires_at > stale_threshold
                });
            }
        }
    }
}

async fn fire_notifications(
    store: &PushStore,
    sender: &PushSender,
    topic: &str,
    peer_addrs: &[String],
) {
    let tokens = match store.get_tokens_for_topic(topic).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to get push tokens for topic {topic}: {e}");
            return;
        }
    };

    if tokens.is_empty() {
        log::info!(
            "fire_notifications: no registered tokens for topic {topic} — \
             nothing to push (this is the silent path that masks 'phone never \
             registered' in production)"
        );
        return;
    }

    log::info!(
        "Sending push notifications for topic {topic} to {} devices (peer_addrs: {})",
        tokens.len(),
        peer_addrs.len()
    );

    for token_entry in &tokens {
        let result = match token_entry.platform.as_str() {
            "Fcm" => sender.send_fcm(&token_entry.token, topic, peer_addrs).await,
            "Apns" => {
                sender
                    .send_apns(&token_entry.token, topic, peer_addrs)
                    .await
            }
            other => {
                log::warn!("Unknown push platform: {other}");
                continue;
            }
        };

        match result {
            PushResult::Sent => {
                // Log at info so deploys can confirm FCM/APNs is reaching the
                // upstream API. Token is truncated to first 20 chars to avoid
                // dumping full credentials into log aggregators.
                let short = token_entry.token.chars().take(20).collect::<String>();
                log::info!(
                    "Push sent ({}) to token={}...",
                    token_entry.platform,
                    short
                );
            }
            PushResult::TokenInvalid => {
                log::info!(
                    "Pruning invalid push token {} ({})",
                    token_entry.token,
                    token_entry.platform
                );
                if let Err(e) = store.remove_token(&token_entry.token).await {
                    log::error!("Failed to remove invalid token: {e}");
                }
            }
            PushResult::Error(e) => {
                log::warn!(
                    "Push failed for {} ({}): {e}",
                    token_entry.token,
                    token_entry.platform
                );
            }
        }
    }
}
