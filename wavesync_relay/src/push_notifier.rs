//! Debounce coordinator for push notifications.
//!
//! Batches rapid writes into a single notification per topic.

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
        debounce_duration: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<TopicNotification>(256);
        tokio::spawn(notifier_loop(rx, store, sender, debounce_duration));
        Self { tx }
    }

    /// Queue a topic for notification (non-blocking, drops if channel full).
    pub fn notify(&self, topic: String) {
        let _ = self.tx.try_send(TopicNotification { topic });
    }
}

async fn notifier_loop(
    mut rx: mpsc::Receiver<TopicNotification>,
    store: Arc<PushStore>,
    sender: Arc<PushSender>,
    debounce_duration: Duration,
) {
    // Track pending debounce timers per topic
    let mut pending: HashMap<String, Instant> = HashMap::new();

    loop {
        // Calculate next deadline from pending timers
        let next_deadline = pending.values().min().copied();

        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(notification) => {
                        // Reset or start debounce timer for this topic
                        pending.insert(
                            notification.topic,
                            Instant::now() + debounce_duration,
                        );
                    }
                    None => break, // Channel closed
                }
            }
            _ = async {
                match next_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                // Fire all expired timers
                let now = Instant::now();
                let expired: Vec<String> = pending
                    .iter()
                    .filter(|(_, deadline)| **deadline <= now)
                    .map(|(topic, _)| topic.clone())
                    .collect();

                for topic in expired {
                    pending.remove(&topic);
                    fire_notifications(&store, &sender, &topic).await;
                }
            }
        }
    }
}

async fn fire_notifications(store: &PushStore, sender: &PushSender, topic: &str) {
    let tokens = match store.get_tokens_for_topic(topic).await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to get push tokens for topic {topic}: {e}");
            return;
        }
    };

    if tokens.is_empty() {
        return;
    }

    log::info!(
        "Sending push notifications for topic {topic} to {} devices",
        tokens.len()
    );

    for token_entry in &tokens {
        let result = match token_entry.platform.as_str() {
            "Fcm" => sender.send_fcm(&token_entry.token, topic).await,
            "Apns" => sender.send_apns(&token_entry.token, topic).await,
            other => {
                log::warn!("Unknown push platform: {other}");
                continue;
            }
        };

        match result {
            PushResult::Sent => {
                log::debug!(
                    "Push sent to {} ({})",
                    token_entry.token,
                    token_entry.platform
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
