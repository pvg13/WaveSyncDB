//! Leading-edge debounce coordinator for push notifications, with a
//! persistent retry queue for transient FCM/APNs failures.
//!
//! Two cooperating tokio tasks share the [`PushStore`] + [`PushSender`]:
//!
//! - **Notifier loop** (this module's original behavior): receives
//!   [`TopicNotification`]s from libp2p handlers, debounces per-topic at
//!   `cooldown_duration` (leading + trailing edge), and fans out to all
//!   registered tokens for a topic.
//! - **Retry worker** (new in this commit): event-driven loop that
//!   drains the `push_retries` SQLite table. Sleeps until the soonest
//!   `next_attempt_at`, or until the notifier nudges it because a fresh
//!   transient failure created an earlier deadline. Retries bypass the
//!   debouncer — they're already-decided sends.
//!
//! Wiring the structured `PushResult` variants from
//! [`crate::push_sender`] into the retry queue replaces the previous
//! "log and drop" behavior on transient failures.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::push_retry::{self, MAX_AGE_SECS, compute_delay};
use crate::push_sender::{PushResult, PushSender, TransientError};
use crate::push_store::{PushStore, RetryRow};

/// A notification request for a topic.
pub struct TopicNotification {
    pub topic: String,
    /// Addresses of the peer that triggered the notification.
    pub peer_addrs: Vec<String>,
}

/// One-shot signal sent from the notifier loop to the retry worker when
/// a fresh transient failure creates a retry row with a sooner
/// `next_attempt_at` than the worker is currently sleeping toward.
/// Without this, the worker would keep sleeping until its existing
/// deadline (which could be hours out) and add unnecessary latency to
/// the new retry's first attempt.
struct RetryNudge;

/// Maximum retry rows the worker pulls in a single SQLite read. Bounds
/// memory under burst conditions; soonest-first ordering ensures the
/// first batch is what fires soonest. Subsequent batches drain on the
/// next iteration of the worker loop.
const RETRY_BATCH_SIZE: i64 = 32;

/// Grace period for the startup smear of overdue rows. Pushed at least
/// this far into the future before any retry fires after relay restart,
/// so a long downtime doesn't translate into a thundering herd of
/// retries the moment the relay comes back.
const RESTART_GRACE_SECS: i64 = 30;
/// Width of the random spread applied to overdue rows on top of the
/// grace period. With grace=30 and jitter=15, rows land in `[now+30,
/// now+45)`. Configured per
/// [`PushStore::reschedule_overdue_retries`]'s contract.
const RESTART_JITTER_SECS: i64 = 15;

/// Background tasks that own the push fan-out + retry pipeline.
pub struct PushNotifier {
    tx: mpsc::Sender<TopicNotification>,
}

impl PushNotifier {
    /// Spawn both the notifier loop and the retry worker.
    pub fn spawn(
        store: Arc<PushStore>,
        sender: Arc<PushSender>,
        cooldown_duration: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<TopicNotification>(256);
        let (nudge_tx, nudge_rx) = mpsc::channel::<RetryNudge>(64);

        // Spawn the retry worker first so the startup smear runs before
        // any push attempt could create a fresh retry row. (The
        // notifier loop hasn't started yet — there's no race, but the
        // ordering is also the natural read.)
        tokio::spawn(retry_worker_loop(
            store.clone(),
            sender.clone(),
            nudge_rx,
        ));

        tokio::spawn(notifier_loop(
            rx,
            store,
            sender,
            cooldown_duration,
            nudge_tx,
        ));

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
    nudge_tx: mpsc::Sender<RetryNudge>,
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
                                fire_notifications(&store, &sender, &topic, &notification.peer_addrs, &nudge_tx).await;
                                state.expires_at = now + cooldown_duration;
                                state.pending = None;
                            } else {
                                // During cooldown — suppress, but save for trailing edge
                                state.pending = Some(notification.peer_addrs);
                            }
                        } else {
                            // First notification for this topic — fire immediately
                            fire_notifications(&store, &sender, &topic, &notification.peer_addrs, &nudge_tx).await;
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
                    fire_notifications(&store, &sender, &topic, &peer_addrs, &nudge_tx).await;
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

/// Wall-clock seconds since the Unix epoch. Stored in `push_retries`
/// for `next_attempt_at` and `first_failed_at`. Wall-clock so retries
/// survive process restart; tokio::time::Instant would reset.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn fire_notifications(
    store: &PushStore,
    sender: &PushSender,
    topic: &str,
    peer_addrs: &[String],
    nudge_tx: &mpsc::Sender<RetryNudge>,
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

    let peer_addrs_json = serde_json::to_string(peer_addrs).unwrap_or_else(|_| "[]".to_string());

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

        let short = token_entry.token.chars().take(20).collect::<String>();
        match result {
            PushResult::Sent => {
                log::info!("Push sent ({}) to token={}...", token_entry.platform, short);
                // A fresh send succeeded — clear any stale retry row
                // that had been queued from an earlier failure for the
                // same (topic, token). Latest peer_addrs already won
                // via the debouncer; this just sweeps the stale row.
                if let Err(e) = store.remove_retry(topic, &token_entry.token).await {
                    log::debug!("remove_retry after Sent failed for {short}...: {e}");
                }
            }
            PushResult::TokenInvalid { reason } => {
                log::info!(
                    "Pruning invalid push token {short}... ({}): {reason:?}",
                    token_entry.platform
                );
                if let Err(e) = store.remove_token(&token_entry.token).await {
                    log::error!("Failed to remove invalid token: {e}");
                }
                // Token is dead; corresponding retry rows are now
                // orphans that would never succeed. Sweep them.
                if let Err(e) = store.purge_retries_for_token(&token_entry.token).await {
                    log::error!("Failed to purge retries for invalidated token: {e}");
                }
            }
            PushResult::Transient(err) => {
                let now = now_unix();
                let (error_kind, error_code, error_body, retry_after) =
                    classify_transient(&err);

                let next_delay = match compute_delay(1, retry_after) {
                    Some(d) => d,
                    None => unreachable!("compute_delay(1, _) is always Some"),
                };
                let next_attempt_at = now + next_delay.as_secs() as i64;

                log::warn!(
                    "Push transient failure for {short}... ({}); \
                     queueing retry #1/7 in ~{:?}: {err:?}",
                    token_entry.platform,
                    next_delay,
                );
                if let Err(e) = store
                    .enqueue_retry(
                        topic,
                        &token_entry.token,
                        &token_entry.platform,
                        &peer_addrs_json,
                        1,
                        next_attempt_at,
                        now,
                        error_kind,
                        error_code,
                        error_body.as_deref(),
                    )
                    .await
                {
                    log::error!("Failed to enqueue retry: {e}");
                    continue;
                }
                let _ = nudge_tx.try_send(RetryNudge);
            }
            PushResult::Permanent(err) => {
                log::error!(
                    "Push permanent failure for {short}... ({}): {err:?} \
                     (dropping; misconfiguration won't resolve itself)",
                    token_entry.platform
                );
                // Clear any retry row that might be in flight for this
                // (topic, token) — once we've classified the failure as
                // permanent, retries won't help.
                let _ = store.remove_retry(topic, &token_entry.token).await;
            }
        }
    }
}

/// Decompose a `TransientError` into the (kind, code, body, retry_after)
/// columns the retry row holds.
fn classify_transient(
    err: &TransientError,
) -> (&'static str, Option<i64>, Option<String>, Option<Duration>) {
    match err {
        TransientError::HttpStatus {
            status,
            retry_after,
            body,
            ..
        } => (
            "http_status",
            Some(*status as i64),
            Some(truncate(body, 512)),
            *retry_after,
        ),
        TransientError::Transport { message, .. } => {
            ("transport", None, Some(truncate(message, 512)), None)
        }
        TransientError::OauthTransport { message } => {
            ("oauth", None, Some(truncate(message, 512)), None)
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut t = s[..max].to_string();
        t.push_str("…");
        t
    }
}

// ── Retry worker ────────────────────────────────────────────────────

async fn retry_worker_loop(
    store: Arc<PushStore>,
    sender: Arc<PushSender>,
    mut nudge_rx: mpsc::Receiver<RetryNudge>,
) {
    // Startup smear + stale purge. Without smear, a relay that was down
    // for an hour wakes up and slams FCM/APNs with every queued retry
    // within seconds.
    let now = now_unix();
    match store
        .reschedule_overdue_retries(now, RESTART_GRACE_SECS, RESTART_JITTER_SECS)
        .await
    {
        Ok(n) if n > 0 => log::info!(
            "Retry worker: smeared {n} overdue rows into +{}..{}s after startup",
            RESTART_GRACE_SECS,
            RESTART_GRACE_SECS + RESTART_JITTER_SECS
        ),
        Ok(_) => {}
        Err(e) => log::warn!("reschedule_overdue_retries failed at startup: {e}"),
    }
    match store.purge_stale_retries(now, MAX_AGE_SECS).await {
        Ok(n) if n > 0 => {
            log::info!("Retry worker: purged {n} rows older than {MAX_AGE_SECS}s")
        }
        Ok(_) => {}
        Err(e) => log::warn!("purge_stale_retries failed at startup: {e}"),
    }

    loop {
        // Drain due rows. Soonest-first; bounded batch.
        let now = now_unix();
        let due = match store.fetch_due_retries(now, RETRY_BATCH_SIZE).await {
            Ok(rows) => rows,
            Err(e) => {
                log::error!("fetch_due_retries failed: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        for row in due {
            process_retry(&store, &sender, row).await;
        }

        // Sleep until the next row's deadline, or until a nudge wakes
        // us. If the queue is empty, sleep indefinitely until nudged.
        let next_at = store.peek_next_attempt_at().await.ok().flatten();
        tokio::select! {
            _ = nudge_rx.recv() => {
                // Loop and re-query — a fresh retry just landed.
            }
            _ = async {
                match next_at {
                    Some(at) => {
                        let now = now_unix();
                        if at <= now { return; }
                        tokio::time::sleep(Duration::from_secs((at - now) as u64)).await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }
}

async fn process_retry(store: &PushStore, sender: &PushSender, row: RetryRow) {
    // The row was inserted at attempts=N meaning "next retry is the
    // Nth". Now that we're firing it, the *result* should be processed
    // at attempts=N+1 if it fails again.
    let short = row.token.chars().take(20).collect::<String>();
    let platform_label = row.platform.clone();

    let result = match row.platform.as_str() {
        "Fcm" => sender.send_fcm(&row.token, &row.topic, &row.peer_addrs).await,
        "Apns" => {
            sender
                .send_apns(&row.token, &row.topic, &row.peer_addrs)
                .await
        }
        other => {
            log::warn!("Retry worker: unknown push platform {other}; dropping row");
            let _ = store.remove_retry(&row.topic, &row.token).await;
            return;
        }
    };

    match result {
        PushResult::Sent => {
            log::info!(
                "Push retry succeeded ({platform_label}) to token={short}... \
                 (attempt {})",
                row.attempts
            );
            let _ = store.remove_retry(&row.topic, &row.token).await;
        }
        PushResult::TokenInvalid { reason } => {
            log::info!(
                "Push retry hit TokenInvalid for {short}... ({platform_label}): {reason:?}; \
                 pruning token + retries"
            );
            let _ = store.remove_token(&row.token).await;
            let _ = store.purge_retries_for_token(&row.token).await;
        }
        PushResult::Permanent(err) => {
            log::error!(
                "Push retry hit Permanent for {short}... ({platform_label}): {err:?}; \
                 dropping"
            );
            let _ = store.remove_retry(&row.topic, &row.token).await;
        }
        PushResult::Transient(err) => {
            let now = now_unix();
            if push_retry::age_exceeded(row.first_failed_at, now) {
                log::info!(
                    "Push retry: dropping {short}... ({platform_label}) — \
                     age exceeded ({}s old)",
                    now - row.first_failed_at
                );
                let _ = store.remove_retry(&row.topic, &row.token).await;
                return;
            }

            let next_attempts = row.attempts + 1;
            let (error_kind, error_code, error_body, retry_after) = classify_transient(&err);
            match compute_delay(next_attempts, retry_after) {
                Some(delay) => {
                    let next_attempt_at = now + delay.as_secs() as i64;
                    log::info!(
                        "Push retry attempt {} failed for {short}... ({platform_label}); \
                         scheduling attempt {} in ~{:?}",
                        row.attempts,
                        next_attempts,
                        delay
                    );
                    // UPSERT — same (topic, token), so this updates the
                    // existing row's attempts + next_attempt_at +
                    // last_error_*. peer_addrs stays the same as on the
                    // row we just read; if a fresh NotifyTopic arrives
                    // during this update window it will UPSERT again
                    // with newer addrs.
                    let _ = store
                        .enqueue_retry(
                            &row.topic,
                            &row.token,
                            &row.platform,
                            // serialize the addrs we already have
                            &serde_json::to_string(&row.peer_addrs)
                                .unwrap_or_else(|_| "[]".to_string()),
                            next_attempts,
                            next_attempt_at,
                            row.first_failed_at,
                            error_kind,
                            error_code,
                            error_body.as_deref(),
                        )
                        .await;
                }
                None => {
                    log::info!(
                        "Push retry budget exhausted for {short}... ({platform_label}) \
                         after {} attempts; dropping",
                        row.attempts
                    );
                    let _ = store.remove_retry(&row.topic, &row.token).await;
                }
            }
        }
    }
}

