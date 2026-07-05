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

use crate::metrics::RelayMetrics;
use crate::push_retry::{self, MAX_AGE_SECS, compute_delay};
use crate::push_sender::{PushResult, PushSender, TransientError};
use crate::push_store::{PushStore, RetryRow};

/// A notification request for a topic.
pub struct TopicNotification {
    pub topic: String,
    /// libp2p peer id of the device that triggered the notification. Used to
    /// skip that device's own token in the fan-out so a local write never
    /// wakes the writer itself.
    pub notifying_peer: String,
    /// Addresses of the peer that triggered the notification.
    pub peer_addrs: Vec<String>,
    /// Whether the triggering changeset touched a `SyncNotify`-visible
    /// table. Drives the unbudgeted ALERT-class APNs send in
    /// `fire_notifications` — see its doc comment.
    pub visible: bool,
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
    ///
    /// `apns_coalesce_secs` / `fcm_coalesce_secs` are the per-device
    /// wake-coalescing windows (0 disables coalescing for that platform) —
    /// see [`fire_notifications`]'s gate for the full rationale.
    /// `alert_coalesce_secs` is the equivalent window for unbudgeted
    /// ALERT-class sends, kept separate from `apns_coalesce_secs` because
    /// alerts skip the daily budget entirely and need their own,
    /// independently-tunable anti-spam window.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        store: Arc<PushStore>,
        sender: Arc<PushSender>,
        cooldown_duration: Duration,
        metrics: RelayMetrics,
        apns_coalesce_secs: u64,
        fcm_coalesce_secs: u64,
        alert_coalesce_secs: u64,
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
            metrics.clone(),
        ));

        tokio::spawn(notifier_loop(
            rx,
            store,
            sender,
            cooldown_duration,
            nudge_tx,
            metrics,
            apns_coalesce_secs,
            fcm_coalesce_secs,
            alert_coalesce_secs,
        ));

        Self { tx }
    }

    /// Queue a topic for notification (non-blocking, drops if channel full).
    /// `notifying_peer` is the libp2p peer id of the device that sent the
    /// `NotifyTopic`; its own registered token is excluded from the fan-out.
    /// `visible` is the sender-computed `SyncNotify` signal carried on the
    /// wire — see `NotifyTopic::visible`.
    pub fn notify(
        &self,
        topic: String,
        notifying_peer: String,
        peer_addrs: Vec<String>,
        visible: bool,
    ) {
        let _ = self.tx.try_send(TopicNotification {
            topic,
            notifying_peer,
            peer_addrs,
            visible,
        });
    }
}

/// A suppressed-during-cooldown notification waiting to fire trailing-edge:
/// notifying peer id, its addresses, and the visible-wins-merged flag (see
/// [`CooldownState::pending`]).
type PendingFire = (String, Vec<String>, bool);

/// Per-topic cooldown state.
struct CooldownState {
    /// When the cooldown expires (next fire allowed).
    expires_at: Instant,
    /// If a notification arrived during cooldown, store the notifying peer
    /// id, its addresses, and whether ANY suppressed notification in this
    /// window was visible here. When cooldown expires, this fires as a
    /// trailing-edge notification (excluding that peer's own token).
    /// Visible-wins: a burst that mixes silent and visible writes must
    /// still surface a realtime banner for the visible one, so the flag is
    /// OR'd across every suppressed notification rather than only keeping
    /// the last.
    pending: Option<PendingFire>,
}

/// Key of a wake deferred by the per-device coalescing window: one slot per
/// `(token, topic, class)`. Class-keyed to match the `push_wakes` stamp —
/// a deferred silent wake and a deferred alert for the same device coexist
/// and flush independently.
type DeferredKey = (String, String, bool);

/// A send suppressed by [`PushStore::check_and_stamp_wake`], waiting for its
/// coalescing window to expire. Unlike the pre-#78 behavior (drop the send
/// and rely on the device's own catch-up), the trailing-edge flush guarantees
/// that the LAST write of a burst also produces a wake — the leading-edge
/// wake fires before writes 2..N of the burst even exist, so dropping the
/// rest silenced everything for the remainder of the window. Alerts have no
/// catch-up path at all (a banner only ever comes from an actually-sent
/// push), so for them the flush is the difference between "delayed banner"
/// and "no banner, ever".
///
/// Held in memory: losing a pending flush to a relay restart costs one wake
/// (recovered by the device's next catch-up sync or the next write), same
/// class of loss as the in-memory topic debounce above it.
struct DeferredWake {
    /// When the coalescing window expires and this can fire.
    due_at: Instant,
    platform: String,
    /// Latest writer addresses observed while suppressed — each newer
    /// suppressed notify overwrites these, so the flush carries the
    /// freshest dial hints.
    peer_addrs: Vec<String>,
}

/// Everything the notifier and flush paths need to gate + send one wake.
/// Bundled so the per-token pipeline (`coalesce_gate_and_send`, `send_wake`)
/// doesn't take a dozen loose parameters.
struct SendCtx<'a> {
    store: &'a PushStore,
    sender: &'a PushSender,
    nudge_tx: &'a mpsc::Sender<RetryNudge>,
    metrics: &'a RelayMetrics,
    apns_coalesce_secs: u64,
    fcm_coalesce_secs: u64,
    alert_coalesce_secs: u64,
}

impl SendCtx<'_> {
    /// Coalescing window applying to a send of the given class/platform.
    fn coalesce_window_secs(&self, platform: &str, is_alert: bool) -> u64 {
        if is_alert {
            self.alert_coalesce_secs
        } else {
            match platform {
                "Apns" => self.apns_coalesce_secs,
                "Fcm" => self.fcm_coalesce_secs,
                _ => 0,
            }
        }
    }
}

/// Stamp-table class label for a send. Must be stable — it is persisted in
/// `push_wakes.kind`.
fn wake_kind(is_alert: bool) -> &'static str {
    if is_alert { "alert" } else { "silent" }
}

#[allow(clippy::too_many_arguments)]
async fn notifier_loop(
    mut rx: mpsc::Receiver<TopicNotification>,
    store: Arc<PushStore>,
    sender: Arc<PushSender>,
    cooldown_duration: Duration,
    nudge_tx: mpsc::Sender<RetryNudge>,
    metrics: RelayMetrics,
    apns_coalesce_secs: u64,
    fcm_coalesce_secs: u64,
    alert_coalesce_secs: u64,
) {
    let mut cooldowns: HashMap<String, CooldownState> = HashMap::new();
    let mut deferred: HashMap<DeferredKey, DeferredWake> = HashMap::new();
    let ctx = SendCtx {
        store: &store,
        sender: &sender,
        nudge_tx: &nudge_tx,
        metrics: &metrics,
        apns_coalesce_secs,
        fcm_coalesce_secs,
        alert_coalesce_secs,
    };

    loop {
        // Find the next cooldown expiry to check for trailing-edge fires
        let next_expiry = cooldowns
            .values()
            .filter(|s| s.pending.is_some())
            .map(|s| s.expires_at)
            .min();
        // ...and the next deferred per-device wake to flush.
        let next_deferred = deferred.values().map(|d| d.due_at).min();

        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(notification) => {
                        let now = Instant::now();
                        let topic = notification.topic;

                        if let Some(state) = cooldowns.get_mut(&topic) {
                            if now >= state.expires_at {
                                // Cooldown expired — fire immediately (leading
                                // edge). If a suppressed notification is still
                                // pending from the previous window (the timer
                                // branch hasn't won the select yet), merge its
                                // visible-wins flag in rather than dropping it:
                                // this fire replaces that trailing edge, and a
                                // pending visible write must not be downgraded
                                // to a silent wake by losing the race.
                                let merged_visible = notification.visible
                                    || state.pending.take().map(|(_, _, v)| v).unwrap_or(false);
                                fire_notifications(&ctx, &topic, &notification.notifying_peer, &notification.peer_addrs, merged_visible, &mut deferred).await;
                                state.expires_at = now + cooldown_duration;
                                state.pending = None;
                            } else {
                                // During cooldown — suppress, but save for trailing
                                // edge. Visible-wins merge against whatever was
                                // already pending this window.
                                let merged_visible = state.pending.as_ref().map(|(_, _, v)| *v).unwrap_or(false)
                                    || notification.visible;
                                state.pending = Some((notification.notifying_peer, notification.peer_addrs, merged_visible));
                            }
                        } else {
                            // First notification for this topic — fire immediately
                            fire_notifications(&ctx, &topic, &notification.notifying_peer, &notification.peer_addrs, notification.visible, &mut deferred).await;
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
                let expired: Vec<(String, PendingFire)> = cooldowns
                    .iter()
                    .filter(|(_, state)| state.pending.is_some() && state.expires_at <= now)
                    .map(|(topic, state)| (topic.clone(), state.pending.clone().unwrap()))
                    .collect();

                for (topic, (notifying_peer, peer_addrs, visible)) in expired {
                    fire_notifications(&ctx, &topic, &notifying_peer, &peer_addrs, visible, &mut deferred).await;
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
            _ = async {
                match next_deferred {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                flush_due_deferred(&ctx, &mut deferred).await;
            }
        }
    }
}

/// Fire every deferred wake whose coalescing window has expired. Each flush
/// re-runs the normal coalescing gate: `Allowed` is the expected case (the
/// window that deferred it has lapsed) and stamps the new window; a
/// `Suppressed` result means some fresher leading-edge send stamped a new
/// window after this entry was queued — that send already woke the device
/// with newer dial hints, so the stale entry is simply dropped.
async fn flush_due_deferred(ctx: &SendCtx<'_>, deferred: &mut HashMap<DeferredKey, DeferredWake>) {
    let now = Instant::now();
    let due: Vec<(DeferredKey, DeferredWake)> = {
        let keys: Vec<DeferredKey> = deferred
            .iter()
            .filter(|(_, d)| d.due_at <= now)
            .map(|(k, _)| k.clone())
            .collect();
        keys.into_iter()
            .filter_map(|k| deferred.remove(&k).map(|d| (k, d)))
            .collect()
    };

    for ((token, topic, is_alert), wake) in due {
        let window = ctx.coalesce_window_secs(&wake.platform, is_alert);
        match ctx
            .store
            .check_and_stamp_wake(&token, &topic, wake_kind(is_alert), now_unix(), window as i64)
            .await
        {
            Ok(crate::push_store::WakeGate::Allowed) => {
                tracing::info!(
                    topic = %crate::short_topic(&topic),
                    platform = %wake.platform,
                    kind = wake_kind(is_alert),
                    "Flushing deferred (coalesced) wake"
                );
                send_wake(ctx, &topic, &token, &wake.platform, is_alert, &wake.peer_addrs).await;
            }
            Ok(crate::push_store::WakeGate::Suppressed { .. }) => {
                // A fresher send won the window since this was deferred.
                tracing::debug!(
                    topic = %crate::short_topic(&topic),
                    kind = wake_kind(is_alert),
                    "Dropping deferred wake — a newer send already covered it"
                );
            }
            Err(e) => {
                // Same fail-open policy as the leading-edge gate: push is a
                // best-effort wake hint, so a store error sends anyway.
                tracing::warn!("Wake-coalescing check failed on deferred flush, sending anyway: {e}");
                send_wake(ctx, &topic, &token, &wake.platform, is_alert, &wake.peer_addrs).await;
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

/// `visible` selects the send class for every APNs token in this fire round:
/// `true` means the triggering changeset touched a `SyncNotify`-visible
/// table, so APNs tokens get an unbudgeted ALERT-class send (skips
/// `charge_daily_budget` entirely — including its deny-path refund, which
/// only exists for that budget-gated branch — but keeps
/// `check_and_stamp_wake` under its own `alert_coalesce_secs` window rather
/// than `apns_coalesce_secs`). FCM is unaffected either way: Android stays
/// on the data-only class here, since the app-side `SyncNotify` policy
/// already renders a rich local notification once it wakes and syncs.
async fn fire_notifications(
    ctx: &SendCtx<'_>,
    topic: &str,
    notifying_peer: &str,
    peer_addrs: &[String],
    visible: bool,
    deferred: &mut HashMap<DeferredKey, DeferredWake>,
) {
    // Exclude the writer's own token: a device uses the same libp2p peer id for
    // its RegisterToken and its NotifyTopic, so a local write must not wake the
    // device that made it.
    let tokens = match ctx
        .store
        .get_tokens_for_topic(topic, Some(notifying_peer))
        .await
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("Failed to get push tokens for topic {topic}: {e}");
            return;
        }
    };

    let topic_short = crate::short_topic(topic);

    if tokens.is_empty() {
        // With the writer's own token excluded, an empty set is the normal
        // single-device / self-only case — not necessarily a registration
        // failure. Kept at debug so it doesn't read as an alarm on every write;
        // a genuinely-unregistered peer shows up as a missing RegisterToken on
        // the relay's INFO log, not here.
        tracing::debug!(
            "No other registered devices to wake for {topic_short} (writer {notifying_peer} excluded)"
        );
        return;
    }

    tracing::info!(
        "Waking {} device(s) for {topic_short} (writer {notifying_peer} excluded; {} peer addr(s))",
        tokens.len(),
        peer_addrs.len(),
    );

    let now = now_unix();

    for token_entry in &tokens {
        // ALERT-class only applies to APNs — FCM keeps the data-only class
        // today regardless of `visible` (Android's SyncNotify already
        // renders the rich local once it wakes and syncs).
        let is_alert = visible && token_entry.platform == "Apns";
        let kind_label = wake_kind(is_alert);

        // Per-(token, topic, class) wake-coalescing window: a burst of writes
        // to the same topic must cost this device ONE wake per window, not
        // one per write, since APNs throttles silent background pushes to
        // only a handful per device per day. This composes with (doesn't
        // replace) the topic-keyed debounce above it in the call chain —
        // that coalesces bursts across *all* devices within ~1s; this
        // coalesces a *single* device's wakes across a much longer (minutes)
        // window. A suppressed send is DEFERRED to the window's expiry (see
        // [`DeferredWake`]), not dropped — the leading-edge wake fires before
        // the rest of the burst exists, so a dropped trailing edge would
        // silence every later write of the window. A store error fails OPEN
        // (send anyway): push is a best-effort wake hint, and losing the
        // gate must not lose the wake.
        // ALERT-class sends get their own independent coalescing window
        // (`alert_coalesce_secs`) instead of `apns_coalesce_secs` — they skip
        // the daily budget, so this window is the only anti-spam guard they
        // have.
        let coalesce_window_secs = ctx.coalesce_window_secs(&token_entry.platform, is_alert);
        match ctx
            .store
            .check_and_stamp_wake(
                &token_entry.token,
                topic,
                kind_label,
                now,
                coalesce_window_secs as i64,
            )
            .await
        {
            Ok(crate::push_store::WakeGate::Allowed) => {}
            Ok(crate::push_store::WakeGate::Suppressed { retry_in_secs }) => {
                let short = token_entry.token.chars().take(20).collect::<String>();
                tracing::debug!(
                    topic = %topic_short,
                    platform = %token_entry.platform,
                    kind = kind_label,
                    "Deferring wake for token={short}... ({retry_in_secs}s left in {coalesce_window_secs}s window)"
                );
                ctx.metrics.push_sent(&token_entry.platform, "coalesced");
                // Upsert: newer suppressed notifies refresh the dial hints;
                // due_at converges on the same window expiry either way.
                deferred.insert(
                    (token_entry.token.clone(), topic.to_string(), is_alert),
                    DeferredWake {
                        due_at: Instant::now() + Duration::from_secs(retry_in_secs as u64),
                        platform: token_entry.platform.clone(),
                        peer_addrs: peer_addrs.to_vec(),
                    },
                );
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    topic = %topic_short,
                    platform = %token_entry.platform,
                    kind = kind_label,
                    "Wake-coalescing check failed, sending anyway: {e}"
                );
            }
        }

        send_wake(
            ctx,
            topic,
            &token_entry.token,
            &token_entry.platform,
            is_alert,
            peer_addrs,
        )
        .await;
    }
}

/// Budget-gate and send one wake whose coalescing window already allowed it
/// (and stamped). Shared by the leading-edge path (`fire_notifications`) and
/// the trailing-edge flush (`flush_due_deferred`), so both charge the daily
/// budget identically. Every path that ends with nothing delivered — budget
/// denial, budget-check error, permanent send failure, failure to even queue
/// a retry — refunds the wake stamp, so an undelivered wake never blocks the
/// device's next one.
async fn send_wake(
    ctx: &SendCtx<'_>,
    topic: &str,
    token: &str,
    platform: &str,
    is_alert: bool,
    peer_addrs: &[String],
) {
    let store = ctx.store;
    let topic_short = crate::short_topic(topic);
    let kind_label = wake_kind(is_alert);
    let short = token.chars().take(20).collect::<String>();
    let now = now_unix();
    let today = now / 86_400;

    // Refund the coalescing stamp when this send ends up not delivering —
    // absence of a stamp means "no recent wake", which is exactly the truth.
    let unstamp = || async {
        if let Err(e) = store.unstamp_wake(token, topic, kind_label).await {
            tracing::debug!("unstamp_wake failed for {short}...: {e}");
        }
    };

    // Enforce the per-token daily silent-push budget. Beyond it the platform
    // would throttle/drop wakes anyway; skipping here keeps a spammy or hostile
    // notifier from burning a device's budget and silencing legitimate wakes.
    // The device still catches up on its next foreground resume / periodic sync.
    //
    // ALERT-class sends bypass this entirely — an unbudgeted alert is the
    // whole point of the SyncNotify-visible signal; the per-device coalescing
    // window is its only cap.
    if !is_alert {
        match store.charge_daily_budget(token, today).await {
            Ok(true) => {}
            Ok(false) => {
                tracing::warn!(
                    topic = %topic_short,
                    platform = %platform,
                    kind = kind_label,
                    outcome = "budget_denied",
                    "Daily push budget exhausted for token={short}...; skipping wake"
                );
                ctx.metrics.push_sent(platform, "budget_denied");
                unstamp().await;
                return;
            }
            Err(e) => {
                tracing::error!(
                    topic = %topic_short,
                    platform = %platform,
                    kind = kind_label,
                    outcome = "budget_check_error",
                    error = %e,
                    "Daily budget check failed"
                );
                // Fail closed on the budget check: skip rather than risk unbounded sends.
                ctx.metrics.push_sent(platform, "budget_check_error");
                unstamp().await;
                return;
            }
        }
    }

    let result = match platform {
        "Fcm" => ctx.sender.send_fcm(token, topic, peer_addrs).await,
        "Apns" => ctx.sender.send_apns(token, topic, peer_addrs, is_alert).await,
        other => {
            tracing::warn!("Unknown push platform: {other}");
            return;
        }
    };

    match result {
        PushResult::Sent => {
            tracing::info!(
                topic = %topic_short,
                platform = %platform,
                kind = kind_label,
                outcome = "ok",
                "Push sent to token={short}..."
            );
            ctx.metrics.push_sent(platform, "ok");
            // A fresh send succeeded — clear any stale retry row
            // that had been queued from an earlier failure for the
            // same (topic, token). Latest peer_addrs already won
            // via the debouncer; this just sweeps the stale row.
            if let Err(e) = store.remove_retry(topic, token).await {
                tracing::debug!("remove_retry after Sent failed for {short}...: {e}");
            }
        }
        PushResult::TokenInvalid { reason } => {
            tracing::info!(
                topic = %topic_short,
                platform = %platform,
                kind = kind_label,
                outcome = "token_invalid",
                reason = ?reason,
                "Pruning invalid push token {short}..."
            );
            ctx.metrics.push_sent(platform, "token_invalid");
            if let Err(e) = store.remove_token(token).await {
                tracing::error!("Failed to remove invalid token: {e}");
            }
            // Token is dead; corresponding retry rows are now
            // orphans that would never succeed. Sweep them.
            if let Err(e) = store.purge_retries_for_token(token).await {
                tracing::error!("Failed to purge retries for invalidated token: {e}");
            }
        }
        PushResult::Transient(err) => {
            let now = now_unix();
            let (error_kind, error_code, error_body, retry_after) = classify_transient(&err);

            let next_delay = match compute_delay(1, retry_after) {
                Some(d) => d,
                None => unreachable!("compute_delay(1, _) is always Some"),
            };
            let next_attempt_at = now + next_delay.as_secs() as i64;

            tracing::warn!(
                topic = %topic_short,
                platform = %platform,
                kind = kind_label,
                outcome = "transient",
                error = ?err,
                "Push transient failure for {short}...; queueing retry #1/7 in ~{next_delay:?}"
            );
            ctx.metrics.push_sent(platform, "transient");
            let peer_addrs_json =
                serde_json::to_string(peer_addrs).unwrap_or_else(|_| "[]".to_string());
            if let Err(e) = store
                .enqueue_retry(
                    topic,
                    token,
                    platform,
                    is_alert,
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
                tracing::error!("Failed to enqueue retry: {e}");
                // Nothing was sent and nothing will retry — refund the
                // stamp so the next write isn't coalesced into the void.
                unstamp().await;
                return;
            }
            nudge_tx_send(ctx.nudge_tx);
        }
        PushResult::Permanent(err) => {
            tracing::error!(
                topic = %topic_short,
                platform = %platform,
                kind = kind_label,
                outcome = "permanent",
                error = ?err,
                "Push permanent failure for {short}...; dropping (misconfiguration won't resolve itself)"
            );
            ctx.metrics.push_sent(platform, "permanent");
            // Clear any retry row that might be in flight for this
            // (topic, token) — once we've classified the failure as
            // permanent, retries won't help.
            let _ = store.remove_retry(topic, token).await;
            // Nothing was delivered; don't let the dead send hold the window.
            unstamp().await;
        }
    }
}

/// Non-blocking retry-worker nudge; a full channel just means the worker is
/// already scheduled to wake.
fn nudge_tx_send(tx: &mpsc::Sender<RetryNudge>) {
    let _ = tx.try_send(RetryNudge);
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
        t.push('…');
        t
    }
}

// ── Retry worker ────────────────────────────────────────────────────

async fn retry_worker_loop(
    store: Arc<PushStore>,
    sender: Arc<PushSender>,
    mut nudge_rx: mpsc::Receiver<RetryNudge>,
    metrics: RelayMetrics,
) {
    // Startup smear + stale purge. Without smear, a relay that was down
    // for an hour wakes up and slams FCM/APNs with every queued retry
    // within seconds.
    let now = now_unix();
    match store
        .reschedule_overdue_retries(now, RESTART_GRACE_SECS, RESTART_JITTER_SECS)
        .await
    {
        Ok(n) if n > 0 => tracing::info!(
            "Retry worker: smeared {n} overdue rows into +{}..{}s after startup",
            RESTART_GRACE_SECS,
            RESTART_GRACE_SECS + RESTART_JITTER_SECS
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!("reschedule_overdue_retries failed at startup: {e}"),
    }
    match store.purge_stale_retries(now, MAX_AGE_SECS).await {
        Ok(n) if n > 0 => {
            tracing::info!("Retry worker: purged {n} rows older than {MAX_AGE_SECS}s")
        }
        Ok(_) => {}
        Err(e) => tracing::warn!("purge_stale_retries failed at startup: {e}"),
    }

    loop {
        // Drain due rows. Soonest-first; bounded batch.
        let now = now_unix();
        let due = match store.fetch_due_retries(now, RETRY_BATCH_SIZE).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::error!("fetch_due_retries failed: {e}");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        for row in due {
            process_retry(&store, &sender, row, &metrics).await;
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

async fn process_retry(
    store: &PushStore,
    sender: &PushSender,
    row: RetryRow,
    metrics: &RelayMetrics,
) {
    // The row was inserted at attempts=N meaning "next retry is the
    // Nth". Now that we're firing it, the *result* should be processed
    // at attempts=N+1 if it fails again.
    //
    // Deliberately not gated by check_and_stamp_wake: this is a retry of an
    // already-decided send (the original fire_notifications call already
    // passed or bypassed the coalescing window), so re-applying it here would
    // just drop a send the caller already committed to.
    let short = row.token.chars().take(20).collect::<String>();
    let platform_label = row.platform.clone();
    let topic_short = crate::short_topic(&row.topic);
    // A retried alert must keep firing as an alert (and a retried silent
    // send must not get promoted into one) — `row.is_alert` is what the
    // original `fire_notifications` call decided, carried through the
    // `push_retries` row.
    let kind_label = if row.is_alert { "alert" } else { "silent" };

    let result = match row.platform.as_str() {
        "Fcm" => {
            sender
                .send_fcm(&row.token, &row.topic, &row.peer_addrs)
                .await
        }
        "Apns" => {
            sender
                .send_apns(&row.token, &row.topic, &row.peer_addrs, row.is_alert)
                .await
        }
        other => {
            tracing::warn!("Retry worker: unknown push platform {other}; dropping row");
            let _ = store.remove_retry(&row.topic, &row.token).await;
            return;
        }
    };

    match result {
        PushResult::Sent => {
            tracing::info!(
                topic = %topic_short,
                platform = %platform_label,
                kind = kind_label,
                outcome = "ok",
                "Push retry succeeded to token={short}... (attempt {})",
                row.attempts
            );
            metrics.push_sent(&platform_label, "ok");
            let _ = store.remove_retry(&row.topic, &row.token).await;
        }
        PushResult::TokenInvalid { reason } => {
            tracing::info!(
                topic = %topic_short,
                platform = %platform_label,
                kind = kind_label,
                outcome = "token_invalid",
                reason = ?reason,
                "Push retry hit TokenInvalid for {short}...; pruning token + retries"
            );
            metrics.push_sent(&platform_label, "token_invalid");
            let _ = store.remove_token(&row.token).await;
            let _ = store.purge_retries_for_token(&row.token).await;
        }
        PushResult::Permanent(err) => {
            tracing::error!(
                topic = %topic_short,
                platform = %platform_label,
                kind = kind_label,
                outcome = "permanent",
                error = ?err,
                "Push retry hit Permanent for {short}...; dropping"
            );
            metrics.push_sent(&platform_label, "permanent");
            let _ = store.remove_retry(&row.topic, &row.token).await;
        }
        PushResult::Transient(err) => {
            let now = now_unix();
            if push_retry::age_exceeded(row.first_failed_at, now) {
                tracing::info!(
                    topic = %topic_short,
                    platform = %platform_label,
                    kind = kind_label,
                    outcome = "age_exceeded",
                    "Push retry: dropping {short}... — age exceeded ({}s old)",
                    now - row.first_failed_at
                );
                metrics.push_sent(&platform_label, "age_exceeded");
                let _ = store.remove_retry(&row.topic, &row.token).await;
                return;
            }

            let next_attempts = row.attempts + 1;
            let (error_kind, error_code, error_body, retry_after) = classify_transient(&err);
            match compute_delay(next_attempts, retry_after) {
                Some(delay) => {
                    let next_attempt_at = now + delay.as_secs() as i64;
                    tracing::info!(
                        topic = %topic_short,
                        platform = %platform_label,
                        kind = kind_label,
                        outcome = "transient",
                        "Push retry attempt {} failed for {short}...; scheduling attempt {} in ~{:?}",
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
                    metrics.push_sent(&platform_label, "transient");
                    let _ = store
                        .enqueue_retry(
                            &row.topic,
                            &row.token,
                            &row.platform,
                            row.is_alert,
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
                    tracing::info!(
                        topic = %topic_short,
                        platform = %platform_label,
                        kind = kind_label,
                        outcome = "retry_budget_exhausted",
                        "Push retry budget exhausted for {short}... after {} attempts; dropping",
                        row.attempts
                    );
                    metrics.push_sent(&platform_label, "retry_budget_exhausted");
                    let _ = store.remove_retry(&row.topic, &row.token).await;
                }
            }
        }
    }
}
