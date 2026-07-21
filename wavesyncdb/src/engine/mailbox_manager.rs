//! Relay-mailbox client: append-on-write, watermark healing, and
//! drain-on-wake.
//!
//! The relay's mailbox is a durable, end-to-end-encrypted, append-only
//! per-topic log (see `wire::mailbox_protocol` and `mailbox_seal`). This
//! module is the engine side of it:
//!
//! * **Append (fast path)** — every local changeset is sealed and appended to
//!   the relay in parallel with the peer fan-out. The relay acks only after
//!   fsync, so an acked write is durable even if every peer (including this
//!   one) goes offline the next instant. Appends are NEVER gated on version
//!   comparisons: local `db_version`s come from the connection-side write
//!   counter while remote applies stamp through the persistent meta counter,
//!   and the two interleave over the same numeric range — a fresh local
//!   write can carry a number lower than the newest applied-remote stamp, so
//!   "version already covered" reasoning silently drops appends (found the
//!   hard way in e2e bring-up).
//! * **Watermark + unacked set (outage healing)** — there is deliberately no
//!   queue of sealed envelopes: the durable state IS the shadow tables. Each
//!   group tracks the exact set of local-write versions whose append is not
//!   yet acked (`mailbox_unacked`, in-memory) plus a persisted watermark W
//!   with the invariant **W < every unacked version**. A healing delta built
//!   from `shadow::get_changes_since(W)` therefore always covers everything
//!   un-appended, no matter how long a relay outage lasted. W advances to
//!   the current version stamp only when nothing local is outstanding, which
//!   keeps heal deltas small without ever claiming coverage it doesn't have.
//!   One startup heal per group covers writes stranded by a previous session
//!   that died before its appends acked (the unacked set does not survive a
//!   restart; W does).
//! * **Drain (wake path)** — on relay reservation and on resume/push-wake,
//!   fetch entries after the locally persisted cursor, decrypt, apply via the
//!   normal remote-changeset path (idempotent + commutative CRDT), and
//!   advance the cursor only after the applies commit.
//! * **Gap fallback** — when the fetch response proves the cursor's
//!   continuation is gone (entries aged out / evicted, relay log reset via
//!   epoch mismatch, cursor beyond the newest seq), fall back to the
//!   version-vector reconcile for the group. Convergence never depends on
//!   the mailbox alone.
//! * **Tamper policy** — an entry that fails AEAD is never applied. The drain
//!   advances past it (a poison entry must not wedge the mailbox forever)
//!   AND triggers the same group reconcile, so whatever data the entry
//!   carried still converges via the authenticated sync path.
//!
//! Note on Rule "HMAC on every message path": the mailbox path substitutes
//! AEAD-open for HMAC-verify. AEAD subsumes the guarantee (authenticity under
//! a key derived from the same group root; failure ⇒ payload never applied).
//! Do not add a separate HMAC on top.

use super::*;
use crate::mailbox_seal;
use crate::wire::mailbox_protocol::{
    MailboxEntry, MailboxRequest, MailboxResponse, b64, fetch_gap_detected,
};

/// Fetch page bounds. 256 entries / 4 MiB per page keeps a single response
/// comfortably under the protocol's 16 MiB frame cap while letting a backlog
/// drain in few round-trips.
const DRAIN_PAGE_MAX_ENTRIES: u32 = 256;
const DRAIN_PAGE_MAX_BYTES: u64 = 4 * 1024 * 1024;

/// Target serialized size for one healing-delta chunk. Deliberately well
/// under the relay's default 4 MiB per-entry cap (which is operator-tunable
/// and not known client-side).
const HEAL_CHUNK_BYTES: usize = 1024 * 1024;

/// An in-flight append, keyed by its outbound request id.
pub(crate) struct MailboxAppendCtx {
    pub effective_topic: String,
    /// Fast path: the local `db_version` this entry carries (removed from
    /// `mailbox_unacked` on ack). `None` for healing-delta chunks, whose
    /// coverage is tracked on `GroupState::mailbox_heal` instead.
    pub version: Option<u64>,
}

/// An active healing-delta batch (see `GroupState::mailbox_heal`). The
/// watermark moves to `to` only when every chunk is acked. (The build's
/// `from` is the watermark itself and needs no copy here — it only travels
/// on `MailboxTaskMsg::HealReady` for logging.)
pub(crate) struct MailboxHeal {
    pub to: u64,
    /// Chunks still awaiting their ack.
    pub remaining: usize,
    /// The unacked local versions the delta covers — the snapshot of
    /// `mailbox_unacked` taken when the delta was built. Versions dispatched
    /// AFTER the build are not in the delta and must stay unacked.
    pub covers: Vec<u64>,
}

/// Messages sent from spawned mailbox tasks back to the engine loop.
pub(crate) enum MailboxTaskMsg {
    /// One drained page has been decrypted and applied; the cursor is
    /// persisted through `last_seq`.
    DrainPageDone {
        effective_topic: String,
        last_seq: u64,
        epoch: u64,
        applied: u64,
        truncated: bool,
        /// At least one entry failed AEAD (tampered / non-member garbage).
        tampered: bool,
        /// The page stopped early without consuming the failing entry —
        /// either an apply failed to commit (DB error) or a changeset
        /// targeted a not-yet-registered table (#104). The cursor was NOT
        /// advanced past that entry; the next drain re-fetches it.
        apply_failed: bool,
    },
    /// A healing delta is sealed and ready to append (possibly chunked).
    HealReady {
        effective_topic: String,
        from: u64,
        to: u64,
        covers: Vec<u64>,
        /// `(nonce_b64, ciphertext_b64)` per chunk. Empty = build failed;
        /// abandon and retry on a later tick.
        parts: Vec<(String, String)>,
    },
    /// `get_changes_since(W)` returned nothing although versions advanced —
    /// advance W directly so healing doesn't spin.
    HealEmpty {
        effective_topic: String,
        to: u64,
        covers: Vec<u64>,
    },
}

/// #107 dial helpers — pure so the state transitions are unit-testable.
///
/// Does this group's unacked backlog need a healing delta? Versions inside
/// their dial window are excluded: they are *waiting on acks*, not stranded,
/// and healing them early would re-serialize every deferred append through
/// the relay 3s after the write.
pub(super) fn heal_needed(
    unacked: &std::collections::BTreeSet<u64>,
    deferred: &std::collections::HashMap<u64, tokio::time::Instant>,
) -> bool {
    unacked.iter().any(|v| !deferred.contains_key(v))
}

/// Remove and return every deferred version whose dial window has expired.
pub(super) fn due_deferred(
    deferred: &mut std::collections::HashMap<u64, tokio::time::Instant>,
    now: tokio::time::Instant,
) -> Vec<u64> {
    let due: Vec<u64> = deferred
        .iter()
        .filter(|(_, deadline)| **deadline <= now)
        .map(|(v, _)| *v)
        .collect();
    for v in &due {
        deferred.remove(v);
    }
    due
}

/// Full peer-ack coverage arrived for `version`. If it was still inside its
/// dial window, settle it out of BOTH tracking sets — the append is skipped
/// for good (the data provably lives on the acking replicas, and the
/// watermark may now advance past it on the next idle tick). Returns whether
/// a skip actually happened (drives the diagnostics counter).
pub(super) fn settle_covered(
    unacked: &mut std::collections::BTreeSet<u64>,
    deferred: &mut std::collections::HashMap<u64, tokio::time::Instant>,
    version: u64,
) -> bool {
    if deferred.remove(&version).is_some() {
        unacked.remove(&version);
        true
    } else {
        false
    }
}

/// Greedy-pack `changes` into chunks of at most `max_bytes` serialized size
/// WITHOUT ever splitting one row's cells across chunks. Cells are grouped by
/// `(table, pk)` (first-appearance order) and whole rows are packed: a
/// receiver applies each sealed heal entry independently, so a chunk boundary
/// through a NEW row would deliver a partial insert — the split-insert
/// notification-loss window (#103). A single row larger than `max_bytes`
/// becomes its own oversized chunk (the relay's per-entry cap is
/// operator-tuned well above any realistic row).
fn chunk_rowwise(
    changes: Vec<crate::messages::ColumnChange>,
    max_bytes: usize,
) -> Vec<Vec<crate::messages::ColumnChange>> {
    let mut groups: Vec<Vec<crate::messages::ColumnChange>> = Vec::new();
    let mut index: std::collections::HashMap<(String, String), usize> =
        std::collections::HashMap::new();
    for c in changes {
        let key = (c.table.0.clone(), c.pk.0.clone());
        match index.get(&key) {
            Some(&i) => groups[i].push(c),
            None => {
                index.insert(key, groups.len());
                groups.push(vec![c]);
            }
        }
    }

    let mut chunks: Vec<Vec<crate::messages::ColumnChange>> = Vec::new();
    let mut chunk: Vec<crate::messages::ColumnChange> = Vec::new();
    let mut chunk_bytes = 0usize;
    for group in groups {
        let group_bytes: usize = group
            .iter()
            .map(|c| serde_json::to_vec(c).map(|v| v.len()).unwrap_or(0))
            .sum();
        if chunk_bytes + group_bytes > max_bytes && !chunk.is_empty() {
            chunks.push(std::mem::take(&mut chunk));
            chunk_bytes = 0;
        }
        chunk_bytes += group_bytes;
        chunk.extend(group);
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

/// The watermark invariant, factored out for unit tests: W may sit at
/// `candidate` only if no unacked local version is ≤ it — a healing delta is
/// built from `get_changes_since(W)`, so any unacked version at or below W
/// would be silently excluded from healing forever.
fn effective_watermark(candidate: u64, unacked: &std::collections::BTreeSet<u64>) -> u64 {
    match unacked.first() {
        Some(&lowest) => candidate.min(lowest.saturating_sub(1)),
        None => candidate,
    }
}

/// Outcome of decrypting + applying one fetched mailbox page.
pub(crate) struct DrainPageOutcome {
    /// Entries whose changesets were applied (excludes own-site round-trips
    /// and tampered entries, which are consumed but apply nothing).
    pub applied: u64,
    /// At least one entry failed AEAD (tampered / non-member garbage).
    pub tampered: bool,
    /// The page stopped early without consuming the failing entry — either a
    /// DB-level apply failure or a changeset for a not-yet-registered table.
    /// The cursor was NOT advanced past that entry; the next drain retries.
    pub apply_failed: bool,
    /// The seq the cursor was persisted through (the caller max()es it into
    /// the in-memory cursor; unchanged cursor when nothing was consumed).
    pub last_seq: u64,
}

/// Decrypt and apply one fetched mailbox page, persisting the cursor after
/// each consumed entry. Factored out of the drain task's spawn so the
/// per-entry consume/defer policy is unit-testable without a relay or swarm.
///
/// Consume policy per entry:
/// * AEAD/parse failure → consumed (a poison entry must not wedge the mailbox
///   forever; the caller triggers a group reconcile off `tampered`).
/// * own-site entry → consumed (round-tripped local write, nothing to apply).
/// * changeset touching an unregistered table → NOT consumed; the page stops
///   and a later drain (post-registration) retries. Consuming here is what
///   turned the join-time registration race into permanent data loss (#104).
/// * DB-level apply failure → NOT consumed; the page stops so the next drain
///   re-fetches the failing entry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_drain_page(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    db_version_cache: &std::sync::atomic::AtomicU64,
    notif_registry: &crate::registry::NotificationRegistry,
    notif_tx: &broadcast::Sender<crate::notify::Notification>,
    mailbox_key: &[u8; 32],
    topic_name: &str,
    local_site: crate::messages::NodeId,
    entries: &[MailboxEntry],
    epoch: u64,
) -> DrainPageOutcome {
    let mut applied = 0u64;
    let mut tampered = false;
    let mut apply_failed = false;
    let mut cursor = 0u64;
    for MailboxEntry {
        seq,
        nonce,
        ciphertext,
    } in entries
    {
        let opened = b64::decode(nonce)
            .and_then(|n| <[u8; mailbox_seal::NONCE_LEN]>::try_from(n).ok())
            .zip(b64::decode(ciphertext))
            .and_then(|(nonce, ct)| mailbox_seal::open(mailbox_key, topic_name, &nonce, &ct).ok())
            .and_then(|plain| serde_json::from_slice::<SyncChangeset>(&plain).ok());

        match opened {
            None => {
                // Tampered / non-member garbage / malformed. Never
                // applied; advance past it (the reconcile triggered
                // by the outcome recovers any real data it carried).
                tracing::warn!(
                    seq,
                    "mailbox entry failed authentication; skipping and falling back to reconcile"
                );
                tampered = true;
            }
            Some(changeset) if changeset.site_id == local_site => {
                // Our own entry, round-tripped. Nothing to apply.
            }
            Some(changeset) => {
                // Defer — never consume — an entry touching a table the app
                // hasn't registered (yet). `apply_remote_changeset` skips
                // unregistered tables but still commits the rest, so treating
                // its `committed` as "delivered" would advance the cursor past
                // data that was never applied — a transient join-time
                // registration race would become permanent, group-wide data
                // loss (#104). Not-delivered keeps the entry in the mailbox;
                // the drain retries after registration completes (drains are
                // also gated on `registry_is_ready`).
                if let Some(missing) = changeset
                    .changes
                    .iter()
                    .find(|c| registry.get(&c.table.0).is_none())
                {
                    tracing::warn!(
                        seq,
                        table = %missing.table.0,
                        "mailbox entry targets unregistered table; deferring drain without consuming the entry"
                    );
                    apply_failed = true;
                    break;
                }
                let notify_ctx = sync_handler::NotifyCtx {
                    registry: notif_registry,
                    tx: notif_tx,
                };
                let source = crate::messages::ChangeSource::Remote {
                    peer_site: changeset.site_id,
                };
                let committed = apply_remote_changeset(
                    db,
                    change_tx,
                    registry,
                    &changeset.changes,
                    Some(db_version_cache),
                    source,
                    Some(notify_ctx),
                )
                .await;
                if !committed {
                    // DB-level failure: stop WITHOUT advancing the
                    // cursor past this entry so the next drain
                    // retries it.
                    apply_failed = true;
                    break;
                }
                applied += 1;
            }
        }
        cursor = *seq;
        // Persist after the apply committed — crash between apply
        // and cursor write re-fetches (idempotent), never skips.
        if let Err(e) = shadow::set_mailbox_cursor(db, cursor, epoch).await {
            tracing::warn!("failed to persist mailbox cursor: {e}");
            break;
        }
    }
    let last_seq = if cursor > 0 {
        cursor
    } else {
        entries
            .first()
            .map(|e| e.seq.saturating_sub(1))
            .unwrap_or(0)
    };
    DrainPageOutcome {
        applied,
        tampered,
        apply_failed,
        last_seq,
    }
}

impl EngineRunner {
    /// The relay peer to talk mailbox to, if one is connected.
    fn mailbox_relay_peer(&self) -> Option<libp2p::PeerId> {
        match self.relay_state {
            RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } => Some(relay_peer_id),
            _ => None,
        }
    }

    /// Recompute the group's watermark toward `candidate` (or away from it,
    /// if an unacked version forces a rewind), persisting on change.
    async fn set_mailbox_watermark(&mut self, effective_topic: &str, candidate: u64) {
        let Some(g) = self.groups.get_mut(effective_topic) else {
            return;
        };
        let w = effective_watermark(candidate, &g.mailbox_unacked);
        if w == g.mailbox_acked_version {
            return;
        }
        g.mailbox_acked_version = w;
        let db = g.db.clone();
        // Persisted inline (the loop already awaits DB work in its arms) so
        // watermark writes stay ordered — a spawned persist could race an
        // older value over a newer one.
        if let Err(e) = shadow::set_mailbox_acked_version(&db, w).await {
            tracing::warn!("failed to persist mailbox watermark: {e}");
        }
    }

    /// Fast-path append: seal `changeset` and send it to the relay's mailbox.
    /// Called from `handle_local_changeset` alongside the peer fan-out (in
    /// parallel — the fan-out never waits on the relay round-trip). If the
    /// relay isn't reachable, the version is still recorded as unacked so
    /// the healing path covers it on reconnect.
    ///
    /// With the #107 dial configured and at least one eligible peer
    /// connected, the append is instead *deferred*: the version enters the
    /// group's dial window and either settles by full peer-ack coverage
    /// (append skipped — `note_mailbox_covered_by_acks`) or expires into a
    /// real append on the maintenance tick. The deferral decision uses acks
    /// and time ONLY — never version comparisons (see the module-doc
    /// landmine).
    pub(super) async fn maybe_append_to_mailbox(
        &mut self,
        effective_topic: &str,
        changeset: &SyncChangeset,
    ) {
        if !self.config.mailbox_enabled {
            return;
        }
        let dial = self.config.mailbox_append_after;
        let has_eligible_peers =
            dial.is_some() && !self.eligible_push_peers(effective_topic).is_empty();
        let Some(g) = self.groups.get_mut(effective_topic) else {
            return;
        };
        // No passphrase → no key to seal with → no mailbox for this group.
        if g.group_key.is_none() {
            return;
        }
        let version = changeset.db_version;
        g.mailbox_unacked.insert(version);

        // Restore the W < min(unacked) invariant BEFORE anything can build a
        // heal delta: local versions come from the connection-side write
        // counter and can sit below stamps the remote-apply path already
        // produced, so this new version may be at or below the current W.
        let w = g.mailbox_acked_version;
        self.set_mailbox_watermark(effective_topic, w).await;

        if let Some(delay) = dial
            && has_eligible_peers
        {
            if let Some(g) = self.groups.get_mut(effective_topic) {
                g.mailbox_deferred
                    .insert(version, tokio::time::Instant::now() + delay);
                tracing::debug!(
                    topic = %short_topic(effective_topic),
                    version,
                    delay_ms = delay.as_millis() as u64,
                    "mailbox append deferred: waiting for peer acks (#107 dial)"
                );
            }
            return;
        }
        self.append_to_mailbox_now(effective_topic, changeset).await;
    }

    /// The unconditional send half of the append path: seal and dispatch one
    /// changeset to the relay's mailbox. The version is expected to already
    /// be in `mailbox_unacked` (inserted by `maybe_append_to_mailbox`, which
    /// is the only writer of that set for the fast path); re-dispatch after
    /// a dial-window expiry goes through here too.
    pub(super) async fn append_to_mailbox_now(
        &mut self,
        effective_topic: &str,
        changeset: &SyncChangeset,
    ) {
        let relay_peer = self.mailbox_relay_peer();
        let Some(g) = self.groups.get_mut(effective_topic) else {
            return;
        };
        let Some(ref gk) = g.group_key else {
            return;
        };
        let version = changeset.db_version;
        let mailbox_key = gk.mailbox_key();
        let topic_name = g.topic_name.clone();

        let Some(relay_peer) = relay_peer else {
            tracing::debug!(
                topic = %short_topic(effective_topic),
                version,
                "mailbox append deferred: no relay connection (healing will cover)"
            );
            return;
        };

        let Ok(plaintext) = serde_json::to_vec(changeset) else {
            return;
        };
        let sealed = match mailbox_seal::seal(&mailbox_key, &topic_name, &plaintext) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(topic = %short_topic(effective_topic), "mailbox seal failed: {e}");
                return;
            }
        };
        let request = MailboxRequest::Append {
            topic: topic_name,
            nonce: b64::encode(&sealed.nonce),
            ciphertext: b64::encode(&sealed.ciphertext),
        };
        let request_id = self
            .swarm
            .behaviour_mut()
            .mailbox
            .send_request(&relay_peer, request);
        tracing::debug!(
            topic = %short_topic(effective_topic),
            version,
            ?request_id,
            "mailbox append sent"
        );
        self.pending_mailbox_appends.insert(
            request_id,
            MailboxAppendCtx {
                effective_topic: effective_topic.to_string(),
                version: Some(version),
            },
        );
    }

    /// Start a drain for every group. Called on relay reservation accepted
    /// and on resume/push-wake.
    pub(super) fn start_mailbox_drains_all(&mut self) {
        let topics: Vec<String> = self.groups.keys().cloned().collect();
        for topic in topics {
            self.start_mailbox_drain(&topic);
        }
    }

    /// Issue the first fetch of a drain session for one group. No-op when a
    /// drain is already in flight (reservation-accepted and push-wake can
    /// land together — duplicate concurrent drains would double-apply and
    /// double-write the cursor for nothing).
    pub(super) fn start_mailbox_drain(&mut self, effective_topic: &str) {
        if !self.config.mailbox_enabled {
            return;
        }
        let Some(relay_peer) = self.mailbox_relay_peer() else {
            return;
        };
        let Some(g) = self.groups.get_mut(effective_topic) else {
            return;
        };
        // Never drain before the app has registered the group's schema: the
        // apply path rejects changesets for unregistered tables, and a drain
        // racing `register(...).sync()` at join time turned those transient
        // rejects into consumed-and-lost entries (#104). Readiness arrival
        // (GroupRegistryReady / the default group's Notify) re-kicks the
        // drain, so deferring here delays nothing once the schema lands.
        if !g.registry_is_ready {
            tracing::debug!(
                topic = %short_topic(effective_topic),
                "mailbox drain deferred: schema registry not ready"
            );
            return;
        }
        if g.group_key.is_none() || g.mailbox_drain_in_flight {
            return;
        }
        g.mailbox_drain_in_flight = true;
        g.mailbox_drain_applied = 0;
        let after_seq = g.mailbox_cursor;
        let request = MailboxRequest::Fetch {
            topic: g.topic_name.clone(),
            after_seq,
            max_entries: DRAIN_PAGE_MAX_ENTRIES,
            max_bytes: DRAIN_PAGE_MAX_BYTES,
        };
        tracing::debug!(
            topic = %short_topic(effective_topic),
            after_seq,
            "mailbox drain: fetching"
        );
        let request_id = self
            .swarm
            .behaviour_mut()
            .mailbox
            .send_request(&relay_peer, request);
        self.pending_mailbox_fetches
            .insert(request_id, (effective_topic.to_string(), after_seq));
    }

    /// Sync-context variant for the reservation-accepted handler (which
    /// cannot await): kick heals only; the freshness advance waits for the
    /// next maintenance tick.
    pub(super) fn start_mailbox_heals_all(&mut self) {
        if !self.config.mailbox_enabled || self.mailbox_relay_peer().is_none() {
            return;
        }
        let topics: Vec<String> = self.groups.keys().cloned().collect();
        for topic in topics {
            self.maybe_heal_mailbox(&topic);
        }
    }

    /// Kick the healing path for every group that needs it, and advance idle
    /// groups' watermark freshness. Cheap no-op in the steady state; called
    /// from the periodic redeliver tick and after drains complete.
    pub(super) async fn mailbox_maintenance_tick(&mut self) {
        if !self.config.mailbox_enabled || self.mailbox_relay_peer().is_none() {
            return;
        }
        let topics: Vec<String> = self.groups.keys().cloned().collect();
        for topic in topics {
            self.maybe_heal_mailbox(&topic);
            // Freshness: with nothing outstanding, everything up to the
            // current stamp is covered (local writes acked; remote-origin
            // stamps are the origin peer's job) — advancing W keeps future
            // heal deltas small. Gated on the startup heal having run:
            // before it, W's lag is exactly what covers a previous session's
            // stranded writes.
            let advance = self.groups.get(&topic).and_then(|g| {
                let idle = g.mailbox_startup_healed
                    && g.mailbox_heal.is_none()
                    && g.mailbox_unacked.is_empty()
                    && !self
                        .pending_mailbox_appends
                        .values()
                        .any(|ctx| ctx.effective_topic == topic);
                if !idle || g.group_key.is_none() {
                    return None;
                }
                let current = g
                    .db_version_cache
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .max(g.local_db_version);
                (current > g.mailbox_acked_version).then_some(current)
            });
            if let Some(current) = advance {
                self.set_mailbox_watermark(&topic, current).await;
            }
        }
    }

    /// Spawn a healing-delta build for one group if it needs one: either the
    /// one-shot startup heal (covers a previous session's stranded writes)
    /// or unacked versions with no append in flight (relay outage / failed
    /// appends).
    fn maybe_heal_mailbox(&mut self, effective_topic: &str) {
        let has_outstanding = self
            .pending_mailbox_appends
            .values()
            .any(|ctx| ctx.effective_topic == effective_topic);
        let Some(g) = self.groups.get_mut(effective_topic) else {
            return;
        };
        if g.group_key.is_none()
            || !g.registry_is_ready
            || has_outstanding
            || g.mailbox_heal.is_some()
        {
            return;
        }
        let watermark = g.mailbox_acked_version;
        let current = g
            .db_version_cache
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(g.local_db_version);
        let needs_startup_heal = !g.mailbox_startup_healed && current > watermark;
        let needs_unacked_heal = !g.mailbox_unacked.is_empty();
        if !needs_startup_heal && !needs_unacked_heal {
            // Nothing could be stranded: mark the startup pass done so the
            // freshness advance may engage.
            g.mailbox_startup_healed = true;
            return;
        }

        let covers: Vec<u64> = g.mailbox_unacked.iter().copied().collect();
        let db = g.db.clone();
        let registry = g.registry.clone();
        let site_id = g.site_id;
        let topic = effective_topic.to_string();
        let topic_name = g.topic_name.clone();
        let mailbox_key = g.group_key.as_ref().map(|gk| gk.mailbox_key());
        let tx = self.mailbox_task_tx.clone();
        // Reserve the heal slot before spawning so a second tick can't spawn
        // a duplicate build while this one is reading the shadow tables.
        g.mailbox_heal = Some(MailboxHeal {
            to: current,
            remaining: 0,
            covers: covers.clone(),
        });

        tokio::spawn(async move {
            let Some(mailbox_key) = mailbox_key else {
                return;
            };
            let changes = match shadow::get_changes_since(&db, &registry, watermark).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("mailbox heal: get_changes_since failed: {e}");
                    // Abandon via an empty HealReady.
                    let _ = tx
                        .send(MailboxTaskMsg::HealReady {
                            effective_topic: topic,
                            from: watermark,
                            to: current,
                            covers,
                            parts: Vec::new(),
                        })
                        .await;
                    return;
                }
            };
            if changes.is_empty() {
                let _ = tx
                    .send(MailboxTaskMsg::HealEmpty {
                        effective_topic: topic,
                        to: current,
                        covers,
                    })
                    .await;
                return;
            }

            // Greedy-chunk by serialized size so no single entry exceeds the
            // relay's per-entry cap — but never split one row's cells across
            // chunks (`chunk_rowwise`): each sealed entry is applied
            // independently, and a partial NEW row in one entry loses its
            // insert notification to the split-insert window (#103).
            // Receivers dedup via the CRDT comparator, so chunks (and
            // heal/fast-path overlap) are safe.
            let mut parts: Vec<(String, String)> = Vec::new();
            let mut ok = true;
            for chunk in chunk_rowwise(changes, HEAL_CHUNK_BYTES) {
                let changeset = SyncChangeset {
                    site_id,
                    db_version: current,
                    changes: chunk,
                };
                let Ok(plain) = serde_json::to_vec(&changeset) else {
                    ok = false;
                    break;
                };
                match mailbox_seal::seal(&mailbox_key, &topic_name, &plain) {
                    Ok(sealed) => {
                        parts.push((b64::encode(&sealed.nonce), b64::encode(&sealed.ciphertext)));
                    }
                    Err(e) => {
                        tracing::warn!("mailbox heal: seal failed: {e}");
                        ok = false;
                        break;
                    }
                }
            }

            let _ = tx
                .send(MailboxTaskMsg::HealReady {
                    effective_topic: topic,
                    from: watermark,
                    to: current,
                    covers,
                    parts: if ok { parts } else { Vec::new() },
                })
                .await;
        });
    }

    /// A fully-acked healing delta: everything in its coverage snapshot is
    /// durably at the relay. Versions dispatched after the build stay
    /// unacked (they are not in the delta) and keep W rewound via the
    /// invariant.
    async fn complete_mailbox_heal(&mut self, effective_topic: &str, to: u64, covers: &[u64]) {
        if let Some(g) = self.groups.get_mut(effective_topic) {
            for v in covers {
                g.mailbox_unacked.remove(v);
            }
            g.mailbox_heal = None;
            g.mailbox_startup_healed = true;
        }
        self.set_mailbox_watermark(effective_topic, to).await;
    }

    /// Handle a mailbox request-response event from the swarm.
    pub(super) async fn handle_mailbox_event(
        &mut self,
        event: request_response::Event<MailboxRequest, MailboxResponse>,
    ) {
        match event {
            request_response::Event::Message {
                message:
                    request_response::Message::Response {
                        request_id,
                        response,
                    },
                ..
            } => {
                if let Some(ctx) = self.pending_mailbox_appends.remove(&request_id) {
                    self.handle_append_response(ctx, response).await;
                } else if let Some((topic, after_seq)) =
                    self.pending_mailbox_fetches.remove(&request_id)
                {
                    self.handle_fetch_response(&topic, after_seq, response)
                        .await;
                }
            }
            request_response::Event::Message {
                message: request_response::Message::Request { .. },
                peer,
                ..
            } => {
                // Only the relay serves the mailbox; a peer sending us a
                // mailbox request is confused or hostile. Drop it (the
                // channel closes unanswered).
                tracing::debug!("ignoring inbound mailbox request from {peer}");
            }
            request_response::Event::OutboundFailure {
                request_id, error, ..
            } => {
                if let Some(ctx) = self.pending_mailbox_appends.remove(&request_id) {
                    tracing::debug!(
                        topic = %short_topic(&ctx.effective_topic),
                        version = ctx.version,
                        "mailbox append failed: {error} (healing will cover)"
                    );
                    // Fast path: the version stays in `mailbox_unacked`.
                    // Heal chunk: abandon the whole batch; the covers stay
                    // unacked and a later heal retries. Sibling-chunk acks
                    // find their ctx gone and are ignored.
                    if ctx.version.is_none()
                        && let Some(g) = self.groups.get_mut(&ctx.effective_topic)
                    {
                        g.mailbox_heal = None;
                    }
                } else if let Some((topic, _)) = self.pending_mailbox_fetches.remove(&request_id) {
                    tracing::debug!(
                        topic = %short_topic(&topic),
                        "mailbox fetch failed: {error}"
                    );
                    if let Some(g) = self.groups.get_mut(&topic) {
                        g.mailbox_drain_in_flight = false;
                    }
                }
            }
            _ => {}
        }
    }

    async fn handle_append_response(&mut self, ctx: MailboxAppendCtx, response: MailboxResponse) {
        match response {
            MailboxResponse::Appended { seq, epoch: _ } => {
                tracing::debug!(
                    topic = %short_topic(&ctx.effective_topic),
                    seq,
                    version = ctx.version,
                    "mailbox append acked"
                );
                match ctx.version {
                    // Fast path: this exact local version is durable now.
                    Some(version) => {
                        let advance = if let Some(g) = self.groups.get_mut(&ctx.effective_topic) {
                            g.mailbox_unacked.remove(&version);
                            // With nothing outstanding, everything up to the
                            // current stamp is covered (see the maintenance
                            // tick for why this is sound only after the
                            // startup heal).
                            (g.mailbox_startup_healed
                                && g.mailbox_unacked.is_empty()
                                && g.mailbox_heal.is_none())
                            .then(|| {
                                g.db_version_cache
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    .max(g.local_db_version)
                            })
                        } else {
                            None
                        };
                        if let Some(current) = advance {
                            self.set_mailbox_watermark(&ctx.effective_topic, current)
                                .await;
                        }
                    }
                    // Heal chunk: count down the batch.
                    None => {
                        let done = match self
                            .groups
                            .get_mut(&ctx.effective_topic)
                            .and_then(|g| g.mailbox_heal.as_mut())
                        {
                            Some(heal) => {
                                heal.remaining = heal.remaining.saturating_sub(1);
                                (heal.remaining == 0)
                                    .then(|| (heal.to, std::mem::take(&mut heal.covers)))
                            }
                            // Batch was abandoned (a sibling failed) — ignore.
                            None => None,
                        };
                        if let Some((to, covers)) = done {
                            self.complete_mailbox_heal(&ctx.effective_topic, to, &covers)
                                .await;
                        }
                    }
                }
            }
            MailboxResponse::Error { kind, message } => {
                tracing::warn!(
                    topic = %short_topic(&ctx.effective_topic),
                    ?kind,
                    "mailbox append rejected: {message}"
                );
                // Fast path: version stays unacked; healing retries later
                // (rate limits and quota rejections are exactly the cases
                // where backing off to the periodic tick is right).
                if ctx.version.is_none()
                    && let Some(g) = self.groups.get_mut(&ctx.effective_topic)
                {
                    g.mailbox_heal = None;
                }
            }
            other => {
                tracing::debug!("unexpected mailbox append response: {other:?}");
            }
        }
    }

    async fn handle_fetch_response(
        &mut self,
        effective_topic: &str,
        after_seq: u64,
        response: MailboxResponse,
    ) {
        let entries = match response {
            MailboxResponse::Entries {
                entries,
                latest_seq,
                first_retained_seq,
                epoch,
                truncated,
            } => {
                let stored_epoch = self
                    .groups
                    .get(effective_topic)
                    .and_then(|g| g.mailbox_epoch);
                if fetch_gap_detected(
                    after_seq,
                    stored_epoch,
                    first_retained_seq,
                    latest_seq,
                    epoch,
                ) {
                    tracing::warn!(
                        topic = %short_topic(effective_topic),
                        after_seq,
                        first_retained_seq,
                        latest_seq,
                        "mailbox gap detected (entries aged out or relay log reset) — falling back to full reconcile"
                    );
                    self.diagnostics
                        .mailbox_gap_fallbacks
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // Reset the cursor to the edge of what's retained and
                    // adopt the (possibly new) epoch, then reconcile: the
                    // retained window still drains below, the reconcile
                    // recovers the lost middle.
                    let new_cursor = first_retained_seq.saturating_sub(1);
                    if let Some(g) = self.groups.get_mut(effective_topic) {
                        g.mailbox_cursor = new_cursor;
                        g.mailbox_epoch = Some(epoch);
                    }
                    if let Some(g) = self.groups.get(effective_topic) {
                        let db = g.db.clone();
                        if let Err(e) = shadow::set_mailbox_cursor(&db, new_cursor, epoch).await {
                            tracing::warn!("failed to persist mailbox cursor: {e}");
                        }
                    }
                    self.trigger_group_reconcile(effective_topic).await;
                } else if stored_epoch.is_none()
                    && let Some(g) = self.groups.get_mut(effective_topic)
                {
                    // First-ever drain: adopt the epoch (persisted with the
                    // cursor once the page applies).
                    g.mailbox_epoch = Some(epoch);
                }

                if entries.is_empty() {
                    self.finish_mailbox_drain(effective_topic).await;
                    return;
                }
                (entries, epoch, truncated)
            }
            MailboxResponse::Error { kind, message } => {
                tracing::debug!(
                    topic = %short_topic(effective_topic),
                    ?kind,
                    "mailbox fetch rejected: {message}"
                );
                if let Some(g) = self.groups.get_mut(effective_topic) {
                    g.mailbox_drain_in_flight = false;
                }
                return;
            }
            other => {
                tracing::debug!("unexpected mailbox fetch response: {other:?}");
                if let Some(g) = self.groups.get_mut(effective_topic) {
                    g.mailbox_drain_in_flight = false;
                }
                return;
            }
        };
        let (entries, epoch, truncated) = entries;

        // Decrypt + apply off-loop (Rule: never hold the swarm across DB
        // awaits). The task reports back via `mailbox_task_tx` so the loop
        // can page the next fetch / finish the drain.
        let Some(g) = self.groups.get(effective_topic) else {
            return;
        };
        let Some(mailbox_key) = g.group_key.as_ref().map(|gk| gk.mailbox_key()) else {
            return;
        };
        let db = g.db.clone();
        let change_tx = g.change_tx.clone();
        let registry = g.registry.clone();
        let cache = g.db_version_cache.clone();
        let notif_registry = g.notification_registry.clone();
        let notif_tx = g.notification_tx.clone();
        let topic_name = g.topic_name.clone();
        let local_site = g.site_id;
        let topic = effective_topic.to_string();
        let tx = self.mailbox_task_tx.clone();

        tokio::spawn(async move {
            let outcome = apply_drain_page(
                &db,
                &change_tx,
                &registry,
                &cache,
                &notif_registry,
                &notif_tx,
                &mailbox_key,
                &topic_name,
                local_site,
                &entries,
                epoch,
            )
            .await;
            let _ = tx
                .send(MailboxTaskMsg::DrainPageDone {
                    effective_topic: topic,
                    last_seq: outcome.last_seq,
                    epoch,
                    applied: outcome.applied,
                    truncated,
                    tampered: outcome.tampered,
                    apply_failed: outcome.apply_failed,
                })
                .await;
        });
    }

    /// Handle a message from a spawned mailbox task.
    pub(super) async fn handle_mailbox_task_msg(&mut self, msg: MailboxTaskMsg) {
        match msg {
            MailboxTaskMsg::DrainPageDone {
                effective_topic,
                last_seq,
                epoch,
                applied,
                truncated,
                tampered,
                apply_failed,
            } => {
                if let Some(g) = self.groups.get_mut(&effective_topic) {
                    g.mailbox_cursor = g.mailbox_cursor.max(last_seq);
                    g.mailbox_epoch = Some(epoch);
                    g.mailbox_drain_applied += applied;
                }
                if tampered {
                    self.trigger_group_reconcile(&effective_topic).await;
                }
                if apply_failed {
                    // Leave the session; the next wake retries from the
                    // persisted cursor.
                    if let Some(g) = self.groups.get_mut(&effective_topic) {
                        g.mailbox_drain_in_flight = false;
                    }
                    return;
                }
                if truncated {
                    // Next page. Re-arm the fetch from the advanced cursor.
                    let next = self
                        .groups
                        .get(&effective_topic)
                        .map(|g| (g.topic_name.clone(), g.mailbox_cursor));
                    if let (Some((topic_name, after_seq)), Some(relay_peer)) =
                        (next, self.mailbox_relay_peer())
                    {
                        let request = MailboxRequest::Fetch {
                            topic: topic_name,
                            after_seq,
                            max_entries: DRAIN_PAGE_MAX_ENTRIES,
                            max_bytes: DRAIN_PAGE_MAX_BYTES,
                        };
                        let request_id = self
                            .swarm
                            .behaviour_mut()
                            .mailbox
                            .send_request(&relay_peer, request);
                        self.pending_mailbox_fetches
                            .insert(request_id, (effective_topic, after_seq));
                    } else if let Some(g) = self.groups.get_mut(&effective_topic) {
                        g.mailbox_drain_in_flight = false;
                    }
                } else {
                    self.finish_mailbox_drain(&effective_topic).await;
                }
            }
            MailboxTaskMsg::HealReady {
                effective_topic,
                from,
                to,
                covers,
                parts,
            } => {
                if parts.is_empty() {
                    // Build/seal failure — abandon, retry on a later tick.
                    if let Some(g) = self.groups.get_mut(&effective_topic) {
                        g.mailbox_heal = None;
                    }
                    return;
                }
                let (Some(relay_peer), Some(g)) = (
                    self.mailbox_relay_peer(),
                    self.groups.get_mut(&effective_topic),
                ) else {
                    if let Some(g) = self.groups.get_mut(&effective_topic) {
                        g.mailbox_heal = None;
                    }
                    return;
                };
                g.mailbox_heal = Some(MailboxHeal {
                    to,
                    remaining: parts.len(),
                    covers,
                });
                let topic_name = g.topic_name.clone();
                tracing::info!(
                    topic = %short_topic(&effective_topic),
                    from,
                    to,
                    parts = parts.len(),
                    "mailbox heal: appending delta for un-acked versions"
                );
                for (nonce, ciphertext) in parts {
                    let request = MailboxRequest::Append {
                        topic: topic_name.clone(),
                        nonce,
                        ciphertext,
                    };
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .mailbox
                        .send_request(&relay_peer, request);
                    self.pending_mailbox_appends.insert(
                        request_id,
                        MailboxAppendCtx {
                            effective_topic: effective_topic.clone(),
                            version: None,
                        },
                    );
                }
            }
            MailboxTaskMsg::HealEmpty {
                effective_topic,
                to,
                covers,
            } => {
                self.complete_mailbox_heal(&effective_topic, to, &covers)
                    .await;
            }
        }
    }

    /// Final page applied: emit the completion event, release the session,
    /// and use the quiet moment to heal any append backlog.
    async fn finish_mailbox_drain(&mut self, effective_topic: &str) {
        let applied = if let Some(g) = self.groups.get_mut(effective_topic) {
            g.mailbox_drain_in_flight = false;
            std::mem::take(&mut g.mailbox_drain_applied)
        } else {
            0
        };
        tracing::info!(
            topic = %short_topic(effective_topic),
            entries = applied,
            "mailbox drain complete"
        );
        self.diagnostics
            .mailbox_entries_drained
            .fetch_add(applied, std::sync::atomic::Ordering::Relaxed);
        self.emit_network_event(crate::network_status::NetworkEvent::MailboxDrained {
            topic: effective_topic.to_string(),
            entries: applied,
        });
        self.maybe_heal_mailbox(effective_topic);
    }

    /// Group-scoped equivalent of `EngineCommand::RequestFullSync`: clear the
    /// group's peer cursors so the next version-vector round asks from 0,
    /// then kick a sync pass. Used by the gap and tamper fallbacks — the
    /// paths where mailbox data was lost or unusable and correctness now
    /// rides on the reconcile.
    async fn trigger_group_reconcile(&mut self, effective_topic: &str) {
        if let Some(g) = self.groups.get_mut(effective_topic) {
            g.peer_db_versions.clear();
            g.peer_reported_versions.clear();
            g.pending_sync_peers.clear();
        }
        self.sync_all_known_peers().await;
    }
}

#[cfg(test)]
mod tests {
    use super::effective_watermark;
    use super::*;
    use crate::messages::{ColumnChange, NodeId};
    use crate::registry::{TableMeta, TableRegistry};
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicU64;

    const MAILBOX_KEY: [u8; 32] = [7u8; 32];
    const TOPIC: &str = "wavesync2-drain-test";
    const LOCAL_SITE: NodeId = NodeId([1u8; 16]);
    const REMOTE_SITE: NodeId = NodeId([2u8; 16]);

    /// #107: the dial must default OFF — always-append is the 0.9.0
    /// behavior and stays the default until field-proven.
    #[test]
    fn dial_defaults_to_none() {
        assert!(EngineConfig::default().mailbox_append_after.is_none());
    }

    /// #107 helper contracts. `deferred` is always a subset of `unacked`
    /// (the watermark invariant rides on unacked; deferred only marks
    /// which members are inside their dial window).
    #[test]
    fn dial_helpers() {
        use std::collections::HashMap;
        use tokio::time::Instant;

        // heal_needed: a backlog that is ALL deferred must not trigger the
        // unacked heal (the maintenance tick would otherwise append every
        // deferred version 3s after the write and defeat the dial).
        let now = Instant::now();
        let unacked: BTreeSet<u64> = [5, 7].into_iter().collect();
        let mut deferred: HashMap<u64, Instant> = HashMap::new();
        deferred.insert(5, now);
        deferred.insert(7, now);
        assert!(!heal_needed(&unacked, &deferred));
        // ...but one non-deferred unacked version (real outage backlog)
        // does need healing.
        deferred.remove(&7);
        assert!(heal_needed(&unacked, &deferred));
        // Empty unacked never needs the unacked heal.
        assert!(!heal_needed(&BTreeSet::new(), &HashMap::new()));

        // due_deferred: drains exactly the expired deadlines.
        let later = now + std::time::Duration::from_secs(60);
        let mut d: HashMap<u64, Instant> = HashMap::new();
        d.insert(1, now); // due
        d.insert(2, later); // not due
        let mut due = due_deferred(&mut d, now);
        due.sort_unstable();
        assert_eq!(due, vec![1]);
        assert!(d.contains_key(&2) && !d.contains_key(&1));

        // settle_covered: a fully-acked deferred version leaves BOTH sets
        // (skip); a non-deferred version is untouched (its append is
        // in-flight or done — acks say nothing about the relay).
        let mut unacked: BTreeSet<u64> = [3, 4].into_iter().collect();
        let mut deferred: HashMap<u64, Instant> = HashMap::new();
        deferred.insert(3, later);
        assert!(settle_covered(&mut unacked, &mut deferred, 3));
        assert!(!unacked.contains(&3) && deferred.is_empty());
        assert!(!settle_covered(&mut unacked, &mut deferred, 4));
        assert!(unacked.contains(&4));
    }

    async fn drain_test_db() -> DatabaseConnection {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        shadow::create_meta_table(&db).await.unwrap();
        crate::capture::ensure_capture_tables(&db).await.unwrap();
        crate::peer_tracker::create_peer_versions_table(&db)
            .await
            .unwrap();
        db
    }

    /// Registers `tasks` (table + shadow + registry meta) the way a real
    /// schema registration would, so applies against it commit for real.
    async fn register_tasks_table(db: &DatabaseConnection, registry: &TableRegistry) {
        use sea_orm::ConnectionTrait;
        db.execute_unprepared("CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT)")
            .await
            .unwrap();
        shadow::create_shadow_table(db, "tasks").await.unwrap();
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
    }

    fn changeset_for(table: &str, site: NodeId) -> SyncChangeset {
        SyncChangeset {
            site_id: site,
            db_version: 1,
            changes: vec![ColumnChange {
                table: table.into(),
                pk: "row-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("hello")),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 1,
                deleted_ts: None,
            }],
        }
    }

    fn sealed_entry(seq: u64, changeset: &SyncChangeset) -> MailboxEntry {
        let plain = serde_json::to_vec(changeset).unwrap();
        let sealed = mailbox_seal::seal(&MAILBOX_KEY, TOPIC, &plain).unwrap();
        MailboxEntry {
            seq,
            nonce: b64::encode(&sealed.nonce),
            ciphertext: b64::encode(&sealed.ciphertext),
        }
    }

    async fn run_page(
        db: &DatabaseConnection,
        registry: &TableRegistry,
        entries: &[MailboxEntry],
    ) -> DrainPageOutcome {
        let (change_tx, _) = broadcast::channel::<ChangeNotification>(16);
        let (notif_tx, _) = broadcast::channel::<crate::notify::Notification>(16);
        let notif_registry = crate::registry::NotificationRegistry::new();
        let cache = AtomicU64::new(0);
        apply_drain_page(
            db,
            &change_tx,
            registry,
            &cache,
            &notif_registry,
            &notif_tx,
            &MAILBOX_KEY,
            TOPIC,
            LOCAL_SITE,
            entries,
            3,
        )
        .await
    }

    /// #104 REGRESSION — a drained entry whose changeset targets a table the
    /// app has not registered YET (join-time race: the drain won against
    /// `register(...).sync()`) must NOT be consumed. Advancing the cursor
    /// past it means the entry is never re-fetched and the data is silently
    /// lost group-wide once no live peer still holds the rows.
    #[tokio::test]
    async fn drain_defers_unregistered_table_entries_without_consuming() {
        let db = drain_test_db().await;
        let registry = TableRegistry::new(); // schema not registered yet

        let entries = vec![sealed_entry(5, &changeset_for("tasks", REMOTE_SITE))];
        let outcome = run_page(&db, &registry, &entries).await;

        assert!(
            outcome.apply_failed,
            "unregistered-table entry must end the page as not-delivered"
        );
        assert_eq!(outcome.applied, 0);
        let meta = shadow::get_mailbox_meta(&db).await.unwrap();
        assert_eq!(
            meta.cursor, 0,
            "cursor must not be persisted past an entry whose table isn't registered — \
             consuming it here is permanent data loss (#104)"
        );
    }

    /// Companion to the #104 test: once the table IS registered, the same
    /// entry applies and the cursor advances normally.
    #[tokio::test]
    async fn drain_applies_registered_table_entries_and_advances_cursor() {
        let db = drain_test_db().await;
        let registry = TableRegistry::new();
        register_tasks_table(&db, &registry).await;

        let entries = vec![sealed_entry(5, &changeset_for("tasks", REMOTE_SITE))];
        let outcome = run_page(&db, &registry, &entries).await;

        assert!(!outcome.apply_failed);
        assert_eq!(outcome.applied, 1);
        assert_eq!(outcome.last_seq, 5);
        let meta = shadow::get_mailbox_meta(&db).await.unwrap();
        assert_eq!(meta.cursor, 5);
    }

    /// Policy lock-in: a tampered entry is consumed (poison must not wedge
    /// the mailbox) and flagged so the caller falls back to reconcile.
    #[tokio::test]
    async fn drain_consumes_tampered_entries_and_flags_reconcile() {
        let db = drain_test_db().await;
        let registry = TableRegistry::new();

        let entries = vec![MailboxEntry {
            seq: 5,
            nonce: b64::encode(&[0u8; mailbox_seal::NONCE_LEN]),
            ciphertext: b64::encode(b"garbage"),
        }];
        let outcome = run_page(&db, &registry, &entries).await;

        assert!(outcome.tampered);
        assert!(!outcome.apply_failed);
        assert_eq!(outcome.applied, 0);
        let meta = shadow::get_mailbox_meta(&db).await.unwrap();
        assert_eq!(meta.cursor, 5, "poison entries are skipped, not retried");
    }

    /// Policy lock-in: our own round-tripped entry is consumed without
    /// applying — even when its table isn't registered (there is nothing to
    /// lose: the data is already local).
    #[tokio::test]
    async fn drain_consumes_own_site_entries_without_applying() {
        let db = drain_test_db().await;
        let registry = TableRegistry::new();

        let entries = vec![sealed_entry(5, &changeset_for("tasks", LOCAL_SITE))];
        let outcome = run_page(&db, &registry, &entries).await;

        assert!(!outcome.apply_failed);
        assert_eq!(outcome.applied, 0);
        let meta = shadow::get_mailbox_meta(&db).await.unwrap();
        assert_eq!(meta.cursor, 5);
    }

    // ── heal chunker row alignment (#103) ──

    fn cell(table: &str, pk: &str, cid: &str, seq: u32) -> ColumnChange {
        ColumnChange {
            table: table.into(),
            pk: pk.into(),
            cid: cid.into(),
            val: Some(serde_json::json!("v")),
            site_id: REMOTE_SITE,
            col_version: 1,
            cl: 1,
            seq,
            db_version: 1,
            deleted_ts: None,
        }
    }

    fn cell_bytes(c: &ColumnChange) -> usize {
        serde_json::to_vec(c).unwrap().len()
    }

    /// #103 — a heal chunk boundary must fall between rows, never through
    /// one: each sealed entry applies independently, so a split NEW row
    /// re-creates the partial-insert notification loss.
    #[test]
    fn heal_chunks_never_split_a_row() {
        // Interleave two rows' cells so naive per-cell packing would split
        // both, and size the budget so the two rows can't share a chunk.
        let changes = vec![
            cell("t", "a", "c1", 0),
            cell("t", "b", "c1", 1),
            cell("t", "a", "c2", 2),
            cell("t", "b", "c2", 3),
            cell("t", "a", "c3", 4),
        ];
        let row_a_bytes: usize = changes
            .iter()
            .filter(|c| c.pk.0 == "a")
            .map(cell_bytes)
            .sum();
        let chunks = super::chunk_rowwise(changes, row_a_bytes + 1);

        assert!(chunks.len() >= 2, "budget forces at least two chunks");
        for chunk in &chunks {
            for pk in ["a", "b"] {
                let n = chunk.iter().filter(|c| c.pk.0 == pk).count();
                assert!(
                    n == 0 || n == (if pk == "a" { 3 } else { 2 }),
                    "row {pk} must appear whole in one chunk or not at all, found {n} cells"
                );
            }
        }
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, 5, "no cell may be dropped");
    }

    /// A single row larger than the budget still ships — as its own
    /// oversized chunk, never split.
    #[test]
    fn heal_chunk_oversized_row_ships_whole() {
        let changes = vec![
            cell("t", "big", "c1", 0),
            cell("t", "big", "c2", 1),
            cell("t", "big", "c3", 2),
        ];
        let chunks = super::chunk_rowwise(changes, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 3);
    }

    #[test]
    fn heal_chunks_pack_multiple_small_rows_together() {
        let changes = vec![
            cell("t", "a", "c1", 0),
            cell("t", "b", "c1", 1),
            cell("t", "c", "c1", 2),
        ];
        let chunks = super::chunk_rowwise(changes, usize::MAX);
        assert_eq!(chunks.len(), 1, "everything fits in one chunk");
        assert_eq!(chunks[0].len(), 3);
    }

    #[test]
    fn no_unacked_versions_lets_candidate_through() {
        let unacked = BTreeSet::new();
        assert_eq!(effective_watermark(7, &unacked), 7);
    }

    #[test]
    fn unacked_version_below_candidate_rewinds() {
        // A local write stamped 3 (connection-side counter) while the
        // remote-apply path already produced stamp 7: W must sit below 3 or
        // healing (`get_changes_since(W)`) would never pick the write up.
        let unacked = BTreeSet::from([3, 9]);
        assert_eq!(effective_watermark(7, &unacked), 2);
    }

    #[test]
    fn unacked_version_above_candidate_is_no_constraint() {
        let unacked = BTreeSet::from([9]);
        assert_eq!(effective_watermark(7, &unacked), 7);
    }

    #[test]
    fn unacked_version_zero_saturates() {
        let unacked = BTreeSet::from([0]);
        assert_eq!(effective_watermark(5, &unacked), 0);
    }
}
