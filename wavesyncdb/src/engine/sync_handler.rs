//! Sync request handling and remote changeset application.

use super::*;

impl EngineRunner {
    pub(super) fn handle_snapshot(
        &mut self,
        event: request_response::Event<crate::protocol::SyncRequest, crate::protocol::SyncResponse>,
    ) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    log::info!("Received sync request from peer {peer}: {request:?}");

                    match request {
                        SyncRequest::VersionVector {
                            my_db_version,
                            your_last_db_version,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            self.handle_version_vector_request(
                                peer,
                                channel,
                                my_db_version,
                                your_last_db_version,
                                peer_site_id,
                                peer_topic,
                                req_hmac,
                            );
                        }
                        SyncRequest::Push {
                            changeset,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            self.handle_push_request(
                                peer, channel, changeset, peer_topic, req_hmac,
                            );
                        }
                        SyncRequest::IdentityAnnounce {
                            app_id,
                            hmac: req_hmac,
                        } => {
                            self.handle_identity_announce_request(peer, channel, app_id, req_hmac);
                        }
                    }
                }
                request_response::Message::Response { response, .. } => {
                    // Response received — this peer no longer has an in-flight
                    // request in any group.
                    for g in self.groups.values_mut() {
                        g.pending_sync_peers.remove(&peer);
                    }
                    log::info!("Received sync response from peer {peer}");

                    match response {
                        crate::protocol::SyncResponse::ChangesetResponse {
                            changes,
                            my_db_version,
                            your_last_db_version,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: resp_hmac,
                        } => {
                            // Route to the group this response belongs to. The
                            // (PSK-derived) topic on the wire selects the group;
                            // an empty topic maps to the default (single-group /
                            // back-compat). HMAC is then verified with THAT
                            // group's key — the topic selects the key, it does
                            // not bypass verification (Rule 2.7).
                            let effective = if peer_topic.is_empty() {
                                self.default_effective_topic.clone()
                            } else {
                                peer_topic.clone()
                            };
                            let Some(g) = self.groups.get(&effective) else {
                                // Response for a topic none of our groups hold —
                                // silently ignore. One connection is shared across
                                // every group on the node, and a peer that serves a
                                // group we don't have may still share another group
                                // with us; rejecting it here would poison those
                                // shared groups too (Rule 2.8, multi-group).
                                log::debug!(
                                    "Ignoring sync response from {peer} for unknown topic {effective}"
                                );
                                return;
                            };

                            if let Some(ref gk) = g.group_key {
                                let tag = match resp_hmac {
                                    Some(t) => t,
                                    None => {
                                        log::debug!(
                                            "Rejecting unauthenticated sync response from peer {peer}"
                                        );
                                        return;
                                    }
                                };
                                let verify_resp =
                                    crate::protocol::SyncResponse::ChangesetResponse {
                                        changes: changes.clone(),
                                        my_db_version,
                                        your_last_db_version,
                                        site_id: peer_site_id,
                                        topic: peer_topic.clone(),
                                        hmac: None,
                                    };
                                if let Ok(bytes) = serde_json::to_vec(&verify_resp)
                                    && !gk.verify(&bytes, &tag)
                                {
                                    log::debug!(
                                        "Rejecting sync response with invalid HMAC from peer {peer}"
                                    );
                                    return;
                                }
                            }

                            // Arcs for the spawned persistence task.
                            let db = g.db.clone();
                            let cache = g.db_version_cache.clone();
                            let group_local_db_version = g.local_db_version;

                            // Track the display-only "reported" version. The
                            // authoritative peer_db_versions entry is recorded
                            // after the changes commit (or immediately, in the
                            // no-changes branch below) so it never runs ahead of
                            // data we have actually applied.
                            if let Some(g) = self.groups.get_mut(&effective) {
                                let reported = g.peer_reported_versions.entry(peer).or_insert(0);
                                *reported = (*reported).max(my_db_version);
                            }
                            self.emit_network_event(
                                crate::network_status::NetworkEvent::PeerSynced {
                                    peer_id: crate::network_status::PeerId(peer.to_string()),
                                    db_version: my_db_version,
                                },
                            );
                            self.update_network_status();

                            // Update the group's local db_version (Lamport).
                            let lamport_bump = my_db_version > group_local_db_version;
                            if lamport_bump && let Some(g) = self.groups.get_mut(&effective) {
                                g.local_db_version = my_db_version;
                            }

                            if changes.is_empty() {
                                log::info!(
                                    "Version vector sync with peer {peer}: already up to date"
                                );
                                // Nothing to apply, so it is safe to record the
                                // peer's version (and any Lamport bump) right away.
                                if let Some(g) = self.groups.get_mut(&effective) {
                                    g.peer_db_versions.insert(peer, my_db_version);
                                }
                                let peer_str = peer.to_string();
                                tokio::spawn(async move {
                                    if lamport_bump
                                        && shadow::set_db_version(&db, my_db_version).await.is_ok()
                                    {
                                        cache.fetch_max(
                                            my_db_version,
                                            std::sync::atomic::Ordering::Release,
                                        );
                                    }
                                    let _ = peer_tracker::upsert_peer_version(
                                        &db,
                                        &peer_str,
                                        &peer_site_id,
                                        my_db_version,
                                    )
                                    .await;
                                });
                            } else {
                                log::info!(
                                    "Received {} changes from peer {peer} (their db_version: {})",
                                    changes.len(),
                                    my_db_version,
                                );

                                // Emit PeerSynced so subscribers (notably
                                // background_sync) see a success signal on the
                                // initiating side too.
                                self.emit_network_event(
                                    crate::network_status::NetworkEvent::PeerSynced {
                                        peer_id: crate::network_status::PeerId(peer.to_string()),
                                        db_version: my_db_version,
                                    },
                                );
                                self.update_network_status();

                                // Persist the Lamport bump now — it reflects our
                                // own clock, not unapplied peer data. The peer's
                                // db_version is recorded only after these changes
                                // commit; see the remote_changeset_rx handler.
                                if lamport_bump {
                                    let db = db.clone();
                                    let cache = cache.clone();
                                    tokio::spawn(async move {
                                        if shadow::set_db_version(&db, my_db_version).await.is_ok()
                                        {
                                            cache.fetch_max(
                                                my_db_version,
                                                std::sync::atomic::Ordering::Release,
                                            );
                                        }
                                    });
                                }

                                if let Err(e) = self.remote_changeset_tx.try_send(RemoteChangeset {
                                    peer,
                                    peer_site: peer_site_id,
                                    peer_db_version: Some(my_db_version),
                                    effective_topic: effective.clone(),
                                    changes,
                                }) {
                                    log::warn!(
                                        "Remote changeset queue full, dropping sync response: {e}"
                                    );
                                }
                            }
                        }
                        crate::protocol::SyncResponse::PushAck => {
                            log::debug!("Received PushAck from peer {peer}");
                        }
                        crate::protocol::SyncResponse::IdentityAck => {
                            log::debug!("Received IdentityAck from peer {peer}");
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure { peer, error, .. } => {
                for g in self.groups.values_mut() {
                    g.pending_sync_peers.remove(&peer);
                }
                log::warn!("Sync request to {peer} failed: {error}");
                // Connection might be dead — re-dial if we know the peer's address
                if let Some(addr) = self.peers.get(&peer).cloned()
                    && !self.swarm.is_connected(&peer)
                {
                    log::info!("Re-dialing {peer} after outbound failure");
                    let _ = self.swarm.dial(addr);
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                log::warn!("Sync inbound from {peer} failed: {error}");
            }
            _ => {}
        }
    }

    /// Verify HMAC + topic, reject mismatched peers, then spawn a task to query
    /// changes since the peer's last known version and send a `ChangesetResponse`.
    #[allow(clippy::too_many_arguments)]
    fn handle_version_vector_request(
        &mut self,
        peer: libp2p::PeerId,
        channel: request_response::ResponseChannel<crate::protocol::SyncResponse>,
        my_db_version: u64,
        your_last_db_version: u64,
        peer_site_id: NodeId,
        peer_topic: String,
        req_hmac: Option<[u8; 32]>,
    ) {
        // Route to the group this request targets (empty topic → default).
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            // Version-vector request for a topic none of our groups hold —
            // silently ignore. The peer reaches us over a connection shared by
            // all groups and may still share another group with us (e.g. a
            // household), so rejecting it would also kill that shared group's
            // sync. Reject only on per-group HMAC failure (Rule 2.8, multi-group).
            log::debug!(
                "Ignoring version-vector request from {peer} for unknown topic {effective}"
            );
            return;
        };
        let group_key = g.group_key.clone();

        // Verify HMAC with THIS group's key (topic selects the key; it never
        // bypasses verification — Rule 2.7).
        if let Some(ref gk) = group_key {
            let tag = match req_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated sync request from peer {peer}");
                    return;
                }
            };
            // Re-serialize with hmac: None for verification
            let verify_req = SyncRequest::VersionVector {
                my_db_version,
                your_last_db_version,
                site_id: peer_site_id,
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_req)
                && !gk.verify(&bytes, &tag)
            {
                log::debug!("Rejecting sync request with invalid HMAC from peer {peer}");
                return;
            }
            // HMAC verified — mark peer as a member of THIS group.
            let newly_verified = match self.groups.get_mut(&effective) {
                Some(g) if !g.verified_peers.contains(&peer) => {
                    g.verified_peers.insert(peer);
                    true
                }
                _ => false,
            };
            if newly_verified {
                self.emit_network_event(crate::network_status::NetworkEvent::PeerVerified(
                    crate::network_status::PeerId(peer.to_string()),
                ));
                self.update_network_status();
                // Announce identity to newly verified peer
                if let Some(ref id) = self.local_app_id {
                    let id = id.clone();
                    self.send_identity_announce(peer, &id);
                }
            }
        }

        // NOTE: Do NOT update peer_db_versions here. The peer's
        // reported db_version tells us what THEY have, but we haven't
        // received their data yet. peer_db_versions is only updated in
        // the response handler where we actually receive and process
        // changes. Updating here would cause us to skip changes in the
        // next sync request (your_last_db_version would be too high).
        //
        // However, track in peer_reported_versions for display purposes.
        if let Some(g) = self.groups.get_mut(&effective) {
            let reported = g.peer_reported_versions.entry(peer).or_insert(0);
            *reported = (*reported).max(my_db_version);
        }

        // NOTE: We deliberately do NOT reverse-trigger a sync here (initiate our
        // own VersionVector back at the requester). Doing so fires once per
        // *inbound* request, so with many group members it amplifies into an
        // unbounded VersionVector storm that exhausts the request-response
        // substreams ("unexpected end of file"). Symmetry is instead provided by
        // the *bounded* connect/discovery-time initiation (once per peer per
        // connection — see `initiate_sync_for_peer` callers in peer_manager /
        // relay_manager), which both sides run, plus the periodic tick and
        // real-time push fan-out. Joined groups participate in those paths now
        // that they reach `registry_is_ready` (see `handle_group_registry_ready`).
        self.emit_network_event(crate::network_status::NetworkEvent::PeerSynced {
            peer_id: crate::network_status::PeerId(peer.to_string()),
            db_version: my_db_version,
        });
        self.update_network_status();

        // Snapshot this group's state for the spawned task.
        let Some(g) = self.groups.get(&effective) else {
            return;
        };
        let db = g.db.clone();
        let registry = g.registry.clone();
        let resp_tx = self.snapshot_resp_tx.clone();
        let local_db_version = g.local_db_version;
        let local_site_id = g.site_id;
        let change_tx = g.change_tx.clone();
        let topic_name = g.topic_name.clone();

        tokio::spawn(async move {
            // Get changes since the peer's last known version of us
            let changes =
                match shadow::get_changes_since(&db, &registry, your_last_db_version).await {
                    Ok(c) => c,
                    Err(e) => {
                        log::error!(
                            "Failed to get changes since {}: {}",
                            your_last_db_version,
                            e
                        );
                        Vec::new()
                    }
                };

            let mut resp = crate::protocol::SyncResponse::ChangesetResponse {
                changes,
                my_db_version: local_db_version,
                your_last_db_version: my_db_version,
                site_id: local_site_id,
                topic: topic_name,
                hmac: None,
            };

            // Sign response if group key is configured
            if let Some(ref gk) = group_key
                && let Ok(bytes) = serde_json::to_vec(&resp)
            {
                let tag = gk.mac(&bytes);
                if let crate::protocol::SyncResponse::ChangesetResponse { ref mut hmac, .. } = resp
                {
                    *hmac = Some(tag);
                }
            }

            if let Err(e) = resp_tx.send((channel, resp)).await {
                log::error!("Failed to queue sync response: {}", e);
            }

            // Also persist peer version
            let _ = peer_tracker::upsert_peer_version(
                &db,
                &peer.to_string(),
                &peer_site_id,
                my_db_version,
            )
            .await;

            let _ = change_tx; // keep alive
        });
    }

    /// Verify HMAC + topic, queue changeset for sequential application, send PushAck.
    fn handle_push_request(
        &mut self,
        peer: libp2p::PeerId,
        channel: request_response::ResponseChannel<crate::protocol::SyncResponse>,
        changeset: SyncChangeset,
        peer_topic: String,
        req_hmac: Option<[u8; 32]>,
    ) {
        // Route to the group this push targets (empty topic → default).
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            // Push for a topic none of our groups hold — silently ignore (a
            // group we don't have doesn't mean we share no group with this
            // peer over the shared connection). Rule 2.8, multi-group.
            log::debug!("Ignoring push from {peer} for unknown topic {effective}");
            return;
        };

        // Verify HMAC with THIS group's key.
        if let Some(ref gk) = g.group_key {
            let tag = match req_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated push from peer {peer}");
                    return;
                }
            };
            let verify_req = SyncRequest::Push {
                changeset: changeset.clone(),
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_req)
                && !gk.verify(&bytes, &tag)
            {
                log::debug!("Rejecting push with invalid HMAC from peer {peer}");
                return;
            }
            // HMAC verified — mark peer as a member of THIS group.
            let newly_verified = match self.groups.get_mut(&effective) {
                Some(g) if !g.verified_peers.contains(&peer) => {
                    g.verified_peers.insert(peer);
                    true
                }
                _ => false,
            };
            if newly_verified {
                self.emit_network_event(crate::network_status::NetworkEvent::PeerVerified(
                    crate::network_status::PeerId(peer.to_string()),
                ));
                self.update_network_status();
                // Announce identity to newly verified peer
                if let Some(ref id) = self.local_app_id {
                    let id = id.clone();
                    self.send_identity_announce(peer, &id);
                }
            }
        }

        // Track peer's db_version in the group (max to avoid stale overwrite).
        if let Some(g) = self.groups.get_mut(&effective) {
            let entry = g.peer_db_versions.entry(peer).or_insert(0);
            *entry = (*entry).max(changeset.db_version);
            let reported = g.peer_reported_versions.entry(peer).or_insert(0);
            *reported = (*reported).max(changeset.db_version);
        }

        log::info!(
            "Received push from peer {peer} with {} changes at db_version {}",
            changeset.changes.len(),
            changeset.db_version,
        );

        // Send PushAck immediately via response channel
        let resp_tx = self.snapshot_resp_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = resp_tx
                .send((channel, crate::protocol::SyncResponse::PushAck))
                .await
            {
                log::error!("Failed to send PushAck: {e}");
            }
        });

        // Queue changeset for sequential application in the main loop. Real-time
        // push only updates peer_db_versions in-memory via max() (above), so no
        // version is persisted from this path — peer_db_version is None.
        if let Err(e) = self.remote_changeset_tx.try_send(RemoteChangeset {
            peer,
            peer_site: changeset.site_id,
            peer_db_version: None,
            effective_topic: effective.clone(),
            changes: changeset.changes,
        }) {
            log::warn!("Remote changeset queue full, dropping push: {e}");
        }
    }

    /// Verify HMAC, check peer is verified, store identity, emit event, respond with IdentityAck.
    fn handle_identity_announce_request(
        &mut self,
        peer: libp2p::PeerId,
        channel: request_response::ResponseChannel<crate::protocol::SyncResponse>,
        app_id: String,
        req_hmac: Option<[u8; 32]>,
    ) {
        // Identity is node-level. Verify against the default group's key and
        // accept from a peer verified in ANY group.
        if let Some(ref gk) = self.default_group().group_key {
            let tag = match req_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated identity announce from peer {peer}");
                    return;
                }
            };
            let verify_req = SyncRequest::IdentityAnnounce {
                app_id: app_id.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_req)
                && !gk.verify(&bytes, &tag)
            {
                log::debug!("Rejecting identity announce with invalid HMAC from peer {peer}");
                return;
            }
        }

        // Only accept from peers verified in at least one group.
        let verified_somewhere = self
            .groups
            .values()
            .any(|g| g.verified_peers.contains(&peer));
        if !verified_somewhere {
            log::debug!("Ignoring identity announce from unverified peer {peer}");
            return;
        }

        self.peer_identities.insert(peer, app_id.clone());
        self.emit_network_event(crate::network_status::NetworkEvent::PeerIdentityReceived {
            peer_id: crate::network_status::PeerId(peer.to_string()),
            app_id,
        });
        self.update_network_status();

        // Send IdentityAck
        let resp_tx = self.snapshot_resp_tx.clone();
        tokio::spawn(async move {
            let _ = resp_tx
                .send((channel, crate::protocol::SyncResponse::IdentityAck))
                .await;
        });
    }

    /// Reject a peer for a single group (failed HMAC for that group). Removes it
    /// from that group's tracking only — the peer may still belong to other
    /// groups on this node.
    #[allow(dead_code)]
    fn reject_peer_for_group(&mut self, effective_topic: &str, peer: libp2p::PeerId) {
        if let Some(g) = self.groups.get_mut(effective_topic) {
            g.rejected_peers.insert(peer);
            g.verified_peers.remove(&peer);
            g.pending_sync_peers.remove(&peer);
            g.peer_db_versions.remove(&peer);
            g.peer_reported_versions.remove(&peer);
        }
        self.update_network_status();
    }

}

const CHANGESET_CHUNK_SIZE: usize = 50;

/// Apply a set of remote column changes to the local database.
///
/// Small changesets (up to `CHANGESET_CHUNK_SIZE` rows) run in a single
/// transaction for minimal fsync overhead. Larger changesets are split
/// into chunks so the SQLite write lock is released between batches,
/// allowing local writes (e.g. from Dioxus hooks) to proceed.
///
/// `ChangeNotification`s are buffered during each transaction and emitted
/// only AFTER commit (Rule 2.12 — subscribers must never observe a
/// notification before its data is durable).
/// Handles needed to run per-table notification policies as remote changes are
/// applied. Bundled so the apply functions take one optional parameter; `None`
/// in unit tests and whenever no `#[derive(SyncNotify)]` policy is registered.
#[derive(Clone, Copy)]
pub(super) struct NotifyCtx<'a> {
    pub registry: &'a crate::registry::NotificationRegistry,
    pub tx: &'a broadcast::Sender<crate::notify::Notification>,
}

pub(super) async fn apply_remote_changeset(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    changes: &[ColumnChange],
    db_version_cache: Option<&std::sync::atomic::AtomicU64>,
    source: crate::messages::ChangeSource,
    notify: Option<NotifyCtx<'_>>,
) {
    // Group changes by (table, pk) so a single row is never split across chunks.
    let mut grouped: Vec<((&str, &str), Vec<&ColumnChange>)> = {
        let mut map: HashMap<(&str, &str), Vec<&ColumnChange>> = HashMap::new();
        for change in changes {
            map.entry((&change.table.0, &change.pk.0))
                .or_default()
                .push(change);
        }
        map.into_iter().collect()
    };

    // Small changesets: single transaction (no chunking overhead).
    if grouped.len() <= CHANGESET_CHUNK_SIZE {
        apply_changeset_chunk(
            db,
            change_tx,
            registry,
            &grouped,
            db_version_cache,
            source,
            notify,
        )
        .await;
        return;
    }

    // Large changesets: split into chunks.
    while !grouped.is_empty() {
        let chunk_end = grouped.len().min(CHANGESET_CHUNK_SIZE);
        let chunk: Vec<_> = grouped.drain(..chunk_end).collect();
        apply_changeset_chunk(
            db,
            change_tx,
            registry,
            &chunk,
            db_version_cache,
            source,
            notify,
        )
        .await;
    }
}

async fn apply_changeset_chunk<'a>(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    grouped: &[((&'a str, &'a str), Vec<&'a ColumnChange>)],
    db_version_cache: Option<&std::sync::atomic::AtomicU64>,
    source: crate::messages::ChangeSource,
    notify: Option<NotifyCtx<'_>>,
) {
    use sea_orm::TransactionTrait;

    let txn = match db.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to begin transaction for remote changeset: {e}");
            return;
        }
    };

    let local_db_version = match shadow::increment_db_version(&txn).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to increment db_version: {e}");
            let _ = txn.rollback().await;
            return;
        }
    };

    let mut pending_notifications: Vec<ChangeNotification> = Vec::new();

    for ((table, pk), row_changes) in grouped {
        let meta = match registry.get(table) {
            Some(m) => m,
            None => {
                log::warn!("Rejecting remote changes for unregistered table: {}", table);
                continue;
            }
        };

        let mut any_applied = false;
        let mut changed_pairs: Vec<(String, serde_json::Value)> = Vec::new();
        let mut is_delete = false;
        // Whether the row already existed before this changeset was applied —
        // drives the Insert vs Update classification of the notification below.
        let mut existed = false;

        let delete_change = row_changes.iter().find(|c| c.cid.0 == "__deleted");
        if let Some(change) = delete_change {
            if apply_remote_delete(&txn, table, pk, change, &meta, local_db_version).await {
                any_applied = true;
                is_delete = true;
            }
        } else {
            let (applied, row_existed, pairs) =
                apply_remote_column_changes(&txn, table, pk, row_changes, &meta, local_db_version)
                    .await;
            if applied {
                any_applied = true;
                existed = row_existed;
                changed_pairs = pairs;
            }
        }

        if any_applied {
            // A remote edit to a pre-existing row is an Update; first-time
            // creation is an Insert. This lets an `on_sync` policy that only
            // notifies on `Insert` stay quiet when a peer merely edits a row.
            let kind = if is_delete {
                WriteKind::Delete
            } else if existed {
                WriteKind::Update
            } else {
                WriteKind::Insert
            };
            let (changed_columns, column_values) = if is_delete || changed_pairs.is_empty() {
                (None, None)
            } else {
                let cols: Vec<String> = changed_pairs.iter().map(|(c, _)| c.clone()).collect();
                let vals: Vec<(crate::ColumnName, serde_json::Value)> = changed_pairs
                    .iter()
                    .map(|(c, v)| (crate::ColumnName(c.clone()), v.clone()))
                    .collect();
                (Some(cols), Some(vals))
            };
            pending_notifications.push(ChangeNotification {
                table: (*table).into(),
                kind,
                source,
                primary_key: (*pk).into(),
                changed_columns,
                column_values,
            });
        }
    }

    if let Err(e) = txn.commit().await {
        log::error!("Failed to commit remote changeset transaction: {e}");
        return;
    }

    if let Some(cache) = db_version_cache {
        cache.fetch_max(local_db_version, std::sync::atomic::Ordering::Release);
    }

    for n in pending_notifications {
        // Run the per-table notification policy (remote-only) before broadcasting
        // the raw change. The gate inside `dispatch` de-spams bursts.
        if let Some(ctx) = notify
            && let Some(user_notif) = ctx.registry.dispatch(&n)
        {
            // Visibility: confirms the SyncNotify policy fired and passed the
            // anti-spam gate. If you see this but no OS notification, the gap is
            // downstream (no `use_sync_notifications` subscriber in this process
            // — e.g. a background wake — or the native display call failed).
            log::info!(
                "notification: generated for table {} ({} receiver(s) subscribed)",
                user_notif.table,
                ctx.tx.receiver_count(),
            );
            let _ = ctx.tx.send(user_notif);
        }
        let _ = change_tx.send(n);
    }
}

/// Apply a remote delete: check conflict resolution, delete row, update shadow.
/// Returns `true` if the delete was applied.
async fn apply_remote_delete(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    change: &ColumnChange,
    meta: &crate::registry::TableMeta,
    local_db_version: u64,
) -> bool {
    let local_entries = shadow::get_clock_entries_for_row(db, table, pk)
        .await
        .unwrap_or_default();
    let local_max_cv = local_entries
        .iter()
        .map(|e| e.col_version)
        .max()
        .unwrap_or(0);

    if !conflict::should_apply_delete(change.cl, local_max_cv, &meta.delete_policy) {
        return false;
    }

    let delete_sql = format!(
        "DELETE FROM \"{}\" WHERE \"{}\" = $1",
        table, meta.primary_key_column
    );
    if let Err(e) = db
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            &delete_sql,
            [pk.to_string().into()],
        ))
        .await
    {
        log::error!("Failed to delete row {}/{}: {}", table, pk, e);
        return false;
    }

    let _ = shadow::delete_clock_entries(db, table, pk).await;
    let _ = shadow::insert_tombstone(
        db,
        table,
        pk,
        change.col_version,
        local_db_version,
        &change.site_id,
    )
    .await;

    true
}

/// Apply non-delete column changes: resolve conflicts per-column, write winning values,
/// update shadow tables. Returns `(applied, existed, changed_column_pairs)` where
/// `existed` is whether the row already existed *before* this changeset was applied
/// (so the caller can classify the write as `Update` vs `Insert`), and each pair in
/// `changed_column_pairs` is `(column_name, post_write_json_value)` for the columns
/// that actually got applied. Reactive hooks consume the JSON values to update signal
/// state in place without re-querying SeaORM.
async fn apply_remote_column_changes(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    row_changes: &[&ColumnChange],
    meta: &crate::registry::TableMeta,
    local_db_version: u64,
) -> (bool, bool, Vec<(String, serde_json::Value)>) {
    let exists = row_exists(db, table, &meta.primary_key_column, pk).await;
    let mut winning_columns: Vec<(String, sea_orm::Value)> = Vec::new();
    let mut pending_shadow_updates: Vec<(String, u64, crate::messages::NodeId, u32)> = Vec::new();
    let mut changed_columns: Vec<(String, serde_json::Value)> = Vec::new();

    for change in row_changes {
        // SECURITY (WSDB-PoC-1): the column id arrives unauthenticated
        // over the network and is interpolated into raw SQL further down
        // (`format!("\"{}\"", col)` in the UPDATE / INSERT paths). Reject
        // anything that isn't (a) a registered column name for this
        // table, and (b) different from the primary-key column — peers
        // are not allowed to rewrite other peers' PKs via the
        // column-update path. Both checks must run BEFORE we touch the
        // shadow clock for this `cid`, otherwise a malicious peer can
        // corrupt convergence state without producing any user-table
        // write (the original WSDB-PoC-1).
        if !meta.columns.iter().any(|c| c == &change.cid.0) {
            log::warn!(
                "Rejecting remote change for unregistered column: {}/{}/{}",
                table,
                pk,
                change.cid.0
            );
            continue;
        }
        if change.cid.0 == meta.primary_key_column {
            log::warn!(
                "Rejecting remote change targeting the primary-key column: {}/{}",
                table,
                pk
            );
            continue;
        }

        let (local_cv, local_site) =
            shadow::get_col_version_with_site(db, table, pk, &change.cid.0)
                .await
                .unwrap_or((0, NodeId([0u8; 16])));

        let remote_val_bytes = serde_json::to_vec(&change.val).unwrap_or_default();
        let remote_site = change.site_id;

        let should_apply = if local_cv == 0 {
            true
        } else if change.col_version != local_cv {
            change.col_version > local_cv
        } else {
            let local_val_bytes =
                get_local_value_bytes(db, table, &meta.primary_key_column, pk, &change.cid.0).await;
            conflict::should_apply_column(
                change.col_version,
                &remote_val_bytes,
                &remote_site,
                local_cv,
                &local_val_bytes,
                &local_site,
            )
        };

        if should_apply {
            winning_columns.push((change.cid.0.clone(), json_to_sea_value(change.val.as_ref())));
            changed_columns.push((
                change.cid.0.clone(),
                change.val.clone().unwrap_or(serde_json::Value::Null),
            ));
            pending_shadow_updates.push((
                change.cid.0.clone(),
                change.col_version,
                remote_site,
                change.seq,
            ));
        }
    }

    if winning_columns.is_empty() {
        return (false, exists, changed_columns);
    }

    if exists {
        // UPDATE each winning column
        for (col, val) in &winning_columns {
            let update_sql = format!(
                "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                table, col, meta.primary_key_column
            );
            if let Err(e) = db
                .execute_raw(sea_orm::Statement::from_sql_and_values(
                    sea_orm::DatabaseBackend::Sqlite,
                    &update_sql,
                    [val.clone(), pk.to_string().into()],
                ))
                .await
            {
                log::error!("Failed to update column {}/{}/{}: {}", table, pk, col, e);
            }
        }
        flush_shadow_updates(db, table, pk, &pending_shadow_updates, local_db_version).await;
        return (true, exists, changed_columns);
    }

    // INSERT OR IGNORE — silently skips if row was created by a concurrent task
    let mut col_names = vec![format!("\"{}\"", meta.primary_key_column)];
    let mut values: Vec<sea_orm::Value> = vec![pk.to_string().into()];

    for (col, val) in &winning_columns {
        if *col != meta.primary_key_column {
            col_names.push(format!("\"{}\"", col));
            values.push(val.clone());
        }
    }

    let placeholders: Vec<String> = (1..=values.len()).map(|i| format!("${}", i)).collect();
    let insert_sql = format!(
        "INSERT OR IGNORE INTO \"{}\" ({}) VALUES ({})",
        table,
        col_names.join(", "),
        placeholders.join(", ")
    );

    let _ = db
        .execute_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            &insert_sql,
            values,
        ))
        .await;

    // UPDATE each winning column individually — works whether INSERT
    // succeeded or was ignored due to concurrent insert
    for (col, val) in &winning_columns {
        if *col != meta.primary_key_column {
            let update_sql = format!(
                "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                table, col, meta.primary_key_column
            );
            if let Err(e) = db
                .execute_raw(sea_orm::Statement::from_sql_and_values(
                    sea_orm::DatabaseBackend::Sqlite,
                    &update_sql,
                    [val.clone(), pk.to_string().into()],
                ))
                .await
            {
                log::error!("Failed to update column {}/{}/{}: {}", table, pk, col, e);
            }
        }
    }

    // Verify INSERT actually created the row before writing shadow
    if row_exists(db, table, &meta.primary_key_column, pk).await {
        flush_shadow_updates(db, table, pk, &pending_shadow_updates, local_db_version).await;
        (true, exists, changed_columns)
    } else {
        log::debug!(
            "Row {}/{} not created (likely missing NOT NULL columns from \
             out-of-order delivery), deferring shadow updates",
            table,
            pk
        );
        (false, exists, changed_columns)
    }
}

/// Write pending shadow table clock entries after a successful DB write.
async fn flush_shadow_updates(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    updates: &[(String, u64, crate::messages::NodeId, u32)],
    local_db_version: u64,
) {
    for (cid, cv, site, seq) in updates {
        let _ =
            shadow::upsert_clock_entry(db, table, pk, cid, *cv, local_db_version, site, *seq).await;
    }
}

/// Check if a row exists in a table.
pub(super) async fn row_exists(
    db: &impl ConnectionTrait,
    table: &str,
    pk_col: &str,
    pk: &str,
) -> bool {
    let sql = format!(
        "SELECT 1 FROM \"{}\" WHERE \"{}\" = $1 LIMIT 1",
        table, pk_col
    );
    db.query_one_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        &sql,
        [pk.into()],
    ))
    .await
    .ok()
    .flatten()
    .is_some()
}

/// Convert a JSON value to a SeaORM value for parameterized queries.
pub(super) fn json_to_sea_value(v: Option<&serde_json::Value>) -> sea_orm::Value {
    match v {
        None | Some(serde_json::Value::Null) => sea_orm::Value::String(None),
        Some(serde_json::Value::Bool(b)) => sea_orm::Value::Int(Some(if *b { 1 } else { 0 })),
        Some(serde_json::Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                sea_orm::Value::BigInt(Some(i))
            } else if let Some(f) = n.as_f64() {
                sea_orm::Value::Double(Some(f))
            } else {
                sea_orm::Value::String(Some(n.to_string()))
            }
        }
        Some(serde_json::Value::String(s)) => sea_orm::Value::String(Some(s.clone())),
        Some(other) => sea_orm::Value::String(Some(other.to_string())),
    }
}

/// Fetch the current value of a column as JSON-serialized bytes for conflict tiebreaking.
pub(super) async fn get_local_value_bytes(
    db: &impl ConnectionTrait,
    table: &str,
    pk_col: &str,
    pk: &str,
    cid: &str,
) -> Vec<u8> {
    let sql = format!(
        "SELECT json_object('v', \"{}\") as json_val FROM \"{}\" WHERE \"{}\" = $1",
        cid, table, pk_col
    );
    let result = db
        .query_one_raw(sea_orm::Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Sqlite,
            &sql,
            [pk.into()],
        ))
        .await
        .ok()
        .flatten();

    match result {
        Some(qr) => {
            let raw: Option<String> = qr.try_get("", "json_val").ok();
            let val = raw.and_then(|s| {
                let obj: serde_json::Value = serde_json::from_str(&s).ok()?;
                Some(obj.get("v")?.clone())
            });
            serde_json::to_vec(&val).unwrap_or_default()
        }
        None => serde_json::to_vec(&Option::<serde_json::Value>::None).unwrap_or_default(),
    }
}

/// Strip the RETURNING clause from a SQL statement.
#[cfg(test)]
pub(super) fn strip_returning(sql: &str) -> String {
    if let Some(pos) = sql.to_ascii_uppercase().rfind(" RETURNING ") {
        sql[..pos].to_string()
    } else {
        sql.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{ColumnChange, NodeId};
    use crate::registry::TableMeta;
    use sea_orm::Database;
    use std::sync::Arc;

    async fn setup_engine_test_db() -> (sea_orm::DatabaseConnection, Arc<TableRegistry>) {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::shadow::create_meta_table(&db).await.unwrap();
        crate::peer_tracker::create_peer_versions_table(&db)
            .await
            .unwrap();
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)"
        ).await.unwrap();
        crate::shadow::create_shadow_table(&db, "tasks")
            .await
            .unwrap();
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
        (db, registry)
    }

    // ── strip_returning tests ──

    #[test]
    fn test_strip_returning_with_clause() {
        let result = strip_returning(r#"INSERT INTO "tasks" ("id") VALUES ('1') RETURNING "id""#);
        assert!(
            !result.contains("RETURNING"),
            "Expected RETURNING clause to be stripped, got: {result}"
        );
    }

    #[test]
    fn test_strip_returning_without_clause() {
        let input = r#"INSERT INTO "tasks" ("id") VALUES ('1')"#;
        let result = strip_returning(input);
        assert_eq!(result, input);
    }

    #[test]
    fn test_strip_returning_case_insensitive() {
        let result = strip_returning(r#"INSERT INTO "tasks" ("id") VALUES ('1') Returning "id""#);
        assert!(
            !result.contains("Returning"),
            "Expected case-insensitive RETURNING clause to be stripped, got: {result}"
        );
    }

    // ── apply_remote_changeset tests ──

    /// REGRESSION — WSDB-PoC-1 (was: SQL injection via unsanitised `cid`).
    ///
    /// Asserts that `apply_remote_column_changes` rejects a `ColumnChange`
    /// whose `cid` is not in the registered column set:
    ///   - the user-data row is not mutated (no malformed SQL ran);
    ///   - **the shadow clock table has no entry for the bogus cid**
    ///     (this is what made the original bug a silent state-corruption
    ///     vulnerability, not just a noisy SQL error).
    ///
    /// Originally this test asserted the *opposite* and proved the bug;
    /// keeping the same setup and flipping the assertions guards the
    /// fix from regressing.
    #[tokio::test]
    async fn regression_wsdb_1_rejects_unknown_column_id() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks (id, title, done) VALUES ('r1', 'before', 0)")
            .await
            .unwrap();

        let evil_cid = "title\"--";

        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "r1".into(),
            cid: evil_cid.into(),
            val: Some(serde_json::json!("after")),
            site_id: NodeId([7u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Row untouched.
        use sea_orm::ConnectionTrait;
        let row = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'r1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = row.try_get("", "title").unwrap();
        assert_eq!(title, "before");

        // Shadow clock has NO entry for the bogus cid (the fix.).
        let shadow_rows = db
            .query_all_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT cid FROM _wavesync_tasks_clock WHERE pk = 'r1'".to_string(),
            ))
            .await
            .unwrap();
        let cids: Vec<String> = shadow_rows
            .iter()
            .filter_map(|r| r.try_get::<String>("", "cid").ok())
            .collect();
        assert!(
            !cids.iter().any(|c| c == evil_cid),
            "shadow clock must not contain an entry for the rejected bogus cid; \
             present cids: {cids:?}"
        );
    }

    /// REGRESSION — WSDB-PoC-1b (was: PK rewrite via `cid = "id"`).
    ///
    /// `id` is in `meta.columns` (it's a registered column) so the
    /// whitelist alone wouldn't catch this — the additional PK guard
    /// rejects the change. After the fix the row's PK is unchanged.
    #[tokio::test]
    async fn regression_wsdb_1b_rejects_pk_column_in_update_path() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks (id, title, done) VALUES ('r1', 'original', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db, "tasks", "r1", "id", 0, 0, &NodeId([0u8; 16]), 0)
            .await
            .unwrap();

        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "r1".into(),
            cid: "id".into(),
            val: Some(serde_json::json!("attacker_chosen_pk")),
            site_id: NodeId([7u8; 16]),
            col_version: 99,
            cl: 1,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        use sea_orm::ConnectionTrait;
        // Original row still exists with its real PK.
        let original = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT id FROM tasks WHERE id = 'r1'".to_string(),
            ))
            .await
            .unwrap();
        assert!(
            original.is_some(),
            "original row must still exist with its original PK"
        );
        // No row created with the attacker-chosen PK.
        let pwned = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT id FROM tasks WHERE id = 'attacker_chosen_pk'".to_string(),
            ))
            .await
            .unwrap();
        assert!(
            pwned.is_none(),
            "remote ColumnChange targeting the PK column must be rejected"
        );
    }

    /// Sanity test — the fix must not regress the legitimate sync path.
    /// A `ColumnChange` for a registered non-PK column (`title`) still
    /// applies normally.
    #[tokio::test]
    async fn regression_wsdb_1_legitimate_column_change_still_applies() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks (id, title, done) VALUES ('r1', 'before', 0)")
            .await
            .unwrap();

        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "r1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("after")),
            site_id: NodeId([7u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        use sea_orm::ConnectionTrait;
        let row = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'r1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = row.try_get("", "title").unwrap();
        assert_eq!(title, "after", "legitimate column change must still apply");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_unregistered_table() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![ColumnChange {
            table: "unknown".into(),
            pk: "1".into(),
            cid: "col".into(),
            val: Some(serde_json::json!("value")),
            site_id: NodeId([2u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;
        assert!(
            rx.try_recv().is_err(),
            "Should not receive notification for unregistered table"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_insert() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "test-1".into(),
                cid: "id".into(),
                val: Some(serde_json::json!("test-1")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "test-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Test Task")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "test-1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(0)),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "test-1");

        // Verify row exists
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'test-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Test Task");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert a row first
        db.execute_unprepared("INSERT INTO tasks VALUES ('del-1', 'To Delete', 0)")
            .await
            .unwrap();

        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "del-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([2u8; 16]),
            col_version: 10,
            cl: 10,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let notif = rx.try_recv().expect("Expected a ChangeNotification");
        assert_eq!(notif.table, "tasks");
        assert_eq!(notif.primary_key, "del-1");

        // Verify row is deleted
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT COUNT(*) as cnt FROM tasks WHERE id = 'del-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let count: i32 = result.try_get("", "cnt").unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_column_conflict_higher_version_wins() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data
        db.execute_unprepared("INSERT INTO tasks VALUES ('c-1', 'Original', 0)")
            .await
            .unwrap();
        // Set local clock entry with version 5
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "c-1",
            "title",
            5,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote change with higher version (10) should win
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "c-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("Remote Winner")),
            site_id: NodeId([2u8; 16]),
            col_version: 10,
            cl: 10,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'c-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Remote Winner");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_lower_version_loses() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data and set local clock high
        db.execute_unprepared("INSERT INTO tasks VALUES ('lv-1', 'Local', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "lv-1",
            "title",
            10,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote change with LOWER version (3) should lose
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "lv-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("Remote Loser")),
            site_id: NodeId([2u8; 16]),
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'lv-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(
            title, "Local",
            "Lower version remote should not overwrite local"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_different_columns_both_survive() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert initial data
        db.execute_unprepared("INSERT INTO tasks VALUES ('dc-1', 'Original', 0)")
            .await
            .unwrap();
        // Set local clock for title only
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "dc-1",
            "title",
            1,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote changes: higher version for title, and a new column "done"
        let changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "dc-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Remote Title")),
                site_id: NodeId([2u8; 16]),
                col_version: 5,
                cl: 5,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "dc-1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(1)),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
        ];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title, done FROM tasks WHERE id = 'dc-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        let done: i32 = result.try_get_by_index(1).unwrap();
        assert_eq!(title, "Remote Title");
        assert_eq!(done, 1, "Both columns should be updated");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete_lower_cl_rejected() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks VALUES ('dlcl-1', 'Keep Me', 0)")
            .await
            .unwrap();
        // Set a high local clock
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "dlcl-1",
            "title",
            10,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote delete with low causal length — should be rejected
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "dlcl-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([2u8; 16]),
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Row should still exist
        let exists = row_exists(&db, "tasks", "id", "dlcl-1").await;
        assert!(
            exists,
            "Row should NOT be deleted when remote cl < local max cv"
        );
        assert!(
            rx.try_recv().is_err(),
            "No notification for rejected delete"
        );
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_delete_wins_on_tie() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        db.execute_unprepared("INSERT INTO tasks VALUES ('dw-1', 'Tie Delete', 0)")
            .await
            .unwrap();
        // Set local clock to 5
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "dw-1",
            "title",
            5,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote delete with cl=5 (tie) — DeleteWins policy (default)
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "dw-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([2u8; 16]),
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let exists = row_exists(&db, "tasks", "id", "dw-1").await;
        assert!(!exists, "DeleteWins: tie should delete the row");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_add_wins_on_tie() {
        let (db, _) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Register with AddWins policy
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::AddWins,
        });

        db.execute_unprepared("INSERT INTO tasks VALUES ('aw-1', 'Tie Keep', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "aw-1",
            "title",
            5,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Remote delete with cl=5 (tie) — AddWins policy
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "aw-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([2u8; 16]),
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        let exists = row_exists(&db, "tasks", "id", "aw-1").await;
        assert!(exists, "AddWins: tie should keep the row");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_insert_after_delete() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        // Insert then delete locally
        db.execute_unprepared("INSERT INTO tasks VALUES ('iad-1', 'Deleted', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(
            &db,
            "tasks",
            "iad-1",
            "title",
            1,
            1,
            &NodeId([1u8; 16]),
            0,
        )
        .await
        .unwrap();

        // Apply remote delete
        let delete_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "iad-1".into(),
            cid: "__deleted".into(),
            val: None,
            site_id: NodeId([2u8; 16]),
            col_version: 5,
            cl: 5,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &delete_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;
        assert!(!row_exists(&db, "tasks", "id", "iad-1").await);

        // Now apply remote insert with higher versions (N3 regression)
        let insert_changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "iad-1".into(),
                cid: "id".into(),
                val: Some(serde_json::json!("iad-1")),
                site_id: NodeId([3u8; 16]),
                col_version: 10,
                cl: 10,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "iad-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Reinserted")),
                site_id: NodeId([3u8; 16]),
                col_version: 10,
                cl: 10,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "iad-1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(0)),
                site_id: NodeId([3u8; 16]),
                col_version: 10,
                cl: 10,
                seq: 2,
                db_version: 0,
            },
        ];
        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &insert_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        assert!(
            row_exists(&db, "tasks", "id", "iad-1").await,
            "Row should reappear after re-insert"
        );
        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'iad-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(title, "Reinserted");
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_multiple_rows() {
        let (db, registry) = setup_engine_test_db().await;
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-1".into(),
                cid: "id".into(),
                val: Some(serde_json::json!("mr-1")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Row 1")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(0)),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-2".into(),
                cid: "id".into(),
                val: Some(serde_json::json!("mr-2")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-2".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Row 2")),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "mr-2".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(1)),
                site_id: NodeId([2u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];

        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        assert!(row_exists(&db, "tasks", "id", "mr-1").await);
        assert!(row_exists(&db, "tasks", "id", "mr-2").await);
    }

    #[tokio::test]
    async fn test_convergence_tied_col_version() {
        // Two peers insert the same PK offline, both at col_version=1.
        // Deterministic tiebreaker must produce the same winner on both sides.
        let site_a: crate::messages::NodeId = NodeId([1u8; 16]);
        let site_b: crate::messages::NodeId = NodeId([2u8; 16]); // B > A

        // Simulate Peer A's DB: has A's data locally, receives B's data
        let (db_a, registry_a) = setup_engine_test_db().await;
        let (tx_a, _rx_a) = broadcast::channel::<ChangeNotification>(16);

        // A wrote locally: title="A-value"
        db_a.execute_unprepared("INSERT INTO tasks VALUES ('tied-1', 'A-value', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "id", 1, 1, &site_a, 0)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "title", 1, 1, &site_a, 1)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "tied-1", "done", 1, 1, &site_a, 2)
            .await
            .unwrap();

        // B's changes arrive at A (same col_version=1, different value)
        let b_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "tied-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("B-value")),
            site_id: site_b,
            col_version: 1,
            cl: 1,
            seq: 1,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db_a,
            &tx_a,
            &registry_a,
            &b_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Simulate Peer B's DB: has B's data locally, receives A's data
        let (db_b, registry_b) = setup_engine_test_db().await;
        let (tx_b, _rx_b) = broadcast::channel::<ChangeNotification>(16);

        // B wrote locally: title="B-value"
        db_b.execute_unprepared("INSERT INTO tasks VALUES ('tied-1', 'B-value', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "id", 1, 1, &site_b, 0)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "title", 1, 1, &site_b, 1)
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "tied-1", "done", 1, 1, &site_b, 2)
            .await
            .unwrap();

        // A's changes arrive at B
        let a_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "tied-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("A-value")),
            site_id: site_a,
            col_version: 1,
            cl: 1,
            seq: 1,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db_b,
            &tx_b,
            &registry_b,
            &a_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Both peers should converge to the same value
        let result_a = db_a
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'tied-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_a: String = result_a.try_get_by_index(0).unwrap();

        let result_b = db_b
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'tied-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_b: String = result_b.try_get_by_index(0).unwrap();

        assert_eq!(
            title_a, title_b,
            "Both peers must converge to the same value (got A='{}', B='{}')",
            title_a, title_b
        );
    }

    #[tokio::test]
    async fn test_convergence_offline_diverged_updates() {
        // Two peers update the same column offline (different col_versions).
        // Higher col_version must win deterministically.
        let site_a: crate::messages::NodeId = NodeId([1u8; 16]);
        let site_b: crate::messages::NodeId = NodeId([2u8; 16]);

        // Peer A: updated title twice (col_version=3)
        let (db_a, registry_a) = setup_engine_test_db().await;
        let (tx_a, _rx_a) = broadcast::channel::<ChangeNotification>(16);

        db_a.execute_unprepared("INSERT INTO tasks VALUES ('div-1', 'A-latest', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_a, "tasks", "div-1", "title", 3, 3, &site_a, 0)
            .await
            .unwrap();

        // Peer B: updated title once (col_version=2)
        let (db_b, registry_b) = setup_engine_test_db().await;
        let (tx_b, _rx_b) = broadcast::channel::<ChangeNotification>(16);

        db_b.execute_unprepared("INSERT INTO tasks VALUES ('div-1', 'B-latest', 0)")
            .await
            .unwrap();
        crate::shadow::upsert_clock_entry(&db_b, "tasks", "div-1", "title", 2, 2, &site_b, 0)
            .await
            .unwrap();

        // B's changes arrive at A (col_version=2 < 3, should lose)
        let b_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "div-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("B-latest")),
            site_id: site_b,
            col_version: 2,
            cl: 2,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db_a,
            &tx_a,
            &registry_a,
            &b_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // A's changes arrive at B (col_version=3 > 2, should win)
        let a_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "div-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("A-latest")),
            site_id: site_a,
            col_version: 3,
            cl: 3,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db_b,
            &tx_b,
            &registry_b,
            &a_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Both should converge to A-latest (higher col_version)
        let result_a = db_a
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'div-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_a: String = result_a.try_get_by_index(0).unwrap();

        let result_b = db_b
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'div-1'".to_string(),
            ))
            .await
            .unwrap()
            .unwrap();
        let title_b: String = result_b.try_get_by_index(0).unwrap();

        assert_eq!(title_a, "A-latest", "A should keep its value (higher cv)");
        assert_eq!(title_b, "A-latest", "B should accept A's value (higher cv)");
        assert_eq!(title_a, title_b, "Both peers must converge");
    }

    async fn setup_engine_test_db_no_defaults() -> (sea_orm::DatabaseConnection, Arc<TableRegistry>)
    {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::shadow::create_meta_table(&db).await.unwrap();
        crate::peer_tracker::create_peer_versions_table(&db)
            .await
            .unwrap();
        // No DEFAULT on any NOT NULL column — INSERT missing columns will fail
        db.execute_unprepared(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL)",
        )
        .await
        .unwrap();
        crate::shadow::create_shadow_table(&db, "tasks")
            .await
            .unwrap();
        let registry = Arc::new(TableRegistry::new());
        registry.register(TableMeta {
            table_name: "tasks".to_string(),
            primary_key_column: "id".to_string(),
            columns: vec!["id".to_string(), "title".to_string(), "done".to_string()],
            delete_policy: crate::messages::DeletePolicy::default(),
        });
        (db, registry)
    }

    #[tokio::test]
    async fn test_apply_remote_changeset_out_of_order_update_before_insert() {
        // UPDATE arrives for a non-existent row → shadow must stay clean.
        // Then INSERT arrives → row is created with all columns, shadow populated.
        let (db, registry) = setup_engine_test_db_no_defaults().await;
        let (tx, mut rx) = broadcast::channel::<ChangeNotification>(16);
        let site = NodeId([2u8; 16]);

        // Step 1: Send UPDATE for non-existent row (only "title" column, cv=2)
        let update_changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "ooo-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("Updated Title")),
            site_id: site,
            col_version: 2,
            cl: 2,
            seq: 0,
            db_version: 0,
        }];
        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &update_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Row should NOT exist
        assert!(
            !row_exists(&db, "tasks", "id", "ooo-1").await,
            "Row should not exist after out-of-order UPDATE"
        );

        // Shadow should be clean (no entries for this row)
        let (cv, _) = crate::shadow::get_col_version_with_site(&db, "tasks", "ooo-1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 0, "Shadow should be clean after failed INSERT");

        // No notification should have been sent
        assert!(
            rx.try_recv().is_err(),
            "No notification expected for failed out-of-order UPDATE"
        );

        // Step 2: Send full INSERT (all columns, cv=1)
        let insert_changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "ooo-1".into(),
                cid: "id".into(),
                val: Some(serde_json::json!("ooo-1")),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "ooo-1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Original Title")),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "ooo-1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(0)),
                site_id: site,
                col_version: 1,
                cl: 1,
                seq: 2,
                db_version: 0,
            },
        ];
        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &insert_changes,
            None,
            crate::messages::ChangeSource::Local,
            None,
        )
        .await;

        // Row should now exist with INSERT's values (shadow was clean, so cv=1 wins)
        assert!(
            row_exists(&db, "tasks", "id", "ooo-1").await,
            "Row should exist after INSERT"
        );

        let result = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'ooo-1'".to_string(),
            ))
            .await
            .unwrap()
            .expect("Row should exist");
        let title: String = result.try_get_by_index(0).unwrap();
        assert_eq!(
            title, "Original Title",
            "INSERT's value should win since shadow was clean"
        );

        // Shadow should now be populated with cv=1
        let (cv, _) = crate::shadow::get_col_version_with_site(&db, "tasks", "ooo-1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 1, "Shadow should have cv=1 from the INSERT");

        // Notification should have been sent for the INSERT
        let notif = rx.try_recv().expect("Expected notification for INSERT");
        assert_eq!(notif.primary_key, "ooo-1");
    }

    #[tokio::test]
    async fn test_get_col_version_with_site() {
        let (db, _registry) = setup_engine_test_db().await;
        let site_id = NodeId([42u8; 16]);

        // No entry yet
        let (cv, sid) = crate::shadow::get_col_version_with_site(&db, "tasks", "pk1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 0);
        assert_eq!(sid, NodeId([0u8; 16]));

        // Insert a clock entry
        crate::shadow::upsert_clock_entry(&db, "tasks", "pk1", "title", 5, 1, &site_id, 0)
            .await
            .unwrap();

        let (cv, sid) = crate::shadow::get_col_version_with_site(&db, "tasks", "pk1", "title")
            .await
            .unwrap();
        assert_eq!(cv, 5);
        assert_eq!(sid, site_id);
    }
}
