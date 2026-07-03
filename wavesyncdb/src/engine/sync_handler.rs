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
                        SyncRequest::ReconcileDigest {
                            digest,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            self.handle_reconcile_digest_request(
                                peer, channel, digest, peer_topic, req_hmac,
                            );
                        }
                        SyncRequest::ReconcileRange {
                            entries,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: req_hmac,
                        } => {
                            self.handle_reconcile_range_request(
                                peer,
                                channel,
                                entries,
                                peer_site_id,
                                peer_topic,
                                req_hmac,
                            );
                        }
                    }
                }
                request_response::Message::Response {
                    response,
                    request_id,
                    ..
                } => {
                    // Response received — this peer no longer has an in-flight
                    // request in any group. Keep the removed send-time Instants
                    // so a ChangesetResponse below can compute its catch-up RTT
                    // without re-stamping (the request's send time is the only
                    // record of when the round trip started).
                    let mut cleared_sync_starts: HashMap<String, tokio::time::Instant> =
                        HashMap::new();
                    for (topic, g) in self.groups.iter_mut() {
                        if let Some(sent_at) = g.pending_sync_peers.remove(&peer) {
                            cleared_sync_starts.insert(topic.clone(), sent_at);
                        }
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
                                if let Ok(bytes) = serde_json::to_vec(&verify_resp) {
                                    // Counted regardless of verify outcome — the
                                    // bytes were spent on the wire either way.
                                    self.record_wire_bytes(&peer, bytes.len() as u64, true);
                                    if !gk.verify(&bytes, &tag) {
                                        log::debug!(
                                            "Rejecting sync response with invalid HMAC from peer {peer}"
                                        );
                                        return;
                                    }
                                }
                            }

                            // Catch-up round-trip time: reuse the request's
                            // send-time Instant captured above (do not
                            // re-stamp). If multiple groups had a request in
                            // flight to this peer, sample whichever group's
                            // response this is.
                            if let Some(sent_at) = cleared_sync_starts.get(&effective) {
                                let rtt_ms = sent_at.elapsed().as_millis() as u64;
                                self.diagnostics.observe_sync_rtt(rtt_ms);
                                self.peer_health.record_rtt(&peer, rtt_ms);
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
                                // Round trip completed successfully even with
                                // nothing to apply — still a sync.
                                self.peer_health.stamp_synced(&peer);
                                self.update_network_status();
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
                            // Confirmed delivery — drop this changeset from the
                            // group's pending-push retry set for this peer (#81).
                            self.note_push_ack(request_id, peer);
                        }
                        crate::protocol::SyncResponse::IdentityAck => {
                            log::debug!("Received IdentityAck from peer {peer}");
                        }
                        crate::protocol::SyncResponse::ReconcileResult {
                            converged,
                            digest,
                            topic: peer_topic,
                            hmac: resp_hmac,
                        } => {
                            self.handle_reconcile_result(
                                peer, converged, digest, peer_topic, resp_hmac,
                            );
                        }
                        crate::protocol::SyncResponse::ReconcileRangeResult {
                            entries,
                            site_id: peer_site_id,
                            topic: peer_topic,
                            hmac: resp_hmac,
                        } => {
                            self.handle_reconcile_range_result(
                                peer,
                                entries,
                                peer_site_id,
                                peer_topic,
                                resp_hmac,
                            );
                        }
                    }
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                error,
                request_id,
                ..
            } => {
                for g in self.groups.values_mut() {
                    g.pending_sync_peers.remove(&peer);
                }
                // Drop any pending-push correlation for this request: the
                // changeset stays in `pending_pushes` (still un-acked by this
                // peer) and the next redelivery tick retries it (#81).
                self.pending_push_reqs.remove(&request_id);
                if matches!(
                    error,
                    request_response::OutboundFailure::UnsupportedProtocols
                ) {
                    // The peer is connected but its substream negotiation rejected
                    // our snapshot protocol id — it runs an incompatible WaveSyncDB
                    // version. Surface it (once per peer) instead of silently never
                    // syncing, and do NOT re-dial: a redial won't change versions,
                    // and the periodic sync would otherwise re-trigger this forever.
                    self.note_protocol_mismatch(peer);
                } else {
                    log::warn!("Sync request to {peer} failed: {error}");
                    // Connection might be dead — re-dial if we know the peer's
                    // address. Storm guards (#84 regression): (1) never re-dial a
                    // circuit address for a peer we already reach directly — the
                    // failure was likely the demotion closing a redundant relay
                    // connection, and redialing just re-opens the circuit;
                    // (2) dedup against in-flight dials; (3) route through a
                    // `DialOpts::peer_id` build (default `DisconnectedAndNotDialing`
                    // condition) instead of a raw address dial so concurrent
                    // attempts fold into one.
                    if let Some(addr) = self.peers.get(&peer).cloned()
                        && !self.swarm.is_connected(&peer)
                        && !self.dialing_peers.contains(&peer)
                        && self.dial_backoff_ok(&peer)
                        && !(addr_is_relayed(&addr) && self.suppress_relay_dial(&peer))
                    {
                        log::info!("Re-dialing {peer} after outbound failure");
                        let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer)
                            .addresses(vec![addr])
                            .build();
                        if self.swarm.dial(dial_opts).is_ok() {
                            self.dialing_peers.insert(peer);
                        }
                    }
                }
            }
            request_response::Event::InboundFailure { peer, error, .. } => {
                if matches!(
                    error,
                    request_response::InboundFailure::UnsupportedProtocols
                ) {
                    self.note_protocol_mismatch(peer);
                } else {
                    log::warn!("Sync inbound from {peer} failed: {error}");
                }
            }
            _ => {}
        }
    }

    /// Log + emit a `PeerProtocolMismatch` exactly once per peer (deduped via
    /// `protocol_mismatch_peers`). Called when a sync request/response substream
    /// reports `UnsupportedProtocols` — the peer is connected at the transport
    /// level but runs an incompatible WaveSyncDB protocol version, so no data
    /// will sync with it. Surfacing this (instead of silently never syncing) is
    /// the diagnosable signal a stalled rolling upgrade needs. See PROBLEMS.md N5.
    fn note_protocol_mismatch(&mut self, peer: libp2p::PeerId) {
        if !self.protocol_mismatch_peers.insert(peer) {
            return; // already surfaced for this peer this connection
        }
        let our_protocol = super::snapshot_protocol::SNAPSHOT_PROTOCOL
            .as_ref()
            .to_string();
        log::warn!(
            "Peer {peer} does not speak our sync protocol {our_protocol} — it is running an \
             incompatible WaveSyncDB version; no data will sync with it until the versions match"
        );
        self.emit_network_event(crate::network_status::NetworkEvent::PeerProtocolMismatch {
            peer_id: crate::network_status::PeerId(peer.to_string()),
            our_protocol,
        });
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
            if let Ok(bytes) = serde_json::to_vec(&verify_req) {
                // Counted regardless of verify outcome — the bytes were spent
                // on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    // Per-group HMAC failure for a topic we hold → time-boxed
                    // rejection with backoff (Rule 2.8 / N6).
                    self.reject_peer_for_group(&effective, peer);
                    return;
                }
            }
            // HMAC verified — mark peer as a member of THIS group, and clear any
            // prior rejection backoff (it just proved the right key — recovery).
            let newly_verified = match self.groups.get_mut(&effective) {
                Some(g) if !g.verified_peers.contains(&peer) => {
                    g.verified_peers.insert(peer);
                    g.rejected_peers.remove(&peer);
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
        // No `&mut self` inside the spawned task below, so the relay
        // classification and the shared counters are captured here.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            // Note on long-offline cursors: shadow tables are upsert-in-place,
            // so an incremental response at ANY cursor is complete for all
            // live cells — physical tombstone GC removes only deletes older
            // than the retention window, which are intentionally
            // unrecoverable by any response shape (the documented
            // resurrection window). No full-resync fallback is needed.
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
                // Unsigned mode never reaches this branch, so its byte
                // metrics are approximate — acceptable since production
                // groups run passphrases.
                account_wire_bytes(
                    &diagnostics,
                    &peer_health,
                    &peer,
                    relayed,
                    bytes.len() as u64,
                    false,
                );
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
            // Push for a topic none of our groups hold — don't apply it (a group
            // we don't have doesn't mean we share no group with this peer over
            // the shared connection; Rule 2.8, multi-group). But we MUST still
            // answer the request: libp2p request-response requires a response
            // per inbound request, and dropping the channel here makes the
            // sender see an `unexpected end of file`, never receive its PushAck,
            // and redeliver this push to us every few seconds forever (#81
            // un-acked-redelivery loop) — a permanent battery/bandwidth drain
            // for any two peers with asymmetric group membership. PushAck just
            // completes the substream ("received"); it does not apply the data
            // and does not reject the peer, so Rules 2.7/2.8 are preserved. It
            // carries no HMAC (unit variant), identical to the success path, so
            // the sender accepts it regardless of which group key it holds.
            log::debug!(
                "Ignoring push from {peer} for unknown topic {effective} (acking so the sender stops redelivering)"
            );
            let resp_tx = self.snapshot_resp_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = resp_tx
                    .send((channel, crate::protocol::SyncResponse::PushAck))
                    .await
                {
                    log::error!("Failed to send PushAck for unknown topic: {e}");
                }
            });
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
            if let Ok(bytes) = serde_json::to_vec(&verify_req) {
                // Counted regardless of verify outcome — the bytes were spent
                // on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    // Per-group HMAC failure for a topic we hold → time-boxed
                    // rejection with backoff (Rule 2.8 / N6).
                    self.reject_peer_for_group(&effective, peer);
                    return;
                }
            }
            // HMAC verified — mark peer as a member of THIS group, and clear any
            // prior rejection backoff (it just proved the right key — recovery).
            let newly_verified = match self.groups.get_mut(&effective) {
                Some(g) if !g.verified_peers.contains(&peer) => {
                    g.verified_peers.insert(peer);
                    g.rejected_peers.remove(&peer);
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
            if let Ok(bytes) = serde_json::to_vec(&verify_req) {
                // Counted regardless of verify outcome — the bytes were spent
                // on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    log::debug!("Rejecting identity announce with invalid HMAC from peer {peer}");
                    return;
                }
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

    /// Reject a peer for a single group on a per-group HMAC failure (a
    /// spoofed / incorrect-key request for a topic we hold). The rejection is
    /// time-boxed with exponential backoff (Rule 2.8 anti-storm) rather than
    /// permanent: the peer is skipped while the window is open, the window grows
    /// on repeated failures, and a later successful verify removes the entry
    /// (recovery, N6 — see `handle_version_vector_request`). Scoped to this
    /// group only; the peer may still be a valid member of other groups.
    fn reject_peer_for_group(&mut self, effective_topic: &str, peer: libp2p::PeerId) {
        let backoff = if let Some(g) = self.groups.get_mut(effective_topic) {
            let attempts = g.rejected_peers.get(&peer).map(|r| r.attempts).unwrap_or(0) + 1;
            let dur = super::rejection_backoff(attempts);
            g.rejected_peers.insert(
                peer,
                super::RejectionState {
                    attempts,
                    until: tokio::time::Instant::now() + dur,
                },
            );
            g.verified_peers.remove(&peer);
            g.pending_sync_peers.remove(&peer);
            g.peer_db_versions.remove(&peer);
            g.peer_reported_versions.remove(&peer);
            Some((attempts, dur))
        } else {
            None
        };
        if let Some((attempts, dur)) = backoff {
            log::warn!(
                "Rejecting peer {peer} for group {effective_topic} (HMAC failure, attempt \
                 {attempts}); backing off for {dur:?}"
            );
            self.emit_network_event(crate::network_status::NetworkEvent::PeerRejected(
                crate::network_status::PeerId(peer.to_string()),
            ));
        }
        self.update_network_status();
    }

    /// Send a convergence-verification reconcile digest (#82) to every
    /// non-infrastructure, non-rejected peer for each registry-ready group.
    /// Called from the periodic tick alongside the version-vector catch-up.
    pub(super) fn send_reconcile_digests(&mut self) {
        let peers: Vec<libp2p::PeerId> = self
            .peers
            .keys()
            .filter(|p| !self.infrastructure_peers.contains(p))
            .copied()
            .collect();
        let topics: Vec<String> = self
            .groups
            .iter()
            .filter(|(_, g)| g.registry_is_ready)
            .map(|(t, _)| t.clone())
            .collect();
        for topic in &topics {
            for peer in &peers {
                self.send_reconcile_digest(*peer, topic);
            }
        }
    }

    /// Compute (off-loop) and send a single reconcile digest to `peer` for
    /// `effective_topic`. The digest is an async DB scan, so it's built in a
    /// spawned task and handed back via `reconcile_req_tx` for the event loop to
    /// send on the swarm (Rule 2.10). No-op for a gone group or a peer in
    /// rejection backoff.
    pub(super) fn send_reconcile_digest(&mut self, peer: libp2p::PeerId, effective_topic: &str) {
        let Some(g) = self.groups.get(effective_topic) else {
            return;
        };
        if g.is_rejected(&peer) {
            return;
        }
        let db = g.db.clone();
        let registry = g.registry.clone();
        let topic_name = g.topic_name.clone();
        let group_key = g.group_key.clone();
        let req_tx = self.reconcile_req_tx.clone();
        // No `&mut self` inside the spawned task below.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            let digest = reconcile::compute_group_digest(&db, &registry).await;
            let mut req = SyncRequest::ReconcileDigest {
                digest,
                topic: topic_name,
                hmac: None,
            };
            if let Some(ref gk) = group_key
                && let Ok(bytes) = serde_json::to_vec(&req)
            {
                let tag = gk.mac(&bytes);
                if let SyncRequest::ReconcileDigest { ref mut hmac, .. } = req {
                    *hmac = Some(tag);
                }
                account_wire_bytes(
                    &diagnostics,
                    &peer_health,
                    &peer,
                    relayed,
                    bytes.len() as u64,
                    false,
                );
            }
            let _ = req_tx.send((peer, req)).await;
        });
    }

    /// Handle an inbound reconcile digest (#82): verify HMAC, compute our own
    /// group digest off-loop, and reply whether the two match (proven
    /// converged). A per-group HMAC failure rejects the peer (Rule 2.8 / N6).
    fn handle_reconcile_digest_request(
        &mut self,
        peer: libp2p::PeerId,
        channel: request_response::ResponseChannel<crate::protocol::SyncResponse>,
        remote_digest: [u8; 32],
        peer_topic: String,
        req_hmac: Option<[u8; 32]>,
    ) {
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            log::debug!("Ignoring reconcile digest from {peer} for unknown topic {effective}");
            return;
        };
        let group_key = g.group_key.clone();

        if let Some(ref gk) = group_key {
            let tag = match req_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated reconcile digest from peer {peer}");
                    return;
                }
            };
            let verify_req = SyncRequest::ReconcileDigest {
                digest: remote_digest,
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_req) {
                // Counted regardless of verify outcome — the bytes were
                // spent on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    self.reject_peer_for_group(&effective, peer);
                    return;
                }
            }
        }

        let Some(g) = self.groups.get(&effective) else {
            return;
        };
        let db = g.db.clone();
        let registry = g.registry.clone();
        let topic_name = g.topic_name.clone();
        let resp_tx = self.snapshot_resp_tx.clone();
        // No `&mut self` inside the spawned task below.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            let local_digest = reconcile::compute_group_digest(&db, &registry).await;
            let converged = local_digest == remote_digest;
            let mut resp = crate::protocol::SyncResponse::ReconcileResult {
                converged,
                digest: local_digest,
                topic: topic_name,
                hmac: None,
            };
            if let Some(ref gk) = group_key
                && let Ok(bytes) = serde_json::to_vec(&resp)
            {
                let tag = gk.mac(&bytes);
                if let crate::protocol::SyncResponse::ReconcileResult { ref mut hmac, .. } = resp {
                    *hmac = Some(tag);
                }
                account_wire_bytes(
                    &diagnostics,
                    &peer_health,
                    &peer,
                    relayed,
                    bytes.len() as u64,
                    false,
                );
            }
            if let Err(e) = resp_tx.send((channel, resp)).await {
                log::error!("Failed to queue reconcile result: {e}");
            }
        });
    }

    /// Handle a reconcile result (#82): verify HMAC, then either record proven
    /// convergence (emit `PeerConverged` + diagnostic) or, on divergence, nudge
    /// a version-vector catch-up to repair the diff promptly.
    fn handle_reconcile_result(
        &mut self,
        peer: libp2p::PeerId,
        converged: bool,
        remote_digest: [u8; 32],
        peer_topic: String,
        resp_hmac: Option<[u8; 32]>,
    ) {
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            log::debug!("Ignoring reconcile result from {peer} for unknown topic {effective}");
            return;
        };
        if let Some(ref gk) = g.group_key {
            let tag = match resp_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated reconcile result from peer {peer}");
                    return;
                }
            };
            let verify_resp = crate::protocol::SyncResponse::ReconcileResult {
                converged,
                digest: remote_digest,
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_resp) {
                // Counted regardless of verify outcome — the bytes were
                // spent on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    log::debug!("Rejecting reconcile result with invalid HMAC from peer {peer}");
                    return;
                }
            }
        }

        if converged {
            // Proven converged → this peer fully speaks the reconcile protocol,
            // so the periodic version-vector catch-up is skipped for it from now
            // on (the digest exchange keeps proving convergence cheaply). We do
            // NOT mark capable on mere divergence: a peer that answers the
            // digest but can't do the bucket transfer (e.g. the browser engine)
            // must keep getting the version-vector so its data still flows.
            self.reconcile_capable.insert(peer);
            self.diagnostics
                .reconcile_converged
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            log::debug!("Reconcile: proven converged with peer {peer} for group {effective}");
            // The peer holds all our data for this group — clear it from the
            // pending-push retry set so we stop re-pushing to it (#81).
            self.note_peer_converged_pushes(&effective, peer);
            self.peer_health.stamp_converged(&peer);
            self.emit_network_event(crate::network_status::NetworkEvent::PeerConverged {
                peer_id: crate::network_status::PeerId(peer.to_string()),
                topic: effective,
            });
            self.update_network_status();
        } else {
            self.diagnostics
                .reconcile_diverged
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            log::debug!(
                "Reconcile: diverged with peer {peer} for group {effective}; \
                 starting recursive range reconciliation"
            );
            // Kick off the recursive range exchange (round 1: the keyspace split
            // into fingerprinted sub-ranges).
            self.send_reconcile_range_initial(peer, &effective);
        }
    }

    /// Send the first round of recursive range reconciliation to `peer` (#82):
    /// the keyspace split into fingerprinted sub-ranges. Computed off-loop (DB
    /// scan) and handed back via `reconcile_req_tx`.
    pub(super) fn send_reconcile_range_initial(
        &mut self,
        peer: libp2p::PeerId,
        effective_topic: &str,
    ) {
        let Some(g) = self.groups.get(effective_topic) else {
            return;
        };
        if g.is_rejected(&peer) {
            return;
        }
        let db = g.db.clone();
        let registry = g.registry.clone();
        let topic_name = g.topic_name.clone();
        let group_key = g.group_key.clone();
        let site_id = g.site_id;
        let req_tx = self.reconcile_req_tx.clone();
        // No `&mut self` inside the spawned task below.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            let cells = reconcile::enumerate_sorted_cells(&db, &registry).await;
            let entries = reconcile::initial_entries(&cells);
            let req = build_reconcile_range(
                entries,
                site_id,
                topic_name,
                group_key.as_ref(),
                &diagnostics,
                &peer_health,
                &peer,
                relayed,
            );
            let _ = req_tx.send((peer, req)).await;
        });
    }

    /// Handle an inbound `ReconcileRange` round (#82): verify HMAC, run one
    /// reconciliation step against our cells, apply anything the peer sent that
    /// we lacked, and reply with the next round's entries.
    fn handle_reconcile_range_request(
        &mut self,
        peer: libp2p::PeerId,
        channel: request_response::ResponseChannel<crate::protocol::SyncResponse>,
        entries: Vec<crate::protocol::RangeEntry>,
        peer_site_id: NodeId,
        peer_topic: String,
        req_hmac: Option<[u8; 32]>,
    ) {
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            log::debug!("Ignoring reconcile range from {peer} for unknown topic {effective}");
            return;
        };
        let group_key = g.group_key.clone();

        if let Some(ref gk) = group_key {
            let tag = match req_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated reconcile range from peer {peer}");
                    return;
                }
            };
            let verify_req = SyncRequest::ReconcileRange {
                entries: entries.clone(),
                site_id: peer_site_id,
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_req) {
                // Counted regardless of verify outcome — the bytes were
                // spent on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    self.reject_peer_for_group(&effective, peer);
                    return;
                }
            }
        }

        let Some(g) = self.groups.get(&effective) else {
            return;
        };
        let db = g.db.clone();
        let registry = g.registry.clone();
        let topic_name = g.topic_name.clone();
        let local_site_id = g.site_id;
        let resp_tx = self.snapshot_resp_tx.clone();
        let changeset_tx = self.remote_changeset_tx.clone();
        let effective_for_apply = effective.clone();
        // No `&mut self` inside the spawned task below.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            let cells = reconcile::enumerate_sorted_cells(&db, &registry).await;
            let (reply, to_apply) = reconcile::reconcile_step(&cells, &entries);
            if !to_apply.is_empty() {
                let _ = changeset_tx
                    .send(RemoteChangeset {
                        peer,
                        peer_site: peer_site_id,
                        peer_db_version: None,
                        effective_topic: effective_for_apply,
                        changes: to_apply,
                    })
                    .await;
            }
            let mut resp = crate::protocol::SyncResponse::ReconcileRangeResult {
                entries: reply,
                site_id: local_site_id,
                topic: topic_name,
                hmac: None,
            };
            if let Some(ref gk) = group_key
                && let Ok(bytes) = serde_json::to_vec(&resp)
            {
                let tag = gk.mac(&bytes);
                if let crate::protocol::SyncResponse::ReconcileRangeResult {
                    ref mut hmac, ..
                } = resp
                {
                    *hmac = Some(tag);
                }
                account_wire_bytes(
                    &diagnostics,
                    &peer_health,
                    &peer,
                    relayed,
                    bytes.len() as u64,
                    false,
                );
            }
            if let Err(e) = resp_tx.send((channel, resp)).await {
                log::error!("Failed to queue reconcile range result: {e}");
            }
        });
    }

    /// Handle a `ReconcileRangeResult` (#82): verify HMAC, run a reconciliation
    /// step against the reply, apply what the peer sent, and — if the exchange
    /// isn't done — send the next round.
    fn handle_reconcile_range_result(
        &mut self,
        peer: libp2p::PeerId,
        entries: Vec<crate::protocol::RangeEntry>,
        peer_site_id: NodeId,
        peer_topic: String,
        resp_hmac: Option<[u8; 32]>,
    ) {
        let effective = if peer_topic.is_empty() {
            self.default_effective_topic.clone()
        } else {
            peer_topic.clone()
        };
        let Some(g) = self.groups.get(&effective) else {
            log::debug!(
                "Ignoring reconcile range result from {peer} for unknown topic {effective}"
            );
            return;
        };
        if let Some(ref gk) = g.group_key {
            let tag = match resp_hmac {
                Some(t) => t,
                None => {
                    log::debug!("Rejecting unauthenticated reconcile range result from {peer}");
                    return;
                }
            };
            let verify_resp = crate::protocol::SyncResponse::ReconcileRangeResult {
                entries: entries.clone(),
                site_id: peer_site_id,
                topic: peer_topic.clone(),
                hmac: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&verify_resp) {
                // Counted regardless of verify outcome — the bytes were
                // spent on the wire either way.
                self.record_wire_bytes(&peer, bytes.len() as u64, true);
                if !gk.verify(&bytes, &tag) {
                    log::debug!("Rejecting reconcile range result with invalid HMAC from {peer}");
                    return;
                }
            }
        }

        if entries.is_empty() {
            // Exchange complete — the peer had nothing further. Convergence is
            // confirmed (and the peer gated off the version-vector) by the next
            // digest round.
            return;
        }

        let Some(g) = self.groups.get(&effective) else {
            return;
        };
        let db = g.db.clone();
        let registry = g.registry.clone();
        let topic_name = g.topic_name.clone();
        let site_id = g.site_id;
        let group_key = g.group_key.clone();
        let changeset_tx = self.remote_changeset_tx.clone();
        let req_tx = self.reconcile_req_tx.clone();
        let effective_for_apply = effective.clone();
        // No `&mut self` inside the spawned task below.
        let relayed = self.peer_via_relay.get(&peer).copied().unwrap_or(false);
        let diagnostics = self.diagnostics.clone();
        let peer_health = self.peer_health.clone();

        tokio::spawn(async move {
            let cells = reconcile::enumerate_sorted_cells(&db, &registry).await;
            let (next, to_apply) = reconcile::reconcile_step(&cells, &entries);
            if !to_apply.is_empty() {
                let _ = changeset_tx
                    .send(RemoteChangeset {
                        peer,
                        peer_site: peer_site_id,
                        peer_db_version: None,
                        effective_topic: effective_for_apply,
                        changes: to_apply,
                    })
                    .await;
            }
            if !next.is_empty() {
                let req = build_reconcile_range(
                    next,
                    site_id,
                    topic_name,
                    group_key.as_ref(),
                    &diagnostics,
                    &peer_health,
                    &peer,
                    relayed,
                );
                let _ = req_tx.send((peer, req)).await;
            }
        });
    }
}

/// Build a `ReconcileRange` request, signed when a group key is configured (Rule 2.7).
#[allow(clippy::too_many_arguments)]
fn build_reconcile_range(
    entries: Vec<crate::protocol::RangeEntry>,
    site_id: NodeId,
    topic: String,
    group_key: Option<&GroupKey>,
    diagnostics: &crate::diagnostics::Counters,
    peer_health: &crate::diagnostics::PeerHealthStore,
    peer: &libp2p::PeerId,
    relayed: bool,
) -> SyncRequest {
    let mut req = SyncRequest::ReconcileRange {
        entries,
        site_id,
        topic,
        hmac: None,
    };
    if let Some(gk) = group_key
        && let Ok(bytes) = serde_json::to_vec(&req)
    {
        let tag = gk.mac(&bytes);
        if let SyncRequest::ReconcileRange { ref mut hmac, .. } = req {
            *hmac = Some(tag);
        }
        account_wire_bytes(
            diagnostics,
            peer_health,
            peer,
            relayed,
            bytes.len() as u64,
            false,
        );
    }
    req
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
pub struct NotifyCtx<'a> {
    pub registry: &'a crate::registry::NotificationRegistry,
    pub tx: &'a broadcast::Sender<crate::notify::Notification>,
}

// `pub` (reachable only through the hidden `engine::convergence` re-export)
// so the cross-implementation convergence test suite can push changesets
// through the real native apply path.
/// Returns `true` only if every chunk's transaction committed. A caller that
/// wants to know whether the apply is durable (e.g. before recording the sync
/// as successful) must check this — the function itself already rolls back
/// and logs on failure, so this return value is purely for the caller's own
/// bookkeeping and never changes what got applied or rolled back.
pub async fn apply_remote_changeset(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    changes: &[ColumnChange],
    db_version_cache: Option<&std::sync::atomic::AtomicU64>,
    source: crate::messages::ChangeSource,
    notify: Option<NotifyCtx<'_>>,
) -> bool {
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
        return apply_changeset_chunk(
            db,
            change_tx,
            registry,
            &grouped,
            db_version_cache,
            source,
            notify,
        )
        .await;
    }

    // Large changesets: split into chunks. A chunk that fails is logged and
    // rolled back by `apply_changeset_chunk` itself; subsequent chunks still
    // run (unchanged behavior) but the overall result reflects the failure.
    let mut all_committed = true;
    while !grouped.is_empty() {
        let chunk_end = grouped.len().min(CHANGESET_CHUNK_SIZE);
        let chunk: Vec<_> = grouped.drain(..chunk_end).collect();
        let committed = apply_changeset_chunk(
            db,
            change_tx,
            registry,
            &chunk,
            db_version_cache,
            source,
            notify,
        )
        .await;
        all_committed &= committed;
    }
    all_committed
}

/// Returns `true` if this chunk's transaction committed, `false` if it was
/// rolled back (or never began) due to an error along the way. Every failure
/// branch below already logs and rolls back on its own — the return value
/// only lets the caller distinguish "applied" from "rolled back" without
/// re-deriving it from log output.
async fn apply_changeset_chunk<'a>(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    grouped: &[((&'a str, &'a str), Vec<&'a ColumnChange>)],
    db_version_cache: Option<&std::sync::atomic::AtomicU64>,
    source: crate::messages::ChangeSource,
    notify: Option<NotifyCtx<'_>>,
) -> bool {
    use sea_orm::TransactionTrait;

    let txn = match db.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to begin transaction for remote changeset: {e}");
            return false;
        }
    };

    // Suppress change capture for the duration of this transaction: the
    // user-table writes below are REMOTE state, and re-capturing them would
    // re-broadcast every applied changeset — two peers would echo the same
    // changes back and forth forever. The flag is row state inside this
    // transaction, so a rollback (or crash) restores it, and SQLite's
    // single-writer locking means no concurrent local write can observe it.
    // Fail closed: applying without suppression must never happen.
    if let Err(e) = crate::capture::set_capture_suppressed(&txn, true).await {
        log::error!("Failed to suppress change capture for remote apply: {e}");
        let _ = txn.rollback().await;
        return false;
    }

    let local_db_version = match shadow::increment_db_version(&txn).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to increment db_version: {e}");
            let _ = txn.rollback().await;
            return false;
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
            match apply_remote_delete(&txn, table, pk, change, &meta, local_db_version).await {
                Ok(true) => {
                    any_applied = true;
                    is_delete = true;
                }
                Ok(false) => {}
                Err(e) => {
                    // Fail closed: a shadow/row write failed mid-apply. Roll back
                    // the entire chunk rather than commit a partially-applied row
                    // whose clock state would diverge silently. The changeset is
                    // idempotent and will be re-delivered / re-reconciled.
                    log::error!("Remote delete {table}/{pk} failed, rolling back chunk: {e}");
                    let _ = txn.rollback().await;
                    return false;
                }
            }
        } else {
            match apply_remote_column_changes(&txn, table, pk, row_changes, &meta, local_db_version)
                .await
            {
                Ok((applied, row_existed, pairs)) => {
                    if applied {
                        any_applied = true;
                        existed = row_existed;
                        changed_pairs = pairs;
                    }
                }
                Err(e) => {
                    log::error!("Remote column apply {table}/{pk} failed, rolling back chunk: {e}");
                    let _ = txn.rollback().await;
                    return false;
                }
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
            // `changed_columns` always reflects only what THIS changeset altered.
            let changed_columns: Option<Vec<String>> = if is_delete || changed_pairs.is_empty() {
                None
            } else {
                Some(changed_pairs.iter().map(|(c, _)| c.clone()).collect())
            };
            // `column_values` feeds both the reactive-hook fast path and the
            // SyncNotify row reconstruction. A catch-up path (RBSR, #82) can
            // deliver a row's columns across several changesets, so this batch
            // may carry only a subset of the row. When the table has a
            // notification policy, use the committed full row as a base so
            // `on_sync` sees a complete model regardless of how the cells were
            // batched — BUT the changeset's own values must win where present:
            // they carry correct JSON types (e.g. booleans as `true`/`false`),
            // whereas a generic `json_object` read returns SQLite-typed values
            // (booleans as `0`/`1`) that fail typed deserialization and would
            // make `wavesync_from_changes` — and thus the notification — return
            // None. Merge: full row first, then overlay the changeset values.
            let column_values: Option<Vec<(crate::ColumnName, serde_json::Value)>> = if is_delete {
                None
            } else {
                let mut merged: std::collections::BTreeMap<String, serde_json::Value> =
                    std::collections::BTreeMap::new();
                if notify.is_some_and(|ctx| ctx.registry.has(table)) {
                    // Read from the open transaction so we see the just-applied
                    // (not-yet-committed) row state.
                    for (c, v) in read_row_values(&txn, table, &meta, pk).await {
                        merged.insert(c, v);
                    }
                }
                // Overlay the changeset's correctly-typed values (these win).
                for (c, v) in &changed_pairs {
                    merged.insert(c.clone(), v.clone());
                }
                if merged.is_empty() {
                    None
                } else {
                    Some(
                        merged
                            .into_iter()
                            .map(|(c, v)| (crate::ColumnName(c), v))
                            .collect(),
                    )
                }
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

    // Lift the capture suppression inside the same transaction so the flag
    // can never leak past it — committed applies and rolled-back applies
    // both leave the guard at 0.
    if let Err(e) = crate::capture::set_capture_suppressed(&txn, false).await {
        log::error!("Failed to restore change capture after remote apply: {e}");
        let _ = txn.rollback().await;
        return false;
    }

    if let Err(e) = txn.commit().await {
        log::error!("Failed to commit remote changeset transaction: {e}");
        return false;
    }

    if let Some(cache) = db_version_cache {
        cache.fetch_max(local_db_version, std::sync::atomic::Ordering::Release);
    }

    for n in pending_notifications {
        // Run the per-table notification policy (remote-only) before broadcasting
        // the raw change. The gate inside `dispatch` de-spams bursts.
        if let Some(ctx) = notify {
            // Diagnostic: for every applied change on a table that HAS a policy,
            // record the write kind and the dispatch outcome. This pins down why
            // a notification did or didn't fire — "Insert DECLINED" means the
            // policy's `on_sync` returned None (e.g. the typed row couldn't be
            // rebuilt); "Update DECLINED" is the expected silence for edits;
            // "generated" means it fired and was queued for display.
            if ctx.registry.has(&n.table.0) {
                match ctx.registry.dispatch(&n) {
                    Some(user_notif) => {
                        log::info!(
                            "notification: generated for table {} kind={:?} pk={} ({} receiver(s) subscribed)",
                            n.table.0,
                            n.kind,
                            n.primary_key.0,
                            ctx.tx.receiver_count(),
                        );
                        let _ = ctx.tx.send(user_notif);
                    }
                    None => {
                        log::info!(
                            "notification: policy DECLINED table {} kind={:?} pk={} cols={:?} (on_sync returned None)",
                            n.table.0,
                            n.kind,
                            n.primary_key.0,
                            n.column_values
                                .as_ref()
                                .map(|cv| cv.iter().map(|(c, _)| c.0.as_str()).collect::<Vec<_>>()),
                        );
                    }
                }
            }
        }
        let _ = change_tx.send(n);
    }

    true
}

/// Apply a remote delete: check conflict resolution, delete row, update shadow.
///
/// Returns `Ok(true)` if the delete was applied, `Ok(false)` if it lost the
/// conflict check (a legitimate skip). Any *database* failure — the row delete,
/// the clock-entry clear, or the tombstone insert — is returned as `Err` so the
/// caller fails closed and rolls back: a row deleted without its tombstone
/// landing could never be adjudicated against a later resurrection.
async fn apply_remote_delete(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    change: &ColumnChange,
    meta: &crate::registry::TableMeta,
    local_db_version: u64,
) -> Result<bool, sea_orm::DbErr> {
    // An incoming tombstone already past the retention cutoff is
    // semantically nonexistent — applying it would re-introduce a delete
    // that peers with a physically-collected copy no longer hold, and the
    // pair would never re-converge.
    if let Some(ts) = change.deleted_ts
        && let Ok(Some(cutoff)) = shadow::tombstone_cutoff(db).await
        && ts < cutoff
    {
        log::debug!("Skipping aged incoming tombstone for {table}/{pk}");
        return Ok(false);
    }

    let local_entries = shadow::get_clock_entries_for_row(db, table, pk)
        .await
        .unwrap_or_default();
    let local_max_cv = local_entries
        .iter()
        .map(|e| e.col_version)
        .max()
        .unwrap_or(0);

    if !conflict::should_apply_delete(change.cl, local_max_cv, &meta.delete_policy) {
        return Ok(false);
    }

    let delete_sql = format!(
        "DELETE FROM \"{}\" WHERE \"{}\" = $1",
        table, meta.primary_key_column
    );
    db.execute_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        &delete_sql,
        [pk.to_string().into()],
    ))
    .await?;

    shadow::delete_clock_entries(db, table, pk).await?;
    // Store the DELETER's wire timestamp verbatim — the shared value is what
    // makes retention aging deterministic across replicas. The fallback can
    // only fire for a peer that somehow omitted it; local receipt time is
    // the safest remaining approximation.
    shadow::insert_tombstone(
        db,
        table,
        pk,
        change.col_version,
        local_db_version,
        &change.site_id,
        change.deleted_ts.unwrap_or_else(shadow::unix_now_secs),
    )
    .await?;

    Ok(true)
}

/// Apply non-delete column changes: resolve conflicts per-column, write winning values,
/// update shadow tables. Returns `(applied, existed, changed_column_pairs)` where
/// `existed` is whether the row already existed *before* this changeset was applied
/// (so the caller can classify the write as `Update` vs `Insert`), and each pair in
/// `changed_column_pairs` is `(column_name, post_write_json_value)` for the columns
/// that actually got applied. Reactive hooks consume the JSON values to update signal
/// state in place without re-querying SeaORM.
#[allow(clippy::type_complexity)]
async fn apply_remote_column_changes(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    row_changes: &[&ColumnChange],
    meta: &crate::registry::TableMeta,
    local_db_version: u64,
) -> Result<(bool, bool, Vec<(String, serde_json::Value)>), sea_orm::DbErr> {
    let exists = row_exists(db, table, &meta.primary_key_column, pk).await;

    // N8: adjudicate the row against a local tombstone BEFORE applying columns.
    // If this replica deleted the row, an incoming column edit either provably
    // outlives the delete (clear the tombstone and let the row survive) or does
    // not (the delete still dominates — skip the edit and do NOT resurrect). This
    // is the same deterministic predicate as the delete path, rerun with the
    // incoming edit's causal position, so every tombstone-holder converges on the
    // same clear/keep decision regardless of message order (Rule 2.6 preserved).
    if let Some(tomb_cl) = shadow::get_tombstone_cl(db, table, pk).await? {
        let incoming_max = row_changes
            .iter()
            .filter(|c| {
                meta.columns.iter().any(|col| col == &c.cid.0) && c.cid.0 != meta.primary_key_column
            })
            .map(|c| c.col_version)
            .max()
            .unwrap_or(0);
        if conflict::should_apply_delete(tomb_cl, incoming_max, &meta.delete_policy) {
            // Delete still dominates: don't apply the edit, don't resurrect.
            return Ok((false, exists, Vec::new()));
        }
        // Delete provably lost — clear it so the pair reconverges (this heals the
        // asymmetric-tombstone state even when no column ends up winning below).
        if exists {
            shadow::clear_tombstone(db, table, pk).await?;
        } else {
            // Resurrection: drop the stale tombstone + any residual clock entries
            // so this replica's cell set matches a peer that saw the delete and
            // then the re-insert (winning columns are re-inserted fresh below at
            // their remote col_versions).
            shadow::delete_clock_entries(db, table, pk).await?;
        }
    }

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
        return Ok((false, exists, changed_columns));
    }

    if exists {
        // UPDATE each winning column. A DB failure here propagates so the caller
        // rolls back the whole chunk — committing the user value without its
        // matching shadow clock (flushed just below) is the silent-divergence bug.
        for (col, val) in &winning_columns {
            let update_sql = format!(
                "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                table, col, meta.primary_key_column
            );
            db.execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                &update_sql,
                [val.clone(), pk.to_string().into()],
            ))
            .await?;
        }
        flush_shadow_updates(db, table, pk, &pending_shadow_updates, local_db_version).await?;
        return Ok((true, exists, changed_columns));
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

    db.execute_raw(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        &insert_sql,
        values,
    ))
    .await?;

    // UPDATE each winning column individually — works whether INSERT
    // succeeded or was ignored due to concurrent insert
    for (col, val) in &winning_columns {
        if *col != meta.primary_key_column {
            let update_sql = format!(
                "UPDATE \"{}\" SET \"{}\" = $1 WHERE \"{}\" = $2",
                table, col, meta.primary_key_column
            );
            db.execute_raw(sea_orm::Statement::from_sql_and_values(
                sea_orm::DatabaseBackend::Sqlite,
                &update_sql,
                [val.clone(), pk.to_string().into()],
            ))
            .await?;
        }
    }

    // Verify INSERT actually created the row before writing shadow. A row that
    // wasn't created (e.g. missing NOT NULL columns from out-of-order delivery)
    // is a legitimate deferral, not an error — return Ok((false, ...)) and let a
    // later changeset complete it. Only genuine DB failures above roll back.
    if row_exists(db, table, &meta.primary_key_column, pk).await {
        flush_shadow_updates(db, table, pk, &pending_shadow_updates, local_db_version).await?;
        Ok((true, exists, changed_columns))
    } else {
        log::debug!(
            "Row {}/{} not created (likely missing NOT NULL columns from \
             out-of-order delivery), deferring shadow updates",
            table,
            pk
        );
        Ok((false, exists, changed_columns))
    }
}

/// Write pending shadow table clock entries after a successful DB write.
async fn flush_shadow_updates(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    updates: &[(String, u64, crate::messages::NodeId, u32)],
    local_db_version: u64,
) -> Result<(), sea_orm::DbErr> {
    // Propagate any shadow-clock write failure instead of swallowing it. A user
    // row committed without its authoritative col_version is the seed of silent
    // divergence (a peer applies the value but never learns the correct version),
    // so the caller must fail closed and roll back the whole changeset for retry.
    for (cid, cv, site, seq) in updates {
        shadow::upsert_clock_entry(db, table, pk, cid, *cv, local_db_version, site, *seq).await?;
    }
    Ok(())
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
    // The blob-safe shared expression keeps these bytes identical to what a
    // sender's capture trigger produced for the same logical value — the
    // tiebreak compares them byte-wise, so the spellings must match.
    let sql = format!(
        "SELECT json_object('v', {}) as json_val FROM \"{}\" WHERE \"{}\" = $1",
        crate::capture::json_col_expr("", cid),
        table,
        pk_col
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

/// Read the current value of every column of a row as JSON `(column, value)`
/// pairs. Returns an empty vec if the row is absent.
///
/// Used to reconstruct the *full* row for a `#[derive(SyncNotify)]` policy. A
/// catch-up path (notably RBSR, #82) can deliver a row's columns across several
/// separate changesets, so the columns present in any one apply call may be only
/// a subset of the row. The notification dispatch needs every required column at
/// once to rebuild the typed model, so we read the committed row here rather than
/// relying on whatever subset this particular changeset happened to carry.
pub(super) async fn read_row_values(
    db: &impl ConnectionTrait,
    table: &str,
    meta: &crate::registry::TableMeta,
    pk: &str,
) -> Vec<(String, serde_json::Value)> {
    if meta.columns.is_empty() {
        return Vec::new();
    }
    // Build `json_object('col', <blob-safe expr>, …)` so SQLite handles the
    // per-type value→JSON conversion for us. Column names are schema
    // identifiers (never user input), so they are safe to interpolate.
    let obj_args = meta
        .columns
        .iter()
        .map(|c| format!("'{c}', {}", crate::capture::json_col_expr("", c)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT json_object({obj_args}) AS row_json FROM \"{}\" WHERE \"{}\" = $1",
        table, meta.primary_key_column
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
    let Some(qr) = result else {
        return Vec::new();
    };
    let Ok(raw) = qr.try_get::<String>("", "row_json") else {
        return Vec::new();
    };
    match serde_json::from_str::<serde_json::Value>(&raw) {
        Ok(serde_json::Value::Object(map)) => map.into_iter().collect(),
        _ => Vec::new(),
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
        crate::capture::ensure_capture_tables(&db).await.unwrap();
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

    /// REGRESSION — a remote changeset carrying only a SUBSET of a row's
    /// columns must still produce a user notification.
    ///
    /// RBSR (#82) delivers a row's columns as individual cells that can be split
    /// across separate changesets, so the columns in any one apply call may be a
    /// strict subset of the row. A `#[derive(SyncNotify)]` policy needs every
    /// required column to rebuild the typed row, so the notification is
    /// reconstructed from the committed row state — not just the batch's columns.
    /// Before the fix, the absent `done` column made row reconstruction fail and
    /// the notification was silently dropped even though the data synced.
    #[tokio::test]
    async fn notify_reconstructs_full_row_from_partial_changeset() {
        struct Task {
            id: String,
            title: String,
            #[allow(dead_code)]
            done: i64,
        }
        impl crate::SyncedModel for Task {
            fn wavesync_apply_change(&mut self, _c: &str, _v: &serde_json::Value) {}
            fn wavesync_from_changes(
                _pk_col: &str,
                pk: &str,
                changes: &[(String, serde_json::Value)],
            ) -> Option<Self> {
                let mut title = None;
                let mut done = None;
                for (c, v) in changes {
                    match c.as_str() {
                        "title" => title = v.as_str().map(str::to_string),
                        "done" => done = v.as_i64(),
                        _ => {}
                    }
                }
                // Both `title` and `done` are required — reconstruction fails if
                // either is absent, which is the crux of the dropped-notification bug.
                Some(Task {
                    id: pk.to_string(),
                    title: title?,
                    done: done?,
                })
            }
            fn wavesync_pk_string(&self) -> String {
                self.id.clone()
            }
        }
        impl crate::SyncNotify for Task {
            fn on_sync(ev: &crate::SyncEvent<Self>) -> Option<crate::Notification> {
                ev.row
                    .as_ref()
                    .map(|t| crate::Notification::new("task", &t.title))
            }
        }

        let (db, registry) = setup_engine_test_db().await;
        let (change_tx, _crx) = broadcast::channel::<ChangeNotification>(16);

        let notif_registry = crate::registry::NotificationRegistry::new();
        notif_registry.register("tasks".to_string(), crate::notify::make_dispatch::<Task>());
        let (notif_tx, mut notif_rx) = broadcast::channel::<crate::Notification>(16);

        // Deliver ONLY the "title" cell — "done" is absent and falls to the
        // table default. This is the shape RBSR produces when a row's cells are
        // split across batches.
        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "t1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("Hello")),
            site_id: NodeId([7u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
            deleted_ts: None,
        }];

        let ctx = NotifyCtx {
            registry: &notif_registry,
            tx: &notif_tx,
        };
        apply_remote_changeset(
            &db,
            &change_tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Remote {
                peer_site: NodeId([9u8; 16]),
            },
            Some(ctx),
        )
        .await;

        let got = notif_rx.try_recv();
        assert!(
            got.is_ok(),
            "a subset-of-columns changeset must still notify (full row read from committed state)"
        );
        assert_eq!(got.unwrap().body, "Hello");
    }

    /// REGRESSION — a `bool` column must not break notification reconstruction.
    ///
    /// SQLite stores booleans as integers `0/1`, so the full-row read used to
    /// rebuild the typed model returns them as JSON numbers. Typed
    /// deserialization (`wavesync_from_changes`) of a `bool` field then fails on
    /// `0/1`, `on_sync` gets `row = None`, and the notification is silently
    /// dropped — exactly what broke notifications for `grocery_item`
    /// (`checked`/`is_recurring`). The changeset carries the correctly-typed
    /// `true/false`, so the apply path must let the changeset value win over the
    /// SQLite-typed full-row read.
    #[tokio::test]
    async fn notify_reconstructs_bool_column_from_changeset_value() {
        struct Task {
            id: String,
            #[allow(dead_code)]
            title: String,
            done: bool,
        }
        impl crate::SyncedModel for Task {
            fn wavesync_apply_change(&mut self, _c: &str, _v: &serde_json::Value) {}
            fn wavesync_from_changes(
                _pk_col: &str,
                pk: &str,
                changes: &[(String, serde_json::Value)],
            ) -> Option<Self> {
                let mut title = None;
                let mut done = None;
                for (c, v) in changes {
                    match c.as_str() {
                        "title" => title = v.as_str().map(str::to_string),
                        // Strict bool deserialization — fails on JSON 0/1,
                        // which is the crux of the regression.
                        "done" => done = serde_json::from_value::<bool>(v.clone()).ok(),
                        _ => {}
                    }
                }
                Some(Task {
                    id: pk.to_string(),
                    title: title?,
                    done: done?,
                })
            }
            fn wavesync_pk_string(&self) -> String {
                self.id.clone()
            }
        }
        impl crate::SyncNotify for Task {
            fn on_sync(ev: &crate::SyncEvent<Self>) -> Option<crate::Notification> {
                ev.row
                    .as_ref()
                    .map(|t| crate::Notification::new("task", if t.done { "done" } else { "todo" }))
            }
        }

        let (db, registry) = setup_engine_test_db().await;
        let (change_tx, _crx) = broadcast::channel::<ChangeNotification>(16);
        let notif_registry = crate::registry::NotificationRegistry::new();
        notif_registry.register("tasks".to_string(), crate::notify::make_dispatch::<Task>());
        let (notif_tx, mut notif_rx) = broadcast::channel::<crate::Notification>(16);

        // New row; `done` arrives as a proper JSON bool in the changeset but is
        // stored in SQLite as integer 1 — the full-row read would surface it as
        // `1`, which must not clobber the changeset's `true`.
        let changes = vec![
            ColumnChange {
                table: "tasks".into(),
                pk: "t1".into(),
                cid: "title".into(),
                val: Some(serde_json::json!("Hello")),
                site_id: NodeId([7u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 0,
                db_version: 0,
                deleted_ts: None,
            },
            ColumnChange {
                table: "tasks".into(),
                pk: "t1".into(),
                cid: "done".into(),
                val: Some(serde_json::json!(true)),
                site_id: NodeId([7u8; 16]),
                col_version: 1,
                cl: 1,
                seq: 1,
                db_version: 0,
                deleted_ts: None,
            },
        ];

        let ctx = NotifyCtx {
            registry: &notif_registry,
            tx: &notif_tx,
        };
        apply_remote_changeset(
            &db,
            &change_tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Remote {
                peer_site: NodeId([9u8; 16]),
            },
            Some(ctx),
        )
        .await;

        let got = notif_rx.try_recv();
        assert!(
            got.is_ok(),
            "a bool column must reconstruct from the changeset's typed value, not the SQLite int"
        );
        assert_eq!(got.unwrap().body, "done");
    }

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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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

    /// A remote apply must not be re-captured by the change triggers —
    /// re-capturing would re-broadcast the changeset and two peers would
    /// echo it back and forth forever. A local write afterwards must still
    /// capture normally (the suppression is scoped to the apply tx).
    #[tokio::test]
    async fn test_remote_apply_is_not_recaptured() {
        let (db, registry) = setup_engine_test_db().await;
        let meta = registry.get("tasks").unwrap();
        crate::capture::ensure_triggers(&db, &meta).await.unwrap();
        let (tx, _rx) = broadcast::channel::<ChangeNotification>(16);

        let changes = vec![ColumnChange {
            table: "tasks".into(),
            pk: "remote-1".into(),
            cid: "title".into(),
            val: Some(serde_json::json!("from peer")),
            site_id: NodeId([2u8; 16]),
            col_version: 1,
            cl: 1,
            seq: 0,
            db_version: 0,
            deleted_ts: None,
        }];
        apply_remote_changeset(
            &db,
            &tx,
            &registry,
            &changes,
            None,
            crate::messages::ChangeSource::Remote {
                peer_site: NodeId([2u8; 16]),
            },
            None,
        )
        .await;

        // The remote row landed...
        let row = db
            .query_one_raw(sea_orm::Statement::from_string(
                sea_orm::DatabaseBackend::Sqlite,
                "SELECT title FROM tasks WHERE id = 'remote-1'".to_string(),
            ))
            .await
            .unwrap();
        assert!(row.is_some(), "remote row should have been applied");
        // ...but nothing was captured.
        let captured = crate::capture::fetch_capture_rows(&db).await.unwrap();
        assert!(
            captured.is_empty(),
            "remote apply must not enter the capture table, got {captured:?}"
        );

        // A local (unsuppressed) write still captures.
        db.execute_unprepared("INSERT INTO tasks (id, title) VALUES ('local-1', 'mine')")
            .await
            .unwrap();
        let captured = crate::capture::fetch_capture_rows(&db).await.unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].pk, "local-1");
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
            deleted_ts: None,
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
        crate::capture::ensure_capture_tables(&db).await.unwrap();
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
            deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
                deleted_ts: None,
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
