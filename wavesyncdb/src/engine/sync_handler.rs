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
                    self.pending_sync_peers.remove(&peer);
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
                            // Verify HMAC if group key is configured
                            if let Some(ref gk) = self.group_key {
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

                            // Ignore responses from peers on a different topic
                            if !peer_topic.is_empty() && peer_topic != self.topic_name {
                                log::debug!(
                                    "Ignoring sync response from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                                    self.topic_name
                                );
                                // Permanently reject this peer so mDNS won't re-add it
                                self.reject_peer(peer);
                                return;
                            }

                            // Update our knowledge of this peer's version
                            self.peer_db_versions.insert(peer, my_db_version);
                            let reported = self.peer_reported_versions.entry(peer).or_insert(0);
                            *reported = (*reported).max(my_db_version);
                            self.emit_network_event(
                                crate::network_status::NetworkEvent::PeerSynced {
                                    peer_id: crate::network_status::PeerId(peer.to_string()),
                                    db_version: my_db_version,
                                },
                            );
                            self.update_network_status();

                            // Update local db_version with Lamport semantics
                            let lamport_bump = if my_db_version > self.local_db_version {
                                self.local_db_version = my_db_version;
                                true
                            } else {
                                false
                            };

                            if changes.is_empty() {
                                log::info!(
                                    "Version vector sync with peer {peer}: already up to date"
                                );
                                // Still need to persist the Lamport bump even if no changes
                                if lamport_bump {
                                    let db = self.db.clone();
                                    tokio::spawn(async move {
                                        let _ = shadow::set_db_version(&db, my_db_version).await;
                                    });
                                }
                            } else {
                                log::info!(
                                    "Received {} changes from peer {peer} (their db_version: {})",
                                    changes.len(),
                                    my_db_version,
                                );

                                // Emit PeerSynced so subscribers (notably
                                // background_sync, which polls network_event_rx
                                // to decide whether the FCM-triggered sync
                                // actually accomplished anything) see a
                                // success signal here. Previously this event
                                // was only emitted on the request-handling
                                // path, so a peer that *initiated* sync and
                                // received changes never observed PeerSynced
                                // for its own counterparty — and
                                // background_sync would (mis)report
                                // BackgroundSyncResult::NoPeers despite a
                                // successful 40-change pull.
                                self.emit_network_event(
                                    crate::network_status::NetworkEvent::PeerSynced {
                                        peer_id: crate::network_status::PeerId(peer.to_string()),
                                        db_version: my_db_version,
                                    },
                                );
                                self.update_network_status();

                                // Persist Lamport bump and peer version in a spawned task,
                                // but queue changeset for sequential application in main loop.
                                let db = self.db.clone();
                                let peer_str = peer.to_string();
                                tokio::spawn(async move {
                                    // Persist Lamport bump BEFORE applying changes so
                                    // increment_db_version reads the adjusted base value.
                                    if lamport_bump {
                                        let _ = shadow::set_db_version(&db, my_db_version).await;
                                    }

                                    // Persist peer version
                                    let _ = peer_tracker::upsert_peer_version(
                                        &db,
                                        &peer_str,
                                        &peer_site_id,
                                        my_db_version,
                                    )
                                    .await;
                                });

                                if let Err(e) = self.remote_changeset_tx.try_send(changes) {
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
                self.pending_sync_peers.remove(&peer);
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
        // Verify HMAC if group key is configured
        if let Some(ref gk) = self.group_key {
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
            // HMAC verified — mark peer as group member
            if !self.verified_peers.contains(&peer) {
                self.verified_peers.insert(peer);
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

        // Reject requests from peers on a different topic
        if !peer_topic.is_empty() && peer_topic != self.topic_name {
            log::debug!(
                "Ignoring sync request from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                self.topic_name
            );
            // Permanently reject this peer so mDNS won't re-add it
            self.reject_peer(peer);
            return;
        }

        // NOTE: Do NOT update peer_db_versions here. The peer's
        // reported db_version tells us what THEY have, but we haven't
        // received their data yet. peer_db_versions is only updated in
        // the response handler where we actually receive and process
        // changes. Updating here would cause us to skip changes in the
        // next sync request (your_last_db_version would be too high).
        //
        // However, track in peer_reported_versions for display purposes.
        let reported = self.peer_reported_versions.entry(peer).or_insert(0);
        *reported = (*reported).max(my_db_version);

        self.emit_network_event(crate::network_status::NetworkEvent::PeerSynced {
            peer_id: crate::network_status::PeerId(peer.to_string()),
            db_version: my_db_version,
        });
        self.update_network_status();

        // Spawn async task to query changes and respond
        let db = self.db.clone();
        let registry = self.registry.clone();
        let resp_tx = self.snapshot_resp_tx.clone();
        let local_db_version = self.local_db_version;
        let local_site_id = self.site_id;
        let change_tx = self.change_tx.clone();
        let topic_name = self.topic_name.clone();
        let group_key = self.group_key.clone();

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
        // Verify HMAC if group key is configured
        if let Some(ref gk) = self.group_key {
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
            // HMAC verified — mark peer as group member
            if !self.verified_peers.contains(&peer) {
                self.verified_peers.insert(peer);
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

        // Reject pushes from peers on a different topic
        if !peer_topic.is_empty() && peer_topic != self.topic_name {
            log::debug!(
                "Ignoring push from peer {peer}: topic mismatch (theirs={peer_topic}, ours={})",
                self.topic_name
            );
            self.reject_peer(peer);
            return;
        }

        // Track peer's db_version (use max to avoid overwriting with stale values)
        let entry = self.peer_db_versions.entry(peer).or_insert(0);
        *entry = (*entry).max(changeset.db_version);
        let reported = self.peer_reported_versions.entry(peer).or_insert(0);
        *reported = (*reported).max(changeset.db_version);

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

        // Queue changeset for sequential application in the main loop
        if let Err(e) = self.remote_changeset_tx.try_send(changeset.changes) {
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
        // Verify HMAC if group key is configured
        if let Some(ref gk) = self.group_key {
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

        // Only accept from verified peers
        if !self.verified_peers.contains(&peer) {
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

    /// Permanently reject a peer: remove from all tracking sets and emit PeerRejected.
    fn reject_peer(&mut self, peer: libp2p::PeerId) {
        self.rejected_peers.insert(peer);
        self.verified_peers.remove(&peer);
        self.peer_identities.remove(&peer);
        self.pending_sync_peers.remove(&peer);
        self.peers.remove(&peer);
        self.peer_db_versions.remove(&peer);
        self.peer_reported_versions.remove(&peer);
        self.emit_network_event(crate::network_status::NetworkEvent::PeerRejected(
            crate::network_status::PeerId(peer.to_string()),
        ));
        self.update_network_status();
    }
}

/// Apply a set of remote column changes to the local database.
///
/// All shadow + base-table writes for a single changeset commit as one
/// transaction. Without this, a 30-change sync round did ~120 separate
/// auto-commits to the SQLite WAL — visible as ~3s of `DELETE`/`INSERT OR
/// REPLACE` lines in logcat. Batching into one transaction collapses that
/// to a single commit (~50–200ms wall-clock) and is the load-bearing
/// latency win for FCM-triggered cold-start sync.
///
/// `ChangeNotification`s are buffered during the transaction and emitted
/// only AFTER commit (Rule 2.12 — subscribers must never observe a
/// notification before its data is durable).
pub(super) async fn apply_remote_changeset(
    db: &DatabaseConnection,
    change_tx: &broadcast::Sender<ChangeNotification>,
    registry: &TableRegistry,
    changes: &[ColumnChange],
) {
    use sea_orm::TransactionTrait;

    let txn = match db.begin().await {
        Ok(t) => t,
        Err(e) => {
            log::error!("Failed to begin transaction for remote changeset: {e}");
            return;
        }
    };

    // Increment local db_version once for the batch of remote changes
    let local_db_version = match shadow::increment_db_version(&txn).await {
        Ok(v) => v,
        Err(e) => {
            log::error!("Failed to increment db_version: {e}");
            let _ = txn.rollback().await;
            return;
        }
    };

    // Group changes by (table, pk) for efficient processing
    let mut grouped: HashMap<(&str, &str), Vec<&ColumnChange>> = HashMap::new();
    for change in changes {
        grouped
            .entry((&change.table.0, &change.pk.0))
            .or_default()
            .push(change);
    }

    // Collect notifications and emit only after commit (Rule 2.12)
    let mut pending_notifications: Vec<ChangeNotification> = Vec::new();

    for ((table, pk), row_changes) in &grouped {
        let meta = match registry.get(table) {
            Some(m) => m,
            None => {
                log::warn!("Rejecting remote changes for unregistered table: {}", table);
                continue;
            }
        };

        let mut any_applied = false;
        let mut changed_columns = Vec::new();
        let mut is_delete = false;

        // Check for delete first
        let delete_change = row_changes.iter().find(|c| c.cid.0 == "__deleted");
        if let Some(change) = delete_change {
            if apply_remote_delete(&txn, table, pk, change, &meta, local_db_version).await {
                any_applied = true;
                is_delete = true;
            }
        } else {
            let (applied, cols) =
                apply_remote_column_changes(&txn, table, pk, row_changes, &meta, local_db_version)
                    .await;
            if applied {
                any_applied = true;
                changed_columns = cols;
            }
        }

        if any_applied {
            let kind = if is_delete {
                WriteKind::Delete
            } else {
                WriteKind::Insert
            };
            pending_notifications.push(ChangeNotification {
                table: (*table).into(),
                kind,
                primary_key: (*pk).into(),
                changed_columns: if changed_columns.is_empty() {
                    None
                } else {
                    Some(changed_columns)
                },
            });
        }
    }

    if let Err(e) = txn.commit().await {
        log::error!("Failed to commit remote changeset transaction: {e}");
        // Notifications are not sent — data was rolled back.
        return;
    }

    for n in pending_notifications {
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
/// update shadow tables. Returns `(applied, changed_column_names)`.
async fn apply_remote_column_changes(
    db: &impl ConnectionTrait,
    table: &str,
    pk: &str,
    row_changes: &[&ColumnChange],
    meta: &crate::registry::TableMeta,
    local_db_version: u64,
) -> (bool, Vec<String>) {
    let exists = row_exists(db, table, &meta.primary_key_column, pk).await;
    let mut winning_columns: Vec<(String, sea_orm::Value)> = Vec::new();
    let mut pending_shadow_updates: Vec<(String, u64, crate::messages::NodeId, u32)> = Vec::new();
    let mut changed_columns = Vec::new();

    for change in row_changes {
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
            changed_columns.push(change.cid.0.clone());
            pending_shadow_updates.push((
                change.cid.0.clone(),
                change.col_version,
                remote_site,
                change.seq,
            ));
        }
    }

    if winning_columns.is_empty() {
        return (false, changed_columns);
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
        return (true, changed_columns);
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
        (true, changed_columns)
    } else {
        log::debug!(
            "Row {}/{} not created (likely missing NOT NULL columns from \
             out-of-order delivery), deferring shadow updates",
            table,
            pk
        );
        (false, changed_columns)
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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;
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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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
        apply_remote_changeset(&db, &tx, &registry, &delete_changes).await;
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
        apply_remote_changeset(&db, &tx, &registry, &insert_changes).await;

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

        apply_remote_changeset(&db, &tx, &registry, &changes).await;

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
        apply_remote_changeset(&db_a, &tx_a, &registry_a, &b_changes).await;

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
        apply_remote_changeset(&db_b, &tx_b, &registry_b, &a_changes).await;

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
        apply_remote_changeset(&db_a, &tx_a, &registry_a, &b_changes).await;

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
        apply_remote_changeset(&db_b, &tx_b, &registry_b, &a_changes).await;

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
        apply_remote_changeset(&db, &tx, &registry, &update_changes).await;

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
        apply_remote_changeset(&db, &tx, &registry, &insert_changes).await;

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
