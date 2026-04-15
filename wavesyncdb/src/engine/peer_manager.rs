//! Peer discovery, sync initiation, and mDNS handling.

use super::*;

impl EngineRunner {
    pub(super) async fn sync_all_known_peers(&mut self) {
        // Refresh local_db_version from DB before syncing, in case spawned tasks
        // (apply_remote_changeset) have incremented it since we last checked.
        self.local_db_version = shadow::get_db_version(&self.db)
            .await
            .unwrap_or(self.local_db_version);

        let peer_ids: Vec<libp2p::PeerId> = self
            .peers
            .keys()
            .filter(|p| !self.infrastructure_peers.contains(p))
            .cloned()
            .collect();
        for peer_id in peer_ids {
            self.initiate_sync_for_peer(peer_id);
        }
    }

    pub(super) fn initiate_sync_for_peer(&mut self, peer_id: libp2p::PeerId) {
        if self.pending_sync_peers.contains(&peer_id) {
            return;
        }
        if self.rejected_peers.contains(&peer_id) {
            return;
        }
        if self.infrastructure_peers.contains(&peer_id) {
            return;
        }

        let their_last_db_version = self.peer_db_versions.get(&peer_id).copied().unwrap_or(0);

        log::info!(
            "Requesting version vector sync from peer {peer_id} (their last known version: {their_last_db_version})"
        );

        let mut req = SyncRequest::VersionVector {
            my_db_version: self.local_db_version,
            your_last_db_version: their_last_db_version,
            site_id: self.site_id,
            topic: self.topic_name.clone(),
            hmac: None,
        };

        if let Some(ref gk) = self.group_key {
            // Serialize with hmac: None, compute MAC, then set hmac
            if let Ok(bytes) = serde_json::to_vec(&req) {
                let tag = gk.mac(&bytes);
                if let SyncRequest::VersionVector { ref mut hmac, .. } = req {
                    *hmac = Some(tag);
                }
            }
        }

        let _req_id = self
            .swarm
            .behaviour_mut()
            .snapshot
            .send_request(&peer_id, req);
        self.pending_sync_peers.insert(peer_id);
    }

    pub(super) fn handle_mdns(&mut self, event: mdns::Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, multiaddr) in list {
                    // Never re-add peers rejected for topic mismatch
                    if self.rejected_peers.contains(&peer_id) {
                        continue;
                    }
                    // Already tracked and connected — just sync if ready
                    if self.peers.contains_key(&peer_id) && self.swarm.is_connected(&peer_id) {
                        if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                        continue;
                    }

                    if self.swarm.is_connected(&peer_id) {
                        // Connected but not in self.peers — track it now
                        // (can happen if connection arrived before registry was ready)
                        self.peers.insert(peer_id, multiaddr.clone());
                        self.update_network_status();
                        if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                        continue;
                    }

                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                    // Dial if not currently connected — peer will be added to self.peers
                    // by handle_connection_established once the connection succeeds.
                    if let Err(e) = self.swarm.dial(multiaddr.clone()) {
                        log::warn!("Failed to dial peer {peer_id}: {e}");
                    }

                    // Update last_seen for peer tracking DB
                    let db = self.db.clone();
                    let peer_str = peer_id.to_string();
                    tokio::spawn(async move {
                        let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
                    });
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, multiaddr) in list {
                    log::debug!("Expired peer {peer_id} at {multiaddr}");
                    self.peers.remove(&peer_id);
                    self.pending_sync_peers.remove(&peer_id);
                    self.verified_peers.remove(&peer_id);
                    self.peer_identities.remove(&peer_id);
                    self.emit_network_event(crate::network_status::NetworkEvent::PeerDisconnected(
                        crate::network_status::PeerId(peer_id.to_string()),
                    ));
                    self.update_network_status();
                }
            }
        }
    }

    pub(super) fn trigger_rediscovery(&mut self) {
        log::info!("Triggering mDNS rediscovery");
        match mdns::tokio::Behaviour::new(self.config.mdns_config(), self.local_peer_id) {
            Ok(new_mdns) => {
                self.swarm.behaviour_mut().mdns =
                    libp2p::swarm::behaviour::toggle::Toggle::from(Some(new_mdns));
            }
            Err(e) => log::warn!("mDNS unavailable during rediscovery: {e}"),
        }
    }
}
