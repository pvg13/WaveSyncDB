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
                // mDNS often produces multiple addresses for the same peer-id
                // in one event (each network interface that responded). Count
                // each *peer* once, not each address.
                let mut counted_peers = std::collections::HashSet::new();
                for (peer_id, multiaddr) in list {
                    if counted_peers.insert(peer_id) {
                        self.diagnostics
                            .mdns_discoveries
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Never re-add peers rejected for topic mismatch
                    if self.rejected_peers.contains(&peer_id) {
                        continue;
                    }
                    // Skip dial and address update if already tracked and connected —
                    // avoids duplicate dials from multi-address mDNS discovery
                    // (TCP+QUIC × multiple IPs), but still allow sync initiation below
                    if self.peers.contains_key(&peer_id) && self.swarm.is_connected(&peer_id) {
                        if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                        continue;
                    }
                    log::info!("Discovered peer {peer_id} at {multiaddr}");
                    // Dial if not currently connected (handles both new peers and reconnections
                    // after network disruption where the peer is still in self.peers but the
                    // TCP/QUIC connection is dead)
                    if !self.swarm.is_connected(&peer_id) {
                        match self.swarm.dial(multiaddr.clone()) {
                            Ok(()) => {
                                self.diagnostics
                                    .peer_dial_attempts
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(e) => {
                                log::warn!("Failed to dial peer {peer_id}: {e}");
                            }
                        }
                    }
                    self.peers.insert(peer_id, multiaddr.clone());

                    self.emit_network_event(crate::network_status::NetworkEvent::PeerConnected(
                        crate::network_status::PeerInfo {
                            peer_id: crate::network_status::PeerId(peer_id.to_string()),
                            address: multiaddr.to_string(),
                            db_version: self.peer_db_versions.get(&peer_id).copied(),
                            is_bootstrap: self.bootstrap_peers.contains(&peer_id),
                            is_group_member: false,
                            app_id: None,
                        },
                    ));
                    self.update_network_status();

                    // Register peer and update last_seen
                    let db = self.db.clone();
                    let peer_str = peer_id.to_string();
                    tokio::spawn(async move {
                        let _ = peer_tracker::update_last_seen(&db, &peer_str).await;
                    });

                    if self.registry_is_ready && self.swarm.is_connected(&peer_id) {
                        self.initiate_sync_for_peer(peer_id);
                    }
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

    /// Pre-dial peers from the local address cache (#29). Called once at
    /// engine startup, after `listen_on` and bootstrap-peer dials, before
    /// the main event loop begins.
    ///
    /// **Why this matters.** Without the cache, cold-start sync waits for
    /// at least one of mDNS (~1s on LAN), rendezvous discovery (round-trip
    /// to the rendezvous server), or the relay's `AnnouncePresence` →
    /// `PeerList` round-trip — typically 1–10s on cellular. With the
    /// cache, the dial is in flight before the swarm even processes its
    /// first event. libp2p races the cached path against discovery and
    /// keeps whichever connection completes first; failures bump the
    /// per-row `fail_count` (recorded in `OutgoingConnectionError`) and
    /// drop out of the next start's load via `MAX_FAIL_COUNT`.
    ///
    /// Errors are intentionally swallowed at debug level: this is a
    /// best-effort optimisation, not part of the connectivity contract.
    pub(super) async fn predial_cached_addrs(&mut self) {
        // Tunables. Max age is 7 days (after a week of not seeing a
        // peer, its address is most likely stale anyway); fail cap is
        // 10 (matches a rough "circuit-breaker open" threshold used by
        // gRPC / Envoy for outlier detection).
        const MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;
        const MAX_FAIL_COUNT: u32 = 10;

        // Opportunistic GC. Cheap and only runs once per engine lifetime,
        // so we don't bother throttling it.
        if let Err(e) = crate::peer_addrs::gc(&self.db, MAX_AGE_SECS, MAX_FAIL_COUNT).await {
            log::debug!("peer_addrs::gc failed: {e}");
        }

        let cached =
            match crate::peer_addrs::load_recent(&self.db, MAX_AGE_SECS, MAX_FAIL_COUNT).await {
                Ok(v) => v,
                Err(e) => {
                    log::debug!("peer_addrs::load_recent failed: {e}");
                    return;
                }
            };

        if cached.is_empty() {
            log::debug!("peer_addrs cache empty — no cold-start pre-dials");
            return;
        }

        log::info!(
            "Pre-dialing {} cached peer address(es) from previous session",
            cached.len()
        );
        for entry in cached {
            // Skip the relay (its dial goes through `try_dial_relay` and
            // we don't want a second uncoordinated path competing with
            // the reservation state machine).
            if let Some(ref relay_addr) = self.config.relay_server
                && let Some(libp2p::multiaddr::Protocol::P2p(relay_pid)) = relay_addr.iter().last()
                && entry.peer_id == relay_pid.to_string()
            {
                continue;
            }

            let addr: libp2p::Multiaddr = match entry.multiaddr.parse() {
                Ok(a) => a,
                Err(e) => {
                    log::debug!(
                        "skipping un-parseable cached multiaddr {}: {e}",
                        entry.multiaddr
                    );
                    continue;
                }
            };
            match self.swarm.dial(addr.clone()) {
                Ok(()) => {
                    self.diagnostics
                        .cached_addr_dials
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.diagnostics
                        .peer_dial_attempts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(e) => log::debug!("cached pre-dial of {addr} failed: {e}"),
            }
        }
    }

    pub(super) fn trigger_rediscovery(&mut self) {
        if !self.mdns_enabled {
            log::debug!("mDNS rediscovery skipped: mDNS disabled at runtime");
            return;
        }
        log::info!("Triggering mDNS rediscovery");
        match mdns::tokio::Behaviour::new(self.config.mdns_config(), self.local_peer_id) {
            Ok(new_mdns) => {
                self.swarm.behaviour_mut().mdns =
                    libp2p::swarm::behaviour::toggle::Toggle::from(Some(new_mdns));
            }
            Err(e) => log::warn!("mDNS unavailable during rediscovery: {e}"),
        }
    }

    /// Toggle mDNS at runtime. Idempotent: a no-op when the requested
    /// state already matches `self.mdns_enabled`.
    ///
    /// When enabling, builds a fresh mDNS behaviour with the configured
    /// query interval / TTL and swaps it into the swarm — the next query
    /// goes out on the next mDNS tick.
    ///
    /// When disabling, replaces the mDNS slot with `Toggle::from(None)`.
    /// Existing mDNS-discovered peer connections are intentionally NOT
    /// torn down: they're already authenticated swarm peers and the
    /// rejection on disable would surprise apps that toggle mid-session
    /// to stop *future* announcements while keeping existing sync going.
    pub(super) fn set_mdns_enabled(&mut self, enabled: bool) {
        if self.mdns_enabled == enabled {
            return;
        }
        self.mdns_enabled = enabled;
        if enabled {
            log::info!("mDNS enabled at runtime — starting LAN announcements");
            match mdns::tokio::Behaviour::new(self.config.mdns_config(), self.local_peer_id) {
                Ok(new_mdns) => {
                    self.swarm.behaviour_mut().mdns =
                        libp2p::swarm::behaviour::toggle::Toggle::from(Some(new_mdns));
                }
                Err(e) => log::warn!("mDNS unavailable on enable: {e}"),
            }
        } else {
            log::info!("mDNS disabled at runtime — stopping LAN announcements");
            self.swarm.behaviour_mut().mdns = libp2p::swarm::behaviour::toggle::Toggle::from(None);
        }
    }
}
