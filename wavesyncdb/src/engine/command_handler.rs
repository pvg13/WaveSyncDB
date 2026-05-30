//! Engine command dispatch and app resume handling.

use super::*;

impl EngineRunner {
    /// Dispatch an engine command received from the application.
    /// Returns `true` if the engine should shut down.
    pub(super) async fn handle_command(&mut self, cmd: EngineCommand) -> bool {
        match cmd {
            EngineCommand::Resume => {
                while let Ok(EngineCommand::Resume) = self.cmd_rx.try_recv() {}
                self.handle_resume().await;
                false
            }
            EngineCommand::NetworkTransition => {
                // Drain duplicate NetworkTransition commands
                while let Ok(EngineCommand::NetworkTransition) = self.cmd_rx.try_recv() {}
                log::info!("Network transition detected — force-disconnecting all peers");
                self.handle_resume().await;
                false
            }
            EngineCommand::RequestFullSync => {
                log::info!("Full sync requested by user");
                // Reset peer versions to trigger full re-sync (every group)
                for g in self.groups.values_mut() {
                    g.peer_db_versions.clear();
                    g.peer_reported_versions.clear();
                    g.pending_sync_peers.clear();
                }
                self.dialing_peers.clear();
                self.pending_rendezvous_dials.clear();
                self.trigger_rediscovery();
                if self.config.relay_server.is_some() {
                    self.maybe_reconnect_relay();
                }
                if self.config.rendezvous_server.is_some() {
                    self.rendezvous_discover();
                }
                self.sync_all_known_peers().await;
                self.resume_sync_deadline =
                    Some(tokio::time::Instant::now() + Duration::from_secs(2));
                false
            }
            EngineCommand::RegisterPushToken { platform, token } => {
                log::info!("Registering push token (platform: {platform})");
                self.push_token = Some((platform, token));
                self.push_registered = false;
                // If relay is already connected, register immediately
                if let RelayState::Connected { relay_peer_id, .. }
                | RelayState::Listening { relay_peer_id } = self.relay_state
                {
                    self.maybe_register_push_token(relay_peer_id);
                }
                false
            }
            EngineCommand::SetPeerIdentity(app_id) => {
                self.local_app_id = app_id.clone();
                if let Some(ref id) = app_id {
                    self.announce_identity_to_verified_peers(id);
                }
                false
            }
            EngineCommand::SetMdnsEnabled(enabled) => {
                self.set_mdns_enabled(enabled);
                false
            }
            EngineCommand::Shutdown => {
                log::info!("Engine shutdown requested");
                true
            }
            EngineCommand::JoinGroup(init) => {
                self.handle_join_group(*init);
                false
            }
            EngineCommand::LeaveGroup { effective_topic } => {
                self.handle_leave_group(effective_topic);
                false
            }
        }
    }

    /// Stand up a new group's [`GroupState`] from the application-supplied
    /// [`GroupInit`], then wire it into discovery: register the rendezvous
    /// namespace, re-announce presence to the relay, and sweep already-connected
    /// peers with a version-vector request for the new topic. Idempotent — a
    /// JoinGroup for an effective topic we already serve is a no-op.
    fn handle_join_group(&mut self, init: GroupInit) {
        let effective_topic = init.effective_topic.clone();
        if self.groups.contains_key(&effective_topic) {
            log::debug!("JoinGroup for already-joined topic {effective_topic}; ignoring");
            return;
        }

        let GroupInit {
            db,
            user_topic,
            effective_topic: topic_name,
            group_key,
            site_id,
            local_db_version,
            db_version_cache,
            registry,
            registry_ready,
            change_tx,
            notification_tx,
            notification_registry,
            peer_db_versions,
        } = init;

        let rendezvous_namespace = topic_name.clone();
        let group = GroupState {
            db,
            change_tx,
            registry,
            notification_tx,
            notification_registry,
            site_id,
            user_topic,
            topic_name: topic_name.clone(),
            local_db_version,
            db_version_cache,
            peer_db_versions,
            peer_reported_versions: HashMap::new(),
            registry_ready,
            registry_is_ready: false,
            group_key,
            rendezvous_namespace,
            rendezvous_cookie: None,
            rendezvous_registered: false,
            rejected_peers: std::collections::HashSet::new(),
            verified_peers: std::collections::HashSet::new(),
            pending_sync_peers: std::collections::HashSet::new(),
        };
        self.groups.insert(effective_topic.clone(), group);
        log::info!("Joined sync group (effective topic {effective_topic})");

        // Register the new namespace + re-announce presence if the
        // rendezvous / relay infrastructure is already connected. Both helpers
        // iterate every group, so the freshly-inserted one is picked up.
        if let Some(ref rv_addr) = self.config.rendezvous_server
            && let Some(libp2p::multiaddr::Protocol::P2p(rv_peer_id)) = rv_addr.iter().last()
            && self.swarm.is_connected(&rv_peer_id)
            && self.swarm.external_addresses().count() > 0
        {
            self.rendezvous_register(rv_peer_id);
        }
        if let RelayState::Connected { relay_peer_id, .. }
        | RelayState::Listening { relay_peer_id } = self.relay_state
        {
            self.announce_presence_to_relay(relay_peer_id);
        }

        self.update_network_status();
    }

    /// Remove a group's [`GroupState`] so it stops syncing. The rendezvous
    /// namespace registration simply TTL-expires on the server; we do not
    /// touch the DB file (the connection side owns its handle). A LeaveGroup
    /// for the default group, or for a topic we don't serve, is ignored.
    fn handle_leave_group(&mut self, effective_topic: String) {
        if effective_topic == self.default_effective_topic {
            log::warn!("Refusing to leave the default group {effective_topic}");
            return;
        }
        if self.groups.remove(&effective_topic).is_some() {
            log::info!("Left sync group (effective topic {effective_topic})");
            self.update_network_status();
        } else {
            log::debug!("LeaveGroup for unknown topic {effective_topic}; ignoring");
        }
    }

    pub(super) async fn handle_resume(&mut self) {
        log::info!("App resumed — triggering rediscovery and sync");

        // Clear version maps so the next sync requests all changes since the
        // last *persisted* peer version, not stale in-memory values that may
        // have drifted during a network transition.
        for g in self.groups.values_mut() {
            g.peer_db_versions.clear();
            g.peer_reported_versions.clear();
            g.pending_sync_peers.clear();
        }
        self.dialing_peers.clear();
        self.pending_rendezvous_dials.clear();

        // Force-disconnect relay — the TCP socket is likely dead on the old
        // network interface. ConnectionClosed handler will reset relay_state
        // and trigger reconnect.
        if let RelayState::Connected { relay_peer_id, .. }
        | RelayState::Listening { relay_peer_id } = self.relay_state
        {
            log::info!("Resume: disconnecting relay {relay_peer_id} for clean reconnection");
            let _ = self.swarm.disconnect_peer_id(relay_peer_id);
        }

        // Force-disconnect all non-infrastructure peers (likely dead sockets)
        let stale: Vec<_> = self.peers.keys().cloned().collect();
        for pid in stale {
            let _ = self.swarm.disconnect_peer_id(pid);
        }

        // 1. Trigger mDNS rediscovery (LAN)
        self.trigger_rediscovery();

        // 2. WAN: reconnect relay if disconnected
        if self.config.relay_server.is_some() {
            self.maybe_reconnect_relay();
        }

        // 3. WAN: trigger rendezvous rediscovery
        if self.config.rendezvous_server.is_some() {
            self.rendezvous_discover();
        }

        // 4. Sync with any peers still connected (may be none — that's OK)
        self.sync_all_known_peers().await;

        // 5. Schedule a delayed retry to catch peers rediscovered via mDNS/rendezvous
        self.resume_sync_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
    }
}
