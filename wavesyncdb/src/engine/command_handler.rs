//! Engine command dispatch and app resume handling.

use super::*;

impl EngineRunner {
    /// Dispatch an engine command received from the application.
    /// Returns `true` if the engine should shut down.
    pub(super) async fn handle_command(&mut self, cmd: EngineCommand) -> bool {
        match cmd {
            EngineCommand::Resume => {
                while let Ok(EngineCommand::Resume) = self.cmd_rx.try_recv() {}
                // Same suspension-gap gate as PushWake (#111): Doze / deep
                // sleep freezes the sockets WITHOUT an interface change, so
                // an unconditional keep-connections resume trusts dead
                // sockets and waits out the ~20s reactive sync-timeout
                // eviction (measured 22s Doze-recovery TTFS). A genuine
                // quick resume observes no wall-clock gap and keeps the
                // anti-churn path — the reservation-churn protection this
                // arm exists for.
                let force = self.wake_wants_relay_reset();
                if force {
                    tracing::info!(
                        "Resume after suspension-length gap — forcing relay reset (dead frozen sockets)"
                    );
                }
                self.handle_resume(force).await;
                false
            }
            EngineCommand::NetworkTransition => {
                // Drain duplicate NetworkTransition commands
                while let Ok(EngineCommand::NetworkTransition) = self.cmd_rx.try_recv() {}
                tracing::info!("Network transition detected — force-disconnecting all peers");
                // Network changed: old sockets are dead, force a clean reconnect.
                self.handle_resume(true).await;
                false
            }
            EngineCommand::PushWake => {
                while let Ok(EngineCommand::PushWake) = self.cmd_rx.try_recv() {}
                // Force a relay reset only when the event loop observed a
                // suspension-length gap — frozen-process sockets are dead but
                // QUIC won't notice for ~30s, and the reactive
                // sync_timeout_strikes eviction takes ~20s the push window
                // doesn't have. A wake without a detected suspension behaves
                // exactly like a plain Resume, preserving the anti-churn
                // guarantee for healthy relay reservations.
                let force = self.wake_wants_relay_reset();
                tracing::info!(
                    force_relay_reset = force,
                    "Push wake — rediscovery and sync"
                );
                self.handle_resume(force).await;
                false
            }
            EngineCommand::RequestFullSync => {
                tracing::info!("Full sync requested by user");
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
                tracing::info!("Registering push token (platform: {platform})");
                self.push_token = Some((platform, token));
                // Token rotated — every topic must be re-registered with the
                // new token, so clear the per-topic registration set and any
                // in-flight registrations carrying the old token.
                self.push_registered_topics.clear();
                self.push_pending_registrations.clear();
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
                tracing::info!("Engine shutdown requested");
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
            EngineCommand::GroupRegistryReady { effective_topic } => {
                if let Some(g) = self.groups.get_mut(&effective_topic)
                    && !g.registry_is_ready
                {
                    g.registry_is_ready = true;
                    tracing::info!(
                        "Group {effective_topic} schema registered — now eligible for \
                         connect-time sync initiation"
                    );
                    self.update_network_status();
                    // Catch up on mailbox entries appended while no engine
                    // served this group. Drains are gated on registry
                    // readiness (an earlier drain would consume entries the
                    // apply path rejects as "unregistered table" — #104), so
                    // this is the group's first drain opportunity. No-op when
                    // the relay isn't connected yet; the reservation-accepted
                    // handler covers that ordering.
                    self.start_mailbox_drain(&effective_topic);
                    // No eager peer sweep here on purpose: the periodic tick
                    // already syncs every group with known peers, and adding a
                    // burst of version-vector requests across all connected peers
                    // the instant a group goes ready amplifies traffic (and, with
                    // a busy mDNS segment, can starve the request-response
                    // substreams). Connect/discovery events drive the (bounded)
                    // per-peer initiation from here on.
                }
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
            tracing::debug!("JoinGroup for already-joined topic {effective_topic}; ignoring");
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
            mailbox_meta,
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
            rendezvous_registered: false,
            rejected_peers: std::collections::HashMap::new(),
            verified_peers: std::collections::HashSet::new(),
            pending_sync_peers: std::collections::HashMap::new(),
            pending_pushes: std::collections::BTreeMap::new(),
            mailbox_cursor: mailbox_meta.cursor,
            mailbox_epoch: mailbox_meta.epoch,
            mailbox_acked_version: mailbox_meta.acked_version,
            mailbox_unacked: std::collections::BTreeSet::new(),
            mailbox_startup_healed: false,
            mailbox_drain_in_flight: false,
            mailbox_drain_applied: 0,
            mailbox_heal: None,
        };
        self.groups.insert(effective_topic.clone(), group);
        tracing::info!("Joined sync group (effective topic {effective_topic})");

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
            // Register the device push token for the new group's topic so the
            // relay can wake this device for writes to it. The one-shot
            // relay-connect registration only covered groups present at that
            // moment; a group joined later (e.g. a household joined after
            // login) would otherwise never be registered.
            self.register_push_token_for_topic(relay_peer_id, &topic_name);
            // NOTE: no mailbox drain here. The app registers the group's
            // schema only after `join_group()` returns, and a drain racing
            // that registration consumed entries the apply path rejected as
            // "unregistered table" — permanent data loss (#104). The drain
            // fires from the GroupRegistryReady handler instead.
        }

        self.update_network_status();
    }

    /// Remove a group's [`GroupState`] so it stops syncing. The rendezvous
    /// namespace registration simply TTL-expires on the server; we do not
    /// touch the DB file (the connection side owns its handle). A LeaveGroup
    /// for the default group, or for a topic we don't serve, is ignored.
    fn handle_leave_group(&mut self, effective_topic: String) {
        if effective_topic == self.default_effective_topic {
            tracing::warn!("Refusing to leave the default group {effective_topic}");
            return;
        }
        if let Some(group) = self.groups.remove(&effective_topic) {
            tracing::info!("Left sync group (effective topic {effective_topic})");
            // Stop the relay from waking this device for the left group's
            // topic. Skip if another remaining group shares the same topic name.
            if let RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } = self.relay_state
                && !self
                    .groups
                    .values()
                    .any(|g| g.topic_name == group.topic_name)
            {
                self.unregister_push_token_for_topic(relay_peer_id, &group.topic_name);
            }
            self.update_network_status();
        } else {
            tracing::debug!("LeaveGroup for unknown topic {effective_topic}; ignoring");
        }
    }

    /// Re-establish connectivity after an app resume or a network change.
    ///
    /// `force_relay_reset` controls how aggressive the teardown is:
    /// - `false` (plain app resume on the same network): keep the existing relay
    ///   reservation and peer circuits — they're still valid. Only rediscover
    ///   and re-sync.
    /// - `true` (network transition): the old sockets are bound to a departed
    ///   interface and are dead, so force-disconnect the relay and all peers to
    ///   re-establish on the new interface.
    pub(super) async fn handle_resume(&mut self, force_relay_reset: bool) {
        tracing::info!("App resumed — triggering rediscovery and sync");

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
        // Resume is an explicit "sync now" signal, so drop any per-peer dial
        // backoff accumulated before we were backgrounded: a peer that was
        // unreachable on the old network (or while suspended) must be re-dialable
        // immediately now, not throttled by stale failure history. The backoff
        // re-arms from scratch if dials keep failing on the current network.
        self.peer_dial_backoff.clear();

        // On an actual network transition the previous LAN is gone — drop the
        // LAN-preference markers so peers are re-classified from fresh mDNS on
        // the new network (a peer that was same-LAN before may now be remote, and
        // vice-versa). On a plain resume (same network) the markers stay valid.
        if force_relay_reset {
            self.lan_peers.clear();
        }

        // Only tear down live connections on an actual network transition. On a
        // plain app resume (same network) the relay reservation and peer
        // circuits are still valid — force-reconnecting them here caused a
        // reservation-churn storm: every resume dropped the relay, the reconnect
        // asked for a fresh reservation, and with the relay's long
        // `reservation_duration` the stale ones lingered until the per-peer cap
        // was hit and every new circuit was denied with `ResourceLimitExceeded`,
        // silently breaking sync for the whole group. A genuinely dead relay
        // connection is still caught by the ping / idle timeout and reconnected.
        if force_relay_reset {
            // The old sockets are bound to a now-departed interface and are
            // dead. ConnectionClosed handler resets relay_state and reconnects.
            if let RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } = self.relay_state
            {
                tracing::info!(
                    "Network transition: disconnecting relay {relay_peer_id} for clean reconnection"
                );
                let _ = self.swarm.disconnect_peer_id(relay_peer_id);
            }

            // Force-disconnect all non-infrastructure peers (dead sockets).
            let stale: Vec<_> = self.peers.keys().cloned().collect();
            for pid in stale {
                let _ = self.swarm.disconnect_peer_id(pid);
            }
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

        // 5. Drain the relay mailbox — the path that delivers changes even
        // when NO peer is reachable (both-offline scenario: the writer's
        // changes sit durably at the relay). If the relay connection is
        // still being (re)established this no-ops; the reservation-accepted
        // handler fires the drain once we're reachable.
        self.start_mailbox_drains_all();
        self.mailbox_maintenance_tick().await;

        // 6. Schedule a delayed retry to catch peers rediscovered via mDNS/rendezvous
        self.resume_sync_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
        // Budget for the retry arm's bounded re-arms while the relay is
        // still down (#111): post-suspension network wake can outlast the
        // first retry.
        self.resume_retries_left = 3;
    }
}
