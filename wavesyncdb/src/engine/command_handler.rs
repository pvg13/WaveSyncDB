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
                // Reset peer versions to trigger full re-sync
                self.peer_db_versions.clear();
                self.peer_reported_versions.clear();
                self.pending_sync_peers.clear();
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
            EngineCommand::Shutdown => {
                log::info!("Engine shutdown requested");
                true
            }
        }
    }

    pub(super) async fn handle_resume(&mut self) {
        log::info!("App resumed — triggering rediscovery and sync");

        // Clear version maps so the next sync requests all changes since the
        // last *persisted* peer version, not stale in-memory values that may
        // have drifted during a network transition.
        self.peer_db_versions.clear();
        self.peer_reported_versions.clear();
        self.pending_sync_peers.clear();

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
