//! Relay, NAT traversal, rendezvous, and push notification handling.

use super::*;

/// Maximum number of rendezvous-discovered peers dialed concurrently.
/// Remaining peers are queued and drained as connections complete or fail.
const MAX_CONCURRENT_RENDEZVOUS_DIALS: usize = 5;

impl EngineRunner {
    pub(super) fn handle_relay_client(&mut self, event: relay::client::Event) {
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } => {
                log::info!("Relay reservation accepted by {relay_peer_id}");
                self.relay_state = RelayState::Listening { relay_peer_id };
                self.circuit_retry_count = 0;
                self.circuit_accepted_at = Some(tokio::time::Instant::now());
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Listening,
                ));
                self.update_network_status();
            }
            _ => {
                log::info!("Relay client event (non-acceptance): {:?}", event);
            }
        }
    }

    pub(super) fn handle_dcutr(&mut self, event: dcutr::Event) {
        let peer = event.remote_peer_id;
        match event.result {
            Ok(_) => {
                log::info!(
                    "DCUtR: direct connection upgrade succeeded with {peer} (sync now goes peer-to-peer, bypassing the relay)"
                );
            }
            Err(error) => {
                log::info!(
                    "DCUtR: direct connection upgrade failed with {peer}: {error} (sync stays on circuit-relay path; expected on symmetric-NAT cellular)"
                );
            }
        }
    }

    pub(super) fn handle_autonat(&mut self, event: autonat::v2::client::Event) {
        // AutoNAT completed — cancel the assumption timer (real result takes precedence)
        self.nat_assumption_deadline = None;
        match &event.result {
            Ok(()) => {
                log::info!(
                    "AutoNAT: address {} is reachable (tested by {})",
                    event.tested_addr,
                    event.server
                );
                let changed = self.nat_status != NatStatus::Public;
                self.nat_status = NatStatus::Public;
                if changed {
                    self.emit_network_event(crate::network_status::NetworkEvent::NatStatusChanged(
                        NatStatus::Public,
                    ));
                    self.update_network_status();
                }
            }
            Err(e) => {
                log::info!(
                    "AutoNAT: address {} is NOT reachable (tested by {}): {e}",
                    event.tested_addr,
                    event.server
                );
                let changed = self.nat_status != NatStatus::Private;
                self.nat_status = NatStatus::Private;
                if changed {
                    self.emit_network_event(crate::network_status::NetworkEvent::NatStatusChanged(
                        NatStatus::Private,
                    ));
                    self.update_network_status();
                }
                // Relay circuit is now requested eagerly on ConnectionEstablished,
                // so we no longer trigger it from AutoNAT. NAT status is still tracked
                // above for NetworkStatus reporting.
            }
        }
    }

    pub(super) fn handle_rendezvous(&mut self, event: rendezvous::client::Event) {
        match event {
            rendezvous::client::Event::Registered {
                rendezvous_node,
                ttl,
                namespace,
            } => {
                log::info!(
                    "Registered at rendezvous server {rendezvous_node} with namespace '{namespace}' (TTL: {ttl}s)"
                );
                self.rendezvous_registered = true;
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RendezvousStatusChanged {
                        registered: true,
                    },
                );
                self.update_network_status();
            }
            rendezvous::client::Event::RegisterFailed {
                rendezvous_node,
                namespace,
                error,
            } => {
                log::warn!(
                    "Rendezvous registration failed at {rendezvous_node} namespace '{namespace}': {error:?}"
                );
                self.rendezvous_registered = false;
                self.emit_network_event(
                    crate::network_status::NetworkEvent::RendezvousStatusChanged {
                        registered: false,
                    },
                );
                self.update_network_status();
            }
            rendezvous::client::Event::Discovered {
                rendezvous_node,
                registrations,
                cookie,
            } => {
                log::info!(
                    "Discovered {} peers via rendezvous at {rendezvous_node}",
                    registrations.len()
                );
                self.rendezvous_cookie = Some(cookie);

                // Collect one address per peer, skip ineligible
                let mut to_dial: Vec<(libp2p::PeerId, libp2p::Multiaddr)> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for registration in registrations {
                    let peer_id = registration.record.peer_id();
                    if peer_id == self.local_peer_id
                        || self.dialing_peers.contains(&peer_id)
                        || self.rejected_peers.contains(&peer_id)
                        || !seen.insert(peer_id)
                    {
                        continue;
                    }
                    if self.swarm.is_connected(&peer_id) {
                        if self.registry_is_ready {
                            self.initiate_sync_for_peer(peer_id);
                        }
                        continue;
                    }
                    // Take first address for this peer
                    if let Some(addr) = registration.record.addresses().first() {
                        to_dial.push((peer_id, addr.clone()));
                    }
                }

                // Dial up to MAX immediately, queue the rest
                let immediate = to_dial.len().min(MAX_CONCURRENT_RENDEZVOUS_DIALS);
                for (peer_id, addr) in to_dial.drain(..immediate) {
                    log::info!("Rendezvous dialing peer {peer_id} at {addr}");
                    if let Err(e) = self.swarm.dial(addr) {
                        log::warn!("Failed to dial rendezvous peer {peer_id}: {e}");
                    } else {
                        self.dialing_peers.insert(peer_id);
                    }
                }
                if !to_dial.is_empty() {
                    log::info!(
                        "Queued {} rendezvous peers for rate-limited dialing",
                        to_dial.len()
                    );
                    self.pending_rendezvous_dials.extend(to_dial);
                }
            }
            rendezvous::client::Event::DiscoverFailed {
                rendezvous_node,
                namespace,
                error,
            } => {
                log::warn!(
                    "Rendezvous discovery failed at {rendezvous_node} namespace {namespace:?}: {error:?}"
                );
            }
            rendezvous::client::Event::Expired { peer } => {
                log::debug!("Rendezvous registration expired for peer {peer}");
            }
        }
    }

    /// Register with the rendezvous server.
    pub(super) fn rendezvous_register(&mut self, server_peer_id: libp2p::PeerId) {
        let namespace = match rendezvous::Namespace::new(self.rendezvous_namespace.clone()) {
            Ok(ns) => ns,
            Err(e) => {
                log::error!("Invalid rendezvous namespace: {e:?}");
                return;
            }
        };

        match self.swarm.behaviour_mut().rendezvous.register(
            namespace,
            server_peer_id,
            None, // Let server assign default TTL (server MIN_TTL=7200s)
        ) {
            Ok(()) => {
                log::info!("Sent rendezvous registration to {server_peer_id}");
            }
            Err(e) => {
                log::warn!("Failed to send rendezvous registration: {e}");
            }
        }
    }

    /// Discover peers from the rendezvous server.
    pub(super) fn rendezvous_discover(&mut self) {
        let server_peer_id = match &self.config.rendezvous_server {
            Some(addr) => match addr.iter().last() {
                Some(libp2p::multiaddr::Protocol::P2p(peer_id)) => peer_id,
                _ => {
                    log::debug!("Rendezvous server address has no peer ID, skipping discover");
                    return;
                }
            },
            None => return,
        };

        if !self.swarm.is_connected(&server_peer_id) {
            log::debug!("Not connected to rendezvous server, skipping discover");
            return;
        }

        let namespace = match rendezvous::Namespace::new(self.rendezvous_namespace.clone()) {
            Ok(ns) => ns,
            Err(e) => {
                log::error!("Invalid rendezvous namespace: {e:?}");
                return;
            }
        };

        // Re-register if we have external addresses (avoids NoExternalAddresses error).
        // Handles TTL expiry and stale state after silent disconnects.
        if self.swarm.external_addresses().count() > 0 {
            self.rendezvous_register(server_peer_id);
        }

        self.swarm.behaviour_mut().rendezvous.discover(
            Some(namespace),
            self.rendezvous_cookie.clone(),
            None,
            server_peer_id,
        );
    }

    /// Attempt to reconnect to the relay server if disconnected.
    pub(super) fn maybe_reconnect_relay(&mut self) {
        match &self.relay_state {
            RelayState::Connecting { retry_count } => {
                if let Some(ref relay_addr) = self.config.relay_server {
                    let count = *retry_count;
                    // Exponential backoff via tick-skipping: dial every 2^min(count,3) ticks
                    // With 5s base interval: 5s, 10s, 20s, 40s, 40s, 40s...
                    let skip = 1u32 << count.min(3); // 1, 2, 4, 8, 8, 8...
                    if count % skip == 0 {
                        log::info!("Attempting relay reconnection (attempt {})", count + 1);
                        if let Err(e) = self.swarm.dial(relay_addr.clone()) {
                            log::warn!("Failed to redial relay server: {}", e);
                        }
                    }
                    self.relay_state = RelayState::Connecting {
                        retry_count: count + 1,
                    };
                }
            }
            RelayState::Connected {
                relay_peer_id,
                connected_at,
            } => {
                // Circuit reservation hasn't completed — retry if stuck for >5s
                if connected_at.elapsed() > Duration::from_secs(5) {
                    self.circuit_retry_count += 1;
                    if self.circuit_retry_count >= 3 {
                        log::warn!(
                            "Circuit reservation failed {} times, forcing full relay reconnect",
                            self.circuit_retry_count
                        );
                        let pid = *relay_peer_id;
                        self.circuit_retry_count = 0;
                        let _ = self.swarm.disconnect_peer_id(pid);
                        // ConnectionClosed handler will reset to Connecting and trigger full reconnect
                    } else {
                        log::info!(
                            "Relay stuck in Connected for >5s, retrying circuit reservation (attempt {})",
                            self.circuit_retry_count
                        );
                        if let Some(ref relay_addr) = self.config.relay_server {
                            let circuit_addr = relay_addr
                                .clone()
                                .with(libp2p::multiaddr::Protocol::P2pCircuit);
                            if let Err(e) = self.swarm.listen_on(circuit_addr) {
                                log::warn!("Retry listen_on relay circuit failed: {e}");
                            }
                        }
                        // Reset connected_at to space out retries
                        let pid = *relay_peer_id;
                        self.relay_state = RelayState::Connected {
                            relay_peer_id: pid,
                            connected_at: tokio::time::Instant::now(),
                        };
                    }
                }
            }
            RelayState::Disabled | RelayState::Listening { .. } => {
                // No action needed
            }
        }
    }

    /// Register push token with the relay server if we have one and are connected.
    pub(super) fn maybe_register_push_token(&mut self, relay_peer_id: libp2p::PeerId) {
        if self.push_registered {
            return;
        }
        let (platform, token) = match &self.push_token {
            Some(pt) => pt.clone(),
            None => {
                log::info!(
                    "maybe_register_push_token skipped: no push_token set — \
                     either the platform isn't mobile, push-sync feature is off, \
                     or the FCM/APNs token file wasn't written by the OS service \
                     in time. Push notifications won't be delivered."
                );
                return;
            }
        };

        let push_platform = match platform.as_str() {
            "Fcm" => push_protocol::PushPlatform::Fcm,
            "Apns" => push_protocol::PushPlatform::Apns,
            other => {
                log::warn!("Unknown push platform: {other}");
                return;
            }
        };

        let req = push_protocol::PushRequest::RegisterToken {
            topic: self.topic_name.clone(),
            platform: push_platform,
            token,
        };

        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
        self.push_registered = true;
        self.update_network_status();
        log::info!("Sent push token registration to relay {relay_peer_id}");
    }

    /// Announce this peer's presence to the relay so it can introduce us
    /// to other peers on the same topic. Called on every relay connect,
    /// regardless of whether push notifications are configured — this is
    /// how two foreground peers behind NAT discover each other without
    /// running a separate rendezvous server.
    pub(super) fn announce_presence_to_relay(&mut self, relay_peer_id: libp2p::PeerId) {
        let req = push_protocol::PushRequest::AnnouncePresence {
            topic: self.topic_name.clone(),
        };
        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
        log::info!("Announced presence to relay {relay_peer_id} for topic");
    }

    /// Dial a peer introduced by the relay (via PeerList response or
    /// PeerJoined request) using *all* of the addresses the relay supplied
    /// for that peer at once. libp2p races them and connects via whichever
    /// works first — important because the relay sends both direct addresses
    /// (often unreachable, since most peers are NAT'd) and a circuit-relay
    /// fallback. Dialing them one-at-a-time previously meant the direct
    /// address was tried first and the circuit address never got a turn.
    ///
    /// Skips self, infra peers, rejected peers, and peers we're already
    /// connected to or actively dialing.
    pub(super) fn dial_introduced_peer(&mut self, addr_strs: &[String]) {
        let addrs: Vec<libp2p::Multiaddr> = addr_strs
            .iter()
            .filter_map(|s| match s.parse::<libp2p::Multiaddr>() {
                Ok(a) => Some(a),
                Err(e) => {
                    log::warn!("Relay introduced unparseable address {s:?}: {e}");
                    None
                }
            })
            .collect();

        let peer_id = addrs.iter().find_map(|a| {
            a.iter().find_map(|p| match p {
                libp2p::multiaddr::Protocol::P2p(pid) => Some(pid),
                _ => None,
            })
        });
        let Some(peer_id) = peer_id else {
            log::warn!("Relay introduced peer with no /p2p/ suffix on any address");
            return;
        };

        if peer_id == self.local_peer_id
            || self.infrastructure_peers.contains(&peer_id)
            || self.rejected_peers.contains(&peer_id)
            || self.swarm.is_connected(&peer_id)
            || self.dialing_peers.contains(&peer_id)
        {
            return;
        }

        if addrs.is_empty() {
            return;
        }

        log::info!(
            "Dialing relay-introduced peer {peer_id} with {} address(es): {addrs:?}",
            addrs.len()
        );
        let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
            .addresses(addrs.clone())
            .build();
        match self.swarm.dial(dial_opts) {
            Ok(()) => {
                self.dialing_peers.insert(peer_id);
                if let Some(first) = addrs.into_iter().next() {
                    self.peers.entry(peer_id).or_insert(first);
                }
            }
            Err(e) => {
                log::warn!("Failed to dial relay-introduced peer {peer_id}: {e}");
            }
        }
    }

    /// Send a NotifyTopic request to the relay after pushing changesets to peers.
    pub(super) fn notify_relay_topic(&mut self) {
        let relay_peer_id = match &self.relay_state {
            RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } => *relay_peer_id,
            other => {
                log::info!(
                    "notify_relay_topic skipped: relay_state is {other:?} \
                     (expected Connected or Listening) — relay won't send FCM \
                     for this write"
                );
                return;
            }
        };

        let req = push_protocol::PushRequest::NotifyTopic {
            topic: self.topic_name.clone(),
            sender_site_id: self
                .site_id
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        };

        log::info!(
            "Sending NotifyTopic to relay {relay_peer_id} for topic {} \
             (this should produce a 'NotifyTopic received' log on the relay)",
            self.topic_name
        );
        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
    }

    /// Pop peers from the rendezvous dial queue until we hit the concurrency limit
    /// or the queue is empty. Called after each connection completes or fails.
    pub(super) fn drain_pending_rendezvous_dials(&mut self) {
        while self.dialing_peers.len() < MAX_CONCURRENT_RENDEZVOUS_DIALS {
            let Some((peer_id, addr)) = self.pending_rendezvous_dials.pop_front() else {
                break;
            };
            if self.swarm.is_connected(&peer_id) || self.dialing_peers.contains(&peer_id) {
                continue;
            }
            log::info!("Rendezvous dialing queued peer {peer_id} at {addr}");
            if let Err(e) = self.swarm.dial(addr) {
                log::warn!("Failed to dial queued rendezvous peer {peer_id}: {e}");
            } else {
                self.dialing_peers.insert(peer_id);
            }
        }
    }
}
