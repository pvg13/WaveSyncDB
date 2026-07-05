//! Relay, NAT traversal, rendezvous, and push notification handling.

use super::*;

/// Maximum number of rendezvous-discovered peers dialed concurrently.
/// Remaining peers are queued and drained as connections complete or fail.
const MAX_CONCURRENT_RENDEZVOUS_DIALS: usize = 5;

/// DCUtR retry schedule. Index = attempt count (1-based). Values are base
/// delays in seconds; actual delay adds ±25% jitter to break thundering
/// herds when many peers fail in lockstep after a network blip.
///
/// libp2p's measured DCUtR success rate (~70% across 4.4M attempts per
/// arxiv 2510.27500) is roughly 20pp below Tailscale's (~90%); one of
/// the known gaps is no retry on transient hole-punch failures (RTT
/// jitter trips the synchronized punch, especially on cellular). This
/// implements the simplest version of that — re-dial after a short
/// delay, up to a small cap.
pub(super) const DCUTR_RETRY_DELAYS_SECS: &[u64] = &[2, 8, 30];
pub(super) const DCUTR_MAX_ATTEMPTS: u32 = DCUTR_RETRY_DELAYS_SECS.len() as u32;

/// Per-peer DCUtR retry book-keeping. Created when a hole-punch attempt
/// fails; cleared on success (direct connection established) or on peer
/// disconnect.
#[derive(Debug, Clone)]
pub(crate) struct DcutrRetryState {
    /// 1-based — `attempts == 1` means "one failure, retry #1 pending."
    pub(super) attempts: u32,
    /// When the next dial should fire. `process_dcutr_retries` drains
    /// entries whose time has passed.
    pub(super) next_attempt: tokio::time::Instant,
}

impl DcutrRetryState {
    fn schedule(attempts: u32) -> Option<Self> {
        if attempts > DCUTR_MAX_ATTEMPTS {
            return None;
        }
        let base = DCUTR_RETRY_DELAYS_SECS[(attempts - 1) as usize];
        // ±25% jitter. Uses the OS RNG once per scheduling — cheap.
        let jitter_range = (base as f64) * 0.25;
        let jitter = (rand::random::<f64>() * 2.0 - 1.0) * jitter_range;
        let delay_secs = (base as f64) + jitter;
        let delay = std::time::Duration::from_secs_f64(delay_secs.max(0.5));
        Some(Self {
            attempts,
            next_attempt: tokio::time::Instant::now() + delay,
        })
    }
}

impl EngineRunner {
    pub(super) fn handle_relay_client(&mut self, event: relay::client::Event) {
        match event {
            relay::client::Event::ReservationReqAccepted { relay_peer_id, .. } => {
                tracing::info!("Relay reservation accepted by {relay_peer_id}");
                self.relay_state = RelayState::Listening { relay_peer_id };
                self.circuit_retry_count = 0;
                self.circuit_accepted_at = Some(tokio::time::Instant::now());
                self.diagnostics
                    .circuit_reservations_accepted
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Clear the in-flight flag — the relay accepted our request,
                // so the next legitimate caller (proactive renewal,
                // listener-closed re-listen) can issue a fresh one without
                // the helper short-circuiting.
                self.circuit_listen_pending = false;
                self.emit_network_event(crate::network_status::NetworkEvent::RelayStatusChanged(
                    crate::network_status::RelayStatus::Listening,
                ));
                self.update_network_status();
            }
            _ => {
                tracing::info!("Relay client event (non-acceptance): {:?}", event);
            }
        }
    }

    pub(super) fn handle_dcutr(&mut self, event: dcutr::Event) {
        // Every event is one upgrade attempt that completed (Ok or Err).
        // Counting at event arrival keeps "attempted = succeeded + failed"
        // exact, which matters for the success-rate metric in
        // diagnostics consumers and netem benchmark assertions.
        self.diagnostics
            .dcutr_upgrades_attempted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let peer = event.remote_peer_id;
        match event.result {
            Ok(_) => {
                self.diagnostics
                    .dcutr_upgrades_succeeded
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(
                    "DCUtR: direct connection upgrade succeeded with {peer} (sync now goes peer-to-peer, bypassing the relay)"
                );
            }
            Err(error) => {
                let current_attempts = self
                    .dcutr_retries
                    .get(&peer)
                    .map(|s| s.attempts)
                    .unwrap_or(0);
                let next_attempts = current_attempts + 1;
                match DcutrRetryState::schedule(next_attempts) {
                    Some(state) => {
                        tracing::info!(
                            "DCUtR: upgrade failed with {peer}: {error} \
                             (scheduling retry #{next_attempts}/{DCUTR_MAX_ATTEMPTS} \
                             in ~{:?})",
                            state
                                .next_attempt
                                .duration_since(tokio::time::Instant::now())
                        );
                        self.dcutr_retries.insert(peer, state);
                    }
                    None => {
                        tracing::info!(
                            "DCUtR: upgrade failed with {peer}: {error} \
                             (retry budget exhausted after {DCUTR_MAX_ATTEMPTS} attempts; \
                             sync stays on circuit-relay path)"
                        );
                        self.dcutr_retries.remove(&peer);
                    }
                }
            }
        }
    }

    /// Walk the DCUtR retry map and fire a dial for any peer whose
    /// `next_attempt` has elapsed. Called from the engine's periodic tick.
    ///
    /// Dialing the peer (without specifying an explicit address) triggers
    /// the libp2p DCUtR Behaviour to attempt another hole-punch through
    /// the existing circuit-relay connection — exactly what we want.
    pub(super) fn process_dcutr_retries(&mut self) {
        let now = tokio::time::Instant::now();
        let due: Vec<libp2p::PeerId> = self
            .dcutr_retries
            .iter()
            .filter(|(_, s)| s.next_attempt <= now)
            .map(|(peer, _)| *peer)
            .collect();
        for peer in due {
            tracing::info!("DCUtR: re-attempting direct upgrade with {peer}");
            // Mark the next slot in advance. If this dial also fails, the
            // dcutr event handler will overwrite with a fresh schedule
            // (or remove the entry if max attempts exceeded).
            if let Some(state) = self.dcutr_retries.get_mut(&peer) {
                let next_attempts = state.attempts + 1;
                match DcutrRetryState::schedule(next_attempts) {
                    Some(new) => *state = new,
                    None => {
                        self.dcutr_retries.remove(&peer);
                    }
                }
            }
            // Use DialOpts::peer_id so libp2p picks the peer's known
            // direct addresses (from identify); the relay path is
            // implicitly held open by the existing connection.
            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer).build();
            if let Err(e) = self.swarm.dial(dial_opts) {
                tracing::debug!("DCUtR retry dial for {peer} failed to enqueue: {e}");
            }
        }
    }

    pub(super) fn handle_autonat(&mut self, event: autonat::v2::client::Event) {
        // AutoNAT completed — cancel the assumption timer (real result takes precedence)
        self.nat_assumption_deadline = None;
        match &event.result {
            Ok(()) => {
                tracing::info!(
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
                tracing::info!(
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
                tracing::info!(
                    "Registered at rendezvous server {rendezvous_node} with namespace '{namespace}' (TTL: {ttl}s)"
                );
                // Mark the matching group's namespace as registered.
                let ns = namespace.to_string();
                if let Some(g) = self
                    .groups
                    .values_mut()
                    .find(|g| g.rendezvous_namespace == ns)
                {
                    g.rendezvous_registered = true;
                }
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
                tracing::warn!(
                    "Rendezvous registration failed at {rendezvous_node} namespace '{namespace}': {error:?}"
                );
                let ns = namespace.to_string();
                if let Some(g) = self
                    .groups
                    .values_mut()
                    .find(|g| g.rendezvous_namespace == ns)
                {
                    g.rendezvous_registered = false;
                }
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
                // Pagination cookie intentionally ignored: each `discover` is
                // issued with a fresh (None) cookie since the event doesn't
                // report which namespace it answered, so a stored cookie can't
                // be attributed to the right group (see `rendezvous_discover`).
                ..
            } => {
                tracing::info!(
                    "Discovered {} peers via rendezvous at {rendezvous_node}",
                    registrations.len()
                );

                // Collect every registered address per peer, skip ineligible.
                // All addresses are raced in a single dial (like
                // `dial_introduced_peer`): a registration typically holds
                // both direct and circuit addresses, and picking just the
                // first meant one stale direct address could fail the whole
                // attempt, feed the dial backoff, and stall discovery until
                // the next tick.
                let mut to_dial: Vec<(libp2p::PeerId, Vec<libp2p::Multiaddr>)> = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for registration in registrations {
                    let peer_id = registration.record.peer_id();
                    let rejected_by_all = !self.groups.is_empty()
                        && self.groups.values().all(|g| g.is_rejected(&peer_id));
                    if peer_id == self.local_peer_id
                        || self.dialing_peers.contains(&peer_id)
                        || rejected_by_all
                        || !seen.insert(peer_id)
                    {
                        continue;
                    }
                    if self.swarm.is_connected(&peer_id) {
                        let ready: Vec<String> = self
                            .groups
                            .iter()
                            .filter(|(_, g)| g.registry_is_ready)
                            .map(|(t, _)| t.clone())
                            .collect();
                        for t in ready {
                            self.initiate_sync_for_peer(peer_id, &t);
                        }
                        continue;
                    }
                    let addrs = registration.record.addresses().to_vec();
                    if !addrs.is_empty() {
                        to_dial.push((peer_id, addrs));
                    }
                }

                // Dial up to MAX immediately, queue the rest
                let immediate = to_dial.len().min(MAX_CONCURRENT_RENDEZVOUS_DIALS);
                for (peer_id, addrs) in to_dial.drain(..immediate) {
                    self.dial_rendezvous_peer(peer_id, addrs);
                }
                if !to_dial.is_empty() {
                    tracing::info!(
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
                tracing::warn!(
                    "Rendezvous discovery failed at {rendezvous_node} namespace {namespace:?}: {error:?}"
                );
            }
            rendezvous::client::Event::Expired { peer } => {
                tracing::debug!("Rendezvous registration expired for peer {peer}");
            }
        }
    }

    /// Register every group's namespace with the rendezvous server.
    pub(super) fn rendezvous_register(&mut self, server_peer_id: libp2p::PeerId) {
        let namespaces: Vec<String> = self
            .groups
            .values()
            .map(|g| g.rendezvous_namespace.clone())
            .collect();
        for ns_str in namespaces {
            let namespace = match rendezvous::Namespace::new(ns_str.clone()) {
                Ok(ns) => ns,
                Err(e) => {
                    tracing::error!("Invalid rendezvous namespace {ns_str}: {e:?}");
                    continue;
                }
            };
            match self.swarm.behaviour_mut().rendezvous.register(
                namespace,
                server_peer_id,
                None, // Let server assign default TTL (server MIN_TTL=7200s)
            ) {
                Ok(()) => {
                    tracing::info!(
                        "Sent rendezvous registration for '{ns_str}' to {server_peer_id}"
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to send rendezvous registration for '{ns_str}': {e}");
                }
            }
        }
    }

    /// Discover peers from the rendezvous server.
    pub(super) fn rendezvous_discover(&mut self) {
        let server_peer_id = match &self.config.rendezvous_server {
            Some(addr) => match addr.iter().last() {
                Some(libp2p::multiaddr::Protocol::P2p(peer_id)) => peer_id,
                _ => {
                    tracing::debug!("Rendezvous server address has no peer ID, skipping discover");
                    return;
                }
            },
            None => return,
        };

        if !self.swarm.is_connected(&server_peer_id) {
            tracing::debug!("Not connected to rendezvous server, skipping discover");
            return;
        }

        // Re-register if we have external addresses (avoids NoExternalAddresses error).
        // Handles TTL expiry and stale state after silent disconnects.
        if self.swarm.external_addresses().count() > 0 {
            self.rendezvous_register(server_peer_id);
        }

        // Discover each group's namespace with a fresh (None) cookie. libp2p
        // rendezvous cookies are namespace-scoped pagination markers, but the
        // `Discovered` event doesn't report which namespace it answered, so a
        // single shared cookie cannot be attributed back to the right group.
        // Reusing one namespace's cookie for another makes the server treat the
        // second request as already-paginated and return nothing — which
        // silently broke discovery for every secondary group. At our scale
        // (a handful of peers per namespace) re-fetching the full registration
        // list each tick is negligible, and it always returns the namespace's
        // current registrations.
        let namespaces: Vec<String> = self
            .groups
            .values()
            .map(|g| g.rendezvous_namespace.clone())
            .collect();
        for ns_str in namespaces {
            let namespace = match rendezvous::Namespace::new(ns_str.clone()) {
                Ok(ns) => ns,
                Err(e) => {
                    tracing::error!("Invalid rendezvous namespace {ns_str}: {e:?}");
                    continue;
                }
            };
            self.swarm.behaviour_mut().rendezvous.discover(
                Some(namespace),
                None,
                None,
                server_peer_id,
            );
        }
    }

    /// Attempt to reconnect to the relay server if disconnected.
    pub(super) fn maybe_reconnect_relay(&mut self) {
        // Snapshot the state up front so we can call `&mut self` helpers
        // (try_dial_relay, try_listen_on_circuit) without overlapping with
        // an immutable borrow of `self.relay_state`.
        enum Action {
            None,
            Connecting(u32),
            Stuck {
                relay_peer_id: libp2p::PeerId,
                stuck_for: Duration,
            },
        }
        let action = match &self.relay_state {
            RelayState::Connecting { retry_count } => Action::Connecting(*retry_count),
            RelayState::Connected {
                relay_peer_id,
                connected_at,
            } => Action::Stuck {
                relay_peer_id: *relay_peer_id,
                stuck_for: connected_at.elapsed(),
            },
            RelayState::Disabled | RelayState::Listening { .. } => Action::None,
        };

        match action {
            Action::Connecting(count) => {
                // Exponential backoff via tick-skipping: dial every 2^min(count,3) ticks.
                // With 5s base interval: 5s, 10s, 20s, 40s, 40s, 40s...
                let skip = 1u32 << count.min(3); // 1, 2, 4, 8, 8, 8...
                if count % skip == 0 {
                    tracing::info!("Attempting relay reconnection (attempt {})", count + 1);
                    self.try_dial_relay();
                }
                // Rotate to the next fallback relay after every 4 failed
                // attempts at the current one (~40s of stale backoff with
                // base 5s tick). The primary stays at index 0 in the
                // rotation; on wrap-around we come back to it, which is
                // what we want if the primary recovered.
                let next_count = count + 1;
                if next_count.is_multiple_of(4) && !self.config.relay_fallbacks.is_empty() {
                    self.rotate_to_next_relay();
                }
                self.relay_state = RelayState::Connecting {
                    retry_count: next_count,
                };
            }
            Action::Stuck {
                relay_peer_id,
                stuck_for,
            } if stuck_for > Duration::from_secs(5) => {
                // Circuit reservation hasn't completed — retry if stuck for >5s.
                // The retry is also where the renewal storm originated: a stuck
                // Connected used to fire a fresh `listen_on(circuit)` per tick
                // even when an earlier request was still in flight. Routing
                // through `try_listen_on_circuit` makes it a no-op while the
                // first request is pending.
                self.circuit_retry_count += 1;
                if self.circuit_retry_count >= 3 {
                    tracing::warn!(
                        "Circuit reservation failed {} times, forcing full relay reconnect",
                        self.circuit_retry_count
                    );
                    self.circuit_retry_count = 0;
                    let _ = self.swarm.disconnect_peer_id(relay_peer_id);
                    // ConnectionClosed handler will reset to Connecting and
                    // trigger full reconnect
                } else {
                    tracing::info!(
                        "Relay stuck in Connected for >5s, retrying circuit reservation (attempt {})",
                        self.circuit_retry_count
                    );
                    self.try_listen_on_circuit(false);
                    // Reset connected_at to space out retries
                    self.relay_state = RelayState::Connected {
                        relay_peer_id,
                        connected_at: tokio::time::Instant::now(),
                    };
                }
            }
            Action::Stuck { .. } | Action::None => {
                // Either we just connected and haven't been stuck long enough,
                // or relay is Disabled / already Listening — nothing to do.
            }
        }
    }

    /// Rotate the primary relay with the next fallback. Called after the
    /// current relay has failed several consecutive dial attempts. Moves
    /// the failed primary to the back of the fallback list, so we come
    /// back to it after exhausting the others — useful when the original
    /// failure was a transient outage.
    pub(super) fn rotate_to_next_relay(&mut self) {
        if self.config.relay_fallbacks.is_empty() {
            return;
        }
        // Swap: old primary → back of fallbacks; first fallback → primary.
        let new_primary = self.config.relay_fallbacks.remove(0);
        let old_primary = self.config.relay_server.replace(new_primary);
        if let Some(addr) = old_primary {
            self.config.relay_fallbacks.push(addr);
        }
        if let Some(ref addr) = self.config.relay_server {
            tracing::info!("Rotated to next relay fallback: {addr}");
        }
    }

    /// Register the push token with the relay for every group topic that
    /// hasn't been registered yet on the current relay connection. Idempotent:
    /// topics already in `push_registered_topics` are skipped, so this is safe
    /// to call both on relay-connect and whenever a new group is joined.
    pub(super) fn maybe_register_push_token(&mut self, relay_peer_id: libp2p::PeerId) {
        if self.push_token.is_none() {
            // Fresh-install race: the OS may deliver the push token AFTER the
            // engine started (the token file appears once the platform service
            // writes it). Re-read here so registration succeeds mid-run
            // instead of waiting for the next app launch.
            #[cfg(all(feature = "push-sync", target_os = "ios"))]
            if let Some(token) = crate::push::read_apns_token_file(&self.database_url) {
                tracing::info!("APNs token file appeared mid-run — registering push token");
                self.push_token = Some(("Apns".to_string(), token));
            }
            #[cfg(all(feature = "push-sync", target_os = "android"))]
            if let Some(token) = crate::push::read_token_file(&self.database_url) {
                tracing::info!("FCM token file appeared mid-run — registering push token");
                self.push_token = Some(("Fcm".to_string(), token));
            }
        }
        if self.push_token.is_none() {
            if self.push_token_skip_logged {
                tracing::debug!("maybe_register_push_token skipped: no push_token set");
            } else {
                self.push_token_skip_logged = true;
                tracing::info!(
                    "maybe_register_push_token skipped: no push_token set — \
                     either the platform isn't mobile, push-sync feature is off, \
                     or the FCM/APNs token file wasn't written by the OS service \
                     in time. Retrying quietly; push registers as soon as the \
                     token file appears."
                );
            }
            return;
        }
        // Register every topic not yet covered on this connection.
        let topics: Vec<String> = self.groups.values().map(|g| g.topic_name.clone()).collect();
        for topic in topics {
            self.register_push_token_for_topic(relay_peer_id, &topic);
        }
    }

    /// Send a `RegisterToken` for a single group topic if we hold a push token
    /// and the topic isn't already registered or mid-registration on the
    /// current relay connection. The topic is recorded in
    /// `push_registered_topics` only once the relay *acks* the request (see the
    /// push response handler); here we only track the in-flight request id in
    /// `push_pending_registrations`. This keeps repeated calls (relay-connect
    /// sweep, join-time registration, 5s reconcile) idempotent without
    /// optimistically marking a topic registered before delivery is confirmed —
    /// a `RegisterToken` that loses the race against a not-yet-ready relayed
    /// substream fails, the topic stays unregistered, and the reconcile retries.
    pub(super) fn register_push_token_for_topic(
        &mut self,
        relay_peer_id: libp2p::PeerId,
        topic: &str,
    ) {
        let (platform, token) = match &self.push_token {
            Some(pt) => pt.clone(),
            None => return,
        };
        // Skip if confirmed-registered or a request for this topic is already
        // in flight (don't pile on duplicates every reconcile tick).
        if self.push_registered_topics.contains(topic)
            || self.push_pending_registrations.values().any(|t| t == topic)
        {
            return;
        }
        let push_platform = match platform.as_str() {
            "Fcm" => push_protocol::PushPlatform::Fcm,
            "Apns" => push_protocol::PushPlatform::Apns,
            other => {
                tracing::warn!("Unknown push platform: {other}");
                return;
            }
        };

        let req = push_protocol::PushRequest::RegisterToken {
            topic: topic.to_string(),
            platform: push_platform,
            token,
        };
        let request_id = self
            .swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
        self.push_pending_registrations
            .insert(request_id, topic.to_string());
        tracing::info!("Sent push token registration for topic {topic} to relay {relay_peer_id}");
    }

    /// Tell the relay to stop waking this device for a topic we've left, and
    /// drop the topic from the registered set so a later re-join re-registers.
    pub(super) fn unregister_push_token_for_topic(
        &mut self,
        relay_peer_id: libp2p::PeerId,
        topic: &str,
    ) {
        let token = match &self.push_token {
            Some((_, token)) => token.clone(),
            None => return,
        };
        let req = push_protocol::PushRequest::UnregisterToken {
            topic: topic.to_string(),
            token,
        };
        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
        self.push_registered_topics.remove(topic);
        // Drop any in-flight RegisterToken for this topic so a late ack can't
        // resurrect it into the registered set after we've left the group.
        self.push_pending_registrations.retain(|_, t| t != topic);
        self.update_network_status();
        tracing::info!("Sent push token unregistration for topic {topic} to relay {relay_peer_id}");
    }

    /// Announce this peer's presence to the relay so it can introduce us
    /// to other peers on the same topic. Called on every relay connect,
    /// regardless of whether push notifications are configured — this is
    /// how two foreground peers behind NAT discover each other without
    /// running a separate rendezvous server.
    pub(super) fn announce_presence_to_relay(&mut self, relay_peer_id: libp2p::PeerId) {
        // Announce presence under every group's topic so the relay can introduce
        // us to peers in each of our groups.
        let topics: Vec<String> = self.groups.values().map(|g| g.topic_name.clone()).collect();
        for topic in topics {
            let req = push_protocol::PushRequest::AnnouncePresence { topic };
            self.swarm
                .behaviour_mut()
                .push
                .send_request(&relay_peer_id, req);
        }
        tracing::info!("Announced presence to relay {relay_peer_id} for all group topics");
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
    ///
    /// `fresh_announce` marks a `PeerJoined` push (the peer just attached to
    /// the relay — live right now, by definition) as opposed to a `PeerList`
    /// re-introduction on our own announce. A fresh announce clears any
    /// standing dial backoff for the peer: the backoff exists to stop
    /// re-dialing an *unreachable* peer that discovery keeps re-surfacing,
    /// and fresh liveness evidence invalidates that premise. Without this,
    /// a backoff seeded by earlier failures (e.g. stale cold-start cache
    /// dials) silently swallowed the relay's one-shot introduction and
    /// discovery stalled until the next rendezvous tick.
    pub(super) fn dial_introduced_peer(&mut self, addr_strs: &[String], fresh_announce: bool) {
        let addrs: Vec<libp2p::Multiaddr> = addr_strs
            .iter()
            .filter_map(|s| match s.parse::<libp2p::Multiaddr>() {
                Ok(a) => Some(a),
                Err(e) => {
                    tracing::warn!("Relay introduced unparseable address {s:?}: {e}");
                    None
                }
            })
            .collect();

        // Walk each multiaddr forward and keep the **last** `/p2p/<id>` —
        // for a circuit-relay address (`/.../p2p/<relay>/p2p-circuit/p2p/<dest>`)
        // the first /p2p/ is the relay, the last is the actual destination.
        // Picking the first means every circuit address is mis-attributed
        // to the relay we're already connected to, and the dial is
        // silently dropped by the "already connected" guard below.
        let peer_id = addrs.iter().find_map(|a| {
            let mut last = None;
            for p in a.iter() {
                if let libp2p::multiaddr::Protocol::P2p(pid) = p {
                    last = Some(pid);
                }
            }
            last
        });
        let Some(peer_id) = peer_id else {
            tracing::warn!("Relay introduced peer with no /p2p/ suffix on any address");
            return;
        };

        if fresh_announce {
            self.clear_dial_backoff(&peer_id);
        }

        let rejected_by_all =
            !self.groups.is_empty() && self.groups.values().all(|g| g.is_rejected(&peer_id));
        if peer_id == self.local_peer_id
            || self.infrastructure_peers.contains(&peer_id)
            || rejected_by_all
            || self.swarm.is_connected(&peer_id)
            || self.dialing_peers.contains(&peer_id)
        {
            return;
        }
        if !self.dial_backoff_ok(&peer_id) {
            // Never silent: a dropped introduction is otherwise
            // indistinguishable from the relay not introducing at all.
            tracing::debug!("Skipping introduced peer {peer_id}: inside dial backoff window");
            return;
        }

        // Storm guard (#84 regression): if a direct path to this peer already
        // exists, never dial its circuit-relay address. The relay re-introduces
        // peers on every presence announce; without this each re-introduction
        // re-opens a circuit that the demotion logic immediately closes — an
        // establish→close→redial loop that exhausts the relay's per-peer circuit
        // cap. See `dialable_addrs_preferring_direct`.
        let addrs = dialable_addrs_preferring_direct(addrs, self.suppress_relay_dial(&peer_id));

        if addrs.is_empty() {
            tracing::debug!(
                "Skipping dial to {peer_id}: no non-circuit address while a direct path is preferred"
            );
            return;
        }

        tracing::info!(
            "Dialing relay-introduced peer {peer_id} with {} address(es): {addrs:?}",
            addrs.len()
        );
        let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
            .addresses(addrs.clone())
            .build();
        match self.swarm.dial(dial_opts) {
            Ok(()) => {
                self.diagnostics
                    .peer_dial_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.dialing_peers.insert(peer_id);
                if let Some(first) = addrs.into_iter().next() {
                    self.peers.entry(peer_id).or_insert(first);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to dial relay-introduced peer {peer_id}: {e}");
            }
        }
    }

    /// Send a NotifyTopic request to the relay after pushing changesets to peers
    /// for the group identified by `effective_topic`. `visible` is computed
    /// sender-side per changeset — true only when the changeset touched a
    /// table with a registered `SyncNotify` policy — and tells the relay
    /// whether it may spend an unbudgeted ALERT-class APNs send (realtime
    /// banner) instead of today's silent background wake. Callers outside
    /// the local-changeset path (no changeset content to inspect) must pass
    /// `false`.
    pub(super) fn notify_relay_topic(&mut self, effective_topic: &str, visible: bool) {
        let relay_peer_id = match &self.relay_state {
            RelayState::Connected { relay_peer_id, .. }
            | RelayState::Listening { relay_peer_id } => *relay_peer_id,
            other => {
                tracing::info!(
                    "notify_relay_topic skipped: relay_state is {other:?} \
                     (expected Connected or Listening) — relay won't send FCM \
                     for this write"
                );
                return;
            }
        };

        let Some(g) = self.groups.get(effective_topic) else {
            return;
        };
        let req = push_protocol::PushRequest::NotifyTopic {
            topic: g.topic_name.clone(),
            sender_site_id: g
                .site_id
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            visible,
        };

        tracing::info!(
            "Sending NotifyTopic to relay {relay_peer_id} for topic {effective_topic} \
             (this should produce a 'NotifyTopic received' log on the relay)"
        );
        self.swarm
            .behaviour_mut()
            .push
            .send_request(&relay_peer_id, req);
    }

    /// Dial a rendezvous-discovered peer, racing all of its registered
    /// addresses in one attempt (libp2p connects via whichever works first).
    fn dial_rendezvous_peer(&mut self, peer_id: libp2p::PeerId, addrs: Vec<libp2p::Multiaddr>) {
        tracing::info!(
            "Rendezvous dialing peer {peer_id} with {} address(es): {addrs:?}",
            addrs.len()
        );
        let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
            .addresses(addrs)
            .build();
        if let Err(e) = self.swarm.dial(dial_opts) {
            tracing::warn!("Failed to dial rendezvous peer {peer_id}: {e}");
        } else {
            self.dialing_peers.insert(peer_id);
        }
    }

    /// Pop peers from the rendezvous dial queue until we hit the concurrency limit
    /// or the queue is empty. Called after each connection completes or fails.
    pub(super) fn drain_pending_rendezvous_dials(&mut self) {
        while self.dialing_peers.len() < MAX_CONCURRENT_RENDEZVOUS_DIALS {
            let Some((peer_id, addrs)) = self.pending_rendezvous_dials.pop_front() else {
                break;
            };
            if self.swarm.is_connected(&peer_id) || self.dialing_peers.contains(&peer_id) {
                continue;
            }
            self.dial_rendezvous_peer(peer_id, addrs);
        }
    }
}
