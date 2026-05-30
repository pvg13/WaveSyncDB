//! Application-level identity announcement to verified peers.

use super::*;

impl EngineRunner {
    /// Send an `IdentityAnnounce` to a single verified peer.
    pub(super) fn send_identity_announce(&mut self, peer_id: libp2p::PeerId, app_id: &str) {
        let mut req = SyncRequest::IdentityAnnounce {
            app_id: app_id.to_string(),
            hmac: None,
        };

        // Identity is node-level; sign with the default group's key for now.
        // Per-group identity announce is a P3 refinement.
        if let Some(ref gk) = self.default_group().group_key
            && let Ok(bytes) = serde_json::to_vec(&req)
        {
            let tag = gk.mac(&bytes);
            if let SyncRequest::IdentityAnnounce { ref mut hmac, .. } = req {
                *hmac = Some(tag);
            }
        }

        self.swarm
            .behaviour_mut()
            .snapshot
            .send_request(&peer_id, req);
    }

    /// Announce identity to all currently verified peers.
    pub(super) fn announce_identity_to_verified_peers(&mut self, app_id: &str) {
        let peers: Vec<libp2p::PeerId> = self
            .groups
            .values()
            .flat_map(|g| g.verified_peers.iter().copied())
            .collect();
        for peer_id in peers {
            self.send_identity_announce(peer_id, app_id);
        }
    }
}
