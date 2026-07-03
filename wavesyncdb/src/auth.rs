//! Peer group authentication via PSK-derived topic isolation and HMAC verification.
//!
//! Two layers, both derived from a single user-supplied passphrase:
//! 1. **Topic isolation** — derive topic name from BLAKE3 hash so different
//!    passphrases yield different topics and peers never see each other's messages.
//! 2. **HMAC on all messages** — request-response messages carry a BLAKE3-keyed MAC.
//!    Peers without the PSK cannot forge or inject valid messages.
//!
//! ## Key derivation
//!
//! The group key is the entire security boundary: anyone who recovers it can forge
//! valid MACs and inject arbitrary writes. It is derived from the passphrase with
//! **Argon2id**, salted with the user topic. The memory-hard cost (~19 MiB, 2
//! passes) prices each offline guess at ~100 ms of CPU plus real RAM instead of
//! nanoseconds — an observer who sees a group's derived topic in cleartext (notably
//! the relay, which sees every topic) can no longer run a cheap dictionary attack
//! against it, and GPUs gain little because of the memory requirement. The salt
//! keeps derivation deterministic across peers (no coordination needed) while
//! binding the key to the deployment: the same passphrase under a different user
//! topic yields an unrelated key, so no precomputed table transfers between
//! deployments.
//!
//! Every peer of a passphrase group must run the same derivation — there is no
//! multi-version negotiation. The derived-topic prefix (`wavesync2-`) namespaces
//! this scheme's topics so any peer running an older derivation lands on a
//! different topic string and is silently ignored (never rejected).
//!
//! ## Trust model — known limitations (by design)
//!
//! The group key is a single shared symmetric secret: every member is
//! cryptographically equal. That yields a few deliberate limitations callers must
//! understand — none is a bug, but each bounds what HMAC can promise:
//!
//! - **No replay window.** The MAC intentionally excludes wall-clock time (clock skew
//!   between peers must not cause valid messages to fail), and messages carry no nonce
//!   or sequence number, so a captured authenticated message replays as authentic.
//!   CRDT idempotence neutralizes the meaningful cases — a replayed stale column change
//!   loses the `col_version` comparison and a replayed stale delete loses against a
//!   higher local clock, so replay cannot resurrect a deleted row or roll a value back.
//!   The residual cost is wasted re-processing, not divergence.
//! - **Authorship is self-asserted.** The MAC binds the message topic but not the
//!   sender's libp2p `PeerId`, and `site_id` is a sender-supplied field. Any valid
//!   member can therefore attribute a write to another `site_id` or pick one to win the
//!   deterministic conflict tiebreak. There is no intra-group least-privilege: one
//!   leaked passphrase grants full write authority to the whole dataset.
//! - **Membership is not private from the relay.** The relay/rendezvous sees each
//!   group's derived topic in cleartext and can enumerate the peers announcing on it.
//!   Topic isolation hides message *content* and blocks *injection* (no key ⇒ no valid
//!   MAC), but it does not hide *who* is in a group from infrastructure that knows the
//!   topic string. What the Argon2id derivation adds: knowing the topic string no
//!   longer lets that infrastructure cheaply recover the passphrase behind it.

use subtle::ConstantTimeEq;

/// Argon2id cost parameters for group-key derivation.
///
/// Derivation happens rarely (engine build / config load), so these can be well
/// above interactive-login budgets. Values meet the OWASP Argon2id minimum
/// (19 MiB, 2 iterations, 1 lane) — heavy enough to price out GPU dictionary
/// attacks, light enough to run once on a mobile cold start.
const ARGON2_M_COST_KIB: u32 = 19_456;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;

/// A group authentication key.
///
/// All peers sharing the same `(passphrase, user_topic)` pair derive the same
/// `GroupKey`, enabling them to join the same sync topic and verify each other's
/// messages.
#[derive(Clone)]
pub struct GroupKey {
    key: [u8; 32],
}

impl GroupKey {
    /// Derive a group key with Argon2id over `(passphrase, salt = user_topic)`.
    ///
    /// The salt is the BLAKE3 hash of the user topic, giving a deterministic 16-byte
    /// salt every peer computes identically while still binding the key to the
    /// deployment (no cross-deployment rainbow tables). Derivation is intentionally
    /// slow (~100 ms native, seconds on wasm); call it once and cache the result.
    pub fn from_passphrase(passphrase: &str, user_topic: &str) -> Self {
        use argon2::{Algorithm, Argon2, Params, Version};

        // Deterministic per-deployment salt. Argon2 requires >= 8 bytes; use 16.
        let salt_hash = blake3::hash(user_topic.as_bytes());
        let salt = &salt_hash.as_bytes()[..16];

        let params = Params::new(ARGON2_M_COST_KIB, ARGON2_T_COST, ARGON2_P_COST, Some(32))
            .expect("static Argon2 params are valid");
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

        let mut key = [0u8; 32];
        argon2
            .hash_password_into(passphrase.as_bytes(), salt, &mut key)
            .expect("Argon2id derivation with static params cannot fail");
        Self { key }
    }

    /// Derive a sync topic name from the user topic and this group key.
    ///
    /// Different passphrases and different user topics produce different topic
    /// names, providing topic-level isolation so peers in different groups never
    /// see each other's messages. The `wavesync2-` prefix namespaces this
    /// derivation scheme: a peer running an older scheme derives a different
    /// prefix and lands in the silent-ignore path instead of colliding on a
    /// same-name topic with mismatched keys (which would trigger HMAC-failure
    /// rejection and backoff churn).
    pub fn derive_topic(&self, user_topic: &str) -> String {
        let mut hasher = blake3::Hasher::new_derive_key("wavesyncdb-topic-v1");
        hasher.update(user_topic.as_bytes());
        hasher.update(&self.key);
        let hash = hasher.finalize();
        format!("wavesync2-{}", hash.to_hex())
    }

    /// Derive a rendezvous namespace from the effective topic name.
    ///
    /// This reuses the topic derivation output as-is, since the effective topic
    /// is already derived from `(user_topic, group_key)` and provides the
    /// same namespace isolation needed for rendezvous discovery.
    pub fn derive_namespace(&self, user_topic: &str) -> String {
        self.derive_topic(user_topic)
    }

    /// Compute a BLAKE3 keyed MAC over the given data.
    pub fn mac(&self, data: &[u8]) -> [u8; 32] {
        *blake3::keyed_hash(&self.key, data).as_bytes()
    }

    /// Verify a BLAKE3 keyed MAC over the given data in constant time.
    ///
    /// Uses a constant-time comparison so a network attacker cannot recover a valid
    /// tag byte-by-byte from response-timing differences.
    pub fn verify(&self, data: &[u8], tag: &[u8; 32]) -> bool {
        let expected = self.mac(data);
        expected.ct_eq(tag).into()
    }
}

impl std::fmt::Debug for GroupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupKey").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_and_salted() {
        // Same (passphrase, user_topic) → same key, across independent derivations.
        let a = GroupKey::from_passphrase("my-secret", "my-app");
        let b = GroupKey::from_passphrase("my-secret", "my-app");
        assert_eq!(a.key, b.key);

        // Salt = user_topic: the same passphrase under a different user topic yields a
        // different key, defeating cross-deployment precomputation.
        let c = GroupKey::from_passphrase("my-secret", "other-app");
        assert_ne!(a.key, c.key);
    }

    #[test]
    fn test_different_passphrases_different_keys() {
        let k1 = GroupKey::from_passphrase("alpha", "app");
        let k2 = GroupKey::from_passphrase("beta", "app");
        assert_ne!(k1.key, k2.key);
    }

    #[test]
    fn test_peers_agree_on_topic() {
        // Two independent nodes with the same passphrase+user_topic land on the
        // same derived topic — the property group formation relies on.
        let a = GroupKey::from_passphrase("shared-pw", "roommates");
        let b = GroupKey::from_passphrase("shared-pw", "roommates");
        assert_eq!(a.derive_topic("roommates"), b.derive_topic("roommates"));
    }

    #[test]
    fn test_different_passphrases_different_topics() {
        let k1 = GroupKey::from_passphrase("alpha", "my-app");
        let k2 = GroupKey::from_passphrase("beta", "my-app");
        let t1 = k1.derive_topic("my-app");
        let t2 = k2.derive_topic("my-app");
        assert_ne!(t1, t2);
        assert!(t1.starts_with("wavesync2-"));
        assert!(t2.starts_with("wavesync2-"));
    }

    #[test]
    fn test_topic_derivation_deterministic() {
        let k = GroupKey::from_passphrase("test", "app");
        let t1 = k.derive_topic("app");
        let t2 = k.derive_topic("app");
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_mac_roundtrip_and_tamper() {
        let k = GroupKey::from_passphrase("secret", "app");
        let data = b"hello world";
        let tag = k.mac(data);
        assert!(k.verify(data, &tag));
        assert!(!k.verify(b"tampered", &tag));
    }

    #[test]
    fn test_mac_wrong_key() {
        let k1 = GroupKey::from_passphrase("key1", "app");
        let k2 = GroupKey::from_passphrase("key2", "app");
        let data = b"hello world";
        let tag = k1.mac(data);
        assert!(!k2.verify(data, &tag));
    }
}
