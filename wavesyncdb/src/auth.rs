//! Peer group authentication via PSK-derived topic isolation and HMAC verification.
//!
//! Two layers, both derived from a single user-supplied passphrase:
//! 1. **Topic isolation** — derive topic name from BLAKE3 hash so different
//!    passphrases yield different topics and peers never see each other's messages.
//! 2. **HMAC on all messages** — request-response messages carry a BLAKE3-keyed MAC.
//!    Peers without the PSK cannot forge or inject valid messages.
//!
//! ## Key derivation and the v2 migration
//!
//! The group key is the entire security boundary: anyone who recovers it can forge
//! valid MACs and inject arbitrary writes. The v1 scheme derived it with a *fast*,
//! unsalted function (`blake3::derive_key`), so an observer who saw a group's topic
//! hash (notably the relay, which sees every topic in cleartext) could mount an
//! offline dictionary attack — two hashes per passphrase guess, no salt, so one
//! precomputed table covered every deployment sharing a user topic.
//!
//! The v2 scheme derives the key with **Argon2id**, salted with the user topic so it
//! stays deterministic across peers (no coordination needed) while making each guess
//! ~six orders of magnitude more expensive and defeating cross-deployment precomputation.
//!
//! Because the derived key — and therefore the derived topic — differs between v1 and
//! v2, a node cannot simply switch: it would stop syncing with un-upgraded peers. During
//! the transition window a node holds **both** keys, listens on **both** topics, verifies
//! an inbound MAC against **either** key, and signs an outbound message with the key that
//! matches the topic it is sent on. A later release drops the v1 rung.
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
//!   topic string.

use subtle::ConstantTimeEq;

/// Argon2id cost parameters for v2 group-key derivation.
///
/// Derivation happens rarely (engine build / config load), so these can be well
/// above interactive-login budgets. Values meet the OWASP Argon2id minimum
/// (19 MiB, 2 iterations, 1 lane) — heavy enough to price out GPU dictionary
/// attacks, light enough to run once on a mobile cold start.
const ARGON2_M_COST_KIB: u32 = 19_456;
const ARGON2_T_COST: u32 = 2;
const ARGON2_P_COST: u32 = 1;

/// Which key-derivation scheme produced a [`GroupKey`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KdfVersion {
    /// Legacy fast, unsalted `blake3::derive_key`. Retained for interop with
    /// un-upgraded peers during the migration window; do not use for new material.
    V1Blake3,
    /// Salted Argon2id over `(passphrase, salt = user_topic)`.
    V2Argon2id,
}

/// A single group authentication key plus the scheme that derived it.
///
/// All peers sharing the same passphrase (and, for v2, the same user topic) derive
/// the same `GroupKey`, enabling them to join the same sync topic and verify each
/// other's messages. Prefer [`GroupKeySet`] at call sites that must interoperate
/// across the v1→v2 migration; a bare `GroupKey` signs and verifies with one scheme.
#[derive(Clone)]
pub struct GroupKey {
    key: [u8; 32],
    version: KdfVersion,
}

impl GroupKey {
    /// Derive a **v1** group key from a passphrase using BLAKE3 key derivation.
    ///
    /// Legacy scheme — fast and unsalted. Retained only for migration interop; new
    /// deployments should rely on the v2 key carried by [`GroupKeySet`].
    pub fn from_passphrase(passphrase: &str) -> Self {
        let key = blake3::derive_key("wavesyncdb-group-key-v1", passphrase.as_bytes());
        Self {
            key,
            version: KdfVersion::V1Blake3,
        }
    }

    /// Derive a **v2** group key with Argon2id over `(passphrase, salt = user_topic)`.
    ///
    /// The salt is the BLAKE3 hash of the user topic, giving a deterministic 16-byte
    /// salt every peer computes identically while still binding the key to the
    /// deployment (no cross-deployment rainbow tables). Derivation is intentionally
    /// slow; call it once and cache the result.
    pub fn from_passphrase_v2(passphrase: &str, user_topic: &str) -> Self {
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
        Self {
            key,
            version: KdfVersion::V2Argon2id,
        }
    }

    /// The derivation scheme behind this key.
    pub fn version(&self) -> KdfVersion {
        self.version
    }

    /// Derive a sync topic name from the user topic and this group key.
    ///
    /// Different passphrases (and, for v2, different user topics) produce different
    /// topic names, providing topic-level isolation so peers in different groups
    /// never see each other's messages. The scheme is folded into the topic prefix
    /// (`wavesync-` for v1, `wavesync2-` for v2) so the two never collide and a peer
    /// can tell which key a topic expects.
    pub fn derive_topic(&self, user_topic: &str) -> String {
        let mut hasher = blake3::Hasher::new_derive_key("wavesyncdb-topic-v1");
        hasher.update(user_topic.as_bytes());
        hasher.update(&self.key);
        let hash = hasher.finalize();
        let prefix = match self.version {
            KdfVersion::V1Blake3 => "wavesync-",
            KdfVersion::V2Argon2id => "wavesync2-",
        };
        format!("{}{}", prefix, hash.to_hex())
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
        f.debug_struct("GroupKey")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// The set of group keys a node holds for one `(passphrase, user_topic)` pair.
///
/// During the v1→v2 migration a node carries the v2 (primary) key and, unless
/// migration is disabled, the v1 (legacy) key as well. It listens on both derived
/// topics, verifies an inbound MAC against whichever key matches the message's
/// topic, and signs an outbound message with the key for the topic it targets.
/// New material is always signed and published under v2; v1 exists only so
/// un-upgraded peers keep syncing until the window closes.
#[derive(Clone, Debug)]
pub struct GroupKeySet {
    primary: GroupKey,
    legacy: Option<GroupKey>,
    user_topic: String,
}

impl GroupKeySet {
    /// Build the migration key set for a passphrase and user topic: v2 primary plus
    /// the v1 legacy key for transition interop.
    pub fn new(passphrase: &str, user_topic: &str) -> Self {
        Self {
            primary: GroupKey::from_passphrase_v2(passphrase, user_topic),
            legacy: Some(GroupKey::from_passphrase(passphrase)),
            user_topic: user_topic.to_string(),
        }
    }

    /// Build a v2-only key set (no legacy interop). Use once the migration window
    /// has closed to stop announcing and honoring the weak v1 topic.
    pub fn new_v2_only(passphrase: &str, user_topic: &str) -> Self {
        Self {
            primary: GroupKey::from_passphrase_v2(passphrase, user_topic),
            legacy: None,
            user_topic: user_topic.to_string(),
        }
    }

    /// The primary (v2) key — used to sign all newly published messages.
    pub fn primary(&self) -> &GroupKey {
        &self.primary
    }

    /// The primary (v2) topic this node publishes under.
    pub fn primary_topic(&self) -> String {
        self.primary.derive_topic(&self.user_topic)
    }

    /// Every topic this node accepts messages on (primary first, then legacy).
    /// The engine subscribes to and gates inbound messages against all of these.
    pub fn all_topics(&self) -> Vec<String> {
        let mut topics = vec![self.primary.derive_topic(&self.user_topic)];
        if let Some(legacy) = &self.legacy {
            topics.push(legacy.derive_topic(&self.user_topic));
        }
        topics
    }

    /// Return the key that signs/verifies messages for `topic`, if this set owns it.
    pub fn key_for_topic(&self, topic: &str) -> Option<&GroupKey> {
        if topic == self.primary.derive_topic(&self.user_topic) {
            return Some(&self.primary);
        }
        if let Some(legacy) = &self.legacy
            && topic == legacy.derive_topic(&self.user_topic)
        {
            return Some(legacy);
        }
        None
    }

    /// Verify a MAC against whichever held key matches `topic`. Messages on an
    /// unknown topic are not ours to verify and return `false`.
    pub fn verify_for_topic(&self, topic: &str, data: &[u8], tag: &[u8; 32]) -> bool {
        match self.key_for_topic(topic) {
            Some(key) => key.verify(data, tag),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_derivation() {
        let k1 = GroupKey::from_passphrase("my-secret");
        let k2 = GroupKey::from_passphrase("my-secret");
        assert_eq!(k1.key, k2.key);
    }

    #[test]
    fn test_different_passphrases_different_keys() {
        let k1 = GroupKey::from_passphrase("alpha");
        let k2 = GroupKey::from_passphrase("beta");
        assert_ne!(k1.key, k2.key);
    }

    #[test]
    fn test_v2_deterministic_and_salted() {
        // Same (passphrase, user_topic) → same key, across independent derivations.
        let a = GroupKey::from_passphrase_v2("my-secret", "my-app");
        let b = GroupKey::from_passphrase_v2("my-secret", "my-app");
        assert_eq!(a.key, b.key);
        assert_eq!(a.version(), KdfVersion::V2Argon2id);

        // Salt = user_topic: the same passphrase under a different user topic yields a
        // different key, defeating cross-deployment precomputation.
        let c = GroupKey::from_passphrase_v2("my-secret", "other-app");
        assert_ne!(a.key, c.key);

        // v2 differs from v1 for the same passphrase (distinct scheme → distinct topic).
        let v1 = GroupKey::from_passphrase("my-secret");
        assert_ne!(a.key, v1.key);
    }

    #[test]
    fn test_topic_prefixes_distinguish_schemes() {
        let v1 = GroupKey::from_passphrase("pw");
        let v2 = GroupKey::from_passphrase_v2("pw", "app");
        assert!(v1.derive_topic("app").starts_with("wavesync-"));
        assert!(v2.derive_topic("app").starts_with("wavesync2-"));
        assert_ne!(v1.derive_topic("app"), v2.derive_topic("app"));
    }

    #[test]
    fn test_key_set_dual_topic_and_verify() {
        let set = GroupKeySet::new("pw", "app");
        let topics = set.all_topics();
        assert_eq!(topics.len(), 2, "migration set holds primary + legacy");
        assert_eq!(topics[0], set.primary_topic());
        assert!(topics[0].starts_with("wavesync2-"));
        assert!(topics[1].starts_with("wavesync-"));

        // A message signed under either topic's key verifies via the set.
        let data = b"changeset";
        let v2_topic = &topics[0];
        let v1_topic = &topics[1];
        let v2_tag = set.key_for_topic(v2_topic).unwrap().mac(data);
        let v1_tag = set.key_for_topic(v1_topic).unwrap().mac(data);
        assert!(set.verify_for_topic(v2_topic, data, &v2_tag));
        assert!(set.verify_for_topic(v1_topic, data, &v1_tag));
        // Cross-topic tag must not verify.
        assert!(!set.verify_for_topic(v2_topic, data, &v1_tag));
        // Unknown topic is not ours.
        assert!(!set.verify_for_topic("wavesync-deadbeef", data, &v2_tag));
    }

    #[test]
    fn test_key_set_v2_only_drops_legacy() {
        let set = GroupKeySet::new_v2_only("pw", "app");
        assert_eq!(set.all_topics().len(), 1);
        assert!(set.primary_topic().starts_with("wavesync2-"));
    }

    #[test]
    fn test_v2_peers_agree_on_topic() {
        // Two independent nodes with the same passphrase+user_topic land on the same
        // primary topic — the property the whole migration relies on.
        let a = GroupKeySet::new("shared-pw", "roommates");
        let b = GroupKeySet::new("shared-pw", "roommates");
        assert_eq!(a.primary_topic(), b.primary_topic());
        assert_eq!(a.all_topics(), b.all_topics());
    }

    #[test]
    fn test_different_passphrases_different_topics() {
        let k1 = GroupKey::from_passphrase("alpha");
        let k2 = GroupKey::from_passphrase("beta");
        let t1 = k1.derive_topic("my-app");
        let t2 = k2.derive_topic("my-app");
        assert_ne!(t1, t2);
        assert!(t1.starts_with("wavesync-"));
        assert!(t2.starts_with("wavesync-"));
    }

    #[test]
    fn test_topic_derivation_deterministic() {
        let k = GroupKey::from_passphrase("test");
        let t1 = k.derive_topic("app");
        let t2 = k.derive_topic("app");
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_mac_roundtrip() {
        let k = GroupKey::from_passphrase("secret");
        let data = b"hello world";
        let tag = k.mac(data);
        assert!(k.verify(data, &tag));
    }

    #[test]
    fn test_mac_tamper_detection() {
        let k = GroupKey::from_passphrase("secret");
        let data = b"hello world";
        let tag = k.mac(data);
        assert!(!k.verify(b"tampered", &tag));
    }

    #[test]
    fn test_mac_wrong_key() {
        let k1 = GroupKey::from_passphrase("key1");
        let k2 = GroupKey::from_passphrase("key2");
        let data = b"hello world";
        let tag = k1.mac(data);
        assert!(!k2.verify(data, &tag));
    }
}
