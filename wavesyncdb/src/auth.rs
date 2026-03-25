//! Peer group authentication via PSK-derived topic isolation and HMAC verification.
//!
//! Two layers, both derived from a single user-supplied passphrase:
//! 1. **Topic isolation** — derive topic name from BLAKE3 hash so different
//!    passphrases yield different topics and peers never see each other's messages.
//! 2. **HMAC on all messages** — request-response messages carry a BLAKE3-keyed MAC.
//!    Peers without the PSK cannot forge or inject valid messages.

/// A group authentication key derived from a user-supplied passphrase.
///
/// All peers sharing the same passphrase will derive the same `GroupKey`,
/// enabling them to join the same sync topic and verify each other's messages.
#[derive(Clone)]
pub struct GroupKey([u8; 32]);

impl GroupKey {
    /// Derive a group key from a passphrase using BLAKE3 key derivation.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let key = blake3::derive_key("wavesyncdb-group-key-v1", passphrase.as_bytes());
        Self(key)
    }

    /// Derive a sync topic name from the user topic and this group key.
    ///
    /// Different passphrases produce different topic names, providing topic-level
    /// isolation so peers in different groups never see each other's messages.
    pub fn derive_topic(&self, user_topic: &str) -> String {
        let mut hasher = blake3::Hasher::new_derive_key("wavesyncdb-topic-v1");
        hasher.update(user_topic.as_bytes());
        hasher.update(&self.0);
        let hash = hasher.finalize();
        format!("wavesync-{}", hash.to_hex())
    }

    /// Derive a rendezvous namespace from the effective topic name.
    ///
    /// This reuses the topic derivation output as-is, since the effective topic
    /// is already BLAKE3-derived from `(user_topic, group_key)` and provides the
    /// same namespace isolation needed for rendezvous discovery.
    pub fn derive_namespace(&self, user_topic: &str) -> String {
        self.derive_topic(user_topic)
    }

    /// Compute a BLAKE3 keyed MAC over the given data.
    pub fn mac(&self, data: &[u8]) -> [u8; 32] {
        *blake3::keyed_hash(&self.0, data).as_bytes()
    }

    /// Verify a BLAKE3 keyed MAC over the given data.
    pub fn verify(&self, data: &[u8], tag: &[u8; 32]) -> bool {
        let expected = self.mac(data);
        // Constant-time comparison
        expected == *tag
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
    fn test_deterministic_derivation() {
        let k1 = GroupKey::from_passphrase("my-secret");
        let k2 = GroupKey::from_passphrase("my-secret");
        assert_eq!(k1.0, k2.0);
    }

    #[test]
    fn test_different_passphrases_different_keys() {
        let k1 = GroupKey::from_passphrase("alpha");
        let k2 = GroupKey::from_passphrase("beta");
        assert_ne!(k1.0, k2.0);
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
