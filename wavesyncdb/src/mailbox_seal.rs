//! End-to-end payload sealing for the relay mailbox.
//!
//! The relay stores mailbox entries durably so changesets survive windows
//! where no recipient is online. The relay operator has full database access,
//! so entries are sealed client-side before they ever leave the device:
//! XChaCha20-Poly1305 under a key derived from the group key
//! ([`crate::auth::GroupKey::mailbox_key`]). The relay holds only ciphertext
//! plus routing metadata (derived topic, sequence number, timestamps, sizes).
//!
//! Construction:
//! - **Key**: BLAKE3 derive_key sub-key of the group key (context
//!   `wavesyncdb-mailbox-v1`) — domain-separated from topic derivation and
//!   from message MACs.
//! - **Nonce**: 24 random bytes per message. The 192-bit space makes fresh
//!   random nonces safe without any counter bookkeeping across devices.
//! - **AAD**: `"wavesync-mailbox-v1:" + effective_topic`. An entry copied by
//!   the relay into a different topic's log fails authentication on open.
//!   The relay-assigned sequence number is deliberately NOT in the AAD: the
//!   sender seals before the relay assigns it, and in-topic reordering or
//!   duplication is harmless anyway (changeset application is idempotent and
//!   commutative via the per-column CRDT). Withholding is covered by the
//!   version-vector reconcile fallback, not by cryptography.
//! - **Authenticity**: opening with the group-derived key is the mailbox
//!   path's equivalent of the HMAC verification mandated on every message
//!   path — AEAD subsumes it (same key-derivation root, drop-on-failure).
//!   Do not add a separate HMAC on top.
//!
//! Explicit non-goal: forward secrecy. The group key is static, so a later
//! passphrase compromise decrypts any mailbox history still retained at the
//! relay. This matches the trust model of the rest of the protocol (HMAC and
//! topic derivation share the same root secret).

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};

/// Length of the XChaCha20-Poly1305 nonce in bytes.
pub(crate) const NONCE_LEN: usize = 24;

/// AAD domain prefix. Frozen: changing it orphans every sealed entry.
const AAD_PREFIX: &str = "wavesync-mailbox-v1:";

/// A sealed mailbox entry: fresh random nonce plus ciphertext (which includes
/// the 16-byte Poly1305 tag).
pub(crate) struct SealedEntry {
    pub nonce: [u8; NONCE_LEN],
    pub ciphertext: Vec<u8>,
}

/// Errors from sealing or opening a mailbox entry.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SealError {
    /// Encryption failed (only possible on pathological plaintext sizes).
    #[error("mailbox seal failed")]
    Seal,
    /// Authentication failed: tampered ciphertext, wrong key (non-member),
    /// or an entry moved across topics (AAD mismatch). Indistinguishable by
    /// design — the payload must be treated as unauthenticated garbage.
    #[error("mailbox entry failed authentication")]
    Open,
}

fn aad_for(effective_topic: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_PREFIX.len() + effective_topic.len());
    aad.extend_from_slice(AAD_PREFIX.as_bytes());
    aad.extend_from_slice(effective_topic.as_bytes());
    aad
}

/// Seal a serialized changeset for the mailbox of `effective_topic`.
pub(crate) fn seal(
    mailbox_key: &[u8; 32],
    effective_topic: &str,
    plaintext: &[u8],
) -> Result<SealedEntry, SealError> {
    let cipher = XChaCha20Poly1305::new(mailbox_key.into());
    let mut nonce = [0u8; NONCE_LEN];
    // getrandom-backed OS entropy; the wasm32 `js` backend is configured in
    // Cargo.toml so this is target-independent.
    chacha20poly1305::aead::rand_core::RngCore::fill_bytes(
        &mut chacha20poly1305::aead::OsRng,
        &mut nonce,
    );
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad_for(effective_topic),
            },
        )
        .map_err(|_| SealError::Seal)?;
    Ok(SealedEntry { nonce, ciphertext })
}

/// Open a sealed mailbox entry. Fails if the ciphertext was tampered with,
/// sealed under a different group's key, or sealed for a different topic.
pub(crate) fn open(
    mailbox_key: &[u8; 32],
    effective_topic: &str,
    nonce: &[u8; NONCE_LEN],
    ciphertext: &[u8],
) -> Result<Vec<u8>, SealError> {
    let cipher = XChaCha20Poly1305::new(mailbox_key.into());
    cipher
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: &aad_for(effective_topic),
            },
        )
        .map_err(|_| SealError::Open)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::GroupKey;

    fn key() -> [u8; 32] {
        GroupKey::from_raw([7u8; 32]).mailbox_key()
    }

    #[test]
    fn roundtrip() {
        let k = key();
        let sealed = seal(&k, "wavesync2-abc", b"changeset bytes").unwrap();
        let opened = open(&k, "wavesync2-abc", &sealed.nonce, &sealed.ciphertext).unwrap();
        assert_eq!(opened, b"changeset bytes");
    }

    #[test]
    fn fresh_nonce_per_seal() {
        let k = key();
        let a = seal(&k, "t", b"same").unwrap();
        let b = seal(&k, "t", b"same").unwrap();
        assert_ne!(a.nonce, b.nonce);
        assert_ne!(a.ciphertext, b.ciphertext);
    }

    #[test]
    fn tampered_ciphertext_rejected() {
        let k = key();
        let mut sealed = seal(&k, "t", b"payload").unwrap();
        sealed.ciphertext[0] ^= 0x01;
        assert!(open(&k, "t", &sealed.nonce, &sealed.ciphertext).is_err());
    }

    #[test]
    fn cross_topic_move_rejected() {
        // A relay copying an entry into another topic's log must fail AEAD.
        let k = key();
        let sealed = seal(&k, "wavesync2-aaa", b"payload").unwrap();
        assert!(open(&k, "wavesync2-bbb", &sealed.nonce, &sealed.ciphertext).is_err());
    }

    #[test]
    fn non_member_key_rejected() {
        let sealed = seal(&key(), "t", b"payload").unwrap();
        let other = GroupKey::from_raw([8u8; 32]).mailbox_key();
        assert!(open(&other, "t", &sealed.nonce, &sealed.ciphertext).is_err());
    }

    #[test]
    fn mailbox_key_derivation_is_frozen() {
        // Pins the derive_key context string and construction. If this test
        // breaks, existing peers can no longer open each other's mailbox
        // entries — that is a wire-protocol break, not a refactor.
        let k = GroupKey::from_raw([0u8; 32]).mailbox_key();
        let mut hasher = blake3::Hasher::new_derive_key("wavesyncdb-mailbox-v1");
        hasher.update(&[0u8; 32]);
        assert_eq!(k, *hasher.finalize().as_bytes());
        // And it must differ from every other sub-key domain of the same root.
        let group = GroupKey::from_raw([0u8; 32]);
        assert_ne!(&k[..], &group.mac(b"")[..]);
    }
}
