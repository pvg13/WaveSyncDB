//! libp2p request-response codec for the relay mailbox protocol.
//!
//! The mailbox is the relay's durable, end-to-end-encrypted store-and-forward
//! log: writers append sealed changesets (`mailbox_seal`), readers fetch
//! entries after a locally persisted cursor. The relay assigns the per-topic
//! monotonic sequence number — clients are multi-writer and cannot.
//!
//! Uses length-prefixed serde_json serialization for `MailboxRequest` /
//! `MailboxResponse` (4-byte big-endian prefix, matching the push and
//! snapshot protocols). Sealed payloads travel base64-encoded — serde_json
//! would otherwise emit `Vec<u8>` as an array of numbers at ~3.7x the size.
//!
//! ## Cursor / gap contract
//!
//! Sequence numbers start at 1 and are per-topic. Every response carries:
//! - `first_retained_seq`: the oldest entry still served for the topic
//!   (equals the next unassigned seq when the log is empty),
//! - `latest_seq`: the newest assigned seq (0 when none was ever assigned),
//! - `epoch`: a random value minted when the relay first creates the topic's
//!   log. A client stores `(cursor, epoch)` together. Epoch mismatch means
//!   the relay's log was reset (store wiped/replaced) and the cursor is
//!   meaningless — without it, a fresh log would silently look "caught up"
//!   to a client holding a high cursor.
//!
//! A client must fall back to the version-vector reconcile when any of:
//! `after_seq + 1 < first_retained_seq` (entries aged/evicted unseen),
//! `epoch != stored_epoch`, or `after_seq > latest_seq` (see
//! [`fetch_gap_detected`]). The mailbox is an additive durability layer;
//! convergence never depends on it alone.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::StreamProtocol;
use libp2p::request_response;
use serde::{Deserialize, Serialize};

/// Protocol identifier for the mailbox protocol.
pub const MAILBOX_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/mailbox/1.0.0");

/// Max frame size. Entries are single sealed changesets and fetches self-limit
/// via `max_bytes`, so this is a hard sanity ceiling, not a tuning knob —
/// between the push protocol's 1 MiB and the snapshot protocol's 64 MiB.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Request sent by a peer to the relay's mailbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MailboxRequest {
    /// Append one sealed changeset to the topic's log. The relay assigns and
    /// returns the sequence number; the append is acknowledged only after the
    /// entry is durably committed (fsynced).
    Append {
        topic: String,
        /// 24-byte XChaCha20-Poly1305 nonce, base64.
        nonce: String,
        /// Sealed changeset (ciphertext + tag), base64.
        ciphertext: String,
    },
    /// Fetch entries with `seq > after_seq`, oldest first, bounded by
    /// `max_entries` and `max_bytes` (of ciphertext). Response:
    /// `MailboxResponse::Entries`.
    Fetch {
        topic: String,
        after_seq: u64,
        max_entries: u32,
        max_bytes: u64,
    },
}

/// One stored mailbox entry as returned by `Fetch`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxEntry {
    pub seq: u64,
    /// 24-byte nonce, base64.
    pub nonce: String,
    /// Sealed changeset, base64.
    pub ciphertext: String,
}

/// Why the relay refused a mailbox request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxErrorKind {
    /// The relay has no mailbox store configured.
    Disabled,
    /// The entry exceeds the relay's per-entry size cap.
    TooLarge,
    /// A storage quota (global byte cap) is exhausted.
    QuotaExceeded,
    /// The sender exceeded its append rate limit; retry later.
    RateLimited,
    /// Relay-side storage failure.
    Internal,
}

/// Response from the relay's mailbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MailboxResponse {
    /// The entry is durably stored under `seq`.
    Appended { seq: u64, epoch: u64 },
    /// Entries after the requested cursor, oldest first. `truncated` means
    /// more entries remain past the `max_entries`/`max_bytes` bounds — issue
    /// another `Fetch` from the last returned seq.
    Entries {
        entries: Vec<MailboxEntry>,
        latest_seq: u64,
        first_retained_seq: u64,
        epoch: u64,
        truncated: bool,
    },
    Error {
        kind: MailboxErrorKind,
        message: String,
    },
}

/// The client-side gap predicate for a `Fetch` issued with `after_seq`
/// against an `Entries` response. `stored_epoch` is `None` on a topic's
/// first-ever drain (no gap — adopt the epoch). `true` means entries were
/// lost to the client (aged out, evicted, or the relay log was reset):
/// fall back to the version-vector reconcile, reset the cursor to
/// `first_retained_seq - 1`, and adopt the response's epoch.
pub fn fetch_gap_detected(
    after_seq: u64,
    stored_epoch: Option<u64>,
    first_retained_seq: u64,
    latest_seq: u64,
    epoch: u64,
) -> bool {
    if stored_epoch.is_some_and(|e| e != epoch) {
        return true;
    }
    // Entries in (after_seq, first_retained_seq) existed but are gone.
    if after_seq + 1 < first_retained_seq {
        return true;
    }
    // Cursor beyond the newest assigned seq: the log restarted but happens to
    // share the epoch slot shape (defensive; epoch normally catches this).
    if after_seq > latest_seq {
        return true;
    }
    false
}

/// Base64 helpers shared by every mailbox call site (engine, web, tests) so
/// the wire encoding of sealed bytes has exactly one definition.
pub mod b64 {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    pub fn encode(bytes: &[u8]) -> String {
        STANDARD.encode(bytes)
    }

    pub fn decode(s: &str) -> Option<Vec<u8>> {
        STANDARD.decode(s).ok()
    }
}

/// Codec for serializing/deserializing mailbox messages.
#[derive(Debug, Clone, Default)]
pub struct MailboxCodec;

#[async_trait]
impl request_response::Codec for MailboxCodec {
    type Protocol = StreamProtocol;
    type Request = MailboxRequest;
    type Response = MailboxResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_length_prefixed(io).await?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_length_prefixed(io).await?;
        serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes =
            serde_json::to_vec(&req).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_length_prefixed(io, &bytes).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let bytes =
            serde_json::to_vec(&res).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_length_prefixed(io, &bytes).await
    }
}

/// Read a 4-byte big-endian length prefix followed by the payload.
async fn read_length_prefixed<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("mailbox payload too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a 4-byte big-endian length prefix followed by the payload.
async fn write_length_prefixed<T: AsyncWrite + Unpin>(io: &mut T, data: &[u8]) -> io::Result<()> {
    let len = u32::try_from(data.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "payload too large for u32 length prefix: {} bytes",
                data.len()
            ),
        )
    })?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(data).await?;
    io.flush().await?;
    Ok(())
}

// `#[tokio::test]` needs native tokio; see push_protocol.rs for why this
// module must not compile under wasm32.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    #[tokio::test]
    async fn codec_append_roundtrip() {
        let mut codec = MailboxCodec;
        let req = MailboxRequest::Append {
            topic: "wavesync2-abc".into(),
            nonce: b64::encode(&[9u8; 24]),
            ciphertext: b64::encode(b"sealed bytes"),
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_request(&MAILBOX_PROTOCOL, &mut buf, req)
            .await
            .unwrap();
        let mut reader = Cursor::new(buf.into_inner());
        match codec.read_request(&MAILBOX_PROTOCOL, &mut reader).await {
            Ok(MailboxRequest::Append {
                topic,
                nonce,
                ciphertext,
            }) => {
                assert_eq!(topic, "wavesync2-abc");
                assert_eq!(b64::decode(&nonce).unwrap(), vec![9u8; 24]);
                assert_eq!(b64::decode(&ciphertext).unwrap(), b"sealed bytes");
            }
            other => panic!("expected Append, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn codec_entries_roundtrip() {
        let mut codec = MailboxCodec;
        let res = MailboxResponse::Entries {
            entries: vec![MailboxEntry {
                seq: 7,
                nonce: b64::encode(&[1u8; 24]),
                ciphertext: b64::encode(b"ct"),
            }],
            latest_seq: 9,
            first_retained_seq: 3,
            epoch: 42,
            truncated: true,
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_response(&MAILBOX_PROTOCOL, &mut buf, res)
            .await
            .unwrap();
        let mut reader = Cursor::new(buf.into_inner());
        match codec.read_response(&MAILBOX_PROTOCOL, &mut reader).await {
            Ok(MailboxResponse::Entries {
                entries,
                latest_seq,
                first_retained_seq,
                epoch,
                truncated,
            }) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].seq, 7);
                assert_eq!(latest_seq, 9);
                assert_eq!(first_retained_seq, 3);
                assert_eq!(epoch, 42);
                assert!(truncated);
            }
            other => panic!("expected Entries, got {other:?}"),
        }
    }

    #[test]
    fn gap_predicate() {
        // Fresh topic, fresh client: no gap.
        assert!(!fetch_gap_detected(0, None, 1, 0, 1));
        // Caught-up client, no new entries: no gap.
        assert!(!fetch_gap_detected(5, Some(1), 3, 5, 1));
        // Normal catch-up from within the retained window: no gap.
        assert!(!fetch_gap_detected(3, Some(1), 3, 9, 1));
        // Cursor exactly at the retention edge: no gap.
        assert!(!fetch_gap_detected(2, Some(1), 3, 9, 1));
        // Entries aged out below the cursor's continuation: gap.
        assert!(fetch_gap_detected(1, Some(1), 3, 9, 1));
        // Epoch mismatch (relay log reset): gap, even if seqs look fine.
        assert!(fetch_gap_detected(3, Some(1), 1, 9, 2));
        // First-ever drain adopts whatever epoch it sees: no gap.
        assert!(!fetch_gap_detected(0, None, 1, 9, 2));
        // Cursor beyond latest (reset without epoch change caught): gap.
        assert!(fetch_gap_detected(10, Some(1), 1, 4, 1));
    }
}
