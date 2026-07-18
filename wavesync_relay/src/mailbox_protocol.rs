//! libp2p request-response codec for the relay mailbox protocol.
//!
//! Mirrors `wavesyncdb/src/wire/mailbox_protocol.rs` — kept separate to avoid
//! coupling the relay binary to the full `wavesyncdb` crate.
//!
//! Kept in lockstep with wavesyncdb/src/wire/mailbox_protocol.rs (single
//! source there for both wavesyncdb targets). The client-side gap predicate
//! (`fetch_gap_detected`) lives only in the wavesyncdb copy — the relay
//! never evaluates gaps, it only reports `first_retained_seq`/`epoch`.

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

/// Base64 helpers matching the wavesyncdb copy — the wire encoding of sealed
/// bytes has exactly one definition per crate, kept in lockstep.
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
