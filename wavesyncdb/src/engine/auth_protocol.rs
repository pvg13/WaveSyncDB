//! libp2p request-response codec for managed relay authentication.
//!
//! Two sub-protocols:
//! - `/wavesync/auth/challenge/1.0.0` — relay sends challenge, peer responds with signed nonce + API key
//! - `/wavesync/auth/result/1.0.0`    — relay sends accept/reject, peer acks
//!
//! Uses 4-byte **little-endian** length-prefixed JSON framing (matches the cloud relay).

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::StreamProtocol;
use libp2p::request_response;
use serde::{Deserialize, Serialize};

pub const AUTH_CHALLENGE_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/wavesync/auth/challenge/1.0.0");
pub const AUTH_RESULT_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/auth/result/1.0.0");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthChallenge {
    pub nonce: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    pub api_key: String,
    pub nonce_sig: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResult {
    pub accepted: bool,
    pub reason: Option<String>,
}

/// Codec for `/wavesync/auth/challenge/1.0.0`.
///
/// Request  = `AuthChallenge`  (relay → peer)
/// Response = `AuthResponse`   (peer → relay)
#[derive(Debug, Clone, Default)]
pub struct AuthChallengeCodec;

/// Codec for `/wavesync/auth/result/1.0.0`.
///
/// Request  = `AuthResult`  (relay → peer)
/// Response = `()`          (peer acks)
#[derive(Debug, Clone, Default)]
pub struct AuthResultCodec;

// ── helpers ──────────────────────────────────────────────────────────

/// Read a 4-byte little-endian length prefix followed by the payload.
async fn read_length_prefixed_le<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    if len > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("auth payload too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a 4-byte little-endian length prefix followed by the payload.
async fn write_length_prefixed_le<T: AsyncWrite + Unpin>(
    io: &mut T,
    data: &[u8],
) -> io::Result<()> {
    let len = data.len() as u32;
    io.write_all(&len.to_le_bytes()).await?;
    io.write_all(data).await?;
    io.flush().await?;
    Ok(())
}

// ── AuthChallengeCodec ───────────────────────────────────────────────

#[async_trait]
impl request_response::Codec for AuthChallengeCodec {
    type Protocol = StreamProtocol;
    type Request = AuthChallenge;
    type Response = AuthResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_length_prefixed_le(io).await?;
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
        let bytes = read_length_prefixed_le(io).await?;
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
        write_length_prefixed_le(io, &bytes).await
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
        write_length_prefixed_le(io, &bytes).await
    }
}

// ── AuthResultCodec ──────────────────────────────────────────────────

#[async_trait]
impl request_response::Codec for AuthResultCodec {
    type Protocol = StreamProtocol;
    type Request = AuthResult;
    type Response = ();

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let bytes = read_length_prefixed_le(io).await?;
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
        // Read and discard the empty frame
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > 0 {
            let mut buf = vec![0u8; len];
            io.read_exact(&mut buf).await?;
        }
        Ok(())
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
        write_length_prefixed_le(io, &bytes).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        _res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        // Empty ack — write zero-length frame
        io.write_all(&0u32.to_le_bytes()).await?;
        io.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    #[tokio::test]
    async fn test_challenge_codec_roundtrip() {
        let mut codec = AuthChallengeCodec;
        let protocol = AUTH_CHALLENGE_PROTOCOL;
        let req = AuthChallenge { nonce: [42u8; 32] };
        let mut buf = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut buf, req).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        assert_eq!(result.nonce, [42u8; 32]);
    }

    #[tokio::test]
    async fn test_challenge_response_roundtrip() {
        let mut codec = AuthChallengeCodec;
        let protocol = AUTH_CHALLENGE_PROTOCOL;
        let resp = AuthResponse {
            api_key: "wsc_live_test".to_string(),
            nonce_sig: vec![1, 2, 3, 4],
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_response(&protocol, &mut buf, resp)
            .await
            .unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_response(&protocol, &mut reader).await.unwrap();
        assert_eq!(result.api_key, "wsc_live_test");
        assert_eq!(result.nonce_sig, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_result_codec_roundtrip() {
        let mut codec = AuthResultCodec;
        let protocol = AUTH_RESULT_PROTOCOL;
        let req = AuthResult {
            accepted: true,
            reason: None,
        };
        let mut buf = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut buf, req).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        assert!(result.accepted);
        assert!(result.reason.is_none());
    }

    #[tokio::test]
    async fn test_result_ack_roundtrip() {
        let mut codec = AuthResultCodec;
        let protocol = AUTH_RESULT_PROTOCOL;
        let mut buf = Cursor::new(Vec::new());
        codec.write_response(&protocol, &mut buf, ()).await.unwrap();
        let written = buf.into_inner();
        // Should be exactly 4 zero bytes (LE length prefix for empty payload)
        assert_eq!(written, vec![0, 0, 0, 0]);
        let mut reader = Cursor::new(written);
        codec.read_response(&protocol, &mut reader).await.unwrap();
    }

    #[tokio::test]
    async fn test_result_rejected_roundtrip() {
        let mut codec = AuthResultCodec;
        let protocol = AUTH_RESULT_PROTOCOL;
        let req = AuthResult {
            accepted: false,
            reason: Some("invalid API key".to_string()),
        };
        let mut buf = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut buf, req).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        assert!(!result.accepted);
        assert_eq!(result.reason.as_deref(), Some("invalid API key"));
    }
}
