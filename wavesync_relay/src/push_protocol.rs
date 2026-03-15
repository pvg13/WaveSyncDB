//! libp2p request-response codec for push notification protocol.
//!
//! Mirrors `wavesyncdb/src/engine/push_protocol.rs` — kept separate to avoid
//! coupling the relay binary to the full `wavesyncdb` crate.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::StreamProtocol;
use libp2p::request_response;
use serde::{Deserialize, Serialize};

/// Protocol identifier for the push notification protocol.
pub const PUSH_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/push/1.0.0");

/// Push notification platform.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PushPlatform {
    Fcm,
    Apns,
}

/// Request sent to the relay for push notification management.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PushRequest {
    RegisterToken {
        topic: String,
        platform: PushPlatform,
        token: String,
    },
    UnregisterToken {
        topic: String,
        token: String,
    },
    NotifyTopic {
        topic: String,
        sender_site_id: String,
    },
}

/// Response from the relay for push notification requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PushResponse {
    Ok,
    Error { message: String },
}

/// Codec for serializing/deserializing push notification messages.
#[derive(Debug, Clone, Default)]
pub struct PushCodec;

#[async_trait]
impl request_response::Codec for PushCodec {
    type Protocol = StreamProtocol;
    type Request = PushRequest;
    type Response = PushResponse;

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

    if len > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("push payload too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a 4-byte big-endian length prefix followed by the payload.
async fn write_length_prefixed<T: AsyncWrite + Unpin>(io: &mut T, data: &[u8]) -> io::Result<()> {
    let len = data.len() as u32;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(data).await?;
    io.flush().await?;
    Ok(())
}
