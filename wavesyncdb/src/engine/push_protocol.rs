//! libp2p request-response codec for push notification protocol.
//!
//! Uses length-prefixed serde_json serialization for `PushRequest` / `PushResponse`.

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

/// Request sent to the relay for push notification management, or from the
/// relay to a peer for presence notifications.
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
    /// Sent by a peer on relay connect to announce presence for a topic
    /// and receive the list of other peers currently registered on that
    /// topic. Response: `PushResponse::PeerList`.
    AnnouncePresence {
        topic: String,
    },
    /// Sent by the relay to each existing peer on a topic when a new peer
    /// announces presence. The receiver dials the addresses so the normal
    /// sync flow can start. Response: `PushResponse::Ok`.
    PeerJoined {
        topic: String,
        peer_addrs: Vec<String>,
    },
}

/// Response from the relay (or from a peer responding to a relay-pushed
/// `PeerJoined`) for push notification requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PushResponse {
    Ok,
    Error {
        message: String,
    },
    /// Response to `AnnouncePresence` — the circuit multiaddrs of other
    /// peers on the same topic. Each entry is a fully-qualified multiaddr
    /// with a trailing `/p2p/<peer-id>` component.
    PeerList {
        peers: Vec<String>,
    },
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

    // Sanity check: reject payloads > 1 MiB (push messages are small)
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    #[tokio::test]
    async fn test_codec_request_roundtrip_register() {
        let mut codec = PushCodec;
        let protocol = PUSH_PROTOCOL;
        let req = PushRequest::RegisterToken {
            topic: "abc123".to_string(),
            platform: PushPlatform::Fcm,
            token: "fcm-token-xyz".to_string(),
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_request(&protocol, &mut buf, req.clone())
            .await
            .unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        match result {
            PushRequest::RegisterToken {
                topic,
                platform,
                token,
            } => {
                assert_eq!(topic, "abc123");
                assert_eq!(platform, PushPlatform::Fcm);
                assert_eq!(token, "fcm-token-xyz");
            }
            _ => panic!("Expected RegisterToken"),
        }
    }

    #[tokio::test]
    async fn test_codec_request_roundtrip_notify() {
        let mut codec = PushCodec;
        let protocol = PUSH_PROTOCOL;
        let req = PushRequest::NotifyTopic {
            topic: "topic-hash".to_string(),
            sender_site_id: "site-abc".to_string(),
        };
        let mut buf = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut buf, req).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        match result {
            PushRequest::NotifyTopic {
                topic,
                sender_site_id,
            } => {
                assert_eq!(topic, "topic-hash");
                assert_eq!(sender_site_id, "site-abc");
            }
            _ => panic!("Expected NotifyTopic"),
        }
    }

    #[tokio::test]
    async fn test_codec_response_roundtrip() {
        let mut codec = PushCodec;
        let protocol = PUSH_PROTOCOL;
        let resp = PushResponse::Ok;
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_response(&protocol, &mut buf, resp)
            .await
            .unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_response(&protocol, &mut reader).await.unwrap();
        assert!(matches!(result, PushResponse::Ok));
    }

    #[tokio::test]
    async fn test_codec_response_error_roundtrip() {
        let mut codec = PushCodec;
        let protocol = PUSH_PROTOCOL;
        let resp = PushResponse::Error {
            message: "token expired".to_string(),
        };
        let mut buf = Cursor::new(Vec::new());
        codec
            .write_response(&protocol, &mut buf, resp)
            .await
            .unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_response(&protocol, &mut reader).await.unwrap();
        match result {
            PushResponse::Error { message } => assert_eq!(message, "token expired"),
            _ => panic!("Expected Error"),
        }
    }
}
