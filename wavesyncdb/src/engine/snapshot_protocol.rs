//! libp2p request-response codec for snapshot sync protocol.
//!
//! Uses length-prefixed postcard serialization for `SyncRequest` / `SyncResponse`.

use std::io;

use async_trait::async_trait;
use futures::prelude::*;
use libp2p::StreamProtocol;
use libp2p::request_response;

use crate::protocol::{SyncRequest, SyncResponse};

/// Protocol identifier for the snapshot sync protocol.
pub const SNAPSHOT_PROTOCOL: StreamProtocol = StreamProtocol::new("/wavesync/snapshot/1.2.0");

/// Codec for serializing/deserializing snapshot sync messages.
#[derive(Debug, Clone, Default)]
pub struct SnapshotCodec;

#[async_trait]
impl request_response::Codec for SnapshotCodec {
    type Protocol = StreamProtocol;
    type Request = SyncRequest;
    type Response = SyncResponse;

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
        let bytes = serde_json::to_vec(&req)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
        let bytes = serde_json::to_vec(&res)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        write_length_prefixed(io, &bytes).await
    }
}

/// Read a 4-byte big-endian length prefix followed by the payload.
async fn read_length_prefixed<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    io.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check: reject payloads > 64 MiB
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot payload too large: {len} bytes"),
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
    use crate::protocol::{SyncRequest, SyncResponse, TableSnapshot};
    use libp2p::request_response::Codec as _;

    #[tokio::test]
    async fn test_length_prefixed_roundtrip() {
        let data = b"hello world";
        let mut buf = Cursor::new(Vec::new());
        write_length_prefixed(&mut buf, data).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = read_length_prefixed(&mut reader).await.unwrap();
        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_length_prefixed_rejects_oversized() {
        let mut buf = Vec::new();
        let huge_len: u32 = 65 * 1024 * 1024; // > 64 MiB
        buf.extend_from_slice(&huge_len.to_be_bytes());
        buf.extend_from_slice(&[0u8; 16]); // some dummy data
        let mut reader = Cursor::new(buf);
        let result = read_length_prefixed(&mut reader).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_codec_request_roundtrip() {
        let mut codec = SnapshotCodec;
        let protocol = SNAPSHOT_PROTOCOL;
        let req = SyncRequest::FullSync;
        let mut buf = Cursor::new(Vec::new());
        codec.write_request(&protocol, &mut buf, req.clone()).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await.unwrap();
        // Compare by checking it's FullSync
        assert!(matches!(result, SyncRequest::FullSync));
    }

    #[tokio::test]
    async fn test_codec_response_roundtrip() {
        let mut codec = SnapshotCodec;
        let protocol = SNAPSHOT_PROTOCOL;
        let resp = SyncResponse::FullSnapshot {
            tables: vec![],
            recent_ops: vec![],
            current_hlc: 42,
        };
        let mut buf = Cursor::new(Vec::new());
        codec.write_response(&protocol, &mut buf, resp).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_response(&protocol, &mut reader).await.unwrap();
        match result {
            SyncResponse::FullSnapshot { current_hlc, .. } => assert_eq!(current_hlc, 42),
            _ => panic!("Expected FullSnapshot"),
        }
    }

    #[tokio::test]
    async fn test_codec_response_with_json_values() {
        // Verify that serde_json::Value in TableSnapshot serializes correctly
        let mut codec = SnapshotCodec;
        let protocol = SNAPSHOT_PROTOCOL;
        let resp = SyncResponse::FullSnapshot {
            tables: vec![TableSnapshot {
                table_name: "tasks".to_string(),
                columns: vec!["id".to_string(), "title".to_string()],
                rows: vec![vec![
                    serde_json::Value::String("abc".to_string()),
                    serde_json::Value::String("test task".to_string()),
                ]],
            }],
            recent_ops: vec![],
            current_hlc: 100,
        };
        let mut buf = Cursor::new(Vec::new());
        codec.write_response(&protocol, &mut buf, resp).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_response(&protocol, &mut reader).await.unwrap();
        match result {
            SyncResponse::FullSnapshot { tables, current_hlc, .. } => {
                assert_eq!(current_hlc, 100);
                assert_eq!(tables.len(), 1);
                assert_eq!(tables[0].rows.len(), 1);
            }
            _ => panic!("Expected FullSnapshot"),
        }
    }

    #[tokio::test]
    async fn test_codec_read_request_invalid_data() {
        let mut codec = SnapshotCodec;
        let protocol = SNAPSHOT_PROTOCOL;
        let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC];
        let mut buf = Cursor::new(Vec::new());
        write_length_prefixed(&mut buf, &garbage).await.unwrap();
        let written = buf.into_inner();
        let mut reader = Cursor::new(written);
        let result = codec.read_request(&protocol, &mut reader).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
