//! Wire-level libp2p codecs shared by the native engine (`crate::engine`)
//! and the browser engine (`crate::web_engine`).
//!
//! These live outside `engine/` because `engine/*` is gated to
//! `not(target_arch = "wasm32")` while the browser engine speaks the very
//! same protocols. A single definition is what guarantees the two targets
//! can never drift apart on protocol identifiers or framing — the browser
//! build once shipped a stale snapshot protocol id from a hand-kept copy,
//! which silently broke web↔native negotiation.
//!
//! Framing invariant: snapshot and push use a **big-endian** 4-byte length
//! prefix + serde_json. (The native-only auth handshake uses little-endian
//! and stays in `engine/auth_protocol.rs`.)

pub mod push_protocol;
pub mod snapshot_protocol;

/// Identify protocol/agent version string, advertised by both the native
/// and browser behaviours. Must match between peers.
pub const IDENTIFY_PROTOCOL_VERSION: &str = "/wavesync/2.0.0";
