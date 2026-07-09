//! Time-boxed peer rejection with exponential backoff, shared by the native
//! engine (per-(group, peer), `tokio::time::Instant`) and the browser engine
//! (single group, epoch-ms `f64` — `std::time::Instant` panics on wasm).
//!
//! A peer that fails HMAC verification for a topic we hold is skipped
//! (dialing / sync / fan-out) while its backoff window is open, gets exactly
//! one re-evaluation after the window expires, and is fully readmitted on a
//! later successful verify. The window doubles per consecutive failure and
//! caps at one hour — bounding how often a persistently-mismatching
//! (spoofed-topic) peer is re-evaluated while letting a transiently
//! misconfigured peer recover without a restart.

use std::time::Duration;

/// Per-peer rejection backoff state, generic over the clock representation.
#[derive(Debug, Clone)]
pub(crate) struct RejectionState<I> {
    /// Consecutive rejections for this peer (1-based).
    pub(crate) attempts: u32,
    /// The peer is skipped until this instant; after it, one re-evaluation.
    pub(crate) until: I,
}

impl<I: Copy + PartialOrd> RejectionState<I> {
    /// True while the backoff window is open (peer must be skipped). An
    /// expired entry returns `false` so the peer gets one re-evaluation.
    pub(crate) fn is_active(&self, now: I) -> bool {
        self.until > now
    }
}

/// Exponential rejection backoff: 30s, 60s, 120s, … capped at 1 hour.
pub(crate) fn rejection_backoff(attempts: u32) -> Duration {
    const BASE_SECS: u64 = 30;
    const MAX_SECS: u64 = 3600;
    let shift = attempts.saturating_sub(1).min(20);
    let secs = BASE_SECS.saturating_mul(1u64 << shift).min(MAX_SECS);
    Duration::from_secs(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejection_backoff_grows_exponentially_and_caps() {
        assert_eq!(rejection_backoff(0), Duration::from_secs(30));
        assert_eq!(rejection_backoff(1), Duration::from_secs(30));
        assert_eq!(rejection_backoff(2), Duration::from_secs(60));
        assert_eq!(rejection_backoff(3), Duration::from_secs(120));
        assert_eq!(rejection_backoff(4), Duration::from_secs(240));
        assert_eq!(rejection_backoff(7), Duration::from_secs(1920));
        assert_eq!(rejection_backoff(8), Duration::from_secs(3600));
        assert_eq!(rejection_backoff(1000), Duration::from_secs(3600));
    }

    #[test]
    fn is_active_window_semantics() {
        let r = RejectionState {
            attempts: 1,
            until: 100.0_f64,
        };
        assert!(r.is_active(99.9));
        assert!(!r.is_active(100.0)); // boundary: expired == eligible for re-eval
        assert!(!r.is_active(100.1));
    }
}
