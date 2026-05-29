//! Backoff schedule and timing math for the persistent retry queue.
//!
//! Pure logic — no I/O, no SQLite. Lives in its own file so the schedule
//! is unit-testable without spinning up a database. The queue (commit
//! B4) and worker (commit B5) consume this module.
//!
//! # Schedule
//!
//! Fixed table of delays indexed by `attempts` (1-based — 1 = first
//! retry queued). After exhausting the table, the row is dropped:
//!
//! | Attempt | Base delay |
//! |---------|------------|
//! | 1       | 30 s       |
//! | 2       | 60 s       |
//! | 3       | 2 m        |
//! | 4       | 5 m        |
//! | 5       | 15 m       |
//! | 6       | 1 h        |
//! | 7       | 4 h        |
//!
//! Plus ±20% jitter on every delay to break thundering-herd after a
//! provider outage (Matrix synapse #5414 was caused by zero jitter
//! — even a small jitter fixes it).
//!
//! Total max wall-clock spread before drop: ~5h45m. We additionally
//! cap at 24h via `MAX_AGE_SECS` so a row missed by a long downtime
//! doesn't retry forever after the relay recovers.
//!
//! # Retry-After interaction
//!
//! If the provider sent a `Retry-After` header (FCM and APNs do this on
//! 429), it overrides the table value for the next attempt — but never
//! below the table value. Providers under load sometimes underreport
//! Retry-After to push load downstream; we don't trust them to set it
//! honestly.

use std::time::Duration;

/// Base delays in seconds, one per attempt. Length = max attempts.
pub const RETRY_DELAYS_SECS: &[u64] = &[30, 60, 120, 300, 900, 3600, 14400];

/// Maximum number of retry attempts before dropping the row.
pub const MAX_ATTEMPTS: u32 = RETRY_DELAYS_SECS.len() as u32;

/// Hard wall-clock ceiling on retry lifetime, regardless of attempt
/// count. Matches Matrix's default `max_retry_period` (24 h).
pub const MAX_AGE_SECS: i64 = 86_400;

/// Fraction by which the base delay is jittered (both up and down).
const JITTER_FRACTION: f64 = 0.20;

/// Compute the delay before the `attempts`-th retry.
///
/// - `attempts` is 1-based. `attempts == 1` means "first retry after
///   the original send failed."
/// - `retry_after`, if present, overrides the schedule but never lowers
///   it below the base value (see module docs).
/// - Returns `None` when `attempts > MAX_ATTEMPTS`, signalling that the
///   retry budget is exhausted and the row should be dropped.
pub fn compute_delay(attempts: u32, retry_after: Option<Duration>) -> Option<Duration> {
    if attempts == 0 || attempts > MAX_ATTEMPTS {
        return None;
    }
    let base_secs = RETRY_DELAYS_SECS[(attempts - 1) as usize] as f64;
    // Symmetric jitter in [-JITTER_FRACTION, +JITTER_FRACTION].
    // (rand::random::<f64>() yields uniform [0, 1), so 2x - 1 yields
    // uniform (-1, 1].)
    let jitter = (rand::random::<f64>() * 2.0 - 1.0) * JITTER_FRACTION * base_secs;
    let jittered_secs = (base_secs + jitter).max(0.5);
    let scheduled = Duration::from_secs_f64(jittered_secs);

    match retry_after {
        // Provider asked for at least `ra`. Take the larger of `ra` and
        // the (jittered) base — never go below the table floor.
        Some(ra) => Some(scheduled.max(ra)),
        None => Some(scheduled),
    }
}

/// True if a retry row's first failure was more than `MAX_AGE_SECS`
/// ago — caller should drop the row rather than schedule another
/// attempt.
pub fn age_exceeded(first_failed_at: i64, now: i64) -> bool {
    now.saturating_sub(first_failed_at) >= MAX_AGE_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_attempt_is_around_30s() {
        // 1000 samples — assert all land within the documented jitter band.
        for _ in 0..1000 {
            let d = compute_delay(1, None).expect("attempt 1 is valid");
            let secs = d.as_secs_f64();
            assert!(
                (24.0..=36.0).contains(&secs),
                "expected 24-36s (30 ±20%), got {secs}"
            );
        }
    }

    #[test]
    fn schedule_matches_table() {
        for (i, &base) in RETRY_DELAYS_SECS.iter().enumerate() {
            let attempts = (i + 1) as u32;
            let d = compute_delay(attempts, None).unwrap();
            let secs = d.as_secs_f64();
            let lower = (base as f64) * (1.0 - JITTER_FRACTION);
            let upper = (base as f64) * (1.0 + JITTER_FRACTION);
            assert!(
                (lower..=upper).contains(&secs),
                "attempt {attempts}: base={base}, expected {lower}..={upper}, got {secs}",
            );
        }
    }

    #[test]
    fn over_max_attempts_returns_none() {
        assert!(compute_delay(MAX_ATTEMPTS + 1, None).is_none());
        assert!(compute_delay(0, None).is_none());
    }

    #[test]
    fn retry_after_above_table_wins() {
        // Provider asks for 2 minutes; attempt 1's table value is 30s.
        // The 2-minute value should win.
        let d = compute_delay(1, Some(Duration::from_secs(120))).unwrap();
        assert!(d >= Duration::from_secs(120));
    }

    #[test]
    fn retry_after_below_table_floor_does_not_win() {
        // Provider asks for 5s; attempt 2's table value is 60s.
        // The table floor (with jitter) should win.
        for _ in 0..100 {
            let d = compute_delay(2, Some(Duration::from_secs(5))).unwrap();
            assert!(
                d.as_secs() >= 48,
                "table floor (60s -20% = 48s) should beat retry_after=5s, got {d:?}"
            );
        }
    }

    #[test]
    fn jitter_spreads_results() {
        // Take 1000 samples of attempt 1; assert the spread is at least
        // half the documented jitter band. Catches "jitter is always
        // zero" regressions.
        let samples: Vec<f64> = (0..1000)
            .map(|_| compute_delay(1, None).unwrap().as_secs_f64())
            .collect();
        let min = samples.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = samples.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let band_width = max - min;
        assert!(
            band_width >= 6.0,
            "1000 samples should span ≥6s; got {band_width}s (min={min}, max={max})"
        );
    }

    #[test]
    fn age_exceeded_triggers_at_24h() {
        assert!(!age_exceeded(0, 86_399));
        assert!(age_exceeded(0, 86_400));
        assert!(age_exceeded(0, 90_000));
    }

    #[test]
    fn age_exceeded_handles_negative_skew() {
        // If wall-clock jumps backward, now - first_failed_at could be
        // negative. `saturating_sub` clamps to 0; we should not panic
        // and should treat the row as not-yet-expired.
        assert!(!age_exceeded(1000, 500));
    }
}
