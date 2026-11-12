//! Phase 2 public market-data recorder.
//!
//! Strictly public data: CLOB WebSocket market channel + REST book/market
//! snapshots. No keys, no authentication, no orders. The recorder produces
//! the evidence layer (raw frames) and the verified layer (normalized book
//! states) consumed by the shadow-execution simulators.
//!
//! Module map:
//! * [`messages`] - CLOB market-channel wire types (tolerant parsing).
//! * [`book`] - snapshot+delta state machine with gap/staleness invariants.
//! * `session` (next increment) - WS lifecycle: bounded channels, backoff,
//!   reconnect with jitter, snapshot refresh after reconnect, dynamic
//!   subscription updates.

pub mod book;
pub mod messages;
pub mod session;

#[cfg(test)]
mod book_tests;

use std::time::Duration;

/// Recorder configuration (persisted alongside sessions; versioned).
#[derive(Debug, Clone)]
pub struct RecorderConfig {
    /// Public CLOB WebSocket endpoint.
    pub ws_url: String,
    /// Public CLOB REST base for book/market snapshots.
    pub rest_base: String,
    /// Staleness bound: a book older than this is flagged STALE and refused
    /// by decision paths.
    pub max_book_staleness: Duration,
    /// Reconnect backoff bounds (exponential with full jitter between).
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Bounded queue depth for raw-frame persistence (backpressure surface).
    pub raw_queue_depth: usize,
    /// Bounded queue depth for normalized states.
    pub state_queue_depth: usize,
    /// Fresh REST snapshot forced after this much time without any message
    /// for a subscribed token (belt-and-braces against silent stalls).
    pub snapshot_refresh_after: Duration,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
            rest_base: "https://clob.polymarket.com".into(),
            max_book_staleness: Duration::from_secs(10),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            raw_queue_depth: 8_192,
            state_queue_depth: 2_048,
            snapshot_refresh_after: Duration::from_secs(60),
        }
    }
}

/// Recorder errors. Network failures are retryable upstream conditions;
/// normalization never fails silently.
#[derive(Debug, thiserror::Error)]
pub enum RecorderError {
    #[error("websocket: {0}")]
    Ws(String),
    #[error("rest: {0}")]
    Rest(String),
    #[error("persistence: {0}")]
    Persist(String),
    #[error("config: {0}")]
    Config(String),
}

/// Wall-clock nanos (UTC epoch) - the common timestamp currency for all
/// recorder stamps. Elapsed-time math pairs these with monotonic clocks at
/// the call site (an Instant is captured alongside when needed).
pub fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

/// Exponential backoff with full jitter, capped (AWS-style): delay is uniform
/// over [0, min(cap, base × 2^attempt)).
pub fn backoff_delay(attempt: u32, min: Duration, max: Duration) -> Duration {
    use rand::Rng as _;
    let base_ms = min.as_millis().max(1) as u64;
    let cap_ms = max.as_millis().max(u128::from(base_ms)) as u64;
    let exp = base_ms
        .saturating_mul(2u64.saturating_pow(attempt))
        .min(cap_ms);
    let jitter = rand::thread_rng().gen_range(0..=exp);
    Duration::from_millis(jitter)
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_and_jittered() {
        let min = Duration::from_millis(500);
        let max = Duration::from_secs(30);
        for attempt in 0..20u32 {
            let d = backoff_delay(attempt, min, max);
            assert!(d <= max, "attempt {attempt}: {d:?} exceeds cap");
            assert!(d >= Duration::ZERO);
        }
        // attempt 0 stays within [0, min] (full jitter over the first step)
        for _ in 0..20 {
            assert!(backoff_delay(0, min, max) <= min);
        }
    }
}
