//! Polite pacing between upstream calls (rate-limit awareness).

use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Serializes upstream wallet-activity calls with a minimum interval.
/// Not a token bucket — deliberate pacing for a background ingestor,
/// not a latency-sensitive trader.
pub struct Pacer {
    min_interval: Duration,
    last: tokio::sync::Mutex<Option<Instant>>,
}

impl Pacer {
    pub fn new(min_interval_ms: u64) -> Self {
        Self {
            min_interval: Duration::from_millis(min_interval_ms),
            last: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn wait(&self) {
        let mut last = self.last.lock().await;
        if let Some(prev) = *last {
            let since = prev.elapsed();
            if since < self.min_interval {
                sleep(self.min_interval - since).await;
            }
        }
        *last = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn pacer_enforces_min_interval() {
        let p = Pacer::new(60);
        let t0 = Instant::now();
        p.wait().await;
        p.wait().await;
        p.wait().await;
        let elapsed = t0.elapsed();
        assert!(elapsed >= Duration::from_millis(110), "elapsed {elapsed:?}");
    }
}
