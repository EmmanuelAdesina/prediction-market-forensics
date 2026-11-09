//! `WalletFeedProvider` abstraction + the Data API v2 implementation.
//!
//! Contract (spec §6):
//! - Follow `next_cursor` while `has_more` — the *ingestor* does that;
//!   the provider fetches exactly ONE page per call.
//! - Filters must be resent identically; the provider builds the URL from
//!   `ActivityFilters` so a page walk can't silently drift.
//! - The provider returns the *actual* URL, query params, HTTP status, and the
//!   verbatim body so the ingestor can persist them forensically.
//! - HTTP timeouts, structured errors (`Retry-After` honored upstream).

use std::time::Duration;

use tokor_domain::activity::{ActivityFilters, IngestionBatch};
use tokor_domain::error::IngestionError;

#[derive(Debug, Clone)]
pub struct DataApiActivityRequest {
    pub filters: ActivityFilters,
    pub cursor: Option<String>,
}

/// The full forensic page envelope from one provider call.
#[derive(Debug, Clone)]
pub struct FetchedPage {
    pub batch: IngestionBatch,
    /// Exact URL used (for the persisted forensic record).
    pub url: String,
    /// Exact query string (URL-decoded parameter list).
    pub query_params: String,
    pub http_status: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum WalletFeedError {
    #[error("upstream error: {0}")]
    Upstream(#[from] IngestionError),
    #[error("transport failure: {0}")]
    Transport(String),
}

/// A single page fetch. Implemented by HTTP (production), fixtures (tests),
/// and mocks (fault injection).
pub trait WalletFeedProvider: Send + Sync {
    fn fetch_activity_page(
        &self,
        request: &DataApiActivityRequest,
    ) -> impl std::future::Future<Output = Result<FetchedPage, WalletFeedError>> + Send;
}

/// Retry wrapper around any provider. Polices the transient/permanent split
/// and the Retry-After contract. Callers use `fetch_with_retry`.
pub struct RetryingProvider<P: WalletFeedProvider> {
    pub inner: P,
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl<P: WalletFeedProvider> WalletFeedProvider for RetryingProvider<P> {
    async fn fetch_activity_page(
        &self,
        request: &DataApiActivityRequest,
    ) -> Result<FetchedPage, WalletFeedError> {
        self.fetch_with_retry(request).await
    }
}

impl<P: WalletFeedProvider> RetryingProvider<P> {
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            max_retries: 5,
            base_delay_ms: 400,
            max_delay_ms: 8_000,
        }
    }

    /// Exponential backoff with full jitter, capped at max_delay_ms.
    fn delay_for(&self, attempt: u32) -> u64 {
        let exp = self
            .base_delay_ms
            .saturating_mul(2u64.saturating_pow(attempt))
            .min(self.max_delay_ms);
        let jitter = {
            use rand::Rng as _;
            rand::thread_rng().gen_range(0..=exp.max(1))
        };
        jitter.min(self.max_delay_ms)
    }

    pub async fn fetch_with_retry(
        &self,
        request: &DataApiActivityRequest,
    ) -> Result<FetchedPage, WalletFeedError> {
        let mut attempt = 0u32;
        loop {
            match self.inner.fetch_activity_page(request).await {
                Ok(page) => return Ok(page),
                Err(WalletFeedError::Upstream(IngestionError::RateLimited {
                    retry_after_hint,
                    ..
                })) => {
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(WalletFeedError::Upstream(IngestionError::RateLimited {
                            retry_after_hint,
                            message: "retries exhausted".into(),
                        }));
                    }
                    let delay = self
                        .delay_for(attempt)
                        .max(retry_after_hint.unwrap_or(0).saturating_mul(1000));
                    tracing::warn!(attempt, delay_ms = delay, "rate limited; backing off");
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(WalletFeedError::Upstream(IngestionError::Transient { code, .. })) => {
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(WalletFeedError::Upstream(IngestionError::Transient {
                            code,
                            message: "retries exhausted".into(),
                        }));
                    }
                    let delay = self.delay_for(attempt);
                    tracing::warn!(
                        attempt,
                        code,
                        delay_ms = delay,
                        "transient failure; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Canonical URL construction from filters (source of truth, testable).
pub fn build_activity_url(
    base_url: &str,
    filters: &ActivityFilters,
    cursor: Option<&str>,
) -> String {
    let mut url = format!(
        "{}/activity?user={}",
        base_url.trim_end_matches('/'),
        filters.user.to_ascii_lowercase()
    );
    let types: Vec<&str> = filters
        .record_types
        .iter()
        .map(|t| activity_type_str(*t))
        .collect();
    if !types.is_empty() {
        url.push_str("&type=");
        url.push_str(&types.join("&type="));
    }
    url.push_str(&format!("&limit={}", filters.limit));
    if let Some(c) = cursor {
        // next_cursor is opaque; passed verbatim, URL-encoded for transport.
        url.push_str("&cursor=");
        url.push_str(&rfc3986_encode(c));
    }
    url
}

pub const fn activity_type_str(t: tokor_domain::activity::ActivityType) -> &'static str {
    use tokor_domain::activity::ActivityType::*;
    match t {
        Trade => "TRADE",
        Split => "SPLIT",
        Merge => "MERGE",
        Redeem => "REDEEM",
        Reward => "REWARD",
        Conversion => "CONVERSION",
    }
}

fn rfc3986_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokor_domain::activity::ActivityType;

    #[test]
    fn url_construction_resends_identical_filters() {
        let f = ActivityFilters {
            user: "0xA19cBababc312f9df185e49d7004c249ed1ade6b".into(),
            record_types: vec![ActivityType::Trade],
            limit: 500,
        };
        let url = build_activity_url(
            "https://data-api.polymarket.com/v2",
            &f,
            Some("abc-def_1.~"),
        );
        assert!(url.starts_with("https://data-api.polymarket.com/v2/activity?user=0xa19c"));
        assert!(url.contains("&type=TRADE"));
        assert!(url.contains("&limit=500"));
        assert!(url.contains("&cursor=abc-def_1.~"));
    }

    #[test]
    fn cursor_url_encoding_is_rfc3986_safe() {
        let f = ActivityFilters {
            user: "0xa19cbababc312f9df185e49d7004c249ed1ade6b".into(),
            record_types: vec![],
            limit: 100,
        };
        let url = build_activity_url("https://data-api.polymarket.com/v2", &f, Some("a b+?&="));
        assert!(url.contains("a%20b%2B%3F%26%3D"), "url: {url}");
    }
}
