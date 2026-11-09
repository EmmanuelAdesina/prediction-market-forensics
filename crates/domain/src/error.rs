use thiserror::Error;

/// Structured errors for the ingestion pipeline.
#[derive(Debug, Error)]
pub enum IngestionError {
    /// The upstream returned a transient response we may retry.
    #[error("transient upstream error {code}: {message}")]
    Transient { code: u16, message: String },

    /// The upstream explicitly asked us to back off.
    #[error("rate limited (Retry-After {retry_after_hint:?}s): {message}")]
    RateLimited {
        retry_after_hint: Option<u64>,
        message: String,
    },

    /// The upstream rejected the request deterministically; retrying is pointless.
    #[error("permanent upstream error {code}: {message}")]
    Permanent { code: u16, message: String },

    /// A human must intervene before we resume; the operator should be informed.
    #[error("manual review required: {0}")]
    ManualReview(String),

    /// The stream is inconsistent with itself (e.g. cursor bounced backwards
    /// beyond tolerance, or a page claims has_more while the cursor is unchanged).
    #[error("inconsistent feed state: {0}")]
    InconsistentFeed(String),

    #[error("validation failed: {0}")]
    Validation(String),

    #[error("storage failure: {0}")]
    Storage(String),
}

/// A parsed `Retry-After` hint in seconds, if any (HTTP header value as string).
pub fn parse_retry_after(s: Option<&str>) -> Option<u64> {
    s.and_then(|s| s.trim().parse::<u64>().ok())
}
