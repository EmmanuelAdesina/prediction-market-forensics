//! Positions fetcher: `GET /v2/positions` cursor walk (verified wire shape).
//!
//! The endpoint paginates exactly like `/v2/activity`: a `pagination` object
//! with `limit`, `offset`, `has_more`, and an opaque (signed) `next_cursor` —
//! the cursor must be followed, not the offset. Polymarket's cursor embeds the
//! sort key (`s=CURRENT_VALUE`, `dir=DESC`), so pages are newest-value first.
//!
//! Phase 1 uses this for reconciliation only. Raw pages are persisted as
//! forensic records through the same `raw_api_pages` table.

use std::time::Duration;

use serde_json::Value;

use tokor_domain::error::{parse_retry_after, IngestionError};

const USER_AGENT: &str = "tokor-engine/0.1 (positions reconciliation research)";

#[derive(Debug, thiserror::Error)]
pub enum PositionsError {
    #[error("upstream error: {0}")]
    Upstream(#[from] IngestionError),
    #[error("transport failure: {0}")]
    Transport(String),
}

/// One fetched positions page: parsed envelope + forensic capture.
#[derive(Debug, Clone)]
pub struct FetchedPositionsPage {
    pub rows: Vec<Value>,
    pub raw_body: Value,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub url: String,
    pub query_params: String,
    pub http_status: u16,
}

pub struct HttpPositionsFetcher {
    client: reqwest::Client,
    base_url: String,
}

pub fn build_positions_url(
    base_url: &str,
    user: &str,
    limit: u64,
    cursor: Option<&str>,
) -> (String, String) {
    build_positions_url_status(base_url, user, limit, cursor, None)
}

/// `status` selects the API's position view: `OPEN`, `CLOSED`, `REDEEMABLE`,
/// or None for the default (open) view.
pub fn build_positions_url_status(
    base_url: &str,
    user: &str,
    limit: u64,
    cursor: Option<&str>,
    status: Option<&str>,
) -> (String, String) {
    let mut url = format!(
        "{}/positions?user={}&limit={}",
        base_url.trim_end_matches('/'),
        user.to_ascii_lowercase(),
        limit
    );
    let mut query = format!("user={}&limit={}", user.to_ascii_lowercase(), limit);
    if let Some(st) = status {
        url.push_str(&format!("&status={st}"));
        query.push_str(&format!("&status={st}"));
    }
    if let Some(c) = cursor {
        url.push_str("&cursor=");
        url.push_str(&rfc3986_encode(c));
        query.push_str(&format!("&cursor={c}"));
    }
    (url, query)
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

impl HttpPositionsFetcher {
    pub fn new(base_url: &str) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()?,
            base_url: base_url.to_string(),
        })
    }

    pub async fn fetch_page(
        &self,
        user: &str,
        limit: u64,
        cursor: Option<&str>,
    ) -> Result<FetchedPositionsPage, PositionsError> {
        self.fetch_page_status(user, limit, cursor, None).await
    }

    /// Fetch with an explicit status view (`OPEN`/`CLOSED`/`REDEEMABLE`).
    pub async fn fetch_page_status(
        &self,
        user: &str,
        limit: u64,
        cursor: Option<&str>,
        status: Option<&str>,
    ) -> Result<FetchedPositionsPage, PositionsError> {
        let (url, query) = build_positions_url_status(&self.base_url, user, limit, cursor, status);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| -> PositionsError {
                IngestionError::Transient {
                    code: 0,
                    message: format!("transport: {e}"),
                }
                .into()
            })?;

        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            let raw_body: Value = resp
                .json()
                .await
                .map_err(|e| PositionsError::Transport(format!("body decode: {e}")))?;
            let data = raw_body
                .get("data")
                .and_then(|d| d.as_array())
                .ok_or_else(|| {
                    PositionsError::Upstream(IngestionError::InconsistentFeed(
                        "positions response missing .data array".into(),
                    ))
                })?;
            let pagination = raw_body.get("pagination").cloned().unwrap_or(Value::Null);
            let next_cursor = pagination
                .get("next_cursor")
                .and_then(|c| c.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            let has_more = pagination
                .get("has_more")
                .and_then(|h| h.as_bool())
                .unwrap_or(false);
            if has_more && next_cursor.is_none() {
                return Err(PositionsError::Upstream(IngestionError::InconsistentFeed(
                    "positions has_more=true but next_cursor missing".into(),
                )));
            }
            return Ok(FetchedPositionsPage {
                rows: data.clone(),
                raw_body,
                next_cursor,
                has_more,
                url,
                query_params: query,
                http_status: status,
            });
        }

        let retry_after = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        let text = resp.text().await.unwrap_or_default();
        let err = match status {
            429 => IngestionError::RateLimited {
                retry_after_hint: parse_retry_after(retry_after.as_deref()),
                message: text.chars().take(200).collect(),
            },
            c if c >= 500 || c == 408 => IngestionError::Transient {
                code: c,
                message: text.chars().take(200).collect(),
            },
            c => IngestionError::Permanent {
                code: c,
                message: text.chars().take(400).collect(),
            },
        };
        Err(err.into())
    }
}

/// Walk every positions page with bounded retries + exponential backoff +
/// pacing, invoking `on_page` for each (caller persists forensically and may
/// stop by returning an error).
pub async fn walk_positions<F, T>(
    fetcher: &HttpPositionsFetcher,
    user: &str,
    limit: u64,
    max_pages: u64,
    page_pause: Duration,
    retry: PaginationRetry,
    on_page: F,
) -> Result<Vec<T>, PositionsError>
where
    F: FnMut(&FetchedPositionsPage) -> Result<T, PositionsError>,
{
    walk_positions_status(
        fetcher, user, limit, max_pages, page_pause, retry, None, on_page,
    )
    .await
}

/// Walk with an explicit status view (see `build_positions_url_status`).
#[allow(clippy::too_many_arguments)]
pub async fn walk_positions_status<F, T>(
    fetcher: &HttpPositionsFetcher,
    user: &str,
    limit: u64,
    max_pages: u64,
    page_pause: Duration,
    retry: PaginationRetry,
    status: Option<&str>,
    mut on_page: F,
) -> Result<Vec<T>, PositionsError>
where
    F: FnMut(&FetchedPositionsPage) -> Result<T, PositionsError>,
{
    let mut cursor: Option<String> = None;
    let mut results = Vec::new();
    let mut page_index = 0u64;
    loop {
        if page_index > 0 {
            tokio::time::sleep(page_pause).await;
        }
        if page_index >= max_pages {
            return Ok(results);
        }
        let page = fetch_with_retry_status(fetcher, user, limit, cursor.as_deref(), status, &retry)
            .await?;
        let done = !page.has_more;
        let next = page.next_cursor.clone();
        results.push(on_page(&page)?);
        if done {
            return Ok(results);
        }
        cursor = next;
        page_index += 1;
    }
}

/// Retry policy for pagination walks (bounded, honoring Retry-After).
#[derive(Debug, Clone, Copy)]
pub struct PaginationRetry {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for PaginationRetry {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 400,
            max_delay_ms: 8_000,
        }
    }
}

async fn fetch_with_retry_status(
    fetcher: &HttpPositionsFetcher,
    user: &str,
    limit: u64,
    cursor: Option<&str>,
    status: Option<&str>,
    policy: &PaginationRetry,
) -> Result<FetchedPositionsPage, PositionsError> {
    let mut attempt = 0u32;
    loop {
        match fetcher.fetch_page_status(user, limit, cursor, status).await {
            Ok(p) => return Ok(p),
            Err(PositionsError::Upstream(
                ref e @ (IngestionError::RateLimited { .. } | IngestionError::Transient { .. }),
            )) => {
                attempt += 1;
                if attempt > policy.max_retries {
                    return Err(PositionsError::Upstream(retries_exhausted(e)));
                }
                let exp = policy
                    .base_delay_ms
                    .saturating_mul(2u64.saturating_pow(attempt))
                    .min(policy.max_delay_ms);
                let jitter = {
                    use rand::Rng as _;
                    rand::thread_rng().gen_range(0..=exp.max(1))
                };
                let hint = match e {
                    IngestionError::RateLimited {
                        retry_after_hint, ..
                    } => retry_after_hint.unwrap_or(0).saturating_mul(1000),
                    _ => 0,
                };
                let delay = jitter.max(hint).min(policy.max_delay_ms);
                tracing::warn!(attempt, delay_ms = delay, "positions fetch retrying");
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn retries_exhausted(e: &IngestionError) -> IngestionError {
    match e {
        IngestionError::RateLimited {
            retry_after_hint, ..
        } => IngestionError::RateLimited {
            retry_after_hint: *retry_after_hint,
            message: "retries exhausted".into(),
        },
        IngestionError::Transient { code, .. } => IngestionError::Transient {
            code: *code,
            message: "retries exhausted".into(),
        },
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn pos_row(token: &str, size: f64, avg: f64) -> Value {
        json!({
            "proxy_wallet": "0xa19cbababc312f9df185e49d7004c249ed1ade6b",
            "token_id": token,
            "condition_id": "0xcond123",
            "current_size": size,
            "avg_price": avg,
            "entry_cost_usdc": size * avg,
            "entry_fees_usdc": 0.0,
            "total_cost_usdc": size * avg,
            "current_price": 0.6,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "status": "OPEN",
            "event_slug": "some-event"
        })
    }

    #[tokio::test]
    async fn walks_multiple_pages_following_cursor() {
        let server = MockServer::start().await;
        // Page 1: no cursor param.
        Mock::given(method("GET"))
            .and(query_param("limit", "2"))
            .and(wiremock::matchers::query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [pos_row("t1", 100.0, 0.5)],
                "pagination": {"limit": 2, "offset": 0, "has_more": true, "next_cursor": "P2"}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(query_param("limit", "2"))
            .and(query_param("cursor", "P2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [pos_row("t2", 50.0, 0.3)],
                "pagination": {"limit": 2, "offset": 2, "has_more": false, "next_cursor": ""}
            })))
            .mount(&server)
            .await;

        let fetcher = HttpPositionsFetcher::new(&server.uri()).unwrap();
        let user = "0xa19cbababc312f9df185e49d7004c249ed1ade6b";
        let pages = walk_positions(
            &fetcher,
            user,
            2,
            10,
            Duration::from_millis(10),
            PaginationRetry::default(),
            |p| Ok(p.rows.len()),
        )
        .await
        .unwrap();
        assert_eq!(pages, vec![1, 1]);
    }

    #[tokio::test]
    async fn inconsistent_feed_missing_data_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"nope": 1})))
            .mount(&server)
            .await;
        let fetcher = HttpPositionsFetcher::new(&server.uri()).unwrap();
        let err = fetcher
            .fetch_page("0xa19cbababc312f9df185e49d7004c249ed1ade6b", 10, None)
            .await;
        assert!(matches!(
            err,
            Err(PositionsError::Upstream(IngestionError::InconsistentFeed(
                _
            )))
        ));
    }
}
