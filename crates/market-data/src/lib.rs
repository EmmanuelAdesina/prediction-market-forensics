//! Phase-1 market data slice: CLOB REST metadata + minimal Gamma enrichment.
//!
//! This is deliberately NOT the Phase-2 live order-book engine. It provides
//! exactly what Phase 1 needs for classification and reconciliation:
//! - CLOB `GET /markets/{condition_id}` (verified endpoint) as the metadata
//!   authority for tick size / min order / status.
//! - Gamma `GET /events?slug=` for tags and descriptions (best effort).
//!
//!ambiguity rule: metadata is cached in `markets` with a `clob_meta_json` raw
//! copy for forensics.

use std::time::Duration;

use thiserror::Error;
use tokor_domain::market::MarketRef;

const USER_AGENT: &str = "tokor-engine/0.1 (market metadata enrichment)";

#[derive(Debug, Error)]
pub enum MarketDataError {
    #[error("transport: {0}")]
    Transport(String),
    #[error("upstream status {status}: {body}")]
    Upstream { status: u16, body: String },
}

pub struct MarketDataClient {
    client: reqwest::Client,
    clob_base: String,
    gamma_base: String,
}

impl MarketDataClient {
    pub fn new(clob_base: &str, gamma_base: &str) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: reqwest::Client::builder()
                .user_agent(USER_AGENT)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .build()?,
            clob_base: clob_base.to_string(),
            gamma_base: gamma_base.to_string(),
        })
    }

    /// Fetch CLOB market metadata by condition id, returning the raw JSON.
    pub async fn clob_market(
        &self,
        condition_id: &str,
    ) -> Result<serde_json::Value, MarketDataError> {
        let url = format!(
            "{}/markets/{}",
            self.clob_base.trim_end_matches('/'),
            condition_id
        );
        self.get_json(&url).await
    }

    /// Fetch Gamma event(s) by slug (tags/description enrichment).
    pub async fn gamma_event_by_slug(
        &self,
        slug: &str,
    ) -> Result<serde_json::Value, MarketDataError> {
        let url = format!(
            "{}/events?slug={}",
            self.gamma_base.trim_end_matches('/'),
            slug
        );
        self.get_json(&url).await
    }

    /// Fetch one page of the Gamma events listing filtered by tag slug.
    ///
    /// Verified endpoint: `GET {gamma}/events?tag_slug=<t>&closed=<b>&limit=<n>&offset=<o>`
    /// - returns a JSON array of event objects, each embedding its `markets`
    /// array (verified against live responses 2026-11). Note: multiple
    /// `tag_slug` params do NOT intersect (verified); callers must filter by
    /// asset tags locally.
    pub async fn gamma_events_by_tag(
        &self,
        tag_slug: &str,
        closed: bool,
        limit: u64,
        offset: u64,
    ) -> Result<Vec<serde_json::Value>, MarketDataError> {
        let url = format!(
            "{}/events?tag_slug={}&closed={}&limit={}&offset={}",
            self.gamma_base.trim_end_matches('/'),
            urlencode(tag_slug),
            closed,
            limit,
            offset
        );
        let val = self.get_json(&url).await?;
        match val {
            serde_json::Value::Array(items) => Ok(items),
            other => Err(MarketDataError::Transport(format!(
                "gamma events listing: expected JSON array, got {}",
                other
            ))),
        }
    }

    /// Fetch and shape a MarketRef from the CLOB payload.
    pub async fn enriched_market_ref(
        &self,
        condition_id: &str,
    ) -> Result<MarketRef, MarketDataError> {
        let clob = self.clob_market(condition_id).await?;
        let slug = clob
            .get("market_slug")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        let event_slug = clob
            .get("events")
            .and_then(|e| e.as_array())
            .and_then(|v| v.first())
            .and_then(|ev| ev.get("slug"))
            .and_then(|s| s.as_str())
            .map(|s| s.to_string());
        Ok(MarketRef {
            condition_id: condition_id.to_string(),
            title: clob
                .get("question")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string()),
            market_slug: slug,
            event_slug,
            icon: None,
            tags: None,
            description: None,
            tokens: None,
            active: clob.get("active").and_then(|v| v.as_bool()),
            closed: clob.get("closed").and_then(|v| v.as_bool()),
            minimum_tick_size: clob.get("minimum_tick_size").and_then(str_val),
            minimum_order_size: clob.get("minimum_order_size").and_then(str_val),
            neg_risk: clob.get("neg_risk").and_then(|v| v.as_bool()),
        })
    }

    async fn get_json(&self, url: &str) -> Result<serde_json::Value, MarketDataError> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| MarketDataError::Transport(format!("{e}")))?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = resp.text().await.unwrap_or_default();
            return Err(MarketDataError::Upstream {
                status,
                body: body.chars().take(200).collect(),
            });
        }
        resp.json()
            .await
            .map_err(|e| MarketDataError::Transport(format!("{e}")))
    }
}

fn str_val(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Minimal percent-encoding for query parameters (tags are slugs: safe set).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
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
    use wiremock::matchers::method;
    use wiremock::MockServer;
    use wiremock::{Mock, ResponseTemplate};

    #[tokio::test]
    async fn clob_market_parses_minimum_constraints() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "question": "Will Bitcoin dip to $73,000 on November 9?",
                "market_slug": "will-bitcoin-dip-to-73k-on-november-9-2026",
                "minimum_order_size": 5,
                "minimum_tick_size": 0.01,
                "neg_risk": false,
                "active": true,
                "closed": false
            })))
            .mount(&server)
            .await;

        let client = MarketDataClient::new(&server.uri(), "http://unused.example").unwrap();
        let raw = client.clob_market("0xcond").await.unwrap();
        assert_eq!(raw["minimum_order_size"], 5);
        let mref = client.enriched_market_ref("0xcond").await.unwrap();
        assert_eq!(
            mref.market_slug.as_deref(),
            Some("will-bitcoin-dip-to-73k-on-november-9-2026")
        );
        assert_eq!(mref.minimum_tick_size.as_deref(), Some("0.01"));
        assert!(mref.active.unwrap());
    }

    #[tokio::test]
    async fn gamma_events_listing_parses_array() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(wiremock::matchers::query_param("tag_slug", "hit-price"))
            .and(wiremock::matchers::query_param("closed", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "slug": "what-price-will-bitcoin-hit-in-2025",
                    "tags": [{"slug": "bitcoin"}, {"slug": "hit-price"}],
                    "markets": [{"question": "Will Bitcoin dip to $90,000?"}]
                }
            ])))
            .mount(&server)
            .await;

        let client = MarketDataClient::new("http://unused.example", &server.uri()).unwrap();
        let events = client
            .gamma_events_by_tag("hit-price", true, 100, 0)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0]["markets"][0]["question"],
            "Will Bitcoin dip to $90,000?"
        );
    }
}
