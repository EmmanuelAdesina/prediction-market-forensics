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
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
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
            minimum_tick_size: clob.get("minimum_tick_size").and_then(|v| str_val(v)),
            minimum_order_size: clob.get("minimum_order_size").and_then(|v| str_val(v)),
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
}
