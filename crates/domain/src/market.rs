//! Market reference and metadata containers.

use serde::{Deserialize, Serialize};

/// Minimal metadata needed to classify and reference a market.
/// Populated first from activity records, enriched from CLOB/Gamma when
/// available. Optional fields trace enrichment status explicitly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketRef {
    pub condition_id: String,
    /// Question text (e.g. "Will Bitcoin dip to $73,000 on November 9?").
    pub title: Option<String>,
    pub market_slug: Option<String>,
    pub event_slug: Option<String>,
    /// Icon URL - contains asset markers like "BTC+fullsize.png".
    pub icon: Option<String>,
    /// Gamma event tags (slugs), e.g. `["crypto","ethereum","price"]`.
    pub tags: Option<Vec<String>>,
    /// Gamma market/event description text.
    pub description: Option<String>,
    /// Outcome token ids for this market, when known.
    pub tokens: Option<Vec<(String, String)>>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    /// Tick size minimum from CLOB metadata (may be null if unknown).
    pub minimum_tick_size: Option<String>,
    pub minimum_order_size: Option<String>,
    pub neg_risk: Option<bool>,
}

impl MarketRef {
    /// Stable string key for joins/logging.
    pub fn key(&self) -> &str {
        &self.condition_id
    }
}
