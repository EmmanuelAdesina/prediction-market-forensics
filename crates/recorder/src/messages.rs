//! CLOB market-channel wire types (public, documented protocol).
//!
//! Shapes verified against Polymarket's documented WS protocol; parsing is
//! tolerant (unknown event types are preserved as `Other` raw JSON, never
//! dropped) because the raw layer is the research evidence.

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// One level of the visible book, as sent by the exchange.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct WireLevel {
    pub price: String,
    pub size: String,
}

/// Full book (REST snapshot or WS `book` event).
#[derive(Debug, Clone, Deserialize)]
pub struct BookMsg {
    pub asset_id: String,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub bids: Vec<WireLevel>,
    #[serde(default)]
    pub asks: Vec<WireLevel>,
}

/// Incremental level update (WS `price_change`).
#[derive(Debug, Clone, Deserialize)]
pub struct PriceChangeMsg {
    pub asset_id: String,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub changes: Vec<PriceChangeLevel>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PriceChangeLevel {
    pub price: String,
    pub side: String, // "BUY" | "SELL" - the side of the BOOK the level sits on
    pub size: String,
}

/// Tick-size change (WS `tick_size_change`).
#[derive(Debug, Clone, Deserialize)]
pub struct TickSizeMsg {
    pub asset_id: String,
    #[serde(default)]
    pub old_tick_size: Option<Json>,
    #[serde(default)]
    pub new_tick_size: Option<Json>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// Observable public trade (WS `last_trade_price`).
#[derive(Debug, Clone, Deserialize)]
pub struct LastTradeMsg {
    pub asset_id: String,
    pub price: String,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// A parsed market-channel message. `Other` keeps the verbatim JSON.
#[derive(Debug, Clone)]
pub enum MarketMessage {
    Book(BookMsg),
    PriceChange(PriceChangeMsg),
    TickSize(TickSizeMsg),
    LastTrade(LastTradeMsg),
    Other(Json),
}

/// Parse one WS text frame. Unparseable frames surface as `Other` (the raw
/// layer records them; normalization never fabricates from unknown shapes).
pub fn parse_market_message(text: &str) -> MarketMessage {
    match serde_json::from_str::<Json>(text) {
        Ok(Json::Object(obj)) => {
            let val = Json::Object(obj.clone());
            match obj.get("event_type").and_then(|v| v.as_str()) {
                Some("book") => serde_json::from_value::<BookMsg>(val)
                    .map(MarketMessage::Book)
                    .unwrap_or_else(|_| MarketMessage::Other(Json::Object(obj))),
                Some("price_change") => serde_json::from_value::<PriceChangeMsg>(val)
                    .map(MarketMessage::PriceChange)
                    .unwrap_or_else(|_| MarketMessage::Other(Json::Object(obj))),
                Some("tick_size_change") => serde_json::from_value::<TickSizeMsg>(val)
                    .map(MarketMessage::TickSize)
                    .unwrap_or_else(|_| MarketMessage::Other(Json::Object(obj))),
                Some("last_trade_price") => serde_json::from_value::<LastTradeMsg>(val)
                    .map(MarketMessage::LastTrade)
                    .unwrap_or_else(|_| MarketMessage::Other(Json::Object(obj))),
                _ => MarketMessage::Other(Json::Object(obj)),
            }
        }
        _ => MarketMessage::Other(
            serde_json::from_str(text).unwrap_or(Json::String(text.to_string())),
        ),
    }
}

/// Subscription request body for the market channel (the protocol's spelling
/// `assets_ids` is intentional).
pub fn subscribe_body(asset_ids: &[String]) -> String {
    serde_json::json!({
        "assets_ids": asset_ids,
        "type": "market",
    })
    .to_string()
}

/// Parse a REST `/book` response body. The REST body carries the same shape
/// as a WS `book` event but without the `event_type` discriminator; inject it
/// so one parser serves both. Returns `None` for bodies that fail to parse -
/// the raw layer still records them verbatim.
pub fn parse_book_body(text: &str) -> Option<BookMsg> {
    let mut v: Json = serde_json::from_str(text).ok()?;
    if let Json::Object(ref mut obj) = v {
        obj.entry("event_type".to_string())
            .or_insert_with(|| Json::String("book".into()));
    }
    serde_json::from_value::<BookMsg>(v).ok()
}
