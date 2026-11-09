//! Wallet activity: source fills, cursors, ingestion batches.
//!
//! ## Event-identity strategy
//!
//! The Data API v2 activity records have **no unique fill id**. A single
//! on-chain `transactionHash` may contain several fills (batched / split
//! executions), so the hash alone cannot identify a fill.
//!
//! We define the canonical identity of a source fill as the SHA-256 of
//! a stable, field-ordered composite:
//!
//! ```text
//! v1|{transaction_hash}|{timestamp}|{condition_id}|{token_id}|{side}|{price}|{size}
//! ```
//!
//! Consequences (documented limitation):
//! - Two *genuinely identical* fills in the same second (same side, price,
//!   size, market, outcome) are indistinguishable and collapse to one record.
//!   We measure how often raw rows collide on this key but differ in raw JSON
//!   (`identity_collisions` counter); in the observed sample (1,000 fills)
//!   every row had a distinct composite key.
//! - Re-fetching the same page is fully idempotent: same key → same row.

use std::fmt;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use crate::error::IngestionError;

/// Direction of a source fill as observed on the wallet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

/// The activity record types the Data API can return. Phase 1 consumes
/// `Trade` (fills) only; other variants exist downstream but are recorded raw.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ActivityType {
    Trade,
    Split,
    Merge,
    Redeem,
    Reward,
    Conversion,
}

/// Exact raw shape of one record from `GET /v2/activity`.
///
/// This mirrors the verified wire format. Numbers arrive as JSON numbers
/// (f64 on the wire); we stringify immediately and hand parsing to `Decimal`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFillRaw {
    pub proxy_wallet: String,
    pub timestamp: i64,
    pub condition_id: String,
    #[serde(rename = "type")]
    pub record_type: ActivityType,
    pub size: f64,
    pub usdc_size: f64,
    pub transaction_hash: String,
    pub price: f64,
    pub token_id: String,
    pub side: Side,
    pub outcome_index: i64,
    pub title: String,
    pub slug: String,
    pub icon: String,
    pub event_slug: String,
    pub outcome: String,
    pub name: String,
    pub pseudonym: String,
}

/// Wallet address validation (0x + 40 hex chars, case-insensitive).
pub fn is_valid_address(a: &str) -> bool {
    a.len() == 42 && a.starts_with("0x") && a.chars().skip(2).all(|c| c.is_ascii_hexdigit())
}

fn dec_from_json_f64(name: &str, v: f64) -> Result<Decimal, IngestionError> {
    if !v.is_finite() {
        return Err(IngestionError::Validation(format!(
            "field {name} is not finite: {v}"
        )));
    }
    // Parse through the shortest-roundtrip decimal string to avoid
    // f64 -> Decimal noise (e.g. 0.09 -> 0.09000000000000000111...).
    s_decimal(&format!("{v}")).map_err(|e| {
        IngestionError::Validation(format!("field {name}: not representable as Decimal: {e}"))
    })
}

fn s_decimal(s: &str) -> Result<Decimal, String> {
    s.parse::<Decimal>().map_err(|e| format!("{s:?}: {e}"))
}

/// A validated, normalized source fill.
#[derive(Debug, Clone, Serialize)]
pub struct SourceFill {
    /// Canonical identity (sha256 hex of the composite key).
    pub event_key: String,
    /// Source wallet address (proxy wallet in Polymarket parlance).
    pub wallet: String,
    /// UNIX seconds, as delivered by the API (1-second resolution; documented).
    pub timestamp: i64,
    pub condition_id: String,
    pub token_id: String,
    pub side: Side,
    /// Share quantity (positive).
    pub size: Decimal,
    /// USDC notional (may be `size * price` up to rounding by the source).
    pub usdc_size: Decimal,
    /// Execution price, always in `[0, 1]` — the source's displayed view
    /// (observed: floor-rounded to 2 decimals on some rows).
    pub price: Decimal,
    /// Exact per-share price implied by source amounts, `usdc_size / size`.
    /// Used for cost basis (matches Polymarket's own `avg_price` semantics);
    /// `price` stays in the identity composite for source stability.
    pub exec_price: Decimal,
    pub outcome_index: i64,
    pub outcome: String,
    /// Market-scoped denormalized metadata from the activity record.
    pub market_title: String,
    pub market_slug: String,
    pub event_slug: String,
    pub icon: String,
    /// Full raw record, losslessly retained.
    pub raw_json: String,
}

impl SourceFill {
    /// Validate + normalize a raw record. Never discards information:
    /// the raw JSON string is carried through verbatim.
    pub fn from_raw(raw: &SourceFillRaw, raw_json: &str) -> Result<SourceFill, IngestionError> {
        if !is_valid_address(&raw.proxy_wallet) {
            return Err(IngestionError::Validation(format!(
                "invalid wallet address {:#?}",
                raw.proxy_wallet
            )));
        }
        if raw.condition_id.len() < 40 {
            return Err(IngestionError::Validation(format!(
                "suspicious condition_id length {}",
                raw.condition_id.len()
            )));
        }
        if raw.token_id.is_empty() {
            return Err(IngestionError::Validation("empty token_id".into()));
        }
        if raw.timestamp <= 0 {
            return Err(IngestionError::Validation(format!(
                "invalid timestamp {}",
                raw.timestamp
            )));
        }
        if raw.size <= 0.0 {
            return Err(IngestionError::Validation(format!(
                "size must be > 0, got {}",
                raw.size
            )));
        }
        let price = dec_from_json_f64("price", raw.price)?;
        if !(Decimal::ZERO..=Decimal::ONE).contains(&price) {
            return Err(IngestionError::Validation(format!(
                "price out of [0,1]: {price}"
            )));
        }
        let size = dec_from_json_f64("size", raw.size)?;
        let usdc = dec_from_json_f64("usdc_size", raw.usdc_size)?;

        // Consistency analysis between the two source-provided price views:
        // `price` (possibly displayed/rounded) and `usdc_size` (exact notional).
        let implied = price * size;
        if usdc <= Decimal::ZERO || implied <= Decimal::ZERO {
            return Err(IngestionError::Validation(format!(
                "usdc_size {usdc} and price*size {implied} must both be positive"
            )));
        }

        // Exact execution price implied by the source amounts: usdc/size.
        // This is source-derived arithmetic, not fabrication — but only
        // trustworthy when the displayed price corroborates it.
        let exec_price = usdc / size;

        // Accept when either (a) usdc ≈ price*size within 5% (f64-rounding
        // tolerance for source arithmetic), or (b) the displayed price is the
        // 2-decimal floor of the exact price (observed source behavior:
        // `price` is floor₂(usdc/size) while usdc keeps full precision).
        let exact_floor_ok = price <= exec_price
            && exec_price < price + Decimal::new(1, 2)
            && (usdc - implied).abs() / implied > Decimal::new(5, 2);
        let tolerance_ok = (usdc - implied).abs() / implied <= Decimal::new(5, 2);
        if !exact_floor_ok && !tolerance_ok {
            // Outside any verified interpretation: cash notional exceeds
            // every reconcilable price view. Preserved raw upstream, rejected
            // here (documented: fee-premium suspect rows await the
            // fee-schedule verification referenced in the risk register).
            return Err(IngestionError::Validation(format!(
                "usdc_size {usdc} is inconsistent with price {price} × size {size} \
                 (exactiable price {exec_price} is not the 2dp floor of the displayed price)"
            )));
        }

        let key = event_identity(
            &raw.transaction_hash,
            raw.timestamp,
            &raw.condition_id,
            &raw.token_id,
            &raw.side,
            &price,
            &size,
        );

        Ok(SourceFill {
            event_key: key,
            wallet: raw.proxy_wallet.to_ascii_lowercase(),
            timestamp: raw.timestamp,
            condition_id: raw.condition_id.to_ascii_lowercase(),
            token_id: raw.token_id.clone(),
            side: raw.side,
            size,
            usdc_size: usdc,
            price,
            exec_price,
            outcome_index: raw.outcome_index,
            outcome: raw.outcome.clone(),
            market_title: raw.title.clone(),
            market_slug: raw.slug.clone(),
            event_slug: raw.event_slug.clone(),
            icon: raw.icon.clone(),
            raw_json: raw_json.to_string(),
        })
    }
}

/// Canonical event identity: SHA-256 of the documented composite key.
pub fn event_identity(
    tx_hash: &str,
    timestamp: i64,
    condition_id: &str,
    token_id: &str,
    side: &Side,
    price: &Decimal,
    size: &Decimal,
) -> String {
    let composite = format!(
        "v1|{}|{}|{}|{}|{}|{}|{}",
        tx_hash, timestamp, condition_id, token_id, side, price, size
    );
    let mut h = Sha256::new();
    h.update(composite.as_bytes());
    hex(&h.finalize())
}

pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Normalized outcome token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeToken {
    pub token_id: String,
    pub condition_id: String,
    pub outcome: String,
    pub outcome_index: i64,
}

/// Polymarket labels outcomes either "Yes"/"No" or richer strings;
/// this enum carries the normalized view without discarding the raw label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Outcome {
    Yes,
    No,
    Other,
}

/// Immutable cursor state for the wallet feed.
///
/// Filters must be carried *with* the cursor: re-using a cursor saved under
/// different filters/limit without validation is prohibited (spec §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalletCursor {
    pub last_cursor: Option<String>,
    pub filters_fingerprint: String,
    pub updated_at: Option<DateTime<Utc>>,
    /// Count of pages successfully committed with this state line.
    pub pages_committed: u64,
}

impl WalletCursor {
    pub fn initial(filters_fingerprint: &str) -> Self {
        Self {
            last_cursor: None,
            filters_fingerprint: filters_fingerprint.to_string(),
            updated_at: None,
            pages_committed: 0,
        }
    }
}

/// Query-parameter fingerprint so we can detect filter drift between runs.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ActivityFilters {
    pub user: String,
    pub record_types: Vec<ActivityType>,
    pub limit: u64,
}

impl ActivityFilters {
    /// Canonical fingerprint of the query parameters (order-stable).
    pub fn fingerprint(&self) -> String {
        let mut types: Vec<&str> = self
            .record_types
            .iter()
            .map(|t| match t {
                ActivityType::Trade => "TRADE",
                ActivityType::Split => "SPLIT",
                ActivityType::Merge => "MERGE",
                ActivityType::Redeem => "REDEEM",
                ActivityType::Reward => "REWARD",
                ActivityType::Conversion => "CONVERSION",
            })
            .collect();
        types.sort();
        format!(
            "user={}|types={}|limit={}",
            self.user.to_ascii_lowercase(),
            types.join(","),
            self.limit
        )
    }
}

/// One page of activity: raw envelope + normalized fills.
///
/// The raw envelope is preserved verbatim for forensic reconstruction.
#[derive(Debug, Clone)]
pub struct IngestionBatch {
    /// Raw JSON body as received (may be larger than the sum of normalized rows).
    pub raw_body: Json,
    /// Normalized fills (only TRADE rows in Phase 1).
    pub fills: Vec<SourceFill>,
    /// Non-TRADE records preserved raw (counted, not normalized).
    pub non_trade_records: usize,
    /// TRADE rows that failed validation — never silently dropped.
    pub invalid_records: usize,
    /// Opaque continuation cursor from the server.
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// Server-reported request params, recorded for drift detection.
    pub meta: PageMeta,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PageMeta {
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub has_more: Option<bool>,
    /// Raw cursor string as received.
    pub next_cursor_raw: Option<String>,
}

impl IngestionBatch {
    /// Parse and validate a page response body into a batch.
    /// Rows that fail validation are counted and *surfaced*, not silently dropped.
    pub fn parse(body: &Json, filters: &ActivityFilters) -> Result<IngestionBatch, IngestionError> {
        let data = body
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| IngestionError::InconsistentFeed("missing .data array".into()))?;
        let pagination = body
            .get("pagination")
            .ok_or_else(|| IngestionError::InconsistentFeed("missing .pagination object".into()))?;

        let mut fills = Vec::with_capacity(data.len());
        let mut invalid = 0usize;
        let mut non_trade = 0usize;
        for row in data {
            let raw_json = serde_json::to_string(row).map_err(|e| {
                IngestionError::Validation(format!("row re-serialization failed: {e}"))
            })?;
            let raw: SourceFillRaw = serde_json::from_str(&raw_json).map_err(|e| {
                IngestionError::Validation(format!("row does not match activity schema: {e}"))
            })?;
            if raw.record_type != ActivityType::Trade {
                // Non-trade activity types are outside Phase 1 scope; they are
                // still preserved inside raw_body. Count but do not normalize.
                non_trade += 1;
                continue;
            }
            match SourceFill::from_raw(&raw, &raw_json) {
                Ok(f) => fills.push(f),
                Err(e) => {
                    invalid += 1;
                    tracing::warn!(%e, "invalid activity row preserved raw but not normalized");
                    // Strict contract: never ingest an unverifiable fill.
                }
            }
        }

        let next_cursor = pagination
            .get("next_cursor")
            .and_then(|c| c.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let next_cursor_clone = next_cursor.clone();

        // The API embeds the limit we requested in the cursor payload; if the
        // response limit differs from our filter limit that's filter drift.
        let resp_limit = pagination.get("limit").and_then(|l| l.as_u64());
        if let Some(l) = resp_limit {
            if l != filters.limit {
                return Err(IngestionError::InconsistentFeed(format!(
                    "response limit {} != requested {}",
                    l, filters.limit
                )));
            }
        }

        let has_more = pagination.get("has_more").and_then(|h| h.as_bool());

        Ok(IngestionBatch {
            raw_body: body.clone(),
            fills,
            non_trade_records: non_trade,
            invalid_records: invalid,
            next_cursor,
            has_more: has_more.unwrap_or(false),
            meta: PageMeta {
                limit: resp_limit,
                offset: pagination.get("offset").and_then(|o| o.as_u64()),
                has_more,
                next_cursor_raw: next_cursor_clone,
            },
        })
    }
}
