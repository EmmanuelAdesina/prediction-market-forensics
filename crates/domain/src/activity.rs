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

/// The activity record types the Data API can return. Phase 1 consumed
/// `Trade` (fills) only; Phase 1.5 normalizes every observed type
/// (MERGE/REDEEM disposals, cash events: rewards/rebates/yield).
///
/// Verified wallet-observed types (Phase 1.5 census over 19,431 unfiltered
/// rows): TRADE, MERGE, REDEEM, REWARD, YIELD, TAKER_REBATE, MAKER_REBATE.
/// SPLIT and CONVERSION returned zero rows for this wallet but remain
/// supported (verified-not-assumed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ActivityType {
    Trade,
    Split,
    Merge,
    Redeem,
    Reward,
    Conversion,
    /// Distribution cash events (e.g. Dana/USDC yield): cash-only rows.
    #[serde(rename = "YIELD")]
    Yield,
    #[serde(rename = "TAKER_REBATE")]
    TakerRebate,
    #[serde(rename = "MAKER_REBATE")]
    MakerRebate,
}

impl ActivityType {
    /// The uppercase wire label.
    pub const fn as_str(self) -> &'static str {
        match self {
            ActivityType::Trade => "TRADE",
            ActivityType::Split => "SPLIT",
            ActivityType::Merge => "MERGE",
            ActivityType::Redeem => "REDEEM",
            ActivityType::Reward => "REWARD",
            ActivityType::Conversion => "CONVERSION",
            ActivityType::Yield => "YIELD",
            ActivityType::TakerRebate => "TAKER_REBATE",
            ActivityType::MakerRebate => "MAKER_REBATE",
        }
    }

    /// Event types that touch ERC-1155 share inventory.
    pub const fn is_inventory_event(self) -> bool {
        matches!(
            self,
            ActivityType::Trade | ActivityType::Split | ActivityType::Merge | ActivityType::Redeem
        )
    }

    /// Event types that are cash-only (no token inventory effect).
    pub const fn is_cash_only_event(self) -> bool {
        matches!(
            self,
            ActivityType::Reward
                | ActivityType::Yield
                | ActivityType::TakerRebate
                | ActivityType::MakerRebate
        )
    }
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
    /// Execution price, always in `[0, 1]` - the source's displayed view
    /// (observed: floor-rounded to 2 decimals on some rows).
    pub price: Decimal,
    /// Gross notional `price × size` - fee-free view of the trade.
    ///
    /// Phase-1.5 accounting correction: the four money quantities of a fill
    /// are distinct and must never be conflated:
    ///   displayed execution price = `price`
    ///   gross notional            = `gross_usdc()`
    ///   charged fee               = `fee_reconciliation.fee_usdc` (category
    ///                               model, docs/02)
    ///   total cash flow           = `usdc_size`
    pub usdc_size: Decimal,
    /// Per-share CASH amount `usdc_size / size` (fee-inclusive for taker
    /// fills, gross for makers). **This is NOT the execution price** - the
    /// execution price is the displayed `price` field. Retained because it
    /// reproduces Polymarket's own cost-basis arithmetic in the position
    /// reconstruction; the fee component is tracked separately so the gross
    /// view is always recoverable.
    pub cash_implied_price: Decimal,
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
    /// Gross notional: displayed execution price × shares, fee-free.
    /// The charged fee (if any) lives in `fee_reconciliation`; total cash is
    /// `usdc_size`. Distinct fields by Phase-1.5 accounting contract.
    pub fn gross_usdc(&self) -> Decimal {
        self.price * self.size
    }

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

        // Exact per-share price implied by the source amounts: usdc/size.
        // This is source-derived arithmetic, not fabrication - but only
        // trustworthy when the displayed price corroborates it.
        let cash_implied_price = usdc / size;

        // Fee-aware cash consistency (docs/02 §2 - the verified model):
        // total cash is `gross + fee` where `fee = size × rate × p × (1−p)`,
        // rate ≤ 0.07 across every published category, serialized by the
        // backend with a ≤ 2×10⁻⁵ absolute float envelope. Ingestion accepts
        // any row whose cash lies in [gross − env, gross × (1 + 0.07) + env];
        // category-exact pinning (verdict + maker/taker face) happens in the
        // fee model (`derive-fees`), never here. Verified over the full
        // 13,956-row raw corpus: 0 rows outside this band.
        let envelope = Decimal::new(2, 5); // 2×10⁻⁵ USDC
        let max_fee = implied * Decimal::new(7, 2) + envelope; // rate ≤ 0.07
        let fee_band_ok = usdc >= implied - envelope && usdc - implied <= max_fee;
        if !fee_band_ok {
            // Outside every published fee schedule: cash cannot be
            // reconciled with the displayed price under any category.
            // Preserved raw upstream, rejected here - never force-fit.
            return Err(IngestionError::Validation(format!(
                "usdc_size {usdc} outside the fee band of gross {implied} \
                 (price {price} × size {size}): cash − gross must lie in \
                 [−2e-5, 0.07 × gross + 2e-5]"
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
            cash_implied_price,
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
        let mut types: Vec<&str> = self.record_types.iter().map(|t| t.as_str()).collect();
        types.sort();
        format!(
            "user={}|types={}|limit={}",
            self.user.to_ascii_lowercase(),
            types.join(","),
            self.limit
        )
    }
}

/// One page of activity: raw envelope + normalized fills + all-type events.
///
/// The raw envelope is preserved verbatim for forensic reconstruction.
#[derive(Debug, Clone)]
pub struct IngestionBatch {
    /// Raw JSON body as received (may be larger than the sum of normalized rows).
    pub raw_body: Json,
    /// TRADE fills passing the strict Phase 1 validation.
    pub fills: Vec<SourceFill>,
    /// All-type activity events (Phase 1.5): lenient normalization with
    /// deterministic occurrence identity.
    pub events: Vec<crate::activity_events::ActivityEvent>,
    /// Rows preserved raw but failing normalization - never silently dropped.
    pub invalid_records: usize,
    /// Rows normalized as events but not as strict fills (non-TRADE types).
    pub non_trade_records: usize,
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
        // All-type events first (lenient, occurrence-identities assigned).
        let activity = crate::activity_events::parse_activity_batch(body)?;

        // Strict TRADE fills (Phase 1 path) on top of the same rows.
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
            let raw: SourceFillRaw = match serde_json::from_str(&raw_json) {
                Ok(r) => r,
                Err(_) => {
                    // Not a TRADE-shaped row (other types legitimately omit
                    // side/price fields); already normalized as an event.
                    non_trade += 1;
                    continue;
                }
            };
            if raw.record_type != ActivityType::Trade {
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
            events: activity.events,
            invalid_records: activity.invalid + invalid,
            non_trade_records: non_trade,
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
