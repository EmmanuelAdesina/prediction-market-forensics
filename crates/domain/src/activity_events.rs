//! Phase 1.5: normalization of ALL activity-event types (not just TRADE).
//!
//! The wallet's unfiltered `/v2/activity` feed produces: TRADE, MERGE, REDEEM,
//! REWARD, YIELD, TAKER_REBATE, MAKER_REBATE (SPLIT and CONVERSION return zero
//! rows for this wallet but remain supported).
//!
//! Semantics verified against the Phase 1.5 census (docs/03):
//! - MERGE rows carry `size == usdc_size` (pairs × $1) and an empty token_id;
//!   the merged shares are paired complementary outcomes of one condition.
//! - REDEEM rows carry a token_id and `usdc_size == 0`; proceeds are derived
//!   at $1/share for winning outcomes.
//! - Cash-only events (REWARD, YIELD, TAKER_REBATE, MAKER_REBATE) carry
//!   `usdc_size` and no share inventory effect.
//!
//! Identity: the composite key is deterministic AND collision-safe for
//! genuinely identical rows inside one transaction (e.g. two maker fills at
//! the same price level): identical rows are disambiguated by an occurrence
//! ordinal assigned by a canonical ordering (raw-JSON hash), so identities are
//! invariant under page order, pagination boundaries, and re-walks.

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use crate::activity::{ActivityType, Side};
use crate::error::IngestionError;

/// Lenient raw shape: any activity row. Fields optional because the API omits
/// them per type (cash events carry no token; MERGE carries no token_id).
#[derive(Debug, Clone, Deserialize)]
pub struct ActivityEventRaw {
    pub proxy_wallet: String,
    pub timestamp: i64,
    #[serde(rename = "type")]
    pub record_type: ActivityType,
    pub transaction_hash: String,
    #[serde(default)]
    pub condition_id: String,
    #[serde(default)]
    pub token_id: String,
    #[serde(default)]
    pub side: Option<Side>,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub usdc_size: Option<f64>,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub outcome_index: Option<i64>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub event_slug: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pseudonym: Option<String>,
}

/// A validated, normalized activity event of any type.
#[derive(Debug, Clone, Serialize)]
pub struct ActivityEvent {
    /// Canonical identity: composite key + occurrence ordinal.
    pub event_key: String,
    pub wallet: String,
    pub event_type: ActivityType,
    pub timestamp: i64,
    pub condition_id: String,
    pub token_id: String,
    pub side: Option<Side>,
    /// Share quantity when the event touches inventory.
    pub size: Option<rust_decimal::Decimal>,
    /// USDC cash moved (source-serialized amount; REDEEM rows are 0 here and
    /// proceeds are derived at $1/share - never "fabricated" into this field).
    pub usdc_size: Option<rust_decimal::Decimal>,
    pub price: Option<rust_decimal::Decimal>,
    pub outcome_index: Option<i64>,
    pub outcome: Option<String>,
    pub market_title: Option<String>,
    pub market_slug: Option<String>,
    pub event_slug: Option<String>,
    pub transaction_hash: String,
    /// Occurrence ordinal among canonically-identical rows of one tx (0-based).
    pub occurrence: u32,
    pub raw_json: String,
}

/// Canonical composite of the row's content (before occurrence ordinal).
/// Every distinguishing field participates; floats are pre-stringified by
/// the shortest-roundtrip path so the composite is stable.
fn canonical_composite(e: &ActivityEvent) -> String {
    format!(
        "v2|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        e.transaction_hash,
        e.timestamp,
        e.event_type.as_str(),
        e.condition_id,
        e.token_id,
        e.side.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        e.size.as_ref().map(|d| d.to_string()).unwrap_or_default(),
        e.usdc_size
            .as_ref()
            .map(|d| d.to_string())
            .unwrap_or_default(),
        e.price.as_ref().map(|d| d.to_string()).unwrap_or_default(),
        e.outcome_index.map(|i| i.to_string()).unwrap_or_default(),
        e.outcome.as_deref().unwrap_or(""),
    )
}

/// SHA-256 hex of a string.
fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    crate::activity::hex(&h.finalize())
}

/// Canonical identity for one activity event occurrence.
pub fn activity_event_identity(composite_hash: &str, occurrence: u32) -> String {
    sha256_hex(&format!("ev|{composite_hash}|{occurrence}"))
}

/// Validate + normalize one raw row. Validation is *type-aware*:
/// - TRADE must carry positive size, usdc, and a price in [0,1].
/// - MERGE must carry equal size/usdc (pairs × $1) per the verified semantics.
/// - REDEEM carries size (shares) and usdc == 0 (proceeds derived, not shown).
/// - Cash-only events carry usdc >= 0 and no inventory fields of interest.
pub fn activity_event_from_raw(
    raw: &ActivityEventRaw,
    raw_json: &str,
) -> Result<ActivityEvent, IngestionError> {
    use rust_decimal::Decimal;

    if !crate::activity::is_valid_address(&raw.proxy_wallet) {
        return Err(IngestionError::Validation(format!(
            "invalid wallet address {:?}",
            raw.proxy_wallet
        )));
    }
    if raw.timestamp <= 0 {
        return Err(IngestionError::Validation(format!(
            "invalid timestamp {}",
            raw.timestamp
        )));
    }
    if raw.transaction_hash.is_empty() {
        return Err(IngestionError::Validation("empty transaction_hash".into()));
    }

    let dec = |name: &str, v: f64| -> Result<Decimal, IngestionError> {
        if !v.is_finite() {
            return Err(IngestionError::Validation(format!(
                "field {name} is not finite: {v}"
            )));
        }
        v.to_string()
            .parse::<Decimal>()
            .map_err(|err| IngestionError::Validation(format!("field {name}: {err}")))
    };

    let size = match raw.size {
        Some(v) => {
            let d = dec("size", v)?;
            if d <= Decimal::ZERO {
                return Err(IngestionError::Validation(format!(
                    "size must be > 0 for {}, got {d}",
                    raw.record_type.as_str()
                )));
            }
            Some(d)
        }
        None => None,
    };
    let usdc = match raw.usdc_size {
        Some(v) => {
            let d = dec("usdc_size", v)?;
            if d < Decimal::ZERO {
                return Err(IngestionError::Validation(format!(
                    "usdc_size must be >= 0 for {}, got {d}",
                    raw.record_type.as_str()
                )));
            }
            Some(d)
        }
        None => None,
    };
    let price = match raw.price {
        Some(v) => {
            let d = dec("price", v)?;
            if !(Decimal::ZERO..=Decimal::ONE).contains(&d) {
                return Err(IngestionError::Validation(format!(
                    "price out of [0,1]: {d}"
                )));
            }
            Some(d)
        }
        None => None,
    };

    // Type-aware consistency (verified wire semantics; never force-fit).
    match raw.record_type {
        ActivityType::Trade => {
            if size.is_none() || usdc.is_none() || price.is_none() || raw.side.is_none() {
                return Err(IngestionError::Validation(
                    "TRADE row missing size/usdc_size/price/side".into(),
                ));
            }
        }
        ActivityType::Merge => {
            let (s, u) = (size, usdc);
            match (s, u) {
                (Some(s), Some(u)) if s == u => {}
                other => {
                    return Err(IngestionError::Validation(format!(
                        "MERGE row must carry size == usdc_size (pairs × $1), got {other:?}"
                    )))
                }
            }
        }
        ActivityType::Redeem => {
            if size.is_none() {
                return Err(IngestionError::Validation(
                    "REDEEM row missing share size".into(),
                ));
            }
        }
        ActivityType::Reward
        | ActivityType::Yield
        | ActivityType::TakerRebate
        | ActivityType::MakerRebate => {
            if usdc.is_none() {
                return Err(IngestionError::Validation(format!(
                    "{} row missing usdc_size",
                    raw.record_type.as_str()
                )));
            }
        }
        ActivityType::Split | ActivityType::Conversion => {
            // Zero observed rows for this wallet; accept per generic shape.
        }
    }

    let mut ev = ActivityEvent {
        event_key: String::new(), // assigned below
        wallet: raw.proxy_wallet.to_ascii_lowercase(),
        event_type: raw.record_type,
        timestamp: raw.timestamp,
        condition_id: raw.condition_id.to_ascii_lowercase(),
        token_id: raw.token_id.clone(),
        side: raw.side,
        size,
        usdc_size: usdc,
        price,
        outcome_index: raw.outcome_index,
        outcome: raw.outcome.clone(),
        market_title: raw.title.clone(),
        market_slug: raw.slug.clone(),
        event_slug: raw.event_slug.clone(),
        transaction_hash: raw.transaction_hash.to_ascii_lowercase(),
        occurrence: 0,
        raw_json: raw_json.to_string(),
    };
    let composite_hash = sha256_hex(&canonical_composite(&ev));
    ev.event_key = activity_event_identity(&composite_hash, 0);
    Ok(ev)
}

/// Assign occurrence ordinals within each (transaction, canonical composite)
/// group by sorting on the raw-JSON hash. Deterministic under any input
/// ordering: identical rows receive 0..n by canonical order, distinct rows
/// keep ordinal 0.
pub fn assign_occurrence_ordinals(events: &mut [ActivityEvent]) {
    use std::collections::HashMap;

    // group key: (tx_hash, composite_hash) -> Vec<(raw_hash, index into slice)>
    let mut groups: HashMap<(String, String), Vec<(String, usize)>> = HashMap::new();
    for (i, e) in events.iter().enumerate() {
        let composite_hash = sha256_hex(&canonical_composite(e));
        let raw_hash = sha256_hex(&e.raw_json);
        groups
            .entry((e.transaction_hash.clone(), composite_hash))
            .or_default()
            .push((raw_hash, i));
    }
    for (_, mut members) in groups {
        if members.len() == 1 {
            continue;
        }
        // Canonical order: raw-JSON hash (ties broken by full raw bytes).
        members.sort_by(|a, b| a.0.cmp(&b.0));
        for (occ, (_, idx)) in members.into_iter().enumerate() {
            let e = &mut events[idx];
            e.occurrence = occ as u32;
            e.event_key = activity_event_identity(&sha256_hex(&canonical_composite(e)), occ as u32);
        }
    }
}

/// Parsed page of activity events (lenient envelope: `data` may be absent on
/// an empty terminal page; `pagination` presence is NOT required here - the
/// positions endpoint showed the API nests it; the activity feed does too.
pub struct ActivityBatch {
    pub events: Vec<ActivityEvent>,
    /// Rows that failed validation (surfaced, never silently dropped).
    pub invalid: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// Parse an unfiltered activity page body into events.
pub fn parse_activity_batch(body: &Json) -> Result<ActivityBatch, IngestionError> {
    let data = body
        .get("data")
        .and_then(|d| d.as_array())
        .ok_or_else(|| IngestionError::InconsistentFeed("missing .data array".into()))?;
    let pagination = body
        .get("pagination")
        .ok_or_else(|| IngestionError::InconsistentFeed("missing .pagination object".into()))?;

    let mut events = Vec::with_capacity(data.len());
    let mut invalid = 0usize;
    for row in data {
        let raw_json = serde_json::to_string(row)
            .map_err(|e| IngestionError::Validation(format!("row re-serialization failed: {e}")))?;
        match serde_json::from_str::<ActivityEventRaw>(&raw_json)
            .map_err(|e| IngestionError::Validation(format!("row schema: {e}")))
            .and_then(|raw| activity_event_from_raw(&raw, &raw_json))
        {
            Ok(ev) => events.push(ev),
            Err(e) => {
                invalid += 1;
                tracing::warn!(%e, "invalid activity row preserved raw, not normalized");
            }
        }
    }

    // Deterministic identities across page boundaries and re-walks.
    assign_occurrence_ordinals(&mut events);

    let next_cursor = pagination
        .get("next_cursor")
        .and_then(|c| c.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let has_more = pagination
        .get("has_more")
        .and_then(|h| h.as_bool())
        .unwrap_or(false);

    Ok(ActivityBatch {
        events,
        invalid,
        next_cursor,
        has_more,
    })
}
