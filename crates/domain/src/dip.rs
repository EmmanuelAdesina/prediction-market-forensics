//! Dip-market registry domain types: canonical ETH/BTC dip-market universe
//! for the independent strategy research engine.
//!
//! These types are the boundary between market *discovery* (this module and
//! `tokor-market-classifier::dip`) and any downstream probability, strategy,
//! or execution module. Discovery never makes trading decisions; it only
//! normalizes, verifies, and records.
//!
//! Core invariants:
//! * Nothing is eligible until its resolution semantics are **verified**
//!   against explicit contract text. Ambiguity is a recorded outcome, not an
//!   exception.
//! * Every normalized value keeps its verbatim source alongside it.
//! * Metadata changes append history (`metadata_version`), they never
//!   overwrite in place.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Underlying asset of a dip market. The registry is initially scoped to
/// BTC and ETH only; every other asset is out of scope by construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Asset {
    Btc,
    Eth,
}

impl Asset {
    /// Canonical display/slug forms used by Polymarket metadata.
    pub const ALL: [Asset; 2] = [Asset::Btc, Asset::Eth];

    /// Parse a user-supplied asset token ("BTC", "btc", "ETH"). Strict:
    /// unknown tokens are errors, never silently mapped.
    pub fn parse(s: &str) -> Result<Asset, String> {
        match s.trim().to_uppercase().as_str() {
            "BTC" | "BITCOIN" => Ok(Asset::Btc),
            "ETH" | "ETHEREUM" => Ok(Asset::Eth),
            other => Err(format!(
                "unsupported asset {other:?} (registry scope: BTC, ETH)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Asset::Btc => "BTC",
            Asset::Eth => "ETH",
        }
    }

    /// Binance spot pair named by the resolution contract (verified pattern:
    /// dip markets resolve on BTCUSDT / ETHUSDT 1-minute candles).
    pub fn binance_pair(self) -> &'static str {
        match self {
            Asset::Btc => "BTCUSDT",
            Asset::Eth => "ETHUSDT",
        }
    }
}

/// Structured settlement semantics parsed from the verbatim description.
///
/// These are the only classes the registry can mechanically verify today.
/// Anything else - close-based prints, average-price windows, volume
/// triggers - falls into [`BarrierSemantics::Other`] and is **ineligible**
/// until a human (or a future verifier version) pins the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BarrierSemantics {
    /// Resolves Yes when any 1-minute candle *Low* is at or below the
    /// barrier (the canonical "dip" contract: touch semantics, immediate).
    TouchLowAtMost,
    /// Resolves Yes when any 1-minute candle *High* is at or above the
    /// barrier (the canonical "reach" contract).
    TouchHighAtLeast,
    /// Description states settlement on a closing price - recorded but NOT
    /// eligible under the current verifier (rare; needs manual review).
    CloseBased,
    /// Explicitly documented but unrecognized settlement condition.
    Other,
}

impl BarrierSemantics {
    /// Textual fingerprint expected inside a verified contract for this class.
    pub fn descriptor(self) -> &'static str {
        match self {
            BarrierSemantics::TouchLowAtMost => "touch: candle Low at or below barrier",
            BarrierSemantics::TouchHighAtLeast => "touch: candle High at or above barrier",
            BarrierSemantics::CloseBased => "close-based settlement (manual review required)",
            BarrierSemantics::Other => "unrecognized settlement condition",
        }
    }
}

/// Outcome of the structured resolution-verification gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResolutionStatus {
    /// Semantics, source, and barrier cross-check all passed.
    Verified,
    /// Explicitly documented but contradictory/unparseable in a load-bearing
    /// way (e.g. title barrier ≠ description barrier). Never eligible.
    Ambiguous,
    /// Failed a hard requirement (no explicit settlement text, no source).
    Rejected,
    /// Not yet verified (discovered but the verifier has not run / changed).
    Unverified,
}

/// Evidence bundle for one verification run. Persisted as JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionVerification {
    pub status: ResolutionStatus,
    /// Version of the verifier that produced this record. Bump on rule changes.
    pub verifier_version: String,
    pub checked_at: DateTime<Utc>,
    /// Verbatim excerpts the decision was made from (description sentences).
    pub evidence: Vec<String>,
    /// Resolution source URL extracted from the contract, if stated.
    pub source_url: Option<String>,
    /// Binance pair the contract names, if stated (e.g. "BTCUSDT").
    pub source_pair: Option<String>,
    /// Human-readable reason for AMBIGUOUS/REJECTED, or window-parse notes.
    pub notes: Vec<String>,
}

/// Registry-side market lifecycle. Derived from Gamma flags + outcome prices;
/// resolved markets keep their outcome so backtests know the label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketStatus {
    /// Trading open (gamma: active && !closed).
    Active,
    /// Resolved Yes - the barrier was touched/reached per contract.
    ResolvedYes,
    /// Resolved No - barrier never reached within the window.
    ResolvedNo,
    /// Closed but outcome not confidently derivable from metadata.
    ResolvedUnknown,
}

impl MarketStatus {
    pub fn is_resolved(self) -> bool {
        !matches!(self, MarketStatus::Active)
    }
}

impl ResolutionStatus {
    /// Stable key for the rejection ledger (`rejections_json`).
    pub fn as_json_key(self) -> &'static str {
        match self {
            ResolutionStatus::Verified => "verified",
            ResolutionStatus::Ambiguous => "ambiguous",
            ResolutionStatus::Rejected => "resolution_rejected",
            ResolutionStatus::Unverified => "unverified",
        }
    }

    /// Human-facing rendering for CLI output.
    pub fn as_str(self) -> &'static str {
        match self {
            ResolutionStatus::Verified => "VERIFIED",
            ResolutionStatus::Ambiguous => "AMBIGUOUS",
            ResolutionStatus::Rejected => "REJECTED",
            ResolutionStatus::Unverified => "UNVERIFIED",
        }
    }
}

/// Why a market was refused entry into the registry (or into eligibility).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RejectionReason {
    /// Condition id missing or empty.
    MissingConditionId,
    /// YES/NO token ids missing or not a distinct pair.
    MissingTokens,
    /// Barrier price absent or not a positive decimal.
    InvalidBarrier,
    /// Deadline absent or unparseable.
    InvalidDeadline,
    /// Asset is not BTC/ETH (or cannot be determined).
    UnsupportedAsset,
    /// Resolution semantics ambiguous, rejected, or unverified.
    UnverifiedResolution,
}

impl RejectionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RejectionReason::MissingConditionId => "missing_condition_id",
            RejectionReason::MissingTokens => "missing_tokens",
            RejectionReason::InvalidBarrier => "invalid_barrier",
            RejectionReason::InvalidDeadline => "invalid_deadline",
            RejectionReason::UnsupportedAsset => "unsupported_asset",
            RejectionReason::UnverifiedResolution => "unverified_resolution",
        }
    }
}

/// Gamma-reported fee configuration, captured **raw**. The effective fee
/// schedule is deliberately NOT derived here: observed Gamma `makerBaseFee`
/// / `takerBaseFee` are base bps (e.g. 1000) that do not equal the verified
/// effective taker rate on crypto markets (docs/02 fee model: fee =
/// size × rate × p × (1−p)). Effective rates come from executed fills, never
/// from these metadata fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FeeInfo {
    pub fee_type: Option<String>,
    pub fees_enabled: Option<bool>,
    /// Raw base fee in bps as reported by Gamma (uninterpreted).
    pub maker_base_fee_bps: Option<Decimal>,
    pub taker_base_fee_bps: Option<Decimal>,
}

/// The canonical registry row: a normalized dip market with verification.
///
/// Field-by-field this mirrors `dip_markets`; the struct is the typed contract
/// downstream modules consume. Raw Gamma payloads stay in `markets`
/// (`gamma_meta_json`) - this type stores what the *registry* asserts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DipMarket {
    pub condition_id: String,
    /// Gamma numeric market id, when present in the payload.
    pub gamma_market_id: Option<String>,
    pub asset: Asset,

    /// Strike/barrier price from the contract text (title cross-checked).
    pub barrier_price: Decimal,
    /// Contract observation-window end, when the description states one
    /// explicitly. `endDate` alone is NOT substituted here (verified: the two
    /// differ - e.g. endDate 12:00Z vs contract window 23:59 ET).
    pub observation_end: Option<DateTime<Utc>>,
    /// Gamma `endDate` - the API deadline. Required (used as fallback expiry).
    pub end_date: DateTime<Utc>,

    pub yes_token_id: String,
    pub no_token_id: String,

    pub question: String,
    pub event_slug: Option<String>,
    pub event_title: Option<String>,
    /// Verbatim resolution/description text - never paraphrased.
    pub resolution_rule: String,
    pub oracle_source: Option<String>,

    pub tick_size: Decimal,
    pub min_order_size: Decimal,
    pub fees: FeeInfo,
    pub neg_risk: Option<bool>,

    pub status: MarketStatus,

    /// Where this record came from (e.g. `gamma:events?tag_slug=hit-price`).
    pub source_provenance: String,

    pub discovered_at: DateTime<Utc>,
    pub last_verified_at: DateTime<Utc>,
    /// Bumped on every metadata change; history rows capture before/after.
    pub metadata_version: u64,

    pub resolution_verification: ResolutionVerification,
}

impl DipMarket {
    /// Eligibility gate for downstream strategy consumption. Deliberately
    /// conservative: every condition must be positively proven.
    pub fn is_eligible(&self) -> bool {
        self.resolution_verification.status == ResolutionStatus::Verified
            && self.barrier_price > Decimal::ZERO
            && !self.yes_token_id.is_empty()
            && self.yes_token_id != self.no_token_id
            && !self.resolution_rule.trim().is_empty()
    }
}

/// Result of normalizing one Gamma market payload: either a registry record
/// or a structured rejection. Malformed payloads are rejections with a
/// reason, never panics and never silent drops.
#[derive(Debug, Clone, PartialEq)]
pub enum DipNormalization {
    Accepted(Box<DipMarket>),
    Rejected {
        condition_id: Option<String>,
        reason: RejectionReason,
        detail: String,
    },
}
