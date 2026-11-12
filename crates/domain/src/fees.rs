//! Polymarket fee model - implemented exactly per the published schedule
//! (help.polymarket.com "Trading Fees", 2026-07-10) and verified against
//! source observations (docs/02-fee-model-investigation.md).
//!
//! Semantics (all source-verified on 13,948 unique fills; see Phase 1.5):
//! - Fee applies to the TAKER side of a match: `fee = C × feeRate × p × (1−p)`,
//!   with C = shares and p = execution price (the displayed `price`).
//! - Makers pay zero. A fee-category market fill whose observed cash equals
//!   gross notional exactly is therefore a maker fill.
//! - Fee-bearing categories: crypto 0.07, finance 0.04 (WTI/gold ladders),
//!   sports 0.05, politics 0.04, economics/culture/weather/other 0.05,
//!   mentions/tech 0.04, geopolitics 0.
//! - Rounding: the backend serializes cash with 5-decimal precision using
//!   binary-float arithmetic; the observed residual envelope
//!   `cash − gross − round5(fee)` is |r| ≤ 2×10⁻⁵ (2 units of the last
//!   decimal place), strictly ≤ 0 in the exact-decimal case (floor at
//!   serialization, with HALF_UP ties split by float representation).
//! - Minimum fee: unrounded fee < 0.00001 USDC is dropped entirely.
//!
//! Every fill whose cash equals gross within the float-noise envelope is
//! classified VERIFIED_GROSS_NOTIONAL (fee-free category, min-fee-zero, or
//! maker fill on a fee category); else VERIFIED_FEE_INCLUSIVE if cash fits
//! gross + fee within the envelope. Everything else stays unverified.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::error::IngestionError;

/// Published fee precision: 5 decimals.
pub const FEE_QUANTUM: Decimal = dec!(0.00001);
/// Minimum fee charged; anything smaller is dropped by the exchange.
pub const MIN_FEE: Decimal = dec!(0.00001);
/// Backend binary-float rounding envelope for the serialized cash amount.
/// Two units of the last decimal place (empirically bounded by 1.31e-5).
pub const FLOAT_NOISE_ENVELOPE: Decimal = dec!(0.00002);

/// Numeric fee rate per category, exactly as published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeCategory {
    Crypto,
    Finance,
    Sports,
    Politics,
    Economics,
    Culture,
    Weather,
    OtherGeneral,
    Mentions,
    Tech,
    Geopolitics,
    /// Not resolvable from retrieved metadata.
    Unknown,
}

impl FeeCategory {
    pub fn taker_rate(&self) -> Option<Decimal> {
        let r = match self {
            FeeCategory::Crypto => dec!(0.07),
            FeeCategory::Finance => dec!(0.04),
            FeeCategory::Sports => dec!(0.05),
            FeeCategory::Politics => dec!(0.04),
            FeeCategory::Economics => dec!(0.05),
            FeeCategory::Culture => dec!(0.05),
            FeeCategory::Weather => dec!(0.05),
            FeeCategory::OtherGeneral => dec!(0.05),
            FeeCategory::Mentions => dec!(0.04),
            FeeCategory::Tech => dec!(0.04),
            FeeCategory::Geopolitics => dec!(0),
            FeeCategory::Unknown => return None,
        };
        Some(r)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            FeeCategory::Crypto => "crypto",
            FeeCategory::Finance => "finance",
            FeeCategory::Sports => "sports",
            FeeCategory::Politics => "politics",
            FeeCategory::Economics => "economics",
            FeeCategory::Culture => "culture",
            FeeCategory::Weather => "weather",
            FeeCategory::OtherGeneral => "other",
            FeeCategory::Mentions => "mentions",
            FeeCategory::Tech => "tech",
            FeeCategory::Geopolitics => "geopolitics",
            FeeCategory::Unknown => "unknown",
        }
    }
}

/// Unrounded fee per the official formula.
pub fn unrounded_fee(shares: Decimal, price: Decimal, rate: Decimal) -> Decimal {
    shares * rate * price * (Decimal::ONE - price)
}

/// The fee charged by the exchange for a taker fill:
/// rounded-half-up to 5 decimals; the unrounded value below the published
/// minimum (0.00001 USDC) is charged as exactly zero.
pub fn charged_fee(shares: Decimal, price: Decimal, rate: Decimal) -> Decimal {
    let raw = unrounded_fee(shares, price, rate);
    if raw < MIN_FEE {
        return Decimal::ZERO;
    }
    raw.round_dp(5)
}

/// Cash debit for a taker fill: gross + charged fee. The backend keeps the
/// displayed cash within the float-noise envelope of this value; the source
/// `usdc_size` remains the authoritative realized-cash amount for accounting.
pub fn expected_taker_cash(gross: Decimal, fee: Decimal) -> Decimal {
    gross + fee
}

/// Cash debit for a maker fill: gross notional, no fee.
pub fn expected_maker_cash(gross: Decimal) -> Decimal {
    gross
}

/// Classification of one observed fill (source `usdc_size` authoritative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillVerdict {
    /// Cash equals gross notional exactly (float-noise band): maker fill,
    /// min-fee-zero taker, or fee-free category.
    VerifiedGrossNotional,
    /// Cash equals gross + fee within the observed float envelope.
    VerifiedFeeInclusive,
    /// Fee-category market, cash fits neither: unresolved.
    UnknownFeeSemantics,
    /// No category metadata and no gross fit.
    Unexplained,
}

/// The side of the wallet in the match, when derivable from observed semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillFace {
    /// Wallet order rested and was hit (published: makers pay zero).
    Maker,
    /// Wallet order crossed the book and paid the fee.
    Taker,
    /// Not derivable (e.g. fee-free category with no fee applied at all).
    Indeterminate,
}

/// Reconcile one fill row per the fee model.
/// Returns the verdict plus the derived face (maker/taker) when determinable.
#[allow(clippy::too_many_arguments)]
pub fn classify_fill(
    shares: Decimal,
    price: Decimal,
    observed_cash: Decimal,
    category: FeeCategory,
) -> Result<(FillVerdict, FillFace), IngestionError> {
    if shares <= Decimal::ZERO || price < Decimal::ZERO || price > Decimal::ONE {
        return Err(IngestionError::Validation(format!(
            "fill out of range: shares {shares} price {price}"
        )));
    }
    let gross = shares * price;
    match category.taker_rate() {
        None => {
            if (observed_cash - gross).abs() <= FLOAT_NOISE_ENVELOPE {
                Ok((FillVerdict::VerifiedGrossNotional, FillFace::Indeterminate))
            } else {
                Ok((FillVerdict::Unexplained, FillFace::Indeterminate))
            }
        }
        Some(rate) => {
            let fee_unrounded = unrounded_fee(shares, price, rate);
            if fee_unrounded < MIN_FEE {
                // min-fee-zero: cash == gross. Face is only determinable as
                // Taker when the category actually charges fees at larger
                // sizes; a zero-rate category is always indeterminate.
                let face = if rate > Decimal::ZERO
                    && (observed_cash - gross).abs() <= FLOAT_NOISE_ENVELOPE
                {
                    FillFace::Taker
                } else {
                    FillFace::Indeterminate
                };
                return Ok((FillVerdict::VerifiedGrossNotional, face));
            }
            let fee = charged_fee(shares, price, rate);
            if (observed_cash - gross).abs() <= FLOAT_NOISE_ENVELOPE {
                // Fee-bearing category, cash shows no fee: maker fill.
                // (Taker fills on fee categories were never observed without a fee.)
                Ok((FillVerdict::VerifiedGrossNotional, FillFace::Maker))
            } else if (observed_cash - expected_taker_cash(gross, fee)).abs()
                <= FLOAT_NOISE_ENVELOPE
            {
                Ok((FillVerdict::VerifiedFeeInclusive, FillFace::Taker))
            } else {
                Ok((FillVerdict::UnknownFeeSemantics, FillFace::Indeterminate))
            }
        }
    }
}
