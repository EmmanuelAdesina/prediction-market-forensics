//! Fee-model tests: fixtures sampled from the source-verified corpus
//! (docs/02-fee-model-investigation.md). Every expectation below reproduces a
//! real observed fill from the wallet's history.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::error::IngestionError;
use crate::fees::*;

/// Sampled live fixture: taker sweep on a crypto dip ladder (fee-inclusive).
/// `Will Bitcoin reach $82,000 on October 28?` tx 0xb31294d51abab222b5...
#[test]
fn taker_crypto_sweep_is_fee_inclusive() {
    let (verdict, face) = classify_fill(
        dec!(100.258889),
        dec!(0.7109594043),
        dec!(72.72219),
        FeeCategory::Crypto,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedFeeInclusive);
    assert_eq!(face, FillFace::Taker);
    // gross + fee: 71.27999999 + 1.44220
    let fee = charged_fee(dec!(100.258889), dec!(0.7109594043), dec!(0.07));
    assert_eq!(fee, dec!(1.44220));
}

/// Sampled live fixture: the original 82-row fee-premium family member.
/// `Will Ethereum dip to $2,000 by December 31, 2026?` (crypto taker 0.07)
#[test]
fn fee_premium_family_member_resolves_to_taker_with_fee() {
    let (verdict, face) = classify_fill(
        dec!(4144.31),
        dec!(0.28),
        dec!(1218.8913), // observed usdc
        FeeCategory::Crypto,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedFeeInclusive);
    assert_eq!(face, FillFace::Taker);
    let fee = charged_fee(dec!(4144.31), dec!(0.28), dec!(0.07));
    assert_eq!(fee, dec!(58.48450));
    assert_eq!(dec!(0.28) * dec!(4144.31) + fee, dec!(1218.89130));
}

/// Sampled live fixture: finance-category taker (WTI ladder at 0.04).
#[test]
fn finance_category_taker_uses_004() {
    // `Will WTI Crude Oil (WTI) hit (LOW) $65 in October?` tx 0xe114afcd
    let (verdict, face) = classify_fill(
        dec!(26700.0),
        dec!(0.0369994509),
        dec!(1025.9387),
        FeeCategory::Finance,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedFeeInclusive);
    assert_eq!(face, FillFace::Taker);
}

/// Sampled live fixture: MAKER fill on a fee-category market.
/// `Will WTI Crude Oil (WTI) hit (HIGH) $115 in October?` tx 0xb3973625:
/// cash equals gross exactly though 71.70 would apply to a taker.
#[test]
fn maker_pays_no_fee_on_fee_category() {
    let (verdict, face) = classify_fill(
        dec!(39232.3),
        dec!(0.0479930063),
        dec!(1882.87602),
        FeeCategory::Finance,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedGrossNotional);
    assert_eq!(face, FillFace::Maker);
}

/// Minimum-fee rule: unrounded fee below 0.00001 is charged as zero.
#[test]
fn min_fee_below_quantum_is_dropped() {
    // extreme price: fee = 1 × 0.07 × 0.0001 × 0.9999 ≈ 7.0e-6 < 1e-5 → dropped
    let f = charged_fee(dec!(1.0), dec!(0.0001), dec!(0.07));
    assert_eq!(f, Decimal::ZERO);
    // boundary pair at the same extreme price:
    //   1.42 × 0.07 × 0.0001 × 0.9999 ≈ 9.939e-6  (< min → zero)
    //   1.43 × 0.07 × 0.0001 × 0.9999 ≈ 1.0009e-5 (≥ min → charges 0.00001)
    let below = charged_fee(dec!(1.42), dec!(0.0001), dec!(0.07));
    let above = charged_fee(dec!(1.43), dec!(0.0001), dec!(0.07));
    assert_eq!(below, Decimal::ZERO);
    assert_eq!(above, dec!(0.00001));
}

/// Float-noise envelope: residual exactly at 2e-5 still classifies inclusive.
#[test]
fn envelope_boundary_accepted() {
    let (verdict, _) = classify_fill(
        dec!(10.0),
        dec!(0.5),
        dec!(5.17502), // gross 5.0 + fee 0.175 = 5.175; shifted by +2e-5
        FeeCategory::Crypto,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedFeeInclusive);
}

/// Outside the envelope on a fee category: unresolved, never forced.
#[test]
fn inconsistent_row_is_unknown() {
    let (verdict, face) = classify_fill(
        dec!(100.0),
        dec!(0.3),
        dec!(40.0), // fee-inclusive would be 31.4; gross 30.0; anything else
        FeeCategory::Crypto,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::UnknownFeeSemantics);
    assert_eq!(face, FillFace::Indeterminate);
}

/// Fee-figure math: symmetric around 50c.
#[test]
fn fee_symmetry_around_half() {
    let a = charged_fee(dec!(1000.0), dec!(0.3), dec!(0.07));
    let b = charged_fee(dec!(1000.0), dec!(0.7), dec!(0.07));
    assert_eq!(a, b);
    assert_eq!(a, dec!(14.7));
}

/// Geopolitics category is fee-free.
#[test]
fn geopolitics_pays_nothing() {
    let (verdict, face) = classify_fill(
        dec!(500.0),
        dec!(0.6),
        dec!(300.0),
        FeeCategory::Geopolitics,
    )
    .expect("valid fill");
    assert_eq!(verdict, FillVerdict::VerifiedGrossNotional);
    assert_eq!(face, FillFace::Indeterminate);
}

/// Validation errors surface as IngestionError::Validation.
#[test]
fn invalid_inputs_rejected() {
    let err = classify_fill(dec!(0.0), dec!(0.3), dec!(1.0), FeeCategory::Crypto)
        .expect_err("zero shares must fail");
    assert!(matches!(err, IngestionError::Validation(_)));
    let err = classify_fill(dec!(10.0), dec!(1.2), dec!(12.0), FeeCategory::Crypto)
        .expect_err("price outside [0,1] must fail");
    assert!(matches!(err, IngestionError::Validation(_)));
}

#[test]
fn category_rates_match_published_schedule() {
    assert_eq!(FeeCategory::Crypto.taker_rate(), Some(dec!(0.07)));
    assert_eq!(FeeCategory::Finance.taker_rate(), Some(dec!(0.04)));
    assert_eq!(FeeCategory::Sports.taker_rate(), Some(dec!(0.05)));
    assert_eq!(FeeCategory::Politics.taker_rate(), Some(dec!(0.04)));
    assert_eq!(FeeCategory::Geopolitics.taker_rate(), Some(Decimal::ZERO));
    assert_eq!(FeeCategory::Unknown.taker_rate(), None);
}
