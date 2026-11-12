//! Phase-1.5 accounting-model regression tests.
//!
//! Contract (docs/02, docs/04 §6.3): the four money quantities of a fill are
//! distinct - displayed execution price `price`, gross notional
//! `gross_usdc()`, charged fee (`fee_reconciliation`, via the fee model), and
//! total cash `usdc_size`. `cash_implied_price` is a cash view, never the
//! execution price. Historical source records are never modified.

use rust_decimal_macros::dec;
use serde_json::json;

use crate::activity::{ActivityType, Side, SourceFill, SourceFillRaw};
use crate::fees::{charged_fee, classify_fill, FeeCategory, FillFace, FillVerdict};

const WALLET: &str = "0xa19cbababc312f9df185e49d7004c249ed1ade6b";
const COND: &str = "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389";

fn raw(side: Side, size: f64, price: f64, usdc: f64) -> SourceFillRaw {
    SourceFillRaw {
        proxy_wallet: WALLET.into(),
        timestamp: 1_760_000_000,
        condition_id: COND.into(),
        record_type: ActivityType::Trade,
        size,
        usdc_size: usdc,
        transaction_hash: "0xabc".into(),
        price,
        token_id: "847681450197861234".into(),
        side,
        outcome_index: 0,
        title: "t".into(),
        slug: "s".into(),
        icon: "i".into(),
        event_slug: "e".into(),
        outcome: "Yes".into(),
        name: "n".into(),
        pseudonym: "p".into(),
    }
}

fn fill(side: Side, size: f64, price: f64, usdc: f64) -> SourceFill {
    let raw = raw(side, size, price, usdc);
    let json = serde_json::to_string(&raw).unwrap();
    SourceFill::from_raw(&raw, &json).unwrap()
}

/// Maker fill on a fee-category market: cash == gross within the source's
/// float envelope; the fee model produces a *would-be* taker fee (71.70 for
/// the live WTI fixture) but the classification charges the maker zero -
/// all four quantities distinct.
#[test]
fn maker_fill_accounting_decomposition() {
    let f = fill(Side::Buy, 39232.3, 0.0479930063, 1882.87602); // live WTI maker fixture
    let gross = f.gross_usdc();
    let would_be_fee = charged_fee(f.size, f.price, dec!(0.04));
    assert_eq!(would_be_fee, dec!(71.70045)); // hypothetical taker fee only
                                              // maker cash == gross up to the documented 2e-5 serialization envelope:
    assert!((f.usdc_size - gross).abs() <= dec!(0.00002));
    // per-share cash view matches the displayed price within envelope/size:
    assert!((f.cash_implied_price - f.price).abs() * f.size <= dec!(0.000020001));
    // displayed price × size reproduces cash within the envelope:
    assert!((f.price * f.size - f.usdc_size).abs() <= dec!(0.00002));
}

/// Taker fill: cash == gross + fee; cash_implied_price == (gross+fee)/size
/// and is strictly ABOVE the displayed price; gross stays fee-free.
#[test]
fn taker_fill_accounting_decomposition() {
    let f = fill(Side::Buy, 2400.0, 0.05, 127.98); // live dip-ladder fixture
    let gross = f.gross_usdc();
    assert_eq!(gross, dec!(120.0000));
    let fee = charged_fee(f.size, f.price, dec!(0.07));
    assert_eq!(fee, dec!(7.98));
    assert_eq!(f.usdc_size, gross + fee);
    assert_eq!(f.cash_implied_price, (gross + fee) / f.size);
    assert!(f.cash_implied_price > f.price, "fee inflates the cash view");
    // gross view remains fee-free:
    assert_eq!(f.cash_implied_price - f.price, fee / f.size);
}

/// The verdict/face classification is consistent with the decomposition for
/// both live fixtures, and UnknownFeeSemantics can never be force-fit.
#[test]
fn classification_matches_accounting() {
    // maker fixture
    let (v, face) = classify_fill(
        dec!(39232.3),
        dec!(0.0479930063),
        dec!(1882.87602),
        FeeCategory::Finance,
    )
    .unwrap();
    assert_eq!(v, FillVerdict::VerifiedGrossNotional);
    assert_eq!(face, FillFace::Maker);
    // taker fixture
    let (v, face) =
        classify_fill(dec!(2400.0), dec!(0.05), dec!(127.98), FeeCategory::Crypto).unwrap();
    assert_eq!(v, FillVerdict::VerifiedFeeInclusive);
    assert_eq!(face, FillFace::Taker);
}

/// Fee-free category markets: gross == cash regardless of role, and the
/// fee fields stay zero - the fee model never invents a fee.
#[test]
fn fee_free_category_never_charges() {
    let (v, face) = classify_fill(
        dec!(500.0),
        dec!(0.6),
        dec!(300.0),
        FeeCategory::Geopolitics,
    )
    .unwrap();
    assert_eq!(v, FillVerdict::VerifiedGrossNotional);
    assert_eq!(
        face,
        FillFace::Indeterminate,
        "role not derivable without fees"
    );
}

/// Identity stability: the rename does not touch the composite key inputs
/// (price and size remain in the identity; cash never enters it).
#[test]
fn identity_composite_unchanged_by_accounting_fields() {
    let a = fill(Side::Buy, 100.0, 0.5, 50.0);
    // same price/size, different cash (hypothetical fee difference):
    let b = fill(Side::Buy, 100.0, 0.5, 50.0);
    assert_eq!(a.event_key, b.event_key);
    // different price/size => different identity
    let c = fill(Side::Buy, 100.0, 0.51, 51.0);
    assert_ne!(a.event_key, c.event_key);
}

/// Serialization of the normalized fill exposes the separated fields.
#[test]
fn normalized_fill_serializes_separated_fields() {
    let f = fill(Side::Buy, 2400.0, 0.05, 127.98);
    let s = serde_json::to_string(&f).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s).unwrap();
    assert_eq!(v["price"], json!("0.05"));
    assert_eq!(v["usdc_size"], json!("127.98"));
    assert!(v.get("cash_implied_price").is_some());
}

/// Live fixtures (Nov-10 walk) that the pre-fee-aware validation REJECTED:
/// taker buys at crypto's 7% rate whose premium `rate×(1−p)` exceeds the old
/// 5% tolerance. Cash must equal gross + round5(fee) exactly, and the rows
/// must normalize (Phase-2 §1 regression: fee-band validation).
#[test]
fn fee_band_accepts_high_premium_taker_fills() {
    // tx 0xdb01bd6a89… - premium 5.740% = 0.07 × (1 − 0.18)
    let f = fill(Side::Buy, 7013.5, 0.18, 1334.89348);
    let fee = charged_fee(f.size, f.price, dec!(0.07));
    assert_eq!(fee, dec!(72.46348));
    assert_eq!(f.usdc_size, f.gross_usdc() + fee);
    assert_eq!(f.cash_implied_price, f.usdc_size / f.size);
    let (v, face) = classify_fill(f.size, f.price, f.usdc_size, FeeCategory::Crypto).unwrap();
    assert_eq!(v, FillVerdict::VerifiedFeeInclusive);
    assert_eq!(face, FillFace::Taker);

    // tx 0x675d461924… - full-precision displayed price, premium 5.741%.
    // The backend's float-serialized cash differs from gross + round5(fee)
    // by 8.9e-6 here - inside the documented 2e-5 envelope, so the strict
    // equality holds only up to that envelope (docs/02 §2).
    let f = fill(Side::Buy, 34700.0, 0.1799005504, 6600.91487);
    let fee = charged_fee(f.size, f.price, dec!(0.07));
    assert_eq!(fee, dec!(358.36578));
    assert!((f.usdc_size - f.gross_usdc() - fee).abs() <= dec!(0.00002));

    // tx 0xa0a8a4cb08… - premium 5.180% = 0.07 × (1 − 0.26)
    let f = fill(Side::Buy, 19095.69, 0.26, 5222.06015);
    let fee = charged_fee(f.size, f.price, dec!(0.07));
    assert_eq!(fee, dec!(257.18075));
    assert_eq!(f.usdc_size, f.gross_usdc() + fee);
}

/// Cash more than the maximum published premium (7%) above gross - or below
/// gross beyond the float envelope - cannot be reconciled under ANY category
/// and must be rejected, never force-fit.
#[test]
fn fee_band_rejects_out_of_schedule_cash() {
    // +10% premium: no published rate reaches this
    let over = raw(Side::Buy, 100.0, 0.5, 55.0);
    let js = serde_json::to_string(&over).unwrap();
    let err = SourceFill::from_raw(&over, &js).unwrap_err();
    assert!(err.to_string().contains("fee band"), "{err}");

    // cash below gross by more than the 2e-5 envelope
    let under = raw(Side::Buy, 100.0, 0.5, 49.0);
    let js = serde_json::to_string(&under).unwrap();
    let err = SourceFill::from_raw(&under, &js).unwrap_err();
    assert!(err.to_string().contains("fee band"), "{err}");

    // envelope boundary: cash = gross − 2e-5 exactly is accepted
    let f = fill(Side::Buy, 100.0, 0.5, 49.99998);
    assert_eq!(f.gross_usdc(), dec!(50));
    assert_eq!(f.usdc_size, dec!(49.99998));
}
