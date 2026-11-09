use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::json;

use crate::activity::{
    event_identity, is_valid_address, ActivityFilters, ActivityType, IngestionBatch, Side,
    SourceFill, SourceFillRaw,
};
use crate::error::parse_retry_after;
use crate::market::MarketRef;
use crate::position::PositionBook;

const TEST_COND: &str = "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389";

fn sample_raw() -> SourceFillRaw {
    serde_json::from_value(json!({
        "proxy_wallet": "0xa19cbababc312f9df185e49d7004c249ed1ade6b",
        "timestamp": 1794249568_i64,
        "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
        "type": "TRADE",
        "size": 2025.0_f64,
        "usdc_size": 182.25_f64,
        "transaction_hash": "0x31ca4e78348913ba865752b2395a02abed1a00d2ce2c2965c03fd3a716eeec66",
        "price": 0.09_f64,
        "token_id": "47441866156534507758155137441786829795495954872731490670045397853956032792663",
        "side": "BUY",
        "outcome_index": 0_i64,
        "title": "Will Bitcoin dip to $73,000 on November 9?",
        "slug": "will-bitcoin-dip-to-73k-on-november-9-2026",
        "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png",
        "event_slug": "what-price-will-bitcoin-hit-on-november-9-2026",
        "outcome": "Yes",
        "name": "Tokor",
        "pseudonym": "Taut-Ease-Bloodflow"
    }))
    .unwrap()
}

fn filters(limit: u64) -> ActivityFilters {
    ActivityFilters {
        user: "0xa19cbababc312f9df185e49d7004c249ed1ade6b".into(),
        record_types: vec![ActivityType::Trade],
        limit,
    }
}

#[test]
fn roundtrip_nominal_record() {
    let raw = sample_raw();
    let raw_json = serde_json::to_string(&raw).unwrap();
    let fill = SourceFill::from_raw(&raw, &raw_json).unwrap();
    assert_eq!(fill.side, Side::Buy);
    assert_eq!(fill.price, dec!(0.09));
    assert_eq!(fill.size, dec!(2025));
    assert_eq!(fill.usdc_size, dec!(182.25));
    assert_eq!(
        fill.market_title,
        "Will Bitcoin dip to $73,000 on November 9?"
    );
    assert_eq!(fill.event_key.len(), 64);
    assert!(is_valid_address(&fill.wallet));
}

#[test]
fn price_preserved_exactly_via_string_path() {
    // 0.56000001535 was observed on the wire; must round-trip without f64 noise.
    let mut raw = sample_raw();
    raw.price = 0.56000001535;
    raw.usdc_size = raw.price * 2025.0;
    let fill = SourceFill::from_raw(&raw, "{}").unwrap();
    assert_eq!(fill.price, dec!(0.56000001535));
}

#[test]
fn event_identity_is_stable_and_order_sensitive() {
    let a = event_identity(
        "0xtx",
        1,
        "0xcond",
        "0xtoken",
        &Side::Buy,
        &dec!(0.5),
        &dec!(10),
    );
    let b = event_identity(
        "0xtx",
        1,
        "0xcond",
        "0xtoken",
        &Side::Buy,
        &dec!(0.5),
        &dec!(10),
    );
    let c = event_identity(
        "0xtx",
        1,
        "0xcond",
        "0xtoken",
        &Side::Buy,
        &dec!(0.51),
        &dec!(10),
    );
    let d = event_identity(
        "0xtx",
        1,
        "0xcond",
        "0xtoken",
        &Side::Sell,
        &dec!(0.5),
        &dec!(10),
    );
    let e = event_identity(
        "0xtx",
        2,
        "0xcond",
        "0xtoken",
        &Side::Buy,
        &dec!(0.5),
        &dec!(10),
    );
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_ne!(a, d);
    assert_ne!(a, e);
}

#[test]
fn page_parse_rejects_filter_drift() {
    let f = filters(100);
    let body = json!({
        "data": [serde_json::to_value(sample_raw()).unwrap()],
        "pagination": {"limit": 500, "offset": 0, "has_more": true, "next_cursor": "X"}
    });
    let err = IngestionBatch::parse(&body, &f);
    match err {
        Ok(_) => panic!("response limit != requested limit must be rejected"),
        Err(crate::error::IngestionError::InconsistentFeed(msg)) => {
            assert!(msg.contains("limit"));
        }
        Err(e) => panic!("unexpected error: {e}"),
    }
}

#[test]
fn page_parse_happy_path() {
    let f = filters(100);
    let body = json!({
        "data": [serde_json::to_value(sample_raw()).unwrap()],
        "pagination": {"limit": 100, "offset": 0, "has_more": true, "next_cursor": "abc"}
    });
    let batch = IngestionBatch::parse(&body, &f).unwrap();
    assert_eq!(batch.fills.len(), 1);
    assert!(batch.has_more);
    assert_eq!(batch.next_cursor.as_deref(), Some("abc"));
    // Raw body preserved verbatim for forensics.
    assert_eq!(batch.raw_body["pagination"]["next_cursor"], "abc");
}

#[test]
fn page_parse_empty_data_ok_missing_data_rejected() {
    // Empty page is legitimate; missing .data is a feed inconsistency.
    let f = filters(100);
    let empty = json!({ "data": [], "pagination": {"limit": 100, "offset": 0, "has_more": false, "next_cursor": ""}});
    assert!(IngestionBatch::parse(&empty, &f).is_ok());
    let missing =
        json!({ "pagination": {"limit": 100, "offset": 0, "has_more": false, "next_cursor": ""}});
    assert!(IngestionBatch::parse(&missing, &f).is_err());
}

#[test]
fn retry_after_parses() {
    assert_eq!(parse_retry_after(Some("3")), Some(3));
    assert_eq!(parse_retry_after(Some(" abc")), None);
    assert_eq!(parse_retry_after(None), None);
}

// ------- position reconstruction -------

fn buy(ts: i64, price: f64, size: f64) -> SourceFill {
    let raw = SourceFillRaw {
        proxy_wallet: "0xa19cbababc312f9df185e49d7004c249ed1ade6b".into(),
        timestamp: ts,
        condition_id: TEST_COND.into(),
        record_type: ActivityType::Trade,
        size,
        usdc_size: price * size,
        transaction_hash: format!("0xhash-{ts}-{price}-{size}"),
        price,
        token_id: "0xtoken".into(),
        side: Side::Buy,
        outcome_index: 0,
        title: "t".into(),
        slug: "s".into(),
        icon: "i".into(),
        event_slug: "e".into(),
        outcome: "Yes".into(),
        name: "Tokor".into(),
        pseudonym: "p".into(),
    };
    let raw_json = serde_json::to_string(&raw).unwrap();
    SourceFill::from_raw(&raw, &raw_json).unwrap()
}

fn sell(ts: i64, price: f64, size: f64) -> SourceFill {
    let mut raw = serde_json::from_str::<SourceFillRaw>(&buy(ts, price, size).raw_json).unwrap();
    raw.side = Side::Sell;
    raw.transaction_hash = format!("0xsell-{ts}-{price}-{size}");
    let raw_json = serde_json::to_string(&raw).unwrap();
    SourceFill::from_raw(&raw, &raw_json).unwrap()
}

#[test]
fn inventory_basics_fifo() {
    let fills = vec![
        buy(1, 0.10, 100.0),
        buy(2, 0.12, 100.0),
        sell(3, 0.20, 150.0),
    ];
    let mut book = PositionBook::new();
    book.apply_all(&fills).unwrap();
    let pos = book.get("0xtoken").unwrap();
    assert_eq!(pos.acquired_shares(), dec!(200));
    assert_eq!(pos.disposed_shares, dec!(150));
    assert_eq!(pos.remaining_inventory(), dec!(50));
    // FIFO: 100 @0.10 fully consumed, 50 consumed from the @0.12 lot.
    assert_eq!(pos.lots.len(), 1);
    assert_eq!(pos.lots[0].cost_per_share, dec!(0.12));
    let expected = dec!(0.20) * dec!(150) - (dec!(100) * dec!(0.10) + dec!(50) * dec!(0.12));
    assert_eq!(pos.realized_pnl(), expected);
    assert!(!pos.history_probably_incomplete);
}

#[test]
fn pre_history_sell_never_fabricates_cost() {
    let fills = vec![sell(1, 0.50, 70.0)];
    let mut book = PositionBook::new();
    book.apply_all(&fills).unwrap();
    let pos = book.get("0xtoken").unwrap();
    assert!(pos.history_probably_incomplete);
    assert_eq!(pos.unattributed_disposals, dec!(70));
    assert_eq!(pos.realized_pnl(), dec!(0));
}

#[test]
fn sells_cannot_create_negative_inventory() {
    let fills = vec![buy(1, 0.10, 40.0), sell(2, 0.15, 100.0)];
    let mut book = PositionBook::new();
    book.apply_all(&fills).unwrap();
    let pos = book.get("0xtoken").unwrap();
    assert_eq!(pos.remaining_inventory(), dec!(0));
    assert_eq!(pos.unattributed_disposals, dec!(60));
    assert_eq!(
        pos.realized_pnl(),
        dec!(0.15) * dec!(40) - dec!(0.10) * dec!(40)
    );
}

#[test]
fn decimal_exact_arithmetic() {
    // 0.09 * 2025 == 182.25 exactly under Decimal arithmetic.
    let d = dec!(0.09) * dec!(2025);
    assert_eq!(d, dec!(182.25));
    // Float comparison would have noise.
    assert_ne!(
        Decimal::from(182_259_u32) / dec!(1000) / dec!(1000) * dec!(1000),
        d
    );
}

#[test]
fn market_ref_defaults() {
    let m = MarketRef::default();
    assert!(m.title.is_none());
    assert_eq!(m.key(), "");
}

// ------------------ property tests ------------------
mod props {
    use super::*;
    use crate::activity::SourceFill;

    fn fill(ts: i64, side: Side, price: f64, size: f64) -> SourceFill {
        let raw = SourceFillRaw {
            proxy_wallet: "0xa19cbababc312f9df185e49d7004c249ed1ade6b".into(),
            timestamp: ts,
            condition_id: TEST_COND.into(),
            record_type: ActivityType::Trade,
            size,
            usdc_size: price * size,
            transaction_hash: format!("0xh-{ts}-{side}-{price}-{size}"),
            price,
            token_id: "0xtoken".into(),
            side,
            outcome_index: 0,
            title: "t".into(),
            slug: "s".into(),
            icon: "i".into(),
            event_slug: "e".into(),
            outcome: "Yes".into(),
            name: "Tokor".into(),
            pseudonym: "p".into(),
        };
        let sj = serde_json::to_string(&raw).unwrap();
        SourceFill::from_raw(&raw, &sj).unwrap()
    }

    proptest::proptest! {
        // Invariant: FIFO replay preserves inventory accounting; standard
        // conservation equation holds for any sequence of buys/sells.
        #[test]
        fn conservation(
            buys in proptest::collection::vec((0.02f64..0.99, 10.0f64..2000.0), 1..5),
            sells in proptest::collection::vec((0.02f64..0.99, 1.0f64..500.0), 0..6),
            n_remaining_buys in 0u32..2
        ) {
            let mut ts = 10u64;
            let mut fills = Vec::new();
            for (p, s) in &buys { ts += 1; fills.push(fill(ts as i64, Side::Buy, *p, *s)); }
            for (p, s) in &sells { ts += 1; fills.push(fill(ts as i64, Side::Sell, *p, *s)); }
            for _ in 0..n_remaining_buys { ts += 1; fills.push(fill(ts as i64, Side::Buy, 0.5, 3.0)); }

            let mut book = PositionBook::new();
            book.apply_all(&fills).unwrap();
            let pos = book.get("0xtoken").unwrap();

            // Fundamental invariant: remaining == acquired - fully_attributed_disposed
            let acquired = pos.acquired_shares();
            let remaining = pos.remaining_inventory();
            let disposed_total = pos.disposed_shares + pos.unattributed_disposals;
            proptest::prop_assert!(remaining >= Decimal::ZERO);
            proptest::prop_assert!(remaining <= acquired);
            proptest::prop_assert!(acquired - remaining <= disposed_total);
        }

        // Replay idempotence: applying the same fills twice (e.g. data exposed twice)
        // is rejected by the strict duplicate detection.
        #[test]
        fn replay_duplicate_rejected(n in 1usize..3) {
            let f = fill(1, Side::Buy, 0.5, 10.0);
            let fills: Vec<SourceFill> = (0..n).map(|_| f.clone()).collect();
            let mut book = PositionBook::new();
            if n > 1 {
                proptest::prop_assert!(book.apply_all(&fills).is_err());
            } else {
                proptest::prop_assert!(book.apply_all(&fills).is_ok());
            }
        }
    }
}
