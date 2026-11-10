//! Activity-event normalization tests: adversarial dedup/identity invariants.

use rust_decimal::Decimal;
use serde_json::json;

use crate::activity_events::*;

/// Two genuinely identical maker fills inside one tx must get DISTINCT keys,
/// and the assignment must be invariant under input reordering.
#[test]
fn identical_rows_in_one_tx_get_distinct_stable_identities() {
    let mk = |raw_json: String| {
        let raw: ActivityEventRaw = serde_json::from_str(&raw_json).unwrap();
        activity_event_from_raw(&raw, &raw_json).unwrap()
    };
    // Two identical BUYs of 10 shares @ $0.05 in the SAME transaction.
    let row = json!({
        "proxy_wallet": "0x00000000000000000000000000000000000000A1",
        "timestamp": 1_760_000_000i64,
        "type": "TRADE", "side": "BUY",
        "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
        "token_id": "847681450197861234",
        "size": 10.0, "usdc_size": 0.5, "price": 0.05,
        "outcome_index": 0, "outcome": "Yes",
        "title": "t", "slug": "s", "event_slug": "e",
        "transaction_hash": "0xaaa1",
    })
    .to_string();

    // Forward order
    let mut a = vec![mk(row.clone()), mk(row.clone())];
    assign_occurrence_ordinals(&mut a);
    // Reversed order
    let mut b = vec![mk(row.clone()), mk(row)];
    assign_occurrence_ordinals(&mut b);

    assert_ne!(a[0].event_key, a[1].event_key, "distinct occurrences");
    assert_eq!(a[0].occurrence, 0);
    assert_eq!(a[1].occurrence, 1);
    // Order-invariance: same multiset of keys either way.
    let mut ka = [a[0].event_key.clone(), a[1].event_key.clone()];
    ka.sort();
    let mut kb = [b[0].event_key.clone(), b[1].event_key.clone()];
    kb.sort();
    assert_eq!(ka, kb, "identities invariant under input order");
}

/// Distinct rows never collide; identical rows across DIFFERENT transactions
/// do not share identity.
#[test]
fn distinct_content_and_distinct_txs_are_distinct_identities() {
    let mk = |raw_json: String| {
        let raw: ActivityEventRaw = serde_json::from_str(&raw_json).unwrap();
        activity_event_from_raw(&raw, &raw_json).unwrap()
    };
    let row = |tx: &str, size: f64| {
        json!({
            "proxy_wallet": "0x00000000000000000000000000000000000000A1",
            "timestamp": 1_760_000_000i64,
            "type": "TRADE", "side": "SELL",
            "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
            "token_id": "847681450197861234",
            "size": size, "usdc_size": size * 0.4, "price": 0.4,
            "outcome_index": 1, "outcome": "No",
            "title": "t", "slug": "s", "event_slug": "e",
            "transaction_hash": tx,
        })
        .to_string()
    };
    let mut events = vec![
        mk(row("0x1", 5.0)),
        mk(row("0x1", 6.0)),
        mk(row("0x2", 5.0)),
    ];
    assign_occurrence_ordinals(&mut events);
    let keys: std::collections::HashSet<&String> = events.iter().map(|e| &e.event_key).collect();
    assert_eq!(keys.len(), 3, "no cross-row collisions");
    assert!(events.iter().all(|e| e.occurrence == 0));
}

/// MERGE semantics: size == usdc enforced; token_id may be empty.
#[test]
fn merge_rows_require_size_equals_usdc() {
    let ok = json!({
        "proxy_wallet": "0x00000000000000000000000000000000000000A1",
        "timestamp": 1_760_000_000i64, "type": "MERGE",
        "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
        "token_id": "", "size": 1061.6005, "usdc_size": 1061.6005,
        "transaction_hash": "0xm1",
    });
    let raw: ActivityEventRaw = serde_json::from_str(&ok.to_string()).unwrap();
    let ev = activity_event_from_raw(&raw, &ok.to_string()).unwrap();
    assert_eq!(ev.size, Some(Decimal::new(10616005, 4)));
    assert_eq!(ev.usdc_size, ev.size);

    let bad = json!({
        "proxy_wallet": "0x00000000000000000000000000000000000000A1",
        "timestamp": 1_760_000_000i64, "type": "MERGE",
        "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
        "token_id": "", "size": 10.0, "usdc_size": 9.0,
        "transaction_hash": "0xm2",
    });
    let raw: ActivityEventRaw = serde_json::from_str(&bad.to_string()).unwrap();
    assert!(activity_event_from_raw(&raw, &bad.to_string()).is_err());
}

/// Cash-only rows (MAKER_REBATE) normalize with usdc only.
#[test]
fn cash_only_rows_normalize() {
    let row = json!({
        "proxy_wallet": "0x00000000000000000000000000000000000000A1",
        "timestamp": 1_760_000_000i64, "type": "MAKER_REBATE",
        "usdc_size": 1.234567,
        "transaction_hash": "0xr1",
    });
    let raw: ActivityEventRaw = serde_json::from_str(&row.to_string()).unwrap();
    let ev = activity_event_from_raw(&raw, &row.to_string()).unwrap();
    assert!(ev.event_type.is_cash_only_event());
    assert_eq!(ev.usdc_size, Some(Decimal::new(1234567, 6)));
    assert!(ev.size.is_none());
}

/// Full-page parse: cursor semantics + invalid-row surfacing.
#[test]
fn page_parse_surfaces_invalid_and_preserves_cursor() {
    let body = json!({
        "data": [
            {
                "proxy_wallet": "0x00000000000000000000000000000000000000A1",
                "timestamp": 1_760_000_000i64, "type": "REDEEM",
                "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
                "token_id": "999", "size": 100.0, "usdc_size": 0.0,
                "transaction_hash": "0xr9",
            },
            {
                // invalid: negative cash
                "proxy_wallet": "0x00000000000000000000000000000000000000A1",
                "timestamp": 1_760_000_000i64, "type": "YIELD",
                "usdc_size": -5.0, "transaction_hash": "0xy1",
            },
        ],
        "pagination": {"has_more": true, "next_cursor": "LArI", "limit": 500},
    });
    let batch = parse_activity_batch(&body).unwrap();
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.invalid, 1, "invalid rows are surfaced, not dropped");
    assert_eq!(batch.next_cursor.as_deref(), Some("LArI"));
    assert!(batch.has_more);
    let ev = &batch.events[0];
    assert_eq!(ev.event_type.as_str(), "REDEEM");
    // usdc == 0 preserved verbatim (never derived here).
    assert_eq!(ev.usdc_size, Some(Decimal::ZERO));
}
