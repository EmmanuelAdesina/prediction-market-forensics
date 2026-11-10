//! Book-engine invariants: snapshot/delta reconciliation, gap handling,
//! staleness, ordering. These are the §11 mandatory tests for the recorder.

use std::time::Duration;

use rust_decimal_macros::dec;

use crate::book::{ApplyOutcome, BookEngine, BookQuality, GapReason};
use crate::messages::{parse_market_message, subscribe_body};

const TOKEN: &str = "847681450197861234";

fn book_frame(hash: &str, ts: &str) -> String {
    format!(
        r#"{{"event_type":"book","asset_id":"{TOKEN}","hash":"{hash}","timestamp":"{ts}",
        "bids":[{{"price":"0.46","size":"100"}},{{"price":"0.45","size":"250"}}],
        "asks":[{{"price":"0.48","size":"120"}},{{"price":"0.49","size":"80"}}]}}"#
    )
}

fn delta_frame(hash: &str, ts: &str, side: &str, price: &str, size: &str) -> String {
    format!(
        r#"{{"event_type":"price_change","asset_id":"{TOKEN}","hash":"{hash}","timestamp":"{ts}",
        "changes":[{{"price":"{price}","side":"{side}","size":"{size}"}}]}}"#
    )
}

fn parse_book(hash: &str) -> crate::messages::BookMsg {
    book_at(hash, "1000")
}

fn book_at(hash: &str, ts: &str) -> crate::messages::BookMsg {
    match parse_market_message(&book_frame(hash, ts)) {
        crate::messages::MarketMessage::Book(b) => b,
        _ => panic!("book frame must parse"),
    }
}

fn delta_at(
    hash: &str,
    ts: &str,
    side: &str,
    price: &str,
    size: &str,
) -> crate::messages::PriceChangeMsg {
    match parse_market_message(&delta_frame(hash, ts, side, price, size)) {
        crate::messages::MarketMessage::PriceChange(p) => p,
        _ => panic!("delta frame must parse"),
    }
}

fn parse_delta(hash: &str, side: &str, price: &str, size: &str) -> crate::messages::PriceChangeMsg {
    delta_at(hash, "1001", side, price, size)
}

/// A delta before any snapshot must NOT fabricate a book.
#[test]
fn delta_without_snapshot_needs_snapshot() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    assert!(eng.state().is_none());
    let out = eng.apply_price_change(&parse_delta("h1", "BUY", "0.45", "10"), 1);
    assert_eq!(
        out,
        ApplyOutcome::NeedsSnapshot(GapReason::NoBaseState),
        "never build a book from incomplete deltas"
    );
    assert!(eng.is_stale(1_000_000_000), "no state = stale");
}

/// Snapshot establishes the base state; deltas apply with hash continuity.
#[test]
fn snapshot_then_delta_applies_in_order() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    let out = eng.apply_snapshot(&parse_book("h1"), 1_000);
    let st = match out {
        ApplyOutcome::Normalized(s) => s,
        other => panic!("expected normalized, got {other:?}"),
    };
    assert_eq!(st.quality, BookQuality::Snapshot);
    assert_eq!(st.best_bid().unwrap().price, dec!(0.46));
    assert_eq!(st.best_ask().unwrap().price, dec!(0.48));

    // consume the whole best ask via delta
    let out = eng.apply_price_change(&parse_delta("h2", "SELL", "0.48", "0"), 2_000);
    let st = match out {
        ApplyOutcome::Normalized(s) => s,
        other => panic!("expected normalized, got {other:?}"),
    };
    assert_eq!(st.quality, BookQuality::DeltaConsistent);
    assert_eq!(st.best_ask().unwrap().price, dec!(0.49), "level removed");
    assert!(st.best_bid().unwrap().size == dec!(100));
}

/// Timestamp regression drops decision-readiness and demands a snapshot; the
/// refreshed snapshot is labeled RECONCILED_AFTER_GAP.
#[test]
fn timestamp_gap_blocks_decisions_until_snapshot() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    eng.apply_snapshot(&book_at("h1", "2000"), 1_000);
    assert!(eng.is_decision_ready(1_500));

    // a delta stamped BEFORE the last applied event ⇒ missed messages
    let out = eng.apply_price_change(&delta_at("h9", "1500", "BUY", "0.45", "10"), 2_000);
    assert_eq!(
        out,
        ApplyOutcome::NeedsSnapshot(GapReason::TimestampRegression {
            last: "2000".into(),
            got: "1500".into()
        })
    );
    assert!(
        !eng.is_decision_ready(2_500),
        "gap-pending state must not drive decisions"
    );

    // refresh: the snapshot after a gap is labeled, never plain SNAPSHOT
    let out = eng.apply_snapshot(&book_at("h10", "3000"), 3_000);
    let st = match out {
        ApplyOutcome::Normalized(s) => s,
        other => panic!("expected normalized, got {other:?}"),
    };
    assert_eq!(st.quality, BookQuality::ReconciledAfterGap);
    assert!(eng.is_decision_ready(3_500), "continuity restored");
    // and a normal forward delta still applies
    let out = eng.apply_price_change(&delta_at("h11", "3500", "SELL", "0.49", "0"), 4_000);
    assert!(matches!(out, ApplyOutcome::Normalized(_)));
}

/// Reconnect invalidation: the retained state must not decide until a fresh
/// snapshot arrives.
#[test]
fn reconnect_invalidation_requires_fresh_snapshot() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    eng.apply_snapshot(&parse_book("1000"), 1_000);
    assert!(eng.is_decision_ready(1_500));
    eng.invalidate();
    assert!(
        !eng.is_decision_ready(1_600),
        "post-reconnect stale integrity"
    );
    let out = eng.apply_snapshot(&parse_book("2000"), 2_000);
    match out {
        ApplyOutcome::Normalized(s) => assert_eq!(s.quality, BookQuality::ReconciledAfterGap),
        other => panic!("expected normalized, got {other:?}"),
    }
}

/// Messages for other tokens are ignored by this engine (multi-token fan-out).
#[test]
fn foreign_asset_messages_are_ignored() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    let mut foreign = parse_book("h1");
    foreign.asset_id = "999".into();
    assert_eq!(eng.apply_snapshot(&foreign, 1), ApplyOutcome::Ignored);
}

/// Staleness: fresh within the bound, stale beyond it.
#[test]
fn staleness_bound_is_respected() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    eng.apply_snapshot(&parse_book("h1"), 0);
    assert!(!eng.is_stale(5_000_000_000), "5s < 10s bound");
    assert!(eng.is_stale(11_000_000_000), "11s > 10s bound");
}

/// Level ordering: bids best-first (desc), asks best-first (asc); a delta's
/// `size` is the NEW aggregate size for the level (protocol semantics:
/// replace, never sum).
#[test]
fn level_ordering_and_replacement() {
    let mut eng = BookEngine::new(TOKEN, Duration::from_secs(10));
    let msg = parse_book("h1");
    eng.apply_snapshot(&msg, 1_000);
    let out = eng.apply_price_change(&parse_delta("h2", "BUY", "0.46", "50"), 2_000);
    let st = match out {
        ApplyOutcome::Normalized(s) => s,
        other => panic!("expected normalized, got {other:?}"),
    };
    assert_eq!(
        st.best_bid().unwrap().size,
        dec!(50),
        "size replaced, not summed"
    );
    let bids: Vec<_> = st.bids.iter().map(|l| l.price).collect();
    let asks: Vec<_> = st.asks.iter().map(|l| l.price).collect();
    let mut sorted = bids.clone();
    sorted.sort();
    sorted.reverse();
    assert_eq!(bids, sorted, "bids desc");
    let mut sorted = asks.clone();
    sorted.sort();
    assert_eq!(asks, sorted, "asks asc");
}

/// Tick-size and last-trade frames parse; unknown frames survive as Other.
#[test]
fn wire_frames_parse_tolerantly() {
    let ts = r#"{"event_type":"tick_size_change","asset_id":"T","old_tick_size":0.01,"new_tick_size":0.001}"#;
    assert!(matches!(
        parse_market_message(ts),
        crate::messages::MarketMessage::TickSize(_)
    ));
    let lt = r#"{"event_type":"last_trade_price","asset_id":"T","price":"0.47","size":"12","side":"BUY"}"#;
    assert!(matches!(
        parse_market_message(lt),
        crate::messages::MarketMessage::LastTrade(_)
    ));
    let unknown = r#"{"event_type":"future_thing","x":1}"#;
    assert!(matches!(
        parse_market_message(unknown),
        crate::messages::MarketMessage::Other(_)
    ));
    let garbage = "not json at all";
    assert!(matches!(
        parse_market_message(garbage),
        crate::messages::MarketMessage::Other(_)
    ));
}

/// Subscription body uses the protocol's exact field spelling.
#[test]
fn subscribe_body_uses_assets_ids() {
    let body = subscribe_body(&[TOKEN.into()]);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v.get("assets_ids").is_some(), "protocol spelling");
    assert!(v.get("asset_ids").is_none());
    assert_eq!(v["type"], "market");
}
