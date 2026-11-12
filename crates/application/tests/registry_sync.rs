//! Registry sync-loop integration tests (assignment cases 14–16):
//! repeated synchronization idempotency, malformed API responses, and
//! partial API failures - driven by fixture providers, no network.

use std::time::Duration;

use serde_json::json;
use tokor_domain::dip::Asset;
use tokor_persistence::dip_registry::DipRegistryRepository;
use tokor_persistence::Database;

use tokor_application::registry::{sync_core, SyncTuning, Universe};

/// Minimal valid BTC dip market payload (contract template verified against
/// live Gamma responses; see market-classifier tests/fixtures for real ones).
fn valid_btc_market(condition_id: &str) -> serde_json::Value {
    json!({
        "conditionId": condition_id,
        "question": "Will Bitcoin dip to $90,000 by December 31, 2025?",
        "description": "This market will immediately resolve to \"Yes\" if any Binance 1 minute candle for Bitcoin (BTCUSDT) between December 30, 2024, 20:00 and December 31, 2025, 23:59 in the ET timezone has a final \"Low\" price of $90,000 or lower. Otherwise, this market will resolve to \"No.\"\n\nThe resolution source for this market is Binance, specifically the BTCUSDT \"Low\" prices available at https://www.binance.com/en/trade/BTC_USDT, with the chart settings on \"1m\" for one-minute candles selected on the top bar.",
        "outcomes": "[\"Yes\", \"No\"]",
        "clobTokenIds": "[\"111111\", \"222222\"]",
        "endDate": "2025-12-31T12:00:00Z",
        "orderPriceMinTickSize": 0.001,
        "orderMinSize": 5,
        "active": true,
        "closed": false,
        "outcomePrices": "[\"0.5\", \"0.5\"]"
    })
}

fn event_with_markets(slug: &str, markets: Vec<serde_json::Value>) -> serde_json::Value {
    json!({
        "slug": slug,
        "title": slug,
        "tags": [
            {"slug": "crypto"}, {"slug": "hit-price"}, {"slug": "bitcoin"}
        ],
        "markets": markets,
    })
}

/// Case 14 + 11: repeated synchronization - first run creates, second run
/// changes nothing.
#[test]
fn repeated_synchronization_is_idempotent() {
    let db = Database::open_memory().unwrap();
    let page = vec![event_with_markets(
        "btc-dips",
        vec![valid_btc_market("0xbtc1")],
    )];

    let fetch = |closed: bool, _offset: u64| -> Result<Vec<serde_json::Value>, String> {
        if closed {
            Ok(vec![])
        } else {
            Ok(page.clone())
        }
    };

    let run1 = sync_core(
        &db,
        &[Asset::Btc, Asset::Eth],
        Universe::Active,
        SyncTuning {
            page_limit: 100,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "run-1",
    )
    .unwrap();
    assert_eq!(run1.counts.new_count, 1, "{:#?}", run1.counts);
    assert_eq!(run1.counts.unchanged_count, 0);
    assert_eq!(run1.counts.errors.len(), 0);

    let run2 = sync_core(
        &db,
        &[Asset::Btc, Asset::Eth],
        Universe::Active,
        SyncTuning {
            page_limit: 100,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "run-2",
    )
    .unwrap();
    assert_eq!(run2.counts.new_count, 0);
    assert_eq!(run2.counts.unchanged_count, 1);
    assert_eq!(run2.counts.updated_count, 0);
    let (total, eligible) = db.count_dip_markets().unwrap();
    assert_eq!((total, eligible), (1, 1), "no duplicate rows");
}

/// Case 15: malformed API responses - missing condition ids, events without
/// markets arrays, garbage token encodings - are recorded, never panic,
/// never silently dropped.
#[test]
fn malformed_api_responses_are_recorded() {
    let db = Database::open_memory().unwrap();
    let page = vec![
        // Event missing its markets array entirely.
        json!({"slug": "no-markets", "tags": []}),
        // Market missing conditionId.
        event_with_markets(
            "bad-cond",
            vec![json!({
                "question": "Will Bitcoin dip to $80,000?",
                "outcomes": "[\"Yes\", \"No\"]",
                "clobTokenIds": "[\"1\", \"2\"]",
                "endDate": "2026-12-31T00:00:00Z",
                "description": "resolves on Binance BTCUSDT ... $80,000 or lower ... https://www.binance.com/en/trade/BTC_USDT",
            })],
        ),
        // Market with unparseable token encoding.
        event_with_markets(
            "bad-tokens",
            vec![json!({
                "conditionId": "0xbadtokens",
                "question": "Will Bitcoin dip to $80,000?",
                "outcomes": "not-json-at-all",
                "clobTokenIds": "[\"1\", \"2\"]",
                "endDate": "2026-12-31T00:00:00Z",
                "description": "Binance BTCUSDT $80,000 or lower https://www.binance.com/en/trade/BTC_USDT",
            })],
        ),
        // One good market so the run has something to persist.
        event_with_markets("good", vec![valid_btc_market("0xgood")]),
    ];

    let fetch = |_closed: bool, _offset: u64| -> Result<Vec<serde_json::Value>, String> {
        Ok(page.clone())
    };

    let report = sync_core(
        &db,
        &[Asset::Btc, Asset::Eth],
        Universe::Active,
        SyncTuning {
            page_limit: 100,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "malformed-run",
    )
    .unwrap();

    assert_eq!(report.counts.markets_seen, 3, "{:#?}", report.counts);
    assert_eq!(report.counts.discovered, 1);
    assert_eq!(report.counts.new_count, 1);
    assert_eq!(report.counts.rejected_count, 2);
    assert!(report.counts.errors.len() >= 3, "each defect recorded");
    // Rejection reasons are keyed and counted.
    assert_eq!(
        report
            .counts
            .rejections
            .get("missing_condition_id")
            .copied(),
        Some(1)
    );
    assert_eq!(
        report.counts.rejections.get("missing_tokens").copied(),
        Some(1)
    );
    // The good market survived.
    let (total, _) = db.count_dip_markets().unwrap();
    assert_eq!(total, 1);
}

/// Case 16: partial API failures - a provider error on page 2 stops the run
/// cleanly, keeps page-1 data, and records the failure.
#[test]
fn partial_api_failures_stop_cleanly_and_keep_data() {
    let db = Database::open_memory().unwrap();
    let page1 = vec![event_with_markets("p1", vec![valid_btc_market("0xp1")])];
    // The second fetch errors before any page-2 content matters, so no
    // page-2 fixture exists - the error path is what's under test.
    let mut calls = 0u32;

    let fetch = move |_closed: bool, _offset: u64| -> Result<Vec<serde_json::Value>, String> {
        calls += 1;
        if calls == 1 {
            Ok(page1.clone())
        } else {
            Err("connection reset by peer".to_string())
        }
    };

    // page_limit = 1 makes page 1 "full" (len == limit), so pagination
    // actually attempts a second fetch - which fails.
    let report = sync_core(
        &db,
        &[Asset::Btc, Asset::Eth],
        Universe::Active,
        SyncTuning {
            page_limit: 1,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "partial-run",
    )
    .unwrap();

    assert_eq!(report.counts.pages_fetched, 1, "stopped after the error");
    assert_eq!(report.counts.new_count, 1, "page-1 data persisted");
    assert!(report
        .counts
        .errors
        .iter()
        .any(|e| e.contains("connection reset by peer")));
    // The failure is visible in the run record.
    let (total, _) = db.count_dip_markets().unwrap();
    assert_eq!(total, 1);
}

/// Out-of-scope assets and non-requested assets are counted, not stored.
#[test]
fn asset_filter_counts_out_of_scope() {
    let db = Database::open_memory().unwrap();
    let sol = event_with_markets(
        "sol-dips",
        vec![json!({
            "conditionId": "0xsol",
            "question": "Will Solana dip to $140 in May?",
            "description": "Binance SOLUSDT $140 or lower https://www.binance.com/en/trade/SOL_USDT",
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": "[\"9\", \"8\"]",
            "endDate": "2026-06-01T00:00:00Z",
        })],
    );
    // NOTE: sol tags come from the event; this fixture uses bitcoin tags so
    // asset determination needs the description pair to carry it.
    let page = vec![json!({
        "slug": "sol-dips",
        "title": "sol-dips",
        "tags": [{"slug": "solana"}, {"slug": "hit-price"}],
        "markets": [sol["markets"][0]],
    })];

    let fetch = |_closed: bool, _offset: u64| -> Result<Vec<serde_json::Value>, String> {
        Ok(page.clone())
    };

    let report = sync_core(
        &db,
        &[Asset::Btc, Asset::Eth],
        Universe::Active,
        SyncTuning {
            page_limit: 100,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "scope-run",
    )
    .unwrap();

    assert_eq!(report.counts.rejected_count, 1);
    assert_eq!(
        report.counts.rejections.get("unsupported_asset").copied(),
        Some(1)
    );
    let (total, _) = db.count_dip_markets().unwrap();
    assert_eq!(total, 0, "out-of-scope assets never enter the registry");
}

/// Pagination: full pages keep fetching until a short page arrives.
#[test]
fn pagination_stops_at_short_page() {
    let db = Database::open_memory().unwrap();
    let fetch = |_closed: bool, offset: u64| -> Result<Vec<serde_json::Value>, String> {
        if offset == 0 {
            // Full page (page_limit = 2 in this test).
            Ok(vec![
                event_with_markets("a", vec![valid_btc_market("0xa")]),
                event_with_markets("b", vec![valid_btc_market("0xb")]),
            ])
        } else {
            // Short page ends the segment.
            Ok(vec![event_with_markets("c", vec![valid_btc_market("0xc")])])
        }
    };

    let report = sync_core(
        &db,
        &[Asset::Btc],
        Universe::Active,
        SyncTuning {
            page_limit: 2,
            max_pages: 10,
            page_pause: Duration::ZERO,
        },
        fetch,
        "pager-run",
    )
    .unwrap();

    assert_eq!(report.counts.pages_fetched, 2);
    assert_eq!(report.counts.new_count, 3);
}

/// max_pages bounds the loop even when pages never end.
#[test]
fn max_pages_bounds_sync() {
    let db = Database::open_memory().unwrap();
    // Every page is exactly `page_limit` events (never a short page), so the
    // only thing that can stop the loop is max_pages.
    let fetch = |_closed: bool, _offset: u64| -> Result<Vec<serde_json::Value>, String> {
        Ok(vec![
            event_with_markets("x", vec![valid_btc_market("0xx")]),
            event_with_markets("y", vec![valid_btc_market("0xy")]),
        ])
    };

    let report = sync_core(
        &db,
        &[Asset::Btc],
        Universe::Active,
        // page_limit 2: every page is "full"; max_pages 3 is the only stop.
        SyncTuning {
            page_limit: 2,
            max_pages: 3,
            page_pause: Duration::ZERO,
        },
        fetch,
        "bounded-run",
    )
    .unwrap();

    assert_eq!(report.counts.pages_fetched, 3, "{:#?}", report.counts);
    assert!(
        report
            .counts
            .errors
            .iter()
            .any(|e| e.contains("max_pages=3")),
        "the bound must be recorded: {:#?}",
        report.counts.errors
    );
    // Distinct condition ids per page would duplicate 0xx/0xy across pages -
    // idempotent upserts keep the registry at 2 rows.
    let (total, _) = db.count_dip_markets().unwrap();
    assert_eq!(total, 2);
}
