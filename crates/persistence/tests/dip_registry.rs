//! Registry persistence integration tests (assignment cases 11–13):
//! duplicate discovery idempotency, metadata-change history, resolved-market
//! retention, and full row roundtrip fidelity.

use tokor_domain::dip::{Asset, DipNormalization, ResolutionStatus};
use tokor_market_classifier::dip::normalize_gamma_market;
use tokor_market_classifier::dip::GammaMarketInput;
use tokor_persistence::dip_registry::{DipRegistryRepository, UpsertOutcome};
use tokor_persistence::Database;

fn fixture_market(name: &str) -> tokor_domain::dip::DipMarket {
    let raw = std::fs::read_to_string(format!(
        "{}/../market-classifier/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("fixture present");
    let fx: serde_json::Value = serde_json::from_str(&raw).expect("fixture parses");
    let tags: Vec<String> = fx["event_tags"]
        .as_array()
        .map(|a| a.iter().map(|t| t.as_str().unwrap().to_string()).collect())
        .unwrap_or_default();
    match normalize_gamma_market(&GammaMarketInput {
        event_tags: &tags,
        event_slug: fx["event_slug"].as_str(),
        event_title: fx["event_title"].as_str(),
        market: &fx["market"],
    }) {
        DipNormalization::Accepted(m) => *m,
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("fixture {name} rejected: {reason:?} {detail}")
        }
    }
}

/// Case 11: repeated discovery of the same market is a no-op.
#[test]
fn duplicate_market_discovery_is_idempotent() {
    let db = Database::open_memory().unwrap();
    let m = fixture_market("btc_dip_90k.json");

    assert_eq!(db.upsert_dip_market(&m).unwrap(), UpsertOutcome::New);
    assert_eq!(db.upsert_dip_market(&m).unwrap(), UpsertOutcome::Unchanged);
    assert_eq!(db.upsert_dip_market(&m).unwrap(), UpsertOutcome::Unchanged);

    let (total, eligible) = db.count_dip_markets().unwrap();
    assert_eq!((total, eligible), (1, 1));
    assert_eq!(db.history_len(&m.condition_id).unwrap(), 0);
    assert_eq!(db.metadata_version(&m.condition_id).unwrap(), 1);
}

/// Case 12: metadata changes append history and bump the version; the
/// original row is never destructively overwritten.
#[test]
fn metadata_change_creates_history_and_bumps_version() {
    let db = Database::open_memory().unwrap();
    let m = fixture_market("btc_dip_90k.json");
    db.upsert_dip_market(&m).unwrap();

    // Simulate a Gamma metadata drift: tick size changes 0.001 → 0.01.
    let mut drifted = m.clone();
    drifted.tick_size = rust_decimal::Decimal::from_str_exact("0.01").unwrap();
    assert_eq!(
        db.upsert_dip_market(&drifted).unwrap(),
        UpsertOutcome::Updated(1)
    );
    assert_eq!(db.metadata_version(&m.condition_id).unwrap(), 2);
    assert_eq!(db.history_len(&m.condition_id).unwrap(), 1);

    // Reverting back to the original metadata is another change (history
    // records both directions - before/after are explicit).
    assert_eq!(db.upsert_dip_market(&m).unwrap(), UpsertOutcome::Updated(1));
    assert_eq!(db.metadata_version(&m.condition_id).unwrap(), 3);
    assert_eq!(db.history_len(&m.condition_id).unwrap(), 2);
}

/// Case 13: resolved markets are retained (backtest labels stay available).
#[test]
fn resolved_market_retained() {
    let db = Database::open_memory().unwrap();
    let m = fixture_market("btc_dip_90k.json"); // resolved YES in the wild
    assert!(m.status.is_resolved());
    db.upsert_dip_market(&m).unwrap();

    assert_eq!(db.count_resolved().unwrap(), 1);
    let rows = db.list_dip_markets(Some(Asset::Btc), false, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, m.status);
}

/// Full roundtrip: every persisted field reads back identical.
#[test]
fn row_roundtrip_preserves_all_fields() {
    let db = Database::open_memory().unwrap();
    let m = fixture_market("eth_dip_monthly.json");
    db.upsert_dip_market(&m).unwrap();

    let rows = db.list_dip_markets(Some(Asset::Eth), true, 10).unwrap();
    assert_eq!(rows.len(), 1);
    let back = &rows[0];
    assert_eq!(back.condition_id, m.condition_id);
    assert_eq!(back.asset, m.asset);
    assert_eq!(back.barrier_price, m.barrier_price);
    assert_eq!(back.observation_end, m.observation_end);
    assert_eq!(back.end_date, m.end_date);
    assert_eq!(back.yes_token_id, m.yes_token_id);
    assert_eq!(back.no_token_id, m.no_token_id);
    assert_eq!(back.question, m.question);
    assert_eq!(back.resolution_rule, m.resolution_rule);
    assert_eq!(back.oracle_source, m.oracle_source);
    assert_eq!(back.tick_size, m.tick_size);
    assert_eq!(back.min_order_size, m.min_order_size);
    assert_eq!(back.fees, m.fees);
    assert_eq!(back.status, m.status);
    assert_eq!(back.metadata_version, m.metadata_version);
    assert_eq!(
        back.resolution_verification.status,
        ResolutionStatus::Verified
    );
    assert_eq!(
        back.resolution_verification.notes,
        m.resolution_verification.notes
    );
    assert_eq!(
        back.resolution_verification.source_pair,
        m.resolution_verification.source_pair
    );
    assert_eq!(back.discovered_at, m.discovered_at);
    assert_eq!(back.last_verified_at, m.last_verified_at);
    assert_eq!(back, &m, "full structural equality");
}

/// Ambiguous candidates are retained but never counted eligible.
#[test]
fn ambiguous_candidate_retained_ineligible() {
    let db = Database::open_memory().unwrap();
    let mut m = fixture_market("btc_dip_90k.json");
    let desc = m
        .resolution_rule
        .replace("$90,000 or lower", "$91,000 or lower");
    m.resolution_rule = desc.clone();
    let verification = tokor_market_classifier::dip::normalize_gamma_market(&GammaMarketInput {
        event_tags: &["bitcoin".to_string()],
        event_slug: None,
        event_title: None,
        market: &serde_json::json!({
            "conditionId": m.condition_id,
            "question": m.question,
            "description": desc,
            "outcomes": "[\"Yes\", \"No\"]",
            "clobTokenIds": format!("[\"{}\", \"{}\"]", m.yes_token_id, m.no_token_id),
            "endDate": m.end_date.to_rfc3339(),
            "orderPriceMinTickSize": 0.001,
            "orderMinSize": 5,
            "closed": true,
            "active": true,
            "outcomePrices": "[\"1\", \"0\"]",
        }),
    });
    let m2 = match verification {
        DipNormalization::Accepted(m) => *m,
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("should be retained: {reason:?} {detail}")
        }
    };
    db.upsert_dip_market(&m2).unwrap();

    let (total, eligible) = db.count_dip_markets().unwrap();
    assert_eq!(total, 1);
    assert_eq!(eligible, 0, "ambiguous must never be eligible");
    assert_eq!(
        m2.resolution_verification.status,
        ResolutionStatus::Ambiguous
    );
    let rows = db.list_dip_markets(None, true, 10).unwrap();
    assert!(rows.is_empty(), "eligible-only listing excludes ambiguous");
}
