//! Dip-market extraction/verification tests (Engineering Assignment 001).
//!
//! Cases 1–10 of the assignment test matrix live here, driven by real Gamma
//! payloads captured 2026-11 (see `fixtures/README.md`). Cases 11–16 are
//! integration tests in `tokor-persistence` and `tokor-application`.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokor_domain::dip::{Asset, DipNormalization, MarketStatus, RejectionReason, ResolutionStatus};
use tokor_market_classifier::dip::{
    determine_asset, is_dip_question, normalize_gamma_market, parse_barrier_from_title,
    GammaMarketInput,
};

fn load_fixture(name: &str) -> serde_json::Value {
    let raw = std::fs::read_to_string(format!(
        "{}/tests/fixtures/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("fixture present");
    serde_json::from_str(&raw).expect("fixture parses")
}

fn normalize(name: &str) -> DipNormalization {
    let fx = load_fixture(name);
    let tags: Vec<String> = fx["event_tags"]
        .as_array()
        .map(|a| a.iter().map(|t| t.as_str().unwrap().to_string()).collect())
        .unwrap_or_default();
    normalize_gamma_market(&GammaMarketInput {
        event_tags: &tags,
        event_slug: fx["event_slug"].as_str(),
        event_title: fx["event_title"].as_str(),
        market: &fx["market"],
    })
}

/// Case 1: valid BTC dip market - verified, eligible, touch-low semantics,
/// resolved YES in the wild (BTC touched $90,000 in Jan 2025).
#[test]
fn btc_dip_fixture_is_verified_and_eligible() {
    let norm = normalize("btc_dip_90k.json");
    let m = match &norm {
        DipNormalization::Accepted(m) => m,
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("BTC dip rejected: {reason:?} {detail}")
        }
    };
    assert_eq!(m.asset, Asset::Btc);
    assert_eq!(m.barrier_price, Decimal::from(90_000));
    assert_eq!(m.status, MarketStatus::ResolvedYes);
    assert_eq!(m.resolution_verification.status, ResolutionStatus::Verified);
    assert!(m.is_eligible());
    assert_eq!(
        m.oracle_source.as_deref(),
        Some("https://www.binance.com/en/trade/BTC_USDT")
    );
    assert_eq!(
        m.resolution_verification.source_pair.as_deref(),
        Some("BTCUSDT")
    );
    // Tokens: yes/no distinct, non-empty.
    assert!(!m.yes_token_id.is_empty() && m.yes_token_id != m.no_token_id);
    // Verbatim contract preserved.
    assert!(m
        .resolution_rule
        .contains("final \"Low\" price of $90,000 or lower"));
    // Observation window end: Dec 31 2025 23:59 ET (EST, UTC-5) → 04:59Z Jan 1.
    let window = m.observation_end.expect("window parsed");
    assert_eq!(window.to_rfc3339(), "2026-01-01T04:59:00+00:00");
}

/// Case 2: valid ETH dip market - verified; also proves the description may
/// state the barrier with decimals ("$3,000.00") while the title has none.
#[test]
fn eth_dip_fixture_is_verified_and_eligible() {
    let norm = normalize("eth_dip_monthly.json");
    let m = match &norm {
        DipNormalization::Accepted(m) => m,
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("ETH dip rejected: {reason:?} {detail}")
        }
    };
    assert_eq!(m.asset, Asset::Eth);
    assert_eq!(m.barrier_price, Decimal::from(3_000));
    assert_eq!(m.resolution_verification.status, ResolutionStatus::Verified);
    assert_eq!(
        m.resolution_verification.source_pair.as_deref(),
        Some("ETHUSDT")
    );
    assert!(m.is_eligible());
}

/// Case 3: unrelated cryptocurrency (SOL dip) - structurally rejected.
#[test]
fn solana_dip_is_rejected_unsupported_asset() {
    let norm = normalize("sol_dip_out_of_scope.json");
    match norm {
        DipNormalization::Rejected { reason, .. } => {
            assert_eq!(reason, RejectionReason::UnsupportedAsset);
        }
        DipNormalization::Accepted(m) => panic!("SOL accepted: {}", m.question),
    }
}

/// Case 4: non-cryptocurrency market - rejected before anything else.
#[test]
fn non_crypto_market_is_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["question"] = serde_json::json!("Will the Fed cut rates in March?");
    market["conditionId"] = serde_json::json!("0xnoncrypto");
    let tags: Vec<String> = vec!["fed".into(), "rates".into()];
    let norm = normalize_gamma_market(&GammaMarketInput {
        event_tags: &tags,
        event_slug: Some("fed-rates"),
        event_title: None,
        market: &market,
    });
    match norm {
        DipNormalization::Rejected { reason, .. } => {
            assert_eq!(reason, RejectionReason::UnsupportedAsset);
        }
        DipNormalization::Accepted(m) => panic!("non-crypto accepted: {}", m.question),
    }
}

/// Case 5: missing condition identifier.
#[test]
fn missing_condition_id_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["conditionId"] = serde_json::json!("");
    let norm = normalize_fixture_market(&fx, &market);
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::MissingConditionId,
            ..
        } => {}
        other => panic!("expected MissingConditionId, got {other:?}"),
    }
}

/// Case 6: missing outcome-token identifiers.
#[test]
fn missing_tokens_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["clobTokenIds"] = serde_json::json!(null);
    let norm = normalize_fixture_market(&fx, &market);
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::MissingTokens,
            ..
        } => {}
        other => panic!("expected MissingTokens, got {other:?}"),
    }
}

/// Case 6b: yes/no token ids must be distinct.
#[test]
fn identical_tokens_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["clobTokenIds"] = serde_json::json!("[\"111\", \"111\"]");
    let norm = normalize_fixture_market(&fx, &market);
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::MissingTokens,
            ..
        } => {}
        other => panic!("expected MissingTokens, got {other:?}"),
    }
}

/// Case 7: invalid barrier (no $-number in the title).
#[test]
fn invalid_barrier_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["question"] = serde_json::json!("Will Bitcoin dip to one hundred thousand this year?");
    let norm = normalize_fixture_market(&fx, &market);
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::InvalidBarrier,
            ..
        } => {}
        other => panic!("expected InvalidBarrier, got {other:?}"),
    }
}

/// Case 8: invalid deadline (endDate not a timestamp).
#[test]
fn invalid_deadline_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    market["endDate"] = serde_json::json!("soon-ish");
    let norm = normalize_fixture_market(&fx, &market);
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::InvalidDeadline,
            ..
        } => {}
        other => panic!("expected InvalidDeadline, got {other:?}"),
    }
}

/// Case 9: ambiguous resolution conditions - the description binds a
/// DIFFERENT barrier than the title. The market is RETAINED in the registry
/// (evidence preservation) but is AMBIGUOUS and never strategy-eligible,
/// even though its title looks like a perfectly valid dip market.
#[test]
fn ambiguous_barrier_rejected_despite_valid_title() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    let desc = market["description"].as_str().unwrap();
    market["description"] = serde_json::json!(desc.replace("$90,000 or lower", "$91,000 or lower"));
    let norm = normalize_fixture_market(&fx, &market);
    match &norm {
        DipNormalization::Accepted(m) => {
            assert_eq!(
                m.resolution_verification.status,
                ResolutionStatus::Ambiguous,
                "notes: {:?}",
                m.resolution_verification.notes
            );
            assert!(!m.is_eligible(), "ambiguous market must never be eligible");
            assert_eq!(
                m.resolution_verification.verifier_version,
                tokor_market_classifier::dip::DIP_VERIFIER_VERSION
            );
        }
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("ambiguous candidate should be retained, got {reason:?} {detail}")
        }
    }
}

/// Case 10: unverified resolution source - the Binance URL is absent (the
/// prose still mentions the exchange elsewhere), so the binding to an
/// observable source cannot be verified. Retained, AMBIGUOUS, ineligible.
#[test]
fn unverified_source_rejected() {
    let fx = load_fixture("btc_dip_90k.json");
    let mut market = fx["market"].clone();
    let desc = market["description"].as_str().unwrap();
    market["description"] = serde_json::json!(desc
        .replace("https://www.binance.com/en/trade/BTC_USDT", "")
        .replace(
            "The resolution source for this market is Binance, specifically the BTCUSDT",
            "The resolution source for this market is SomeExchange, specifically the BTCUSDT"
        ));
    let norm = normalize_fixture_market(&fx, &market);
    match &norm {
        DipNormalization::Accepted(m) => {
            assert_eq!(
                m.resolution_verification.status,
                ResolutionStatus::Ambiguous
            );
            assert_eq!(m.oracle_source, None, "no URL extracted");
            assert!(!m.is_eligible());
            assert!(m
                .resolution_verification
                .notes
                .iter()
                .any(|n| n.contains("no Binance resolution URL")));
        }
        DipNormalization::Rejected { reason, detail, .. } => {
            panic!("source-less candidate should be retained, got {reason:?} {detail}")
        }
    }
}

/// Reach (touch-high) markets are the complementary family - out of the
/// dip registry's scope.
#[test]
fn reach_market_out_of_scope() {
    let norm = normalize("btc_reach_out_of_scope.json");
    match norm {
        DipNormalization::Rejected {
            reason: RejectionReason::UnsupportedAsset,
            ..
        } => {}
        other => panic!("expected UnsupportedAsset rejection, got {other:?}"),
    }
}

/// Asset determination conflicts are rejections, never guesses.
#[test]
fn asset_conflict_is_rejected() {
    let err = determine_asset(
        "Will Bitcoin dip to $90,000?",
        "resolves on Binance ETHUSDT 1 minute candles",
        &["bitcoin".into()],
    );
    assert!(err.is_err(), "conflicting signals must error");
    // No signal at all also errors.
    assert!(determine_asset("Will it dip?", "", &[]).is_err());
}

/// Barrier parser: thousands separators, decimals, first-match.
#[test]
fn barrier_parsing() {
    assert_eq!(
        parse_barrier_from_title("Will Bitcoin dip to $90,000 by December 31, 2025?"),
        Some(Decimal::from(90_000))
    );
    assert_eq!(
        parse_barrier_from_title("Will Ethereum dip to $1,500.50 in August?"),
        Some(dec!(1500.50))
    );
    assert_eq!(
        parse_barrier_from_title("Will XRP dip to $1.00 in November?"),
        Some(dec!(1.00))
    );
    assert_eq!(parse_barrier_from_title("no barrier here"), None);
    // "October 5-11" must not parse as a barrier (no $ anchor).
    assert_eq!(
        parse_barrier_from_title("Will Bitcoin dip to $76,000 October 5-11?"),
        Some(Decimal::from(76_000))
    );
}

/// Dip-question shape: the family gate.
#[test]
fn dip_question_shape() {
    assert!(is_dip_question(
        "Will Bitcoin dip to $90,000 by December 31, 2025?"
    ));
    assert!(is_dip_question("will ethereum dip below $1,000"));
    assert!(!is_dip_question(
        "Will Bitcoin reach $1,000,000 by December 31, 2025?"
    ));
    assert!(!is_dip_question("Will Bitcoin be up in August?"));
}

fn normalize_fixture_market(
    fx: &serde_json::Value,
    market: &serde_json::Value,
) -> DipNormalization {
    let tags: Vec<String> = fx["event_tags"]
        .as_array()
        .map(|a| a.iter().map(|t| t.as_str().unwrap().to_string()).collect())
        .unwrap_or_default();
    normalize_gamma_market(&GammaMarketInput {
        event_tags: &tags,
        event_slug: fx["event_slug"].as_str(),
        event_title: fx["event_title"].as_str(),
        market,
    })
}
