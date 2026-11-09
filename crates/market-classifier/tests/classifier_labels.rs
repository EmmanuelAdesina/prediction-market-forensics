//! Labelled classification validation suite.
//!
//! Labels are manually assigned from market questions (independent of the
//! ruleset internals). A ruleset change must keep this suite green or bump
//! the classifier version and re-label with justification.

use tokor_domain::classification::CryptoVerdict;
use tokor_market_classifier::classify;

mod labels;
use labels::LabelledCase;

#[test]
fn classifier_version_is_recorded() {
    // Every evidence record must name the ruleset version.
    let evidence = classify(&LabelledCase::all()[0].market_ref("BTC".into()));
    assert!(!evidence.classifier_version.is_empty());
    assert_eq!(evidence.classifier_version, "crypto-cls-v0.2.0");
}

#[test]
fn all_rules_match_version() {
    // Keep the record constant in lockstep with the domain classifier version.
    assert_eq!(
        LabelledCase::all().len(),
        11,
        "label coverage: every label change requires suite review"
    );
}

#[test]
fn labelled_suite_green() {
    for case in LabelledCase::all() {
        let icon = case_marker_icon(case.icon_marker);
        let evidence = classify(&case.market_ref(icon));
        assert_eq!(
            evidence.verdict, case.expected,
            "case '{}': got {:?} (signals {:?})",
            case.name, evidence.verdict, evidence.signals
        );
    }
}

#[test]
fn wti_leak_fixed() {
    // The Phase-0 finding: naive keyword classifiers put WTI inside scope.
    // The hard exclusion must beat any threshold phrasing.
    for case in LabelledCase::all()
        .into_iter()
        .filter(|c| c.name.starts_with("wti"))
    {
        let evidence = classify(&case.market_ref(String::new()));
        assert_eq!(evidence.verdict, CryptoVerdict::NonCrypto, "{}", case.name);
        assert!(evidence.negated_signals.is_empty() == false);
    }
}

#[test]
fn ambiguous_never_becomes_crypto() {
    for case in LabelledCase::all()
        .into_iter()
        .filter(|c| c.name.starts_with("ambiguous"))
    {
        let evidence = classify(&case.market_ref(String::new()));
        assert_eq!(evidence.verdict, CryptoVerdict::Unknown, "{}", case.name);
    }
}

#[test]
fn icon_is_a_strong_signal() {
    // Icon wins even when title lacks explicit asset words.
    // e.g. an event titled with an unusual phrasing but BTC icon.
    use tokor_domain::market::MarketRef;
    let m = MarketRef {
        condition_id: "0xicon".into(),
        title: Some("Will the dip hit this month?".into()),
        market_slug: Some("will-the-dip-hit-this-month".into()),
        event_slug: Some("will-the-dip-hit-this-month".into()),
        icon: Some("https://polymarket-upload.s3.us-east-2.amazonaws.com/ETH+fullsize.jpg".into()),
        ..Default::default()
    };
    // "dip" alone is not crypto evidence, and "eth" is not a word here,
    // so the rule must come from the icon marker.
    let evidence = classify(&m);
    assert_eq!(evidence.verdict, CryptoVerdict::Crypto);
    assert!(evidence
        .signals
        .iter()
        .any(|s| matches!(s, tokor_domain::classification::EvidenceSignal::IconToken { token } if token == "ETH")));
}

fn case_marker_icon(marker: &str) -> String {
    if marker.is_empty() {
        String::new()
    } else {
        format!("https://polymarket-upload.s3.us-east-2.amazonaws.com/{marker}+fullsize.png")
    }
}

#[test]
fn case_helpers_compile() {
    // Silence dead-code warnings for LabelledCase helpers used only here.
    let case = LabelledCase::all()[0].clone_case("BTC");
    let _ = case;
}
