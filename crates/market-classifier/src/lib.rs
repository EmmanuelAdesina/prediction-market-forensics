//! Deterministic crypto-scope classifier.
//!
//! Two-layer rules over locally verifiable signals:
//! 1. **Hard exclusion**: commodity / FX / equity language → `NonCrypto`.
//!    Threshold phrasing is shared with WTI / gold / stock markets, and naive
//!    keyword matching on threshold language overmatches (see the WTI leak in
//!    the Phase 0 data). The exclusion layer wins first, always.
//! 2. **Crypto affirmation**: at least one reliable crypto signal — icon
//!    filename markers ("BTC", "ETH"), event tags, or word-boundary asset
//!    naming in title/slug/description.
//!
//! Everything else → `Unknown`: out of scope by default, queued for review.
//!
//! Verdicts are pure functions of their inputs; every decision records its
//! evidence and the ruleset version.

use tokor_domain::classification::{
    ClassifiedVerdict, ClassifierEvidence, CryptoVerdict, EvidenceSignal,
};
use tokor_domain::market::MarketRef;

/// Known crypto asset markers (word-boundary matched).
const CRYPTO_TERMS: &[&str] = &[
    "bitcoin",
    "btc",
    "ethereum",
    "eth",
    "solana",
    "sol",
    "xrp",
    "ripple",
    "dogecoin",
    "doge",
    "cardano",
    "bnb",
    "chainlink",
    "sui",
    "zcash",
    "monero",
    "hyperliquid",
];

/// Hard exclusion terms: commodities / FX / equities / macro / sports.
const NON_CRYPTO_TERMS: &[&str] = &[
    "wti",
    "oil",
    "crude",
    "brent",
    "gold",
    "silver",
    "platinum",
    "copper",
    "wheat",
    "corn",
    "eurusd",
    "usdjpy",
    "dxy",
    "nvidia",
    "tesla",
    "nasdaq",
    "dow-",
    "inflation",
    "rate-cut",
    "powell",
    "election",
    "world series",
    "nba",
    "soccer",
    "premier league",
    "cricket",
];

/// Icon filename markers observed on Polymarket crypto events.
const CRYPTO_ICON_MARKERS: &[&str] = &[
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "BNB", "SUI", "ZEC", "XMR", "HYPE", "LINK",
];

/// Main entry: classify one market reference.
pub fn classify(market: &MarketRef) -> ClassifierEvidence {
    let title = market.title.clone().unwrap_or_default().to_lowercase();
    let market_slug = market
        .market_slug
        .clone()
        .unwrap_or_default()
        .to_lowercase();
    let event_slug = market.event_slug.clone().unwrap_or_default().to_lowercase();
    let description = market
        .description
        .clone()
        .unwrap_or_default()
        .to_lowercase();
    let icon = market.icon.clone().unwrap_or_default();
    let tags = market.tags.clone().unwrap_or_default();

    // ---- Layer 1: hard exclusions ---------------------------------------
    let mut negated: Vec<EvidenceSignal> = Vec::new();
    for term in NON_CRYPTO_TERMS {
        for (src, name) in [
            (&title, "title"),
            (&market_slug, "market_slug"),
            (&event_slug, "event_slug"),
            (&description, "description"),
        ] {
            if src.as_str().contains(term) {
                negated.push(EvidenceSignal::SlugHardTerm {
                    term: format!("{name}:{term}"),
                });
            }
        }
    }
    if !negated.is_empty() {
        let mut ev = ClassifierEvidence::new(CryptoVerdict::NonCrypto);
        ev.negated_signals = negated;
        return ev;
    }

    // ---- Layer 2: crypto affirmations -----------------------------------
    let mut signals: Vec<EvidenceSignal> = Vec::new();

    for marker in CRYPTO_ICON_MARKERS {
        if icon.contains(marker) {
            signals.push(EvidenceSignal::IconToken {
                token: marker.to_string(),
            });
        }
    }
    for tag in &tags {
        let t = tag.to_lowercase();
        if t == "crypto" || CRYPTO_TERMS.iter().any(|c| t.contains(c)) {
            signals.push(EvidenceSignal::Tags { tag: tag.clone() });
        }
    }
    for term in CRYPTO_TERMS {
        for (src, name) in [
            (&title, "title"),
            (&market_slug, "market_slug"),
            (&event_slug, "event_slug"),
            (&description, "description"),
        ] {
            if word_match(src, term) {
                signals.push(EvidenceSignal::SlugTerm {
                    term: format!("{name}:{term}"),
                });
            }
        }
    }

    let mut ev = ClassifierEvidence::new(if signals.is_empty() {
        CryptoVerdict::Unknown
    } else {
        CryptoVerdict::Crypto
    });
    ev.signals = dedup_signals(signals);
    ev
}

/// Word-boundary matching: a bare substring match would hit "eth" inside
/// "the"; require non-alphanumerics around the hit.
fn word_match(haystack: &str, term: &str) -> bool {
    let hay = haystack.as_bytes();
    let needle = term.as_bytes();
    if needle.is_empty() {
        return false;
    }
    let mut from = 0usize;
    while let Some(rel) = find_sub(&hay[from.min(hay.len())..], needle) {
        let pos = from + rel;
        let before_ok = pos == 0 || !hay[pos - 1].is_ascii_alphanumeric();
        let after = pos + needle.len();
        let after_ok = after >= hay.len() || !hay[after].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return true;
        }
        from = pos + 1;
    }
    false
}

fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

fn dedup_signals(s: Vec<EvidenceSignal>) -> Vec<EvidenceSignal> {
    let mut keys: Vec<String> = s.iter().map(|x| x.sort_key()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .map(|k| match k.split_once(':') {
            Some(("slug", t)) => EvidenceSignal::SlugTerm {
                term: t.to_string(),
            },
            Some(("hard", t)) => EvidenceSignal::SlugHardTerm {
                term: t.to_string(),
            },
            Some(("icon", t)) => EvidenceSignal::IconToken {
                token: t.to_string(),
            },
            Some(("tag", t)) => EvidenceSignal::Tags { tag: t.to_string() },
            Some(("desc", t)) => EvidenceSignal::DescriptionTerm {
                term: t.to_string(),
            },
            _ => unreachable!("sort key format"),
        })
        .collect()
}

/// Re-export the version alias used in persisted classification records.
pub type Decision = ClassifiedVerdict;
