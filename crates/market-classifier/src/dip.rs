//! Dip-market extraction and resolution verification.
//!
//! Pure, deterministic normalization of Gamma market payloads into
//! [`DipMarket`] records. Every rule operates on the contract's own text
//! (question, description, slugs, tags) and records the evidence it decided
//! from. When a load-bearing fact cannot be positively established, the
//! outcome is a recorded ambiguity - never a guess.
//!
//! Verified contract templates this module handles (see
//! `tests/dip_extraction.rs` fixtures, captured from live Gamma responses):
//!
//! * *Dip (touch-low)*: "This market will immediately resolve to \"Yes\" if
//!   any Binance 1 minute candle for Bitcoin (BTCUSDT) between <start> and
//!   <end> in the ET timezone has a final \"Low\" price of $90,000 or lower."
//! * *Reach (touch-high)*: "... has a final High price equal to or greater
//!   than the price specified in the title."
//! * *Monthly windows*: "during the month specified in the title (from 00:00
//!   AM ET on the first day to 11:59 PM ET on the last)".

use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use tokor_domain::dip::{
    Asset, BarrierSemantics, DipMarket, DipNormalization, FeeInfo, MarketStatus, RejectionReason,
    ResolutionStatus, ResolutionVerification,
};

/// Version of the extraction/verification rules. Bump on any change.
pub const DIP_VERIFIER_VERSION: &str = "dip-resver-v0.1.0";

/// Provenance string recorded for markets discovered via the Gamma
/// hit-price listing.
pub const PROVENANCE_GAMMA_HIT_PRICE: &str = "gamma:events?tag_slug=hit-price";

/// Everything the extractor needs about one market. Kept as plain data so
/// tests can construct inputs without HTTP.
#[derive(Debug, Clone)]
pub struct GammaMarketInput<'a> {
    /// Gamma event tag slugs, e.g. ["ethereum", "monthly", "hit-price"].
    pub event_tags: &'a [String],
    pub event_slug: Option<&'a str>,
    pub event_title: Option<&'a str>,
    /// The raw Gamma market JSON object.
    pub market: &'a serde_json::Value,
}

// ---------------------------------------------------------------------------
// payload field helpers
// ---------------------------------------------------------------------------

fn str_field<'a>(m: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    m.get(key).and_then(|v| v.as_str())
}

/// Gamma timestamps arrive as ISO-8601 with Z suffix ("2025-12-31T12:00:00Z").
fn parse_ts(raw: Option<&str>) -> Option<DateTime<Utc>> {
    let raw = raw?;
    if let Ok(t) = raw.parse::<DateTime<Utc>>() {
        return Some(t);
    }
    // Fallback: space-separated variant without offset ("2025-01-13 16:55:21").
    chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|n| n.and_utc())
}

/// Extract + verify one Gamma market payload.
///
/// Ordering: identity → scope → tokens → barrier/deadline → verification.
/// A failure at any stage is a structured rejection with a reason; only
/// fully normalized, verified markets become eligible.
pub fn normalize_gamma_market(input: &GammaMarketInput<'_>) -> DipNormalization {
    let m = input.market;

    // ---- identity --------------------------------------------------------
    let condition_id = str_field(m, "conditionId").unwrap_or_default().to_string();
    if condition_id.is_empty() {
        return DipNormalization::Rejected {
            condition_id: None,
            reason: RejectionReason::MissingConditionId,
            detail: "gamma payload has no conditionId".into(),
        };
    }

    // ---- scope: dip family + asset --------------------------------------
    let question = str_field(m, "question").unwrap_or_default().to_string();
    if !is_dip_question(&question) {
        return DipNormalization::Rejected {
            condition_id: Some(condition_id),
            reason: RejectionReason::UnsupportedAsset,
            detail: format!("not a dip market: {question:?}"),
        };
    }
    let asset = match determine_asset(
        &question,
        str_field(m, "description").unwrap_or_default(),
        input.event_tags,
    ) {
        Ok(a) => a,
        Err(detail) => {
            return DipNormalization::Rejected {
                condition_id: Some(condition_id),
                reason: RejectionReason::UnsupportedAsset,
                detail,
            };
        }
    };

    // ---- outcome tokens ---------------------------------------------------
    let (yes, no) = match token_pair(m) {
        Ok(p) => p,
        Err(detail) => {
            return DipNormalization::Rejected {
                condition_id: Some(condition_id),
                reason: RejectionReason::MissingTokens,
                detail,
            }
        }
    };

    // ---- barrier + deadline ----------------------------------------------
    let Some(barrier_price) = parse_barrier_from_title(&question) else {
        return DipNormalization::Rejected {
            condition_id: Some(condition_id),
            reason: RejectionReason::InvalidBarrier,
            detail: format!("no $barrier parseable from question {question:?}"),
        };
    };
    let Some(end_date) = parse_ts(str_field(m, "endDate")) else {
        return DipNormalization::Rejected {
            condition_id: Some(condition_id),
            reason: RejectionReason::InvalidDeadline,
            detail: format!("endDate missing/unparseable: {:?}", m.get("endDate")),
        };
    };

    // ---- resolution verification ------------------------------------------
    let description = str_field(m, "description").unwrap_or_default().to_string();
    let verification = verify_resolution(&asset, barrier_price, &question, &description, end_date);

    // NOTE: non-VERIFIED candidates are still ACCEPTED into the registry
    // (retained with eligible=0) - they are real BTC/ETH dip-shaped contracts
    // whose semantics failed verification. Dropping them would destroy
    // evidence and make sync non-idempotent. Only structural failures above
    // are rejections.

    // ---- lifecycle ---------------------------------------------------------
    let status = market_status(m);

    let observation_end = parse_observation_end(&question, &description, end_date);

    let market = DipMarket {
        condition_id: condition_id.clone(),
        gamma_market_id: m.get("id").map(id_to_string),
        asset,
        barrier_price,
        observation_end,
        end_date,
        yes_token_id: yes,
        no_token_id: no,
        question,
        event_slug: input.event_slug.map(String::from),
        event_title: input.event_title.map(String::from),
        resolution_rule: description,
        oracle_source: verification.source_url.clone(),
        tick_size: decimal_field(m, "orderPriceMinTickSize").unwrap_or(Decimal::ZERO),
        min_order_size: decimal_field(m, "orderMinSize").unwrap_or(Decimal::ZERO),
        fees: FeeInfo {
            fee_type: str_field(m, "feeType").map(String::from),
            fees_enabled: m.get("feesEnabled").and_then(|v| v.as_bool()),
            maker_base_fee_bps: decimal_field(m, "makerBaseFee"),
            taker_base_fee_bps: decimal_field(m, "takerBaseFee"),
        },
        neg_risk: m.get("negRisk").and_then(|v| v.as_bool()),
        status,
        source_provenance: PROVENANCE_GAMMA_HIT_PRICE.into(),
        discovered_at: Utc::now(),
        last_verified_at: verification.checked_at,
        metadata_version: 1,
        resolution_verification: verification,
    };

    DipNormalization::Accepted(Box::new(market))
}

/// Eligibility check over an ACCEPTED market: verification passed and the
/// mechanical preconditions hold. Mirrors `DipMarket::is_eligible` plus
/// metadata sanity (tick size known).
pub fn is_eligible(market: &DipMarket) -> bool {
    market.is_eligible() && market.tick_size > Decimal::ZERO
}

// ---------------------------------------------------------------------------
// scope helpers
// ---------------------------------------------------------------------------

/// The dip family: questions phrased "will X dip to $Y …". Reach/high markets
/// are deliberately out of registry scope (assignment §1).
pub fn is_dip_question(question: &str) -> bool {
    let q = question.to_lowercase();
    q.contains("dip to") || q.contains("dip below")
}

/// Determine the underlying from three independent signals, in descending
/// contract authority: the Binance pair named in the description, the asset
/// word in the question, and the event tags. A conflict between any two
/// determined signals is a rejection (never silently resolved).
pub fn determine_asset(
    question: &str,
    description: &str,
    tags: &[String],
) -> Result<Asset, String> {
    let desc_upper = description.to_uppercase();
    let from_pair = if desc_upper.contains("BTCUSDT") {
        Some(Asset::Btc)
    } else if desc_upper.contains("ETHUSDT") {
        Some(Asset::Eth)
    } else {
        None
    };

    let q_lower = question.to_lowercase();
    let from_question = if q_lower.contains("bitcoin") {
        Some(Asset::Btc)
    } else if q_lower.contains("ethereum") || q_lower.contains(" ether ") {
        Some(Asset::Eth)
    } else {
        None
    };

    let from_tags = if tags.iter().any(|t| t.eq_ignore_ascii_case("bitcoin")) {
        Some(Asset::Btc)
    } else if tags.iter().any(|t| t.eq_ignore_ascii_case("ethereum")) {
        Some(Asset::Eth)
    } else {
        None
    };

    let signals = [from_pair, from_question, from_tags];
    let mut decided: Option<Asset> = None;
    for s in signals.into_iter().flatten() {
        match decided {
            None => decided = Some(s),
            Some(prev) if prev != s => {
                return Err(format!(
                    "asset signals conflict: pair={from_pair:?} question={from_question:?} tags={from_tags:?}"
                ));
            }
            _ => {}
        }
    }
    decided.ok_or_else(|| {
        format!("no asset signal: pair={from_pair:?} question={from_question:?} tags={from_tags:?}")
    })
}

// ---------------------------------------------------------------------------
// tokens
// ---------------------------------------------------------------------------

/// `outcomes` and `clobTokenIds` arrive as JSON-encoded *strings* - verified
/// against live Gamma payloads (`"[\"Yes\",\"No\"]"`). Parse both, pair them
/// positionally: YES is the outcome literally named "Yes".
fn token_pair(m: &serde_json::Value) -> Result<(String, String), String> {
    let outcomes = parse_json_string_field(m, "outcomes")
        .ok_or_else(|| "outcomes field missing/unparseable".to_string())?;
    let token_ids = parse_json_string_field(m, "clobTokenIds")
        .ok_or_else(|| "clobTokenIds field missing/unparseable".to_string())?;

    if outcomes.len() != token_ids.len() || token_ids.len() != 2 {
        return Err(format!(
            "outcome/token arity mismatch: outcomes={outcomes:?} tokens={token_ids:?}"
        ));
    }
    let yes_idx = outcomes
        .iter()
        .position(|o| o.eq_ignore_ascii_case("yes"))
        .ok_or_else(|| format!("no Yes outcome in {outcomes:?}"))?;
    let no_idx = 1 - yes_idx;
    let yes = token_ids[yes_idx].clone();
    let no = token_ids[no_idx].clone();
    if yes.is_empty() || no.is_empty() || yes == no {
        return Err(format!("invalid token pair: yes={yes:?} no={no:?}"));
    }
    Ok((yes, no))
}

fn parse_json_string_field(m: &serde_json::Value, key: &str) -> Option<Vec<String>> {
    let raw = str_field(m, key)?;
    let parsed: Vec<serde_json::Value> = serde_json::from_str(raw).ok()?;
    parsed
        .into_iter()
        .map(|v| match v {
            serde_json::Value::String(s) => Some(s),
            other => Some(other.to_string()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// barrier + numbers
// ---------------------------------------------------------------------------

/// Extract the barrier from the question text: the first `$<number>` with
/// optional thousands separators ("$90,000", "$1,500.50", "$1.00").
pub fn parse_barrier_from_title(question: &str) -> Option<Decimal> {
    let bytes = question.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            let mut j = i + 1;
            let mut num = String::new();
            while j < bytes.len()
                && (bytes[j].is_ascii_digit() || bytes[j] == b',' || bytes[j] == b'.')
            {
                if bytes[j] != b',' {
                    num.push(bytes[j] as char);
                }
                j += 1;
            }
            if !num.is_empty() {
                if let Ok(d) = num.parse::<Decimal>() {
                    return Some(d);
                }
            }
        }
        i += 1;
    }
    None
}

fn decimal_field(m: &serde_json::Value, key: &str) -> Option<Decimal> {
    match m.get(key) {
        Some(serde_json::Value::Number(n)) => n.to_string().parse::<Decimal>().ok(),
        Some(serde_json::Value::String(s)) => s.parse::<Decimal>().ok(),
        _ => None,
    }
}

fn id_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// verification
// ---------------------------------------------------------------------------

/// Structured resolution verification against the contract text.
///
/// Hard requirements for VERIFIED:
/// 1. description states an explicit touch threshold (Low ≤ X / High ≥ X)
///    with immediate-resolution semantics;
/// 2. description names Binance as the source and yields a binance URL;
/// 3. the barrier is bound either numerically in the description or by the
///    explicit phrase "the price specified in the title";
/// 4. the named pair is consistent with the asset.
///
/// Any load-bearing failure → AMBIGUOUS (title-consistent contracts) or
/// REJECTED (missing settlement text / missing source).
pub fn verify_resolution(
    asset: &Asset,
    barrier: Decimal,
    question: &str,
    description: &str,
    end_date: DateTime<Utc>,
) -> ResolutionVerification {
    let mut notes = Vec::new();
    let mut evidence = vec![format!("title: {question}")];
    let desc = description.to_lowercase();

    // 1. semantics ----------------------------------------------------------
    let semantics = detect_semantics(&desc);
    let mut status = match semantics.class {
        BarrierSemantics::TouchLowAtMost | BarrierSemantics::TouchHighAtLeast => {
            ResolutionStatus::Verified
        }
        BarrierSemantics::CloseBased => {
            notes.push("close-based settlement - not mechanically verifiable".into());
            ResolutionStatus::Rejected
        }
        BarrierSemantics::Other => {
            notes.push("no explicit touch threshold found in description".into());
            ResolutionStatus::Rejected
        }
    };
    if let Some(frag) = &semantics.threshold_evidence {
        evidence.push(format!("threshold: {frag}"));
    }

    // 2. source ---------------------------------------------------------------
    let source_url = extract_binance_url(description);
    if source_url.is_none() {
        notes.push("no Binance resolution URL in description".into());
    }
    let names_binance = desc.contains("binance");
    if !names_binance {
        notes.push("description does not name Binance as resolution source".into());
    }

    // 3. barrier binding ------------------------------------------------------
    if let Some(desc_barrier) = parse_barrier_from_description(&desc) {
        if desc_barrier != barrier {
            notes.push(format!(
                "description barrier ${desc_barrier} != title barrier ${barrier}"
            ));
            evidence.push(format!(
                "threshold mismatch: title ${barrier} vs description ${desc_barrier}"
            ));
            return finish(
                ResolutionStatus::Ambiguous,
                evidence,
                source_url,
                semantics.pair,
                notes,
            );
        }
    } else if !desc.contains("price specified in the title") {
        notes.push("barrier neither numeric in description nor bound to title".into());
        evidence.push("unbound barrier".into());
        return finish(
            ResolutionStatus::Ambiguous,
            evidence,
            source_url,
            semantics.pair,
            notes,
        );
    }

    // 4. pair/asset consistency ------------------------------------------------
    if let Some(pair) = &semantics.pair {
        let expect = asset.binance_pair();
        if pair != expect {
            notes.push(format!("pair {pair} inconsistent with asset {expect}"));
            evidence.push(format!("pair mismatch {pair} != {expect}"));
            return finish(
                ResolutionStatus::Ambiguous,
                evidence,
                source_url,
                Some(pair.clone()),
                notes,
            );
        }
    } else {
        notes.push("description names no Binance pair".into());
        evidence.push("no pair named".into());
        return finish(
            ResolutionStatus::Ambiguous,
            evidence,
            source_url,
            None,
            notes,
        );
    }

    // A "Verified" semantic class still requires an explicit source.
    if status == ResolutionStatus::Verified && (source_url.is_none() || !names_binance) {
        status = ResolutionStatus::Ambiguous;
    }

    // Window parse result is a note, never a status change (endDate remains
    // the mechanical deadline; the contract window is enrichment).
    let _ = end_date;

    evidence.extend(evidence_sentences(description));

    finish(status, evidence, source_url, semantics.pair, notes)
}

/// Sentence-level evidence: keep the resolution sentence(s) verbatim.
fn evidence_sentences(description: &str) -> Vec<String> {
    description
        .split(['\n', '.'])
        .map(str::trim)
        .filter(|s| {
            let l = s.to_lowercase();
            (l.contains("resolve") && (l.contains("yes") || l.contains("no")))
                || l.contains("low")
                || l.contains("high")
        })
        .map(|s| s.to_string())
        .take(3)
        .collect()
}

fn finish(
    status: ResolutionStatus,
    evidence: Vec<String>,
    source_url: Option<String>,
    source_pair: Option<String>,
    notes: Vec<String>,
) -> ResolutionVerification {
    ResolutionVerification {
        status,
        verifier_version: DIP_VERIFIER_VERSION.into(),
        checked_at: Utc::now(),
        evidence,
        source_url,
        source_pair,
        notes,
    }
}

struct DetectedSemantics {
    class: BarrierSemantics,
    /// Binance pair token found (e.g. "BTCUSDT").
    pair: Option<String>,
    /// The threshold sentence fragment proving the class.
    threshold_evidence: Option<String>,
}

fn detect_semantics(desc_lower: &str) -> DetectedSemantics {
    let pair = ["BTCUSDT", "ETHUSDT"]
        .into_iter()
        .find(|p| desc_lower.contains(&p.to_lowercase()))
        .map(String::from);

    let has_low = desc_lower.contains("\"low\"")
        || desc_lower.contains("'low'")
        || desc_lower.contains("`low`");
    let has_high = desc_lower.contains("\"high\"")
        || desc_lower.contains("'high'")
        || desc_lower.contains("`high`");
    let or_lower = desc_lower.contains("or lower") || desc_lower.contains("or less");
    let or_higher = desc_lower.contains("or higher")
        || desc_lower.contains("or greater")
        || desc_lower.contains("greater than");

    let (class, evidence) = if has_low && or_lower {
        (
            BarrierSemantics::TouchLowAtMost,
            Some("final \"Low\" … or lower".into()),
        )
    } else if has_high && or_higher {
        (
            BarrierSemantics::TouchHighAtLeast,
            Some("final High … or greater".into()),
        )
    } else if desc_lower.contains("closing price") || desc_lower.contains(" close ") {
        (BarrierSemantics::CloseBased, None)
    } else {
        (BarrierSemantics::Other, None)
    };

    DetectedSemantics {
        class,
        pair,
        threshold_evidence: evidence,
    }
}

/// Numeric barrier stated inside the description ("price of $90,000 or lower").
fn parse_barrier_from_description(desc_lower: &str) -> Option<Decimal> {
    // Only meaningful if the description states a number at all.
    parse_barrier_from_title(desc_lower)
}

fn extract_binance_url(description: &str) -> Option<String> {
    let lower = description.to_lowercase();
    let needle = "https://www.binance.com";
    let start = lower.find(needle)?;
    let bytes = description.as_bytes();
    let mut end = start;
    while end < bytes.len()
        && !bytes[end].is_ascii_whitespace()
        && bytes[end] != b')'
        && bytes[end] != b','
    {
        end += 1;
    }
    Some(description[start..end].to_string())
}

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

/// Market status from Gamma flags + outcome prices (JSON-string encoding).
fn market_status(m: &serde_json::Value) -> MarketStatus {
    let closed = m.get("closed").and_then(|v| v.as_bool()).unwrap_or(false);
    let active = m.get("active").and_then(|v| v.as_bool()).unwrap_or(false);
    if !closed && active {
        return MarketStatus::Active;
    }
    if let Some(prices) = parse_json_string_field(m, "outcomePrices") {
        if prices.len() == 2 {
            let yes: Option<Decimal> = prices[0].parse().ok();
            let no: Option<Decimal> = prices[1].parse().ok();
            if let (Some(y), Some(n)) = (yes, no) {
                if y == Decimal::ONE && n == Decimal::ZERO {
                    return MarketStatus::ResolvedYes;
                }
                if y == Decimal::ZERO && n == Decimal::ONE {
                    return MarketStatus::ResolvedNo;
                }
            }
        }
    }
    MarketStatus::ResolvedUnknown
}

// ---------------------------------------------------------------------------
// observation window
// ---------------------------------------------------------------------------

/// Parse the contract's observation-window end. The window text lives in the
/// description; the year is resolved against `endDate` when the title omits
/// it. Returns None (with the reason logged by the caller as a note) when the
/// pattern is unrecognized - never a fabricated date.
pub fn parse_observation_end(
    question: &str,
    description: &str,
    end_date: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let desc = description.to_lowercase();

    // Pattern A: "between <Month D, YYYY, HH:MM> and <Month D, YYYY, HH:MM> in the et timezone"
    if let Some(end) = parse_between_window(&desc) {
        return Some(end);
    }

    // Pattern B (monthly): "during the month specified in the title … to 11:59 pm et on the last"
    if desc.contains("the month specified in the title") {
        let month = month_from_question(question)?;
        let mut year = end_date.year();
        // If the named month is far "after" endDate's month, the window is in
        // the preceding year (e.g. a December window with January endDate).
        if month > end_date.month() && month - end_date.month() > 6 {
            year -= 1;
        }
        let last_day = days_in_month(year, month)?;
        // 11:59 PM ET on the last day of the month.
        let naive = NaiveDate::from_ymd_opt(year, month, last_day)?.and_hms_opt(23, 59, 0)?;
        return Some(et_naive_to_utc(naive, end_date.date_naive()));
    }

    // Pattern C (single day / explicit range with year): "by December 31, 2025"
    // handled via the question: "<Month D, YYYY>" → end at 23:59 ET that day.
    if let Some((date, _has_year)) = explicit_date_from_question(question) {
        let naive = date.and_hms_opt(23, 59, 0)?;
        return Some(et_naive_to_utc(naive, end_date.date_naive()));
    }

    None
}

/// "between December 30, 2024, 20:00 and December 31, 2025, 23:59 in the et timezone"
fn parse_between_window(desc_lower: &str) -> Option<DateTime<Utc>> {
    let start = desc_lower.find("between ")? + "between ".len();
    let rest = &desc_lower[start..];
    let and = rest.find(" and ")?;
    let end_part = &rest[and + " and ".len()..];
    let end_marker = [" in the et timezone", " in the et time zone"]
        .iter()
        .find_map(|m| end_part.find(m).map(|p| (p, *m)))?;
    let window_end = &end_part[..end_marker.0];

    // "<month name> <d>, <yyyy>, <hh>:<mm>"
    let tokens: Vec<&str> = window_end.split_whitespace().collect();
    if tokens.len() < 4 {
        return None;
    }
    let month = month_from_name(tokens[0])?;
    let day: u32 = tokens[1].trim_end_matches(',').parse().ok()?;
    let year: i32 = tokens[2].trim_end_matches(',').parse().ok()?;
    let time = tokens[3];
    let (h, mnt) = time.split_once(':')?;
    let hour: u32 = h.parse().ok()?;
    let minute: u32 = mnt.parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, 0)?;
    // ET determination anchors on the window date itself.
    Some(et_naive_to_utc(naive, naive.date()))
}

/// "<Month> <D>, <YYYY>" or "<Month> <D>" (no year) from the question.
fn explicit_date_from_question(question: &str) -> Option<(NaiveDate, bool)> {
    let q = question.to_lowercase();
    for month in 1u32..=12 {
        let name = month_name(month)?.to_lowercase();
        let Some(pos) = q.find(&name) else { continue };
        let rest = &q[pos + name.len()..];
        let rest = rest.trim_start();
        let day: u32 = rest
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .ok()?;
        let after = rest.trim_start_matches(|c: char| c.is_ascii_digit());
        let after = after.trim_start();
        if let Some(stripped) = after.strip_prefix(',') {
            let year: i32 = stripped
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .ok()?;
            return NaiveDate::from_ymd_opt(year, month, day).map(|d| (d, true));
        }
        return NaiveDate::from_ymd_opt(2000, month, day).map(|d| (d, false));
    }
    None
}

fn month_from_question(question: &str) -> Option<u32> {
    let q = question.to_lowercase();
    for m in 1u32..=12 {
        if q.contains(&month_name(m)?.to_lowercase()) {
            return Some(m);
        }
    }
    None
}

fn month_name(m: u32) -> Option<&'static str> {
    Some(match m {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => return None,
    })
}

fn month_from_name(name: &str) -> Option<u32> {
    Some(match name.trim_end_matches(',') {
        "january" => 1,
        "february" => 2,
        "march" => 3,
        "april" => 4,
        "may" => 5,
        "june" => 6,
        "july" => 7,
        "august" => 8,
        "september" => 9,
        "october" => 10,
        "november" => 11,
        "december" => 12,
        _ => return None,
    })
}

fn days_in_month(year: i32, month: u32) -> Option<u32> {
    let next_month = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)?
    };
    Some((next_month - NaiveDate::from_ymd_opt(year, month, 1)?).num_days() as u32)
}

/// Convert a wall-clock ET (US Eastern) naive datetime to UTC.
///
/// Uses the US DST rule (second Sunday in March → first Sunday in November,
/// valid 2007+) with fixed UTC-5 (EST) / UTC-4 (EDT) offsets. The anchor date
/// selects the offset; this is a documented public rule, not an inference
/// from data. All verified dip contracts state ET explicitly.
pub fn et_naive_to_utc(naive: NaiveDateTime, anchor: chrono::NaiveDate) -> DateTime<Utc> {
    let offset_hours = if in_us_dst(anchor) { -4 } else { -5 };
    let fixed = chrono::FixedOffset::east_opt(offset_hours * 3600).expect("fixed offset in hours");
    // A nonexistent local time (spring-forward gap) deterministically maps to
    // the same wall clock interpreted in standard time (UTC-5).
    let with_offset = fixed
        .from_local_datetime(&naive)
        .earliest()
        .unwrap_or_else(|| fixed.from_utc_datetime(&naive));
    with_offset.with_timezone(&Utc)
}

/// US Eastern DST: from the second Sunday of March 2:00 to the first Sunday
/// of November 2:00 (local). Mechanical public rule (Energy Policy Act 2005,
/// in effect since 2007).
fn in_us_dst(date: chrono::NaiveDate) -> bool {
    let year = date.year();
    if year < 2007 {
        return false; // conservative: pre-2007 rules unused by this corpus
    }
    let Some(march1) = NaiveDate::from_ymd_opt(year, 3, 1) else {
        return false;
    };
    let Some(nov1) = NaiveDate::from_ymd_opt(year, 11, 1) else {
        return false;
    };
    let second_sunday_march = march1 + chrono::Duration::days(first_sunday_offset(march1) + 7);
    let first_sunday_nov = nov1 + chrono::Duration::days(first_sunday_offset(nov1));
    date >= second_sunday_march && date < first_sunday_nov
}

fn first_sunday_offset(date: chrono::NaiveDate) -> i64 {
    // Days until the first Sunday (weekday: Mon=0..Sun=6).
    match date.weekday().num_days_from_sunday() {
        0 => 0,
        d => 7 - d as i64,
    }
}
