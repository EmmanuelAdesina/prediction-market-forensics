//! Order-book state machine: snapshot + delta reconciliation with gap and
//! staleness detection.
//!
//! Invariants (docs/07):
//! * A valid book is NEVER constructed from deltas alone - a delta arriving
//!   without a base state yields `NeedsSnapshot`, not a fabricated book.
//! * A hash discontinuity invalidates the local state and forces a snapshot
//!   refresh; the refreshed state is labeled `ReconciledAfterGap`.
//! * Stale books are retained (forensics) but flagged; decision paths must
//!   refuse them.
//! * Level size 0 removes the level; levels are kept sorted best-first and
//!   quantized to Decimal exactly (no floats in the book).

use std::collections::BTreeMap;
use std::time::Duration;

use rust_decimal::Decimal;
use serde_json::json;

use crate::messages::{BookMsg, PriceChangeMsg};

/// One price level: exact Decimal price and aggregate visible size.
#[derive(Debug, Clone, PartialEq)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// Quality label persisted with every normalized state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookQuality {
    /// Built from a full REST/WS snapshot.
    Snapshot,
    /// Delta applied with hash continuity.
    DeltaConsistent,
    /// Resynchronized after a gap/reconnect.
    ReconciledAfterGap,
}

impl BookQuality {
    pub fn as_str(self) -> &'static str {
        match self {
            BookQuality::Snapshot => "SNAPSHOT",
            BookQuality::DeltaConsistent => "DELTA_CONSISTENT",
            BookQuality::ReconciledAfterGap => "RECONCILED_AFTER_GAP",
        }
    }
}

/// A verified book state.
#[derive(Debug, Clone, PartialEq)]
pub struct BookState {
    /// bids sorted best (highest) first; asks sorted best (lowest) first.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub hash: Option<String>,
    pub exchange_ts: Option<String>,
    pub quality: BookQuality,
    /// Local receive stamp (wall-clock nanos) of the last applied message.
    pub updated_ns: i64,
}

impl BookState {
    pub fn best_bid(&self) -> Option<&Level> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&Level> {
        self.asks.first()
    }

    /// JSON-serializable view (persisted verbatim into normalized states).
    pub fn to_json(&self) -> (String, String) {
        let ser = |ls: &[Level]| {
            json!(ls
                .iter()
                .map(|l| [l.price.to_string(), l.size.to_string()])
                .collect::<Vec<_>>())
            .to_string()
        };
        (ser(&self.bids), ser(&self.asks))
    }

    /// Rehydrate a state from persisted `to_json` strings. Returns None if
    /// either side is not the expected `[["price","size"], ...]` shape -
    /// callers treat that as no usable book (DATA_INSUFFICIENT), never as
    /// an empty book.
    pub fn from_json(
        bids_json: &str,
        asks_json: &str,
        quality: BookQuality,
        updated_ns: i64,
    ) -> Option<BookState> {
        let parse = |s: &str| -> Option<Vec<Level>> {
            let raw: Vec<Vec<String>> = serde_json::from_str(s).ok()?;
            raw.into_iter()
                .map(|pair| {
                    let mut it = pair.into_iter();
                    let price = it.next()?.parse::<Decimal>().ok()?;
                    let size = it.next()?.parse::<Decimal>().ok()?;
                    Some(Level { price, size })
                })
                .collect()
        };
        let bids = parse(bids_json)?;
        let asks = parse(asks_json)?;
        Some(BookState {
            bids,
            asks,
            hash: None,
            exchange_ts: None,
            quality,
            updated_ns,
        })
    }
}

/// Why an update could not be normalized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapReason {
    /// Delta with no base snapshot - never fabricate.
    NoBaseState,
    /// The delta's exchange timestamp regresses below the last applied one -
    /// evidence that at least one message was missed or reordered. (The
    /// protocol's `hash` field is the POST-update book hash: consecutive
    /// events never match, so hashes are recorded for reconciliation against
    /// REST snapshots but cannot detect gaps by themselves.)
    TimestampRegression { last: String, got: String },
}

/// Outcome of applying one message.
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    Normalized(BookState),
    NeedsSnapshot(GapReason),
    /// Message for a different asset or an empty delta - recorded, no state.
    Ignored,
}

/// Per-token book builder.
#[derive(Debug)]
pub struct BookEngine {
    token_id: String,
    state: Option<BookState>,
    max_staleness: Duration,
    /// Set when a delta was dropped for a gap; the next snapshot is labeled
    /// `ReconciledAfterGap` instead of a plain `Snapshot`.
    gap_pending: bool,
}

impl BookEngine {
    pub fn new(token_id: impl Into<String>, max_staleness: Duration) -> Self {
        Self {
            token_id: token_id.into(),
            state: None,
            max_staleness,
            gap_pending: false,
        }
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn state(&self) -> Option<&BookState> {
        self.state.as_ref()
    }

    /// True when the state exists but is older than the staleness bound.
    /// STALE is a decision-time property, never a reason to fabricate.
    pub fn is_stale(&self, now_ns: i64) -> bool {
        match &self.state {
            None => true,
            Some(s) => {
                let age_ns = now_ns - s.updated_ns;
                age_ns > self.max_staleness.as_nanos() as i64
            }
        }
    }

    /// Apply a full snapshot (WS `book` event or REST /book body).
    pub fn apply_snapshot(&mut self, msg: &BookMsg, received_ns: i64) -> ApplyOutcome {
        if msg.asset_id != self.token_id {
            return ApplyOutcome::Ignored;
        }
        let was_gap = self.gap_pending;
        let bids = level_map(
            msg.bids.iter().map(|l| (l.price.as_str(), l.size.as_str())),
            true,
        );
        let asks = level_map(
            msg.asks.iter().map(|l| (l.price.as_str(), l.size.as_str())),
            false,
        );
        let state = BookState {
            bids,
            asks,
            hash: msg.hash.clone(),
            exchange_ts: msg.timestamp.clone(),
            quality: if was_gap {
                BookQuality::ReconciledAfterGap
            } else {
                BookQuality::Snapshot
            },
            updated_ns: received_ns,
        };
        self.gap_pending = false;
        self.state = Some(state.clone());
        ApplyOutcome::Normalized(state)
    }

    /// Apply an incremental `price_change` delta.
    pub fn apply_price_change(&mut self, msg: &PriceChangeMsg, received_ns: i64) -> ApplyOutcome {
        if msg.asset_id != self.token_id {
            return ApplyOutcome::Ignored;
        }
        // Invariant: never build a book from deltas without a base state.
        let Some(current) = self.state.clone() else {
            self.gap_pending = true;
            return ApplyOutcome::NeedsSnapshot(GapReason::NoBaseState);
        };
        // Exchange-timestamp monotonicity: a regression means we missed or
        // reordered messages - the local state is integrity-suspect until a
        // snapshot refresh (gap_pending blocks decisions).
        if let (Some(last), Some(got)) = (&current.exchange_ts, &msg.timestamp) {
            if let (Some(l), Some(g)) = (parse_exchange_ts(last), parse_exchange_ts(got)) {
                if g < l {
                    self.gap_pending = true;
                    return ApplyOutcome::NeedsSnapshot(GapReason::TimestampRegression {
                        last: last.clone(),
                        got: got.clone(),
                    });
                }
            }
        }

        let mut bids: BTreeMap<Decimal, Decimal> = current
            .bids
            .into_iter()
            .map(|l| (l.price, l.size))
            .collect();
        let mut asks: BTreeMap<Decimal, Decimal> = current
            .asks
            .into_iter()
            .map(|l| (l.price, l.size))
            .collect();
        for ch in &msg.changes {
            let Ok(price) = ch.price.parse::<Decimal>() else {
                continue;
            };
            let Ok(size) = ch.size.parse::<Decimal>() else {
                continue;
            };
            let book = match ch.side.as_str() {
                "BUY" => &mut bids,
                "SELL" => &mut asks,
                _ => continue,
            };
            if size.is_zero() {
                book.remove(&price);
            } else {
                book.insert(price, size);
            }
        }
        let state = BookState {
            bids: into_levels(bids, true),
            asks: into_levels(asks, false),
            hash: msg.hash.clone().or(current.hash),
            exchange_ts: msg.timestamp.clone().or(current.exchange_ts),
            quality: BookQuality::DeltaConsistent,
            updated_ns: received_ns,
        };
        self.state = Some(state.clone());
        ApplyOutcome::Normalized(state)
    }

    /// Integrity-suspect entry points (reconnect, detected gap): the
    /// retained state is DROPPED and the gap flag set - deltas cannot extend
    /// a suspect book, so the next delta hits `NoBaseState` and the session
    /// rescues the token from REST. The last good state remains in the
    /// persistence layer for forensics.
    pub fn invalidate(&mut self) {
        self.state = None;
        self.gap_pending = true;
    }

    /// Decision gate: a book drives simulated decisions only when it exists,
    /// carries no unresolved gap, and is inside the staleness bound.
    pub fn is_decision_ready(&self, now_ns: i64) -> bool {
        self.state.is_some() && !self.gap_pending && !self.is_stale(now_ns)
    }
}

// -- helpers -----------------------------------------------------------------

/// Normalize an exchange timestamp string to milliseconds for monotonicity
/// checks. Tolerates both epoch-seconds (≤ 1e11) and epoch-milliseconds -
/// the protocol has used both across versions; equality/ordering is what
/// matters, not the unit.
fn parse_exchange_ts(ts: &str) -> Option<u128> {
    let raw: u128 = ts.trim().parse().ok()?;
    Some(if raw > 100_000_000_000 {
        raw
    } else {
        raw * 1000
    })
}

/// Parse wire levels into a sorted map keyed by exact Decimal price, then
/// normalize best-first (`bids_desc` selects the sort direction).
fn level_map<'a, I: Iterator<Item = (&'a str, &'a str)>>(levels: I, bids_desc: bool) -> Vec<Level> {
    let mut m: BTreeMap<Decimal, Decimal> = BTreeMap::new();
    for (p, s) in levels {
        if let (Ok(p), Ok(s)) = (p.parse::<Decimal>(), s.parse::<Decimal>()) {
            if s.is_zero() {
                m.remove(&p);
            } else {
                m.insert(p, s);
            }
        }
    }
    into_levels(m, bids_desc)
}

fn into_levels(m: BTreeMap<Decimal, Decimal>, bids_desc: bool) -> Vec<Level> {
    let mut v: Vec<Level> = m
        .into_iter()
        .map(|(price, size)| Level { price, size })
        .collect();
    if bids_desc {
        v.sort_by_key(|l| std::cmp::Reverse(l.price));
    } else {
        v.sort_by_key(|l| l.price);
    }
    v
}
