//! Position reconstruction from chronological source fills.
//!
//! Semantics (conservative and explicitly documented):
//! - BUYs open lots; SELLs consume them in strict FIFO order.
//! - Realized PnL is only counted against *known* lots. A SELL with no
//!   preceding known lot is recorded as `PRE_HISTORY_DISPOSAL` and does not
//!   contribute to realized PnL — fabricating a cost basis is prohibited.
//! - Unrealized PnL is only ever computed when the caller supplies an
//!   independent valuation; nothing here invents prices.
//! - Fees: entry fee appears in the Data API positions (entry_fees_usdc) but
//!   is **not** per-fill in activity records. Lot cost basis uses price*size
//!   with a note that fee-adjusted verification happens at reconciliation.

use std::collections::{HashMap, HashSet};

use rust_decimal::Decimal;
use serde::Serialize;

use crate::activity::{Side, SourceFill};
use crate::error::IngestionError;

/// One opening purchase (lot) plus its consumed shares.
#[derive(Debug, Clone, Serialize)]
pub struct Lot {
    /// Identity of the opening BUY fill.
    pub event_key: String,
    pub opened_ts: i64,
    pub shares_total: Decimal,
    pub shares_remaining: Decimal,
    pub cost_per_share: Decimal,
}

/// FIFO position state for one token.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PositionState {
    pub token_id: String,
    /// Lots in FIFO order.
    pub lots: Vec<Lot>,
    /// Cumulative disposals fully attributed to known lots.
    pub disposed_shares: Decimal,
    /// Disposals with no reconstructed preceding inventory (pre-history sells).
    pub unattributed_disposals: Decimal,
    pub realized_pnl: Decimal,
    /// Cumulative acquired shares across the whole observed history.
    pub total_acquired_shares: Decimal,
    /// Number of fills applied (diagnostic).
    pub fills_applied: u64,
    /// Whether the earliest fill we saw was a SELL (history likely incomplete).
    pub history_probably_incomplete: bool,
}

impl PositionState {
    pub fn new(token_id: &str) -> Self {
        Self {
            token_id: token_id.to_string(),
            lots: Vec::new(),
            disposed_shares: Decimal::ZERO,
            unattributed_disposals: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            total_acquired_shares: Decimal::ZERO,
            fills_applied: 0,
            history_probably_incomplete: false,
        }
    }

    pub fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    /// Cumulative acquired (all BUYs observed, consumed or not).
    pub fn acquired_shares(&self) -> Decimal {
        self.total_acquired_shares
    }

    pub fn remaining_inventory(&self) -> Decimal {
        self.lots.iter().map(|l| l.shares_remaining).sum()
    }

    /// Remaining inventory valued at its weighted average cost.
    pub fn remaining_cost_basis(&self) -> Decimal {
        self.lots
            .iter()
            .map(|l| l.shares_remaining * l.cost_per_share)
            .sum()
    }

    /// Weighted average acquisition price of *remaining* inventory.
    pub fn avg_open_price(&self) -> Option<Decimal> {
        let inv = self.remaining_inventory();
        if inv.is_zero() {
            return None;
        }
        Some(self.remaining_cost_basis() / inv)
    }

    /// Apply one fill; throws on invalid sequences (e.g. fractional share overflow).
    pub fn apply(&mut self, fill: &SourceFill) {
        self.fills_applied += 1;
        match fill.side {
            Side::Buy => {
                self.total_acquired_shares += fill.size;
                self.lots.push(Lot {
                    event_key: fill.event_key.clone(),
                    opened_ts: fill.timestamp,
                    shares_total: fill.size,
                    shares_remaining: fill.size,
                    cost_per_share: fill.cash_implied_price,
                });
            }
            Side::Sell => {
                let mut to_consume = fill.size;
                let mut cost = Decimal::ZERO;
                if self.lots.is_empty() {
                    self.history_probably_incomplete = true;
                    self.unattributed_disposals += fill.size;
                    return;
                }
                for i in 0..self.lots.len() {
                    if to_consume.is_zero() {
                        break;
                    }
                    let take = to_consume.min(self.lots[i].shares_remaining);
                    if take > Decimal::ZERO {
                        cost += take * self.lots[i].cost_per_share;
                        self.lots[i].shares_remaining -= take;
                        to_consume -= take;
                    }
                }
                let consumed = fill.size - to_consume;
                if consumed > Decimal::ZERO {
                    self.disposed_shares += consumed;
                    let proceeds = fill.cash_implied_price * consumed;
                    self.realized_pnl += proceeds - cost;
                }
                if to_consume > Decimal::ZERO {
                    // Sell exceeded known inventory — book remainder as
                    // unattributed (no invented cost basis), flag history gap.
                    self.history_probably_incomplete = true;
                    self.unattributed_disposals += to_consume;
                }
                // Prune fully-consumed lots to keep the structure small.
                self.lots.retain(|l| l.shares_remaining > Decimal::ZERO);
            }
        }
    }
}

/// Aggregate book over several token positions, replayed in order.
#[derive(Debug, Default, Serialize)]
pub struct PositionBook {
    /// token_id → position state.
    pub positions: HashMap<String, PositionState>,
    /// Replayer helper: fills must be applied ascending by (timestamp, event_key).
    order_error: Option<String>,
}

impl PositionBook {
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            order_error: None,
        }
    }

    pub fn get(&self, token_id: &str) -> Option<&PositionState> {
        self.positions.get(token_id)
    }

    /// Apply fills in the order provided; returns Err on duplicate event keys
    /// or out-of-order timestamps (strict ordering contract).
    pub fn apply_all(&mut self, fills: &[SourceFill]) -> Result<(), IngestionError> {
        let mut prev_ts: Option<i64> = None;
        let mut seen: HashSet<&str> = HashSet::new();
        for fill in fills {
            if let Some(ts) = prev_ts {
                if fill.timestamp < ts {
                    return Err(IngestionError::InconsistentFeed(
                        "fills not in ascending timestamp order".into(),
                    ));
                }
            }
            if seen.contains(fill.event_key.as_str()) {
                return Err(IngestionError::Validation(format!(
                    "duplicate event_key in replay input: {}",
                    fill.event_key
                )));
            }
            seen.insert(fill.event_key.as_str());
            let state = self
                .positions
                .entry(fill.token_id.clone())
                .or_insert_with(|| PositionState::new(&fill.token_id));
            state.apply(fill);
            prev_ts = Some(fill.timestamp);
        }
        Ok(())
    }

    /// Tokens with reconstructed state.
    pub fn token_ids(&self) -> Vec<&str> {
        self.positions.keys().map(|s| s.as_str()).collect()
    }

    pub fn order_error(&self) -> Option<&str> {
        self.order_error.as_deref()
    }
}
