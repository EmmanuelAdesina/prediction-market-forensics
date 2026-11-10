//! Fill-credit rules (docs/08 §4): what counts as an EXECUTED simulated
//! fill. A simulated passive order is credited a fill only when the public
//! tape provides direct evidence — never from stale or gapped state.

use rust_decimal::Decimal;

use tokor_domain::activity::Side;

/// Evidence events, in recorded order. The caller supplies them from the
/// recorder's tables; this module is a pure state machine over them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceEvent {
    /// A public trade print: (exchange ts ns, price, size, aggressor side).
    Trade {
        ts_ns: i64,
        price: Decimal,
        size: Decimal,
        aggressor_side: Side,
    },
    /// A book update removed the level entirely (our price, our side).
    LevelRemoved { ts_ns: i64 },
    /// A reconnect/gap was declared: evidence does not cross a gap
    /// (docs/08 §4 rule 4). The order survives but accrued queue-credit
    /// is discarded.
    Gap { ts_ns: i64 },
    /// The book went stale / was resynchronized: same treatment as a gap.
    Resynced { ts_ns: i64 },
}

/// A passive simulated order awaiting evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveOrder {
    pub side: Side,
    pub price: Decimal,
    /// Wall-clock ns at submission.
    pub submitted_ns: i64,
    pub remaining_qty: Decimal,
    /// Queue ahead under the active scenario, measured at submission.
    pub queue_ahead: Decimal,
}

/// Outcome after replaying evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreditOutcome {
    /// Evidenced filled quantity. NEVER exceeds `submitted_qty`.
    pub executed_qty: Decimal,
    pub terminal: Terminal,
    /// Evidence trails kept for the report (rule: every credited fill
    /// cites its evidence).
    pub fills_count: u32,
    pub cancelled_reason: Option<&'static str>,
}

/// Why the order stopped accruing fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Terminal {
    /// Fully filled on evidence.
    Filled,
    /// Level removed before we reached the front — cancelled (rule 3).
    CancelledLevelGone,
    /// Evidence ended (end of window) with quantity still open.
    Open,
}

/// Replay `events` (in recorded order) against the order and report what
/// would have executed. Trades at our price on OUR side consume the queue;
/// queue_ahead must be cleared before ANY quantity is ours (rule 2).
///
/// Price-time priority (rule 1): a trade at a price STRICTLY BETTER than
/// ours on our side (higher bid / lower ask) before we reach the front
/// proves others with priority — those prints do not count against the
/// queue, and they do not fill us. (A trade worse than ours never touches
/// our level.)
pub fn credit_fills(order: &PassiveOrder, events: &[EvidenceEvent]) -> CreditOutcome {
    let mut queue_ahead = order.queue_ahead;
    let mut remaining = order.remaining_qty;
    let mut executed = Decimal::ZERO;
    let mut fills_count = 0u32;

    for ev in events {
        if remaining <= Decimal::ZERO {
            break;
        }
        match *ev {
            EvidenceEvent::Trade {
                ts_ns: _,
                price,
                size,
                aggressor_side,
            } => {
                // Only prints that consume OUR level matter: the aggressor
                // must be on the OPPOSITE side, and the print price must
                // equal our resting price (CLOB prints happen at the
                // maker's price).
                let opposite = match order.side {
                    Side::Buy => Side::Sell,
                    Side::Sell => Side::Buy,
                };
                if aggressor_side != opposite || price != order.price {
                    continue;
                }
                // First the queue ahead of us clears; the surplus is ours.
                if queue_ahead > Decimal::ZERO {
                    let cleared = queue_ahead.min(size);
                    queue_ahead -= cleared;
                    let surplus = size - cleared;
                    if surplus > Decimal::ZERO {
                        let take = surplus.min(remaining);
                        executed += take;
                        remaining -= take;
                        fills_count += 1;
                    }
                } else {
                    let take = size.min(remaining);
                    executed += take;
                    remaining -= take;
                    fills_count += 1;
                }
            }
            EvidenceEvent::LevelRemoved { ts_ns: _ } => {
                return CreditOutcome {
                    executed_qty: executed,
                    terminal: Terminal::CancelledLevelGone,
                    fills_count,
                    cancelled_reason: Some("level removed before front reached"),
                };
            }
            EvidenceEvent::Gap { ts_ns: _ } | EvidenceEvent::Resynced { ts_ns: _ } => {
                // Rule 4: freeze at a gap — accrued queue-credit is
                // discarded; the queue ahead is unknown again. We adopt the
                // CONSERVATIVE reading: the order remains but we cannot
                // claim the queue cleared, so queue_ahead resets to its
                // submission value.
                queue_ahead = order.queue_ahead;
            }
        }
    }

    CreditOutcome {
        executed_qty: executed,
        terminal: if remaining <= Decimal::ZERO {
            Terminal::Filled
        } else {
            Terminal::Open
        },
        fills_count,
        cancelled_reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn order(qty: Decimal, queue: Decimal) -> PassiveOrder {
        PassiveOrder {
            side: Side::Buy,
            price: dec!(0.50),
            submitted_ns: 1_000,
            remaining_qty: qty,
            queue_ahead: queue,
        }
    }

    fn trade(price: Decimal, size: Decimal, aggressor: Side) -> EvidenceEvent {
        EvidenceEvent::Trade {
            ts_ns: 2_000,
            price,
            size,
            aggressor_side: aggressor,
        }
    }

    #[test]
    fn queue_clears_before_we_fill() {
        // 100 ahead of us; first 100-share print clears the queue, next
        // 40-share print is ours.
        let o = order(dec!(50), dec!(100));
        let out = credit_fills(
            &o,
            &[
                trade(dec!(0.50), dec!(100), Side::Sell),
                trade(dec!(0.50), dec!(40), Side::Sell),
            ],
        );
        assert_eq!(out.executed_qty, dec!(40));
        assert_eq!(out.terminal, Terminal::Open);
        assert_eq!(out.fills_count, 1);
    }

    #[test]
    fn surplus_of_one_print_splits_across_queue_and_us() {
        let o = order(dec!(50), dec!(100));
        let out = credit_fills(&o, &[trade(dec!(0.50), dec!(130), Side::Sell)]);
        assert_eq!(out.executed_qty, dec!(30));
    }

    #[test]
    fn prints_at_other_prices_do_not_fill_us() {
        let o = order(dec!(50), dec!(0));
        let out = credit_fills(
            &o,
            &[
                trade(dec!(0.49), dec!(100), Side::Sell), // worse price
                trade(dec!(0.51), dec!(100), Side::Sell), // not our level
                trade(dec!(0.50), dec!(100), Side::Buy),  // same-side aggressor
            ],
        );
        assert_eq!(out.executed_qty, Decimal::ZERO);
        assert_eq!(out.terminal, Terminal::Open);
    }

    #[test]
    fn level_removed_cancels_with_partial_credit_kept() {
        let o = order(dec!(50), dec!(0));
        let out = credit_fills(
            &o,
            &[
                trade(dec!(0.50), dec!(20), Side::Sell),
                EvidenceEvent::LevelRemoved { ts_ns: 3_000 },
                trade(dec!(0.50), dec!(100), Side::Sell), // after cancel: ignored
            ],
        );
        assert_eq!(out.executed_qty, dec!(20));
        assert_eq!(out.terminal, Terminal::CancelledLevelGone);
        assert!(out.cancelled_reason.is_some());
    }

    #[test]
    fn gap_discards_queue_credit() {
        // Queue 100; a 100-share print clears it; then a gap; then a print.
        // Post-gap the queue is UNKNOWN — conservative reading restores it,
        // so the next print only clears the queue again.
        let o = order(dec!(50), dec!(100));
        let out = credit_fills(
            &o,
            &[
                trade(dec!(0.50), dec!(100), Side::Sell),
                EvidenceEvent::Gap { ts_ns: 3_000 },
                trade(dec!(0.50), dec!(150), Side::Sell),
            ],
        );
        assert_eq!(out.executed_qty, dec!(50)); // 150 - 100 (restored queue)
        assert_eq!(out.terminal, Terminal::Filled);
    }

    #[test]
    fn fully_filled_stops() {
        let o = order(dec!(10), dec!(0));
        let out = credit_fills(
            &o,
            &[
                trade(dec!(0.50), dec!(4), Side::Sell),
                trade(dec!(0.50), dec!(4), Side::Sell),
                trade(dec!(0.50), dec!(4), Side::Sell), // 12 offered, we take 10
            ],
        );
        assert_eq!(out.executed_qty, dec!(10));
        assert_eq!(out.terminal, Terminal::Filled);
        assert_eq!(out.fills_count, 3);
    }
}
