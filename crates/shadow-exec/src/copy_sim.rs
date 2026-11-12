//! Exact-copy and intelligent-copy policies (docs/08 §6).
//!
//! * Exact-copy is the control: same side, same price, budget-clamped.
//! * Intelligent-copy adds three public-data rules, each falsifiable:
//!   1. fee-aware price improvement (makers pay zero on-chain - docs/06
//!      §4.1 - so a Tokor TAKER fill proves the level was consumable; join
//!      one tick inside when the recorded spread supports it),
//!   2. category gate (only verified-fee categories),
//!   3. exposure cap ($49 gross notional; overflow → Resize or reduce).

use rust_decimal::Decimal;

use tokor_domain::activity::Side;

use crate::decision::{Decision, Submission};
use crate::queue::{estimate_queue, QueueEstimate, QueueScenario};

/// Budget/exposure state the policy needs (kept plain - no persistence).
#[derive(Debug, Clone)]
pub struct BudgetState {
    /// Cash available, in USDC.
    pub cash: Decimal,
    /// Gross notional currently committed, in USDC.
    pub gross_exposure: Decimal,
    /// Hard gross-notional cap (the "$49" of the question).
    pub cap: Decimal,
}

impl BudgetState {
    /// Headroom left under the cap, in USDC (never negative).
    pub fn headroom(&self) -> Decimal {
        (self.cap - self.gross_exposure).max(Decimal::ZERO)
    }
}

/// Everything the policy needs to decide. `book` is the decision-ready
/// state at first sight; `None` ⇒ `DataInsufficient`.
#[derive(Debug, Clone)]
pub struct Signal {
    pub side: Side,
    /// Tokor's displayed execution price (fee-inclusive cash-implied prices
    /// are resolved upstream; docs/04 §6.3).
    pub price: Decimal,
    pub size: Decimal,
    /// Fee rate of the market's category, if verified (`None` ⇒ gate
    /// rejects in the intelligent policy; exact-copy ignores it).
    pub fee_rate: Option<Decimal>,
    /// Tick size for price-improvement laddering (docs/08 §9.1).
    pub tick: Decimal,
    /// Observed trade size that triggered the signal.
    pub observed_trade_size: Decimal,
    /// Decision-ready book state at first sight.
    pub book: Option<tokor_recorder::book::BookState>,
}

/// The minimum policy configuration (frozen before results are quoted -
/// docs/08 §9.3).
#[derive(Debug, Clone, Copy)]
pub struct PolicyConfig {
    pub scenario: QueueScenario,
    /// Cross only if the recorded spread is at least this wide (ticks).
    pub min_cross_ticks: u32,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        PolicyConfig {
            scenario: QueueScenario::Baseline,
            min_cross_ticks: 1,
        }
    }
}

/// Exact copy: same side, same price, size clamped to headroom.
pub fn exact_copy(sig: &Signal, budget: &BudgetState, cfg: &PolicyConfig) -> Submission {
    let Some(book) = sig.book.as_ref() else {
        return Submission::none(Decision::DataInsufficient, "no decision-ready book");
    };
    let est = estimate_queue(book, sig.price, sig.side, sig.observed_trade_size);
    // Conservative scenario cannot passively fill: cross instead, but only
    // when there is an opposing side to cross.
    if cfg.scenario == QueueScenario::Conservative {
        return marketable_submission(sig, budget, &est, "exact:conservative-cross");
    }
    passive_submission(sig, budget, sig.price, "exact:passive")
}

/// Intelligent copy: fee-aware improvement + category gate + exposure cap.
pub fn intelligent_copy(sig: &Signal, budget: &BudgetState, cfg: &PolicyConfig) -> Submission {
    let Some(book) = sig.book.as_ref() else {
        return Submission::none(Decision::DataInsufficient, "no decision-ready book");
    };
    // Rule 2: category gate - only markets with a verified fee model.
    if sig.fee_rate.is_none() {
        return Submission::none(Decision::Reject, "category gate: fee model unverified");
    };

    let est = estimate_queue(book, sig.price, sig.side, sig.observed_trade_size);

    if cfg.scenario == QueueScenario::Conservative {
        return marketable_submission(sig, budget, &est, "intelligent:conservative-cross");
    }

    // Rule 1: fee-aware price improvement. Tokor TAKER fills prove the level
    // was consumable: join one tick inside the spread when there is room.
    // (Tokor MAKER fills give no such evidence: copy exact.)
    let improved = price_improvement(sig, &est, cfg.min_cross_ticks);
    passive_submission(sig, budget, improved, "intelligent:passive+improve")
}

/// One tick inside the spread on our side, when the spread is at least
/// `min_cross_ticks` wide and the improved price still beats the signal
/// price. Otherwise the signal price.
fn price_improvement(sig: &Signal, est: &QueueEstimate, min_cross_ticks: u32) -> Decimal {
    let Some(cross) = est.opposite_price else {
        return sig.price;
    };
    if min_cross_ticks > 1 {
        let spread = (cross - sig.price).abs();
        if spread < sig.tick * Decimal::from(min_cross_ticks) {
            return sig.price;
        }
    }
    match sig.side {
        // Joining the bid side more aggressively = bid HIGHER (towards the
        // ask), but never above the best ask itself.
        Side::Buy => {
            let improved = sig.price + sig.tick;
            if improved < cross {
                improved
            } else {
                sig.price
            }
        }
        // Ask LOWER (towards the bid), never below the best bid.
        Side::Sell => {
            let improved = sig.price - sig.tick;
            if improved > cross {
                improved
            } else {
                sig.price
            }
        }
    }
}

/// Passive submission with budget clamping (the `Resize` decision).
fn passive_submission(
    sig: &Signal,
    budget: &BudgetState,
    price: Decimal,
    reason: &'static str,
) -> Submission {
    // USDC needed for the full size: shares × price (BUY side pays cash;
    // SELL side commits inventory - modelled on the same notional cap).
    let wanted = sig.price * sig.size;
    if budget.headroom() <= Decimal::ZERO {
        return Submission::none(Decision::ReduceExistingPosition, "cap reached");
    }
    if wanted <= budget.headroom() {
        Submission {
            decision: Decision::CopyPassive,
            side: sig.side,
            price,
            submitted_qty: sig.size,
            marketable: false,
            reason,
        }
    } else {
        let qty = (budget.headroom() / price).floor();
        if qty <= Decimal::ZERO {
            return Submission::none(Decision::ReduceExistingPosition, "cap reached");
        }
        Submission {
            decision: Decision::Resize,
            side: sig.side,
            price,
            submitted_qty: qty,
            marketable: false,
            reason,
        }
    }
}

/// Marketable (crossing) submission against the recorded opposing top.
fn marketable_submission(
    sig: &Signal,
    budget: &BudgetState,
    est: &QueueEstimate,
    reason: &'static str,
) -> Submission {
    let Some(cross) = est.cross_price(sig.side) else {
        return Submission::none(Decision::Wait, "no opposing side to cross");
    };
    let wanted = cross * sig.size;
    if budget.headroom() <= Decimal::ZERO {
        return Submission::none(Decision::ReduceExistingPosition, "cap reached");
    }
    let qty = if wanted <= budget.headroom() {
        sig.size
    } else {
        (budget.headroom() / cross).floor()
    };
    if qty <= Decimal::ZERO {
        return Submission::none(Decision::ReduceExistingPosition, "cap reached");
    }
    Submission {
        decision: if qty == sig.size {
            Decision::CopyMarketable
        } else {
            Decision::Resize
        },
        side: sig.side,
        price: cross,
        submitted_qty: qty,
        marketable: true,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tokor_recorder::book::{BookQuality, Level};

    fn book(
        bids: Vec<(Decimal, Decimal)>,
        asks: Vec<(Decimal, Decimal)>,
    ) -> tokor_recorder::book::BookState {
        tokor_recorder::book::BookState {
            bids: bids
                .into_iter()
                .map(|(p, s)| Level { price: p, size: s })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(p, s)| Level { price: p, size: s })
                .collect(),
            hash: None,
            exchange_ts: None,
            quality: BookQuality::Snapshot,
            updated_ns: 0,
        }
    }

    fn sig(side: Side, price: Decimal, size: Decimal, fee_rate: Option<Decimal>) -> Signal {
        Signal {
            side,
            price,
            size,
            fee_rate,
            tick: dec!(0.01),
            observed_trade_size: size,
            book: Some(book(
                vec![(dec!(0.49), dec!(100))],
                vec![(dec!(0.53), dec!(200))],
            )),
        }
    }

    fn budget() -> BudgetState {
        BudgetState {
            cash: dec!(49),
            gross_exposure: Decimal::ZERO,
            cap: dec!(49),
        }
    }

    #[test]
    fn exact_copy_passes_through_and_clamps() {
        let s = sig(Side::Buy, dec!(0.49), dec!(10), None);
        let sub = exact_copy(&s, &budget(), &PolicyConfig::default());
        assert_eq!(sub.decision, Decision::CopyPassive);
        assert_eq!(sub.price, dec!(0.49));
        assert_eq!(sub.submitted_qty, dec!(10));
        assert!(sub.submits());

        // Oversized: 0.49 × 1000 = $490 ≫ $49 → Resize to floor(49/0.49) = 100.
        let big = sig(Side::Buy, dec!(0.49), dec!(1000), None);
        let sub2 = exact_copy(&big, &budget(), &PolicyConfig::default());
        assert_eq!(sub2.decision, Decision::Resize);
        assert_eq!(sub2.submitted_qty, dec!(100));
    }

    #[test]
    fn no_book_is_data_insufficient() {
        let mut s = sig(Side::Buy, dec!(0.49), dec!(10), None);
        s.book = None;
        let sub = exact_copy(&s, &budget(), &PolicyConfig::default());
        assert_eq!(sub.decision, Decision::DataInsufficient);
        assert!(!sub.submits());
    }

    #[test]
    fn intelligent_gate_rejects_unverified_category() {
        let s = sig(Side::Buy, dec!(0.49), dec!(10), None);
        let sub = intelligent_copy(&s, &budget(), &PolicyConfig::default());
        assert_eq!(sub.decision, Decision::Reject);
    }

    #[test]
    fn intelligent_improves_taker_signal_one_tick() {
        // Spread 0.49→0.53 is 4 ticks wide: a BUY at the bid can improve to
        // 0.50 (one tick inside), which is still below the ask.
        let s = sig(Side::Buy, dec!(0.49), dec!(10), Some(dec!(0.07)));
        let sub = intelligent_copy(&s, &budget(), &PolicyConfig::default());
        assert_eq!(sub.decision, Decision::CopyPassive);
        assert_eq!(sub.price, dec!(0.50));
    }

    #[test]
    fn intelligent_keeps_exact_price_on_tight_spread() {
        // Spread 0.49→0.50 = 1 tick: improvement would land ON the ask -
        // refuse and copy exact.
        let mut s = sig(Side::Buy, dec!(0.49), dec!(10), Some(dec!(0.07)));
        s.book = Some(book(
            vec![(dec!(0.49), dec!(100))],
            vec![(dec!(0.50), dec!(200))],
        ));
        let sub = intelligent_copy(&s, &budget(), &PolicyConfig::default());
        assert_eq!(sub.price, dec!(0.49));
    }

    #[test]
    fn cap_reached_is_reduce() {
        let b = BudgetState {
            cash: dec!(0),
            gross_exposure: dec!(49),
            cap: dec!(49),
        };
        let s = sig(Side::Buy, dec!(0.49), dec!(10), None);
        let sub = exact_copy(&s, &b, &PolicyConfig::default());
        assert_eq!(sub.decision, Decision::ReduceExistingPosition);
        assert!(!sub.submits());
    }

    #[test]
    fn conservative_scenario_crosses() {
        let s = sig(Side::Buy, dec!(0.49), dec!(10), None);
        let cfg = PolicyConfig {
            scenario: QueueScenario::Conservative,
            ..PolicyConfig::default()
        };
        let sub = exact_copy(&s, &budget(), &cfg);
        assert_eq!(sub.decision, Decision::CopyMarketable);
        assert!(sub.marketable);
        // Crossing a BUY pays the ASK.
        assert_eq!(sub.price, dec!(0.53));
    }
}
