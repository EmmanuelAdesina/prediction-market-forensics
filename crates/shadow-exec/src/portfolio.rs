//! The $49 portfolio simulator (docs/08 §1, §6 rule 3).
//!
//! Absorbs only EVIDENCED fills (`fill_credit` outcomes) - never raw
//! submissions. Gross notional is hard-capped at the budget; overflow is
//! refused, not silently absorbed. Shorting is out of scope for a small
//! follower: sells only reduce held shares (a copy of an opposite-side
//! Tokor fill is modelled as a sell of what we hold, or skipped).

use rust_decimal::Decimal;
use std::collections::BTreeMap;

use tokor_domain::activity::Side;

/// Hard budget: the follower starts with less than $50.
pub const BUDGET_USDC: Decimal = Decimal::from_parts(49, 0, 0, false, 0);

/// One token's position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Position {
    /// Net shares (always ≥ 0 - no shorting).
    pub shares: Decimal,
    /// Average entry price of the position.
    pub avg_price: Decimal,
    /// Total fees paid (USDC) on this position.
    pub fees_paid: Decimal,
}

/// A simulated execution (already evidence-backed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimFill {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    /// Taker fee in USDC (zero for passive fills - on-chain fact, docs/06
    /// §4.1). Computed with the verified model upstream.
    pub fee: Decimal,
}

/// Portfolio error - the simulator refuses impossible states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortfolioError {
    /// Gross-notional cap would be violated by this entry.
    CapViolated { cap: Decimal, would_be: Decimal },
    /// Selling more than held.
    InsufficientShares { held: Decimal, selling: Decimal },
    /// Negative or zero quantity.
    BadQuantity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioSim {
    /// Cash in USDC (starts at the budget).
    pub cash: Decimal,
    /// Hard gross-notional cap.
    pub cap: Decimal,
    pub positions: BTreeMap<String, Position>,
}

impl Default for PortfolioSim {
    fn default() -> Self {
        PortfolioSim::new(BUDGET_USDC)
    }
}

impl PortfolioSim {
    pub fn new(budget: Decimal) -> Self {
        PortfolioSim {
            cash: budget,
            cap: budget,
            positions: BTreeMap::new(),
        }
    }

    /// Gross notional at entry prices (Σ shares × avg_price).
    pub fn gross_exposure(&self) -> Decimal {
        self.positions
            .values()
            .map(|p| p.shares * p.avg_price)
            .sum()
    }

    /// Apply one evidenced fill. Buys pay `price × qty + fee` from cash;
    /// sells add `price × qty − fee`. The gross-notional cap is checked
    /// before the fill lands (entry notional at the fill price; reducing
    /// an existing position never adds gross).
    pub fn apply(&mut self, fill: &SimFill) -> Result<(), PortfolioError> {
        if fill.qty <= Decimal::ZERO {
            return Err(PortfolioError::BadQuantity);
        }
        let notional = fill.price * fill.qty;
        let held = self
            .positions
            .get(&fill.token_id)
            .map(|p| p.shares)
            .unwrap_or_default();

        match fill.side {
            Side::Buy => {
                let would_be = self.gross_exposure() + notional;
                if would_be > self.cap {
                    return Err(PortfolioError::CapViolated {
                        cap: self.cap,
                        would_be,
                    });
                }
                let pos = self.positions.entry(fill.token_id.clone()).or_default();
                let new_shares = pos.shares + fill.qty;
                let new_cost = pos.avg_price * pos.shares + fill.price * fill.qty;
                pos.avg_price = new_cost / new_shares;
                pos.shares = new_shares;
                pos.fees_paid += fill.fee;
                self.cash -= notional + fill.fee;
            }
            Side::Sell => {
                if fill.qty > held {
                    return Err(PortfolioError::InsufficientShares {
                        held,
                        selling: fill.qty,
                    });
                }
                let pos = self.positions.get_mut(&fill.token_id).unwrap();
                pos.shares -= fill.qty;
                pos.fees_paid += fill.fee;
                if pos.shares == Decimal::ZERO {
                    pos.avg_price = Decimal::ZERO;
                }
                self.cash += notional - fill.fee;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn fill(side: Side, price: Decimal, qty: Decimal, fee: Decimal) -> SimFill {
        SimFill {
            token_id: "T1".into(),
            side,
            price,
            qty,
            fee,
        }
    }

    #[test]
    fn buys_and_sells_move_cash_and_avg() {
        let mut p = PortfolioSim::default();
        p.apply(&fill(Side::Buy, dec!(0.50), dec!(20), dec!(0)))
            .unwrap();
        assert_eq!(p.cash, dec!(39));
        let pos = p.positions.get("T1").unwrap();
        assert_eq!(pos.shares, dec!(20));
        assert_eq!(pos.avg_price, dec!(0.50));

        // Add at a different price: avg weighted.
        p.apply(&fill(Side::Buy, dec!(0.60), dec!(20), dec!(0)))
            .unwrap();
        let pos = p.positions.get("T1").unwrap();
        assert_eq!(pos.avg_price, dec!(0.55));
        assert_eq!(p.gross_exposure(), dec!(22));

        // Sell half: cash in, avg unchanged.
        p.apply(&fill(Side::Sell, dec!(0.60), dec!(20), dec!(0)))
            .unwrap();
        assert_eq!(p.cash, dec!(39)); // 49 − 10 − 12 + 12
        let pos = p.positions.get("T1").unwrap();
        assert_eq!(pos.shares, dec!(20));
        assert_eq!(pos.avg_price, dec!(0.55));
    }

    #[test]
    fn fees_leave_cash_but_not_basis() {
        let mut p = PortfolioSim::default();
        p.apply(&fill(Side::Buy, dec!(0.50), dec!(10), dec!(0)))
            .unwrap();
        p.apply(&fill(Side::Buy, dec!(0.50), dec!(10), dec!(1)))
            .unwrap();
        assert_eq!(p.cash, dec!(38)); // 49 − 5 − 6
        let pos = p.positions.get("T1").unwrap();
        assert_eq!(pos.fees_paid, dec!(1));
        // Fees are USDC on top - never inside the share basis (docs/06 §5).
        assert_eq!(pos.avg_price, dec!(0.50));
    }

    #[test]
    fn cap_blocks_oversized_entry() {
        let mut p = PortfolioSim::default();
        let err = p
            .apply(&fill(Side::Buy, dec!(0.60), dec!(100), dec!(0)))
            .unwrap_err();
        assert_eq!(
            err,
            PortfolioError::CapViolated {
                cap: dec!(49),
                would_be: dec!(60)
            }
        );
        // Nothing mutated.
        assert!(!p.positions.contains_key("T1"));
        assert_eq!(p.cash, dec!(49));
    }

    #[test]
    fn oversell_refused() {
        let mut p = PortfolioSim::default();
        p.apply(&fill(Side::Buy, dec!(0.50), dec!(10), dec!(0)))
            .unwrap();
        let err = p
            .apply(&fill(Side::Sell, dec!(0.50), dec!(11), dec!(0)))
            .unwrap_err();
        assert_eq!(
            err,
            PortfolioError::InsufficientShares {
                held: dec!(10),
                selling: dec!(11)
            }
        );
    }

    #[test]
    fn reducing_long_never_violates_cap() {
        let mut p = PortfolioSim::default();
        p.apply(&fill(Side::Buy, dec!(0.90), dec!(50), dec!(0)))
            .unwrap(); // $45
                       // Selling reduces gross exposure: allowed at any price.
        p.apply(&fill(Side::Sell, dec!(0.95), dec!(10), dec!(0)))
            .unwrap();
        assert_eq!(p.positions.get("T1").unwrap().shares, dec!(40));
        assert_eq!(p.cash, dec!(13.5)); // 49 − 45 + 9.5
    }
}
