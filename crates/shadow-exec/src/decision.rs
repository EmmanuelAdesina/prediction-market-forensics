//! The shared decision vocabulary (docs/08 §5). Both simulators emit the
//! same grammar so the report can compare policies like-for-like.

use rust_decimal::Decimal;

use tokor_domain::activity::Side;

/// What a simulator decides to do with one signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Decision {
    /// Join Tokor's price level passively (subject to fill-credit rules).
    CopyPassive,
    /// Cross the spread to take (pays taker fee + spread).
    CopyMarketable,
    /// Copy but clamp size to the budget / exposure cap.
    Resize,
    /// Book not decision-ready, or queue scenario says stand aside.
    Wait,
    /// Signal fails policy checks (fee-tier economics, category filter).
    Reject,
    /// The fill would increase exposure we are capped on — copy only the
    /// reduction (net against the existing holding).
    ReduceExistingPosition,
    /// No usable book/trade evidence; counted, never simulated.
    DataInsufficient,
}

impl Decision {
    /// Submitted ≠ executed: only these decisions submit anything.
    pub fn submits(self) -> bool {
        matches!(
            self,
            Decision::CopyPassive
                | Decision::CopyMarketable
                | Decision::Resize
                | Decision::ReduceExistingPosition
        )
    }
}

/// One simulated submission — the ONLY unit the portfolio absorbs, and only
/// through evidenced fills (`fill_credit`), never through `submitted_qty`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Submission {
    pub decision: Decision,
    pub side: Side,
    /// Limit price for passive copies; the crossing price for marketable.
    pub price: Decimal,
    /// Quantity submitted (clamped by policy). MAY be 0 for Wait/Reject/
    /// DataInsufficient.
    pub submitted_qty: Decimal,
    /// Passive or crossing.
    pub marketable: bool,
    /// Why the policy chose this (short static tag for the report).
    pub reason: &'static str,
}

impl Submission {
    /// A no-op submission (nothing sent).
    pub fn none(decision: Decision, reason: &'static str) -> Self {
        Submission {
            decision,
            side: Side::Buy,
            price: Decimal::ZERO,
            submitted_qty: Decimal::ZERO,
            marketable: false,
            reason,
        }
    }

    /// Submitted ≠ executed: whether this submission sends anything.
    /// A submitting decision with zero quantity (e.g. a no-op
    /// ReduceExistingPosition) sends nothing.
    pub fn submits(&self) -> bool {
        self.decision.submits() && self.submitted_qty > Decimal::ZERO
    }
}
