//! Queue-ahead estimation (docs/08 §3).
//!
//! The recorder cannot see exchange-side queues; what we can do is bound
//! them. At first sight of a signal, let `Q_same` be the resting size at
//! Tokor's price level on the side a passive copier would join, and `Q_opp`
//! the opposing top-of-book size. Three scenarios bracket the truth:
//!
//! | Scenario      | Queue ahead assumed        | Justification |
//! |---------------|----------------------------|---------------|
//! | `Optimistic`  | 0                          | Only justified when the observed trade size ≥ `Q_same` (the level emptied as the trade happened). |
//! | `Baseline`    | `Q_same`                   | The honest central case: queue behind everything visible. |
//! | `Conservative`| `Q_same + Q_opp`           | No passive fill at all; execution only via crossing, paying spread + taker fee. |
//!
//! A signal whose price level is absent from the book is tagged
//! `LEVEL_CONSUMED`: the Baseline assumption degenerates and is evaluated
//! under the Optimistic rule with the flag set - never silently.

use rust_decimal::Decimal;

use tokor_domain::activity::Side;
use tokor_recorder::book::BookState;

/// The three queue scenarios (docs/08 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueScenario {
    /// Queue ahead = 0 (justified only if the level was consumed by the
    /// observed flow).
    Optimistic,
    /// Queue ahead = all visible same-side size at the signal price level.
    Baseline,
    /// Queue ahead = same-side size + opposing top-of-book size (cross now).
    Conservative,
}

/// Queue estimate at first sight of a signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueEstimate {
    /// The price level exists in the recorded book.
    pub level_present: bool,
    /// The observed trade size ≥ `Q_same` - the only state in which the
    /// Optimistic assumption (empty level) has support.
    pub optimistic_justified: bool,
    /// The level is absent (or zero) while the observed trade size is
    /// positive: Baseline degenerates (docs/08 §3 queue validity rule).
    pub level_consumed: bool,
    /// Visible same-side resting size at the signal price level.
    pub same_side_size: Decimal,
    /// Opposing top-of-book size (crossing cost anchor).
    pub opposite_side_size: Decimal,
    /// Best opposite price - the crossing price for Conservative.
    pub opposite_price: Option<Decimal>,
}

impl QueueEstimate {
    /// Queue ahead of the follower under `scenario`.
    pub fn queue_ahead(&self, scenario: QueueScenario) -> Decimal {
        match scenario {
            QueueScenario::Optimistic => Decimal::ZERO,
            // A consumed level has same_side_size = 0 - the degenerate
            // Baseline falls out naturally and the `level_consumed` flag
            // carries the caveat for the report.
            QueueScenario::Baseline => self.same_side_size,
            QueueScenario::Conservative => self.same_side_size + self.opposite_side_size,
        }
    }

    /// The crossing price a Conservative (marketable) copy would pay.
    pub fn cross_price(&self, _side: Side) -> Option<Decimal> {
        self.opposite_price
    }
}

/// Estimate the queue at `price` for a passive copy of `side`'s order,
/// given the book state at first sight and the observed trade size that
/// triggered the signal.
///
/// For a `Buy` signal the passive copier joins the **bid** side (a BUY
/// maker order rests on the bids); for `Sell`, the **ask** side.
pub fn estimate_queue(
    book: &BookState,
    price: Decimal,
    side: Side,
    observed_trade_size: Decimal,
) -> QueueEstimate {
    let (ours, opposite) = match side {
        Side::Buy => (&book.bids, &book.asks),
        Side::Sell => (&book.asks, &book.bids),
    };

    let same_side_size: Decimal = ours
        .iter()
        .filter(|l| l.price == price)
        .map(|l| l.size)
        .sum();

    let opposite_top = opposite.first();
    let opposite_side_size = opposite_top.map(|l| l.size).unwrap_or(Decimal::ZERO);
    let opposite_price = opposite_top.map(|l| l.price);

    let level_present = same_side_size > Decimal::ZERO;
    let optimistic_justified = level_present && observed_trade_size >= same_side_size;
    let level_consumed = !level_present && observed_trade_size > Decimal::ZERO;

    QueueEstimate {
        level_present,
        optimistic_justified,
        level_consumed,
        same_side_size,
        opposite_side_size,
        opposite_price,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use tokor_recorder::book::{BookQuality, Level};

    fn book(bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> BookState {
        BookState {
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

    #[test]
    fn baseline_queues_behind_visible_size() {
        // Bid level at 0.50 holds 250 shares across two resting orders.
        let b = book(
            vec![
                (dec!(0.52), dec!(100)),
                (dec!(0.50), dec!(150)),
                (dec!(0.50), dec!(100)),
            ],
            vec![(dec!(0.55), dec!(80))],
        );
        let e = estimate_queue(&b, dec!(0.50), Side::Buy, dec!(100));
        assert_eq!(e.same_side_size, dec!(250));
        assert_eq!(e.queue_ahead(QueueScenario::Baseline), dec!(250));
        assert_eq!(e.queue_ahead(QueueScenario::Optimistic), Decimal::ZERO);
        assert_eq!(e.queue_ahead(QueueScenario::Conservative), dec!(330));
        assert!(e.level_present);
        assert!(!e.level_consumed);
        // Optimistic (0 queue) is only justified if the observed trade
        // consumed the level: 100 < 250 here.
        assert!(!e.optimistic_justified);
    }

    #[test]
    fn optimistic_justified_only_when_trade_consumes_level() {
        let b = book(vec![(dec!(0.50), dec!(120))], vec![(dec!(0.54), dec!(200))]);
        let e = estimate_queue(&b, dec!(0.50), Side::Buy, dec!(120));
        assert!(e.optimistic_justified);
        let e2 = estimate_queue(&b, dec!(0.50), Side::Buy, dec!(119));
        assert!(!e2.optimistic_justified);
    }

    #[test]
    fn sell_signal_joins_ask_side() {
        let b = book(
            vec![(dec!(0.48), dec!(90))],
            vec![(dec!(0.50), dec!(60)), (dec!(0.51), dec!(40))],
        );
        let e = estimate_queue(&b, dec!(0.50), Side::Sell, dec!(60));
        assert_eq!(e.same_side_size, dec!(60));
        assert_eq!(e.opposite_side_size, dec!(90));
        assert_eq!(e.opposite_price, Some(dec!(0.48)));
        assert_eq!(e.queue_ahead(QueueScenario::Conservative), dec!(150));
    }

    #[test]
    fn missing_level_is_tagged_consumed() {
        let b = book(vec![(dec!(0.49), dec!(90))], vec![(dec!(0.55), dec!(80))]);
        let e = estimate_queue(&b, dec!(0.50), Side::Buy, dec!(30));
        assert!(!e.level_present);
        assert!(e.level_consumed);
        // Baseline degenerates to the (now empty) same-side size - zero -
        // and the tag is carried for the report, never hidden.
        assert_eq!(e.queue_ahead(QueueScenario::Baseline), Decimal::ZERO);
        // Conservative still crosses against the opposing top of book.
        assert_eq!(e.queue_ahead(QueueScenario::Conservative), dec!(80));
    }
}
