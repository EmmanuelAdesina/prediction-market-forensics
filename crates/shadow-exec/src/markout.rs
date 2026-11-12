//! Markouts (docs/08 §7): the honesty metric.
//!
//! For every signal, the public-tape mid at +1 s / +5 s / +30 s / +5 m
//! after first sight. Markouts answer two separate questions:
//!
//! * **Signal quality** - does Tokor's trade predict short-term drift?
//! * **Adverse-selection tax** - how much of the move happens between
//!   exchange time and our first-seen time (the cost of acting late)?
//!
//! The mid is built from public prints only (tape mid): the recorded book
//! mid is preferred when available and substitutes trivially - the API
//! takes a generic mid series.

use rust_decimal::Decimal;

/// Markout windows in seconds (frozen before results are quoted).
pub const WINDOWS_SECS: [i64; 4] = [1, 5, 30, 300];

/// One window's markout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Markout {
    /// Window length in seconds.
    pub window_secs: i64,
    /// Tape mid at `signal_ts + window` (None if the tape was quiet).
    pub mid: Option<Decimal>,
    /// `mid − signal_mid` (None when mid is None).
    pub delta: Option<Decimal>,
}

/// A public print: (unix ns, price, size).
pub type Print = (i64, Decimal, Decimal);

/// Tape mid at or after `target_ns`: the average of the best bid/ask
/// inferred from the two prints bracketing the moment. Simplified,
/// documented model: the mid at a quiet moment is the LAST print price.
fn tape_mid_at(print_ts: &[i64], print_px: &[Decimal], target_ns: i64) -> Option<Decimal> {
    // Find the last print at or before target.
    let idx = print_ts.partition_point(|&t| t <= target_ns);
    if idx == 0 {
        return None;
    }
    let last = print_px[idx - 1];
    // And the first print strictly after target (the "next touch").
    let next = print_px.get(idx).copied();
    Some(match next {
        Some(n) => (last + n) / Decimal::TWO,
        None => last,
    })
}

/// Compute markouts for one signal against its token's tape.
///
/// `signal_mid` is the mid at first sight (from the recorded book when
/// available). Prints must be sorted by timestamp ascending.
pub fn markouts(signal_ts_ns: i64, signal_mid: Decimal, prints: &[Print]) -> Vec<Markout> {
    let (ts, px): (Vec<i64>, Vec<Decimal>) = prints.iter().map(|&(t, p, _)| (t, p)).unzip();
    WINDOWS_SECS
        .iter()
        .map(|&w| {
            let target = signal_ts_ns + w * 1_000_000_000;
            let mid = tape_mid_at(&ts, &px, target);
            Markout {
                window_secs: w,
                mid,
                delta: mid.map(|m| m - signal_mid),
            }
        })
        .collect()
}

/// Aggregate over many signals: mean markout per window (decimals summed;
/// the report formats). Empty when no window had tape evidence.
pub fn mean_delta(all: &[Vec<Markout>], window_secs: i64) -> Option<Decimal> {
    let mut sum = Decimal::ZERO;
    let mut n = 0u32;
    for row in all {
        for m in row {
            if m.window_secs == window_secs {
                if let Some(d) = m.delta {
                    sum += d;
                    n += 1;
                }
            }
        }
    }
    (n > 0).then(|| sum / Decimal::from(n))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ns(secs: i64) -> i64 {
        secs * 1_000_000_000
    }

    #[test]
    fn markout_windows_use_bracketing_prints() {
        // Signal at t=0, mid 0.50. Prints at +0.5s (0.52) and +2s (0.54).
        let prints = vec![
            (ns(0) + 500_000_000, dec!(0.52), dec!(10)),
            (ns(2), dec!(0.54), dec!(10)),
        ];
        let m = markouts(ns(0), dec!(0.50), &prints);
        // +1s: bracketed by 0.52 (before) and 0.54 (after) → mid 0.53.
        assert_eq!(m[0].window_secs, 1);
        assert_eq!(m[0].mid, Some(dec!(0.53)));
        assert_eq!(m[0].delta, Some(dec!(0.03)));
        // +5s: last print 0.54, nothing after → mid 0.54.
        assert_eq!(m[1].mid, Some(dec!(0.54)));
        assert_eq!(m[1].delta, Some(dec!(0.04)));
    }

    #[test]
    fn quiet_tape_before_first_print_is_none() {
        let prints = vec![(ns(10), dec!(0.52), dec!(10))];
        let m = markouts(ns(0), dec!(0.50), &prints);
        // +1s and +5s targets are before the only print.
        assert_eq!(m[0].mid, None);
        assert_eq!(m[0].delta, None);
        // +30s is after it.
        assert_eq!(m[2].mid, Some(dec!(0.52)));
    }

    #[test]
    fn mean_delta_averages_across_signals() {
        let prints = || {
            vec![
                (ns(0) + 500_000_000, dec!(0.52), dec!(10)),
                (ns(2), dec!(0.54), dec!(10)),
            ]
        };
        let a = markouts(ns(0), dec!(0.50), &prints());
        let b = markouts(ns(0), dec!(0.52), &prints());
        let all = vec![a, b];
        // Window 1s mid 0.53: deltas +0.03 and +0.01 → mean +0.02.
        assert_eq!(mean_delta(&all, 1), Some(dec!(0.02)));
    }
}
