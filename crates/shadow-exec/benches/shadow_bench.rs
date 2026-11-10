//! §5 benchmarks: the hot paths a full-corpus shadow run touches.
//!
//! Run: `cargo bench -p tokor-shadow-exec`

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokor_domain::activity::Side;
use tokor_recorder::book::{BookQuality, Level};
use tokor_shadow_exec::copy_sim::{exact_copy, BudgetState, PolicyConfig, Signal};
use tokor_shadow_exec::fill_credit::{credit_fills, EvidenceEvent, PassiveOrder};
use tokor_shadow_exec::markout::{markouts, Print};
use tokor_shadow_exec::portfolio::{PortfolioSim, SimFill};
use tokor_shadow_exec::queue::estimate_queue;

fn book(levels: usize) -> tokor_recorder::book::BookState {
    tokor_recorder::book::BookState {
        bids: (0..levels)
            .map(|i| Level { price: dec!(0.50) - Decimal::from(i), size: dec!(100) + Decimal::from(i) })
            .collect(),
        asks: (0..levels)
            .map(|i| Level { price: dec!(0.55) + Decimal::from(i), size: dec!(100) + Decimal::from(i) })
            .collect(),
        hash: None,
        exchange_ts: None,
        quality: BookQuality::Snapshot,
        updated_ns: 0,
    }
}

fn bench_queue(c: &mut Criterion) {
    let b = book(50);
    c.bench_function("queue/estimate_50_levels", |g| {
        g.iter(|| estimate_queue(black_box(&b), black_box(dec!(0.48)), black_box(Side::Buy), black_box(dec!(120))))
    });
}

fn bench_credit(c: &mut Criterion) {
    let order = PassiveOrder {
        side: Side::Buy,
        price: dec!(0.50),
        submitted_ns: 0,
        remaining_qty: dec!(500),
        queue_ahead: dec!(1_000),
    };
    let events: Vec<EvidenceEvent> = (0..1_000)
        .map(|i| EvidenceEvent::Trade {
            ts_ns: 1_000 + i,
            price: dec!(0.50),
            size: dec!(10),
            aggressor_side: Side::Sell,
        })
        .collect();
    c.bench_function("fill_credit/1000_prints", |g| {
        g.iter(|| credit_fills(black_box(&order), black_box(&events)))
    });
}

fn bench_copy(c: &mut Criterion) {
    let b = book(50);
    let sig = Signal {
        side: Side::Buy,
        price: dec!(0.50),
        size: dec!(100),
        fee_rate: Some(dec!(0.07)),
        tick: dec!(0.01),
        observed_trade_size: dec!(100),
        book: Some(b),
    };
    let budget = BudgetState { cash: dec!(49), gross_exposure: Decimal::ZERO, cap: dec!(49) };
    c.bench_function("copy_sim/exact", |g| {
        g.iter(|| exact_copy(black_box(&sig), black_box(&budget), black_box(&PolicyConfig::default())))
    });
}

fn bench_portfolio(c: &mut Criterion) {
    c.bench_function("portfolio/apply_1000_fills", |g| {
        g.iter(|| {
            let mut p = PortfolioSim::default();
            for i in 0..1_000u64 {
                // Oversized fills are refused by the cap; keep each well under.
                let _ = p.apply(&SimFill {
                    token_id: format!("T{}", i % 50),
                    side: Side::Buy,
                    price: dec!(0.01),
                    qty: dec!(10),
                    fee: Decimal::ZERO,
                });
            }
            p
        })
    });
}

fn bench_markouts(c: &mut Criterion) {
    let prints: Vec<Print> = (0..10_000)
        .map(|i| (i * 100_000_000, dec!(0.50) + Decimal::from(i % 5) * dec!(0.01), dec!(10)))
        .collect();
    c.bench_function("markouts/10k_prints", |g| {
        g.iter(|| markouts(black_box(0), black_box(dec!(0.50)), black_box(&prints)))
    });
}

criterion_group!(
    benches,
    bench_queue,
    bench_credit,
    bench_copy,
    bench_portfolio,
    bench_markouts
);
criterion_main!(benches);
