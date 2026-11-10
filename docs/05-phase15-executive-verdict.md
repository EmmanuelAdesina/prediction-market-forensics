# 05 — Phase 1.5 Executive Verdict

**Verdict: PASS.** The Tokor copy-trading engine's economic model is verified
end-to-end against the wallet's complete public history. Every fill, every
share, and every cent reconciles; the two "unknowns" that motivated this phase
(the 82 rejected fills and the missing-inventory channel) are fully explained,
and one 0.05-share dust residual is explicitly retained.

## The seven required questions

1. **Is every rejected row verdicted?**
   Yes. All 82 Phase-1 "rejected" fills are `VERIFIED_FEE_INCLUSIVE` taker
   sweeps — docs/02 §4. Zero rows remain in `UNKNOWN_FEE_SEMANTICS` or
   `UNEXPLAINED` across all 13,948 unique fills.

2. **Is the fee math tested?**
   Yes, twice. A Decimal-exact Python verification over all 13,948 fills
   (`scripts/fee_verification.py`, zero failures) and a Rust implementation
   with live-fixture tests (`crates/domain/src/fee_tests.rs`, 10 tests incl.
   maker-vs-taker, min-fee boundary, envelope boundary). Ledger closure:
   cash − gross = Σ fees to within 0.0055 USDC over $3.96M (float-envelope
   aggregate).

3. **Are all activity types inspected?**
   Yes — 9 types censused over 19,431 unfiltered rows (docs/03 §5), including
   the two verified-absent types (SPLIT, CONVERSION: 0 rows, verified not
   assumed). Two REDEEM families (paid vs losing-burn) discovered and
   semantics-verified.

4. **Are OPEN + CLOSED positions reconciled?**
   Yes. Condition-level identity
   `bought − sold − redeemed − 2×merged − current = 0` holds **exactly for
   2,009 of 2,010 conditions**; the sole residual is 0.05 shares of dust
   (docs/03 §3), explicitly retained as unresolved.

5. **Is dedup adversarially tested?**
   Yes (docs/04 §4): order-invariance of occurrence identities, same-tx
   byte-identical rows, cross-tx isolation, type-aware validation. The
   empirical audit found 8 composite-collision groups — all byte-identical
   duplicate deliveries, zero genuine collisions.

6. **Is the accounting explainable?**
   Yes (docs/02 §5, docs/03 §3): fee ledger closes to the float envelope;
   share ledger closes to dust. The former "missing inventory" was three
   feed-semantics artifacts (REDEEM token-id duality, losing-burn rows,
   census pagination re-yields) — each demonstrated, none hand-waved.

7. **Are the parquet reports reproducible?**
   Yes: `tokor derive-fees` + `tokor derive-parquet fees|activity|inventory`
   regenerate all three datasets from the SQLite state alone
   (`fee_reconciliation.parquet`, `activity_events.parquet`,
   `inventory_reconciliation.parquet`), with the fee-category map injected as
   an explicit input (`data/derived/event_fee_categories.json`). All engine
   gates green: fmt, clippy `-D warnings`, 56 tests across 8 suites, cargo
   audit clean.

## Corrections inherited by the engine

- `SourceFill::exec_price` is a **cash-implied price**, not the execution
  price (taker cash embeds fees) — docs/04 §6.3.
- Phase-1 completeness claims superseded — docs/04 §6.
- Makers pay zero; taker fee = `size × rate × p × (1−p)` with min-fee
  drop — implemented in `crates/domain/src/fees.rs` as the single source of
  truth for future execution costing.

## What this means for the copy-trading thesis

- **~90% of Tokor's volume is maker** (12,586/13,948 fills): a copying
  strategy that crosses the spread will systematically pay fees Tokor never
  paid, and will systematically miss the maker-side economics. Exact-copy
  fills must therefore be modeled as **resting orders**, not marketable
  sweeps — this is the central design input for the shadow-execution
  feasibility harness (Objective 5, next phase).
- Fill prices for cost basis should use the displayed `price` (plus fee only
  when the copier is a taker).

## Next step

Objective 5: shadow execution feasibility — forward-running harness on
recorded market data, exact-copy (resting at Tokor's price) vs intelligent-copy
(depth-aware), with real min sizes, tick sizes, fees, and depth; no live
orders, no keys.
