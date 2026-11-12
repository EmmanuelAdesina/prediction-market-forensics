# 02 - Fee Model Investigation (Phase 1.5)

**Status: CLOSED - model verified on the complete fill history; zero unexplained rows.**
Artifacts: `data/derived/fee_reconciliation.json` (13,948 rows), Rust implementation
`crates/domain/src/fees.rs` + fixtures `crates/domain/src/fee_tests.rs`, CLI
`tokor derive-fees` (persists to `fee_reconciliation` table; emits
`data/derived/fee_reconciliation.parquet` via `tokor derive-parquet fees`).

## 1. Question and method

Phase 1 left 82 fills "rejected" because `usdc_size` exceeded `price × size`,
and a separate price-display rule (`exec_price = usdc/size`) was inconsistent
across rows. Phase 1.5 asks one question: **is there a single fee accounting
model that explains every observed fill?**

Method (no fabrication at any step):

1. Categorize every market via Gamma tags → fee category
   (`scripts/fetch_event_categories.py`, cached in
   `data/derived/event_fee_categories.json`; **567/567 events resolved**,
   census: crypto 508, finance 55, sports 2, politics 2).
2. Apply the published schedule (help.polymarket.com "Trading Fees",
   article 13364478) as the hypothesis.
3. Verify against all 13,948 unique fills (`scripts/fee_verification.py`),
   Decimal-exact, no float.

## 2. The verified model

For a **taker** fill of `C` shares at displayed price `p`:

```
fee  = round5( C × feeRate × p × (1−p) ),  dropped to 0 when unrounded < 0.00001
cash = gross + fee        with |cash − gross − fee| ≤ 2×10⁻⁵ (float envelope)
```

- `feeRate` by category: **crypto 0.07, finance 0.04 (WTI/gold), sports 0.05,
  politics 0.04.** Fee precision 5 decimals; minimum fee 0.00001 USDC.
- **Makers pay zero** (published schedule, corroborated by the wallet's 115
  MAKER_REBATE cash rows).
- The backend serializes `cash` with binary-float arithmetic: observed
  residuals `cash − gross − round5(fee)` satisfy `|residual| ≤ 1.31×10⁻⁵`
  (777 of 1,361 fee-bearing rows exactly 0; 564 negative, 20 positive).
  This is a serialization envelope, not a model failure.

Premium structure: the fee as a fraction of gross notional is
`fee/gross = feeRate × (1−p)`. Across all 1,361 taker fills the observed
premium spans **0.007%–6.993%** (crypto median 3.99%), driven purely by the
entry price - the dip-ladder family bought at `p ≈ 0.05..0.07`, where the
premium is `0.07×(1−p) ≈ 6.51–6.65%`, exactly the band Phase 1 could not
explain. Worked example (a formerly "rejected" row):
`2400 × 0.07 × 0.05 × 0.95 = 7.98` ⇒ `usdc_size` of price 0.05 × 2400 = 120.00
+ 7.98 = **127.98** ✓ (fee = 6.65% of gross).

## 3. Verdicts over the full history

| verdict | rows | meaning |
|---|---|---|
| `VERIFIED_GROSS_NOTIONAL` | 12,587 | cash == gross exactly: maker fills (12,586) + one min-size WTI row |
| `VERIFIED_FEE_INCLUSIVE` | 1,361 | cash == gross + fee within the envelope |
| `UNKNOWN_FEE_SEMANTICS` / `UNEXPLAINED` | **0** | - |

Fee-bearing split by rate: 0.07 → 843 rows; 0.04 → 517 rows; 0.05 → 1 row.
No fill ever required a category rate other than the published one.

## 3b. Maker share by fill count, share volume, and gross notional

| measure | maker | taker | maker share |
|---|---|---|---|
| fill count | 12,587 | 1,361 | **90.24%** |
| share volume | 5,465,656.10 | 3,741,180.54 | **59.37%** |
| gross notional | 2,461,100.60 | 1,464,251.42 | **62.70%** |

The divergence matters for replication: Tokor's *typical* fill is a small
resting order, but the *taker sweeps* are the large ones - a follower crossing
the spread pays fees on the biggest flow while Tokor paid none of it.

Role evidence note: the maker/taker split above is **inferred from cash
semantics** (fee-bearing ⇒ taker; exact-gross on a fee-category market ⇒
maker). On-chain confirmation status is treated separately in
docs/06-maker-taker-validation.md (`CONFIRMED_*` vs `INFERRED_*` vs
`ROLE_UNKNOWN`); the two views must agree where both exist.

## 4. The structural proof that fee-free ⇒ maker

Fees and fee-free fills interleave from day one (no date boundary), so the
split is not temporal. Transaction-level structure settles it:

- 13,317 transactions: **11,956 uniformly fee-free, 1,361 uniformly
  fee-bearing, 0 mixed.** A transaction contains exactly one wallet order, so
  faces cannot mix inside it.
- Taker sweeps cross multiple book levels (multi-fill txs) and always pay the
  fee; the fee-free rows rest as maker orders and are hit.
- The published schedule charges makers nothing; the wallet earns
  MAKER_REBATE cash rows consistent with maker activity.

**Conclusion: Tokor is a ~90%-maker trader (12,586 maker fills vs 1,361
taker fills), and every one of the Phase-1 "82 rejected rows" is a taker
sweep whose `usdc_size` embeds the category fee.**

## 5. Ledger-level accounting closure

| quantity | value |
|---|---|
| Σ gross notional | 3,925,352.020010 USDC |
| Σ observed cash | 3,959,093.503431 USDC |
| cash − gross | 33,741.483421 USDC |
| Σ model fee (round5) | 33,741.488880 USDC |

Aggregate drift **0.00546 USDC over $3.96M (1.4×10⁻⁷ relative)** - the sum of
per-row serialization residuals. There is no missing or extra money anywhere;
the residual is exactly the float-envelope accumulation documented in §2.

## 6. Corrections to Phase 1

- **Phase-1 "price-display rule" (`exec_price = usdc/size`) was a misreading.**
  `usdc_size` is fee-inclusive for taker fills; the true execution price is the
  displayed `price` field. The 265-row "floor₂ family" dissolves once fees are
  modeled. **Implemented in Phase 2 §1:** the engine field is renamed
  `SourceFill::cash_implied_price` (a cash view, never the execution price)
  with `gross_usdc()` separating the fee-free notional; the four money
  quantities (displayed price / gross / fee / total cash) are distinct fields
  with regression tests (`crates/domain/src/accounting_tests.rs`).
- **The 82 "rejected" rows were taker sweeps** - all VERIFIED_FEE_INCLUSIVE.
- The earlier "floor-truncation" rounding hypothesis (joint floor5) was
  **wrong**: the residuals split across HALF_UP tie cases and 2-ulp
  outliers that only binary-float serialization explains. The model above
  replaced it; no defensible reconciliation existed for a pure-decimal rule.
- **Ingestion validation is now fee-aware** (Phase 2 §1): the pre-fee 5%
  cash-vs-gross tolerance rejected valid taker rows whose premium
  `rate×(1−p)` exceeds 5% (first observed live on 2026-11-10: three crypto
  7%-rate rows at 5.18–5.74% premium, txs `0xdb01bd6a…`, `0x675d4619…`,
  `0xa0a8a4cb…`). `SourceFill::from_raw` now accepts any row whose cash lies
  in `[gross − 2×10⁻⁵, gross × 1.07 + 2×10⁻⁵]` - verified over the full
  13,956-row raw corpus with 0 failures - and out-of-band rows are still
  rejected and preserved raw (never force-fit).

## 7. Engine implementation and tests

- `crates/domain/src/fees.rs`: `FeeCategory` (published rates incl.
  economics/culture/weather/other 0.05, mentions/tech 0.04, geopolitics 0),
  `charged_fee` (HALF_UP @ 5 dp, min-fee drop), `classify_fill` returning
  `(FillVerdict, FillFace)` - maker/taker derived from observed semantics,
  never assumed.
- `crates/domain/src/fee_tests.rs`: 10 tests incl. **live fixtures** -
  taker crypto sweep tx `0xb31294…`, finance taker tx `0xe114afcd…`, **maker
  WTI fill tx `0xb3973625…` (would-be fee 71.70, charged 0)**, min-fee
  boundary pair (9.939×10⁻⁶ dropped vs 1.0009×10⁻⁵ charged), envelope
  boundary, out-of-range validation.
- `tokor derive-fees` persists the per-fill verdict to the
  `fee_reconciliation` table (migration 0003) and prints the census;
  `tokor derive-parquet fees` emits the deliverable parquet.

## 8. Exit-criteria mapping

- *Every rejected row verdicted:* 82/82 VERIFIED_FEE_INCLUSIVE ✓
- *Fee math tested:* Rust fixtures + 13,948-row Decimal verification ✓
- *No reclassification without reconciliation:* every row's category comes
  from Gamma tags; every residual bounded and signed ✓
- *No assumption that `base_fee=1000 ≡ feeRate=0.07`: the `base_fee` field was
  never used; rates derive from the published schedule + per-event Gamma tags ✓

## 9. Market-level fee activation (verified)

Fee activation is per-market (per-category), not per-fill:

- **158 markets carry both maker and taker fills**: in every one, all taker
  fills carry exactly one fee rate and all maker fills carry zero - 0
  violating markets, 0 markets with more than one observed taker rate.
- **37 taker-only markets**: every taker fill fee-bearing (0 missing).

This is consistent with the published schedule's category model and rules out
per-market fee opt-outs for every market the wallet traded. The Phase-2
recorder additionally captures the CLOB market object's fee fields
(`maker_base_fee`, `taker_base_fee`) for forward verification.
