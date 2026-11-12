# 03 - Inventory Reconciliation (Phase 1.5)

**Status: CLOSED - every share accounted for across all 2,010 conditions; total
unexplained inventory: 0.05 shares (dust).**
Artifacts: `data/derived/inventory_replay_v2.json` (final model), Rust command
`tokor ingest-positions` (snapshot) + `tokor derive-parquet inventory` (emits
`data/derived/inventory_reconciliation.parquet`).

## 1. Question and method

Phase 1 could not explain where disposed shares went: token-level replay
against the positions census left large residuals, and MERGE activity rows
carried no `token_id`. Phase 1.5 asks: **does every share ever bought equal
shares sold + redeemed + merged + currently held, per condition?**

Method: replay the unfiltered activity walk (19,431 rows; census below)
against the all-status positions snapshot, at **condition level**. All
arithmetic in exact Decimal.

## 2. Corrections to the Phase-1.5 working assumptions

Three feed-semantics discoveries invalidated the earlier per-token model:

1. **REDEEM rows carry `usdc_size` proceeds - sometimes.** Two families:
   - **Paid** (1,329 rows): `usdc == size` exactly ($1 per winning share);
     Σ proceeds **2,471,231.252312 USDC**.
   - **Zero** (886 rows): `usdc == 0`; these are **losing-side burns** inside
     batch "redeem-all" transactions - Σ burned **1,721,303.659120 shares**.
   The earlier "REDEEM rows always have `usdc_size = 0`" observation was a
   sampling artifact; the "$1/share derived proceeds" model coincided with
   the truth only because winning redemptions pay exactly $1×size.

2. **REDEEM/positions token-id duality.** A REDEEM row's `token_id` need not
   match the traded token id of the same condition (verified:
   `bitcoin-above-84k-on-october-21`: BUY token `34107239…`, REDEEM token
   `17925991…`, same condition, same 5,000 shares). Token-level attribution
   therefore fails; **condition-level attribution is the only defensible
   unit**. This artifact produced the entire Phase-1 "missing inventory".

3. **Positions pagination re-yields rows.** 2,827 raw census rows contain 4
   duplicated tokens (3 REDEEMABLE re-yields across page boundaries, 1 token
   listed under both OPEN and CLOSED). Distinct legs: **2,823** -
   131 OPEN + 2,689 CLOSED + 3 REDEEMABLE.

## 3. The verified identity

For every condition touched by the wallet:

```
bought − sold − redeemed − 2 × merged − currently_held = 0
```

`2 × merged` because a MERGE consumes one share from EACH of the two
complementary outcomes per pair (verified: MERGE rows carry
`size == usdc_size`, i.e. pairs × $1).

| result | value |
|---|---|
| conditions touched | 2,010 |
| identity exact (±0.002 shares) | **2,009** |
| residual conditions | **1** |
| total \|residual\| | **0.05 shares** |

The single residual: `0xbf35c437…` (WTI closes above $96), 0.05 shares
bought that were never recorded in any redeem/merge/sale - 5 cents of dust at
resolution, **explicitly retained as unresolved** per the no-force-fit rule.

## 4. What the Phase-1 "52 discrepancies" were

- **Paired complementary merges** - each MERGE pair takes equal shares from
  the Yes and No legs; per-leg derived merges sum to 2 × reported merge size
  (was verified exactly before the condition-level model superseded it).
- **Losing-side resolution burns** - zero-proceeds REDEEM rows in batch
  redemption transactions (886 rows, 1.72M shares burned).
- **Token-id duality on REDEEM rows** - attribution artifact, not missing data.
- **Census pagination duplicates** - 4 double-counted legs.

**No negRisk conversion or split was needed to close the books** - the
activity feed produced zero CONVERSION/SPLIT rows (verified, not assumed), and
the condition-level identity closes without them. The three conditions that
looked like conversion candidates under the per-token model (current = 2 ×
bought) were exactly the duplicated REDEEMABLE census rows.

## 5. Activity-type census (wallet-complete, Jul 10 → Nov 10 2026)

| type | rows | semantics verified |
|---|---|---|
| TRADE | 13,956 raw / 13,948 unique | fees per docs/02 |
| MERGE | 2,835 | size == usdc (pairs × $1); empty token_id |
| REDEEM | 2,215 | paid 1,329 / burn 886 (§2.1) |
| REWARD | 122 | cash-only |
| YIELD | 105 | cash-only |
| TAKER_REBATE | 83 | cash-only |
| MAKER_REBATE | 115 | cash-only |
| SPLIT | 0 | verified absent (wallet produces none) |
| CONVERSION | 0 | verified absent (wallet produces none) |

## 6. Exit-criteria mapping

- *All activity types inspected:* §5 - every type the wallet produces,
  including verified-absent types ✓
- *OPEN + CLOSED positions reconciled:* condition-level identity exact for
  2,009/2,010; sole remainder 0.05 shares retained as unresolved dust ✓
- *Merges/redeems/conversions closed as the missing channel:* §3–4 ✓
- *No position reduction assumed to be a SELL:* disposals split into
  sells (activity), merges (pairs), redeem-payouts, and burns - each with its
  own evidence ✓
- *On-chain CTF probes:* **not required for closure** - the feed-level identity
  closes without them; an optional confirmation probe is documented in
  docs/05 (next phase) rather than treated as a gap.
