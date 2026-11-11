# 10 — Strategy Inference: The Dip-Ladder

**Phase 2 · Status: pattern characterized from the verified corpus (13,965 fills, fees triple-verified); live validation in progress (capture started 2026-11-11) · Read-only throughout.**

---

## 1. The question

docs/09 answers the *copy-the-fills* question (pessimistically — the static
cost asymmetry alone is 2.062% per notional dollar). This document asks the
other question: **can the strategy itself be inferred from public data and
run directly?** For Tokor's dominant activity, the answer turns out to be
descriptive rather than mysterious.

## 2. The pattern (empirically characterized)

Across every actively-traded "Will X dip to $Y by date" crypto market in
the corpus, the same behavior appears:

| Element | Observation (evidence: `source_fills` + `fee_reconciliation`) |
|---|---|
| Universe | Crypto dip markets, 7% fee tier |
| Direction | **One-way: BUY only, zero sells** on the tracked tokens |
| Execution | ~90% maker: fixed-size resting clips (2,000/4,000/6,000 shares) at successively lower price rungs |
| Laddering | Rungs descend with the market: ETH-$1,750 bought at 0.51 → 0.50 → 0.49 → 0.47 → 0.31 → 0.26 → 0.22 → … → 0.08 |
| Sizing | Aggressive scaling into decline — 30,000-share clips near the bottom (ETH-$1,500 @ 0.098) |
| Exit | **Hold to resolution**: redeem at $1.00 if the dip occurs; no stop-outs observed |
| Occasional taker use | Sweeps when visible liquidity appears (e.g. 20,000 @ 0.25 on 2026-11-10, $5,262 — the single largest fill) |

## 3. Family ledger (per positions snapshot 2026-11-10/11)

| Market | Fills (B/S) | Maker share | Avg entry | Held | Status | Realized |
|---|---|---|---|---|---|---|
| ETH dip $1,750 | 82 / 0 | ~90% | 0.1763 | 180,557 | OPEN | +4,410.26 * |
| ETH dip $1,500 | 14 / 0 | ~71% | 0.109 | 27,099 | **CLOSED** | **+14,648.15** |
| HYPE dip $70 | 15 / 0 | 87% | 0.4951 | 588 | OPEN | +15.29 |
| HYPE dip $60 | 17 / 0 | 88% | 0.1706 | 1,424 | OPEN | 0 |
| XRP dip $1.00 | 2 / 0 | 100% | 0.3939 | 0 | **CLOSED** | +2.55 |

\* ETH-$1,750 shows realized PnL with zero SELLs on this token — the feed's
`realized_pnl` appears to attribute complementary-token trades (the NO
side) to the position. Tracked as an open item; does not affect the
ladder characterization.

**Two of the ladders have already resolved in Tokor's favor** (ETH-$1,500
and XRP), which makes this a completed round-trip of the strategy, not
just an open position: buy the ladder → hold → redeem at $1.00.

## 4. What this means for replication

* **In kind, the strategy is fully public-data-driven**: a follower could
  run the same rule (ladder YES on dip markets as price declines) with
  small clips. No insider information is apparent — the inputs are the
  market's own price and time.
* **In scale, it is capital-hungry**: ETH-$1,750's cost basis alone is
  ~$34,000 (180,557 × 0.1763), the 2026-11-10 sweep was a single $5,262
  fill, and the sizing grows precisely when the position is losing
  (martingale). A $49 follower can run the *shape* but not the *size* —
  and the martingale means drawdown survival is a capital question, not a
  skill question.
* **The edge decomposition** for docs/09's envelope:
  1. market selection (which dip markets to ladder) — learnable, plausibly
     the scarcest part of the edge;
  2. maker execution (0 fees, queue-dependent) — measurable in our capture;
  3. holding discipline (no stop-outs, redeem at resolution) — trivially
     copyable but psychologically/payroll-wise demanding.

## 5. Validation plan (in flight)

1. **Live capture** (started 2026-11-11, 6 active ladder tokens): books +
   public trades for ≥ 48 h → queue-ahead distributions and markouts on
   the *currently active* rungs.
2. **Resolution outcomes**: the open ladders (ETH-$1,750, HYPE-$60/$70)
   resolve by Dec 31 — the strategy's next completed round-trips are
   observable in the wild.
3. **Rule formalization**: fit the ladder parameters (rung spacing, clip
   size, scaling law) from the corpus; then test whether a mechanical
   public-data rule reproduces Tokor's entry timing (docs/09's
   strategy-inference study).

## 6. Open items

1. `realized_pnl` attribution on complementary tokens (footnote *).
2. Whether the ladder's clip sizes follow a fixed schedule or a
   volatility/liquidity-responsive rule (fit from corpus).
3. Market-selection rule: why these dip levels ($1,750 vs $1,500 both
   laddered — strike spacing suggests a deliberate ladder of ladders).
