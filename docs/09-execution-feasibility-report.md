# 09 — Phase 2 Executive Feasibility Report

**Status: SKELETON — questions frozen, results pending the on-chain walk, live capture, and simulation runs. Every number in this file will cite its dataset and regeneration command. Read-only throughout: no keys, no auth, no orders.**

---

## The question of Phase 2

Can a follower with **<$50**, acting only on **public** data (wallet activity
feed + public CLOB market channel + public Polygon RPC), economically
replicate Tokor's observable fills? docs/05 proved the economics of what
Tokor *did*; Phase 2 measures what a late, small, queue-blind follower
*could have done* — and says so as an envelope, not a point.

## The ten required questions

1. **Does the chain agree with the feed about who provided liquidity?**
   Pending — full-corpus on-chain walk (`scripts/onchain_role_validation.py`,
   13,331 txs). Sample: 147/18 maker/taker, 0 unknown, side-flag 165/165.
   Will report role distributions per category and the maker-share
   cross-check against the feed's 90.20% count / 59.25% volume / 62.08%
   notional.

2. **Does the fee model survive on-chain ground truth?**
   Yes (docs/06 §4.1): v2 event `fee` field matches
   `size × rate × p × (1−p)` within 1e-5 USDC; makers' events carry fee=0;
   the repo-main `min(p,1−p)` variant is ruled out at p>0.5. Triple-verified:
   feed-side (13,965 fills, 0 unexplained, Σ34,012.44 USDC), model-side,
   chain-side.

3. **How fast can a follower see and react, measured independently?**
   First smoke chain (docs/07 §3.1): poll→decision-ready **p50 573 ms /
   p95 721 ms** (poll→response 319/453 ms; response→book 241/343 ms).
   Fresh-fill reaction bound (time-since-last-poll + chain) to be measured
   during active capture; publication latency is explicitly *not claimed*
   from feed timestamps.

4. **Do the recorder's books survive reconnects, gaps, and staleness?**
   Acceptance suite green (docs/07 §5): delta-before-snapshot refusal,
   replace semantics, gap blocks decisions until labeled refresh,
   reconnect invalidation, staleness rescue, raw-before-normalize.
   REST-hash reconciliation rate: pending offline job (docs/07 §6.2).

5. **How much queue stands ahead of a late follower?**
   Pending — three-scenario envelope (docs/08 §3): Optimistic (level
   consumed by the observed flow), Baseline (all visible same-side size),
   Conservative (cross-now, pay spread + taker fee). Reported per signal
   with `LEVEL_CONSUMED` flagged explicitly.

6. **What fraction of Tokor's maker fills can exact-copy replicate?**
   Pending — exact-copy simulator, fill-credit rules docs/08 §4
   (price-time priority, size accounting, level-integrity cancel,
   staleness freeze). `submitted ≠ executed` reported side by side.

7. **Does intelligent copying beat exact copying net of costs?**
   Pending — intelligent-copy simulator (docs/08 §6): fee-aware price
   improvement, category gate, $49 exposure cap. Net EV = markout − fees −
   spread − adverse selection, per scenario.

8. **How fast does the signal decay?**
   Pending — markouts at +1 s / +5 s / +30 s / +5 m from first sight,
   decomposed into edge vs late-arrival tax (docs/08 §7).

9. **What is the $49 portfolio's net-EV envelope?**
   Pending — portfolio simulator: P(fill|scenario) × (markout edge −
   costs), cancellation slippage risk, over the captured window.
   Quoted as Optimistic/Baseline/Conservative, never a single number.

10. **Is every conclusion reproducible from committed code + data?**
    Engine gates green (fmt, clippy `-D warnings`, 80 tests). Reproduction
    chain: `ingest-activity` → `ingest-positions` → `derive-fees` →
    `derive-parquet` → `onchain_role_validation.py` → `record-market` →
    simulators (CLI pending). All raw evidence retained (receipts, raw
    frames). Datasets: `liquidity_role_validation.parquet` (full corpus
    pending), `fills_fees.parquet`, `activity_events.parquet`,
    `inventory_reconciliation.parquet` (pending positions re-run after
    snapshot fix).

## Non-goals honored

No private keys, no authentication, no signed orders, no submissions —
enforced by the dependency graph (no CLOB order routes exist in any crate)
and by the simulators being pure functions over recorded data.

## Open items carried into the verdict

* v2 fill-event signature name (topic0 pinned empirically; docs/06 §6).
* v2 data words [5..6] (zero so far, recorded raw).
* 21 ROLE_UNKNOWN fills under investigation (walk in flight).
* **Positions API status semantics**: the `/v2/positions` `status=OPEN`
  filter is *advisory* — observed an OPEN page embedding REDEEMABLE rows and
  one token returned under both OPEN and CLOSED. `positions_snapshot` is a
  point-in-time state per token: cross-status duplicates resolved on write
  with priority REDEEMABLE > OPEN > CLOSED (ingest-positions, run
  snapshot-20261110T135639Z: 2,843 distinct tokens from 2,844 api rows).
* REST-hash silent-miss quantification (docs/07 §6.2).
* Tick lattice per market before enabling price-improvement rule (docs/08 §9.1).
