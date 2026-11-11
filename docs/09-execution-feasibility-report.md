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

   **First quantitative result — the static cost asymmetry (provable
   today, no capture needed):** mirroring Tokor's 12,596 maker fills
   ($2,461,663 notional) as a *taker* costs **$50,769.77 in fees alone**
   (2.062% of notional; $47,393.79 of it in the 7% crypto markets, 11,191
   fills) — fees Tokor provably never paid (docs/06 §4.1: maker events
   carry fee = 0 on-chain). Add the bid–ask spread and the queue, both
   still unmeasured, on top. Tokor's edge is structurally fee-free; a
   late copier's is structurally fee-burdened. The remaining question is
   only whether signal quality (markout edge) exceeds 2.062% + spread —
   the markout study answers that.

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
    Engine gates green (fmt, clippy `-D warnings`, 105 tests). Reproduction
    chain: `ingest-activity` → `ingest-positions` → `derive-fees` →
    `derive-parquet` → `onchain_role_validation.py` → `record-market` →
    `shadow-run` (CLI wired: replays stored fills through the simulators
    over recorded books/trades; every decision persisted to
    `execution_simulations`). Benchmarks: criterion suite in
    `crates/shadow-exec/benches` — all hot paths microsecond-class
    (queue 308 ns / 50 levels; credit replay 6.5 µs / 1k prints;
    markouts 402 µs / 10k prints), so a full-corpus replay is seconds.
    Datasets: `liquidity_role_validation.parquet` (pass 2 in flight),
    `fills_fees.parquet` (13,965 rows), `activity_events.parquet`
    (13,973 rows), `inventory_reconciliation.parquet` (2,843 legs).

## Non-goals honored

No private keys, no authentication, no signed orders, no submissions —
enforced by the dependency graph (no CLOB order routes exist in any crate)
and by the simulators being pure functions over recorded data.

## Reproduction appendix (exact commands)

All read-only; run from the repo root with the operator env loaded.

```bash
# Data foundation (Phase 1.5 state → datasets)
$BIN --config config/default.toml ingest-activity --resume
$BIN --config config/default.toml ingest-positions
$BIN --config config/default.toml derive-fees
$BIN --config config/default.toml derive-parquet fees      --out data/derived/fills_fees.parquet
$BIN --config config/default.toml derive-parquet activity  --out data/derived/activity_events.parquet
$BIN --config config/default.toml derive-parquet inventory --out data/derived/inventory_reconciliation.parquet

# On-chain role + fee validation (§2; idempotent, receipts cached per tx)
python3 scripts/onchain_role_validation.py            # full corpus
python3 scripts/onchain_role_validation.py --sample 200

# Market-data capture (§3) — runs until ctrl-c
$BIN --config config/default.toml record-market <TOKEN_ID>...

# Latency probes (§4)
$BIN --config config/default.toml observe-source --probes 30

# Shadow replay (§5) — over whatever capture exists
$BIN --config config/default.toml shadow-run --limit 1000 --scenario baseline
$BIN --config config/default.toml shadow-run --limit 1000 --scenario optimistic
$BIN --config config/default.toml shadow-run --limit 1000 --scenario conservative

# Gates + benchmarks
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo bench -p tokor-shadow-exec --bench shadow_bench
```

Every simulated decision is persisted (`execution_simulations`) with its
decision, submitted vs executed quantity, and a `decision_json` blob citing
the policy reason — the audit trail for questions 6–9.

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
