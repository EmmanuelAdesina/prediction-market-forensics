# 08 - Shadow Execution Model: Queue Estimation & Replication Simulation

**Phase 2 §5 · Status: design frozen; implementation in `crates/shadow-exec` · Strictly read-only: no orders, no keys, no submissions. A simulated order is NEVER an executed trade.**

---

## 1. The question this document answers

Phase 1.5 established *what* Tokor trades and *what it pays* (fees triple-verified,
docs/02 + docs/06). docs/06 established the role mix on-chain: the wallet is
predominantly a **resting maker** (~90% of fills by count). §5 asks the
economic question:

> Can a follower with **<$50** of capital, watching only **public** data
> (wallet activity feed + CLOB market channel), economically replicate
> Tokor's observable fills - and what does the unavoidable latency +
> queue uncertainty do to the result?

Three coupled components give the answer:

1. **Queue-ahead engine** - given the recorded book state at the moment we
   *first see* a signal, estimate how much same-side queue stands between
   the follower and the front of the book at Tokor's price level.
2. **Fill simulators** - exact-copy and intelligent-copy policies that
   decide `submit / not submit` and, separately, what would have filled.
3. **Portfolio simulator** - a $49 account that absorbs those simulated
   fills and reports net EV after fees, spread cost, and adverse selection.

The cardinal rule, stated everywhere it matters:
**submitted ≠ executed.** Every simulator output separates the quantity
*submitted* from the quantity *evidenced as filled*; fills are only credited
under the explicit conditions of §4.

## 2. Inputs (all public, all recorded by the §3 recorder)

| Input | Source | Freshness guarantee |
|---|---|---|
| Book snapshot + deltas at signal time | `normalized_book_states` | `is_decision_ready`: state ∧ ¬gap_pending ∧ ¬stale |
| Tokor's fill event | wallet activity feed → `source_fills` | 1 s exchange timestamp (documented resolution) |
| Public trades after the signal | `public_trade_events` | markouts + queue validation |
| Fee model | docs/02 (triple-verified) | `size × rate × p × (1−p)`, round5, min 1e-5 |

A signal whose book is not decision-ready at the moment of first sight is
**not simulated** - it is recorded as `DATA_INSUFFICIENT` and counted.
Silently filling from a stale book is the classic shadow-simulator lie; we
refuse it by construction.

## 3. Queue-ahead estimation: three scenarios, one bound

The recorder cannot see exchange-side order queues (nobody can, from public
data). What we *can* do is bound them. At the moment we first observe the
signal, let `Q_same` be the resting size at Tokor's price level on the same
side we would need to join (from the book state), and `Q_opp` the opposing
top-of-book size. The follower, arriving late, sits **behind** `Q_same` at
best. Three scenarios define the envelope:

| Scenario | Queue ahead assumed | Interpretation |
|---|---|---|
| **Optimistic** | `0` | Everyone ahead of us left before the trade cleared; we effectively join an empty level. Requires that the visible `Q_same` was consumed by the very flow we saw. Justified **only** when the observed trade size ≥ `Q_same`. |
| **Baseline** | `Q_same` (all currently visible same-side size at that level) | The honest central case: we queue behind everything currently visible. |
| **Conservative** | `Q_same` + `Q_opp` (price crosses: we demand an immediate marketable fill against the opposing side) | The pessimistic case: no passive fill at all; execution only via crossing, paying spread + taker fee. |

Rationale: the truth is between "the level emptied as the trade happened"
and "we never get to the front." The three numbers bracket the follower's
realistic fill probability; the report (docs/09) quotes all three, never a
single point.

**Queue validity rule:** the queue estimate is only computed from a book
state that is decision-ready *and* from the same price level the signal
traded at. If the level is gone (the trade consumed it), the Baseline
assumption degenerates - that case is tagged `LEVEL_CONSUMED` and evaluated
under the Optimistic rule with an explicit flag, never silently.

## 4. Fill-credit rules (what counts as an executed simulated fill)

A simulated passive order at price `P` submitted at wall-time `T` is credited
a fill of size `s` only when the public tape provides direct evidence:

1. **Price-time priority:** no public trade at a price **worse** than `P`
   on our side occurred between `T` and the fill moment (someone behind us
   could not have filled before us).
2. **Size accounting:** cumulative traded size at `P` (plus size removed by
   book updates at `P` while we are at the front) exceeds the queue ahead of
   us under the active scenario; `s` is then the remaining unfilled
   quantity, capped by the visible consuming flow.
3. **Level integrity:** if a book update *deletes* our price level before we
   are filled, the order is **cancelled** (the market moved through us) -
   recorded as `CANCELLED_LEVEL_GONE`, never as a fill.
4. **Staleness kill:** if book state goes stale or a gap is declared
   (`invalidate()` semantics from docs/07 §4), fills freeze until the
   snapshot reconciles; evidence accrued across a gap is discarded.

Marketable (crossing) submissions are credited only against visible
opposing size at their limit, and always pay the aggressor fee + spread -
the Conservative scenario's cost structure.

## 5. Decision vocabulary (both simulators emit the same grammar)

| Decision | Meaning |
|---|---|
| `COPY_PASSIVE` | Join Tokor's price level passively (subject to §4 credit rules). |
| `COPY_MARKETABLE` | Cross the spread to take (pays taker fee + spread). |
| `RESIZE` | Copy but clamp size to the $49 budget / risk cap. |
| `WAIT` | Book not decision-ready, or queue scenario says stand aside; re-evaluate next signal. |
| `REJECT` | Signal fails policy checks (fee-tier economics, price band, category filter). |
| `REDUCE_EXISTING_POSITION` | Tokor's fill would *increase* exposure we are capped on - copy only the reduction. |
| `DATA_INSUFFICIENT` | No usable book/trade evidence; counted, never simulated. |

`submitted_qty` is set by the decision; `executed_qty` only by §4. The
report always prints both, plus the cancellation/freeze counts.

## 6. Intelligent-copy policy (the only place judgment enters)

Exact-copy is the control: same side, same price, same size (budget-clamped).
Intelligent-copy adds three public-data rules, each falsifiable:

1. **Fee-aware price improvement:** on fee-tier markets, resting at
   Tokor's exact price earns zero fee (docs/06 §4.1: makers pay nothing),
   but so do we - while the *aggressor* pays. If Tokor's fill was an
   aggressor fill (`CONFIRMED_TAKER`), the immediate level was consumable:
   copy passively one tick *inside* (join the far side of the spread) when
   the recorded spread supports it, else copy exact.
2. **Category gate:** only markets the classifier marked crypto (fee
   model verified per category; anything else is `REJECT`ed for now).
3. **Exposure cap:** never let the simulated book exceed $49 gross notional;
   overflow → `RESIZE` or `REDUCE_EXISTING_POSITION`.

Net EV per simulated fill = expected markout (§7) − fees − spread cost −
queue-conditional fill-weighted adverse selection. The formula is computed
per scenario; the report shows the EV *envelope*, not a point estimate.

## 7. Markouts (the honesty metric)

For every signal we record the public-trade mid at +1 s, +5 s, +30 s, +5 m
after first sight. Markouts answer two separate questions:

* **Signal quality:** does Tokor's trade predict short-term drift? (This is
  the follower's edge, if any.)
* **Adverse selection tax:** how much does the follower pay for acting
  *late* - i.e. how much of the move happens between exchange time and our
  first-seen time? (Measured directly by §4's `observe-source` chain plus
  markout deltas between those stamps.)

Markouts are computed per scenario-weight so docs/09 can decompose:
`EV = P(fill|scenario) × (markout_edge − costs) − P(miss) × 0 − P(cancel) × slippage_risk`.

## 8. Non-goals and guarantees

* No order placement of any kind; no authentication; no keys. The
  simulator is a pure function over recorded data (deterministic; same
  inputs → same outputs).
* The simulator never "fills itself" from stale or gapped state; every
  credited fill cites its evidence (trade rows / book-update ids).
* Queue scenarios are *assumptions about an unobservable*, and are labeled
  as such everywhere they appear. docs/09 reports the envelope
  (Optimistic / Baseline / Conservative) and the sensitivity of the
  conclusion to that choice.

## 8.1 Implementation map (`crates/shadow-exec`, committed `94371b4`)

| docs/08 section | Module | Notes |
|---|---|---|
| §3 queue scenarios | `queue::estimate_queue` | Returns per-scenario queue + `level_consumed` / `optimistic_justified` flags. |
| §5 decision vocabulary | `decision::{Decision, Submission}` | `submits()` enforces submitted ≠ executed (submitting decision with zero quantity sends nothing). |
| §6 policies | `copy_sim::{exact_copy, intelligent_copy}` | Budget clamping emits `Resize`; cap-reached emits `ReduceExistingPosition`; missing book emits `DataInsufficient`. |
| §4 fill credit | `fill_credit::credit_fills` | Evidence state machine: prints at other prices never fill us; queue clears first; level removal cancels; gaps restore the queue (conservative reading). |
| §6 rule 3 / §1 budget | `portfolio::PortfolioSim` | $49 cap refuses oversized entries (never silently absorbs); fees are USDC-on-top, never in basis; no shorting. |
| §7 markouts | `markout::markouts` | Tape-mid at +1 s / +5 s / +30 s / +5 m with per-window means. |
| benchmarks | `benches/shadow_bench.rs` | Criterion: queue estimate, 1k-print credit replay, copy policies, 1k portfolio fills, 10k-print markouts. |

## 9. Open items

1. Tick-size lattice for price improvement (polymarket CLOB tick = 0.001
   vs 0.01 markets) - confirm per-market from recorded books before
   enabling rule 6.1.
2. Queue model refinement using public-trade *removal* inference
   (visible-size decrements without trades = cancellations ahead of us).
3. Parameter freeze for docs/09: scenario weights, budget cap, category
   gate - frozen before any results are quoted, to keep the report
   out-of-sample honest.
