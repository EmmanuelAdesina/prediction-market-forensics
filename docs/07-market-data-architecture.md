# 07 — Market-Data Architecture: Recorder, Book Engine & Latency Model

**Phase 2 §3–§4 · Status: recorder foundation implemented (`crates/recorder`), live capture pending · Strictly public data — no keys, no authentication, no orders.**

---

## 1. Purpose and threat model

The shadow-execution question — *can Tokor's fills be economically replicated by a
follower with <$50?* — is decided by book microstructure at the moment of each
source fill. The recorder exists to produce that evidence. Two design rules
dominate everything else:

1. **The raw layer is evidence.** Every frame is persisted verbatim, stamped on
   receipt, never rewritten, and never pruned by anything except an explicit
   age policy (`raw_market_messages` only). Normalized states can always be
   recomputed from raw; raw cannot be regenerated from normalized.
2. **A book that cannot be proven consistent is never used for a decision.**
   `is_decision_ready()` requires state ∧ ¬gap_pending ∧ ¬stale. Consumers of
   book state (the §5–§9 simulators) must gate on it; the recorder's job is to
   make the flag trustworthy, not to make decisions.

---

## 2. Components

```
crates/recorder
├── messages.rs   wire types + tolerant parsing (CLOB market channel)
├── book.rs       per-token snapshot+delta state machine
├── session.rs    WS lifecycle, supervision, persistence workers
└── book_tests.rs engine invariants (§11 mandatory tests)
```

### 2.1 Wire layer (`messages.rs`)

Parses the four documented market-channel event types — `book`,
`price_change`, `tick_size_change`, `last_trade_price` — plus an `Other`
variant that **keeps the verbatim JSON** for anything unrecognized. Unparseable
frames are recorded as raw text and never normalized: the raw layer must be a
complete record even when the protocol evolves. The subscription body uses the
protocol's exact field spelling `assets_ids` (regression-tested).

REST `/book` bodies carry the same shape without the `event_type`
discriminator; `parse_book_body` injects it so one parser serves both paths.

### 2.2 Book engine (`book.rs`)

Per-token state machine over `BTreeMap<Decimal, Decimal>` levels — no floats
anywhere in book state. Invariants, each regression-tested:

| Invariant | Enforcement |
|---|---|
| A valid book is never built from deltas alone | delta without base state → `NeedsSnapshot(NoBaseState)` |
| Deltas are **REPLACE** semantics | a level's `size` is the new aggregate, never summed (protocol behavior; test pins it) |
| Levels stay best-first | bids sorted descending, asks ascending, after every apply |
| Size 0 removes the level | both snapshot and delta paths |
| Staleness is a flag, not a deletion | `is_stale(now)` compares `updated_ns` to the configured bound |
| Suspect state is dropped, not patched | `invalidate()` clears the state and sets `gap_pending`; the next delta hits `NoBaseState` (cannot extend a suspect book) and the next snapshot is labeled `RECONCILED_AFTER_GAP` |

**Gap semantics — a correction worth recording.** The original design treated
`price_change.hash` mismatch against the tracked hash as a gap signal. That is
wrong on this feed: the exchange publishes the **post-update** hash, so
consecutive events *never* share hashes. Hash continuity was removed as a
detection rule and replaced by:

* **Exchange-timestamp regression** — a delta stamped before the last applied
  event proves missed/reordered messages → `NeedsSnapshot(TimestampRegression)`.
  Timestamps are normalized tolerating both epoch-seconds and epoch-millis
  (the feed has used both).
* **Reconnect invalidation** — every disconnect invalidates all retained
  states; the fresh REST snapshot after reconnect is labeled
  `RECONCILED_AFTER_GAP`.

Hashes are still recorded on every delta and state — they enable *offline*
reconciliation against REST snapshots (a future §3 extension), they just cannot
detect gaps online.

### 2.3 Session lifecycle (`session.rs`)

Four task roles, wired with bounded channels (explicit backpressure, never
silent drops):

* **Supervisor** — owns the command channel (`Subscribe` / `Unsubscribe` /
  `Shutdown`). The subscription ledger is the DB table `market_subscriptions`;
  a command updates the ledger and bumps a generation counter, and the
  connector closes + re-subscribes with the new set. Dynamic subscription
  changes therefore take effect at reconnect — deliberately simple and
  correct, rather than protocol-clever.
* **Connector** — one live WS session at a time. Connect → subscribe →
  *REST snapshot refresh for every subscribed token* → read loop. On any
  exit: `on_disconnected()` (invalidates all engines), then exponential
  backoff with full jitter (capped, AWS-style) and reconnect. Gap detections
  and fresh staleness flags trigger an **immediate REST rescue** for that
  token instead of waiting for the periodic sweep.
* **Raw writer** — drains a bounded queue (`raw_queue_depth`, default 8 192),
  batches up to 256 rows into one transaction.
* **State writer** — same pattern (depth 2 048, batches of 128) for
  deltas/states/trades/health.

Every write path in `persistence::market_data` is a single transaction per
batch (per-row transactions on this filesystem are measurably too slow for a
bursting WS session). Retention pruning touches only `raw_market_messages`.

### 2.4 Persistence schema (migration 0004)

| Table | Content | Contract |
|---|---|---|
| `raw_market_messages` | verbatim WS frames + REST bodies, wall-clock ns stamps | evidence layer; age-pruned only |
| `order_book_snapshots` | REST book pulls | provenance for every rebuild |
| `order_book_deltas` | verbatim `price_change` payloads | replay input |
| `normalized_book_states` | verified states only, quality-labeled (`SNAPSHOT` / `DELTA_CONSISTENT` / `RECONCILED_AFTER_GAP`) | a suspect state is *never* normalized |
| `public_trade_events` | observable matched volume | the queue model's consumption signal (§5) |
| `market_data_health` | connect/disconnect/gap/stale/refresh ledger | reconciliation-rate denominators |
| `market_subscriptions` | dynamic subscription ledger | source of truth for the connector |
| `source_observations` | §4 latency stamps | one row per polled event |
| `execution_simulations` | §8 decisions + evidence-backed fills | submitted ≠ executed |

---

## 3. Latency model (§4)

**Principle: publication latency cannot be inferred from wallet-feed data.**
The wallet feed's timestamps have 1-second resolution and describe on-chain
events; they bound nothing about when *we* could have known. All latency
claims come from independent stamps taken by our own code.

Five independent stamps per observation (`source_observations`):

```
poll_start_ns ──→ http_response_ns ──→ first_seen_ns ──→ normalized_ns ──→ decision_ready_ns
 (poll begin)     (bytes received)     (row identified)   (parsed+validated) (book gate passed)
```

* All `*_ns` fields are **wall-clock UTC nanos** (a single comparable
  currency); elapsed math that must be monotonic pairs stamps with an
  `Instant` captured at the same site. Wall clock is never subtracted from
  monotonic or vice versa.
* The wallet-feed ingestor already emits per-page `poll_start`/
  `http_response` timing (`activity page fetched` log line); the
  `source_observations` table generalizes it to per-row latency chains.
* Reporting format: p50 / p95 / p99 **per stage and end-to-end**, by run.
  Distributions, never means — the tail is what the replication question
  turns on.
* Book-side freshness is stamped symmetrically: `received_ns` on every frame,
  `updated_ns` on every state; the *decision-time book age* is reported
  alongside latency so stale-book influence on any simulation is visible.

Exchange-time vs receive-time skew (e.g., `book.timestamp` vs our
`received_ns`) is recorded but never used to claim absolute publication
latency — clock alignment with the exchange is not assumed.

---

## 4. What the recorder does NOT do

* No private keys, no authentication, no signed orders, no submissions (§12).
* No order placement or cancellation messages — the user channel is not
  connected at all.
* No backfilling of history: the recorder starts when it starts. Historical
  microstructure questions route through §2's on-chain corpus instead.
* No publication-latency claims from wallet-feed timestamps (§3).

---

## 5. Acceptance tests (§11 mapping)

* Delta-before-snapshot never fabricates a book (`delta_without_snapshot_needs_snapshot`).
* Replace semantics + ordering (`level_ordering_and_replacement`).
* Gap detection blocks decisions until a labeled refresh
  (`timestamp_gap_blocks_decisions_until_snapshot`).
* Reconnect invalidation (`reconnect_invalidation_requires_fresh_snapshot`,
  `disconnect_invalidates_states`).
* Stale flagging is transition-only and triggers rescue
  (`staleness_sweep_flags_once_and_rescues`).
* Raw-before-normalize for every frame (`frames_record_raw_then_normalize`).
* Tolerant parsing keeps unknown shapes (`wire_frames_parse_tolerantly`).
* Backoff bounds + jitter (`backoff_is_bounded_and_jittered`).

---

## 6. Open items

1. **CLI wiring** — `record-market` / `observe-source` commands in
   `crates/application` (session module API is ready).
2. **REST-hash reconciliation** — offline job comparing delta-chain hashes to
   periodic REST snapshot hashes; quantifies silent miss rate (feeds §6 of
   docs/09).
3. **Live capture sessions** — target the BTC/ETH dip-ladder families the
   wallet trades; ≥ 48h coverage before §8 simulations are run.
4. **Latency harness** — wire `source_observations` into the ingestor run
   loop (stamps exist as logging today; table write pending).
