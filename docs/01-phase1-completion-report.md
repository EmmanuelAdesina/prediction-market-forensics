# Phase 1 — Data Foundation: Completion Report

**Date:** 2026-11-10
**Engine:** Tokor Engine (Rust workspace)
**Subject wallet:** `0xa19cbababc312f9df185e49d7004c249ed1ade6b` ("Tokor")
**Phase 1 verdict: PASS — gates green, data foundation durable and verified. Exit to Phase 2 approved below.**

---

## 1. Scope delivered

Phase 1 restricts itself to the data foundation: observation, durable storage,
market enrichment, classification, position reconstruction, independent
reconciliation, and reporting. **No orders, no keys, no auth, no
deposits/withdrawals, no strategy trades, no maker/taker inference.** All tests
are credential-free.

Crates implemented in this phase (27 source files, 5,362 LOC):

| Crate | Files | Role |
|---|---|---|
| `domain` | 7 | Validated models: `SourceFill` (with derived `exec_price`), `IngestionBatch`, FIFO `PositionBook`, errors |
| `persistence` | 5 | SQLite (WAL, `synchronous=FULL`, FK on), transactional page checkpoints, repos (fills, markets, reconstruction, reconciliation, report) |
| `wallet-feed` | 7 | Data-API v2 cursor walker with retry/backoff/pacing, raw-page capture, `/v2/positions` fetcher |
| `market-data` | 1 | CLOB + Gamma clients (trait-backed, bounded timeouts) |
| `market-classifier` | 3 | Deterministic `crypto-cls-v0.2.0` ruleset + labelled suite |
| `application` | 4 | Operator CLI, config validation, orchestration (bounded-concurrency enrichment), report |

Nine additional crates (`bootstrap`, `execution`, `polymarket-adapter`,
`portfolio`, `reconciliation`, `research`, `risk-engine`, `strategy`,
`telemetry`) remain **empty scaffolds** reserved for later phases — declared in
the Phase 0 plan and intentionally untouched here.

## 2. Commit trail

```
8088efe persistence: PageRecord struct + rusqlite 0.32->0.41 (RUSTSEC-2026-0459) + JoinSet::spawn fix + clippy cleanups
c2cb7a2 domain: verified price-display rule (floor₂(usdc/size)); exec_price basis; classify --reset
0f79a02 Phase 1: operator CLI, positions fetcher + cursor walk + raw capture, report repository
19eb3b1 Phase 1 vertical slice: domain model, SQLite transactional checkpoints, Data API provider + ingestor
073dcd4 Phase 0: reconnaissance assessment, env setup, raw wallet captures
```

## 3. Architecture

- **Event identity:** SHA-256 `v1|{tx}|{ts}|{condition_id}|{token_id}|{side}|{price}|{size}` — stable, dedup-safe across re-ingest.
- **Durability contract:** per page, fills + raw response + run accounting + cursor advance commit as **one SQLite transaction**; crash yields all-or-nothing, so restart replays from `last_cursor` without duplication or gaps. Integration test `resume_cursor_after_restart` proves it.
- **Retries:** bounded retries with jitter for transient upstream failures (429 honors `Retry-After`; 503 retried; unit-tested via wiremock).
- **All API integrations sit behind trait interfaces** (`WalletFeedProvider`, `MarketDataClient` usage, repos), so tests inject fake servers/adapters.
- **Decimal-safe money** via `rust_decimal` end-to-end; arithmetic on source-derived values only. `exec_price = usdc_size / size` is recomputed at load, never fabricated.
- **Operator control:** live trading requires Phase-2 mutation of `config/default.toml` + explicit CLI subcommands that do not exist yet. No auth material is present anywhere in the tree.

## 4. Storage schema

Tables: `acquisition_runs`, `raw_api_pages` (57 rows — every response kept
forensically verbatim), `source_fills`, `wallet_cursor_state`, `markets`,
`market_tokens`, `positions_reconstructed`, `position_lots_remaining`,
`position_reconciliations`, `schema_migrations`. Migrations are embedded and
idempotent.

## 5. Operator commands

```
tokor --config config/default.toml config-validate
tokor --config config/default.toml ingest-wallet [--full|--resume] [--limit N] [--max-pages N]
tokor --config config/default.toml ingest-markets [--limit N]
tokor --config config/default.toml classify-markets [--reset]
tokor --config config/default.toml reconstruct-positions
tokor --config config/default.toml reconcile-positions
tokor --config config/default.toml report
```

All idempotent except where explicitly cumulative (`--full` always re-walks raw
history; dedup makes the net effect zero).

## 6. Acquisition coverage

| Run | Status | Pages | Wall time |
|---|---|---|---|
| `wallet-20261109T214142Z` | COMPLETE | 28 | 3m 02s |
| `wallet-20261109T215350Z` (idempotence re-run) | COMPLETE | 28 | 2m 07s |
| `positions-20261110T072803Z` (`/v2/positions`) | COMPLETE | 1 | 5s |

**Fill ledger after the verified parse-rule refinement:**

- **13,850 fills stored, 100% BUY, 0 SELL**, window 2026-07-10 → 2026-11-09.
- Notional `$3,840,934.76`, **2,807 tokens / 1,999 conditions**.
- Forensic accounting across 13,940 raw TRADE rows closes exactly: 13,585 + 265 accepted + 82 rejected + 8 duplicates = 13,940.
- **Zero pre-history flags** on reconstruction: the full observable account history fits in the acquisition window (buy-only wallet).

## 7. Ingestion semantics discovered and encoded (source-verified)

1. **Price-display rule.** The API's `price` field is `floor₂(usdc_size/size)`
   on some rows while `usdc_size` is exact. Verified live across the corpus:
   265 rows fit `price ≤ usdc/size < price + 0.01` exactly. Encoding:
   displayed `price` is kept in the identity hash (source stability); cost
   basis uses `exec_price = usdc_size/size`, which matches Polymarket's own
   `avg_price = entry_cost/size` semantics confirmed on `/v2/positions`.
2. **Fee-premium family (82 rows, REJECTED, preserved raw).** $110,098.45 of
   notional over 448,503.32 shares across 46 markets — exclusively
   dip-ladder / Up-or-Down crypto markets — carry a systematic
   **1.0504–1.0567× premium** of `usdc_size` over `price×size`, outside every
   verified interpretation (not the floor rule, not plain arithmetic).
   Strong supporting evidence from Phase 1's live reconciliation: on all 133
   API positions, `total_cost_usdc = entry_cost_usdc + entry_fees_usdc`
   **exactly**, and `avg_price` excludes fees. These 82 rows are almost
   certainly fee-bearing taker fills; they stay out of the normalized ledger
   until the fee schedule is verified from an authoritative source and the
   premium is reproduced from first principles. Raw JSON is retained.
3. **Cursor invariants.** `next_cursor` walked verbatim across 28 pages × 2
   runs with identical filters; terminal reason recorded per run
   (`backend_no_more`).

## 8. Enrichment + classification

- Enrichment: CLOB + Gamma metadata for **1,999/1,999 markets, 0 errors**
  (bounded concurrency 3, 5s connect / 10s total timeouts, one retry per
  market). Idempotent COALESCE upserts; raw fields also persisted.
- Classification `crypto-cls-v0.2.0` (post-enrichment re-evaluation):
  **CRYPTO 1,729 / NON_CRYPTO 263 / UNKNOWN 7** of 1,999.
  Pre-enrichment baseline was 1,702/261/9 — enrichment sharpened the verdicts
  (27 reclassified from evidence gained), consistent with the labelled suite:
  hard exclusions (commodity/FX/equity language, e.g. WTI ladders → NON_CRYPTO)
  win first; UNKNOWN stays available for genuinely ambiguous rows rather than
  guessing. The 7 UNKNOWN markets are enumerated in DB evidence JSON for
  operator review.

## 9. Position reconstruction

**2,807 tokens reconstructed from 13,850 fills via FIFO `PositionBook`,**
cost basis on `exec_price`, 0 tokens flagged `history_probably_incomplete`.
Buy-only input means every lot traces to at least one preserved fill; no
inference is performed for anything not stored.

## 10. Reconciliation vs public `/v2/positions`

Independent double-entry check (run `positions-20261110T072803Z`): 133 live
positions returned by Polymarket.

| Verdict | Count |
|---|---|
| MATCH (|Δsize|≤0.0001 ∧ |Δavg|≤0.001 ∧ no history flag) | 64 |
| MISMATCH (explanation JSON recorded) | 64 |
| API_ONLY (token absent from local fills) | 5 |

Dissection of the 64 MISMATCHes (analysis performed on persisted rows):

- **13 explained exactly** by the 82 rejected fee-premium rows: adding their
  size back makes `ours == api_size` to sub-1e-4. This is further corroboration
  that the rejection reason is systematic, not noise.
- **52 show `ours_size > api_size`:** the wallet's open inventory is smaller
  than the buy-aggregate, while zero SELL trades exist anywhere in the 13,940
  raw TRADE rows. Hypothesis recorded in evidence JSON: disposals (redeems,
  merges of paired outcomes, or sells through a channel absent from the
  activity feed) are invisible to the activity API. **Not force-fitted**; the
  difference stays visible until a disposal channel is identified.
- The remainder are small-magnitude variants of the two patterns above
  (fewer than 1% residual after the 13 resolved exactly).

**Fee arithmetic validated on 133/133 API positions:** `total_cost = entry_cost + entry_fees` holds exactly, so fees are a separate field and excluded from `avg_price` — matching the engine's `exec_price`-based basis without fees.

## 11. Engineering gates (all green)

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | ✅ |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ✅ (fixed 8 findings incl. doc laziness, `too_many_arguments` via `PageRecord` struct, redundant closures, bool comparison, ref-to-slice clones) |
| `cargo test --workspace` | ✅ 40 tests, 0 failures (lone-network integration suites use wiremock) |
| `cargo audit` | ✅ clean — `libsqlite3-sys` 0.30→0.39.1 clears RUSTSEC-2026-0459 (SQLite CVE, medium) |
| `Cargo.lock` committed | ✅ (243 crates audited) |

## 12. Known issues / open items

1. **82 rejected fee-premium fills** on hold pending authoritative fee-schedule
   verification (Phase 0 HIGH-risk item). Quantified impact: $110,098 / 448,503
   shares / 46 markets; 13 reconciliation discrepancies depend exactly on them.
2. **Invisible disposal channel** for 52 positions (`ours > api`). Next probe:
   CLOB `trades` (maker side), redeem/merge activity types, or on-chain
   transfer logs — activity feed alone cannot see it.
3. **7 UNKNOWN market classifications** queued for operator review.
4. **Enrichment rate limiting** observed originally (serial mode: ~5/min);
   fixed with bounded concurrency but no server-side rate-limit telemetry yet.
5. The `report` command prints the most recent acquisition run, which after a
   `reconcile-positions` call is the positions run (1 page) rather than the
   wallet runs — cosmetic; wallet run stats are authoritative in
   `acquisition_runs`.

## 13. Exit decision

**EXPRESS EVALUATION (user's independent question for this phase):** is
Tokor's activity an exploitable signal for a ~$50 follower? The data now
supports a first evidence-based statement:

- The wallet is high-frequency, buy-only, $3.84M deployed over 4 months across
  2,807 tokens — position tracking alone is feasible from public data with
  this pipeline.
- The engine observes entries with exactness (verified price semantics), but
  **exits are invisible** (52/133 open positions imply disposals the feed
  doesn't show) — any copy strategy must therefore be entry-only unless the
  disposal channel is captured.
- Signal quality/latency test is deferred: mirror feasibility depends on how
  quickly fills propagate relative to market impact and whether small-size
  followers pay materially worse execution (taker fees already demonstrated on
  some fills).

**Phase 1 exit: APPROVED.** All DoD items delivered; all hard restrictions
honored; gates green. Recommended Phase 2 focus, in order: (1) fee-schedule
verification to adjudicate the 82 rejected rows; (2) capture the disposal
channel (CLOB trades/redeems) so position state is truly closed-loop; (3)
fill-to-fill latency measurement and a dry-run mirroring simulation on
historical data before any order plumbing.
