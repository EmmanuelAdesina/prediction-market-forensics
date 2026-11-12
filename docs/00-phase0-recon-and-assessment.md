# Phase 0 - Repository & API Reconnaissance Assessment

Date: 2026-11-09
Target wallet: `0xa19cbababc312f9df185e49d7004c249ed1ade6b` ("Tokor", pseudonym "Taut-Ease-Bloodflow")
Status: **Phase 0 complete. No live orders placed. No credentials requested or used.**

---

## 1. Existing-project assessment

**Finding: the project is greenfield.** There is no prior Tokor/Polymarket repository, dataset, or
existing source code in the working environment (`/mnt/c` and `/mnt/c/Quant` inspected; the only
nearby related project, `/mnt/c/Quant/atlas`, is an unrelated Python pricing engine for a browser-only
venue). System environment findings that shape the plan:

| Finding | Consequence for the plan |
|---|---|
| `/mnt/c` is a WSL-mounted Windows NTFS drive | drvfs is severely slow for `target/` builds. Mitigation: **source lives at `/mnt/c/Quant/tokor-engine`, `CARGO_TARGET_DIR` points to a Linux-side ext4 path** (documented in `.cargo/config.toml`-style setup script). |
| No Rust toolchain installed | Install via rustup, stable (SDK MSRV is 1.88; current stable satisfies it). |
| Windows `/mnt/c` root is not a git repo | The repo will be initialized fresh at `/mnt/c/Quant/tokor-engine`. |

---

## 2. API compatibility report (verified 2026-11-09, live probes)

### 2.1 Rust SDK - VERIFIED, spec matches reality

Repository `Polymarket/rs-clob-client-v2` is public and actively maintained (270 commits,
147 stars, 110 forks, MIT license). Crate: **`polymarket_client_sdk_v2 = "0.6"`**.

Verified capabilities relevant to this build:

- **Feature flags**: `clob`, `ws` (orderbook/price/midpoint + authenticated user order/trade
  streams), `data` (Data API client: positions, trades, activity, leaderboards), `gamma`,
  `rtds` (real-time crypto prices - Binance/Chainlink), `ctf` (split/merge/redeem), `bridge`,
  `rfq`, `heartbeats` (auto-cancel all orders on disconnect), `tracing`.
- **Signer abstraction**: `alloy::signers::Signer` trait - supports local signers *and* remote
  signers (AWS KMS) directly, without a custom local signer implementation. Re-exports
  `secrecy::SecretString`. This satisfies the security requirements without reimplementation.
- **Auth flow**: type-level state machine prevents calling authenticated endpoints before
  `authenticate()`. Dual flows: normal and Builder-attributed.
- **Signature types**: EOA (0), Magic/email proxy (1), GnosisSafe browser-proxy (2),
  EIP-1271 (3, V2/V3 orders), Poly1271 (deposit wallets). **Funder address (the address
  holding funds) is distinct from the signing key address** - CREATE2-derived or passed
  explicitly. This distinction is critical: our follower wallet will be a proxy-style wallet,
  and correct `signature_type`/`funder` configuration is a Phase 4 hard requirement.
- **Protocols**: V1 (legacy, `clob.polymarket.com`), V2 and V3 (both `clob-v2.polymarket.com`).
  Protocol auto-detected per validation via `GET /version`; V2/V3 use `timestamp`/`metadata`/
  `builder` fields, V1 uses `taker`/`nonce`/`feeRateBps`. Order builder exposes both.
- **MSRV: 1.88**. Committed `Cargo.lock` in the SDK; MIT license fine for vendoring.
- **Geoblock detection** is built into the CLOB client - supports the compliance requirement
  directly at runtime.

### 2.2 Data API v2 - VERIFIED with corrections

**The spec's URL `https://data-api.polymarket.com/v2/docs` is invalid** (HTTP 410/404 territory).
Canonical documentation lives at `docs.polymarket.com/api-reference/data-api/`. The deprecated
`/activity` (v1) path now returns:

```json
{"error":"Data API v1 has been retired","migration":"https://data-api.polymarket.com/v2/","docs":"https://docs.polymarket.com/api-reference/data-api/overview"}
```

Verified live behavior of `GET /v2/activity?user=0xa19c...&limit=3&type=TRADE` (HTTP 200):

- Returns `data[]` of normalized wallet fills with fields:
  `proxy_wallet, timestamp (unix), condition_id, type, size, usdc_size, transaction_hash,
  price, token_id, side, outcome_index, title, slug, icon, event_slug, outcome, name, pseudonym`.
- Pagination: `pagination: {limit, offset, has_more, next_cursor}` with an **opaque signed
  cursor** (base64 payload containing ts + sequence + sig). This is genuine
  cursor-based pagination - our feed must follow `next_cursor` until `has_more=false`
  and must resend identical filters.
- **Note**: response also includes `offset` - do not use it; cursor is authoritative.

Verified live `GET /v2/positions?user=...` (HTTP 200): full position accounting per token:
`current_size, avg_price, entry_cost_usdc, entry_fees_usdc, total_cost_usdc, current_price,
current_value, total_size, realized_pnl, unrealized_pnl, total_pnl, percent_pnl, status,
redeemable, mergeable, negative_risk, event_id, event_slug, end_date, first_entry_at,
last_event_at, opposite_token_id`. This is the primary source for **independent
position-reconstruction validation** of Tokor (and later, the follower's own account).

### 2.3 CLOB REST market metadata - VERIFIED

`GET https://clob.polymarket.com/markets/{condition_id}` (HTTP 200) returns, among
elsewhere: `question, minimum_order_size, minimum_tick_size, neg_risk, active, closed,
maker_base_fee, taker_base_fee`. Verified example (BTC dip $73k Nov-9 market):

- `minimum_order_size: 5` (shares)
- `minimum_tick_size: 0.01`
- `maker_base_fee / taker_base_fee: 1000` (units unconfirmed - see §9 assumptions;
  must be pinned against CLOB fee schedule before EV math is trusted)

Public book: `GET /book?token_id=` (HTTP 200) returns `bids/asks` with string prices and
sizes, `timestamp`, `hash`. Depth observed for one threshold market execs from $0.01–$0.99
side-by-side - these barrier markets have **wide effective spreads** around the mid and deep
fragility; follower fills need actual depth-aware execution modelling, not mid-price assumptions.

### 2.4 WebSocket - to be verified in Phase 2

SDK advertises `subscribe_orderbook/prices/midpoints` (public) and `subscribe_orders/trades`
(authenticated). Spec compliance will be confirmed against SDK source during Phase 2.
Key architectural point already confirmed from docs: the CLOB WebSocket is **not** a
public feed of other wallets' activity - wallet activity comes from the Data API only.
The plan therefore uses a poll/SSO cursor-walk against `/v2/activity` as the sole source of
Tokor events (with an internal `WalletFeedProvider` trait so a validated event-source
can be swapped in later).

---

## 3. Tokor wallet initial reconstruction (first 1000 trades, one page-walk, data on disk)

Sample: 10 consecutive pages × 100 (limit), via cursor walk. Coverage: **2026-11-02 12:09 →
2026-11-09 18:39 UTC** (7 days - the API returned this history on a single 1000-record walk at
limit=100; longer history requires a cursor walk with limit=500 and multiple passes, pending rate-limit constraints).

### 3.1 Activity rhythm

- **140 trades/day** (~6/hour) in the sampled week.
- **100% BUY in this 7-day window.** No sells detected in this sample. This is either
  (a) accumulation phase, or (b) an artifact of recent-window bias - longer history
  is required before claiming "Tokor never sells". The positions endpoint shows realized
  P&L on some positions (e.g., -$1,096 realized on BTC-dip-70k), which proves sells have
  occurred in past epochs - therefore the BUY-heavy week is state-dependent, not permanent.

### 3.2 Size and exposure

- Median trade: **$91.42**; max: **$29,068**; min: $0.01 (real metadata noise; treat
  sub-$1 fills as artifacts of `usdc_size` rounding).
- Weekly sampled notional: **$436,815 USDC deployed to BUY** across **252 distinct markets**.

### 3.3 Market concentration (by notional)

| Market event | USD volume |
|---|---|
| what-price-will-bitcoin-hit-before-2027 | $73,782 |
| what-price-will-ethereum-hit-on-november-9-2026 | $59,114 |
| what-price-will-ethereum-hit-november-2-8-2026 | $57,639 |
| what-price-will-wti-hit-in-november-2026 | $45,734 |
| what-price-will-ethereum-hit-in-november-2026 | $45,191 |
| what-price-will-bitcoin-hit-in-november-2026 | $39,291 |

Top-6 markets = ~70% of notional in the sampled week.

### 3.4 Underlying-asset slug heuristic (rough, classifier to be replaced)

`ethereum 463%` / `bitcoin 191` / `(other) 232` - note **WTI (oil)** appears prominently
(`what-price-will-wti-hit-in-november-2026`, $45,734). The crypto-scope classifier
must therefore teach itself to *exclude* WTI-style non-crypto events; a naive
"price-threshold" keyword match overmatches. Title alone is insufficient - event
slugs and Gamma metadata (icons/tags: `BTC fullsize.png`, `ETH fullsize.jpg` are
already exposed as evidence sources) plus a curated link must be combined.

### 3.5 Open position snapshot (v2 /positions, public)

Sampled top positions show the classic **threshold-accumulation swing pattern**:

- Will ETH dip to $2,000 by Dec 31, 2026: 228,429 shares @ avg $0.2613,
  entry cost $59,694 (+$1,467 entry fees), current value $155,332, **$95,638 unrealized, +160%**.
- Will BTC dip to $70,000 by Dec 31, 2026: 130,989 shares @ avg $0.4936 (+$66,645 total cost),
  **-$1,096 realized, $17,204 unrealized (+26.6%)**.

Both positions are still **OPEN** - Tokor is currently a long-conviction dip-buyer.
This is consistent with the BUY-only week. Fragmented fills across dozens of sub-markets
(same event, multiple strike levels and near-term checkpoints) evidence an **exposure-group**
construction, exactly the risk profile the correlated-exposure model must collapse
(e.g., "ETH dips before 2027" is one risk spanning 10+ strike markets).

### 3.6 Price distribution

Tokor buys across the probability spectrum: price observed 0.001 → 0.998, median 0.560.
Notable: the sampled recent dip-threshold trades execute as low as $0.09 - **deep out-of-the-money
barrier buys** where fees and spread can materially distort EV.

---

## 4. Data model proposal (canonical types, normalized)

Persist these as the canonical database entities (SQLite, WAL) - one row per intent, append-mostly,
integer primary keys with stable upstream hashes for idempotency:

```
SourceFill       : condition_id, token_id, side, size, usdc_size, price,
                   tx_hash, timestamp, outcome_index, raw_json

MarketSet        : condition_id, tick_size, min_order_size, neg_risk,
                   outcome_tokens[], currency, active, closed, event_id,
                   event_slug, title, icon, tags, gamma_json, refreshed_at

BookState        : token_id, snapshot_id, bids[], asks[], ts, hash (latest)
BookSnapshot     : per-token JSONL raw for forensics

Position         : token_id, condition_id, size_shares, avg_price,
                   entry_cost, entry_fees, realized_pnl,
                   first_entry_at, last_event_at, status

OrderIntent      : intent_id (uuid), ts_created, source_fill_ref,
                   decision_id, side, token_id, price, size, otype,
                   state, submitted_at, response_ref, fail_reason

OrderResponse    : intent_id, raw_json, response_ts

Fill             : intent_id, trade_id, ts, price, size, fee, raw_json

Decision         : decision_id, source_fill_ref, strategy_mode (exact|intel),
                   action, features_used, model_version, ev_net,
                   ev_gross, estimated_slippage, estimated_fees,
                   confidence, risk_approval, rejection_reason,
                   feature_snapshot_id, ts

ExposureGroup    : group_id, members[] (condition_ids), underlying,
                   verdict_code, rationale

EventDedupIndex  : composite_key (condition_id + tx_hash + row index), seen_at
```

Where the spec's suggested trades-vs-fills-vs-orders distinction maps:
**Data API `TRADE` records are FILLS, not orders** - the v2 activity view has no
order lifecycle info (no maker/taker flags, no order_id, no cancel/replace traces).
Each Tokor fill is the *consequence* of a resting or immediate order. Therefore:

- Tokor's **intent model** cannot be reconstructed directly. We can reconstruct
  **fill streams** and **position aggregates** only. Any "Tokor strategy inference"
  must treat this explicitly as a limitation, never silently.
- Maker/taker must be inferred (if at all) only through fee-signature analysis
  once CLOB fee terms are verified, or via (for the follower only) the authenticated
  user WS.

---

## 5. Architecture (v0 diagram)

```text
┌────────────────────────── tokor-engine (single Rust runtime) ──────────────────────────┐
│                                                                                      │
│  ┌── Wallet Feed (Data API v2, cursor) ─┐   ┌── Market Data (CLOB) ─┐                │
│  │  /v2/activity cursor walk            │   │  SDK ws::Client       │                │
│  │  normalization, dedup, gaps          │   │  orderbook + prices   │                │
│  └──────────────┬───────────────────────┘   │  authenticated user   │                │
│                 │ downsert to SQLite/JSONL  │  + book snapshot      │                │
│                 ▼                           └──────────┬────────────┘                │
│  ┌── Market Classifier ───────────────────────────────  │                             │
│  │  event → is-crypto? (gamma tags, slugs, icons)  ────┤                             │
│  │  exposure-grouping (underlying, strike, horizons)   │                             │
│  └──────────────┬──────────────────────────────────────┘                             │
│                 ▼                                                                    │
│  ┌── Strategy ────────────┐    ┌── Risk Engine ──────┐   ┌── Portfolio ────────┐      │
│  │ Exact-copy baseline    │    │ veto authority      │   │ inventory, costs,  │      │
│  │ Intelligent-copy       │───▶│ capital, corr, staleness,│▶ P&L from v2     │      │
│  │ (decision + EV)        │    │ staleness, killswitch   │ positions + ledger │     │
│  └────────────────────────┘    └──────────┬──────────┘   └─────────────────────┘     │
│                                           ▼                                          │
│  ┌── Execution ────────────────────────────────────────────────┐                     │
│  │ SDK clob client, order state machine, durable outbox,       │                     │
│  │ post/patch/cancel, partial fills, ambiguous-submission probe│                     │
│  └──────────────────────────────┬───────────────────────────────┘                     │
│                                 ▼                                                      │
│  ┌── Persistence (SQLite WAL) ────────────────────────────────────────┐                 │
│  │ source_fills, decisions, intents, responses, fills, positions,     │                 │
│  │ book_snapshots, exposure_groups, cursors, telemetry                │                 │
│  └───────────────────────────────────────────────────────────────────┘                 │
│                                                                                      │
│  ┌── Telemetry (tracing + prometheus) ──┐   ┌── CLI (application) ─────────────┐      │
│  │ lifecycle latencies, freshness, P&L  │   │ status / research / shadow / ... │      │
│  └──────────────────────────────────────┘   └──────────────────────────────────┘      │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

**Crate consolidation vs the spec's template** (spec permits justified simplification):
13 crates → **11**, by folding:

- `market-classifier` → `market-data` (classification is a pure function over market metadata
  - colocating avoids a network-shaped crate boundary for a CPU-only step).
- `telemetry` → `application` (a facade over `tracing` + prometheus; not a domain unit).
- `domain` stays, `polymarket-adapter` stays, `wallet-feed` stays, `portfolio` stays,
  `strategy` stays, `risk-engine` stays, `execution` stays, `reconciliation` stays,
  `persistence` stays, `research` stays. Binaries/CLI live in `application`.

**Explicitly rejected**: Kafka/Redis/K8s/microservices; LLM in the hot path; unsafe Rust;
custom crypto. All unnecessary for a single-process system on a ~$50 budget.

---

## 6. Threat model (v0)

Assets at risk: follower wallet USDC (< $50), outcome-token inventory, private key/API
credentials, decision history.

| Threat | Vector | Mitigation (phase) |
|---|---|---|
| Credential compromise | private key in env/dotfiles/git | Secrets only via env vars (`POLYMARKET_PRIVATE_KEY` as the SDK expects); `.gitignore` excludes `*.env`; redacted tracing; `secrecy` crate types from SDK (P0+P1) |
| Malicious/keylogger terminal | copy-trading without upper bound | Hard capital ceiling in risk engine; kill switch; no auto-scale (P3+) |
| Vendor trust / supply chain | malicious SDK transitive dep | `cargo audit`, `cargo deny`, committed `Cargo.lock`, vendor pinning; never edit SDK (P0+) |
| Geo/regulatory non-compliance | trading where prohibited | SDK built-in geoblock detection held as precondition for live-arming; **no VPN (out of scope by design)** (P5/P6) |
| Ambiguous order state | duplicate submission after timeout | transactional outbox; only-submit-once with explicit state advance; query-before-retry (P4) |
| Overexposure through correlated markets | BTC/ETH multiple-strike ladder | Exposure-group model collapsing via `event_id`, `underlying`, strike-range, and direction (P3) |
| Stale data | empty/hung ws / data api | freshness budget (`max_order_book_age_ms`), risk vetoes on staleness (P2/P3) |
| Crash mid-execution | outbox intent persisted, submit not done | restart replays outbox in defined order, with reconcile-before-resubmit rule (P10) |

---

## 7. Risk register (SEQ → severity / likelihood → owner phase → mitigation)

1. **Fee model unverified (maker/taker `base_fee` = 1000 bps)** - HIGH severity, HIGH
   likelihood. EV math, exact-copy baseline, and slippage math all depend on it. Must be
   pinned with a defensive fee schedule verified against Polymarket's current fee policy
   + CLOB contract reads before any net-EV claim. Owner: Phase 1.
2. **Crypto classifier accuracy** - MEDIUM. WTI and price-threshold ladders overlap.
   Sample manual labelled set (50 markets) then implement hybrid rules on
   event_slug+tags+icon+question text. Owner: Phase 1 (once gamma client is up).
3. **Short history from cursor walk at limit=100 (only 7 days in one call)** - collapse by
   using limit=500 and multi-page walks; may require batching over multiple API rounds to
   build real history. Owner: Phase 1.
4. **Wallet-fill semantics ambiguity (maker vs taker)** - cannot be resolved from v2 activity
   alone. Mark as data limitation; do not make confident "Tokor is a maker/taker" claims
   until fee-signature or WS-side evidence exists.
5. **Tokor may be running multiple coordinated wallets** - beyond scope; treat observed
   wallet as one signal source.
6. **Minimum order floor ($5 shares × price)** - with $2/order cap may block otherwise valid
   fills. Decision: if order cap can't exceed min notional, REJECT with reason (config-driven);
   never silently exceed. Confirm with operator (config to be tuned in Phase 5).
7. **Extremely reactive `type=TRADE` timestamp** - timestamps have second precision; real
   source-to-detection latency will be dominated by Data API freshness, not by our pipeline.
   Must document this (see Spec 11).
8. **Sparse authentication surface for our own funds** - funder/signature_type validation must
   be verified with a dry-run auth flow in Phase 4 before ever holding balances.

---

## 8. Implementation plan (to match spec Phases 0–7, adapted to findings)

- **Phase 0 (this doc)** - env set up (WSL location chosen, rustup installed in background),
  raw wallet sample stored at `data/raw/page_*.json`, SDK/API verified, plan + risk register
  delivered. **No code yet - assessment is the deliverable.**
- **Phase 1 - Data foundation**: Cargo workspace; `domain` + `persistence` (SQLite WAL +
  migrations); `wallet-feed` (`WalletFeedProvider` trait + `DataApiV2Provider` with cursor
  walk, dedup, Retry-After rate-limit handling, JSONL forensic log); `gamma`-powered market
  metadata cache; first-pass crypto classifier; independent position reconciliation against
  `/v2/positions` to validate our reconstruction vs source-of-truth. Exit gate: `cargo test`
  green on unit tests for cursor walk, dedup, and classifier.
- **Phase 2 - Market data**: `clob` SDK client (public), ws orderbook + prices streams,
  book state machine with gap detection, freshness ledger, bench of parse/to-BookState.
- **Phase 3 - Research & strategy**: reconstruct full Tokor history; lifecycle reconstruction;
  exact-copy simulator (with recorded `DataApiV2Provider` replay); intelligent-copy decision
  engine with early conservative rules; exposure-group model; independent risk-engine veto gates;
  replay harness using recorded fixtures. **No live orders in this phase.**
- **Phase 4 - Execution**: authenticated SDK client behind a `Signer` abstraction;
  funder/signature-type validation; order state machine; durable outbox (SQLite + WAL);
  post/patch/cancel via SDK; ambiguous-state probe (query open orders by token+nonce
  or builder trades); partial fill; replay-based fault injection tests.
- **Phase 5 - Shadow**: live wallet feed + market data + both strategies, no submission.
  Measure actual delays, slippage vs book, classify rejected trades, record runtimes.
- **Phase 6 - Live** (operator-gated): only after every Phase-5 gate is green and the
  explicit `tokor live-arm` preflight flow (geoblock check, capital sanity, signer validation,
  risk config snapshot) is complete.
- **Phase 7 - Continuous evaluation**: daily P&L vs exact-copy vs intelligent-copy,
  decision-report, feedback loop into research crates.

---

## 9. Assumptions requiring verification (explicit register)

1. `minimum_order_size` units are **shares** (verified: BTC dip market showed 5 with price 0.09 →
   min $0.45 notional). Confirm same across market types (neg-risk, less-liquid markets).
2. **Fee schedule** - `maker_base_fee` / `taker_base_fee = 1000` units observed. Units
   unconfirmed (bps vs 1e6 scl?). Must read CLOB fee schedule docs + verify empirically in
   Phase 4 with the smallest possible paper order before any net-P&L math.
3. **`OrderIntent` atomicity guarantee** - durability of SQLite vs WAL flush on abrupt kill:
   use `PRAGMA journal_mode=WAL` + `synchronous=FULL` for execute paths; measure WAL fsync
   latency in Phase 1 bench.
4. **Cursor semantics** - the cursor payload embeds `ts` and `l` (limit) and `sq` (sequence).
   Confirm whether re-sending modified `limit` invalidates the cursor (spec: keep identical
   filters) - this constrains our batching strategy.
5. **Whether CLOB `GET /version` protocol detection can change under us** - cache-detected
   protocol is "lifetime of Client" in the SDK; confirm a stale V1 negotiation could ever
   flip mid-session and test the failure mode.
6. **P&L semantics of the positions endpoint for partially-sold positions** - `realized_pnl`
   present on open positions (observed -$1,096 with `current_size` < `total_size`);
   validate our normalization handles un-realized splits from open runs.
7. **Geographic eligibility** - must be confirmed against current official Polymarket
   platform checks at `tokor live-arm` preflight time, not today.

---

## 10. Data limitations (recorded per spec §3C)

- **No historical order books** for Tokor markets - must be explicit that we *cannot*
  compute historical markouts without fabricating books. Any "what could a follower have
  paid" backtest must state "we did not know the book at that time" - never silently assume
  a fill at the recorded price.
- **Execution timestamps have 1-second resolution** in the wallet API; real micro-microstructure
  is invisible. Markouts must be computed against our *own* book feed only from Phase 2
  onward, recorded by us, not fabricated backwards.
- **No maker/taker flag** in v2 activity. Inference (fee-signature or WS-side) is unproven.
- **Positions endpoint is the truth for aggregates**, but not for the *order-of-accumulation*
  within an event. Reconstruction of "which strike was bought first" is only inferable from
  fills' timestamps, not from a v2 position row.
- **Historical P&L is computed by Polymarket's own accounting**, not re-derived here. Validation
  methodology must spot-check against our own reconstruction to catch drift.

---

## 11. Phase-0 exit status

**GREEN** - proceed to Phase 1 (Data foundation) with the caveats documented above.

Deliverables completed in this phase:
- Environment decision + rustup setup (background).
- SDK compatibility report (§2.1) - `polymarket_client_sdk_v2` confirmed and pinned to 0.6.
- Data API v2 verified (§2.2), including the spec-URL correction and raw-page storage at
  `data/raw/`.
- 1,000-trade wallet sample profiled (§3) - BUY-only week, threshold-accumulation ladders,
  WTI leak into naive classifier identified.
- Canonical data model proposal (§4), initial architecture diagram (§5), threat model (§6),
  risk register (§7), phased plan (§8), assumption register (§9), data limitations (§10).

Not done (deliberate, per spec §18.6 - small reviewable increments):
- No Rust code written yet. No executors purchased or live orders placed. No credentials
  requested. Phase 1 will begin with `domain` + `persistence`, reviewed in isolation.
