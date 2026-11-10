# 06 — Maker/Taker Validation: On-Chain Role Verification

**Phase 2 §2 · Status: decoder + role engine verified on a 200-tx sample; full 13.3k-tx corpus run in flight · Read-only public RPC, no keys.**

---

## 1. Question and method

Phase 1.5 established Tokor's fill mix (90.24% maker by count / 59.37% by
volume) from the wallet feed's *own* accounting. §2 asks the independent
question: **does the chain agree?** Every fill we ingested carries a
transaction hash; every one of those transactions emits fill events from a
Polymarket exchange contract. Decoding those events gives, per fill:

* the wallet's role (order owner = liquidity provider vs aggressor),
* the exact amounts and fee the contract charged,
* which exchange generation executed the trade.

Method: `scripts/onchain_role_validation.py` — for each distinct tx in
`source_fills`, `eth_getTransactionReceipt` over keyless public Polygon RPCs
(batched 10/POST, paced, multi-endpoint rotation, verbatim receipts cached to
`data/raw/onchain_receipts/`), decode fill events, classify, and join back to
the feed rows by `(tx, token_id)`.

## 2. Contract generations in the corpus

The wallet's trading history spans **three** exchange contracts:

| Contract | Address | Fill event |
|---|---|---|
| CTF Exchange (v1) | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | `OrderFilled` |
| NegRisk CTF Exchange (v1) | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | `OrderFilled` |
| **Fee-era exchange (v2)** | `0xe111180000d2663c0091e4f400237545b87b996b` | unnamed (topic0 below) |

The v2 exchange is where all fee-tier (7% crypto) markets execute. Its fill
event is **not** the v1 `OrderFilled`: 3 indexed fields (`orderHash`, `maker`,
`taker`) + **7** data words (v1 has 5). Its signature could not be matched to
any candidate name by keccak (open item — resolve from verified source), so
the topic0 is pinned empirically:

```
0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee
```

v2 layout (validated field-by-field against the feed's ground truth — price,
size, cash — on live tx `0xdb01bd6a…`):

```
[0] side of the event maker's order   (0=BUY, 1=SELL)  — 165/165 sample agreement
[1] tokenId
[2] makerAmountFilled                 (1e6 scale)
[3] takerAmountFilled                 (1e6 scale)
[4] fee charged to the EVENT MAKER, in USDC (1e6)
[5..6] zero in all samples so far (recorded raw)
```

## 3. Role semantics (from `Trading.sol`, confirmed on-chain)

Both generations share the convention: **the event's maker slot is the order
owner.** When the operator matches a taker order against resting maker
orders, the aggressor's own order emits with `taker = the exchange itself`,
and each matched maker order emits with `taker = the aggressor's address`.
Therefore:

| On-chain pattern | Meaning | Label |
|---|---|---|
| `wallet == event.taker` | wallet's order was the aggressor | `CONFIRMED_TAKER` |
| `wallet == maker ∧ taker == v2 exchange` | wallet's own aggressor-order event | `CONFIRMED_TAKER` |
| `wallet == maker ∧ taker ≠ exchange` | wallet's order rested and was taken | `CONFIRMED_MAKER` |
| no event touches the wallet | ingest/decoding mismatch | `ROLE_UNKNOWN` |

## 4. Results (200-tx sample; full corpus pending)

```
txs fetched:            200      fill events decoded: 752
wallet fills classed:   165
  CONFIRMED_MAKER:      147   (89.1%)
  CONFIRMED_TAKER:       18   (10.9%)
  ROLE_UNKNOWN:           0
v2 side-flag agreement: 165/165
```

Consistency with Phase 1.5: the feed-derived maker share (90.24% by count)
matches the on-chain share (89.1% in this sample) within sampling noise.

### 4.1 Fee ground truth — the §2 payoff

The v2 event carries the contract's actual fee, charged **in USDC, to the
event maker, on top of the gross amount** (v1 deducted fees from proceeds in
asset units — a real semantic change). Sample findings:

* Every resting-maker event carries **fee = 0** (18/18 own-order maker events
  in the sample with zero fee) — "makers pay nothing" is now on-chain fact,
  not just feed arithmetic.
* The wallet's aggressor events carry exactly the Phase-1.5 model fee:
  `fee = size × rate × p × (1−p)` with rate = 0.07 on crypto markets —
  agreement within 1×10⁻⁵ USDC (the chain truncates at 1e6-integer
  precision; our model rounds half-up at 5 dp — the documented envelope).
* At p > 0.5 (e.g. p=0.9726, fee 9.56898) the repo-main formula
  `rate × min(p,1−p) × size / p` would give ≈ 10.11 — **ruled out**. The
  deployed contract implements the `p(1−p)` shape that docs/02 verified
  empirically across 13,956 rows.

**Conclusion of §1/§2 together:** the fee model is now triple-verified —
feed-side (13,956 rows, 0 band failures), model-side (exact on three live
rows), and now chain-side (event `fee` field). The `cash_implied_price`
separation and fee-band ingestion validation stand on on-chain evidence.

## 5. Accounting semantics confirmed on-chain

* Maker fills: cash == gross (fee never charged) — event-fee = 0.
* Taker fills: cash == gross + fee (USDC, on top) — matches the feed's
  `usdc_size` decomposition exactly.
* The aggressor emits *two* event kinds in one tx: its own order event
  (maker slot, taker = exchange, fee > 0 when fee-tier) and one event per
  matched resting order (taker slot, maker = rester, fee = 0).

## 6. Open items

1. **Full corpus** — the 13.3k-tx walk is running; this doc's §4/§5 numbers
   will be regenerated from `data/derived/liquidity_role_validation.parquet`
   (all-role distributions per market category, maker share by
   count/volume/notional cross-checked on-chain).
2. **v2 event signature** — resolve the event name/types from verified
   contract source; keep the empirically-pinned topic0 as the fallback.
3. **v2 words [5..6]** — zero in all samples; likely operator/surplus fields.
   Record raw; revisit if any sample shows non-zero.
4. **v1 fee units** — v1 fees are denominated in the order's taker asset
   (tokens for BUY orders); the wallet's v1-era fills pre-date fee tiers, so
   expected fee = 0 — verify in the full corpus.
