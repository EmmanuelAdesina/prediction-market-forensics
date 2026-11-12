# 04 - Event Identity Audit (Phase 1.5)

**Status: CLOSED - identity schemes audited adversarially; the Phase-1 composite
key is verified safe on this wallet's entire history, and the Phase-1.5 event
identity adds occurrence ordinals for the general case.**
Artifacts: `crates/domain/src/activity.rs` (`event_identity` v1),
`crates/domain/src/activity_events.rs` (`activity_event_identity` v2 + occurrence
ordinals), tests `activity_event_tests.rs`, `fee_tests.rs`, `ingestion.rs`.

## 1. The audit question

Phase 1 deduplicated fills on a composite key. Two adversarial possibilities
could silently corrupt the ledger:

1. **Two genuinely distinct fills sharing the full composite** within one
   transaction (e.g. two maker orders filled at the same price level in the
   same tx) - a key collision would drop a real fill.
2. **Duplicate delivery of the same fill** across walks/pages - must collapse.

## 2. Empirical audit over the full raw corpus

13,956 raw TRADE rows from the unfiltered walk, grouped by the Phase-1
composite `(tx_hash, timestamp, condition_id, token_id, side, price, size)`:

| finding | value |
|---|---|
| composite groups with >1 row | 8 (16 rows involved) |
| groups whose rows are **byte-identical** | **8** |
| groups containing genuinely distinct rows (e.g. different `usdc_size`) | **0** |

Conclusions:

- **No genuine composite collision exists in this wallet's history.** All 8
  multi-row groups are exact duplicate deliveries (pagination/re-walk
  re-yields); collapsing them to 13,948 unique fills loses no information.
- Cross-run consistency: the earlier two-run fee extraction (164 raw → 82
  unique) and the ledger containment check (13,932 stored ⊂ 13,948 unfiltered;
  the 16-row delta being post-snapshot Nov-10 buys, all BUY) both reconcile
  under the same key.

## 3. Residual risk of the Phase-1 key (documented, not ignored)

The v1 composite excludes `usdc_size` and `outcome`. Two hypothetical fills
identical in all seven key fields but differing in cash would collide. This
never occurs in 13,956 observed rows - but "never observed" is not a
guarantee. Phase 1.5 therefore introduces the **v2 event identity** for the
general activity stream:

```
v2 composite = tx_hash | timestamp | type | condition_id | token_id | side
             | size | usdc_size | price | outcome_index | outcome
identity     = SHA-256("ev|" + SHA-256(composite) + "|" + occurrence)
```

with **occurrence ordinals** disambiguating byte-identical rows inside one
transaction: identical rows are ordered canonically by their raw-JSON hash and
assigned 0..n. This is deterministic under any page order, pagination
boundary, or re-walk (adversarially tested, §4).

## 4. Adversarial tests (all passing)

`crates/domain/src/activity_event_tests.rs`:

- `identical_rows_in_one_tx_get_distinct_stable_identities` - two byte-equal
  TRADE rows in one tx receive distinct keys (occurrences 0, 1), and the key
  multiset is **invariant under input reordering** (forward vs reversed).
- `distinct_content_and_distinct_txs_are_distinct_identities` - different
  sizes or different txs never collide; distinct rows keep ordinal 0.
- `merge_rows_require_size_equals_usdc` - type-aware validation refuses to
  normalize a MERGE whose `usdc ≠ size` (force-fit prevention).
- `cash_only_rows_normalize` - rebate rows carry cash only.
- `page_parse_surfaces_invalid_and_preserves_cursor` - invalid rows are
  counted and surfaced, never silently dropped; cursor semantics survive.

Fixture corpus test: 13,948 unique fills all carry distinct `event_key`s in
`source_fills` (PRIMARY KEY enforces it; `derive-fees` processes exactly one
row per key).

## 5. Type-aware validation (force-fit prevention)

`activity_event_from_raw` validates per type - the audit's guarantee that the
normalizer cannot launder inconsistent data into clean tables:

- TRADE: requires size, usdc, price ∈ [0,1], side; strict price-vs-cash
  consistency per the Phase-1 contract (now fee-aware in interpretation).
- MERGE: `size == usdc_size` (pairs × $1) - the verified wire semantics.
- REDEEM: requires share size; `usdc_size` accepted as 0 (losing burn) or
  $1×size (winning payout) - see docs/03 §2.1 for the verified families.
- Cash-only events (REWARD, YIELD, TAKER_REBATE, MAKER_REBATE): require
  `usdc_size ≥ 0`.

## 6. Phase-1 completeness-claim corrections (Objective 4)

Corrections to `docs/01-phase1-completion-report.md` superseded by Phase 1.5
evidence:

1. *"Complete TRADE history acquired"* - **corrected**: the Phase-1 walk used
   top-level pagination metadata that never advanced past a script bug
   (`walk_activity.py`); the fee-bearing `type`-filter also hid 5,475
   non-TRADE events. The unfiltered walk (39 pages, 19,431 rows) is the
   complete record; TRADE uniqueness is 13,948 through 2026-11-10 08:23 UTC.
2. *"82 fills rejected as fee-premium anomalies"* - **corrected**: all 82 are
   verified taker sweeps (docs/02 §4); zero rows remain unexplained.
3. *"`exec_price = usdc_size / size` is the execution price"* - **corrected**:
   for taker fills `usdc_size` embeds the fee; the true execution price is the
   displayed `price` field. The engine's `SourceFill::exec_price` must be read
   as a cash-implied price only.
4. *"Positions endpoint returns no pagination metadata"* - **corrected**: it
   nests `pagination.has_more/next_cursor`; the earlier conclusion was a
   script bug (fixed in `scripts/walk_positions_all.py`).
5. *"Every position reduction is a SELL"* - **corrected**: disposals decompose
   into sells, paired complementary merges, winning redemptions, and
   losing-side burns (docs/03 §4).
