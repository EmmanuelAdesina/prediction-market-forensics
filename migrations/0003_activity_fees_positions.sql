-- Migration 0003 (Phase 1.5): full activity events, fee reconciliation,
-- positions snapshots.
--
-- activity_events normalizes EVERY activity type (TRADE, MERGE, REDEEM,
-- REWARD, YIELD, TAKER_REBATE, MAKER_REBATE, SPLIT, CONVERSION) with a
-- deterministic identity that disambiguates genuinely identical rows inside
-- one transaction via a canonical occurrence ordinal.

CREATE TABLE IF NOT EXISTS activity_events (
    event_key     TEXT PRIMARY KEY,
    wallet        TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    timestamp     INTEGER NOT NULL,
    condition_id  TEXT NOT NULL DEFAULT '',
    token_id      TEXT NOT NULL DEFAULT '',
    side          TEXT,
    size          TEXT,
    usdc_size     TEXT,
    price         TEXT,
    outcome_index INTEGER,
    outcome       TEXT,
    market_title  TEXT,
    market_slug   TEXT,
    event_slug    TEXT,
    transaction_hash TEXT NOT NULL,
    occurrence    INTEGER NOT NULL DEFAULT 0,
    raw_json      TEXT NOT NULL,
    acquisition_run_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_ts ON activity_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON activity_events (event_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_events_cond ON activity_events (condition_id);
CREATE INDEX IF NOT EXISTS idx_events_tx ON activity_events (transaction_hash);

-- Per-fill fee model derivation (Phase 1.5, docs/02). One row per stored
-- source fill; rewritten wholesale by `derive-fees`.
CREATE TABLE IF NOT EXISTS fee_reconciliation (
    event_key        TEXT PRIMARY KEY,
    fill_face        TEXT,             -- 'maker' | 'taker' | 'n/a'
    verdict          TEXT NOT NULL,    -- VERIFIED_GROSS_NOTIONAL | VERIFIED_FEE_INCLUSIVE
                                      -- | UNKNOWN_FEE_SEMANTICS | UNEXPLAINED
    gross_usdc       TEXT NOT NULL,
    fee_usdc         TEXT,             -- charged fee (taker rows only)
    fee_wouldbe_usdc TEXT,             -- hypothetical taker fee (maker rows)
    fee_rate         TEXT,
    fee_category     TEXT,
    reconstructed_debit TEXT,
    residual_usdc    TEXT,
    derived_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fee_verdict ON fee_reconciliation (verdict);

-- Read-only snapshots of GET /v2/positions (one ingest overwrites wholesale).
CREATE TABLE IF NOT EXISTS positions_snapshot (
    token_id      TEXT PRIMARY KEY,
    condition_id  TEXT NOT NULL,
    status        TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    current_size  TEXT NOT NULL,
    avg_price     TEXT NOT NULL,
    realized_pnl  TEXT,
    percent_pnl   TEXT,
    cur_price     TEXT,
    cur_value     TEXT,
    entry_cost_usdc  TEXT,
    entry_fees_usdc  TEXT,
    total_size    TEXT,
    negative_risk INTEGER NOT NULL DEFAULT 0,
    mergeable     INTEGER,
    redeemable    INTEGER,
    event_slug    TEXT,
    title         TEXT,
    captured_at   TEXT NOT NULL,
    acquisition_run_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_positions_status ON positions_snapshot (status);
CREATE INDEX IF NOT EXISTS idx_positions_cond ON positions_snapshot (condition_id);
