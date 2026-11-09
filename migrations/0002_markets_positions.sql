-- Migration 0002: market metadata + classification, position reconstruction,
-- and reconciliation results.

CREATE TABLE IF NOT EXISTS markets (
    condition_id  TEXT PRIMARY KEY,
    title         TEXT,
    market_slug   TEXT,
    event_slug    TEXT,
    icon          TEXT,
    tags_json     TEXT,
    description   TEXT,
    active        INTEGER,
    closed        INTEGER,
    minimum_tick_size TEXT,
    minimum_order_size TEXT,
    neg_risk      INTEGER,
    -- classification (nullable until classified)
    class_verdict TEXT CHECK (class_verdict IN ('CRYPTO','NON_CRYPTO','UNKNOWN')),
    class_evidence_json TEXT,
    class_version TEXT,
    classified_at TEXT,
    -- provenance: which fetches populated this record
    clob_meta_json  TEXT,                      -- raw CLOB /markets payload
    gamma_meta_json TEXT,                      -- raw Gamma payload
    enriched_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_markets_event_slug ON markets (event_slug);

CREATE TABLE IF NOT EXISTS market_tokens (
    token_id     TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL REFERENCES markets(condition_id),
    outcome      TEXT,
    outcome_index INTEGER
);

CREATE INDEX IF NOT EXISTS idx_market_tokens_cond ON market_tokens (condition_id);

-- Reconstructed positions (per token), rebuilt from source fills.
CREATE TABLE IF NOT EXISTS positions_reconstructed (
    token_id        TEXT PRIMARY KEY,
    condition_id    TEXT NOT NULL,
    acquired_shares TEXT NOT NULL,             -- Decimal as string
    disposed_shares TEXT NOT NULL,
    unattributed_disposals TEXT NOT NULL,
    remaining_inventory TEXT NOT NULL,
    avg_open_price  TEXT,                      -- only over remaining lots
    remaining_cost_basis TEXT NOT NULL,
    realized_pnl    TEXT NOT NULL,             -- FIFO against known lots only
    fills_applied   INTEGER NOT NULL,
    history_probably_incomplete INTEGER NOT NULL,
    computed_at     TEXT NOT NULL
);

-- FIFO lot detail for auditability.
CREATE TABLE IF NOT EXISTS position_lots_remaining (
    token_id        TEXT NOT NULL,
    opened_ts       INTEGER NOT NULL,
    event_key       TEXT NOT NULL,
    shares_remaining TEXT NOT NULL,
    cost_per_share  TEXT NOT NULL,
    PRIMARY KEY (token_id, event_key)
);

-- Reconciliation runs against the /v2/positions endpoint for our wallet.
CREATE TABLE IF NOT EXISTS position_reconciliations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    api_size        TEXT,
    api_avg_price   TEXT,
    api_entry_cost  TEXT,
    api_entry_fees  TEXT,
    api_realized    TEXT,
    api_unrealized  TEXT,
    ours_size       TEXT,
    ours_avg_price  TEXT,
    ours_realized   TEXT,
    delta_size      TEXT,
    delta_avg       TEXT,
    delta_realized  TEXT,
    verdict         TEXT NOT NULL CHECK (verdict IN ('MATCH','MISMATCH','API_ONLY','LOCAL_ONLY')),
    notes_json      TEXT NOT NULL,             -- discrepancy evidence trail
    computed_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recon_run ON position_reconciliations (run_id, token_id);
