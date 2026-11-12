-- Migration 0005: canonical ETH/BTC dip-market registry.
--
-- Registry rows are typed normalizations of Gamma payloads; raw payloads
-- remain in `markets` (clob_meta_json / gamma_meta_json). Every metadata
-- change appends to dip_market_history - rows are never silently mutated.
-- Rejected/ambiguous candidates are RETAINED with their reject_reason so
-- sync is idempotent and audit counts are reproducible.

CREATE TABLE IF NOT EXISTS dip_markets (
    condition_id   TEXT PRIMARY KEY,
    gamma_market_id TEXT,
    asset          TEXT CHECK (asset IN ('BTC','ETH')),

    -- contract
    question       TEXT NOT NULL,
    event_slug     TEXT,
    event_title    TEXT,
    resolution_rule TEXT NOT NULL,             -- verbatim description
    barrier_price  TEXT NOT NULL,              -- Decimal string
    observation_end TEXT,                      -- parsed contract window end (UTC)
    end_date       TEXT NOT NULL,              -- Gamma endDate (API deadline)
    oracle_source  TEXT,

    -- tokens
    yes_token_id   TEXT NOT NULL,
    no_token_id    TEXT NOT NULL,

    -- metadata (Gamma-reported; raw values, no derivation)
    tick_size      TEXT NOT NULL,
    min_order_size TEXT NOT NULL,
    fee_type       TEXT,
    fees_enabled   INTEGER,
    maker_base_fee_bps TEXT,
    taker_base_fee_bps TEXT,
    neg_risk       INTEGER,

    -- lifecycle
    status         TEXT NOT NULL CHECK (status IN ('ACTIVE','RESOLVED_YES','RESOLVED_NO','RESOLVED_UNKNOWN')),
    eligible       INTEGER NOT NULL DEFAULT 0,
    reject_reason  TEXT,
    source_provenance TEXT NOT NULL,

    -- verification
    resolution_status TEXT NOT NULL CHECK (resolution_status IN ('VERIFIED','AMBIGUOUS','REJECTED','UNVERIFIED')),
    verification_json TEXT NOT NULL,           -- ResolutionVerification bundle
    verifier_version  TEXT NOT NULL,

    -- provenance
    discovered_at  TEXT NOT NULL,
    last_verified_at TEXT NOT NULL,
    last_synced_at TEXT NOT NULL,
    metadata_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_dip_markets_asset ON dip_markets (asset, eligible);
CREATE INDEX IF NOT EXISTS idx_dip_markets_status ON dip_markets (status);

-- Field-level change log; written only when metadata actually changed.
CREATE TABLE IF NOT EXISTS dip_market_history (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id   TEXT NOT NULL REFERENCES dip_markets(condition_id),
    changed_at     TEXT NOT NULL,
    metadata_version INTEGER NOT NULL,
    changes_json   TEXT NOT NULL               -- [{field, before, after}]
);

CREATE INDEX IF NOT EXISTS idx_dip_history_cond ON dip_market_history (condition_id, id);

-- One row per sync invocation: observability + reproducible counts.
CREATE TABLE IF NOT EXISTS dip_sync_runs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT NOT NULL,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    universe       TEXT NOT NULL,              -- active | resolved | all
    assets         TEXT NOT NULL,              -- CSV of requested assets
    pages_fetched  INTEGER NOT NULL DEFAULT 0,
    events_seen    INTEGER NOT NULL DEFAULT 0,
    markets_seen   INTEGER NOT NULL DEFAULT 0,
    discovered     INTEGER NOT NULL DEFAULT 0, -- dip-shaped BTC/ETH candidates
    new_count      INTEGER NOT NULL DEFAULT 0,
    updated_count  INTEGER NOT NULL DEFAULT 0,
    unchanged_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    resolved_retained INTEGER NOT NULL DEFAULT 0,
    out_of_scope   INTEGER NOT NULL DEFAULT 0, -- non-dip / non-BTC-ETH markets skipped
    rejections_json TEXT NOT NULL DEFAULT '{}',-- reason -> count
    errors_json    TEXT NOT NULL DEFAULT '[]', -- [{condition_id?, error}]
    status         TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETE','COMPLETE_WITH_ERRORS','FAILED'))
);
