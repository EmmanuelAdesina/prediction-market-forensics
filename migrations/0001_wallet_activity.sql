-- Migration 0001: core schema for wallet activity ingestion.
-- SQLite, WAL mode is set at connection level.
-- NOTE: event_key is a deterministic composite identity (see domain::activity).
CREATE TABLE IF NOT EXISTS raw_api_pages (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    acquisition_run_id TEXT NOT NULL,
    page_index        INTEGER NOT NULL,
    fetched_at        TEXT NOT NULL,           -- RFC3339 UTC
    http_status       INTEGER NOT NULL,
    url               TEXT NOT NULL,
    query_params      TEXT NOT NULL,           -- exact query string used
    body_json         TEXT NOT NULL,           -- verbatim response body
    next_cursor       TEXT,
    has_more          INTEGER NOT NULL,
    UNIQUE (acquisition_run_id, page_index)
);

CREATE INDEX IF NOT EXISTS idx_raw_pages_run
    ON raw_api_pages (acquisition_run_id);

-- Normalized source fills. Append-mostly; unique composite identity.
CREATE TABLE IF NOT EXISTS source_fills (
    event_key     TEXT PRIMARY KEY,
    wallet        TEXT NOT NULL,
    timestamp     INTEGER NOT NULL,            -- unix seconds from source
    condition_id  TEXT NOT NULL,
    token_id      TEXT NOT NULL,
    side          TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    size          TEXT NOT NULL,               -- Decimal as string
    usdc_size     TEXT NOT NULL,
    price         TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    outcome       TEXT NOT NULL,
    market_title  TEXT NOT NULL,
    market_slug   TEXT NOT NULL,
    event_slug    TEXT NOT NULL,
    icon          TEXT NOT NULL,
    transaction_hash TEXT NOT NULL,            -- NOT unique by itself
    raw_json      TEXT NOT NULL,               -- per-row raw record
    acquisition_run_id TEXT NOT NULL,
    first_seen_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fills_ts ON source_fills (timestamp);
CREATE INDEX IF NOT EXISTS idx_fills_cond ON source_fills (condition_id);
CREATE INDEX IF NOT EXISTS idx_fills_token_ts ON source_fills (token_id, timestamp);
-- tx_hash is NOT unique (one tx can carry many fills) but is a primary
-- forensic lookup key.
CREATE INDEX IF NOT EXISTS idx_fills_tx ON source_fills (transaction_hash);

-- Acquisition run lifecycle. Cursor advances transactionally with page data;
-- this table records run metadata + the last accepted cursor per run.
CREATE TABLE IF NOT EXISTS acquisition_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    status          TEXT NOT NULL CHECK (status IN ('RUNNING','COMPLETE','ABORTED')),
    filters_json    TEXT NOT NULL,
    filters_fingerprint TEXT NOT NULL,
    pages_committed INTEGER NOT NULL DEFAULT 0,
    fills_committed INTEGER NOT NULL DEFAULT 0,
    last_cursor     TEXT
);

-- Durable wallet cursor keyed by filter fingerprint. The cursor may only
-- advance inside the same transaction that commits the page's fills.
CREATE TABLE IF NOT EXISTS wallet_cursor_state (
    fingerprint     TEXT PRIMARY KEY,
    last_cursor     TEXT,
    updated_at      TEXT,
    pages_committed INTEGER NOT NULL DEFAULT 0
);
