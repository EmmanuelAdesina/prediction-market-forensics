-- Migration 0004 (Phase 2): market-data recorder + shadow-execution state.
--
-- Design invariants (docs/07):
-- * raw_market_messages is the immutable evidence layer: verbatim frames,
--   never rewritten, retention-prunable only by age (never by research use).
-- * normalized_book_states stores ONLY verified states - a state derived from
--   deltas that cannot be reconciled is never normalized (stale-book
--   rejection); every state carries a quality label and nanosecond stamps.
-- * execution_simulations records decisions and simulated fills; a submitted
--   simulation order is NOT an executed trade (filled_qty is evidence-backed).

-- Dynamic subscription ledger (MarketSubscription).
CREATE TABLE IF NOT EXISTS market_subscriptions (
    token_id       TEXT PRIMARY KEY,
    asset_type     TEXT NOT NULL DEFAULT '',
    slug           TEXT NOT NULL DEFAULT '',
    added_at       TEXT NOT NULL,
    added_reason   TEXT NOT NULL,      -- 'initial' | 'tokor_entry' | 'manual'
    removed_at     TEXT,
    removed_reason TEXT
);

-- Durable raw frames (WS + REST bodies), session-scoped, append-only.
CREATE TABLE IF NOT EXISTS raw_market_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    channel     TEXT NOT NULL,         -- 'ws:market' | 'rest:book' | 'rest:markets'
    token_id    TEXT NOT NULL DEFAULT '',
    payload     TEXT NOT NULL,         -- verbatim frame/body
    received_at TEXT NOT NULL,         -- wall clock RFC3339
    received_ns INTEGER NOT NULL       -- wall-clock nanos for intra-second ordering
);

CREATE INDEX IF NOT EXISTS idx_raw_token_ns ON raw_market_messages (token_id, received_ns);
CREATE INDEX IF NOT EXISTS idx_raw_session ON raw_market_messages (session_id);

-- REST book snapshots (OrderBookSnapshot).
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id       TEXT NOT NULL,
    book_hash      TEXT,               -- exchange-provided hash where present
    exchange_ts    TEXT,               -- exchange-provided timestamp where present
    bids_json      TEXT NOT NULL,      -- verbatim [[price, size], ...]
    asks_json      TEXT NOT NULL,
    tick_size      TEXT,
    min_order_size TEXT,
    negative_risk  INTEGER,
    fetched_at     TEXT NOT NULL,
    session_id     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snap_token ON order_book_snapshots (token_id, fetched_at);

-- WS delta messages (OrderBookDelta), verbatim.
CREATE TABLE IF NOT EXISTS order_book_deltas (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL,        -- 'book' | 'price_change' | 'tick_size_change'
    changes_json TEXT NOT NULL,
    book_hash    TEXT,
    exchange_ts  TEXT,
    received_ns  INTEGER NOT NULL,
    session_id   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delta_token_ns ON order_book_deltas (token_id, received_ns);

-- Verified normalized states (NormalizedBookState). Quality:
--   SNAPSHOT            - built from a full REST/WS snapshot
--   DELTA_CONSISTENT    - delta applied with exchange-timestamp continuity
--   RECONCILED_AFTER_GAP- resynchronized after reconnect/gap
--   STALE               - retained for forensics, NEVER used for decisions
CREATE TABLE IF NOT EXISTS normalized_book_states (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id       TEXT NOT NULL,
    bids_json      TEXT NOT NULL,      -- normalized [[price, size], ...] best-first
    asks_json      TEXT NOT NULL,
    best_bid       TEXT,
    best_ask       TEXT,
    bid_depth      TEXT,
    ask_depth      TEXT,
    book_hash      TEXT,
    quality        TEXT NOT NULL,
    source         TEXT NOT NULL,      -- 'rest_snapshot' | 'ws_live' | 'ws_replay'
    exchange_ts    TEXT,
    first_seen_ns  INTEGER NOT NULL,
    normalized_at  TEXT NOT NULL,
    session_id     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_state_token_ns ON normalized_book_states (token_id, first_seen_ns);

-- Public trade events (PublicTradeEvent) - observable matched volume, the
-- queue-model's consumption signal.
CREATE TABLE IF NOT EXISTS public_trade_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id     TEXT NOT NULL,
    price        TEXT NOT NULL,
    size         TEXT NOT NULL,
    side         TEXT,                 -- taker side where the feed provides it
    trade_hash   TEXT,
    exchange_ts  TEXT,
    received_ns  INTEGER NOT NULL,
    session_id   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_token_ns ON public_trade_events (token_id, received_ns);

-- Health + reconnection ledger (MarketDataHealth).
CREATE TABLE IF NOT EXISTS market_data_health (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,
    channel     TEXT NOT NULL,
    event       TEXT NOT NULL,         -- 'connect' | 'disconnect' | 'subscribe' | 'unsubscribe'
                                       -- | 'stale_reject' | 'gap_detected' | 'snapshot_refresh'
    detail_json TEXT NOT NULL DEFAULT '{}',
    at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_health_session ON market_data_health (session_id, at);

-- Source observations (SourceObservation): independent latency timestamps for
-- wallet-feed polling. All *ns fields are wall-clock nanos (UTC epoch); all
-- elapsed math uses monotonic clocks at query/analysis time via pairs.
CREATE TABLE IF NOT EXISTS source_observations (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key         TEXT,            -- SourceFill identity once normalized
    raw_row_index     INTEGER NOT NULL DEFAULT 0,
    exchange_ts       INTEGER,         -- wallet timestamp (1s resolution!) if present
    poll_start_ns     INTEGER NOT NULL,
    http_response_ns  INTEGER NOT NULL,
    first_seen_ns     INTEGER NOT NULL,
    normalized_ns     INTEGER,
    decision_ready_ns INTEGER,
    book_ts_ns        INTEGER,         -- freshest book at decision time
    page_url          TEXT NOT NULL DEFAULT '',
    run_id            TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_obs_event ON source_observations (event_key);

-- Shadow-execution simulations (ExecutionSimulation). `decision` uses the
-- intelligent-copy vocabulary; exact-copy rows always record which queue
-- scenario produced them ('optimistic' | 'baseline' | 'conservative').
CREATE TABLE IF NOT EXISTS execution_simulations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy         TEXT NOT NULL,    -- 'exact_copy' | 'intelligent_copy'
    strategy_version TEXT NOT NULL,
    scenario         TEXT NOT NULL DEFAULT 'baseline',
    source_event_key TEXT NOT NULL,
    token_id         TEXT NOT NULL,
    decision         TEXT NOT NULL,    -- COPY_PASSIVE | COPY_MARKETABLE | RESIZE | WAIT
                                       -- | REJECT | REDUCE_EXISTING_POSITION | DATA_INSUFFICIENT
    decided_at_ns    INTEGER NOT NULL,
    submitted_qty    TEXT,
    limit_price      TEXT,
    queue_ahead      TEXT,
    filled_qty       TEXT NOT NULL DEFAULT '0',
    avg_fill_price   TEXT,
    fees             TEXT NOT NULL DEFAULT '0',
    book_state_id    INTEGER,          -- normalized_book_states.id at decision time
    decision_json    TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_sim_event ON execution_simulations (source_event_key);
CREATE INDEX IF NOT EXISTS idx_sim_strategy ON execution_simulations (strategy, scenario);
