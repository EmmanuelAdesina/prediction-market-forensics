//! Phase 2 market-data repositories: raw frames, snapshots, deltas,
//! normalized states, public trades, health, source observations, and
//! shadow-execution simulations (migration 0004).
//!
//! Write paths are batch-transactional (the WS session produces bursts);
//! retention pruning touches ONLY `raw_market_messages` — normalized states,
//! snapshots, trades, and simulations are research evidence and are never
//! pruned automatically.

use chrono::Utc;
use rusqlite::Connection;

use crate::Result;

#[derive(Debug, Clone)]
pub struct RawMessageRow {
    pub session_id: String,
    pub channel: String,
    pub token_id: String,
    pub payload: String,
    pub received_at: String,
    pub received_ns: i64,
}

#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub token_id: String,
    pub book_hash: Option<String>,
    pub exchange_ts: Option<String>,
    pub bids_json: String,
    pub asks_json: String,
    pub tick_size: Option<String>,
    pub min_order_size: Option<String>,
    pub negative_risk: Option<bool>,
    pub fetched_at: String,
    pub session_id: String,
}

#[derive(Debug, Clone)]
pub struct DeltaRow {
    pub token_id: String,
    pub event_type: String,
    pub changes_json: String,
    pub book_hash: Option<String>,
    pub exchange_ts: Option<String>,
    pub received_ns: i64,
    pub session_id: String,
}

#[derive(Debug, Clone)]
pub struct BookStateRow {
    pub token_id: String,
    pub bids_json: String,
    pub asks_json: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub bid_depth: Option<String>,
    pub ask_depth: Option<String>,
    pub book_hash: Option<String>,
    pub quality: String,
    pub source: String,
    pub exchange_ts: Option<String>,
    pub first_seen_ns: i64,
    pub normalized_at: String,
    pub session_id: String,
}

#[derive(Debug, Clone)]
pub struct TradeEventRow {
    pub token_id: String,
    pub price: String,
    pub size: String,
    pub side: Option<String>,
    pub trade_hash: Option<String>,
    pub exchange_ts: Option<String>,
    pub received_ns: i64,
    pub session_id: String,
}

#[derive(Debug, Clone)]
pub struct HealthRow {
    pub session_id: String,
    pub channel: String,
    pub event: String,
    pub detail_json: String,
    pub at: String,
}

#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    pub token_id: String,
    pub asset_type: String,
    pub slug: String,
    pub added_reason: String,
}

/// One wallet-feed polling observation with independent latency stamps
/// (all *ns fields are wall-clock nanos; pairs support monotonic elapsed
/// math at analysis time).
#[derive(Debug, Clone)]
pub struct SourceObservationRow {
    pub event_key: Option<String>,
    pub raw_row_index: i64,
    pub exchange_ts: Option<i64>,
    pub poll_start_ns: i64,
    pub http_response_ns: i64,
    pub first_seen_ns: i64,
    pub normalized_ns: Option<i64>,
    pub decision_ready_ns: Option<i64>,
    pub book_ts_ns: Option<i64>,
    pub page_url: String,
    pub run_id: String,
}

/// One shadow-execution decision. A submitted simulation order is NOT an
/// executed trade — `filled_qty` is evidence-backed only.
#[derive(Debug, Clone)]
pub struct SimulationRow {
    pub strategy: String,
    pub strategy_version: String,
    pub scenario: String,
    pub source_event_key: String,
    pub token_id: String,
    pub decision: String,
    pub decided_at_ns: i64,
    pub submitted_qty: Option<String>,
    pub limit_price: Option<String>,
    pub queue_ahead: Option<String>,
    pub filled_qty: String,
    pub avg_fill_price: Option<String>,
    pub fees: String,
    pub book_state_id: Option<i64>,
    pub decision_json: String,
}

pub trait MarketDataRepository {
    fn record_raw_messages(&self, rows: &[RawMessageRow]) -> Result<u64>;
    fn record_snapshot(&self, row: &SnapshotRow) -> Result<i64>;
    /// Batch writes: each call is ONE transaction (the WS session produces
    /// bursts; per-row transactions on this filesystem are far too slow).
    fn record_deltas(&self, rows: &[DeltaRow]) -> Result<u64>;
    fn record_book_states(&self, rows: &[BookStateRow]) -> Result<u64>;
    fn record_trade_events(&self, rows: &[TradeEventRow]) -> Result<u64>;
    fn record_health(&self, rows: &[HealthRow]) -> Result<u64>;
    fn add_subscription(&self, row: &SubscriptionRow) -> Result<()>;
    fn remove_subscription(&self, token_id: &str, reason: &str) -> Result<()>;
    fn active_subscriptions(&self) -> Result<Vec<(String, String)>>;
    fn record_source_observation(&self, row: &SourceObservationRow) -> Result<i64>;
    fn record_simulation(&self, row: &SimulationRow) -> Result<i64>;
    /// Retention pruning: ONLY raw frames, strictly older than `before_ns`.
    fn prune_raw_messages(&self, before_ns: i64) -> Result<u64>;
    /// Session summary counts for the CLI report.
    fn market_data_counts(&self) -> Result<(u64, u64, u64, u64)>;
    fn load_book_states(&self, token_id: &str, limit: u64) -> Result<Vec<BookStateRow>>;
    fn load_trades(&self, token_id: &str, limit: u64) -> Result<Vec<TradeEventRow>>;
}

fn insert_delta(s: &Connection, r: &DeltaRow) -> rusqlite::Result<()> {
    s.execute(
        "INSERT INTO order_book_deltas
        (token_id, event_type, changes_json, book_hash, exchange_ts, received_ns, session_id)
        VALUES (?1,?2,?3,?4,?5,?6,?7)",
        rusqlite::params![
            r.token_id,
            r.event_type,
            r.changes_json,
            r.book_hash,
            r.exchange_ts,
            r.received_ns,
            r.session_id,
        ],
    )?;
    Ok(())
}

fn insert_state(s: &Connection, r: &BookStateRow) -> rusqlite::Result<()> {
    s.execute(
        "INSERT INTO normalized_book_states
        (token_id, bids_json, asks_json, best_bid, best_ask, bid_depth, ask_depth,
         book_hash, quality, source, exchange_ts, first_seen_ns, normalized_at, session_id)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        rusqlite::params![
            r.token_id,
            r.bids_json,
            r.asks_json,
            r.best_bid,
            r.best_ask,
            r.bid_depth,
            r.ask_depth,
            r.book_hash,
            r.quality,
            r.source,
            r.exchange_ts,
            r.first_seen_ns,
            r.normalized_at,
            r.session_id,
        ],
    )?;
    Ok(())
}

fn insert_trade(s: &Connection, r: &TradeEventRow) -> rusqlite::Result<()> {
    s.execute(
        "INSERT INTO public_trade_events
        (token_id, price, size, side, trade_hash, exchange_ts, received_ns, session_id)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        rusqlite::params![
            r.token_id,
            r.price,
            r.size,
            r.side,
            r.trade_hash,
            r.exchange_ts,
            r.received_ns,
            r.session_id,
        ],
    )?;
    Ok(())
}

fn insert_health(s: &Connection, r: &HealthRow) -> rusqlite::Result<()> {
    s.execute(
        "INSERT INTO market_data_health (session_id, channel, event, detail_json, at)
        VALUES (?1,?2,?3,?4,?5)",
        rusqlite::params![r.session_id, r.channel, r.event, r.detail_json, r.at],
    )?;
    Ok(())
}

fn insert_raw(s: &Connection, r: &RawMessageRow) -> rusqlite::Result<()> {
    s.execute(
        "INSERT INTO raw_market_messages
        (session_id, channel, token_id, payload, received_at, received_ns)
        VALUES (?1,?2,?3,?4,?5,?6)",
        rusqlite::params![
            r.session_id,
            r.channel,
            r.token_id,
            r.payload,
            r.received_at,
            r.received_ns
        ],
    )?;
    Ok(())
}

impl MarketDataRepository for crate::Database {
    fn record_raw_messages(&self, rows: &[RawMessageRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        for r in rows {
            insert_raw(&tx, r)?;
        }
        tx.commit()?;
        Ok(rows.len() as u64)
    }

    fn record_snapshot(&self, row: &SnapshotRow) -> Result<i64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO order_book_snapshots
            (token_id, book_hash, exchange_ts, bids_json, asks_json, tick_size,
             min_order_size, negative_risk, fetched_at, session_id)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                row.token_id,
                row.book_hash,
                row.exchange_ts,
                row.bids_json,
                row.asks_json,
                row.tick_size,
                row.min_order_size,
                row.negative_risk.map(|b| if b { 1 } else { 0 }),
                row.fetched_at,
                row.session_id,
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    fn record_deltas(&self, rows: &[DeltaRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        for r in rows {
            insert_delta(&tx, r)?;
        }
        tx.commit()?;
        Ok(rows.len() as u64)
    }

    fn record_book_states(&self, rows: &[BookStateRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        for r in rows {
            insert_state(&tx, r)?;
        }
        tx.commit()?;
        Ok(rows.len() as u64)
    }

    fn record_trade_events(&self, rows: &[TradeEventRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        for r in rows {
            insert_trade(&tx, r)?;
        }
        tx.commit()?;
        Ok(rows.len() as u64)
    }

    fn record_health(&self, rows: &[HealthRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        for r in rows {
            insert_health(&tx, r)?;
        }
        tx.commit()?;
        Ok(rows.len() as u64)
    }

    fn add_subscription(&self, row: &SubscriptionRow) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO market_subscriptions (token_id, asset_type, slug, added_at, added_reason)
            VALUES (?1,?2,?3,?4,?5)
            ON CONFLICT(token_id) DO UPDATE SET
              removed_at = NULL, removed_reason = NULL,
              added_reason = excluded.added_reason",
            rusqlite::params![
                row.token_id,
                row.asset_type,
                row.slug,
                Utc::now().to_rfc3339(),
                row.added_reason
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn remove_subscription(&self, token_id: &str, reason: &str) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE market_subscriptions
             SET removed_at = ?2, removed_reason = ?3
             WHERE token_id = ?1 AND removed_at IS NULL",
            rusqlite::params![token_id, Utc::now().to_rfc3339(), reason],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn active_subscriptions(&self) -> Result<Vec<(String, String)>> {
        let conn = self.inner_w();
        let mut stmt = conn
            .prepare("SELECT token_id, slug FROM market_subscriptions WHERE removed_at IS NULL")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn record_source_observation(&self, row: &SourceObservationRow) -> Result<i64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO source_observations
            (event_key, raw_row_index, exchange_ts, poll_start_ns, http_response_ns,
             first_seen_ns, normalized_ns, decision_ready_ns, book_ts_ns, page_url, run_id)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            rusqlite::params![
                row.event_key,
                row.raw_row_index,
                row.exchange_ts,
                row.poll_start_ns,
                row.http_response_ns,
                row.first_seen_ns,
                row.normalized_ns,
                row.decision_ready_ns,
                row.book_ts_ns,
                row.page_url,
                row.run_id,
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    fn record_simulation(&self, row: &SimulationRow) -> Result<i64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO execution_simulations
            (strategy, strategy_version, scenario, source_event_key, token_id, decision,
             decided_at_ns, submitted_qty, limit_price, queue_ahead, filled_qty,
             avg_fill_price, fees, book_state_id, decision_json)
            VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            rusqlite::params![
                row.strategy,
                row.strategy_version,
                row.scenario,
                row.source_event_key,
                row.token_id,
                row.decision,
                row.decided_at_ns,
                row.submitted_qty,
                row.limit_price,
                row.queue_ahead,
                row.filled_qty,
                row.avg_fill_price,
                row.fees,
                row.book_state_id,
                row.decision_json,
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(id)
    }

    fn prune_raw_messages(&self, before_ns: i64) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        let n = tx.execute(
            "DELETE FROM raw_market_messages WHERE received_ns < ?1",
            [before_ns],
        )?;
        tx.commit()?;
        Ok(n as u64)
    }

    fn market_data_counts(&self) -> Result<(u64, u64, u64, u64)> {
        let conn = self.inner_w();
        let one = |sql: &str| -> Result<u64> {
            Ok(conn
                .query_row(sql, [], |r| r.get::<_, i64>(0))
                .map(|v| v.max(0) as u64)?)
        };
        let raw = one("SELECT COUNT(*) FROM raw_market_messages")?;
        let states = one("SELECT COUNT(*) FROM normalized_book_states")?;
        let trades = one("SELECT COUNT(*) FROM public_trade_events")?;
        let health = one("SELECT COUNT(*) FROM market_data_health")?;
        Ok((raw, states, trades, health))
    }

    fn load_book_states(&self, token_id: &str, limit: u64) -> Result<Vec<BookStateRow>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT token_id, bids_json, asks_json, best_bid, best_ask, bid_depth, ask_depth,
                    book_hash, quality, source, exchange_ts, first_seen_ns, normalized_at,
                    session_id
             FROM normalized_book_states WHERE token_id = ?1
             ORDER BY first_seen_ns DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![token_id, limit as i64], |r| {
                Ok(BookStateRow {
                    token_id: r.get(0)?,
                    bids_json: r.get(1)?,
                    asks_json: r.get(2)?,
                    best_bid: r.get(3)?,
                    best_ask: r.get(4)?,
                    bid_depth: r.get(5)?,
                    ask_depth: r.get(6)?,
                    book_hash: r.get(7)?,
                    quality: r.get(8)?,
                    source: r.get(9)?,
                    exchange_ts: r.get(10)?,
                    first_seen_ns: r.get(11)?,
                    normalized_at: r.get(12)?,
                    session_id: r.get(13)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn load_trades(&self, token_id: &str, limit: u64) -> Result<Vec<TradeEventRow>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT token_id, price, size, side, trade_hash, exchange_ts, received_ns, session_id
             FROM public_trade_events WHERE token_id = ?1
             ORDER BY received_ns DESC LIMIT ?2",
        )?;
        let rows = stmt
            .query_map(rusqlite::params![token_id, limit as i64], |r| {
                Ok(TradeEventRow {
                    token_id: r.get(0)?,
                    price: r.get(1)?,
                    size: r.get(2)?,
                    side: r.get(3)?,
                    trade_hash: r.get(4)?,
                    exchange_ts: r.get(5)?,
                    received_ns: r.get(6)?,
                    session_id: r.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}
