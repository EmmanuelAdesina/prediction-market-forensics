//! Phase 1.5 repositories: activity events (all types), fee reconciliation,
//! positions snapshots.
//!
//! Transactional invariants match `ingest.rs`: page data + cursor + run stats
//! commit atomically; event identity dedup lives in the DB (`PRIMARY KEY
//! event_key`) via `INSERT OR IGNORE`.

use chrono::Utc;
use rusqlite::Connection;

use tokor_domain::activity::SourceFill;
use tokor_domain::activity_events::ActivityEvent;
use tokor_domain::fees::{FillFace, FillVerdict};

use crate::ingest::PageRecord;
use crate::Result;

/// Extract transaction_hash from the raw row once at persist time.
fn tx_hash_of(f: &SourceFill) -> String {
    serde_json::from_str::<serde_json::Value>(&f.raw_json)
        .ok()
        .and_then(|v| {
            v.get("transaction_hash")
                .and_then(|h| h.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_default()
}

/// Outcome of committing one activity page.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ActivityCheckpoint {
    pub events_inserted: u64,
    pub events_already_seen: u64,
    /// TRADE rows also landed in `source_fills` (new).
    pub fills_inserted: u64,
    pub fills_already_seen: u64,
    pub invalid_rows: usize,
}

/// One derived fee-reconciliation row (Phase 1.5 fee model).
#[derive(Debug, Clone)]
pub struct FeeRow {
    pub event_key: String,
    pub fill_face: Option<FillFace>,
    pub verdict: FillVerdict,
    pub gross_usdc: rust_decimal::Decimal,
    pub fee_usdc: Option<rust_decimal::Decimal>,
    pub fee_wouldbe_usdc: Option<rust_decimal::Decimal>,
    pub fee_rate: Option<rust_decimal::Decimal>,
    pub fee_category: Option<String>,
    pub reconstructed_debit: Option<rust_decimal::Decimal>,
    pub residual_usdc: Option<rust_decimal::Decimal>,
}

/// One positions snapshot row (read-only API capture).
#[derive(Debug, Clone)]
pub struct PositionSnapshotRow {
    pub token_id: String,
    pub condition_id: String,
    pub status: String,
    pub outcome: String,
    pub outcome_index: i64,
    pub current_size: String,
    pub avg_price: String,
    pub realized_pnl: Option<String>,
    pub percent_pnl: Option<String>,
    pub cur_price: Option<String>,
    pub cur_value: Option<String>,
    pub entry_cost_usdc: Option<String>,
    pub entry_fees_usdc: Option<String>,
    pub total_size: Option<String>,
    pub negative_risk: bool,
    pub mergeable: Option<bool>,
    pub redeemable: Option<bool>,
    pub event_slug: Option<String>,
    pub title: Option<String>,
}

pub trait ActivityRepository {
    /// Persist one activity page (events + TRADE fills + raw capture + run
    /// accounting) atomically.
    fn commit_activity_page(
        &self,
        page: &PageRecord,
        events: &[ActivityEvent],
        fills: &[SourceFill],
        invalid_rows: usize,
    ) -> Result<ActivityCheckpoint>;

    /// Load events, optionally filtered by type, ordered by (timestamp, key).
    fn load_activity_events(
        &self,
        event_type: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ActivityEvent>>;

    fn activity_stats(&self) -> Result<Vec<(String, u64)>>;

    /// Replace the fee reconciliation table wholesale.
    fn store_fee_rows(&self, rows: &[FeeRow]) -> Result<u64>;

    /// Load (fill, fee row) pairs for parquet emission / reporting.
    fn load_fee_rows(&self) -> Result<Vec<(SourceFill, FeeRow)>>;

    fn fee_stats(&self) -> Result<Vec<(String, u64)>>;

    /// Replace the positions snapshot wholesale (one run's capture).
    fn store_positions_snapshot(&self, run_id: &str, rows: &[PositionSnapshotRow]) -> Result<u64>;

    fn load_positions_snapshot(&self) -> Result<Vec<PositionSnapshotRow>>;
}

fn bind_activity_event(
    s: &Connection,
    e: &ActivityEvent,
    run_id: &str,
    now: &str,
) -> rusqlite::Result<usize> {
    s.execute(
        "INSERT OR IGNORE INTO activity_events
        (event_key, wallet, event_type, timestamp, condition_id, token_id, side,
         size, usdc_size, price, outcome_index, outcome, market_title, market_slug,
         event_slug, transaction_hash, occurrence, raw_json, acquisition_run_id, first_seen_at)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        rusqlite::params![
            e.event_key,
            e.wallet,
            e.event_type.as_str(),
            e.timestamp,
            e.condition_id,
            e.token_id,
            e.side.as_ref().map(|x| x.to_string()),
            e.size.as_ref().map(|d| d.to_string()),
            e.usdc_size.as_ref().map(|d| d.to_string()),
            e.price.as_ref().map(|d| d.to_string()),
            e.outcome_index,
            e.outcome,
            e.market_title,
            e.market_slug,
            e.event_slug,
            e.transaction_hash,
            e.occurrence,
            e.raw_json,
            run_id,
            now,
        ],
    )
}

fn event_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ActivityEvent> {
    use rust_decimal::Decimal;
    let side_s: Option<String> = r.get("side")?;
    let size_s: Option<String> = r.get("size")?;
    let usdc_s: Option<String> = r.get("usdc_size")?;
    let price_s: Option<String> = r.get("price")?;
    let parse = |s: &Option<String>| -> Option<Decimal> {
        s.as_ref().and_then(|v| v.parse::<Decimal>().ok())
    };
    let type_s: String = r.get("event_type")?;
    let event_type = match type_s.as_str() {
        "TRADE" => tokor_domain::activity::ActivityType::Trade,
        "SPLIT" => tokor_domain::activity::ActivityType::Split,
        "MERGE" => tokor_domain::activity::ActivityType::Merge,
        "REDEEM" => tokor_domain::activity::ActivityType::Redeem,
        "REWARD" => tokor_domain::activity::ActivityType::Reward,
        "CONVERSION" => tokor_domain::activity::ActivityType::Conversion,
        "YIELD" => tokor_domain::activity::ActivityType::Yield,
        "TAKER_REBATE" => tokor_domain::activity::ActivityType::TakerRebate,
        "MAKER_REBATE" => tokor_domain::activity::ActivityType::MakerRebate,
        other => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                format!("unknown event_type {other}").into(),
            ))
        }
    };
    Ok(ActivityEvent {
        event_key: r.get("event_key")?,
        wallet: r.get("wallet")?,
        event_type,
        timestamp: r.get("timestamp")?,
        condition_id: r.get("condition_id")?,
        token_id: r.get("token_id")?,
        side: side_s.and_then(|s| match s.as_str() {
            "BUY" => Some(tokor_domain::activity::Side::Buy),
            "SELL" => Some(tokor_domain::activity::Side::Sell),
            _ => None,
        }),
        size: parse(&size_s),
        usdc_size: parse(&usdc_s),
        price: parse(&price_s),
        outcome_index: r.get("outcome_index")?,
        outcome: r.get("outcome")?,
        market_title: r.get("market_title")?,
        market_slug: r.get("market_slug")?,
        event_slug: r.get("event_slug")?,
        transaction_hash: r.get("transaction_hash")?,
        occurrence: r.get::<_, i64>("occurrence")? as u32,
        raw_json: r.get("raw_json")?,
    })
}

fn fee_row_from_pair(f: &SourceFill, r: &rusqlite::Row<'_>) -> rusqlite::Result<FeeRow> {
    use rust_decimal::Decimal;
    let parse =
        |s: Option<String>| -> Option<Decimal> { s.and_then(|v| v.parse::<Decimal>().ok()) };
    let verdict_s: String = r.get("verdict")?;
    let verdict = match verdict_s.as_str() {
        "VERIFIED_GROSS_NOTIONAL" => FillVerdict::VerifiedGrossNotional,
        "VERIFIED_FEE_INCLUSIVE" => FillVerdict::VerifiedFeeInclusive,
        "UNKNOWN_FEE_SEMANTICS" => FillVerdict::UnknownFeeSemantics,
        _ => FillVerdict::Unexplained,
    };
    let face_s: Option<String> = r.get("fill_face")?;
    let fill_face = match face_s.as_deref() {
        Some("maker") => Some(FillFace::Maker),
        Some("taker") => Some(FillFace::Taker),
        _ => Some(FillFace::Indeterminate),
    };
    Ok(FeeRow {
        event_key: f.event_key.clone(),
        fill_face,
        verdict,
        gross_usdc: r
            .get::<_, String>("gross_usdc")?
            .parse::<Decimal>()
            .unwrap_or_default(),
        fee_usdc: parse(r.get("fee_usdc")?),
        fee_wouldbe_usdc: parse(r.get("fee_wouldbe_usdc")?),
        fee_rate: parse(r.get("fee_rate")?),
        fee_category: r.get("fee_category")?,
        reconstructed_debit: parse(r.get("reconstructed_debit")?),
        residual_usdc: parse(r.get("residual_usdc")?),
    })
}

impl ActivityRepository for crate::Database {
    fn commit_activity_page(
        &self,
        page: &PageRecord,
        events: &[ActivityEvent],
        fills: &[SourceFill],
        invalid_rows: usize,
    ) -> Result<ActivityCheckpoint> {
        let run_id = &page.run_id;
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;

        tx.execute(
            "INSERT INTO raw_api_pages
             (acquisition_run_id, page_index, fetched_at, http_status, url,
              query_params, body_json, next_cursor, has_more)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            rusqlite::params![
                run_id,
                page.page_index as i64,
                page.fetched_at,
                page.http_status,
                page.url,
                page.query_params,
                page.body_json,
                page.next_cursor,
                if page.has_more { 1 } else { 0 },
            ],
        )?;

        let now = Utc::now().to_rfc3339();
        let mut ev_new = 0u64;
        let mut ev_seen = 0u64;
        for e in events {
            let n = bind_activity_event(&tx, e, run_id, &now)?;
            if n > 0 {
                ev_new += 1;
            } else {
                ev_seen += 1;
            }
        }

        // TRADE fills: strict SourceFill normalization happened upstream;
        // dedup-insert here with the composite identity PK.
        let mut fills_new = 0u64;
        let mut fills_seen = 0u64;
        for f in fills {
            let n = tx.execute(
                "INSERT OR IGNORE INTO source_fills
                (event_key, wallet, timestamp, condition_id, token_id, side,
                 size, usdc_size, price, outcome_index, outcome,
                 market_title, market_slug, event_slug, icon,
                 transaction_hash, raw_json, acquisition_run_id, first_seen_at)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19)",
                rusqlite::params![
                    f.event_key,
                    f.wallet,
                    f.timestamp,
                    f.condition_id,
                    f.token_id,
                    f.side.to_string(),
                    f.size.to_string(),
                    f.usdc_size.to_string(),
                    f.price.to_string(),
                    f.outcome_index,
                    f.outcome,
                    f.market_title,
                    f.market_slug,
                    f.event_slug,
                    f.icon,
                    tx_hash_of(f),
                    f.raw_json,
                    run_id,
                    now,
                ],
            )?;
            if n > 0 {
                fills_new += 1;
            } else {
                fills_seen += 1;
            }
        }

        tx.execute(
            "UPDATE acquisition_runs
             SET pages_committed = pages_committed + 1,
                 fills_committed = fills_committed + ?2,
                 last_cursor = COALESCE(?3, last_cursor)
             WHERE run_id = ?1",
            rusqlite::params![run_id, fills_new as i64, page.next_cursor],
        )?;

        // Durable cursor checkpoint, same transaction (crash-invariant).
        let fingerprint: String = tx
            .query_row(
                "SELECT filters_fingerprint FROM acquisition_runs WHERE run_id = ?1",
                [run_id],
                |r| r.get(0),
            )
            .map_err(|e| crate::PersistError::Misuse(format!("unknown run {run_id}: {e}")))?;
        tx.execute(
            "INSERT INTO wallet_cursor_state (fingerprint, last_cursor, updated_at, pages_committed)
             VALUES (?1, ?2, ?3, 1)
             ON CONFLICT(fingerprint) DO UPDATE SET
               last_cursor = excluded.last_cursor,
               updated_at = excluded.updated_at,
               pages_committed = wallet_cursor_state.pages_committed + 1",
            rusqlite::params![fingerprint, page.next_cursor, Utc::now().to_rfc3339()],
        )?;

        tx.commit()?;
        Ok(ActivityCheckpoint {
            events_inserted: ev_new,
            events_already_seen: ev_seen,
            fills_inserted: fills_new,
            fills_already_seen: fills_seen,
            invalid_rows,
        })
    }

    fn load_activity_events(
        &self,
        event_type: Option<&str>,
        limit: Option<u64>,
    ) -> Result<Vec<ActivityEvent>> {
        let conn = self.inner_w();
        let sql = match (event_type, limit) {
            (Some(_), Some(_)) => {
                "SELECT * FROM activity_events WHERE event_type = ?1 \
                 ORDER BY timestamp, event_key LIMIT ?2"
            }
            (Some(_), None) => {
                "SELECT * FROM activity_events WHERE event_type = ?1 \
                 ORDER BY timestamp, event_key"
            }
            (None, Some(_)) => {
                "SELECT * FROM activity_events ORDER BY timestamp, event_key LIMIT ?1"
            }
            (None, None) => "SELECT * FROM activity_events ORDER BY timestamp, event_key",
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = match (event_type, limit) {
            (Some(t), Some(l)) => stmt
                .query_map(rusqlite::params![t, l as i64], event_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            (Some(t), None) => stmt
                .query_map([t], event_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            (None, Some(l)) => stmt
                .query_map([l as i64], event_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            (None, None) => stmt
                .query_map([], event_from_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };
        Ok(rows)
    }

    fn activity_stats(&self) -> Result<Vec<(String, u64)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT event_type, COUNT(*) FROM activity_events GROUP BY event_type ORDER BY 1",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn store_fee_rows(&self, rows: &[FeeRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM fee_reconciliation", [])?;
        let now = Utc::now().to_rfc3339();
        let mut n = 0u64;
        for r in rows {
            tx.execute(
                "INSERT INTO fee_reconciliation
                (event_key, fill_face, verdict, gross_usdc, fee_usdc, fee_wouldbe_usdc,
                 fee_rate, fee_category, reconstructed_debit, residual_usdc, derived_at)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                rusqlite::params![
                    r.event_key,
                    r.fill_face.map(|f| match f {
                        FillFace::Maker => "maker",
                        FillFace::Taker => "taker",
                        FillFace::Indeterminate => "n/a",
                    }),
                    match r.verdict {
                        FillVerdict::VerifiedGrossNotional => "VERIFIED_GROSS_NOTIONAL",
                        FillVerdict::VerifiedFeeInclusive => "VERIFIED_FEE_INCLUSIVE",
                        FillVerdict::UnknownFeeSemantics => "UNKNOWN_FEE_SEMANTICS",
                        FillVerdict::Unexplained => "UNEXPLAINED",
                    },
                    r.gross_usdc.to_string(),
                    r.fee_usdc.map(|d| d.to_string()),
                    r.fee_wouldbe_usdc.map(|d| d.to_string()),
                    r.fee_rate.map(|d| d.to_string()),
                    r.fee_category,
                    r.reconstructed_debit.map(|d| d.to_string()),
                    r.residual_usdc.map(|d| d.to_string()),
                    now,
                ],
            )?;
            n += 1;
        }
        tx.commit()?;
        Ok(n)
    }

    fn load_fee_rows(&self) -> Result<Vec<(SourceFill, FeeRow)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT f.event_key AS event_key, f.wallet AS wallet, f.timestamp AS timestamp,
                    f.condition_id AS condition_id, f.token_id AS token_id,
                    f.side AS side, f.size AS size, f.usdc_size AS usdc_size,
                    f.price AS price, f.outcome_index AS outcome_index,
                    f.outcome AS outcome, f.market_title AS market_title,
                    f.market_slug AS market_slug, f.event_slug AS event_slug,
                    f.icon AS icon, f.raw_json AS raw_json,
                    r.fill_face, r.verdict, r.gross_usdc, r.fee_usdc,
                    r.fee_wouldbe_usdc, r.fee_rate, r.fee_category,
                    r.reconstructed_debit, r.residual_usdc
             FROM source_fills f
             JOIN fee_reconciliation r ON r.event_key = f.event_key
             ORDER BY f.timestamp, f.event_key",
        )?;
        let rows = stmt
            .query_map([], |row| {
                let f = crate::ingest::fill_from_row(row)?;
                let fr = fee_row_from_pair(&f, row)?;
                Ok((f, fr))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn fee_stats(&self) -> Result<Vec<(String, u64)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT verdict, COUNT(*) FROM fee_reconciliation GROUP BY verdict ORDER BY 1",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn store_positions_snapshot(&self, run_id: &str, rows: &[PositionSnapshotRow]) -> Result<u64> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM positions_snapshot", [])?;
        let now = Utc::now().to_rfc3339();
        let mut n = 0u64;
        for r in rows {
            tx.execute(
                "INSERT INTO positions_snapshot
                (token_id, condition_id, status, outcome, outcome_index, current_size,
                 avg_price, realized_pnl, percent_pnl, cur_price, cur_value,
                 entry_cost_usdc, entry_fees_usdc, total_size, negative_risk,
                 mergeable, redeemable, event_slug, title, captured_at, acquisition_run_id)
                VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
                rusqlite::params![
                    r.token_id,
                    r.condition_id,
                    r.status,
                    r.outcome,
                    r.outcome_index,
                    r.current_size,
                    r.avg_price,
                    r.realized_pnl,
                    r.percent_pnl,
                    r.cur_price,
                    r.cur_value,
                    r.entry_cost_usdc,
                    r.entry_fees_usdc,
                    r.total_size,
                    if r.negative_risk { 1 } else { 0 },
                    r.mergeable.map(|b| if b { 1 } else { 0 }),
                    r.redeemable.map(|b| if b { 1 } else { 0 }),
                    r.event_slug,
                    r.title,
                    now,
                    run_id,
                ],
            )?;
            n += 1;
        }
        tx.commit()?;
        Ok(n)
    }

    fn load_positions_snapshot(&self) -> Result<Vec<PositionSnapshotRow>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT token_id, condition_id, status, outcome, outcome_index, current_size,
                    avg_price, realized_pnl, percent_pnl, cur_price, cur_value,
                    entry_cost_usdc, entry_fees_usdc, total_size, negative_risk,
                    mergeable, redeemable, event_slug, title
             FROM positions_snapshot ORDER BY captured_at DESC",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(PositionSnapshotRow {
                    token_id: r.get(0)?,
                    condition_id: r.get(1)?,
                    status: r.get(2)?,
                    outcome: r.get(3)?,
                    outcome_index: r.get(4)?,
                    current_size: r.get(5)?,
                    avg_price: r.get(6)?,
                    realized_pnl: r.get(7)?,
                    percent_pnl: r.get(8)?,
                    cur_price: r.get(9)?,
                    cur_value: r.get(10)?,
                    entry_cost_usdc: r.get(11)?,
                    entry_fees_usdc: r.get(12)?,
                    total_size: r.get(13)?,
                    negative_risk: r.get::<_, i64>(14)? != 0,
                    mergeable: r.get::<_, Option<i64>>(15)?.map(|v| v != 0),
                    redeemable: r.get::<_, Option<i64>>(16)?.map(|v| v != 0),
                    event_slug: r.get(17)?,
                    title: r.get(18)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

// Keep PersistError in scope for the trait impls above.
