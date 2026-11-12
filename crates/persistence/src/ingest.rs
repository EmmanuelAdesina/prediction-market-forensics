//! The transactional page checkpoint: fills + raw page + run stats + cursor
//! are committed as ONE SQLite transaction.
//!
//! Crash-invariant: after any crash, either the whole page (fills, raw capture,
//! run accounting, and cursor movement) is visible, or none of it is.
//! Therefore restart recovery replays from `last_cursor` without duplication
//! and never skips a page.

use chrono::Utc;
use rusqlite::Connection;
use tokor_domain::activity::SourceFill;

use crate::{PersistError, Result};

/// Outcome of committing one page.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PageCheckpoint {
    /// Fills actually inserted (new rows).
    pub fills_inserted: u64,
    /// Page fills that already existed (idempotent replay duplicates).
    pub fills_already_seen: u64,
    /// Non-TRADE records preserved raw on the page (not normalized).
    pub non_trade_records: u64,
    pub page_index: u64,
    /// Whether the persisted wallet cursor advanced.
    pub cursor_advanced: bool,
}

use crate::Database;

/// Cohesive record of one fetched API page, persisted verbatim alongside its
/// normalized fills inside the same transaction.
#[derive(Debug, Clone)]
pub struct PageRecord {
    pub run_id: String,
    pub page_index: u64,
    pub fetched_at: String,
    pub http_status: u16,
    pub url: String,
    pub query_params: String,
    /// Full response body, preserved forensically verbatim.
    pub body_json: String,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub trait IngestRepository {
    /// Begin an acquisition run. Overwrites nothing: run metadata only.
    fn start_run(&self, run_id: &str, filters_json: &str, filters_fingerprint: &str) -> Result<()>;

    /// Mark run finished. `status` is `COMPLETE` or `ABORTED`.
    fn finish_run(&self, run_id: &str, status: &str, last_cursor: Option<&str>) -> Result<()>;

    /// Persist one page + its fills + advanced cursor, atomically.
    /// Performs `INSERT OR IGNORE` per fill, returns the checkpoint.
    fn commit_page(&self, page: &PageRecord, fills: &[SourceFill]) -> Result<PageCheckpoint>;

    /// Durable cursor state for a filter fingerprint.
    fn load_cursor(&self, filters_fingerprint: &str) -> Result<Option<(Option<String>, u64)>>;

    /// Fetch fills (deduped) ordered by (timestamp, event_key).
    fn load_fills(&self, limit: Option<u64>) -> Result<Vec<SourceFill>>;

    /// Counters for metrics reporting.
    fn fill_stats(&self) -> Result<FillsStats>;
}

#[derive(Debug, Clone)]
pub struct FillsStats {
    pub total: u64,
    pub buys: u64,
    pub sells: u64,
}

fn bind_fill(s: &Connection, f: &SourceFill, run_id: &str, now: &str) -> rusqlite::Result<usize> {
    s.execute(
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
    )
}

fn tx_hash_of(f: &SourceFill) -> String {
    // transaction_hash lives inside the raw row; extract once at persist time
    // for indexing purposes (never re-parse the whole row at query time).
    serde_json::from_str::<serde_json::Value>(&f.raw_json)
        .ok()
        .and_then(|v| {
            v.get("transaction_hash")
                .and_then(|h| h.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_else(|| String::from(""))
}

impl IngestRepository for Database {
    fn start_run(&self, run_id: &str, filters_json: &str, filters_fingerprint: &str) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO acquisition_runs
             (run_id, started_at, finished_at, status, filters_json, filters_fingerprint,
              pages_committed, fills_committed, last_cursor)
             VALUES (?1, ?2, NULL, 'RUNNING', ?3, ?4, 0, 0, NULL)",
            rusqlite::params![
                run_id,
                Utc::now().to_rfc3339(),
                filters_json,
                filters_fingerprint
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn finish_run(&self, run_id: &str, status: &str, last_cursor: Option<&str>) -> Result<()> {
        if !matches!(status, "COMPLETE" | "ABORTED") {
            return Err(crate::PersistError::Misuse(format!(
                "invalid run status {status}"
            )));
        }
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE acquisition_runs SET finished_at = ?2, status = ?3,
               last_cursor = COALESCE(?4, last_cursor)
             WHERE run_id = ?1",
            rusqlite::params![run_id, Utc::now().to_rfc3339(), status, last_cursor],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn commit_page(&self, page: &PageRecord, fills: &[SourceFill]) -> Result<PageCheckpoint> {
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
        let mut inserted = 0u64;
        let mut already = 0u64;
        for f in fills {
            let n = bind_fill(&tx, f, run_id, &now)?;
            if n > 0 {
                inserted += 1;
            } else {
                already += 1;
            }
        }

        let cursor_payload = page.next_cursor.clone();

        // Run accounting (counters reflect committed page fill delta).
        tx.execute(
            "UPDATE acquisition_runs
             SET pages_committed = pages_committed + 1,
                 fills_committed = fills_committed + ?2,
                 last_cursor = COALESCE(?3, last_cursor)
             WHERE run_id = ?1",
            rusqlite::params![run_id, inserted as i64, cursor_payload],
        )?;

        // Durable cursor checkpoint. Reuses the row keyed by run fingerprint;
        // the fingerprint is carried by the caller for correctness.
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
            rusqlite::params![fingerprint, cursor_payload, Utc::now().to_rfc3339()],
        )?;

        tx.commit()?;
        Ok(PageCheckpoint {
            fills_inserted: inserted,
            fills_already_seen: already,
            non_trade_records: 0, // computed by caller if needed
            page_index: page.page_index,
            cursor_advanced: true,
        })
    }

    fn load_cursor(&self, filters_fingerprint: &str) -> Result<Option<(Option<String>, u64)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT last_cursor, pages_committed FROM wallet_cursor_state WHERE fingerprint = ?1",
        )?;
        let mut rows = stmt.query([filters_fingerprint])?;
        match rows.next()? {
            Some(row) => {
                let cursor: Option<String> = row.get(0)?;
                let pages: u64 = row.get::<_, i64>(1)? as u64;
                Ok(Some((cursor, pages)))
            }
            None => Ok(None),
        }
    }

    fn load_fills(&self, limit: Option<u64>) -> Result<Vec<SourceFill>> {
        let conn = self.inner_w();
        let sql = match limit {
            Some(l) => format!("{FILL_COLUMNS} ORDER BY timestamp, event_key LIMIT {l}"),
            None => format!("{FILL_COLUMNS} ORDER BY timestamp, event_key"),
        };
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], fill_from_row)?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    fn fill_stats(&self) -> Result<FillsStats> {
        let conn = self.inner_w();
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM source_fills", [], |r| r.get(0))?;
        let buys: i64 = conn.query_row(
            "SELECT COUNT(*) FROM source_fills WHERE side='BUY'",
            [],
            |r| r.get(0),
        )?;
        let sells = total - buys;
        Ok(FillsStats {
            total: total as u64,
            buys: buys as u64,
            sells: sells as u64,
        })
    }
}

/// Shared fill mapper reading by column name; used by `load_fills` and the
/// fee-reconciliation JOIN query (both name every column explicitly).
pub(crate) fn fill_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<SourceFill> {
    let side_s: String = r.get("side")?;
    let side = side_for(&side_s).map_err(from_store)?;
    let size = parse_dec(&r.get::<_, String>("size")?, "size").map_err(from_store)?;
    let usdc_size =
        parse_dec(&r.get::<_, String>("usdc_size")?, "usdc_size").map_err(from_store)?;
    let price = parse_dec(&r.get::<_, String>("price")?, "price").map_err(from_store)?;
    Ok(SourceFill {
        event_key: r.get("event_key")?,
        wallet: r.get("wallet")?,
        timestamp: r.get("timestamp")?,
        condition_id: r.get("condition_id")?,
        token_id: r.get("token_id")?,
        side,
        size,
        usdc_size,
        price,
        // Per-share cash amount recomputed from persisted Decimals -
        // never stored, never fabricated (Phase-1.5: not the execution price).
        cash_implied_price: usdc_size / size,
        outcome_index: r.get("outcome_index")?,
        outcome: r.get("outcome")?,
        market_title: r.get("market_title")?,
        market_slug: r.get("market_slug")?,
        event_slug: r.get("event_slug")?,
        icon: r.get("icon")?,
        raw_json: r.get("raw_json")?,
    })
}

const FILL_COLUMNS: &str = "SELECT event_key, wallet, timestamp, condition_id, token_id, side,
    size, usdc_size, price, outcome_index, outcome,
    market_title, market_slug, event_slug, icon, raw_json FROM source_fills";

fn side_for(s: &str) -> Result<tokor_domain::activity::Side> {
    match s {
        "BUY" => Ok(tokor_domain::activity::Side::Buy),
        "SELL" => Ok(tokor_domain::activity::Side::Sell),
        _ => Err(crate::PersistError::Misuse(format!("unknown side {s}"))),
    }
}

/// Bridge PersistError into rusqlite's error type for query_map closures.
fn from_store(e: PersistError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}

fn parse_dec(s: &str, field: &'static str) -> Result<rust_decimal::Decimal> {
    s.parse::<rust_decimal::Decimal>()
        .map_err(|e| PersistError::Misuse(format!("stored {field} {s:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use serde_json::json;

    fn demo_fill(side: tokor_domain::activity::Side, ts: i64, size: f64) -> SourceFill {
        let raw = json!({
            "proxy_wallet": "0xa19cbababc312f9df185e49d7004c249ed1ade6b",
            "timestamp": ts,
            "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
            "type": "TRADE",
            "size": size,
            "usdc_size": 0.5 * size,
            "transaction_hash": format!("0xhash-{}-{:?}-{}", ts, side, size),
            "price": 0.5,
            "token_id": "0xtoken",
            "side": match side { Side::Buy => "BUY", Side::Sell => "SELL" },
            "outcome_index": 0,
            "title": "t",
            "slug": "s",
            "icon": "i",
            "event_slug": "e",
            "outcome": "Yes",
            "name": "Tokor",
            "pseudonym": "p"
        });
        let raw_json = serde_json::to_string(&raw).unwrap();
        let raw: SourceFillRaw = serde_json::from_value(raw).unwrap();
        SourceFill::from_raw(&raw, &raw_json).unwrap()
    }
    use tokor_domain::activity::{Side, SourceFillRaw};

    use super::PageRecord;

    fn demo_page(index: u64, cursor: Option<&str>, has_more: bool) -> PageRecord {
        PageRecord {
            run_id: "r".into(),
            page_index: index,
            fetched_at: "now".into(),
            http_status: 200,
            url: "u".into(),
            query_params: "q".into(),
            body_json: "{}".into(),
            next_cursor: cursor.map(str::to_owned),
            has_more,
        }
    }

    #[test]
    fn commit_page_atomic_cursor_and_fills() {
        let db = Database::open_memory().unwrap();
        db.start_run("r1", "{}", "fp").unwrap();
        let f1 = demo_fill(Side::Buy, 100, 10.0);
        let f2 = demo_fill(Side::Buy, 110, 20.0);
        let mut page = demo_page(0, Some("cur1"), true);
        page.run_id = "r1".into();
        page.body_json = "{\"data\":[]}".into();
        let ck = db.commit_page(&page, &[f1.clone(), f2.clone()]).unwrap();
        assert_eq!(ck.fills_inserted, 2);
        assert_eq!(ck.fills_already_seen, 0);
        let (cursor, pages) = db.load_cursor("fp").unwrap().unwrap();
        assert_eq!(cursor.as_deref(), Some("cur1"));
        assert_eq!(pages, 1);
        assert_eq!(db.fills_count().unwrap(), 2);
        let stats = db.fill_stats().unwrap();
        assert_eq!(stats.buys, 2);
    }

    #[test]
    fn duplicate_fills_ignored_on_replay() {
        let db = Database::open_memory().unwrap();
        db.start_run("r2", "{}", "fp2").unwrap();
        let f1 = demo_fill(Side::Sell, 200, 7.0);
        let mut p0 = demo_page(0, Some("c"), false);
        p0.run_id = "r2".into();
        let _ = db.commit_page(&p0, std::slice::from_ref(&f1)).unwrap();
        let mut p1 = demo_page(1, Some("c2"), false);
        p1.run_id = "r2".into();
        let ck = db.commit_page(&p1, std::slice::from_ref(&f1)).unwrap();
        assert_eq!(ck.fills_inserted, 0);
        assert_eq!(ck.fills_already_seen, 1);
        assert_eq!(db.fills_count().unwrap(), 1);
        let stats = db.fill_stats().unwrap();
        assert_eq!(stats.sells, 1);
    }

    #[test]
    fn resume_cursor_after_restart() {
        // simulate restart: reopen database (cursor must survive)
        let path = std::env::temp_dir().join("tokor-restart-test.db");
        let _ = std::fs::remove_file(&path);
        {
            let db = Database::open(&path).unwrap();
            db.start_run("r3", "{}", "fp3").unwrap();
            let mut f = demo_fill(Side::Buy, 300, 2.0);
            f.raw_json = f.raw_json.replace("\"timestamp\":300", "\"timestamp\":301");
            let mut page = demo_page(0, Some("cursorX"), true);
            page.run_id = "r3".into();
            let _ = db.commit_page(&page, &[f.clone()]).unwrap();
            db.finish_run("r3", "COMPLETE", Some("cursorX")).unwrap();
        }
        let db2 = Database::open(&path).unwrap();
        let (cursor, _) = db2.load_cursor("fp3").unwrap().unwrap();
        assert_eq!(cursor.as_deref(), Some("cursorX"));
        assert!(matches!(db2.fills_count().unwrap(), 1));
        let _ = std::fs::remove_file(&path);
    }
}
