//! Operator reporting queries (read-only summaries).

use crate::{Database, Result};

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub status: String,
    pub pages_committed: i64,
    pub fills_committed: i64,
    pub last_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionCounts {
    pub total: i64,
    pub open_inventory: i64,
}

pub trait ReportRepository {
    /// Most recent acquisition run (any kind), by start time.
    fn latest_run(&self) -> Result<Option<RunSummary>>;
    /// Reconstructed-position counts.
    fn position_counts(&self) -> Result<PositionCounts>;
    /// run_id of the most recent reconciliation, if any.
    fn last_recon_run_id(&self) -> Result<Option<String>>;
}

impl ReportRepository for Database {
    fn latest_run(&self) -> Result<Option<RunSummary>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT run_id, status, pages_committed, fills_committed, last_cursor
             FROM acquisition_runs ORDER BY started_at DESC LIMIT 1",
        )?;
        let mut rows = stmt.query([])?;
        Ok(rows
            .next()?
            .map(|r| -> std::result::Result<RunSummary, rusqlite::Error> {
                Ok(RunSummary {
                    run_id: r.get(0)?,
                    status: r.get(1)?,
                    pages_committed: r.get(2)?,
                    fills_committed: r.get(3)?,
                    last_cursor: r.get(4)?,
                })
            })
            .transpose()?)
    }

    fn position_counts(&self) -> Result<PositionCounts> {
        let conn = self.inner_w();
        let total: i64 =
            conn.query_row("SELECT COUNT(*) FROM positions_reconstructed", [], |r| {
                r.get(0)
            })?;
        // remaining_inventory is stored as a string decimal; SQL can't compare
        // reliably — fetch and compare at the decimal level here.
        let mut stmt = conn.prepare(
            "SELECT remaining_inventory FROM positions_reconstructed WHERE remaining_inventory NOT IN ('0')",
        )?;
        let mut open = 0i64;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let s: String = row.get(0)?;
            if s.parse::<rust_decimal::Decimal>()
                .map(|d| d > rust_decimal::Decimal::ZERO)
                .unwrap_or(false)
            {
                open += 1;
            }
        }
        Ok(PositionCounts {
            total,
            open_inventory: open,
        })
    }

    fn last_recon_run_id(&self) -> Result<Option<String>> {
        let conn = self.inner_w();
        let mut stmt =
            conn.prepare("SELECT run_id FROM position_reconciliations ORDER BY id DESC LIMIT 1")?;
        let mut rows = stmt.query([])?;
        Ok(rows
            .next()?
            .map(|r| -> std::result::Result<String, rusqlite::Error> { r.get(0) })
            .transpose()?)
    }
}
