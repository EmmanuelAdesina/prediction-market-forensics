//! Persistence of reconstructed positions and reconciliation runs against
//! the public `/v2/positions` endpoint.

use chrono::Utc;
use tokor_domain::position::PositionState;

use crate::{Database, Result};

/// One reconciliation comparison (ours vs API).
pub struct ReconObservation {
    pub run_id: String,
    pub token_id: String,
    pub condition_id: String,
    /// From /v2/positions (stringified decimals, may be None if absent).
    pub api_size: Option<String>,
    pub api_avg_price: Option<String>,
    pub api_entry_cost: Option<String>,
    pub api_entry_fees: Option<String>,
    pub api_realized: Option<String>,
    pub api_unrealized: Option<String>,
    pub ours_size: Option<String>,
    pub ours_avg_price: Option<String>,
    pub ours_realized: Option<String>,
    /// Absolute decimal deltas as strings where computable.
    pub delta_size: Option<String>,
    pub delta_avg: Option<String>,
    pub delta_realized: Option<String>,
    pub verdict: &'static str,
    pub notes_json: String,
}

pub trait ReconRepository {
    /// Persist a reconstructed position snapshot (replacement semantics).
    fn save_reconstructed_position(
        &self,
        token_id: &str,
        condition_id: &str,
        pos: &PositionState,
    ) -> Result<()>;

    /// Fetch a reconstructed position for comparison.
    fn load_reconstructed_position(&self, token_id: &str) -> Result<Option<PositionSnapshot>>;

    /// Persist a reconciliation observation.
    fn save_recon_observation(&self, obs: &ReconObservation) -> Result<()>;

    /// Latest reconciliation observations (for reporting).
    fn load_recon_observations(&self, run_id: &str) -> Result<Vec<StoredRecon>>;

    /// Count observations per verdict for a run.
    fn recon_verdict_counts(&self, run_id: &str) -> Result<Vec<(String, i64)>>;
}

#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    pub token_id: String,
    pub condition_id: String,
    pub remaining_inventory: String,
    pub avg_open_price: Option<String>,
    pub realized_pnl: String,
    pub disposed_shares: String,
    pub unattributed_disposals: String,
    pub history_probably_incomplete: bool,
    pub fills_applied: u64,
}

#[derive(Debug, Clone)]
pub struct StoredRecon {
    pub token_id: String,
    pub condition_id: String,
    pub verdict: String,
    pub notes_json: String,
}

impl ReconRepository for Database {
    fn save_reconstructed_position(
        &self,
        token_id: &str,
        condition_id: &str,
        pos: &PositionState,
    ) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO positions_reconstructed
             (token_id, condition_id, acquired_shares, disposed_shares,
              unattributed_disposals, remaining_inventory, avg_open_price,
              remaining_cost_basis, realized_pnl, fills_applied,
              history_probably_incomplete, computed_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            rusqlite::params![
                token_id,
                condition_id,
                pos.acquired_shares().to_string(),
                pos.disposed_shares.to_string(),
                pos.unattributed_disposals.to_string(),
                pos.remaining_inventory().to_string(),
                pos.avg_open_price().map(|d| d.to_string()),
                pos.remaining_cost_basis().to_string(),
                pos.realized_pnl().to_string(),
                pos.fills_applied as i64,
                if pos.history_probably_incomplete { 1 } else { 0 },
                Utc::now().to_rfc3339(),
            ],
        )?;
        // Lot audit trail: replace the remaining lot set.
        tx.execute("DELETE FROM position_lots_remaining WHERE token_id = ?1", [
            token_id
        ])?;
        for lot in &pos.lots {
            tx.execute(
                "INSERT OR REPLACE INTO position_lots_remaining
                 (token_id, opened_ts, event_key, shares_remaining, cost_per_share)
                 VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    token_id,
                    lot.opened_ts,
                    lot.event_key,
                    lot.shares_remaining.to_string(),
                    lot.cost_per_share.to_string(),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn load_reconstructed_position(&self, token_id: &str) -> Result<Option<PositionSnapshot>> {
        let conn = self.inner_w();
        let mut stmt = conn
            .prepare("SELECT token_id, condition_id, remaining_inventory, avg_open_price,
                             realized_pnl, disposed_shares, unattributed_disposals,
                             history_probably_incomplete, fills_applied
                      FROM positions_reconstructed WHERE token_id = ?1")?;
        let mut rows = stmt.query([token_id])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(PositionSnapshot {
                token_id: row.get(0)?,
                condition_id: row.get(1)?,
                remaining_inventory: row.get(2)?,
                avg_open_price: row.get(3)?,
                realized_pnl: row.get(4)?,
                disposed_shares: row.get(5)?,
                unattributed_disposals: row.get(6)?,
                history_probably_incomplete: row.get::<_, i64>(7)? != 0,
                fills_applied: row.get::<_, i64>(8)? as u64,
            }));
        }
        Ok(None)
    }

    fn save_recon_observation(&self, obs: &ReconObservation) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO position_reconciliations
             (run_id, token_id, condition_id, api_size, api_avg_price, api_entry_cost,
              api_entry_fees, api_realized, api_unrealized, ours_size, ours_avg_price,
              ours_realized, delta_size, delta_avg, delta_realized, verdict,
              notes_json, computed_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
            rusqlite::params![
                obs.run_id,
                obs.token_id,
                obs.condition_id,
                obs.api_size,
                obs.api_avg_price,
                obs.api_entry_cost,
                obs.api_entry_fees,
                obs.api_realized,
                obs.api_unrealized,
                obs.ours_size,
                obs.ours_avg_price,
                obs.ours_realized,
                obs.delta_size,
                obs.delta_avg,
                obs.delta_realized,
                obs.verdict,
                obs.notes_json,
                Utc::now().to_rfc3339(),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn load_recon_observations(&self, run_id: &str) -> Result<Vec<StoredRecon>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT token_id, condition_id, verdict, notes_json
             FROM position_reconciliations
             WHERE run_id = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map([run_id], |r| {
            Ok(StoredRecon {
                token_id: r.get(0)?,
                condition_id: r.get(1)?,
                verdict: r.get(2)?,
                notes_json: r.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn recon_verdict_counts(&self, run_id: &str) -> Result<Vec<(String, i64)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT verdict, COUNT(*) FROM position_reconciliations
             WHERE run_id = ?1 GROUP BY verdict",
        )?;
        let rows = stmt.query_map([run_id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}
