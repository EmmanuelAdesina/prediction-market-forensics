//! Market metadata + classification persistence.

use chrono::Utc;
use tokor_domain::classification::{ClassifierEvidence, CryptoVerdict};
use tokor_domain::market::MarketRef;

use crate::{Database, PersistError, Result};

pub trait MarketRepository {
    /// Upsert a market reference (activity-derived); never overwrites CLOB/Gamma
    /// enrichment columns.
    fn upsert_market_ref(&self, m: &MarketRef) -> Result<()>;

    /// Upsert the full CLOB metadata payload, optionally the Gamma payload,
    /// atomically. Normalized columns are COALESCE'd so re-runs never erase.
    #[allow(clippy::too_many_arguments)]
    fn upsert_market_enriched(
        &self,
        condition_id: &str,
        title: Option<&str>,
        market_slug: Option<&str>,
        event_slug: Option<&str>,
        icon: Option<&str>,
        tags: Option<&[String]>,
        description: Option<&str>,
        active: Option<bool>,
        closed: Option<bool>,
        minimum_tick_size: Option<&str>,
        minimum_order_size: Option<&str>,
        neg_risk: Option<bool>,
        clob_meta_json: &str,
        gamma_meta_json: Option<&str>,
    ) -> Result<()>;

    /// Attach an outcome token to a market.
    fn upsert_token(&self, condition_id: &str, token_id: &str, outcome: Option<&str>)
        -> Result<()>;

    /// Condition ids with no CLOB enrichment yet (optionally bounded).
    fn condition_ids_needing_enrichment(&self, limit: Option<u64>) -> Result<Vec<String>>;

    /// Record a classification decision + evidence.
    fn record_classification(&self, condition_id: &str, ev: &ClassifierEvidence) -> Result<()>;

    /// Condition ids that exist in fills but have CLOB metadata (populated by
    /// activity normalization) yet lack a classification.
    fn condition_ids_missing_classification(&self) -> Result<Vec<String>>;

    /// Fetch a market reference.
    fn market_ref(&self, condition_id: &str) -> Result<Option<MarketRef>>;

    /// All markets with classifications, for reporting.
    fn classified_markets(&self) -> Result<Vec<(String, CryptoVerdict, String)>>;
}

impl MarketRepository for Database {
    fn upsert_market_ref(&self, m: &MarketRef) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO markets
                (condition_id, title, market_slug, event_slug, icon)
             VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(condition_id) DO UPDATE SET
                title = COALESCE(markets.title, excluded.title),
                market_slug = COALESCE(markets.market_slug, excluded.market_slug),
                event_slug = COALESCE(markets.event_slug, excluded.event_slug),
                icon = COALESCE(markets.icon, excluded.icon)",
            rusqlite::params![m.condition_id, m.title, m.market_slug, m.event_slug, m.icon,],
        )?;
        if let Some(tokens) = &m.tokens {
            for (token_id, outcome) in tokens {
                tx.execute(
                    "INSERT OR IGNORE INTO market_tokens (token_id, condition_id, outcome)
                     VALUES (?1,?2,?3)",
                    rusqlite::params![token_id, m.condition_id, outcome],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn upsert_market_enriched(
        &self,
        condition_id: &str,
        title: Option<&str>,
        market_slug: Option<&str>,
        event_slug: Option<&str>,
        icon: Option<&str>,
        tags: Option<&[String]>,
        description: Option<&str>,
        active: Option<bool>,
        closed: Option<bool>,
        minimum_tick_size: Option<&str>,
        minimum_order_size: Option<&str>,
        neg_risk: Option<bool>,
        clob_meta_json: &str,
        gamma_meta_json: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.inner_w();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO markets (condition_id) VALUES (?1)
             ON CONFLICT(condition_id) DO NOTHING",
            [condition_id],
        )?;
        tx.execute(
            "UPDATE markets SET
                title = COALESCE(?2, title),
                market_slug = COALESCE(?3, market_slug),
                event_slug = COALESCE(?4, event_slug),
                icon = COALESCE(?5, icon),
                tags_json = COALESCE(?6, tags_json),
                description = COALESCE(?7, description),
                active = COALESCE(?8, active),
                closed = COALESCE(?9, closed),
                minimum_tick_size = COALESCE(?10, minimum_tick_size),
                minimum_order_size = COALESCE(?11, minimum_order_size),
                neg_risk = COALESCE(?12, neg_risk),
                clob_meta_json = ?13,
                gamma_meta_json = COALESCE(?14, gamma_meta_json),
                enriched_at = ?15
             WHERE condition_id = ?1",
            rusqlite::params![
                condition_id,
                title,
                market_slug,
                event_slug,
                icon,
                tags.map(|t| serde_json::to_string(t).unwrap_or_default()),
                description,
                active.map(|b| b as i64),
                closed.map(|b| b as i64),
                minimum_tick_size,
                minimum_order_size,
                neg_risk.map(|b| b as i64),
                clob_meta_json,
                gamma_meta_json,
                Utc::now().to_rfc3339(),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn upsert_token(
        &self,
        condition_id: &str,
        token_id: &str,
        outcome: Option<&str>,
    ) -> Result<()> {
        let conn = self.inner_w();
        conn.execute(
            "INSERT INTO market_tokens (token_id, condition_id, outcome)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(token_id) DO UPDATE SET
               condition_id = excluded.condition_id,
               outcome = COALESCE(excluded.outcome, market_tokens.outcome)",
            rusqlite::params![token_id, condition_id, outcome],
        )?;
        Ok(())
    }

    fn condition_ids_needing_enrichment(&self, limit: Option<u64>) -> Result<Vec<String>> {
        let conn = self.inner_w();
        let sql = match limit {
            Some(l) => format!(
                "SELECT condition_id FROM markets
                 WHERE clob_meta_json IS NULL ORDER BY condition_id LIMIT {l}"
            ),
            None => String::from(
                "SELECT condition_id FROM markets
                 WHERE clob_meta_json IS NULL ORDER BY condition_id",
            ),
        };
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn record_classification(&self, condition_id: &str, ev: &ClassifierEvidence) -> Result<()> {
        let mut conn = self.inner_w();
        let verdict = match ev.verdict {
            CryptoVerdict::Crypto => "CRYPTO",
            CryptoVerdict::NonCrypto => "NON_CRYPTO",
            CryptoVerdict::Unknown => "UNKNOWN",
        };
        let evidence = serde_json::to_string(ev)
            .map_err(|e| PersistError::Misuse(format!("evidence serialize: {e}")))?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE markets SET class_verdict = ?2, class_evidence_json = ?3,
                    class_version = ?4, classified_at = ?5
             WHERE condition_id = ?1",
            rusqlite::params![
                condition_id,
                verdict,
                evidence,
                ev.classifier_version,
                Utc::now().to_rfc3339()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn condition_ids_missing_classification(&self) -> Result<Vec<String>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT condition_id FROM markets
             WHERE class_verdict IS NULL ORDER BY condition_id",
        )?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn market_ref(&self, condition_id: &str) -> Result<Option<MarketRef>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT condition_id, title, market_slug, event_slug, icon, tags_json, description,
                    active, closed, minimum_tick_size, minimum_order_size, neg_risk
             FROM markets WHERE condition_id = ?1",
        )?;
        let mut rows = stmt.query([condition_id])?;
        match rows.next()? {
            Some(row) => Ok(Some(row_to_ref(row)?)),
            None => Ok(None),
        }
    }

    fn classified_markets(&self) -> Result<Vec<(String, CryptoVerdict, String)>> {
        let conn = self.inner_w();
        let mut stmt = conn.prepare(
            "SELECT condition_id, class_verdict, class_version FROM markets
             WHERE class_verdict IS NOT NULL ORDER BY condition_id",
        )?;
        let rows = stmt.query_map([], |r| {
            let cid: String = r.get(0)?;
            let verdict: String = r.get(1)?;
            let version: String = r.get(2)?;
            Ok((cid, verdict, version))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (cid, verdict, version) = row?;
            let v = match verdict.as_str() {
                "CRYPTO" => CryptoVerdict::Crypto,
                "NON_CRYPTO" => CryptoVerdict::NonCrypto,
                _ => CryptoVerdict::Unknown,
            };
            out.push((cid, v, version));
        }
        Ok(out)
    }
}

fn row_to_ref(row: &rusqlite::Row) -> rusqlite::Result<MarketRef> {
    let tags_json: Option<String> = row.get(5)?;
    Ok(MarketRef {
        condition_id: row.get(0)?,
        title: row.get(1)?,
        market_slug: row.get(2)?,
        event_slug: row.get(3)?,
        icon: row.get(4)?,
        tags: tags_json
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok()),
        description: row.get(6)?,
        tokens: None,
        active: row.get::<_, Option<i64>>(7)?.map(|v| v != 0),
        closed: row.get::<_, Option<i64>>(8)?.map(|v| v != 0),
        minimum_tick_size: row.get(9)?,
        minimum_order_size: row.get(10)?,
        neg_risk: row.get::<_, Option<i64>>(11)?.map(|v| v != 0),
    })
}
