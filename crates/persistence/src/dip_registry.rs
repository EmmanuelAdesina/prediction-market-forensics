//! Dip-market registry persistence.
//!
//! The registry is append-and-annotate: normalizing the same Gamma payload
//! twice is a no-op (`Unchanged`); metadata drift appends a field-level
//! history row and bumps `metadata_version`. Nothing is ever overwritten
//! destructively, and rejected/ambiguous candidates are retained with their
//! reason so every sync report is reproducible from the database alone.

use crate::{Database, PersistError, Result};
use rusqlite::params;
use tokor_domain::dip::{
    Asset, DipMarket, FeeInfo, MarketStatus, RejectionReason, ResolutionStatus,
    ResolutionVerification,
};

/// Result of upserting one normalized market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// First time this condition id entered the registry.
    New,
    /// Existed; `n` fields changed (history row appended, version bumped).
    Updated(usize),
    /// Existed; metadata identical.
    Unchanged,
}

/// One field-level change recorded in history.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FieldChange {
    pub field: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

/// Aggregate counters for one sync invocation.
#[derive(Debug, Clone, Default)]
pub struct SyncCounts {
    pub pages_fetched: u64,
    pub events_seen: u64,
    pub markets_seen: u64,
    pub discovered: u64,
    pub new_count: u64,
    pub updated_count: u64,
    pub unchanged_count: u64,
    pub rejected_count: u64,
    pub resolved_retained: u64,
    pub out_of_scope: u64,
    pub rejections: std::collections::BTreeMap<String, u64>,
    pub errors: Vec<String>,
}

pub trait DipRegistryRepository {
    /// Upsert one normalized market with diffing + history. Idempotent.
    fn upsert_dip_market(&self, m: &DipMarket) -> Result<UpsertOutcome>;

    /// List registry rows, optionally filtered. Ordered by asset, barrier.
    fn list_dip_markets(
        &self,
        asset: Option<Asset>,
        eligible_only: bool,
        limit: u64,
    ) -> Result<Vec<DipMarket>>;

    /// (total rows, eligible rows).
    fn count_dip_markets(&self) -> Result<(u64, u64)>;

    /// Resolved rows retained for backtesting.
    fn count_resolved(&self) -> Result<u64>;

    fn start_sync_run(&self, run_id: &str, universe: &str, assets: &str) -> Result<()>;

    fn finish_sync_run(&self, run_id: &str, counts: &SyncCounts, status: &str) -> Result<()>;

    /// Number of history rows for a condition id (test/audit accessor).
    fn history_len(&self, condition_id: &str) -> Result<u64>;

    /// Current metadata_version for a condition id (0 when absent).
    fn metadata_version(&self, condition_id: &str) -> Result<u64>;
}

fn ts(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339()
}

fn opt_ts(t: &Option<chrono::DateTime<chrono::Utc>>) -> Option<String> {
    t.as_ref().map(ts)
}

fn status_str(s: MarketStatus) -> &'static str {
    match s {
        MarketStatus::Active => "ACTIVE",
        MarketStatus::ResolvedYes => "RESOLVED_YES",
        MarketStatus::ResolvedNo => "RESOLVED_NO",
        MarketStatus::ResolvedUnknown => "RESOLVED_UNKNOWN",
    }
}

fn status_from_str(s: &str) -> Option<MarketStatus> {
    Some(match s {
        "ACTIVE" => MarketStatus::Active,
        "RESOLVED_YES" => MarketStatus::ResolvedYes,
        "RESOLVED_NO" => MarketStatus::ResolvedNo,
        "RESOLVED_UNKNOWN" => MarketStatus::ResolvedUnknown,
        _ => return None,
    })
}

fn resolution_status_str(s: ResolutionStatus) -> &'static str {
    match s {
        ResolutionStatus::Verified => "VERIFIED",
        ResolutionStatus::Ambiguous => "AMBIGUOUS",
        ResolutionStatus::Rejected => "REJECTED",
        ResolutionStatus::Unverified => "UNVERIFIED",
    }
}

/// Rejection reason recorded for registry rows that are not eligible.
/// (Structural rejections - missing tokens, unparseable barrier - never
/// reach this table; they are counted in the sync run ledger.)
fn reject_reason_for(m: &DipMarket) -> Option<String> {
    if m.is_eligible() {
        return None;
    }
    let reason = match m.resolution_verification.status {
        ResolutionStatus::Ambiguous | ResolutionStatus::Rejected | ResolutionStatus::Unverified => {
            RejectionReason::UnverifiedResolution
        }
        // Verified but mechanically ineligible: only barrier <= 0 possible.
        ResolutionStatus::Verified => RejectionReason::InvalidBarrier,
    };
    Some(reason.as_str().to_string())
}

/// Flat string projection of a row - the unit of diffing.
struct RowProjection {
    gamma_market_id: Option<String>,
    asset: String,
    question: String,
    event_slug: Option<String>,
    event_title: Option<String>,
    resolution_rule: String,
    barrier_price: String,
    observation_end: Option<String>,
    end_date: String,
    oracle_source: Option<String>,
    yes_token_id: String,
    no_token_id: String,
    tick_size: String,
    min_order_size: String,
    fee_type: Option<String>,
    fees_enabled: Option<String>,
    maker_base_fee_bps: Option<String>,
    taker_base_fee_bps: Option<String>,
    neg_risk: Option<String>,
    status: String,
    resolution_status: String,
    verifier_version: String,
    /// Full verification bundle - the stored form (includes `checked_at`).
    verification_json: String,
    /// Canonical verification form with `checked_at` normalized away - the
    /// diff form. Two runs verifying identical semantics must NOT count as a
    /// metadata change (idempotency); only semantic drift does.
    verification_semantic: String,
}

/// Canonical diff form of a verification bundle.
fn semantic_verification_json(v: &ResolutionVerification) -> String {
    let mut copy = v.clone();
    copy.checked_at = chrono::DateTime::<chrono::Utc>::UNIX_EPOCH;
    serde_json::to_string(&copy).unwrap_or_else(|_| "{\"error\":\"serialize\"}".to_string())
}

fn project(m: &DipMarket) -> RowProjection {
    let fees_enabled = m.fees.fees_enabled;
    let verification_json = serde_json::to_string(&m.resolution_verification)
        .unwrap_or_else(|_| "{{\"error\":\"serialize\"}}".to_string());
    RowProjection {
        gamma_market_id: m.gamma_market_id.clone(),
        asset: m.asset.as_str().to_string(),
        question: m.question.clone(),
        event_slug: m.event_slug.clone(),
        event_title: m.event_title.clone(),
        resolution_rule: m.resolution_rule.clone(),
        barrier_price: m.barrier_price.to_string(),
        observation_end: opt_ts(&m.observation_end),
        end_date: ts(&m.end_date),
        oracle_source: m.oracle_source.clone(),
        yes_token_id: m.yes_token_id.clone(),
        no_token_id: m.no_token_id.clone(),
        tick_size: m.tick_size.to_string(),
        min_order_size: m.min_order_size.to_string(),
        fee_type: m.fees.fee_type.clone(),
        fees_enabled: fees_enabled.map(|b| b.to_string()),
        maker_base_fee_bps: m.fees.maker_base_fee_bps.map(|d| d.to_string()),
        taker_base_fee_bps: m.fees.taker_base_fee_bps.map(|d| d.to_string()),
        neg_risk: m.neg_risk.map(|b| b.to_string()),
        status: status_str(m.status).to_string(),
        resolution_status: resolution_status_str(m.resolution_verification.status).to_string(),
        verifier_version: m.resolution_verification.verifier_version.clone(),
        verification_json,
        verification_semantic: semantic_verification_json(&m.resolution_verification),
    }
}

const PROJECTION_FIELDS: &[&str] = &[
    "gamma_market_id",
    "asset",
    "question",
    "event_slug",
    "event_title",
    "resolution_rule",
    "barrier_price",
    "observation_end",
    "end_date",
    "oracle_source",
    "yes_token_id",
    "no_token_id",
    "tick_size",
    "min_order_size",
    "fee_type",
    "fees_enabled",
    "maker_base_fee_bps",
    "taker_base_fee_bps",
    "neg_risk",
    "status",
    "resolution_status",
    "verifier_version",
];

/// Read a stored row back into a projection for diffing.
fn read_projection(conn: &rusqlite::Connection, condition_id: &str) -> Result<RowProjection> {
    conn.query_row(
        "SELECT gamma_market_id, asset, question, event_slug, event_title, resolution_rule,
                barrier_price, observation_end, end_date, oracle_source,
                yes_token_id, no_token_id, tick_size, min_order_size,
                fee_type, fees_enabled, maker_base_fee_bps, taker_base_fee_bps, neg_risk,
                status, resolution_status, verifier_version, verification_json
         FROM dip_markets WHERE condition_id = ?1",
        params![condition_id],
        |r| {
            let fee_enabled_i: Option<i64> = r.get(15)?;
            Ok(RowProjection {
                gamma_market_id: r.get(0)?,
                asset: r.get(1)?,
                question: r.get(2)?,
                event_slug: r.get(3)?,
                event_title: r.get(4)?,
                resolution_rule: r.get(5)?,
                barrier_price: r.get(6)?,
                observation_end: r.get(7)?,
                end_date: r.get(8)?,
                oracle_source: r.get(9)?,
                yes_token_id: r.get(10)?,
                no_token_id: r.get(11)?,
                tick_size: r.get(12)?,
                min_order_size: r.get(13)?,
                fee_type: r.get(14)?,
                fees_enabled: fee_enabled_i.map(|b| (b != 0).to_string()),
                maker_base_fee_bps: r.get(16)?,
                taker_base_fee_bps: r.get(17)?,
                neg_risk: r.get::<_, Option<i64>>(18)?.map(|b| (b != 0).to_string()),
                status: r.get(19)?,
                resolution_status: r.get(20)?,
                verifier_version: r.get(21)?,
                verification_semantic: {
                    // Recompute the diff form from the stored bundle. A
                    // bundle that fails to parse yields its raw form, which
                    // cannot equal any freshly-serialized semantic form -
                    // the next sync repairs the row (self-healing).
                    match serde_json::from_str::<ResolutionVerification>(&r.get::<_, String>(22)?) {
                        Ok(v) => semantic_verification_json(&v),
                        Err(_) => r.get::<_, String>(22)?,
                    }
                },
                verification_json: r.get(22)?,
            })
        },
    )
    .map_err(PersistError::from)
}

/// Serialize one projection field for storage (Option<T> → nullable column).
fn projection_value(p: &RowProjection, field: &str) -> Option<String> {
    match field {
        "gamma_market_id" => p.gamma_market_id.clone(),
        "asset" => Some(p.asset.clone()),
        "question" => Some(p.question.clone()),
        "event_slug" => p.event_slug.clone(),
        "event_title" => p.event_title.clone(),
        "resolution_rule" => Some(p.resolution_rule.clone()),
        "barrier_price" => Some(p.barrier_price.clone()),
        "observation_end" => p.observation_end.clone(),
        "end_date" => Some(p.end_date.clone()),
        "oracle_source" => p.oracle_source.clone(),
        "yes_token_id" => Some(p.yes_token_id.clone()),
        "no_token_id" => Some(p.no_token_id.clone()),
        "tick_size" => Some(p.tick_size.clone()),
        "min_order_size" => Some(p.min_order_size.clone()),
        "fee_type" => p.fee_type.clone(),
        "fees_enabled" => p.fees_enabled.clone(),
        "maker_base_fee_bps" => p.maker_base_fee_bps.clone(),
        "taker_base_fee_bps" => p.taker_base_fee_bps.clone(),
        "neg_risk" => p.neg_risk.clone(),
        "status" => Some(p.status.clone()),
        "resolution_status" => Some(p.resolution_status.clone()),
        "verifier_version" => Some(p.verifier_version.clone()),
        "verification_json" => Some(p.verification_json.clone()),
        other => unreachable!("unknown projection field: {other}"),
    }
}

impl DipRegistryRepository for Database {
    fn upsert_dip_market(&self, m: &DipMarket) -> Result<UpsertOutcome> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let new = project(m);
        let now = ts(&chrono::Utc::now());

        let exists: bool = match conn.query_row(
            "SELECT 1 FROM dip_markets WHERE condition_id = ?1",
            params![m.condition_id],
            |_| Ok(true),
        ) {
            Ok(_) => true,
            Err(rusqlite::Error::QueryReturnedNoRows) => false,
            Err(e) => return Err(e.into()),
        };

        if !exists {
            // Projection columns are ?1..?23; fixed columns ?24..?31 below.
            let eligible = m.is_eligible();
            conn.execute(
                "INSERT INTO dip_markets (
                    condition_id, gamma_market_id, asset, question, event_slug, event_title,
                    resolution_rule, barrier_price, observation_end, end_date, oracle_source,
                    yes_token_id, no_token_id, tick_size, min_order_size, fee_type, fees_enabled,
                    maker_base_fee_bps, taker_base_fee_bps, neg_risk, status, eligible,
                    reject_reason, source_provenance, resolution_status, verification_json,
                    verifier_version, discovered_at, last_verified_at, last_synced_at,
                    metadata_version
                ) VALUES (
                    ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,
                    ?22,?23,?24,?25,?26,?27,?28,?29,?30,?31
                )",
                params![
                    m.condition_id,
                    new.gamma_market_id,
                    new.asset,
                    new.question,
                    new.event_slug,
                    new.event_title,
                    new.resolution_rule,
                    new.barrier_price,
                    new.observation_end,
                    new.end_date,
                    new.oracle_source,
                    new.yes_token_id,
                    new.no_token_id,
                    new.tick_size,
                    new.min_order_size,
                    new.fee_type,
                    m.fees.fees_enabled.map(|b| b as i64),
                    new.maker_base_fee_bps,
                    new.taker_base_fee_bps,
                    m.neg_risk.map(|b| b as i64),
                    new.status,
                    eligible as i64,
                    reject_reason_for(m),
                    m.source_provenance,
                    new.resolution_status,
                    new.verification_json,
                    new.verifier_version,
                    ts(&m.discovered_at),
                    ts(&m.last_verified_at),
                    now,
                    m.metadata_version as i64,
                ],
            )?;
            return Ok(UpsertOutcome::New);
        }

        // ---- diff stored vs incoming ---------------------------------------
        let stored = read_projection(&conn, &m.condition_id)?;
        let mut changes: Vec<FieldChange> = PROJECTION_FIELDS
            .iter()
            .filter_map(|field| {
                let before = projection_value(&stored, field);
                let after = projection_value(&new, field);
                if before != after {
                    Some(FieldChange {
                        field: field.to_string(),
                        before,
                        after,
                    })
                } else {
                    None
                }
            })
            .collect();

        // Verification bundles diff on SEMANTIC content only (checked_at is a
        // computation timestamp and churns on every normalization). A semantic
        // change rewrites the full stored bundle with the fresh timestamp and
        // lands in history as before/after full JSON.
        if stored.verification_semantic != new.verification_semantic {
            changes.push(FieldChange {
                field: "verification_json".to_string(),
                before: Some(stored.verification_json.clone()),
                after: Some(new.verification_json.clone()),
            });
        }

        if changes.is_empty() {
            // Refresh sync bookkeeping only; metadata_version is untouched.
            conn.execute(
                "UPDATE dip_markets SET last_synced_at = ?1 WHERE condition_id = ?2",
                params![now, m.condition_id],
            )?;
            return Ok(UpsertOutcome::Unchanged);
        }

        let changed_names: Vec<&str> = changes.iter().map(|c| c.field.as_str()).collect();
        let sets: Vec<String> = changed_names
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{name} = ?{}", i + 1))
            .collect();
        let sql = format!(
            "UPDATE dip_markets SET {}, last_synced_at = ?{}, metadata_version = metadata_version + 1 WHERE condition_id = ?{}",
            sets.join(", "),
            changed_names.len() + 1,
            changed_names.len() + 2
        );

        let mut bind_values: Vec<Option<String>> = changed_names
            .iter()
            .map(|f| projection_value(&new, f))
            .collect();
        bind_values.push(Some(now.clone()));

        let mut stmt = conn.prepare(&sql)?;
        {
            use rusqlite::ToSql;
            let params_slice: Vec<&dyn ToSql> = {
                let mut ps: Vec<&dyn ToSql> = bind_values.iter().map(|v| v as &dyn ToSql).collect();
                ps.push(&m.condition_id);
                ps
            };
            stmt.execute(params_slice.as_slice())?;
        }

        // Append history.
        let changes_json = serde_json::to_string(&changes)
            .map_err(|e| PersistError::Misuse(format!("serialize changes: {e}")))?;
        conn.execute(
            "INSERT INTO dip_market_history (condition_id, changed_at, metadata_version, changes_json)
             SELECT ?1, ?2, metadata_version, ?3 FROM dip_markets WHERE condition_id = ?1",
            params![m.condition_id, now, changes_json],
        )
        ?;

        // Eligibility flag re-derived after metadata change.
        conn.execute(
            "UPDATE dip_markets SET eligible = ?1,
                reject_reason = ?2
             WHERE condition_id = ?3",
            params![m.is_eligible() as i64, reject_reason_for(m), m.condition_id],
        )?;

        Ok(UpsertOutcome::Updated(changes.len()))
    }

    fn list_dip_markets(
        &self,
        asset: Option<Asset>,
        eligible_only: bool,
        limit: u64,
    ) -> Result<Vec<DipMarket>> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let mut sql = String::from(
            "SELECT condition_id, gamma_market_id, asset, question, event_slug, event_title, \
             resolution_rule, barrier_price, observation_end, end_date, oracle_source, \
             yes_token_id, no_token_id, tick_size, min_order_size, fee_type, fees_enabled, \
             maker_base_fee_bps, taker_base_fee_bps, neg_risk, status, source_provenance, \
             discovered_at, last_verified_at, metadata_version, verification_json \
             FROM dip_markets WHERE 1=1",
        );
        if asset.is_some() {
            sql.push_str(" AND asset = ?1");
        }
        if eligible_only {
            sql.push_str(" AND eligible = 1");
        }
        sql.push_str(" ORDER BY asset, barrier_price LIMIT ");
        sql.push_str(&limit.to_string());

        let mut stmt = conn.prepare(&sql)?;
        let map_row = |r: &rusqlite::Row| -> rusqlite::Result<DipMarket> {
            let asset_s: String = r.get(2)?;
            let asset = match asset_s.as_str() {
                "BTC" => Asset::Btc,
                "ETH" => Asset::Eth,
                other => {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        2,
                        rusqlite::types::Type::Text,
                        format!("unknown asset {other}").into(),
                    ))
                }
            };
            let status_s: String = r.get(20)?;
            let status = status_from_str(&status_s).ok_or_else(|| {
                rusqlite::Error::FromSqlConversionFailure(
                    20,
                    rusqlite::types::Type::Text,
                    format!("unknown status {status_s}").into(),
                )
            })?;
            let verification_json: String = r.get(25)?;
            let resolution_verification: ResolutionVerification =
                serde_json::from_str(&verification_json).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        25,
                        rusqlite::types::Type::Text,
                        format!("verification json: {e}").into(),
                    )
                })?;
            let parse_dec = |v: String| -> rusqlite::Result<rust_decimal::Decimal> {
                v.parse::<rust_decimal::Decimal>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Text,
                        format!("decimal: {e}").into(),
                    )
                })
            };
            let parse_ts_col = |v: String| -> rusqlite::Result<chrono::DateTime<chrono::Utc>> {
                chrono::DateTime::parse_from_rfc3339(&v)
                    .map(|t| t.with_timezone(&chrono::Utc))
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            format!("timestamp: {e}").into(),
                        )
                    })
            };
            let observation_end: Option<String> = r.get(8)?;
            let fees_i: Option<i64> = r.get(16)?;
            Ok(DipMarket {
                condition_id: r.get(0)?,
                gamma_market_id: r.get(1)?,
                asset,
                barrier_price: parse_dec(r.get(7)?)?,
                observation_end: observation_end.map(parse_ts_col).transpose()?,
                end_date: parse_ts_col(r.get(9)?)?,
                yes_token_id: r.get(11)?,
                no_token_id: r.get(12)?,
                question: r.get(3)?,
                event_slug: r.get(4)?,
                event_title: r.get(5)?,
                resolution_rule: r.get(6).unwrap_or_default(),
                oracle_source: r.get(10).unwrap_or_default(),
                tick_size: parse_dec(r.get(13)?)?,
                min_order_size: parse_dec(r.get(14)?)?,
                fees: FeeInfo {
                    fee_type: r.get(15)?,
                    fees_enabled: fees_i.map(|b| b != 0),
                    maker_base_fee_bps: r
                        .get::<_, Option<String>>(17)?
                        .and_then(|s| s.parse().ok()),
                    taker_base_fee_bps: r
                        .get::<_, Option<String>>(18)?
                        .and_then(|s| s.parse().ok()),
                },
                neg_risk: r.get::<_, Option<i64>>(19)?.map(|b| b != 0),
                status,
                source_provenance: r.get(21)?,
                discovered_at: parse_ts_col(r.get(22)?)?,
                last_verified_at: parse_ts_col(r.get(23)?)?,
                metadata_version: r.get::<_, i64>(24)? as u64,
                resolution_verification,
            })
        };
        // NOTE: column indices above must match the SELECT list. Verified in
        // tests via roundtrip assertions (see tests/dip_registry.rs).

        let rows: Vec<DipMarket> = match asset {
            Some(a) => stmt
                .query_map(params![a.as_str()], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
            None => stmt
                .query_map([], map_row)?
                .filter_map(|r| r.ok())
                .collect(),
        };
        Ok(rows)
    }

    fn count_dip_markets(&self) -> Result<(u64, u64)> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let total: i64 = conn.query_row("SELECT COUNT(*) FROM dip_markets", [], |r| r.get(0))?;
        let eligible: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dip_markets WHERE eligible = 1",
            [],
            |r| r.get(0),
        )?;
        Ok((total as u64, eligible as u64))
    }

    fn count_resolved(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM dip_markets WHERE status IN ('RESOLVED_YES','RESOLVED_NO','RESOLVED_UNKNOWN')",
                [],
                |r| r.get(0),
            )
            ?;
        Ok(n as u64)
    }

    fn start_sync_run(&self, run_id: &str, universe: &str, assets: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        conn.execute(
            "INSERT INTO dip_sync_runs (run_id, started_at, universe, assets, status)
             VALUES (?1, ?2, ?3, ?4, 'RUNNING')",
            params![run_id, ts(&chrono::Utc::now()), universe, assets],
        )?;
        Ok(())
    }

    fn finish_sync_run(&self, run_id: &str, counts: &SyncCounts, status: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let rejections = serde_json::to_string(&counts.rejections)
            .map_err(|e| PersistError::Misuse(format!("serialize rejections: {e}")))?;
        let errors = serde_json::to_string(&counts.errors)
            .map_err(|e| PersistError::Misuse(format!("serialize errors: {e}")))?;
        conn.execute(
            "UPDATE dip_sync_runs SET finished_at = ?2, pages_fetched = ?3, events_seen = ?4,
                markets_seen = ?5, discovered = ?6, new_count = ?7, updated_count = ?8,
                unchanged_count = ?9, rejected_count = ?10, resolved_retained = ?11,
                out_of_scope = ?12, rejections_json = ?13, errors_json = ?14, status = ?15
             WHERE run_id = ?1",
            params![
                run_id,
                ts(&chrono::Utc::now()),
                counts.pages_fetched as i64,
                counts.events_seen as i64,
                counts.markets_seen as i64,
                counts.discovered as i64,
                counts.new_count as i64,
                counts.updated_count as i64,
                counts.unchanged_count as i64,
                counts.rejected_count as i64,
                counts.resolved_retained as i64,
                counts.out_of_scope as i64,
                rejections,
                errors,
                status,
            ],
        )?;
        Ok(())
    }

    fn history_len(&self, condition_id: &str) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM dip_market_history WHERE condition_id = ?1",
            params![condition_id],
            |r| r.get(0),
        )?;
        Ok(n as u64)
    }

    fn metadata_version(&self, condition_id: &str) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| PersistError::Misuse(e.to_string()))?;
        let v: i64 = conn.query_row(
            "SELECT metadata_version FROM dip_markets WHERE condition_id = ?1",
            params![condition_id],
            |r| r.get(0),
        )?;
        Ok(v as u64)
    }
}
