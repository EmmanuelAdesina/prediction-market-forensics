//! Engineering Assignment 001: canonical ETH/BTC dip-market registry.
//!
//! Sync discovers the universe from the Gamma `hit-price` listing (verified
//! endpoint), normalizes each embedded market through the deterministic
//! extractor (`tokor_market_classifier::dip`), and persists registry rows
//! with diffing + history (`DipRegistryRepository`). The core loop is
//! provider-generic: production uses `MarketDataClient`, tests use fixtures,
//! so malformed payloads and partial API failures are exercised, not assumed.

use std::time::Duration;

use serde_json::Value;
use tokor_domain::dip::{Asset, DipNormalization, MarketStatus, ResolutionStatus};
use tokor_market_classifier::dip::{normalize_gamma_market, GammaMarketInput};
use tokor_persistence::dip_registry::{DipRegistryRepository, SyncCounts, UpsertOutcome};
use tokor_persistence::Database;

use crate::config::AppConfig;
use crate::markets::tag_list;

pub type RegResult<T> = Result<T, String>;

/// Universe segment selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Universe {
    /// `closed=false` - trading-open markets.
    Active,
    /// `closed=true` - historical/resolved markets (backtest corpus).
    Resolved,
    /// Both segments.
    All,
}

impl Universe {
    fn segments(self) -> Vec<bool> {
        match self {
            Universe::Active => vec![false],
            Universe::Resolved => vec![true],
            Universe::All => vec![false, true],
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Universe::Active => "active",
            Universe::Resolved => "resolved",
            Universe::All => "all",
        }
    }
}

/// Outcome of one sync invocation.
#[derive(Debug, Clone)]
pub struct SyncReport {
    pub counts: SyncCounts,
    pub run_id: String,
}

/// Pagination + pacing knobs for one sync invocation.
#[derive(Debug, Clone, Copy)]
pub struct SyncTuning {
    /// Page size requested from the provider (events per listing page).
    pub page_limit: u64,
    /// Hard bound on pages fetched per universe segment.
    pub max_pages: u64,
    /// Pause between page fetches (politeness toward the public API).
    pub page_pause: Duration,
}

/// The core sync loop. `fetch_page(closed, offset)` must return the events
/// array for one listing page or an error; the loop is page-bounded by
/// `tuning.max_pages` per segment. Errors from the provider are recorded in
/// the report (and the segment aborted) - never swallowed, never fatal to
/// the whole run.
pub fn sync_core<P>(
    db: &Database,
    requested: &[Asset],
    universe: Universe,
    tuning: SyncTuning,
    mut fetch_page: P,
    run_id: &str,
) -> RegResult<SyncReport>
where
    P: FnMut(bool, u64) -> Result<Vec<Value>, String>,
{
    if requested.is_empty() {
        return Err("no assets requested".into());
    }
    for a in requested {
        if !Asset::ALL.contains(a) {
            return Err(format!("unsupported asset {a:?} in request"));
        }
    }

    db.start_sync_run(run_id, universe.as_str(), &assets_csv(requested))
        .map_err(|e| format!("start sync run: {e}"))?;

    let mut counts = SyncCounts::default();

    'segments: for closed in universe.segments() {
        let mut offset: u64 = 0;
        let mut pages_this_segment: u64 = 0;
        loop {
            if pages_this_segment >= tuning.max_pages {
                counts
                    .errors
                    .push(format!("max_pages={} reached for closed={closed}", tuning.max_pages));
                break 'segments;
            }
            let page = match fetch_page(closed, offset) {
                Ok(p) => p,
                Err(e) => {
                    counts.errors.push(format!(
                        "page fetch failed (closed={closed}, offset={offset}): {e}"
                    ));
                    break 'segments; // data would be incomplete; stop cleanly
                }
            };
            counts.pages_fetched += 1;
            pages_this_segment += 1;
            let page_len = page.len() as u64;
            counts.events_seen += page_len;

            for event in &page {
                let tags = tag_list(event);
                let event_slug = event.get("slug").and_then(|v| v.as_str()).map(String::from);
                let event_title = event
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let Some(markets) = event.get("markets").and_then(|m| m.as_array()) else {
                    counts.errors.push(format!(
                        "event {:?} has no markets array",
                        event.get("slug")
                    ));
                    continue;
                };
                for market in markets {
                    counts.markets_seen += 1;
                    match normalize_gamma_market(&GammaMarketInput {
                        event_tags: &tags,
                        event_slug: event_slug.as_deref(),
                        event_title: event_title.as_deref(),
                        market,
                    }) {
                        DipNormalization::Rejected {
                            condition_id,
                            reason,
                            detail,
                        } => {
                            counts.rejected_count += 1;
                            *counts
                                .rejections
                                .entry(reason.as_str().to_string())
                                .or_insert(0) += 1;
                            counts.errors.push(format!(
                                "rejected {:?}: {} ({detail})",
                                condition_id.as_deref().unwrap_or("<no-condition-id>"),
                                reason.as_str()
                            ));
                        }
                        DipNormalization::Accepted(m) => {
                            counts.discovered += 1;
                            if !requested.contains(&m.asset) {
                                counts.out_of_scope += 1;
                                continue;
                            }
                            if m.status != MarketStatus::Active {
                                counts.resolved_retained += 1;
                            }
                            if m.resolution_verification.status != ResolutionStatus::Verified {
                                counts.rejected_count += 1;
                                *counts
                                    .rejections
                                    .entry(
                                        m.resolution_verification.status.as_json_key().to_string(),
                                    )
                                    .or_insert(0) += 1;
                            }
                            match db
                                .upsert_dip_market(&m)
                                .map_err(|e| format!("upsert: {e}"))?
                            {
                                UpsertOutcome::New => counts.new_count += 1,
                                UpsertOutcome::Updated(_) => counts.updated_count += 1,
                                UpsertOutcome::Unchanged => counts.unchanged_count += 1,
                            }
                        }
                    }
                }
            }

            if page_len < tuning.page_limit {
                break; // last page of this segment
            }
            offset += tuning.page_limit;
            std::thread::sleep(tuning.page_pause);
        }
    }

    let status = if counts.errors.is_empty() {
        "COMPLETE"
    } else {
        "COMPLETE_WITH_ERRORS"
    };
    db.finish_sync_run(run_id, &counts, status)
        .map_err(|e| format!("finish sync run: {e}"))?;

    Ok(SyncReport {
        counts,
        run_id: run_id.to_string(),
    })
}

fn assets_csv(assets: &[Asset]) -> String {
    assets
        .iter()
        .map(|a| a.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

// ---------------------------------------------------------------------------
// CLI implementations
// ---------------------------------------------------------------------------

/// `registry-sync` - production provider over MarketDataClient.
///
/// The sync loop is synchronous; the Gamma client is async. The loop runs on
/// a blocking thread with a dedicated current-thread runtime so `block_on`
/// never executes inside the ambient tokio runtime (that would panic).
pub async fn registry_sync(
    cfg: &AppConfig,
    assets: &[Asset],
    universe: Universe,
    max_pages: u64,
) -> RegResult<String> {
    let db_path = cfg.db_path.clone();
    let clob_base = cfg.clob_base.clone();
    let gamma_base = cfg.gamma_base.clone();
    let page_limit = cfg.gamma_page_limit;
    let page_pause_ms = cfg.gamma_page_pause_ms;
    let requested: Vec<Asset> = assets.to_vec();
    let run_id = format!(
        "registry-sync-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ")
    );

    let report = tokio::task::spawn_blocking(move || -> RegResult<SyncReport> {
        let db = Database::open(&db_path).map_err(|e| format!("open db: {e}"))?;
        let client = tokor_market_data::MarketDataClient::new(&clob_base, &gamma_base)
            .map_err(|e| format!("client: {e}"))?;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;

        let client_ref = &client;
        let rt_ref = &rt;
        sync_core(
            &db,
            &requested,
            universe,
            SyncTuning {
                page_limit,
                max_pages,
                page_pause: Duration::from_millis(page_pause_ms),
            },
            |closed, offset| {
                rt_ref.block_on(async move {
                    client_ref
                        .gamma_events_by_tag("hit-price", closed, page_limit, offset)
                        .await
                        .map_err(|e| e.to_string())
                })
            },
            &run_id,
        )
    })
    .await
    .map_err(|e| format!("sync task: {e}"))??;

    Ok(format_sync_report(&report))
}

pub fn format_sync_report(report: &SyncReport) -> String {
    let c = &report.counts;
    let mut s = format!(
        "registry-sync [{}]: pages={} events={} markets={}\n  discovered={} new={} updated={} unchanged={}\n  rejected={} resolved_retained={} out_of_scope={}\n  errors={}",
        report.run_id,
        c.pages_fetched,
        c.events_seen,
        c.markets_seen,
        c.discovered,
        c.new_count,
        c.updated_count,
        c.unchanged_count,
        c.rejected_count,
        c.resolved_retained,
        c.out_of_scope,
        c.errors.len()
    );
    if !c.rejections.is_empty() {
        let reasons: Vec<String> = c
            .rejections
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        s.push_str(&format!("\n  rejection reasons: {}", reasons.join(" ")));
    }
    s
}

/// `registry-inspect` - counts + rows summary.
pub fn registry_inspect(
    cfg: &AppConfig,
    asset: Option<Asset>,
    eligible_only: bool,
    limit: u64,
) -> RegResult<String> {
    let db = Database::open(&cfg.db_path).map_err(|e| format!("open db: {e}"))?;
    let (total, eligible) = db.count_dip_markets().map_err(|e| format!("count: {e}"))?;
    let resolved = db
        .count_resolved()
        .map_err(|e| format!("count resolved: {e}"))?;
    let rows = db
        .list_dip_markets(asset, eligible_only, limit)
        .map_err(|e| format!("list: {e}"))?;

    let mut out =
        format!("dip registry: total={total} eligible={eligible} resolved_retained={resolved}\n");
    for m in &rows {
        out.push_str(&format!(
            "  [{}] {} barrier=${} end={} eligible={} status={:?} v{}\n",
            m.asset.as_str(),
            &m.condition_id[..10.min(m.condition_id.len())],
            m.barrier_price,
            m.end_date.format("%Y-%m-%d"),
            m.is_eligible(),
            m.status,
            m.metadata_version,
        ));
    }
    if rows.len() as u64 == limit {
        out.push_str(&format!("  … (showing {limit})\n"));
    }
    Ok(out)
}

/// `registry-export` - deterministic JSON export (sorted by asset, barrier).
pub fn registry_export(
    cfg: &AppConfig,
    asset: Option<Asset>,
    eligible_only: bool,
    limit: u64,
    output: &std::path::Path,
) -> RegResult<String> {
    let db = Database::open(&cfg.db_path).map_err(|e| format!("open db: {e}"))?;
    let rows = db
        .list_dip_markets(asset, eligible_only, limit)
        .map_err(|e| format!("list: {e}"))?;
    let (total, eligible) = db.count_dip_markets().map_err(|e| format!("count: {e}"))?;

    let doc = serde_json::json!({
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "filter": {
            "asset": asset.map(|a| a.as_str().to_string()),
            "eligible_only": eligible_only,
            "limit": limit,
        },
        "registry_totals": { "total": total, "eligible": eligible },
        "markets": rows,
    });
    let json = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize: {e}"))?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    }
    std::fs::write(output, json).map_err(|e| format!("write {}: {e}", output.display()))?;
    Ok(format!(
        "exported {} markets (registry total={total}, eligible={eligible}) → {}",
        rows.len(),
        output.display()
    ))
}
