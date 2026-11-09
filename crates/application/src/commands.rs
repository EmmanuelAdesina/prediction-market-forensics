//! CLI command implementations. Every command is an explicit, logged,
//! idempotent operation against a single SQLite database.

use std::time::Duration;

use chrono::Utc;
use tokor_domain::activity::{ActivityFilters, ActivityType, SourceFill};
use tokor_domain::classification::CryptoVerdict;
use tokor_domain::market::MarketRef;
use tokor_market_data::MarketDataClient;
use tokor_persistence::ingest::IngestRepository;
use tokor_persistence::markets::MarketRepository;
use tokor_persistence::recon::{ReconObservation, ReconRepository};
use tokor_persistence::{Database, PersistError};
use tokor_wallet_feed::http::HttpWalletFeed;
use tokor_wallet_feed::ingestor::FullHistoryIngestor;
use tokor_wallet_feed::positions::{
    walk_positions, HttpPositionsFetcher, PaginationRetry, PositionsError,
};
use tokor_wallet_feed::provider::RetryingProvider;

use crate::config::AppConfig;

pub type CmdResult<T> = Result<T, String>;

fn db(cfg: &AppConfig) -> CmdResult<Database> {
    Database::open(&cfg.db_path).map_err(|e: PersistError| format!("open db: {e}"))
}

fn filters_for(cfg: &AppConfig, limit: u64) -> ActivityFilters {
    ActivityFilters {
        user: cfg.wallet_address.clone(),
        record_types: vec![ActivityType::Trade],
        limit,
    }
}

// ---------------------------------------------------------------- ingest-wallet

pub async fn ingest_wallet(
    cfg: &AppConfig,
    full: bool,
    limit: Option<u64>,
    max_pages: Option<u64>,
) -> CmdResult<String> {
    let database = db(cfg)?;
    let http = HttpWalletFeed::new(&cfg.data_api_base).map_err(|e| format!("http client: {e}"))?;
    let provider = RetryingProvider::new(http);
    let mut ingestor = FullHistoryIngestor::new(&provider, &database);
    ingestor.full = full;
    if let Some(mp) = max_pages {
        ingestor.max_pages = mp;
    }
    let filters = filters_for(cfg, limit.unwrap_or(cfg.page_limit));
    let run_id = format!("wallet-{}", Utc::now().format("%Y%m%dT%H%M%SZ"));
    tracing::info!(run_id=%run_id, %full, limit=filters.limit, "ingest-wallet starting");
    let progress = ingestor
        .run(&filters, &run_id)
        .await
        .map_err(|e| format!("ingest run: {e}"))?;

    // Upsert market references gathered from activity records so downstream
    // market enrichment has rows to attach to.
    let fills =
        IngestRepository::load_fills(&database, None).map_err(|e| format!("load fills: {e}"))?;
    upsert_market_refs_from_fills(&database, &fills)?;

    Ok(format!(
        "run {run_id}: pages={}, inserted={}, duplicates={}, non_trade_rows(raw-only)={}, complete={} ({})",
        progress.pages,
        progress.fills_inserted,
        progress.fills_duplicate,
        progress.non_trade_records,
        progress.complete,
        progress.terminal_reason,
    ))
}

fn upsert_market_refs_from_fills(database: &Database, fills: &[SourceFill]) -> CmdResult<()> {
    let mut seen: std::collections::HashMap<&str, MarketRef> = std::collections::HashMap::new();
    for f in fills {
        let entry = seen
            .entry(f.condition_id.as_str())
            .or_insert_with(|| MarketRef {
                condition_id: f.condition_id.clone(),
                ..Default::default()
            });
        if entry.title.is_none() {
            entry.title = Some(f.market_title.clone());
        }
        if entry.market_slug.is_none() {
            entry.market_slug = Some(f.market_slug.clone());
        }
        if entry.event_slug.is_none() {
            entry.event_slug = Some(f.event_slug.clone());
        }
        if entry.icon.is_none() {
            entry.icon = Some(f.icon.clone());
        }
    }
    for (cid, mref) in seen.iter().map(|(k, v)| (k.to_string(), v.clone())) {
        MarketRepository::upsert_market_ref(database, &mref)
            .map_err(|e| format!("market ref upsert {cid}: {e}"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------- ingest-markets

pub async fn ingest_markets(cfg: &AppConfig, limit: Option<u64>) -> CmdResult<String> {
    let database = db(cfg)?;
    let client = MarketDataClient::new(&cfg.clob_base, &cfg.gamma_base)
        .map_err(|e| format!("market data client: {e}"))?;
    let cids = MarketRepository::condition_ids_needing_enrichment(&database, limit)
        .map_err(|e| format!("query markets: {e}"))?;
    let total = cids.len();
    let mut done = 0usize;
    let mut errors = 0usize;
    for cid in &cids {
        tokio::time::sleep(Duration::from_millis(cfg.page_pause_ms / 4)).await;
        match crate::markets::enrich_market(&database, &client, cid).await {
            Ok(()) => done += 1,
            Err(e) => {
                errors += 1;
                tracing::warn!(condition_id=%cid, error=%e, "market enrichment failed");
            }
        }
    }
    Ok(format!("enriched {done}/{total} markets ({errors} errors)"))
}

// ---------------------------------------------------------------- classify

pub fn classify_markets(cfg: &AppConfig) -> CmdResult<String> {
    let database = db(cfg)?;
    let pending = MarketRepository::condition_ids_missing_classification(&database)
        .map_err(|e| format!("query markets: {e}"))?;
    let mut counts = [0usize; 3]; // crypto, non_crypto, unknown
    let mut classified = 0usize;
    for cid in &pending {
        let Some(mref) = MarketRepository::market_ref(&database, cid)
            .map_err(|e| format!("load market {cid}: {e}"))?
        else {
            continue;
        };
        let evidence = tokor_market_classifier::classify(&mref);
        match evidence.verdict {
            CryptoVerdict::Crypto => counts[0] += 1,
            CryptoVerdict::NonCrypto => counts[1] += 1,
            CryptoVerdict::Unknown => counts[2] += 1,
        }
        MarketRepository::record_classification(&database, cid, &evidence)
            .map_err(|e| format!("record classification {cid}: {e}"))?;
        classified += 1;
    }
    let totals = MarketRepository::classified_markets(&database)
        .map_err(|e| format!("reload classified: {e}"))?;
    Ok(format!(
        "newly classified: {classified} (crypto {}, non_crypto {}, unknown {}) — totals: CRYPTO {} of {}",
        counts[0],
        counts[1],
        counts[2],
        totals.iter().filter(|(_, v, _)| *v == CryptoVerdict::Crypto).count(),
        totals.len(),
    ))
}

// ---------------------------------------------------------------- reconstruct

pub fn reconstruct_positions(cfg: &AppConfig) -> CmdResult<String> {
    let database = db(cfg)?;
    let fills =
        IngestRepository::load_fills(&database, None).map_err(|e| format!("load fills: {e}"))?;
    use tokor_domain::position::PositionBook;
    let mut book = PositionBook::new();
    book.apply_all(&fills).map_err(|e| format!("replay: {e}"))?;

    // Persist one snapshot per token; condition_id from the fills of that token.
    let mut cond_by_token = std::collections::HashMap::new();
    for f in &fills {
        cond_by_token
            .entry(f.token_id.clone())
            .or_insert(f.condition_id.clone());
    }
    for token in book.token_ids() {
        let pos = book.get(token).expect("token exists");
        let cond = cond_by_token.get(token).cloned().unwrap_or_default();
        ReconRepository::save_reconstructed_position(&database, token, &cond, pos)
            .map_err(|e| format!("persist position {token}: {e}"))?;
    }
    let with_inventory = book
        .token_ids()
        .iter()
        .filter(|t| {
            book.get(t)
                .map(|p| p.remaining_inventory() > rust_decimal::Decimal::ZERO)
                .unwrap_or(false)
        })
        .count();
    let incomplete = book
        .token_ids()
        .iter()
        .filter(|t| {
            book.get(t)
                .map(|p| p.history_probably_incomplete)
                .unwrap_or(false)
        })
        .count();
    Ok(format!(
        "reconstructed {} tokens ({} with remaining inventory, {} flagged probable pre-history) from {} fills",
        book.token_ids().len(),
        with_inventory,
        incomplete,
        fills.len(),
    ))
}

// ---------------------------------------------------------------- reconcile

pub async fn reconcile_positions(cfg: &AppConfig) -> CmdResult<String> {
    let database = db(cfg)?;
    let fetcher = HttpPositionsFetcher::new(&cfg.data_api_base)
        .map_err(|e| format!("positions client: {e}"))?;

    let run_id = format!("positions-{}", Utc::now().format("%Y%m%dT%H%M%SZ"));
    tokor_persistence::ingest::IngestRepository::start_run(
        &database,
        &run_id,
        "{}",
        "positions-open",
    )
    .map_err(|e| format!("start run: {e}"))?;

    let dbref = &database;
    let run = run_id.clone();
    let mut page_index = 0u64;
    let result = walk_positions(
        &fetcher,
        &cfg.wallet_address,
        cfg.positions_page_limit,
        10_000,
        Duration::from_millis(cfg.page_pause_ms),
        PaginationRetry::default(),
        |page| {
            IngestRepository::commit_page(
                dbref,
                &run,
                page_index,
                &Utc::now().to_rfc3339(),
                page.http_status,
                &page.url,
                &page.query_params,
                &page.raw_body.to_string(),
                page.next_cursor.as_deref(),
                page.has_more,
                &[],
            )
            .map_err(|e| {
                PositionsError::Upstream(tokor_domain::error::IngestionError::Storage(
                    e.to_string(),
                ))
            })?;
            page_index += 1;
            Ok(page.rows.clone())
        },
    )
    .await;

    let rows: Vec<serde_json::Value> = result
        .map_err(|e| format!("positions walk: {e}"))?
        .into_iter()
        .flatten()
        .collect();

    let mut match_ = 0u32;
    let mut mismatch = 0u32;
    let mut api_only = 0u32;
    let mut notes = String::new();
    for row in &rows {
        let obs = compare_position(dbref, row, &run_id)?;
        match obs.verdict {
            "MATCH" => match_ += 1,
            "MISMATCH" => {
                mismatch += 1;
                if mismatch <= 10 {
                    notes.push_str(&format!(
                        "{}: api_size={} vs ours_size={}; notes {}\n",
                        obs.token_id,
                        obs.api_size.as_deref().unwrap_or("?"),
                        obs.ours_size.as_deref().unwrap_or("?"),
                        obs.notes_json
                    ));
                }
            }
            "API_ONLY" => api_only += 1,
            _ => {}
        }
        ReconRepository::save_recon_observation(dbref, &obs)
            .map_err(|e| format!("save obs: {e}"))?;
    }

    let verdict_counts = ReconRepository::recon_verdict_counts(&database, &run_id)
        .map_err(|e| format!("verdict counts: {e}"))?;
    // Persist an explicit completion state on the run row.
    tokor_persistence::ingest::IngestRepository::finish_run(&database, &run_id, "COMPLETE", None)
        .map_err(|e| format!("finish run: {e}"))?;

    let mut out = format!(
        "reconciliation {run_id}: {} api positions — MATCH {match_}, MISMATCH {mismatch}, API_ONLY {api_only}; pages {page_index}; verdict counts: {verdict_counts:?}",
        rows.len(),
    );
    if !notes.is_empty() {
        out.push_str("\nfirst mismatches:\n");
        out.push_str(notes.trim_end());
    }
    Ok(out)
}

/// Compare one `/v2/positions` row against our reconstructed snapshot.
/// Never forces agreement: deltas and verdict are recorded with evidence.
fn compare_position(
    database: &Database,
    row: &serde_json::Value,
    run_id: &str,
) -> CmdResult<ReconObservation> {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    let get_f = |k: &str| -> Option<String> {
        row.get(k)
            .map(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .flatten()
    };
    let get_s =
        |k: &str| -> Option<String> { row.get(k).and_then(|v| v.as_str()).map(|s| s.to_string()) };

    let token_id = get_s("token_id").unwrap_or_default();
    let condition_id = get_s("condition_id").unwrap_or_default();
    let status = get_s("status").unwrap_or_default();

    let ours = ReconRepository::load_reconstructed_position(database, &token_id)
        .map_err(|e| format!("load ours {token_id}: {e}"))?;

    let api_size = get_f("current_size").or_else(|| get_f("total_size"));
    let api_avg = get_f("avg_price");
    let api_entry = get_f("entry_cost_usdc");
    let api_fees = get_f("entry_fees_usdc");
    let api_realized = get_f("realized_pnl");
    let api_unrealized = get_f("unrealized_pnl");

    let ours_size = ours.as_ref().map(|p| p.remaining_inventory.clone());
    let ours_avg = ours.as_ref().and_then(|p| p.avg_open_price.clone());
    let ours_realized = ours.as_ref().map(|p| p.realized_pnl.clone());

    // Fixed-point deltas where both sides exist.
    let delta = |a: &Option<String>, b: &Option<String>, tol: Decimal| -> (Option<String>, bool) {
        match (a, b) {
            (Some(a), Some(b)) => {
                let (da, db) = (a.parse::<Decimal>(), b.parse::<Decimal>());
                match (da, db) {
                    (Ok(da), Ok(db)) => (Some((da - db).abs().to_string()), (da - db).abs() <= tol),
                    _ => (None, false),
                }
            }
            _ => (None, false),
        }
    };
    let (delta_size, size_ok) = delta(&api_size, &ours_size, dec!(0.0001));
    let (delta_avg, avg_ok) = delta(&api_avg, &ours_avg, dec!(0.001));

    let verdict = if ours.is_none() {
        "API_ONLY"
    } else if {
        // OPEN positions must match size + avg price. Positions whose history
        // we flagged as incomplete are recorded as MISMATCH even when sizes
        // coincide — the provenance is different and must stay visible.
        let history_gap = ours
            .as_ref()
            .map(|p| p.history_probably_incomplete)
            .unwrap_or(false);
        history_gap || !size_ok || !avg_ok
    } {
        "MISMATCH"
    } else {
        "MATCH"
    };

    let mut notes = serde_json::json!({
        "api_status": status,
        "history_probably_incomplete": ours.as_ref().map(|p| p.history_probably_incomplete).unwrap_or(false),
        "api_entry_cost_usdc": api_entry,
        "api_entry_fees_usdc": api_fees,
        // Recorded hypothesis under test: api.total_cost ≈ entry_cost + entry_fees,
        // and ours (price*size over fills, fee-free) may differ by those fees.
        "api_total_cost_usdc": get_f("total_cost_usdc"),
        "api_event_slug": get_s("event_slug"),
        "ours_fills_applied": ours.as_ref().map(|p| p.fills_applied),
        "ours_unattributed_disposals": ours.as_ref().map(|p| p.unattributed_disposals.clone()),
    });
    if notes["history_probably_incomplete"] == true {
        notes["explanation"] = serde_json::json!(
            "our FIFO history has pre-history disposals: activity window does not cover position origin"
        );
    }
    if verdict == "MISMATCH" && size_ok == false {
        notes["explanation"] = serde_json::json!("size delta beyond tolerance");
    }

    Ok(ReconObservation {
        run_id: run_id.to_string(),
        token_id: token_id.clone(),
        condition_id: condition_id.clone(),
        api_size,
        api_avg_price: api_avg,
        api_entry_cost: api_entry,
        api_entry_fees: api_fees,
        api_realized,
        api_unrealized,
        ours_size,
        ours_avg_price: ours_avg,
        ours_realized,
        delta_size,
        delta_avg,
        delta_realized: None,
        verdict,
        notes_json: notes.to_string(),
    })
}

// ---------------------------------------------------------------- report

/// Operator status report over the data foundation. Read-only.
pub fn report(cfg: &AppConfig) -> CmdResult<String> {
    use tokor_persistence::markets::MarketRepository;
    use tokor_persistence::recon::ReconRepository;

    let database = db(cfg)?;
    let stats = IngestRepository::fill_stats(&database).map_err(|e| format!("stats: {e}"))?;
    let latest_run = tokor_persistence::report::ReportRepository::latest_run(&database)
        .map_err(|e| format!("run: {e}"))?;
    let positions = tokor_persistence::report::ReportRepository::position_counts(&database)
        .map_err(|e| format!("positions: {e}"))?;
    let last_recon_run = tokor_persistence::report::ReportRepository::last_recon_run_id(&database)
        .map_err(|e| format!("recon run: {e}"))?;

    let classified =
        MarketRepository::classified_markets(&database).map_err(|e| format!("classified: {e}"))?;
    let n_crypto = classified
        .iter()
        .filter(|(_, v, _)| *v == CryptoVerdict::Crypto)
        .count();
    let n_non = classified
        .iter()
        .filter(|(_, v, _)| *v == CryptoVerdict::NonCrypto)
        .count();
    let n_unk = classified
        .iter()
        .filter(|(_, v, _)| *v == CryptoVerdict::Unknown)
        .count();

    let verdicts = match &last_recon_run {
        Some(r) => ReconRepository::recon_verdict_counts(&database, r)
            .map_err(|e| format!("verdicts: {e}"))?,
        None => Vec::new(),
    };

    let run_line = latest_run
        .map(|r| {
            format!(
                "wallet acquisition: run {} status={} pages={} fills_committed={} cursor={:?}",
                r.run_id, r.status, r.pages_committed, r.fills_committed, r.last_cursor
            )
        })
        .unwrap_or_else(|| "wallet acquisition: no runs recorded yet".into());

    Ok(format!(
        "=== tokor data foundation report ===\n\
         fills: total={} BUY={} SELL={}\n\
         {run_line}\n\
         markets classified: CRYPTO={n_crypto} NON_CRYPTO={n_non} UNKNOWN={n_unk} (of {})\n\
         positions reconstructed: {} (open-inventory {})\n\
         last reconciliation: {last_recon_run:?} verdicts {verdicts:?}",
        stats.total,
        stats.buys,
        stats.sells,
        classified.len(),
        positions.total,
        positions.open_inventory,
    ))
}
