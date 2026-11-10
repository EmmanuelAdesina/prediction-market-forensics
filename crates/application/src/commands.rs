//! CLI command implementations. Every command is an explicit, logged,
//! idempotent operation against a single SQLite database.

use std::time::Duration;

use chrono::Utc;
use tokor_domain::activity::{ActivityFilters, ActivityType, SourceFill};
use tokor_domain::classification::CryptoVerdict;
use tokor_domain::market::MarketRef;
use tokor_market_data::MarketDataClient;
use tokor_persistence::activity::{FeeRow, PositionSnapshotRow};
use tokor_persistence::ingest::IngestRepository;
use tokor_persistence::market_data::{MarketDataRepository, SourceObservationRow, SubscriptionRow};
use tokor_persistence::markets::MarketRepository;
use tokor_persistence::recon::{ReconObservation, ReconRepository};
use tokor_persistence::{Database, PersistError};
use tokor_recorder::session::{RecorderCommand, RecorderHandle};
use tokor_recorder::{messages::parse_book_body, RecorderConfig};
use tokor_wallet_feed::http::HttpWalletFeed;
use tokor_wallet_feed::ingestor::FullHistoryIngestor;
use tokor_wallet_feed::positions::{
    walk_positions, walk_positions_status, HttpPositionsFetcher, PaginationRetry, PositionsError,
};
use tokor_wallet_feed::provider::{DataApiActivityRequest, RetryingProvider, WalletFeedProvider};

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
    use std::sync::Arc as StdArc;
    use tokio::sync::Semaphore;
    use tokio::task::JoinSet;

    let database = StdArc::new(db(cfg)?);
    let client = StdArc::new(
        MarketDataClient::new(&cfg.clob_base, &cfg.gamma_base)
            .map_err(|e| format!("market data client: {e}"))?,
    );
    let cids = MarketRepository::condition_ids_needing_enrichment(database.as_ref(), limit)
        .map_err(|e| format!("query markets: {e}"))?;
    let total = cids.len();

    // Bounded concurrency: short CLOB/Gamma fetches with a 3-way cap keeps
    // aggregate request rate polite (~3 market pairs in flight) while not
    // serializing behind multi-second timeouts.
    const CONCURRENCY: usize = 3;
    let permits = StdArc::new(Semaphore::new(CONCURRENCY));
    let mut jobs: JoinSet<Result<(), String>> = JoinSet::new();
    for cid in &cids {
        let db = database.clone();
        let client = client.clone();
        let permit = permits.clone();
        let cid = cid.clone();
        jobs.spawn(async move {
            let _guard = permit.acquire_owned().await;
            // Small stagger so bursts never arrive simultaneously.
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            enrich_with_retry(&db, &client, &cid).await
        });
    }
    let mut done = 0usize;
    let mut errors = 0usize;
    while let Some(res) = jobs.join_next().await {
        match res {
            Ok(Ok(())) => done += 1,
            Ok(Err(e)) => {
                errors += 1;
                tracing::warn!(error=%e, "market enrichment failed");
            }
            Err(e) => {
                errors += 1;
                tracing::warn!(error=%e, "market enrichment task panicked");
            }
        }
    }
    Ok(format!("enriched {done}/{total} markets ({errors} errors)"))
}

/// One retry on transient failure (transport, 5xx, sqlite lock).
async fn enrich_with_retry(
    database: &std::sync::Arc<Database>,
    client: &std::sync::Arc<MarketDataClient>,
    condition_id: &str,
) -> Result<(), String> {
    let mut last = String::from("unknown");
    for attempt in 0..2u32 {
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        }
        match crate::markets::enrich_market(database, client, condition_id).await {
            Ok(()) => return Ok(()),
            Err(e) => last = e,
        }
    }
    Err(last)
}

// ---------------------------------------------------------------- classify

pub fn classify_markets(cfg: &AppConfig, reset: bool) -> CmdResult<String> {
    let database = db(cfg)?;
    if reset {
        MarketRepository::clear_classifications(&database)
            .map_err(|e| format!("reset classifications: {e}"))?;
        tracing::info!("classifications cleared for re-evaluation");
    }
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
                &tokor_persistence::ingest::PageRecord {
                    run_id: run.clone(),
                    page_index,
                    fetched_at: Utc::now().to_rfc3339(),
                    http_status: page.http_status,
                    url: page.url.clone(),
                    query_params: page.query_params.clone(),
                    body_json: page.raw_body.to_string(),
                    next_cursor: page.next_cursor.clone(),
                    has_more: page.has_more,
                },
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
        row.get(k).and_then(|v| match v {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
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

    // OPEN positions must match size + avg price. Positions whose history
    // we flagged as incomplete are recorded as MISMATCH even when sizes
    // coincide — the provenance is different and must stay visible.
    let history_gap = ours
        .as_ref()
        .map(|p| p.history_probably_incomplete)
        .unwrap_or(false);
    let verdict = if ours.is_none() {
        "API_ONLY"
    } else if history_gap || !size_ok || !avg_ok {
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
    if verdict == "MISMATCH" && !size_ok {
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

// ------------------------------------------------------------- ingest-activity

/// Unfiltered wallet-activity walk: every record type the feed produces
/// (TRADE, MERGE, REDEEM, REWARD, YIELD, TAKER_REBATE, MAKER_REBATE, ...),
/// normalized into `activity_events` with TRADE rows also landing in
/// `source_fills`. Read-only public data; cursor is separate from the
/// Phase 1 TRADE-only walk (different filter fingerprint).
pub async fn ingest_activity(
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
    let filters = ActivityFilters {
        user: cfg.wallet_address.clone(),
        record_types: Vec::new(), // unfiltered: all activity types
        limit: limit.unwrap_or(cfg.page_limit),
    };
    let run_id = format!("activity-{}", Utc::now().format("%Y%m%dT%H%M%SZ"));
    tracing::info!(run_id=%run_id, %full, limit=filters.limit, "ingest-activity starting");
    let progress = ingestor
        .run(&filters, &run_id)
        .await
        .map_err(|e| format!("activity run: {e}"))?;

    Ok(format!(
        "run {run_id}: pages={}, events={}(dup {}), fills={}(dup {}), invalid={} (surfaced raw), complete={} ({})",
        progress.pages,
        progress.events_inserted,
        progress.events_duplicate,
        progress.fills_inserted,
        progress.fills_duplicate,
        progress.invalid_rows,
        progress.complete,
        progress.terminal_reason,
    ))
}

// ------------------------------------------------------------ ingest-positions

/// Read-only capture of GET /v2/positions across all status views
/// (OPEN, CLOSED, REDEEMABLE) into `positions_snapshot` + forensic raw pages.
pub async fn ingest_positions(cfg: &AppConfig) -> CmdResult<String> {
    let database = db(cfg)?;
    let fetcher = HttpPositionsFetcher::new(&cfg.data_api_base)
        .map_err(|e| format!("positions client: {e}"))?;

    let run_id = format!("snapshot-{}", Utc::now().format("%Y%m%dT%H%M%SZ"));
    tokor_persistence::ingest::IngestRepository::start_run(
        &database,
        &run_id,
        "{}",
        "positions-all",
    )
    .map_err(|e| format!("start run: {e}"))?;
    let mut all_rows: Vec<PositionSnapshotRow> = Vec::new();
    let mut pages_total = 0u64;
    for status in ["OPEN", "CLOSED", "REDEEMABLE"] {
        let mut page_index = 0u64;
        let result = walk_positions_status(
            &fetcher,
            &cfg.wallet_address,
            cfg.positions_page_limit,
            10_000,
            Duration::from_millis(cfg.page_pause_ms),
            PaginationRetry::default(),
            Some(status),
            |page| {
                IngestRepository::commit_page(
                    &database,
                    &tokor_persistence::ingest::PageRecord {
                        run_id: run_id.clone(),
                        page_index,
                        fetched_at: Utc::now().to_rfc3339(),
                        http_status: page.http_status,
                        url: page.url.clone(),
                        query_params: page.query_params.clone(),
                        body_json: page.raw_body.to_string(),
                        next_cursor: page.next_cursor.clone(),
                        has_more: page.has_more,
                    },
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
        pages_total += page_index;
        for r in rows {
            let dec_s = |k: &str| {
                r.get(k).map(|v| match v {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            };
            let req = |k: &str| -> CmdResult<String> {
                dec_s(k).ok_or_else(|| format!("positions row missing {k}"))
            };
            let b = |k: &str| -> Option<bool> { r.get(k).and_then(|v| v.as_bool()) };
            all_rows.push(PositionSnapshotRow {
                token_id: req("token_id")?,
                condition_id: req("condition_id")?,
                status: status.to_string(),
                outcome: req("outcome")?,
                outcome_index: r.get("outcome_index").and_then(|v| v.as_i64()).unwrap_or(0),
                current_size: req("current_size")?,
                avg_price: req("avg_price")?,
                realized_pnl: dec_s("realized_pnl"),
                percent_pnl: dec_s("percent_pnl"),
                cur_price: dec_s("cur_price"),
                cur_value: dec_s("cur_value"),
                entry_cost_usdc: dec_s("entry_cost_usdc"),
                entry_fees_usdc: dec_s("entry_fees_usdc"),
                total_size: dec_s("total_size"),
                negative_risk: r
                    .get("negative_risk")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                mergeable: b("mergeable"),
                redeemable: b("redeemable"),
                event_slug: r
                    .get("event_slug")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                title: r
                    .get("title")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            });
        }
    }

    let n = all_rows.len();
    tokor_persistence::activity::ActivityRepository::store_positions_snapshot(
        &database, &run_id, &all_rows,
    )
    .map_err(|e| format!("store snapshot: {e}"))?;
    IngestRepository::finish_run(&database, &run_id, "COMPLETE", None)
        .map_err(|e| format!("finish run: {e}"))?;
    let by_status = {
        let mut m = std::collections::BTreeMap::new();
        for r in &all_rows {
            *m.entry(r.status.as_str()).or_insert(0u64) += 1;
        }
        format!("{m:?}")
    };
    Ok(format!(
        "snapshot {run_id}: pages={pages_total}, legs={n} by status {by_status}"
    ))
}

// ----------------------------------------------------------------- derive-fees

/// Classify every stored fill under the verified fee model and persist the
/// reconciliation. Categories come from the Gamma-derived map
/// (data/derived/event_fee_categories.json, produced by
/// scripts/fetch_event_categories.py).
pub fn derive_fees(cfg: &AppConfig, categories_path: &std::path::Path) -> CmdResult<String> {
    use std::collections::HashMap;

    let database = db(cfg)?;
    let fills =
        IngestRepository::load_fills(&database, None).map_err(|e| format!("load fills: {e}"))?;

    let cat_json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(categories_path)
            .map_err(|e| format!("read {}: {e}", categories_path.display()))?,
    )
    .map_err(|e| format!("parse categories: {e}"))?;
    let mut categories: HashMap<String, (String, rust_decimal::Decimal)> = HashMap::new();
    if let serde_json::Value::Object(map) = cat_json {
        for (slug, v) in map {
            let cat = v.get("fee_category").and_then(|c| c.as_str());
            let rate = v.get("fee_rate").and_then(|r| r.as_f64());
            if let (Some(cat), Some(rate)) = (cat, rate) {
                if let Ok(d) = rust_decimal::Decimal::try_from(rate) {
                    categories.insert(slug, (cat.to_string(), d));
                }
            }
        }
    }

    let mut rows = Vec::with_capacity(fills.len());
    let mut verdict_counts: std::collections::BTreeMap<&'static str, u64> = Default::default();
    let mut face_counts: std::collections::BTreeMap<&'static str, u64> = Default::default();
    let mut charged_total = rust_decimal::Decimal::ZERO;
    let mut unexplained = 0usize;
    for f in &fills {
        let (cat_name, category) = match categories.get(&f.event_slug) {
            Some((name, _rate)) => {
                let category = match name.as_str() {
                    "crypto" => tokor_domain::fees::FeeCategory::Crypto,
                    "finance" => tokor_domain::fees::FeeCategory::Finance,
                    "sports" => tokor_domain::fees::FeeCategory::Sports,
                    "politics" => tokor_domain::fees::FeeCategory::Politics,
                    _ => tokor_domain::fees::FeeCategory::Unknown,
                };
                (Some(name.clone()), category)
            }
            None => (None, tokor_domain::fees::FeeCategory::Unknown),
        };
        let rate = category.taker_rate();
        let (verdict, face) =
            tokor_domain::fees::classify_fill(f.size, f.price, f.usdc_size, category)
                .map_err(|e| format!("classify {}: {e}", f.event_key))?;
        let gross = f.price * f.size;
        let (fee_charged, fee_wouldbe) = match verdict {
            tokor_domain::fees::FillVerdict::VerifiedFeeInclusive => {
                let fee = rate
                    .map(|r| tokor_domain::fees::charged_fee(f.size, f.price, r))
                    .unwrap_or_default();
                charged_total += fee;
                (Some(fee), Some(fee))
            }
            _ => {
                let wb = rate.and_then(|r| {
                    let raw = tokor_domain::fees::unrounded_fee(f.size, f.price, r);
                    if raw >= tokor_domain::fees::MIN_FEE {
                        Some(raw.round_dp(5))
                    } else {
                        None
                    }
                });
                (Some(rust_decimal::Decimal::ZERO), wb)
            }
        };
        let reconstructed = match verdict {
            tokor_domain::fees::FillVerdict::VerifiedFeeInclusive => {
                gross + fee_charged.unwrap_or_default()
            }
            _ => gross,
        };
        if matches!(
            verdict,
            tokor_domain::fees::FillVerdict::UnknownFeeSemantics
                | tokor_domain::fees::FillVerdict::Unexplained
        ) {
            unexplained += 1;
        }
        *verdict_counts.entry(verdict_label(verdict)).or_insert(0) += 1;
        *face_counts
            .entry(match face {
                tokor_domain::fees::FillFace::Maker => "maker",
                tokor_domain::fees::FillFace::Taker => "taker",
                tokor_domain::fees::FillFace::Indeterminate => "n/a",
            })
            .or_insert(0) += 1;
        rows.push(FeeRow {
            event_key: f.event_key.clone(),
            fill_face: Some(face),
            verdict,
            gross_usdc: gross,
            fee_usdc: fee_charged,
            fee_wouldbe_usdc: fee_wouldbe,
            fee_rate: rate,
            fee_category: cat_name,
            reconstructed_debit: Some(reconstructed),
            residual_usdc: Some(f.usdc_size - reconstructed),
        });
    }

    let n = tokor_persistence::activity::ActivityRepository::store_fee_rows(&database, &rows)
        .map_err(|e| format!("store fee rows: {e}"))?;
    Ok(format!(
        "fees derived for {n} fills: verdicts {verdict_counts:?}, faces {face_counts:?}, charged fees Σ {charged_total}, unexplained {unexplained}"
    ))
}

// --------------------------------------------------------------- derive-parquet

fn verdict_label(v: tokor_domain::fees::FillVerdict) -> &'static str {
    match v {
        tokor_domain::fees::FillVerdict::VerifiedGrossNotional => "VERIFIED_GROSS_NOTIONAL",
        tokor_domain::fees::FillVerdict::VerifiedFeeInclusive => "VERIFIED_FEE_INCLUSIVE",
        tokor_domain::fees::FillVerdict::UnknownFeeSemantics => "UNKNOWN_FEE_SEMANTICS",
        tokor_domain::fees::FillVerdict::Unexplained => "UNEXPLAINED",
    }
}

/// Emit the Phase 1.5 parquet datasets from persisted state:
/// `fees` (per-fill fee reconciliation), `activity` (all-type events),
/// `inventory` (per-token inventory reconciliation vs the API snapshot).
pub fn derive_parquet(cfg: &AppConfig, dataset: &str, out: &std::path::Path) -> CmdResult<String> {
    match dataset {
        "fees" => write_fees_parquet(cfg, out),
        "activity" => write_activity_parquet(cfg, out),
        "inventory" => write_inventory_parquet(cfg, out),
        other => Err(format!(
            "unknown dataset {other:?}: choose fees | activity | inventory"
        )),
    }
}

fn write_fees_parquet(cfg: &AppConfig, out: &std::path::Path) -> CmdResult<String> {
    use arrow::array::{BooleanArray, Decimal128Array, StringArray, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    const DP: i8 = 9; // 1e-9 USDC resolution — far beyond source precision
    let database = db(cfg)?;
    let pairs = tokor_persistence::activity::ActivityRepository::load_fee_rows(&database)
        .map_err(|e| format!("load fee rows: {e}"))?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_key", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("price", DataType::Decimal128(38, DP), false),
        Field::new("size", DataType::Decimal128(38, DP), false),
        Field::new("gross_usdc", DataType::Decimal128(38, DP), false),
        Field::new("fee_usdc", DataType::Decimal128(38, DP), false),
        Field::new("fee_wouldbe_usdc", DataType::Decimal128(38, DP), true),
        Field::new("fee_rate", DataType::Decimal128(38, 4), true),
        Field::new("fee_category", DataType::Utf8, true),
        Field::new("observed_usdc", DataType::Decimal128(38, DP), false),
        Field::new("reconstructed_debit", DataType::Decimal128(38, DP), true),
        Field::new("residual_usdc", DataType::Decimal128(38, DP), true),
        Field::new("verdict", DataType::Utf8, false),
        Field::new("fill_face", DataType::Utf8, true),
        Field::new("maker_or_taker_chargeable", DataType::Boolean, false),
        Field::new("market_title", DataType::Utf8, false),
        Field::new("event_slug", DataType::Utf8, false),
        Field::new("transaction_hash", DataType::Utf8, false),
    ]));
    let scale = |mut d: rust_decimal::Decimal| -> i128 {
        d.rescale(DP as u32);
        d.mantissa()
    };
    let rate_scale = |mut d: rust_decimal::Decimal| -> i128 {
        d.rescale(4);
        d.mantissa()
    };
    let n = pairs.len();
    let mut event_key = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut cond = Vec::with_capacity(n);
    let mut token = Vec::with_capacity(n);
    let mut side = Vec::with_capacity(n);
    let mut price = Vec::with_capacity(n);
    let mut size = Vec::with_capacity(n);
    let mut gross = Vec::with_capacity(n);
    let mut fee = Vec::with_capacity(n);
    let mut fee_wb: Vec<Option<i128>> = Vec::with_capacity(n);
    let mut rate: Vec<Option<i128>> = Vec::with_capacity(n);
    let mut cat: Vec<Option<String>> = Vec::with_capacity(n);
    let mut obs = Vec::with_capacity(n);
    let mut recon: Vec<Option<i128>> = Vec::with_capacity(n);
    let mut resid: Vec<Option<i128>> = Vec::with_capacity(n);
    let mut verdict = Vec::with_capacity(n);
    let mut face = Vec::with_capacity(n);
    let mut chargeable = Vec::with_capacity(n);
    let mut title = Vec::with_capacity(n);
    let mut slug = Vec::with_capacity(n);
    let mut tx = Vec::with_capacity(n);
    for (f, r) in &pairs {
        event_key.push(f.event_key.clone());
        ts.push(f.timestamp);
        cond.push(f.condition_id.clone());
        token.push(f.token_id.clone());
        side.push(f.side.to_string());
        price.push(scale(f.price));
        size.push(scale(f.size));
        gross.push(scale(r.gross_usdc));
        fee.push(scale(r.fee_usdc.unwrap_or_default()));
        fee_wb.push(r.fee_wouldbe_usdc.map(scale));
        rate.push(r.fee_rate.map(rate_scale));
        cat.push(r.fee_category.clone());
        obs.push(scale(f.usdc_size));
        recon.push(r.reconstructed_debit.map(scale));
        resid.push(r.residual_usdc.map(scale));
        verdict.push(verdict_label(r.verdict).to_string());
        face.push(r.fill_face.map(|x| match x {
            tokor_domain::fees::FillFace::Maker => "maker",
            tokor_domain::fees::FillFace::Taker => "taker",
            tokor_domain::fees::FillFace::Indeterminate => "n/a",
        }));
        chargeable.push(matches!(
            r.fill_face,
            Some(tokor_domain::fees::FillFace::Taker)
        ));
        title.push(f.market_title.clone());
        slug.push(f.event_slug.clone());
        tx.push(tx_of_raw(&f.raw_json));
    }

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(event_key)),
            Arc::new(TimestampSecondArray::from(ts)),
            Arc::new(StringArray::from(cond)),
            Arc::new(StringArray::from(token)),
            Arc::new(StringArray::from(side)),
            Arc::new(
                Decimal128Array::from(price)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(size)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(gross)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(fee)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(fee_wb)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(rate)
                    .with_precision_and_scale(38, 4)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(StringArray::from(cat)),
            Arc::new(
                Decimal128Array::from(obs)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(recon)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(
                Decimal128Array::from(resid)
                    .with_precision_and_scale(38, DP)
                    .map_err(|e| format!("arrow decimal: {e}"))?,
            ),
            Arc::new(StringArray::from(verdict)),
            Arc::new(StringArray::from(face)),
            Arc::new(BooleanArray::from(chargeable)),
            Arc::new(StringArray::from(title)),
            Arc::new(StringArray::from(slug)),
            Arc::new(StringArray::from(tx)),
        ],
    )
    .map_err(|e| format!("batch: {e}"))?;

    let file = File::create(out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("write: {e}"))?;
    let meta = writer.close().map_err(|e| format!("close: {e}"))?;
    Ok(format!(
        "fees parquet: {} rows → {} ({} rows in file)",
        n,
        out.display(),
        meta.num_rows
    ))
}

fn write_activity_parquet(cfg: &AppConfig, out: &std::path::Path) -> CmdResult<String> {
    use arrow::array::{Int32Array, Int64Array, StringArray, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    let database = db(cfg)?;
    let events = tokor_persistence::activity::ActivityRepository::load_activity_events(
        &database, None, None,
    )
    .map_err(|e| format!("load events: {e}"))?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("event_key", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, true),
        Field::new("token_id", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("usdc_size", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("outcome_index", DataType::Int32, true),
        Field::new("outcome", DataType::Utf8, true),
        Field::new("market_title", DataType::Utf8, true),
        Field::new("event_slug", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, false),
        Field::new("occurrence", DataType::Int64, false),
    ]));
    let n = events.len();
    let mut key = Vec::with_capacity(n);
    let mut ts = Vec::with_capacity(n);
    let mut ty = Vec::with_capacity(n);
    let mut cond: Vec<Option<String>> = Vec::with_capacity(n);
    let mut tok: Vec<Option<String>> = Vec::with_capacity(n);
    let mut side: Vec<Option<String>> = Vec::with_capacity(n);
    let mut size: Vec<Option<String>> = Vec::with_capacity(n);
    let mut usdc: Vec<Option<String>> = Vec::with_capacity(n);
    let mut price: Vec<Option<String>> = Vec::with_capacity(n);
    let mut oidx: Vec<Option<i32>> = Vec::with_capacity(n);
    let mut outcome: Vec<Option<String>> = Vec::with_capacity(n);
    let mut title: Vec<Option<String>> = Vec::with_capacity(n);
    let mut slug: Vec<Option<String>> = Vec::with_capacity(n);
    let mut tx = Vec::with_capacity(n);
    let mut occ = Vec::with_capacity(n);
    for e in &events {
        key.push(e.event_key.clone());
        ts.push(e.timestamp);
        ty.push(e.event_type.as_str().to_string());
        cond.push(if e.condition_id.is_empty() {
            None
        } else {
            Some(e.condition_id.clone())
        });
        tok.push(if e.token_id.is_empty() {
            None
        } else {
            Some(e.token_id.clone())
        });
        side.push(e.side.map(|s| s.to_string()));
        size.push(e.size.map(|d| d.to_string()));
        usdc.push(e.usdc_size.map(|d| d.to_string()));
        price.push(e.price.map(|d| d.to_string()));
        oidx.push(e.outcome_index.map(|v| v as i32));
        outcome.push(e.outcome.clone());
        title.push(e.market_title.clone());
        slug.push(e.event_slug.clone());
        tx.push(e.transaction_hash.clone());
        occ.push(e.occurrence as i64);
    }
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(key)),
            Arc::new(TimestampSecondArray::from(ts)),
            Arc::new(StringArray::from(ty)),
            Arc::new(StringArray::from(cond)),
            Arc::new(StringArray::from(tok)),
            Arc::new(StringArray::from(side)),
            Arc::new(StringArray::from(size)),
            Arc::new(StringArray::from(usdc)),
            Arc::new(StringArray::from(price)),
            Arc::new(Int32Array::from(oidx)),
            Arc::new(StringArray::from(outcome)),
            Arc::new(StringArray::from(title)),
            Arc::new(StringArray::from(slug)),
            Arc::new(StringArray::from(tx)),
            Arc::new(Int64Array::from(occ)),
        ],
    )
    .map_err(|e| format!("batch: {e}"))?;
    let file = File::create(out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("write: {e}"))?;
    let meta = writer.close().map_err(|e| format!("close: {e}"))?;
    Ok(format!(
        "activity parquet: {} events → {} ({} rows in file)",
        n,
        out.display(),
        meta.num_rows
    ))
}

fn write_inventory_parquet(cfg: &AppConfig, out: &std::path::Path) -> CmdResult<String> {
    use arrow::array::{StringArray, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use rust_decimal::Decimal;

    let database = db(cfg)?;
    let events = tokor_persistence::activity::ActivityRepository::load_activity_events(
        &database, None, None,
    )
    .map_err(|e| format!("load events: {e}"))?;
    let positions =
        tokor_persistence::activity::ActivityRepository::load_positions_snapshot(&database)
            .map_err(|e| format!("load positions snapshot: {e}"))?;
    if positions.is_empty() {
        return Err("positions snapshot empty — run ingest-positions first".to_string());
    }

    // Per-token activity sums.
    let mut bought = HashMap::<String, Decimal>::new();
    let mut sold = HashMap::<String, Decimal>::new();
    let mut redeemed = HashMap::<String, Decimal>::new();
    let mut token_cond: HashMap<&String, &String> = HashMap::new();
    for e in &events {
        if !e.event_type.is_inventory_event() || e.token_id.is_empty() {
            continue;
        }
        let add = |m: &mut HashMap<String, Decimal>, t: &str, d: Decimal| {
            m.entry(t.to_string()).and_modify(|v| *v += d).or_insert(d);
        };
        match e.event_type {
            tokor_domain::activity::ActivityType::Trade => {
                let d = e.size.unwrap_or_default();
                match e.side {
                    Some(tokor_domain::activity::Side::Buy) => add(&mut bought, &e.token_id, d),
                    Some(tokor_domain::activity::Side::Sell) => add(&mut sold, &e.token_id, d),
                    None => {}
                }
            }
            tokor_domain::activity::ActivityType::Redeem => {
                add(&mut redeemed, &e.token_id, e.size.unwrap_or_default())
            }
            _ => {}
        }
        if !e.condition_id.is_empty() {
            token_cond.entry(&e.token_id).or_insert(&e.condition_id);
        }
    }

    // Per-condition MERGE total (pairs × $1); each merge consumes `merge_size`
    // shares from EACH complementary leg. Attribution across the two legs of a
    // condition is proportional where both legs need disposal; the derived
    // per-leg merged amount is `buys − sells − redeems − current_api` and the
    // condition-level identity Σ(derived) == 2 × merge_size is verified below.
    let mut merged_cond = HashMap::<String, Decimal>::new();
    for e in &events {
        if e.event_type == tokor_domain::activity::ActivityType::Merge {
            let d = e.size.unwrap_or_default();
            merged_cond
                .entry(e.condition_id.clone())
                .and_modify(|v| *v += d)
                .or_insert(d);
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("token_id", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("outcome", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("bought_shares", DataType::Utf8, false),
        Field::new("sold_shares", DataType::Utf8, false),
        Field::new("redeemed_shares", DataType::Utf8, false),
        Field::new("derived_merged_shares", DataType::Utf8, false),
        Field::new("api_current_shares", DataType::Utf8, false),
        Field::new("residual_shares", DataType::Utf8, false),
        Field::new("condition_merge_size", DataType::Utf8, true),
        Field::new("condition_merge_identity_ok", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("event_slug", DataType::Utf8, true),
    ]));

    // captured_at of the snapshot (shared row timestamp)
    let captured = chrono::Utc::now().timestamp();

    let mut ts_v = Vec::new();
    let mut tok_v = Vec::new();
    let mut cond_v = Vec::new();
    let mut outc_v = Vec::new();
    let mut stat_v = Vec::new();
    let mut b_v = Vec::new();
    let mut s_v = Vec::new();
    let mut r_v = Vec::new();
    let mut m_v = Vec::new();
    let mut c_v = Vec::new();
    let mut res_v = Vec::new();
    let mut cm_v: Vec<Option<String>> = Vec::new();
    let mut ok_v: Vec<Option<String>> = Vec::new();
    let mut title_v: Vec<Option<String>> = Vec::new();
    let mut slug_v: Vec<Option<String>> = Vec::new();

    // condition-level derived sums for identity verification
    let mut cond_derived: HashMap<String, Decimal> = HashMap::new();
    let mut leg_calc: Vec<(
        PositionSnapshotRow,
        Decimal, /*derived merged*/
        Decimal, /*residual*/
    )> = Vec::new();
    for p in &positions {
        let t = &p.token_id;
        let bought_v = bought.get(t).copied().unwrap_or_default();
        let sold_v = sold.get(t).copied().unwrap_or_default();
        let redeemed_v = redeemed.get(t).copied().unwrap_or_default();
        let current: Decimal = p
            .current_size
            .parse()
            .map_err(|e| format!("parse current_size {}: {e}", p.token_id))?;
        let derived_merged = bought_v - sold_v - redeemed_v - current;
        let residual = derived_merged; // attribution residual per leg
        cond_derived
            .entry(p.condition_id.clone())
            .and_modify(|v| *v += derived_merged)
            .or_insert(derived_merged);
        leg_calc.push((p.clone(), derived_merged, residual));
    }

    for (p, derived_merged, residual) in &leg_calc {
        let merge_size = merged_cond.get(&p.condition_id);
        let ok = match merge_size {
            Some(ms) => {
                let d = *cond_derived.get(&p.condition_id).unwrap_or(&Decimal::ZERO)
                    - Decimal::new(2, 0) * ms;
                Some(format!(
                    "{}",
                    d.abs() <= Decimal::new(2, 3) /* ±0.002 tolerance */
                ))
            }
            None => None,
        };
        ts_v.push(captured);
        tok_v.push(p.token_id.clone());
        cond_v.push(p.condition_id.clone());
        outc_v.push(p.outcome.clone());
        stat_v.push(p.status.clone());
        let t = &p.token_id;
        b_v.push(bought.get(t).copied().unwrap_or_default().to_string());
        s_v.push(sold.get(t).copied().unwrap_or_default().to_string());
        r_v.push(redeemed.get(t).copied().unwrap_or_default().to_string());
        m_v.push(derived_merged.to_string());
        c_v.push(p.current_size.clone());
        res_v.push(residual.to_string());
        cm_v.push(merge_size.map(|d| d.to_string()));
        ok_v.push(ok);
        title_v.push(p.title.clone());
        slug_v.push(p.event_slug.clone());
    }

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampSecondArray::from(ts_v)),
            Arc::new(StringArray::from(tok_v)),
            Arc::new(StringArray::from(cond_v)),
            Arc::new(StringArray::from(outc_v)),
            Arc::new(StringArray::from(stat_v)),
            Arc::new(StringArray::from(b_v)),
            Arc::new(StringArray::from(s_v)),
            Arc::new(StringArray::from(r_v)),
            Arc::new(StringArray::from(m_v)),
            Arc::new(StringArray::from(c_v)),
            Arc::new(StringArray::from(res_v)),
            Arc::new(StringArray::from(cm_v)),
            Arc::new(StringArray::from(ok_v)),
            Arc::new(StringArray::from(title_v)),
            Arc::new(StringArray::from(slug_v)),
        ],
    )
    .map_err(|e| format!("batch: {e}"))?;
    let file = File::create(out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("write: {e}"))?;
    let meta = writer.close().map_err(|e| format!("close: {e}"))?;
    let legs = leg_calc.len();
    let mut identities_ok = 0usize;
    for (p, _, _) in &leg_calc {
        if let Some(ms) = merged_cond.get(&p.condition_id) {
            let d = cond_derived
                .get(&p.condition_id)
                .copied()
                .unwrap_or_default()
                - Decimal::new(2, 0) * ms;
            if d.abs() <= Decimal::new(2, 3) {
                identities_ok += 1;
            }
        }
    }
    Ok(format!(
        "inventory parquet: {legs} legs → {} ({} bytes); merge-identity legs {identities_ok}/{legs}",
        out.display(),
        meta.num_rows
    ))
}

/// Extract transaction_hash from a stored raw row.
fn tx_of_raw(raw_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw_json)
        .ok()
        .and_then(|v| {
            v.get("transaction_hash")
                .and_then(|h| h.as_str().map(|s| s.to_string()))
        })
        .unwrap_or_default()
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

// ------------------------------------------------- recorder (Phase 2 §3–§4)

/// Wall-clock UTC nanos — the stamp currency for §4 latency chains.
fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
}

/// Phase 2 §3: run the public market-data recorder against the given tokens
/// until ctrl-c. Read-only (WS market channel + REST book snapshots); no
/// keys, no orders. Subscriptions persist in the DB ledger, so a restarted
/// session resumes the same set.
pub async fn record_market(cfg: &AppConfig, tokens: &[String]) -> CmdResult<String> {
    let database = std::sync::Arc::new(db(cfg)?);
    for t in tokens {
        database
            .add_subscription(&SubscriptionRow {
                token_id: t.clone(),
                asset_type: String::new(),
                slug: String::new(),
                added_reason: "manual".into(),
            })
            .map_err(|e| format!("subscription {t}: {e}"))?;
    }
    let config = RecorderConfig {
        rest_base: cfg.clob_base.clone(),
        ..RecorderConfig::default()
    };
    let handle = RecorderHandle::start(config, database.clone());
    tracing::info!(tokens = tokens.len(), "recorder running; ctrl-c to stop");
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| format!("signal: {e}"))?;
    let _ = handle.cmd_tx.send(RecorderCommand::Shutdown).await;
    // Give the workers a moment to drain their bounded queues.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (raw, states, trades, health) = database
        .market_data_counts()
        .map_err(|e| format!("counts: {e}"))?;
    Ok(format!(
        "recorder session ended: {raw} raw frames, {states} normalized states, \
         {trades} public trades, {health} health events"
    ))
}

/// Phase 2 §4: independent-latency probe of the wallet-feed → decision chain.
///
/// Measures OUR pipeline stages with wall-clock nanos — it makes NO claim
/// about exchange publication latency (wallet timestamps are 1s-resolution;
/// docs/07 §3). Uses the raw provider WITHOUT the retry wrapper: a retry
/// would contaminate the latency distribution.
pub async fn observe_source(cfg: &AppConfig, probes: u64) -> CmdResult<String> {
    let database = db(cfg)?;
    let http = HttpWalletFeed::new(&cfg.data_api_base).map_err(|e| format!("http client: {e}"))?;
    let book_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| format!("book client: {e}"))?;

    let filters = ActivityFilters {
        user: cfg.wallet_address.clone(),
        record_types: vec![ActivityType::Trade],
        limit: 100,
    };

    let mut poll_ms = Vec::new();
    let mut first_seen_ms = Vec::new();
    let mut book_ms = Vec::new();
    let mut decision_ms = Vec::new();
    let mut recorded = 0u64;

    for probe in 0..probes {
        let poll_start_ns = now_ns();
        let request = DataApiActivityRequest {
            filters: filters.clone(),
            cursor: None,
        };
        let page = match http.fetch_activity_page(&request).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(probe, error = %e, "probe fetch failed; skipping");
                continue;
            }
        };
        let http_response_ns = now_ns();

        // First-seen: first normalized fill located in the parsed batch
        // (provider-internal parse is included in this stage — documented).
        let Some(fill) = page.batch.fills.first() else {
            tracing::warn!(probe, "probe page had no TRADE fills; skipping");
            continue;
        };
        let first_seen_ns = now_ns();

        // Book probe: decision-readiness = a parseable fresh book for that
        // token (the recorder's `is_decision_ready` equivalent, one-shot).
        let url = format!("{}/book?token_id={}", cfg.clob_base, fill.token_id);
        let resp = match book_http.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(probe, error = %e, "book probe failed; skipping");
                continue;
            }
        };
        let book_ts_ns = now_ns();
        let body = resp.text().await.map_err(|e| format!("book body: {e}"))?;
        let decision_ready_ns = now_ns();
        // Decision-readiness requires a *parseable* book; an unparseable
        // body is recorded as not-yet-ready (None) rather than dropped.
        let book_ok = parse_book_body(&body).is_some();

        poll_ms.push((http_response_ns - poll_start_ns) as f64 / 1e6);
        first_seen_ms.push((first_seen_ns - http_response_ns) as f64 / 1e6);
        book_ms.push((book_ts_ns - first_seen_ns) as f64 / 1e6);
        decision_ms.push((decision_ready_ns - poll_start_ns) as f64 / 1e6);

        database
            .record_source_observation(&SourceObservationRow {
                event_key: Some(fill.event_key.clone()),
                raw_row_index: 0,
                exchange_ts: Some(fill.timestamp),
                poll_start_ns,
                http_response_ns,
                first_seen_ns,
                normalized_ns: Some(first_seen_ns),
                decision_ready_ns: book_ok.then_some(decision_ready_ns),
                book_ts_ns: Some(book_ts_ns),
                page_url: url,
                run_id: format!("observe-{}", now_ns()),
            })
            .map_err(|e| format!("observation write: {e}"))?;
        recorded += 1;
    }

    let fmt = |label: &str, v: &mut Vec<f64>| -> String {
        if v.is_empty() {
            return format!("{label}: n/a");
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        format!(
            "{label}: p50={:.1}ms p95={:.1}ms p99={:.1}ms max={:.1}ms",
            percentile(v, 0.50),
            percentile(v, 0.95),
            percentile(v, 0.99),
            v.last().copied().unwrap_or(0.0)
        )
    };
    Ok(format!(
        "source observations: {recorded}/{probes} probes recorded\n  \
         poll→response   {}\n  response→seen   {}\n  seen→book       {}\n  end-to-end      {}",
        fmt("t", &mut poll_ms),
        fmt("t", &mut first_seen_ms),
        fmt("t", &mut book_ms),
        fmt("t", &mut decision_ms),
    ))
}

/// Nearest-rank percentile on a pre-sorted slice.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((q * (sorted.len() as f64 - 1.0)).ceil()) as usize;
    sorted[idx.min(sorted.len() - 1)]
}
