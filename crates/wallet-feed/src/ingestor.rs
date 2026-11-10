//! Full-history wallet ingestion: cursor walk + durable checkpointing.
//!
//! Invariants:
//! - The durable cursor advances **atomically with** the page's fills
//!   (`IngestRepository::commit_page` wraps fills + raw capture + run stats +
//!   cursor in one SQLite transaction). A crash mid-walk always resumable.
//! - Replay is idempotent by `event_key` (INSERT OR IGNORE).
//! - Full-history walks dedupe against the store; resumable walks continue
//!   from the recorded cursor for the same filter set only.
//! - has_more=true without next_cursor is a feed inconsistency → hard abort.

use tokor_domain::activity::{ActivityFilters, WalletCursor};
use tokor_persistence::ingest::IngestRepository;

use crate::provider::{
    build_activity_url, DataApiActivityRequest, WalletFeedError, WalletFeedProvider,
};

pub struct FullHistoryIngestor<'a, P: WalletFeedProvider> {
    pub provider: &'a P,
    pub db: &'a tokor_persistence::Database,
    /// True → walk to the start of history (dedupe-safe). False → sync only
    /// new activity from the durable cursor forward.
    pub full: bool,
    /// Pause between pages — politeness for the Data API.
    pub page_pause: std::time::Duration,
    /// Hard per-run budget; a bigger history is simply continued next run.
    pub max_pages: u64,
}

#[derive(Debug, Clone)]
pub struct IngestProgress {
    pub pages: u64,
    pub fills_inserted: u64,
    pub fills_duplicate: u64,
    pub non_trade_records: u64,
    pub complete: bool,
    pub last_cursor: Option<String>,
    pub terminal_reason: &'static str,
}

impl<'a, P: WalletFeedProvider> FullHistoryIngestor<'a, P> {
    pub fn new(provider: &'a P, db: &'a tokor_persistence::Database) -> Self {
        Self {
            provider,
            db,
            full: true,
            page_pause: std::time::Duration::from_millis(1200),
            max_pages: 100_000,
        }
    }

    /// Run one ingestion session.
    pub async fn run(
        &self,
        filters: &ActivityFilters,
        run_id: &str,
    ) -> Result<IngestProgress, IngestRunError> {
        let fingerprint = filters.fingerprint();
        let filters_json = serde_json::to_string(filters).map_err(|e| {
            IngestRunError::Storage(tokor_persistence::PersistError::Misuse(format!(
                "filters: {e}"
            )))
        })?;
        self.db.start_run(run_id, &filters_json, &fingerprint)?;

        let durable: Option<WalletCursor> = {
            let (cursor, pages) = self.db.load_cursor(&fingerprint)?.unwrap_or((None, 0));
            Some(WalletCursor {
                last_cursor: cursor,
                filters_fingerprint: fingerprint.clone(),
                updated_at: None,
                pages_committed: pages,
            })
        };

        let mut cursor: Option<String> = if self.full {
            None
        } else {
            durable.as_ref().and_then(|c| c.last_cursor.clone())
        };

        let mut page_index: u64 = 0;
        let mut progress = IngestProgress {
            pages: 0,
            fills_inserted: 0,
            fills_duplicate: 0,
            non_trade_records: 0,
            complete: false,
            last_cursor: cursor.clone(),
            terminal_reason: "start",
        };

        loop {
            if page_index >= self.max_pages {
                progress.terminal_reason = "max_pages_budget_exhausted";
                break;
            }

            // Politeness pause between pages (never before the first page).
            if page_index > 0 {
                tokio::time::sleep(self.page_pause).await;
            }

            let request = DataApiActivityRequest {
                filters: filters.clone(),
                cursor: cursor.clone(),
            };
            let page = self.provider.fetch_activity_page(&request).await?;
            let batch = &page.batch;

            // A page with no data, no more, and no cursor ends the walk.
            if batch.fills.is_empty() && !batch.has_more && batch.next_cursor.is_none() {
                progress.terminal_reason = "backend_empty_no_more";
                break;
            }

            let checkpoint = self.db.commit_page(
                &tokor_persistence::ingest::PageRecord {
                    run_id: run_id.to_string(),
                    page_index,
                    fetched_at: chrono::Utc::now().to_rfc3339(),
                    http_status: page.http_status,
                    url: page.url.clone(),
                    query_params: page.query_params.clone(),
                    body_json: serialize_body(&batch.raw_body),
                    next_cursor: batch.next_cursor.clone(),
                    has_more: batch.has_more,
                },
                &batch.fills,
            )?;

            progress.pages = page_index + 1;
            progress.fills_inserted += checkpoint.fills_inserted;
            progress.fills_duplicate += checkpoint.fills_already_seen;
            progress.non_trade_records += batch.non_trade_records as u64;
            progress.last_cursor = batch.next_cursor.clone();
            page_index += 1;

            if !batch.has_more {
                progress.complete = true;
                progress.terminal_reason = "backend_no_more";
                break;
            }

            let Some(next) = batch.next_cursor.clone() else {
                let err = tokor_persistence::PersistError::Misuse(
                    "feed inconsistency: has_more=true but next_cursor missing".into(),
                );
                return Err(IngestRunError::Storage(err));
            };
            cursor = Some(next);
        }

        self.db.finish_run(
            run_id,
            if progress.complete {
                "COMPLETE"
            } else {
                "ABORTED"
            },
            progress.last_cursor.as_deref(),
        )?;
        Ok(progress)
    }

    /// Reconstructible URL for the forensic record — same builder used by
    /// providers, recorded for feasibility checks during restart analysis.
    #[allow(dead_code)]
    fn record_url(&self, filters: &ActivityFilters, cursor: Option<&str>) -> String {
        build_activity_url(crate::DATA_API_DEFAULT, filters, cursor)
    }
}

fn serialize_body(v: &serde_json::Value) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string())
}

pub const DATA_API_DEFAULT: &str = "https://data-api.polymarket.com/v2";

#[derive(Debug, thiserror::Error)]
pub enum IngestRunError {
    #[error("provider error: {0}")]
    Provider(#[from] WalletFeedError),
    #[error("storage error: {0}")]
    Storage(#[from] tokor_persistence::PersistError),
}
