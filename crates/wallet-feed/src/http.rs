//! HTTP-backed Data API v2 activity provider.

use std::time::Duration;

use tokor_domain::activity::IngestionBatch;
use tokor_domain::error::{IngestionError, parse_retry_after};

use crate::provider::{DataApiActivityRequest, FetchedPage, WalletFeedError, WalletFeedProvider, build_activity_url};

pub struct HttpWalletFeed {
    client: reqwest::Client,
    base_url: String,
}

const USER_AGENT: &str = "tokor-engine/0.1 (research wallet-activity ingestion)";

impl HttpWalletFeed {
    pub fn new(base_url: &str) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            client,
            base_url: base_url.to_string(),
        })
    }

    pub fn with_client(client: reqwest::Client, base_url: String) -> Self {
        Self { client, base_url }
    }
}

impl WalletFeedProvider for HttpWalletFeed {
    async fn fetch_activity_page(
        &self,
        request: &DataApiActivityRequest,
    ) -> Result<FetchedPage, WalletFeedError> {
        let url = build_activity_url(&self.base_url, &request.filters, request.cursor.as_deref());
        let query = url
            .split_once('?')
            .map(|(_, q)| q.to_string())
            .unwrap_or_default();

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| WalletFeedError::Upstream(IngestionError::Transient {
                code: 0,
                message: format!("transport: {e}"),
            }))?;

        let status = response.status().as_u16();
        if (200..300).contains(&status) {
            let body: serde_json::Value = response
                .json()
                .await
                .map_err(|e| WalletFeedError::Transport(format!("body decode: {e}")))?;
            let batch = IngestionBatch::parse(&body, &request.filters)
                .map_err(WalletFeedError::Upstream)?;
            return Ok(FetchedPage {
                batch,
                url,
                query_params: query,
                http_status: status,
            });
        }

        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.to_string());
        let text = response.text().await.unwrap_or_default();

        let ingestion_err = match status {
            429 => IngestionError::RateLimited {
                retry_after_hint: parse_retry_after(retry_after.as_deref()),
                message: trunc(&text, 200),
            },
            c if c >= 500 || c == 408 => IngestionError::Transient {
                code: c,
                message: trunc(&text, 200),
            },
            c => IngestionError::Permanent {
                code: c,
                message: trunc(&text, 400),
            },
        };
        Err(WalletFeedError::Upstream(ingestion_err))
    }
}

fn trunc(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}
