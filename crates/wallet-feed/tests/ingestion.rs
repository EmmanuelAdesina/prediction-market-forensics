//! Integration tests for the wallet feed: real HTTP semantics over wiremock,
//! multi-page cursor walk, retries, rate limiting, restart recovery.
//!
//! All tests are credential-free: the upstream is a local mock.

use serde_json::{json, Value};
use std::time::Duration;
use tokor_domain::activity::{ActivityFilters, ActivityType};
use tokor_persistence::ingest::IngestRepository;
use tokor_persistence::Database;
use tokor_wallet_feed::http::HttpWalletFeed;
use tokor_wallet_feed::ingestor::FullHistoryIngestor;
use tokor_wallet_feed::provider::RetryingProvider;
use wiremock::matchers::{method, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn filters(limit: u64) -> ActivityFilters {
    ActivityFilters {
        user: "0xa19cbababc312f9df185e49d7004c249ed1ade6b".into(),
        record_types: vec![ActivityType::Trade],
        limit,
    }
}

fn trade_row(ts: i64, price: f64, size: f64, side: &str, uniq: &str) -> Value {
    json!({
        "proxy_wallet": "0xa19cbababc312f9df185e49d7004c249ed1ade6b",
        "timestamp": ts,
        "condition_id": "0x92d87c1b3bebe88f45c31bb914d87be3d6f2a47d7cdf10d7210ac0eb92f26389",
        "type": "TRADE",
        "size": size,
        "usdc_size": price * size,
        "transaction_hash": format!("0xhash-{uniq}"),
        "price": price,
        "token_id": "47441866156534507758155137441786829795495954872731490670045397853956032792663",
        "side": side,
        "outcome_index": 0,
        "title": "Will Bitcoin dip to $73,000 on November 9?",
        "slug": "will-bitcoin-dip-to-73k-on-november-9-2026",
        "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png",
        "event_slug": "what-price-will-bitcoin-hit-on-november-9-2026",
        "outcome": "Yes",
        "name": "Tokor",
        "pseudonym": "Taut-Ease-Bloodflow"
    })
}

fn page(data: Vec<Value>, next_cursor: Option<&str>, has_more: bool, limit: u64) -> Value {
    json!({
        "data": data,
        "pagination": {
            "limit": limit,
            "offset": 0,
            "has_more": has_more,
            "next_cursor": next_cursor.unwrap_or("")
        }
    })
}

#[tokio::test]
async fn multi_page_cursor_walk_persists_everything() {
    let server = MockServer::start().await;
    let f = filters(2);

    // Page 1 (no cursor): 2 rows, more available.
    Mock::given(method("GET"))
        .and(query_param("limit", "2"))
        .and(wiremock::matchers::query_param_is_missing("cursor"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![
                trade_row(1000, 0.09, 10.0, "BUY", "a"),
                trade_row(1010, 0.10, 20.0, "BUY", "b"),
            ],
            Some("CUR_A"),
            true,
            2,
        )))
        .mount(&server)
        .await;
    // Page 2 (cursor=CUR_A): 1 row + 1 duplicate, no more.
    Mock::given(method("GET"))
        .and(query_param("limit", "2"))
        .and(query_param("cursor", "CUR_A"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![
                trade_row(1020, 0.11, 30.0, "SELL", "c"),
                trade_row(1010, 0.10, 20.0, "BUY", "b"), // duplicate of page 1
            ],
            Some("CUR_B"),
            false,
            2,
        )))
        .mount(&server)
        .await;

    let provider = RetryingProvider::new(HttpWalletFeed::new(server.uri().as_str()).unwrap());

    let db = Database::open_memory().unwrap();
    let ingestor = FullHistoryIngestor::new(&provider, &db);
    let progress = ingestor.run(&f, "run-1").await.unwrap();

    assert_eq!(progress.pages, 2);
    assert_eq!(progress.fills_inserted, 3); // duplicate row deduped
    assert_eq!(progress.fills_duplicate, 1);
    assert!(progress.complete);
    assert_eq!(db.fills_count().unwrap(), 3);
    // Raw forensic pages preserved.
    let raw = db.ingest_raw_page_count().unwrap();
    assert_eq!(raw, 2);
}

#[tokio::test]
async fn rate_limit_backoff_and_final_success() {
    let server = MockServer::start().await;
    let f = filters(2);

    let page = page(
        vec![trade_row(1000, 0.09, 10.0, "BUY", "a")],
        Some("C"),
        false,
        2,
    );

    // Deterministic two-phase responder: 429 first, then success. The
    // Retry-After hint of 1s dominates the full-jitter base backoff
    // ([0, 400]ms at attempt 0), making the pause deterministic - the old
    // hint=0 form asserted on a jitter roll and flaked ~25% of runs.
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    let counter = Arc::new(AtomicUsize::new(0));
    let c2 = counter.clone();
    let responder = move |_req: &wiremock::Request| {
        let n = c2.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(429)
                .append_header("Retry-After", "1")
                .set_body_json(json!({"error":"rate"}))
        } else {
            ResponseTemplate::new(200).set_body_json(page.clone())
        }
    };
    Mock::given(method("GET"))
        .respond_with(responder)
        .mount(&server)
        .await;

    let provider = RetryingProvider::new(HttpWalletFeed::new(server.uri().as_str()).unwrap());
    let db = Database::open_memory().unwrap();
    let ingestor = FullHistoryIngestor::new(&provider, &db);
    let t0 = std::time::Instant::now();
    let progress = ingestor.run(&f, "run-rl").await.unwrap();
    // The server hint is honored (delay = max(base jitter, 1000ms) = 1000ms).
    assert!(
        t0.elapsed() > Duration::from_millis(900),
        "expected the Retry-After pause to be honored"
    );
    assert_eq!(progress.fills_inserted, 1);
    assert!(progress.complete);
}

#[tokio::test]
async fn transient_503_retries_then_succeeds() {
    let server = MockServer::start().await;
    let f = filters(2);
    let page = page(
        vec![trade_row(1000, 0.09, 10.0, "BUY", "a")],
        Some("C"),
        false,
        2,
    );

    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page))
        .mount(&server)
        .await;

    let provider = RetryingProvider::new(HttpWalletFeed::new(server.uri().as_str()).unwrap());
    let db = Database::open_memory().unwrap();
    let progress = FullHistoryIngestor::new(&provider, &db)
        .run(&f, "run-503")
        .await
        .unwrap();
    assert_eq!(progress.fills_inserted, 1);
}

#[tokio::test]
async fn permanent_404_aborts_run() {
    let server = MockServer::start().await;
    let f = filters(2);
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_string("nope"))
        .mount(&server)
        .await;

    let provider = RetryingProvider::new(HttpWalletFeed::new(server.uri().as_str()).unwrap());
    let db = Database::open_memory().unwrap();
    let ingestor = FullHistoryIngestor::new(&provider, &db);
    let err = ingestor.run(&f, "run-404").await;
    assert!(err.is_err());
    // The run is aborted, not corrupted: no fills, cursor not advanced.
    assert_eq!(db.fills_count().unwrap(), 0);
}

#[tokio::test]
async fn restart_recovers_from_durable_cursor() {
    // Restart semantics: run a full ingest with a 1-page budget, drop the DB
    // handle ("crash"), re-open the FILE database, verify cursor survival.
    let server = MockServer::start().await;
    let f = filters(2);
    Mock::given(method("GET"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![trade_row(1000, 0.09, 10.0, "BUY", "a")],
            Some("CUR_1"),
            true,
            2,
        )))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("cursor", "CUR_1"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            vec![trade_row(1100, 0.12, 15.0, "BUY", "b2")],
            Some("CUR_2"),
            false,
            2,
        )))
        .mount(&server)
        .await;

    let path = {
        let dir = std::env::temp_dir().join("tokor-feed-restart");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("restart.db")
    };
    let _ = std::fs::remove_file(&path);

    {
        let db = Database::open(&path).unwrap();
        let provider = RetryingProvider::new(HttpWalletFeed::new(server.uri().as_str()).unwrap());
        let mut ingestor = FullHistoryIngestor::new(&provider, &db);
        ingestor.max_pages = 1; // simulate crashing after one page
        let p = ingestor.run(&f, "run-restart").await.unwrap();
        assert_eq!(p.pages, 1);
        assert!(!p.complete);
        assert_eq!(db.fills_count().unwrap(), 1);
    } // "crash": database object dropped

    // Restart: reopen the same file. The durable cursor must have survived.
    let db = Database::open(&path).unwrap();
    let (cursor, pages) = db.load_cursor(f.fingerprint().as_str()).unwrap().unwrap();
    assert_eq!(cursor.as_deref(), Some("CUR_1"));
    assert_eq!(pages, 1);
    assert_eq!(db.fills_count().unwrap(), 1);

    let _ = std::fs::remove_file(&path);
}
