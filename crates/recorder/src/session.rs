//! WebSocket session lifecycle for the market-data recorder.
//!
//! Responsibilities (docs/07):
//! * One live connection at a time; on disconnect the retained book states are
//!   invalidated (never trusted across a reconnect) and a fresh REST snapshot
//!   per subscribed token rebuilds them — the ONLY way a book returns to
//!   decision-readiness after an invalidate.
//! * Reconnects back off exponentially with full jitter, capped.
//! * All frames are stamped on receipt (wall-clock nanos) and persisted
//!   verbatim (raw layer) before normalization; bounded queues give explicit
//!   backpressure, never silent drops.
//! * Subscriptions are dynamic: the DB ledger (`market_subscriptions`) is the
//!   source of truth; a command bumps a generation counter, the connector
//!   closes and re-subscribes with the new set on the next connect.
//! * Gap detection (timestamp regression, no-base-state) and staleness flag
//!   health events AND trigger an immediate REST snapshot rescue for that
//!   token; books that go stale are refused by decision paths (consumers gate
//!   on `is_decision_ready`) — the recorder only records.
//!
//! Strictly read-only public data: no keys, no auth, no orders.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

use tokor_persistence::market_data::{
    BookStateRow, DeltaRow, HealthRow, MarketDataRepository, RawMessageRow, SubscriptionRow,
    TradeEventRow,
};

use crate::book::{ApplyOutcome, BookEngine};
use crate::messages::{parse_book_body, parse_market_message, subscribe_body, MarketMessage};
use crate::{backoff_delay, now_ns, RecorderConfig};

/// Commands the supervisor accepts from CLI/other components.
#[derive(Debug, Clone)]
pub enum RecorderCommand {
    Subscribe { token_id: String, slug: String },
    Unsubscribe(String),
    Shutdown,
}

/// Everything one inbound event produced, ready for the persistence workers.
#[derive(Debug, Default)]
pub struct Processed {
    pub raw: Vec<RawMessageRow>,
    pub deltas: Vec<DeltaRow>,
    pub states: Vec<BookStateRow>,
    pub trades: Vec<TradeEventRow>,
    pub health: Vec<HealthRow>,
    /// Tokens needing a REST snapshot rescue (gap detected, freshly stale).
    pub needs_refresh: Vec<String>,
}

impl Processed {
    fn is_empty(&self) -> bool {
        self.raw.is_empty()
            && self.deltas.is_empty()
            && self.states.is_empty()
            && self.trades.is_empty()
            && self.health.is_empty()
            && self.needs_refresh.is_empty()
    }
}

/// Pure recording logic: routes parsed wire messages into per-token book
/// engines and emits persistence rows. Fully offline-testable; the async
/// session below only wires I/O around it.
pub struct RecordingEngine {
    session_id: String,
    config: RecorderConfig,
    books: BTreeSet<String>,
    engines: std::collections::BTreeMap<String, BookEngine>,
    /// Tokens whose staleness was already flagged (transition-only health).
    stale_flagged: BTreeSet<String>,
}

impl RecordingEngine {
    pub fn new(session_id: impl Into<String>, config: RecorderConfig) -> Self {
        Self {
            session_id: session_id.into(),
            config,
            books: BTreeSet::new(),
            engines: std::collections::BTreeMap::new(),
            stale_flagged: BTreeSet::new(),
        }
    }

    /// The tokens this session has seen traffic for.
    pub fn tokens(&self) -> &BTreeSet<String> {
        &self.books
    }

    fn engine(&mut self, token_id: &str) -> &mut BookEngine {
        self.engines
            .entry(token_id.to_string())
            .or_insert_with(|| BookEngine::new(token_id, self.config.max_book_staleness))
    }

    fn health(&self, channel: &str, event: &str, detail: serde_json::Value) -> HealthRow {
        HealthRow {
            session_id: self.session_id.clone(),
            channel: channel.into(),
            event: event.into(),
            detail_json: detail.to_string(),
            at: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn on_connected(&mut self) -> Processed {
        Processed {
            health: vec![self.health("ws:market", "connect", serde_json::json!({}))],
            ..Processed::default()
        }
    }

    /// Disconnect path: every retained state is integrity-suspect until a
    /// fresh snapshot (never trusted across a reconnect).
    fn on_disconnected(&mut self) -> Processed {
        for eng in self.engines.values_mut() {
            eng.invalidate();
        }
        self.stale_flagged.clear();
        Processed {
            health: vec![self.health("ws:market", "disconnect", serde_json::json!({}))],
            ..Processed::default()
        }
    }

    /// One verbatim WS text frame. The raw row is always produced (evidence
    /// layer); normalization follows only for known shapes.
    pub fn on_ws_frame(&mut self, text: &str, received_ns: i64) -> Processed {
        let mut out = Processed::default();
        out.raw.push(RawMessageRow {
            session_id: self.session_id.clone(),
            channel: "ws:market".into(),
            token_id: token_of(text),
            payload: text.to_string(),
            received_at: chrono::Utc::now().to_rfc3339(),
            received_ns,
        });
        match parse_market_message(text) {
            MarketMessage::Book(b) => {
                let token = b.asset_id.clone();
                self.books.insert(token.clone());
                if let ApplyOutcome::Normalized(st) =
                    self.engine(&token).apply_snapshot(&b, received_ns)
                {
                    out.states.push(state_row(
                        &self.session_id,
                        &token,
                        &st,
                        "ws_live",
                        received_ns,
                    ));
                }
            }
            MarketMessage::PriceChange(p) => {
                let token = p.asset_id.clone();
                self.books.insert(token.clone());
                out.deltas.push(DeltaRow {
                    token_id: token.clone(),
                    event_type: "price_change".into(),
                    changes_json: serde_json::json!(&p.changes).to_string(),
                    book_hash: p.hash.clone(),
                    exchange_ts: p.timestamp.clone(),
                    received_ns,
                    session_id: self.session_id.clone(),
                });
                match self.engine(&token).apply_price_change(&p, received_ns) {
                    ApplyOutcome::Normalized(st) => {
                        out.states.push(state_row(
                            &self.session_id,
                            &token,
                            &st,
                            "ws_live",
                            received_ns,
                        ));
                    }
                    ApplyOutcome::NeedsSnapshot(reason) => {
                        out.needs_refresh.push(token.clone());
                        out.health.push(self.health(
                            "ws:market",
                            "gap_detected",
                            serde_json::json!({ "token": token, "reason": format!("{reason:?}") }),
                        ));
                    }
                    ApplyOutcome::Ignored => {}
                }
            }
            MarketMessage::LastTrade(t) => {
                let size = t.size.clone().unwrap_or_default();
                if !size.is_empty() {
                    self.books.insert(t.asset_id.clone());
                    out.trades.push(TradeEventRow {
                        token_id: t.asset_id.clone(),
                        price: t.price.clone(),
                        size,
                        side: t.side.clone(),
                        trade_hash: t.hash.clone(),
                        exchange_ts: t.timestamp.clone(),
                        received_ns,
                        session_id: self.session_id.clone(),
                    });
                }
            }
            MarketMessage::TickSize(_) | MarketMessage::Other(_) => {
                // Recorded raw above; normalization has no use for these yet.
            }
        }
        out
    }

    /// A REST book snapshot (post-reconnect refresh or gap rescue).
    pub fn on_rest_snapshot(&mut self, token_id: &str, body: &str, received_ns: i64) -> Processed {
        let mut out = Processed::default();
        out.raw.push(RawMessageRow {
            session_id: self.session_id.clone(),
            channel: "rest:book".into(),
            token_id: token_id.into(),
            payload: body.to_string(),
            received_at: chrono::Utc::now().to_rfc3339(),
            received_ns,
        });
        match parse_book_body(body) {
            Some(b) if b.asset_id == token_id => {
                match self.engine(token_id).apply_snapshot(&b, received_ns) {
                    ApplyOutcome::Normalized(st) => {
                        self.books.insert(token_id.to_string());
                        self.stale_flagged.remove(token_id);
                        out.health.push(self.health(
                            "rest:book",
                            "snapshot_refresh",
                            serde_json::json!({
                                "token": token_id,
                                "quality": st.quality.as_str(),
                            }),
                        ));
                        out.states.push(state_row(
                            &self.session_id,
                            token_id,
                            &st,
                            "rest_snapshot",
                            received_ns,
                        ));
                    }
                    ApplyOutcome::NeedsSnapshot(_) | ApplyOutcome::Ignored => {}
                }
            }
            _ => {
                // Unparseable body: raw row above; never fabricate a state.
                out.health.push(self.health(
                    "rest:book",
                    "snapshot_parse_failed",
                    serde_json::json!({ "token": token_id }),
                ));
            }
        }
        out
    }

    /// Periodic sweep: flag freshly stale books once (health ledger) and
    /// queue them for a REST rescue.
    pub fn sweep_staleness(&mut self, now_ns: i64) -> Processed {
        let mut out = Processed::default();
        // Collect first: the health() helper needs &self while engines is
        // mutably borrowed for the staleness check.
        let mut stale: Vec<String> = Vec::new();
        for (token, eng) in &mut self.engines {
            if eng.is_stale(now_ns) && self.stale_flagged.insert(token.clone()) {
                stale.push(token.clone());
            }
        }
        for token in stale {
            out.needs_refresh.push(token.clone());
            out.health.push(self.health(
                "ws:market",
                "stale_reject",
                serde_json::json!({ "token": token, "now_ns": now_ns }),
            ));
        }
        out
    }
}

fn token_of(text: &str) -> String {
    serde_json::from_str::<serde_json::Value>(text)
        .ok()
        .and_then(|v| {
            v.get("asset_id")
                .and_then(|a| a.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default()
}

fn state_row(
    session_id: &str,
    token_id: &str,
    st: &crate::book::BookState,
    source: &str,
    first_seen_ns: i64,
) -> BookStateRow {
    let (bids_json, asks_json) = st.to_json();
    BookStateRow {
        token_id: token_id.to_string(),
        bids_json,
        asks_json,
        best_bid: st.best_bid().map(|l| l.price.to_string()),
        best_ask: st.best_ask().map(|l| l.price.to_string()),
        bid_depth: Some(
            st.bids
                .iter()
                .map(|l| l.size)
                .sum::<rust_decimal::Decimal>()
                .to_string(),
        ),
        ask_depth: Some(
            st.asks
                .iter()
                .map(|l| l.size)
                .sum::<rust_decimal::Decimal>()
                .to_string(),
        ),
        book_hash: st.hash.clone(),
        quality: st.quality.as_str().into(),
        source: source.into(),
        exchange_ts: st.exchange_ts.clone(),
        first_seen_ns,
        normalized_at: chrono::Utc::now().to_rfc3339(),
        session_id: session_id.to_string(),
    }
}

/// Live recorder: owns the supervisor, the connector task, and the
/// persistence workers. Dropping the handle detaches the tasks (the shutdown
/// command is the clean stop path — workers drain what they hold).
pub struct RecorderHandle {
    pub cmd_tx: mpsc::Sender<RecorderCommand>,
}

impl RecorderHandle {
    /// Start the recorder. Returns immediately.
    pub fn start(config: RecorderConfig, db: Arc<tokor_persistence::Database>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<RecorderCommand>(64);
        let (generation_tx, generation_rx) = watch::channel(0u64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (raw_tx, raw_rx) = mpsc::channel::<RawMessageRow>(config.raw_queue_depth);
        let (write_tx, write_rx) = mpsc::channel::<Write>(config.state_queue_depth);

        // Persistence workers: bounded queues, batched transactions.
        tokio::spawn(raw_writer(raw_rx, db.clone()));
        tokio::spawn(state_writer(write_rx, db.clone()));

        // Supervisor: commands → DB ledger + generation bump (the connector
        // reconnects and re-reads the ledger); Shutdown → stop flag.
        tokio::spawn(supervise(cmd_rx, db.clone(), generation_tx, shutdown_tx));

        // Connector: one live WS session at a time.
        tokio::spawn(connector(
            config,
            db,
            generation_rx,
            shutdown_rx,
            raw_tx,
            write_tx,
        ));

        Self { cmd_tx }
    }
}

async fn supervise(
    mut cmd_rx: mpsc::Receiver<RecorderCommand>,
    db: Arc<tokor_persistence::Database>,
    generation_tx: watch::Sender<u64>,
    shutdown_tx: watch::Sender<bool>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            RecorderCommand::Subscribe { token_id, slug } => {
                let row = SubscriptionRow {
                    token_id,
                    asset_type: String::new(),
                    slug,
                    added_reason: "manual".into(),
                };
                if let Err(e) = db.add_subscription(&row) {
                    tracing::error!(error = %e, "subscribe ledger write failed");
                }
                generation_tx.send_modify(|g| *g += 1);
            }
            RecorderCommand::Unsubscribe(token_id) => {
                if let Err(e) = db.remove_subscription(&token_id, "manual") {
                    tracing::error!(error = %e, "unsubscribe ledger write failed");
                }
                generation_tx.send_modify(|g| *g += 1);
            }
            RecorderCommand::Shutdown => {
                let _ = shutdown_tx.send(true);
                return;
            }
        }
    }
}

/// Rows destined for the non-raw persistence worker.
enum Write {
    State(BookStateRow),
    Delta(DeltaRow),
    Trade(TradeEventRow),
    Health(HealthRow),
}

async fn raw_writer(mut rx: mpsc::Receiver<RawMessageRow>, db: Arc<tokor_persistence::Database>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while batch.len() < 256 {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        if let Err(e) = db.record_raw_messages(&batch) {
            tracing::error!(count = batch.len(), error = %e, "raw frame write failed");
        }
    }
}

async fn state_writer(mut rx: mpsc::Receiver<Write>, db: Arc<tokor_persistence::Database>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while batch.len() < 128 {
            match rx.try_recv() {
                Ok(w) => batch.push(w),
                Err(_) => break,
            }
        }
        flush_writes(&db, &batch);
    }
}

fn flush_writes(db: &tokor_persistence::Database, batch: &[Write]) {
    for w in batch {
        let res = match w {
            Write::State(r) => db.record_book_states(std::slice::from_ref(r)).map(|_| ()),
            Write::Delta(r) => db.record_deltas(std::slice::from_ref(r)).map(|_| ()),
            Write::Trade(r) => db.record_trade_events(std::slice::from_ref(r)).map(|_| ()),
            Write::Health(r) => db.record_health(std::slice::from_ref(r)).map(|_| ()),
        };
        if let Err(e) = res {
            tracing::error!(error = %e, "market-data write failed");
        }
    }
}

/// Connector loop: connect → subscribe → snapshot-refresh → read until
/// error/shutdown/generation change, then back off (full jitter) and repeat.
async fn connector(
    config: RecorderConfig,
    db: Arc<tokor_persistence::Database>,
    mut generation_rx: watch::Receiver<u64>,
    mut shutdown_rx: watch::Receiver<bool>,
    raw_tx: mpsc::Sender<RawMessageRow>,
    write_tx: mpsc::Sender<Write>,
) {
    let http = match reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "recorder http client build failed");
            return;
        }
    };
    let mut attempt: u32 = 0;
    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        // The active subscription set at connect time (DB ledger is the
        // source of truth). Nothing to watch → wait for a generation bump.
        let tokens = active_tokens(&db);
        if tokens.is_empty() {
            tracing::info!("no market subscriptions; waiting");
            tokio::select! {
                _ = generation_rx.changed() => continue,
                _ = shutdown_rx.changed() => return,
            }
        }

        // Connect.
        let connected = tokio_tungstenite::connect_async(config.ws_url.as_str()).await;
        let (ws, _resp) = match connected {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!(error = %e, attempt, "market ws connect failed");
                let delay = backoff_delay(attempt, config.backoff_min, config.backoff_max);
                attempt = attempt.saturating_add(1);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => continue,
                    _ = generation_rx.changed() => continue,
                    _ = shutdown_rx.changed() => return,
                }
            }
        };
        attempt = 0;

        let (mut write_half, mut read_half) = ws.split();

        // Subscribe (fresh socket: the send cannot block meaningfully).
        if let Err(e) = write_half
            .send(Message::Text(subscribe_body(&tokens)))
            .await
        {
            tracing::warn!(error = %e, "subscribe send failed");
            continue;
        }

        // Session.
        let session = session_id();
        let mut engine = RecordingEngine::new(&session, config.clone());
        let mut rows = engine.on_connected();
        emit(&raw_tx, &write_tx, &mut rows).await;

        // Post-connect rebuild: fresh REST snapshot per token (never trust
        // books across a reconnect).
        refresh_snapshots(&config, &http, &tokens, &mut engine, &raw_tx, &write_tx).await;

        let mut snap_tick = tokio::time::interval(config.snapshot_refresh_after);
        snap_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        snap_tick.reset();

        loop {
            tokio::select! {
                // Subscription set changed → controlled reconnect.
                _ = generation_rx.changed() => break,
                _ = shutdown_rx.changed() => return,

                // Periodic staleness sweep (+ REST rescue of stale tokens).
                _ = snap_tick.tick() => {
                    let now = now_ns();
                    let mut rows = engine.sweep_staleness(now);
                    let rescue = rows.needs_refresh.clone();
                    emit(&raw_tx, &write_tx, &mut rows).await;
                    if !rescue.is_empty() {
                        refresh_snapshots(&config, &http, &rescue, &mut engine, &raw_tx, &write_tx).await;
                    }
                }

                frame = read_half.next() => {
                    let Some(msg) = frame else { break };
                    match msg {
                        Ok(Message::Text(text)) => {
                            let received_ns = now_ns();
                            let mut rows = engine.on_ws_frame(&text, received_ns);
                            let rescue = rows.needs_refresh.clone();
                            emit(&raw_tx, &write_tx, &mut rows).await;
                            // Gap rescue: rebuild THIS token immediately.
                            if !rescue.is_empty() {
                                refresh_snapshots(&config, &http, &rescue, &mut engine, &raw_tx, &write_tx).await;
                            }
                        }
                        Ok(Message::Close(cf)) => {
                            tracing::info!(?cf, "market ws closed by peer");
                            break;
                        }
                        Ok(_) => {} // ping/pong/binary handled by tungstenite
                        Err(e) => {
                            tracing::warn!(error = %e, "market ws read error");
                            break;
                        }
                    }
                }
            }
        }

        let mut rows = engine.on_disconnected();
        emit(&raw_tx, &write_tx, &mut rows).await;

        // Back off before reconnecting; shutdown skips the wait.
        let delay = backoff_delay(attempt, config.backoff_min, config.backoff_max);
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown_rx.changed() => return,
        }
    }
}

/// Post-connect / gap-rescue rebuild: fetch a fresh REST snapshot per token
/// and feed it through the engine as the new base state.
async fn refresh_snapshots(
    config: &RecorderConfig,
    http: &reqwest::Client,
    tokens: &[String],
    engine: &mut RecordingEngine,
    raw_tx: &mpsc::Sender<RawMessageRow>,
    write_tx: &mpsc::Sender<Write>,
) {
    for token in tokens {
        let url = format!("{}/book?token_id={}", config.rest_base, token);
        match http.get(&url).send().await {
            Ok(resp) => match resp.text().await {
                Ok(body) => {
                    let received_ns = now_ns();
                    let mut rows = engine.on_rest_snapshot(token, &body, received_ns);
                    emit(raw_tx, write_tx, &mut rows).await;
                }
                Err(e) => tracing::warn!(token, error = %e, "snapshot body read failed"),
            },
            Err(e) => tracing::warn!(token, error = %e, "snapshot fetch failed"),
        }
    }
}

async fn emit(
    raw_tx: &mpsc::Sender<RawMessageRow>,
    write_tx: &mpsc::Sender<Write>,
    rows: &mut Processed,
) {
    if rows.is_empty() {
        return;
    }
    for r in rows.raw.drain(..) {
        // Bounded queue: backpressure, never silent drops.
        if raw_tx.send(r).await.is_err() {
            return;
        }
    }
    for r in rows.deltas.drain(..) {
        if write_tx.send(Write::Delta(r)).await.is_err() {
            return;
        }
    }
    for r in rows.states.drain(..) {
        if write_tx.send(Write::State(r)).await.is_err() {
            return;
        }
    }
    for r in rows.trades.drain(..) {
        if write_tx.send(Write::Trade(r)).await.is_err() {
            return;
        }
    }
    for r in rows.health.drain(..) {
        if write_tx.send(Write::Health(r)).await.is_err() {
            return;
        }
    }
}

fn active_tokens(db: &tokor_persistence::Database) -> Vec<String> {
    db.active_subscriptions()
        .map(|rows| rows.into_iter().map(|(token, _)| token).collect())
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "subscription ledger read failed");
            Vec::new()
        })
}

fn session_id() -> String {
    format!("mkt-{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"))
}

#[cfg(test)]
mod session_tests {
    use super::*;

    const TOKEN: &str = "847681450197861234";

    fn config() -> RecorderConfig {
        RecorderConfig {
            max_book_staleness: Duration::from_secs(10),
            ..RecorderConfig::default()
        }
    }

    fn book_frame(hash: &str, ts: &str) -> String {
        format!(
            r#"{{"event_type":"book","asset_id":"{TOKEN}","hash":"{hash}","timestamp":"{ts}",
            "bids":[{{"price":"0.46","size":"100"}}],"asks":[{{"price":"0.48","size":"120"}}]}}"#
        )
    }

    fn delta_frame(ts: &str) -> String {
        format!(
            r#"{{"event_type":"price_change","asset_id":"{TOKEN}","hash":"h{n}","timestamp":"{ts}",
            "changes":[{{"price":"0.48","side":"SELL","size":"0"}}]}}"#,
            n = ts
        )
    }

    /// Every frame is persisted verbatim BEFORE normalization; a known shape
    /// additionally yields a delta/state row.
    #[test]
    fn frames_record_raw_then_normalize() {
        let mut eng = RecordingEngine::new("sess-1", config());
        let out = eng.on_ws_frame(&book_frame("h1", "1000"), 1_000);
        assert_eq!(out.raw.len(), 1);
        assert_eq!(out.raw[0].channel, "ws:market");
        assert_eq!(out.raw[0].token_id, TOKEN);
        assert_eq!(out.raw[0].payload, book_frame("h1", "1000"));
        assert_eq!(out.states.len(), 1);
        assert_eq!(out.states[0].quality, "SNAPSHOT");
        assert_eq!(out.states[0].source, "ws_live");
        assert!(out.deltas.is_empty());
        assert!(eng.tokens().contains(TOKEN));

        let out = eng.on_ws_frame(&delta_frame("1001"), 2_000);
        assert_eq!(out.raw.len(), 1);
        assert_eq!(out.deltas.len(), 1);
        assert_eq!(out.states.len(), 1);
        assert_eq!(out.states[0].quality, "DELTA_CONSISTENT");
        // the delta consumed the only ask level:
        assert_eq!(out.states[0].best_ask.as_deref(), None);
    }

    /// A gap (timestamp regression) yields a health event AND a refresh
    /// request — the session rescues that token from REST immediately.
    #[test]
    fn gap_detected_triggers_snapshot_rescue() {
        let mut eng = RecordingEngine::new("sess-1", config());
        eng.on_ws_frame(&book_frame("h1", "2000"), 1_000);
        let out = eng.on_ws_frame(&delta_frame("1500"), 2_000); // ts regressed
        assert_eq!(out.states.len(), 0, "suspect state is not normalized");
        assert_eq!(out.needs_refresh, vec![TOKEN.to_string()]);
        assert!(out.health.iter().any(|h| h.event == "gap_detected"));
    }

    /// REST snapshots rebuild states labeled rest_snapshot and clear the
    /// stale flag.
    #[test]
    fn rest_snapshot_rebuilds_state() {
        let mut eng = RecordingEngine::new("sess-1", config());
        let body = book_frame("h1", "3000");
        let out = eng.on_rest_snapshot(TOKEN, &body, 3_000);
        assert_eq!(out.raw[0].channel, "rest:book");
        assert_eq!(out.states.len(), 1);
        assert_eq!(out.states[0].source, "rest_snapshot");
        assert!(out.health.iter().any(|h| h.event == "snapshot_refresh"));

        // garbage body: raw evidence kept, no state fabricated
        let out = eng.on_rest_snapshot(TOKEN, "not json", 4_000);
        assert_eq!(out.raw.len(), 1);
        assert!(out.states.is_empty());
        assert!(out
            .health
            .iter()
            .any(|h| h.event == "snapshot_parse_failed"));
    }

    /// Stale books are flagged exactly once and queued for rescue.
    #[test]
    fn staleness_sweep_flags_once_and_rescues() {
        let mut eng = RecordingEngine::new("sess-1", config());
        eng.on_ws_frame(&book_frame("h1", "1000"), 1_000);
        // 11s later (> 10s bound): flagged + rescue
        let out = eng.sweep_staleness(11_000_000_000);
        assert_eq!(out.needs_refresh, vec![TOKEN.to_string()]);
        assert!(out.health.iter().any(|h| h.event == "stale_reject"));
        // sweep again at the same instant: no duplicate flag
        let out = eng.sweep_staleness(11_000_000_000);
        assert!(out.needs_refresh.is_empty());
        assert!(out.health.is_empty());
    }

    /// Disconnect invalidates every retained state (never trusted across a
    /// reconnect); the health ledger records the transition.
    #[test]
    fn disconnect_invalidates_states() {
        let mut eng = RecordingEngine::new("sess-1", config());
        eng.on_ws_frame(&book_frame("h1", "1000"), 1_000);
        let out = eng.on_disconnected();
        assert!(out.health.iter().any(|h| h.event == "disconnect"));
        // a delta now hits the invalidated engine: NoBaseState, rescue queued
        let out = eng.on_ws_frame(&delta_frame("1001"), 2_000);
        assert_eq!(out.needs_refresh, vec![TOKEN.to_string()]);
        assert!(out.states.is_empty());
    }
}
