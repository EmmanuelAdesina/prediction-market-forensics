//! Market metadata enrichment: CLOB (authority) + Gamma (tags/description).
//!
//! Raw payloads are stored verbatim (`clob_meta_json`, `gamma_meta_json`);
//! normalized columns are COALESCE-updated so re-runs never erase data. No
//! classification happens here - `classify-markets` is a separate command so
//! decisions are observable and retryable independently.

use tokor_market_data::MarketDataClient;
use tokor_persistence::markets::MarketRepository;
use tokor_persistence::Database;

pub type EnrichResult<T> = Result<T, String>;

/// Enrich one market by condition id. Idempotent.
pub async fn enrich_market(
    database: &Database,
    client: &MarketDataClient,
    condition_id: &str,
) -> EnrichResult<()> {
    let clob = client
        .clob_market(condition_id)
        .await
        .map_err(|e| format!("clob fetch: {e}"))?;

    let question = clob
        .get("question")
        .and_then(|v| v.as_str())
        .map(String::from);
    let market_slug = clob
        .get("market_slug")
        .and_then(|v| v.as_str())
        .map(String::from);
    let active = clob.get("active").and_then(|v| v.as_bool());
    let closed = clob.get("closed").and_then(|v| v.as_bool());
    let min_tick = clob.get("minimum_tick_size").and_then(value_as_string);
    let min_order = clob.get("minimum_order_size").and_then(value_as_string);
    let neg_risk = clob.get("neg_risk").and_then(|v| v.as_bool());

    // Outcome tokens from the CLOB payload (authoritative token/condition link).
    let tokens = clob
        .get("tokens")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    // Gamma enrichment is best-effort: tags/description/icon from the event
    // that contains this market. Failure to fetch Gamma must not fail the
    // market record (evidence gap is recorded, retried later).
    let mut tags: Option<Vec<String>> = None;
    let mut description = clob
        .get("description")
        .and_then(|v| v.as_str())
        .map(String::from);
    let mut icon = clob.get("icon").and_then(|v| v.as_str()).map(String::from);
    let mut event_slug = clob.get("events").and_then(first_slug);
    let mut gamma_json: Option<String> = None;
    if let Some(slug) = market_slug.as_deref() {
        match client.gamma_event_by_slug(slug).await {
            Ok(gevent) => {
                if let Some(ev) = gevent.as_array().and_then(|a| a.first()) {
                    if tags.is_none() {
                        tags = Some(tag_list(ev));
                    }
                    if description.is_none() {
                        description = ev
                            .get("description")
                            .and_then(|v| v.as_str())
                            .map(String::from);
                    }
                    if icon.is_none() {
                        icon = ev.get("icon").and_then(|v| v.as_str()).map(String::from);
                    }
                    event_slug = ev.get("slug").and_then(|v| v.as_str()).map(String::from);
                }
                gamma_json = Some(gevent.to_string());
            }
            Err(e) => {
                tracing::warn!(market_slug=%slug, error=%e, "gamma enrichment skipped");
            }
        }
    }

    MarketRepository::upsert_market_enriched(
        database,
        condition_id,
        question.as_deref(),
        market_slug.as_deref(),
        event_slug.as_deref(),
        icon.as_deref(),
        tags.as_deref(),
        description.as_deref(),
        active,
        closed,
        min_tick.as_deref(),
        min_order.as_deref(),
        neg_risk,
        &clob.to_string(),
        gamma_json.as_deref(),
    )
    .map_err(|e| format!("persist enriched: {e}"))?;

    for t in &tokens {
        let token_id = t
            .get("token_id")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if token_id.is_empty() {
            continue;
        }
        let outcome = t.get("outcome").and_then(|v| v.as_str()).map(String::from);
        let idx = t.get("winner").and_then(|v| v.as_bool());
        MarketRepository::upsert_token(database, condition_id, token_id, outcome.as_deref())
            .map_err(|e| format!("persist token: {e}"))?;
        let _ = idx;
    }
    Ok(())
}

fn value_as_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn first_slug(v: &serde_json::Value) -> Option<String> {
    v.as_array()
        .and_then(|a| a.first())
        .and_then(|e| e.get("slug"))
        .and_then(|s| s.as_str())
        .map(String::from)
}

/// Gamma tags arrive either as objects (`{"slug": "...", "label": "..."}`) or
/// plain strings; normalize to slugs and never drop entries.
pub(crate) fn tag_list(event: &serde_json::Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(list) = event.get("tags").and_then(|t| t.as_array()) {
        for t in list {
            match t {
                serde_json::Value::String(s) => out.push(s.clone()),
                serde_json::Value::Object(o) => {
                    let slug = o
                        .get("slug")
                        .or_else(|| o.get("label"))
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    if let Some(s) = slug {
                        out.push(s);
                    }
                }
                _ => {}
            }
        }
    }
    out
}
