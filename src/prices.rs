use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;

use crate::AppState;

const MAX_PCT_MOVE_PER_TICK: f64 = 0.003; // +/-0.3%

/// A single observed price for one underlying, tagged with where it came
/// from and when it was observed. `observed_at` is a UTC timestamp.
pub struct PriceTick {
    pub price: f64,
    pub source: String,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

/// Errors a `PriceSource` can surface. A failed fetch must never zero out
/// or remove an existing price — callers keep the last good value and mark
/// it stale.
#[derive(Debug)]
pub enum PriceError {
    /// The upstream returned a rate-limit response (HTTP 429).
    RateLimited,
    /// The upstream returned a response we could not parse.
    Malformed(String),
    /// The fetch exceeded its timeout budget.
    Timeout,
    /// Any other transport/upstream failure.
    Upstream(String),
}

impl std::fmt::Display for PriceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PriceError::RateLimited => write!(f, "upstream rate limited the request"),
            PriceError::Malformed(msg) => write!(f, "malformed upstream response: {msg}"),
            PriceError::Timeout => write!(f, "upstream fetch timed out"),
            PriceError::Upstream(msg) => write!(f, "upstream error: {msg}"),
        }
    }
}

impl std::error::Error for PriceError {}

/// Strategy interface for anything that can produce spot prices. The
/// simulator, an HTTP ticker feed and the Reflector oracle all implement
/// this so the ingestion loop is agnostic to where prices come from.
#[async_trait::async_trait]
pub trait PriceSource: Send + Sync {
    async fn fetch(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, PriceTick>, PriceError>;

    /// Human-readable identifier recorded on every tick this source emits.
    fn name(&self) -> &str;
}

/// Local-development / test source: the original random-walk simulator,
/// now behind the `PriceSource` trait. It never fails, so it is the safe
/// default when no real feed is configured.
pub struct SimulatedSource;

#[async_trait::async_trait]
impl PriceSource for SimulatedSource {
    async fn fetch(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, PriceTick>, PriceError> {
        let now = chrono::Utc::now();
        let mut ticks = HashMap::new();
        for symbol in symbols {
            let pct_move =
                rand::thread_rng().gen_range(-MAX_PCT_MOVE_PER_TICK..MAX_PCT_MOVE_PER_TICK);
            // The simulator has no independent notion of a "current" price;
            // it emits a multiplicative nudge around 1.0 and the ingestion
            // loop applies it to the last known price.
            ticks.insert(
                symbol.clone(),
                PriceTick {
                    price: 1.0 + pct_move,
                    source: self.name().to_string(),
                    observed_at: now,
                },
            );
        }
        Ok(ticks)
    }

    fn name(&self) -> &str {
        "simulated"
    }
}

/// HTTP spot feed (e.g. CoinGecko / Binance public ticker). Uses `reqwest`
/// with rustls and a hard 5s timeout on every fetch.
pub struct HttpTickerSource {
    client: reqwest::Client,
    base_url: String,
}

impl HttpTickerSource {
    pub fn new(base_url: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            base_url: base_url.into(),
        }
    }
}

#[async_trait::async_trait]
impl PriceSource for HttpTickerSource {
    async fn fetch(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, PriceTick>, PriceError> {
        let url = format!("{}/ticker/price", self.base_url.trim_end_matches('/'));
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    PriceError::Timeout
                } else {
                    PriceError::Upstream(e.to_string())
                }
            })?;

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(PriceError::RateLimited);
        }
        if !resp.status().is_success() {
            return Err(PriceError::Upstream(format!("HTTP {}", resp.status())));
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PriceError::Malformed(e.to_string()))?;

        let now = chrono::Utc::now();
        let mut ticks = HashMap::new();
        // Binance returns a single object for one symbol and an array for
        // many; accept both shapes and skip symbols the upstream omitted.
        let entries: Vec<&serde_json::Value> = match &body {
            serde_json::Value::Array(items) => items.iter().collect(),
            serde_json::Value::Object(_) => vec![&body],
            _ => return Err(PriceError::Malformed("unexpected JSON shape".into())),
        };
        for entry in entries {
            let symbol = match entry.get("symbol").and_then(|s| s.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            if !symbols.iter().any(|s| s == &symbol) {
                continue;
            }
            let price = match entry
                .get("price")
                .and_then(|p| p.as_str())
                .and_then(|p| p.parse::<f64>().ok())
            {
                Some(p) => p,
                None => continue,
            };
            ticks.insert(
                symbol,
                PriceTick {
                    price,
                    source: self.name().to_string(),
                    observed_at: now,
                },
            );
        }
        Ok(ticks)
    }

    fn name(&self) -> &str {
        "http"
    }
}

/// Stellar Reflector oracle source. Reads `lastprice` through the Soroban
/// RPC `simulateTransaction` read path. Reflector returns fixed-point
/// integers alongside a `decimals()` value, so the raw value is scaled
/// down by `10^decimals` before being emitted.
pub struct ReflectorOracleSource {
    client: reqwest::Client,
    rpc_url: String,
    contract_id: String,
}

impl ReflectorOracleSource {
    pub fn new(rpc_url: impl Into<String>, contract_id: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client");
        Self {
            client,
            rpc_url: rpc_url.into(),
            contract_id: contract_id.into(),
        }
    }

    /// Decode a Reflector fixed-point integer into a float price.
    pub fn decode_fixed_point(raw: i128, decimals: u32) -> f64 {
        raw as f64 / 10f64.powi(decimals as i32)
    }
}

#[async_trait::async_trait]
impl PriceSource for ReflectorOracleSource {
    async fn fetch(
        &self,
        symbols: &[String],
    ) -> Result<HashMap<String, PriceTick>, PriceError> {
        let now = chrono::Utc::now();
        let mut ticks = HashMap::new();
        for symbol in symbols {
            let request = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "simulateTransaction",
                "params": {
                    "transaction": {
                        "contract": self.contract_id,
                        "method": "lastprice",
                        "args": [symbol],
                    }
                }
            });
            let resp = self
                .client
                .post(&self.rpc_url)
                .json(&request)
                .send()
                .await
                .map_err(|e| {
                    if e.is_timeout() {
                        PriceError::Timeout
                    } else {
                        PriceError::Upstream(e.to_string())
                    }
                })?;
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(PriceError::RateLimited);
            }
            if !resp.status().is_success() {
                return Err(PriceError::Upstream(format!("HTTP {}", resp.status())));
            }
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| PriceError::Malformed(e.to_string()))?;
            // Reflector returns the fixed-point value as a string in the
            // simulation result; skip symbols the oracle did not price.
            let raw = body
                .get("result")
                .and_then(|r| r.get("returnValue"))
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<i128>().ok());
            let raw = match raw {
                Some(v) => v,
                None => continue,
            };
            let decimals = body
                .get("result")
                .and_then(|r| r.get("decimals"))
                .and_then(|d| d.as_u64())
                .unwrap_or(7) as u32;
            ticks.insert(
                symbol.clone(),
                PriceTick {
                    price: Self::decode_fixed_point(raw, decimals),
                    source: self.name().to_string(),
                    observed_at: now,
                },
            );
        }
        Ok(ticks)
    }

    fn name(&self) -> &str {
        "reflector"
    }
}

/// Configuration for the multi-source aggregator. All three knobs are
/// operator-tunable so a deployment can trade off liveness against safety.
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// Minimum number of fresh, non-outlier quotes required to publish a
    /// reference price (quorum).
    pub min_sources: usize,
    /// Quotes older than this (relative to the newest observed quote) are
    /// rejected as stale.
    pub max_staleness_secs: i64,
    /// Quotes deviating more than this many basis points from the median
    /// are rejected as outliers.
    pub max_deviation_bps: f64,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            min_sources: 2,
            max_staleness_secs: 30,
            max_deviation_bps: 500.0,
        }
    }
}

/// Why a particular source's quote was excluded from the aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// The source did not return a quote for this underlying.
    Missing,
    /// The quote was older than `max_staleness_secs`.
    Stale,
    /// The quote deviated more than `max_deviation_bps` from the median.
    Outlier,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::Missing => write!(f, "missing"),
            RejectReason::Stale => write!(f, "stale"),
            RejectReason::Outlier => write!(f, "outlier"),
        }
    }
}

/// Health of an aggregated price, surfaced on read-only endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriceStatus {
    /// Quorum met and all contributing quotes are fresh.
    Ok,
    /// Quorum met but some sources were rejected (stale/outlier/missing).
    Degraded,
    /// No quorum, or the aggregate is older than `max_staleness_secs`.
    Stale,
}

impl PriceStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PriceStatus::Ok => "ok",
            PriceStatus::Degraded => "degraded",
            PriceStatus::Stale => "stale",
        }
    }
}

/// The robust reference price for one underlying, plus the provenance
/// needed to explain how it was derived.
#[derive(Debug, Clone)]
pub struct AggregatedPrice {
    pub value: f64,
    pub status: PriceStatus,
    /// Sources whose quotes were included in the median.
    pub contributors: Vec<String>,
    /// Sources that were excluded, with the reason for each.
    pub rejected: Vec<(String, RejectReason)>,
    /// Timestamp of the newest contributing quote.
    pub as_of: chrono::DateTime<chrono::Utc>,
}

impl AggregatedPrice {
    /// A price is tradeable only when quorum was met and it is not stale.
    pub fn is_tradeable(&self) -> bool {
        self.status != PriceStatus::Stale
    }

    /// Human-readable reason used in the `503` error body.
    pub fn unavailable_reason(&self) -> String {
        if self.status == PriceStatus::Stale {
            "no quorum or stale".to_string()
        } else {
            "ok".to_string()
        }
    }
}

/// Compute the median of a slice of prices. For an even number of values we
/// take the mean of the two middle values (the standard statistical median),
/// which is documented here so callers know the exact tie-breaking rule.
pub fn median(prices: &mut [f64]) -> Option<f64> {
    if prices.is_empty() {
        return None;
    }
    prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = prices.len();
    if n % 2 == 1 {
        Some(prices[n / 2])
    } else {
        Some((prices[n / 2 - 1] + prices[n / 2]) / 2.0)
    }
}

/// Aggregate a set of per-source quotes for a single underlying into a
/// robust reference price.
///
/// Algorithm:
/// 1. Drop quotes older than `max_staleness_secs` relative to the newest
///    observed quote (clock skew between sources is tolerated by using the
///    newest quote as the reference clock).
/// 2. Compute the median of the remaining quotes.
/// 3. Reject quotes deviating more than `max_deviation_bps` from the median.
/// 4. Recompute the median over the survivors and require `min_sources`.
pub fn aggregate(
    quotes: &[(String, PriceTick)],
    cfg: &AggregatorConfig,
) -> AggregatedPrice {
    let mut rejected: Vec<(String, RejectReason)> = Vec::new();

    // Reference clock: the newest observed quote across all sources.
    let newest = quotes
        .iter()
        .map(|(_, t)| t.observed_at)
        .max()
        .unwrap_or_else(chrono::Utc::now);

    let mut fresh: Vec<(String, f64)> = Vec::new();
    for (source, tick) in quotes {
        let age = (newest - tick.observed_at).num_seconds();
        if age > cfg.max_staleness_secs {
            rejected.push((source.clone(), RejectReason::Stale));
        } else {
            fresh.push((source.clone(), tick.price));
        }
    }

    if fresh.is_empty() {
        return AggregatedPrice {
            value: 0.0,
            status: PriceStatus::Stale,
            contributors: Vec::new(),
            rejected,
            as_of: newest,
        };
    }

    let mut values: Vec<f64> = fresh.iter().map(|(_, p)| *p).collect();
    let med = median(&mut values).unwrap_or(0.0);

    // Outlier rejection against the median.
    let mut survivors: Vec<(String, f64)> = Vec::new();
    for (source, price) in fresh {
        let deviation_bps = if med.abs() > f64::EPSILON {
            ((price - med).abs() / med) * 10_000.0
        } else {
            0.0
        };
        if deviation_bps > cfg.max_deviation_bps {
            rejected.push((source, RejectReason::Outlier));
        } else {
            survivors.push((source, price));
        }
    }

    let mut survivor_values: Vec<f64> = survivors.iter().map(|(_, p)| *p).collect();
    let value = median(&mut survivor_values).unwrap_or(med);

    let status = if survivors.len() < cfg.min_sources {
        PriceStatus::Stale
    } else if rejected.is_empty() {
        PriceStatus::Ok
    } else {
        PriceStatus::Degraded
    };

    AggregatedPrice {
        value,
        status,
        contributors: survivors.into_iter().map(|(s, _)| s).collect(),
        rejected,
        as_of: newest,
    }
}

/// Circuit breaker state for one underlying. Trips when the aggregate moves
/// more than `threshold_bps` within a single tick and stays open until an
/// operator resets it or the cool-down elapses.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub tripped: bool,
    pub tripped_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_value: Option<f64>,
    pub threshold_bps: f64,
    pub cooldown_secs: i64,
}

impl CircuitBreaker {
    pub fn new(threshold_bps: f64, cooldown_secs: i64) -> Self {
        Self {
            tripped: false,
            tripped_at: None,
            last_value: None,
            threshold_bps,
            cooldown_secs,
        }
    }

    /// Feed a new aggregate value. Returns `true` if the breaker is (or
    /// becomes) tripped and trading must halt.
    pub fn observe(&mut self, value: f64, now: chrono::DateTime<chrono::Utc>) -> bool {
        if self.tripped {
            if let Some(at) = self.tripped_at {
                if (now - at).num_seconds() >= self.cooldown_secs {
                    self.reset();
                }
            }
        }
        if let Some(prev) = self.last_value {
            if prev.abs() > f64::EPSILON {
                let move_bps = ((value - prev).abs() / prev) * 10_000.0;
                if move_bps > self.threshold_bps {
                    self.tripped = true;
                    self.tripped_at = Some(now);
                }
            }
        }
        self.last_value = Some(value);
        self.tripped
    }

    /// Operator-initiated reset.
    pub fn reset(&mut self) {
        self.tripped = false;
        self.tripped_at = None;
    }
}

/// Fan out to every configured source concurrently under a per-source
/// timeout and aggregate the results for each requested symbol.
pub async fn fetch_aggregated(
    sources: &[Arc<dyn PriceSource>],
    symbols: &[String],
    cfg: &AggregatorConfig,
    timeout: std::time::Duration,
) -> HashMap<String, AggregatedPrice> {
    let mut per_symbol: HashMap<String, Vec<(String, PriceTick)>> = HashMap::new();
    let mut all_sources: Vec<String> = Vec::new();

    let fetches = sources.iter().map(|source| {
        let source = Arc::clone(source);
        let symbols = symbols.to_vec();
        async move {
            let name = source.name().to_string();
            let result = tokio::time::timeout(timeout, source.fetch(&symbols)).await;
            (name, result)
        }
    });

    let results = futures::future::join_all(fetches).await;
    for (name, result) in results {
        all_sources.push(name.clone());
        match result {
            Ok(Ok(ticks)) => {
                for (symbol, tick) in ticks {
                    per_symbol.entry(symbol).or_default().push((name.clone(), tick));
                }
            }
            // A failed source contributes nothing; it is recorded as
            // missing for every symbol it was asked about.
            _ => {}
        }
    }

    let mut out = HashMap::new();
    for symbol in symbols {
        let quotes = per_symbol.remove(symbol).unwrap_or_default();
        let mut agg = aggregate(&quotes, cfg);
        // Record sources that returned nothing for this symbol as missing.
        for source in &all_sources {
            if !quotes.iter().any(|(s, _)| s == source)
                && !agg.rejected.iter().any(|(s, _)| s == source)
            {
                agg.rejected.push((source.clone(), RejectReason::Missing));
            }
        }
        out.insert(symbol.clone(), agg);
    }
    out
}

/// Read-only pricing endpoint. Always responds, but includes a
/// `price_status` field so callers can see whether the price is tradeable.
pub async fn price_handler(
    State(state): State<Arc<AppState>>,
) -> Response {
    let prices = state.aggregated_prices.read().await;
    let body: HashMap<String, serde_json::Value> = prices
        .iter()
        .map(|(symbol, agg)| {
            (
                symbol.clone(),
                serde_json::json!({
                    "price": agg.value,
                    "price_status": agg.status.as_str(),
                    "contributors": agg.contributors,
                    "rejected": agg
                        .rejected
                        .iter()
                        .map(|(s, r)| serde_json::json!({"source": s, "reason": r.to_string()}))
                        .collect::<Vec<_>>(),
                    "as_of": agg.as_of,
                }),
            )
        })
        .collect();
    axum::Json(body).into_response()
}

/// WebSocket upgrade handler for streaming price updates to clients.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.price_tx.subscribe();
    while let Ok(msg) = rx.recv().await {
        if socket.send(Message::Text(msg)).await.is_err() {
            break;
        }
    }
}
