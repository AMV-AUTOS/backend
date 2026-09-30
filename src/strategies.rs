use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::auth::AuthUser;
use crate::error::{db_error, AppError, AppJson};
use crate::models::Position;
use crate::positions::{
    close_position_in_tx, current_bs_result, open_position_in_tx, OpenPositionRequest,
};
use crate::quotes::{consume_quote_in_tx, QuoteRequest};
use crate::AppState;

#[derive(Deserialize)]
pub struct ExecuteStrategyRequest {
    pub legs: Vec<OpenPositionRequest>,
    /// Optional time-bound executable quote (see POST /api/v1/quotes).
    /// When present and valid, every leg executes at exactly the quoted
    /// premium; when absent, today's live-pricing behaviour is kept.
    #[serde(default)]
    pub quote_id: Option<String>,
}

/// Opens every leg of a multi-leg strategy (straddle, spread, iron
/// condor, ...) as one atomic transaction under a shared strategy_id —
/// either all legs open or none do, so a mid-strategy insufficient-funds
/// rejection can't leave a naked partial position behind.
pub async fn execute_strategy(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    AppJson(req): AppJson<ExecuteStrategyRequest>,
) -> Result<Json<Vec<Position>>, AppError> {
    if req.legs.len() < 2 {
        // A single "strategy" leg is just a plain open — use
        // /api/v1/positions/open for that instead.
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "a strategy needs at least 2 legs; use /api/v1/positions/open for a single leg",
        ));
    }

    let strategy_id = uuid::Uuid::new_v4().to_string();
    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| db_error("begin strategy transaction", e))?;

    // Validate + consume the quote (if any) inside the same transaction
    // that opens the legs, so a replay or a mid-flight expiry can't slip
    // a stale price through. The quote is bound to the wallet, legs,
    // size and side it was issued for.
    let quoted_premiums = match &req.quote_id {
        Some(quote_id) => {
            let quote_req = QuoteRequest {
                legs: req.legs.clone(),
            };
            Some(
                consume_quote_in_tx(&mut tx, &state, &wallet_address, quote_id, &quote_req)
                    .await?,
            )
        }
        None => None,
    };

    let mut opened = Vec::with_capacity(req.legs.len());
    for (i, leg) in req.legs.iter().enumerate() {
        let quoted_premium = quoted_premiums.as_ref().map(|p| p[i]);
        let position = open_position_in_tx(
            &mut tx,
            &state,
            &wallet_address,
            leg,
            Some(&strategy_id),
            quoted_premium,
        )
        .await?;
        opened.push(position);
    }

    tx.commit()
        .await
        .map_err(|e| db_error("commit strategy transaction", e))?;
    Ok(Json(opened))
}

/// Unrealized P&L for one open leg at today's spot/vol; 0 if the
/// underlying's been delisted since the leg was opened (same convention
/// as get_portfolio_greeks — nothing to reprice against).
fn leg_unrealized_pnl(state: &AppState, p: &Position) -> f64 {
    let Some(result) = current_bs_result(state, p) else {
        return 0.0;
    };
    if p.position_type == "short" {
        (p.entry_premium - result.premium) * p.contracts
    } else {
        (result.premium - p.entry_premium) * p.contracts
    }
}

#[derive(Serialize)]
pub struct StrategySummary {
    pub strategy_id: String,
    /// The underlying shared by every leg opened via execute_strategy. A
    /// roll can only replace a leg with the same underlying, so this stays
    /// singular for the lifetime of the strategy.
    pub underlying: String,
    pub leg_count: usize,
    pub open_leg_count: usize,
    pub status: String, // "open" if any leg is still open, else "closed"
    pub opened_at: String,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
}

fn summarize(state: &AppState, strategy_id: String, legs: &[Position]) -> StrategySummary {
    let open_leg_count = legs.iter().filter(|p| p.status == "open").count();
    let realized_pnl = legs.iter().filter_map(|p| p.realized_pnl).sum();
    let unrealized_pnl = legs
        .iter()
        .filter(|p| p.status == "open")
        .map(|p| leg_unrealized_pnl(state, p))
        .sum();
    // Legs arrive ordered by opened_at ascending, so the first leg is the
    // one the strategy was originally opened with.
    let opened_at = legs
        .first()
        .map(|p| p.opened_at.clone())
        .unwrap_or_default();

    StrategySummary {
        strategy_id,
        underlying: legs
            .first()
            .map(|p| p.underlying.clone())
            .unwrap_or_default(),
        leg_count: legs.len(),
        open_leg_count,
        status: if open_leg_count > 0 { "open" } else { "closed" }.to_string(),
        opened_at,
        realized_pnl,
        unrealized_pnl,
    }
}

/// Lists every multi-leg strategy for the wallet, newest-first by the
/// opened_at of its first leg, with aggregate realized/unrealized P&L
/// across all of its legs. Plain single-leg positions (strategy_id NULL)
/// aren't strategies and don't show up here — see /api/v1/positions for
/// those.
pub async fn list_strategies(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
) -> Result<Json<Vec<StrategySummary>>, AppError> {
    let positions: Vec<Position> = sqlx::query_as(
        "SELECT * FROM positions
            WHERE wallet_address = ? AND strategy_id IS NOT NULL
         ORDER BY opened_at ASC",
    )
    .bind(&wallet_address)
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("list strategies", e))?;

    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<Position>> = HashMap::new();
    for p in positions {
        let strategy_id = p.strategy_id.clone().expect("filtered to NOT NULL above");
        if !groups.contains_key(&strategy_id) {
            order.push(strategy_id.clone());
        }
        groups.entry(strategy_id).or_default().push(p);
    }

    let summaries = order
        .into_iter()
        .rev() // ascending opened_at order in -> newest strategy first out
        .map(|strategy_id| {
            let legs = groups.remove(&strategy_id).expect("just inserted above");
            summarize(&state, strategy_id, &legs)
        })
        .collect();

    Ok(Json(summaries))
}

#[derive(Serialize)]
pub struct StrategyDetail {
    pub strategy_id: String,
    pub status: String,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub legs: Vec<Position>,
}

async fn load_strategy_legs(
    state: &AppState,
    wallet_address: &str,
    strategy_id: &str,
) -> Result<Vec<Position>, AppError> {
    let legs: Vec<Position> = sqlx::query_as(
        "SELECT * FROM positions
            WHERE wallet_address = ? AND strategy_id = ?
         ORDER BY opened_at ASC",
    )
    .bind(wallet_address)
    .bind(strategy_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("load strategy legs", e))?;

    if legs.is_empty() {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            "no strategy with that id for this wallet",
        ));
    }
    Ok(legs)
}

pub async fn get_strategy(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(strategy_id): Path<String>,
) -> Result<Json<StrategyDetail>, AppError> {
    let legs = load_strategy_legs(&state, &wallet_address, &strategy_id).await?;
    let summary = summarize(&state, strategy_id.clone(), &legs);

    Ok(Json(StrategyDetail {
        strategy_id,
        status: summary.status,
        realized_pnl: summary.realized_pnl,
        unrealized_pnl: summary.unrealized_pnl,
        legs,
    }))
}

/// Closes every currently-open leg of a strategy in one atomic
/// transaction — either every open leg closes or none do, mirroring
/// execute_strategy's all-or-nothing open. Legs already closed or rolled
/// are left as-is; a roll's replacement leg (same strategy_id) still gets
/// closed normally.
pub async fn close_strategy(
    State(state): State<AppState>,
    AuthUser(wallet_address): AuthUser,
    Path(strategy_id): Path<String>,
) -> Result<Json<Vec<Position>>, AppError> {
    let open_leg_ids: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM positions
            WHERE wallet_address = ? AND strategy_id = ? AND status = 'open'",
    )
    .bind(&wallet_address)
    .bind(&strategy_id)
    .fetch_all(&state.db)
    .await
    .map_err(|e| db_error("find open strategy legs", e))?;

    if open_leg_ids.is_empty() {
        return Err(AppError::new(
            StatusCode::NOT_FOUND,
            "no open legs in that strategy for this wallet",
        ));
    }

    let mut tx = state
        .db
        .begin()
        .await
        .map_err(|e| db_error("begin close-strategy transaction", e))?;

    let mut closed = Vec::with_capacity(open_leg_ids.len());
    for id in &open_leg_ids {
        closed.push(close_position_in_tx(&mut tx, &state, &wallet_address, id).await?);
    }

    tx.commit()
        .await
        .map_err(|e| db_error("commit close-strategy transaction", e))?;
    Ok(Json(closed))
}
