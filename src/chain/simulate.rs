//! Transaction simulation preflight for the Zenith Soroban contract.
//!
//! Exposes `POST /api/v1/tx/simulate`, which accepts either a high-level
//! action or raw unsigned XDR from a trusted builder and returns a decoded
//! simulation result: resource usage, fee breakdown, auth requirements,
//! contract events, and decoded errors with remediation hints.
//!
//! Simulation is read-only and rate-limited. Only the Zenith contract is
//! simulated; arbitrary third-party contracts are out of scope.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::chain::builder::{self, BuildError, UnsignedTransaction};
use crate::chain::rpc::{RpcClient, RpcError, SimulateResponse};
use crate::error::ApiError;

/// Fraction of a network limit above which we emit a warning.
const LIMIT_WARN_THRESHOLD: f64 = 0.80;

/// Stable API error codes for the Zenith contract's `#[contracterror]` codes.
///
/// Generated from the contract spec where possible; the table below is the
/// fallback used when the spec is unavailable at runtime.
const CONTRACT_ERROR_TABLE: &[(u32, &str, &str)] = &[
    (1, "INSUFFICIENT_COLLATERAL", "Insufficient collateral to cover the requested position."),
    (2, "SERIES_EXPIRED", "The referenced series has expired and can no longer be traded."),
    (3, "MISSING_TRUSTLINE", "The account is missing the required trustline for this asset."),
    (4, "UNAUTHORIZED", "The caller is not authorized to perform this action."),
    (5, "INVALID_AMOUNT", "The supplied amount is invalid (zero, negative, or out of range)."),
    (6, "SERIES_NOT_FOUND", "No series exists for the supplied identifier."),
    (7, "INSUFFICIENT_LIQUIDITY", "Not enough liquidity to fill the requested order."),
    (8, "ALREADY_INITIALIZED", "The contract or series has already been initialized."),
    (9, "NOT_INITIALIZED", "The contract has not been initialized yet."),
    (10, "MATH_OVERFLOW", "An arithmetic operation overflowed the allowed range."),
];

/// Request body for `POST /api/v1/tx/simulate`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum SimulateRequest {
    /// A high-level action, built into an unsigned transaction server-side.
    Action { action: builder::Action },
    /// Raw unsigned XDR from a trusted builder.
    Xdr { xdr: String },
}

/// Decoded resource usage returned by the simulation.
#[derive(Debug, Serialize)]
pub struct ResourceUsage {
    pub cpu_instructions: u64,
    pub memory_bytes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

/// A single resource dimension compared against its network limit.
#[derive(Debug, Serialize)]
pub struct LimitCheck {
    pub used: u64,
    pub limit: u64,
    pub percent: f64,
    pub warning: bool,
}

/// Resource usage compared against network limits.
#[derive(Debug, Serialize)]
pub struct ResourceLimits {
    pub cpu_instructions: LimitCheck,
    pub memory_bytes: LimitCheck,
    pub read_bytes: LimitCheck,
    pub write_bytes: LimitCheck,
}

/// Fee breakdown for the simulated transaction.
#[derive(Debug, Serialize)]
pub struct FeeBreakdown {
    pub inclusion_fee: i64,
    pub resource_fee: i64,
    pub total_fee: i64,
}

/// A decoded contract or host error with a remediation hint.
#[derive(Debug, Serialize)]
pub struct DecodedError {
    pub code: String,
    pub message: String,
    pub hint: String,
}

/// A contract event that would be emitted by the transaction.
#[derive(Debug, Serialize)]
pub struct ContractEvent {
    pub contract_id: String,
    pub topics: Vec<String>,
    pub data: String,
}

/// A restore step required when archived entries are referenced.
#[derive(Debug, Serialize)]
pub struct RestoreStep {
    pub entry: String,
    pub reason: String,
}

/// Full decoded simulation result.
#[derive(Debug, Serialize)]
pub struct SimulateResult {
    pub success: bool,
    pub resources: ResourceUsage,
    pub limits: ResourceLimits,
    pub fee: FeeBreakdown,
    pub auth_requirements: Vec<String>,
    pub events: Vec<ContractEvent>,
    pub errors: Vec<DecodedError>,
    pub restore_preamble: Vec<RestoreStep>,
}

/// Shared state for the simulation route.
#[derive(Clone)]
pub struct SimulateState {
    pub rpc: Arc<RpcClient>,
    pub limits: NetworkLimits,
}

/// Network resource limits used to compare simulation usage.
#[derive(Debug, Clone, Copy)]
pub struct NetworkLimits {
    pub cpu_instructions: u64,
    pub memory_bytes: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl Default for NetworkLimits {
    fn default() -> Self {
        // Soroban mainnet defaults.
        Self {
            cpu_instructions: 100_000_000,
            memory_bytes: 40 * 1024 * 1024,
            read_bytes: 200_000,
            write_bytes: 100_000,
        }
    }
}

/// Build the simulation router.
#[allow(dead_code)]
pub fn router(state: SimulateState) -> Router {
    Router::new()
        .route("/api/v1/tx/simulate", post(simulate_handler))
        .with_state(state)
}

/// Handler for `POST /api/v1/tx/simulate`.
#[allow(dead_code)]
async fn simulate_handler(
    State(state): State<SimulateState>,
    Json(req): Json<SimulateRequest>,
) -> Result<Json<SimulateResult>, ApiError> {
    let unsigned = match req {
        SimulateRequest::Action { action } => builder::build(action).map_err(map_build_error)?,
        SimulateRequest::Xdr { xdr } => {
            builder::from_unsigned_xdr(&xdr).map_err(map_build_error)?
        }
    };

    let response = state
        .rpc
        .simulate_transaction(&unsigned)
        .await
        .map_err(map_rpc_error)?;

    Ok(Json(decode_simulation(response, &state.limits)))
}

/// Decode a raw RPC simulation response into the stable API shape.
#[allow(dead_code)]
pub fn decode_simulation(response: SimulateResponse, limits: &NetworkLimits) -> SimulateResult {
    let resources = ResourceUsage {
        cpu_instructions: response.cpu_instructions,
        memory_bytes: response.memory_bytes,
        read_bytes: response.read_bytes,
        write_bytes: response.write_bytes,
    };

    let limit_checks = ResourceLimits {
        cpu_instructions: check_limit(resources.cpu_instructions, limits.cpu_instructions),
        memory_bytes: check_limit(resources.memory_bytes, limits.memory_bytes),
        read_bytes: check_limit(resources.read_bytes, limits.read_bytes),
        write_bytes: check_limit(resources.write_bytes, limits.write_bytes),
    };

    let fee = FeeBreakdown {
        inclusion_fee: response.inclusion_fee,
        resource_fee: response.resource_fee,
        total_fee: response.inclusion_fee + response.resource_fee,
    };

    let errors = response
        .error
        .as_ref()
        .map(|e| decode_error(e))
        .unwrap_or_default();

    let restore_preamble = response
        .restore_preamble
        .iter()
        .map(|entry| RestoreStep {
            entry: entry.clone(),
            reason: "Archived ledger entry must be restored before simulation can proceed."
                .to_string(),
        })
        .collect();

    SimulateResult {
        success: response.error.is_none(),
        resources,
        limits: limit_checks,
        fee,
        auth_requirements: response.auth_requirements.clone(),
        events: response
            .events
            .iter()
            .map(|e| ContractEvent {
                contract_id: e.contract_id.clone(),
                topics: e.topics.clone(),
                data: e.data.clone(),
            })
            .collect(),
        errors,
        restore_preamble,
    }
}

/// Compare a single resource dimension against its limit.
fn check_limit(used: u64, limit: u64) -> LimitCheck {
    let percent = if limit == 0 {
        0.0
    } else {
        (used as f64 / limit as f64) * 100.0
    };
    LimitCheck {
        used,
        limit,
        percent,
        warning: percent >= LIMIT_WARN_THRESHOLD * 100.0,
    }
}

/// Decode a host or contract error into a stable API error with a hint.
#[allow(dead_code)]
pub fn decode_error(error: &crate::chain::rpc::SimulationError) -> Vec<DecodedError> {
    let mut decoded = Vec::new();

    // Contract error codes from the Zenith `#[contracterror]` enum.
    if let Some(code) = error.contract_error_code {
        if let Some((_, api_code, message)) = CONTRACT_ERROR_TABLE
            .iter()
            .find(|(c, _, _)| *c == code)
        {
            decoded.push(DecodedError {
                code: (*api_code).to_string(),
                message: (*message).to_string(),
                hint: contract_hint(api_code),
            });
        } else {
            decoded.push(DecodedError {
                code: format!("CONTRACT_ERROR_{code}"),
                message: format!("Unknown Zenith contract error code {code}."),
                hint: "Check the contract spec for the latest error definitions.".to_string(),
            });
        }
    }

    // Host errors: budget exceeded, archived entries, auth failure.
    if let Some(host) = error.host_error.as_deref() {
        decoded.push(decode_host_error(host));
    }

    if decoded.is_empty() {
        decoded.push(DecodedError {
            code: "SIMULATION_FAILED".to_string(),
            message: error.message.clone(),
            hint: "Inspect the transaction inputs and retry the simulation.".to_string(),
        });
    }

    decoded
}

/// Map a host error string to a stable API error and remediation hint.
fn decode_host_error(host: &str) -> DecodedError {
    let lower = host.to_ascii_lowercase();
    if lower.contains("budget") || lower.contains("exceededlimit") {
        DecodedError {
            code: "BUDGET_EXCEEDED".to_string(),
            message: "The transaction exceeded the network resource budget.".to_string(),
            hint: "Reduce the number of operations or split the transaction into smaller batches."
                .to_string(),
        }
    } else if lower.contains("archived") || lower.contains("restore") {
        DecodedError {
            code: "ENTRY_ARCHIVED".to_string(),
            message: "A referenced ledger entry has been archived.".to_string(),
            hint: "Include the restore preamble entries before submitting the transaction."
                .to_string(),
        }
    } else if lower.contains("auth") {
        DecodedError {
            code: "AUTH_FAILURE".to_string(),
            message: "The transaction failed an authorization check.".to_string(),
            hint: "Ensure all required signers have authorized the invocation.".to_string(),
        }
    } else {
        DecodedError {
            code: "HOST_ERROR".to_string(),
            message: host.to_string(),
            hint: "Consult the Soroban host error reference for this code.".to_string(),
        }
    }
}

/// Remediation hint for a known contract error code.
fn contract_hint(api_code: &str) -> String {
    match api_code {
        "INSUFFICIENT_COLLATERAL" => "Deposit additional collateral before retrying.".to_string(),
        "SERIES_EXPIRED" => "Select an active series or roll the position forward.".to_string(),
        "MISSING_TRUSTLINE" => "Establish the required trustline for the asset.".to_string(),
        "UNAUTHORIZED" => "Verify the signer has permission for this action.".to_string(),
        "INVALID_AMOUNT" => "Provide a positive amount within the allowed range.".to_string(),
        "SERIES_NOT_FOUND" => "Confirm the series identifier exists on-chain.".to_string(),
        "INSUFFICIENT_LIQUIDITY" => "Reduce the order size or wait for more liquidity.".to_string(),
        "ALREADY_INITIALIZED" => "Skip initialization; the target is already set up.".to_string(),
        "NOT_INITIALIZED" => "Initialize the contract or series before this action.".to_string(),
        "MATH_OVERFLOW" => "Reduce the magnitude of the inputs to avoid overflow.".to_string(),
        _ => "Review the contract error reference for remediation.".to_string(),
    }
}

/// Map a builder error into an API error.
fn map_build_error(err: BuildError) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, "BUILD_ERROR", err.to_string())
}

/// Map an RPC transport error into an API error.
fn map_rpc_error(err: RpcError) -> ApiError {
    ApiError::new(StatusCode::BAD_GATEWAY, "RPC_ERROR", err.to_string())
}

/// Convenience alias used by tests and callers that need the error table.
#[allow(dead_code)]
pub fn contract_error_table() -> BTreeMap<u32, (&'static str, &'static str)> {
    CONTRACT_ERROR_TABLE
        .iter()
        .map(|(code, api, msg)| (*code, (*api, *msg)))
        .collect()
}

impl IntoResponse for SimulateResult {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}
