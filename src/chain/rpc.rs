//! Minimal Soroban RPC client used by the transaction builder service.
//!
//! This module intentionally exposes only the handful of RPC methods the
//! builder needs: fetching an account (for the live sequence number), reading
//! the latest ledger (for time bounds / `valid_until_ledger`) and running
//! `simulateTransaction` so the builder can attach the footprint, resource fee
//! and auth entries before returning unsigned XDR to the wallet.
//!
//! See issue #47: "Transaction Builder Service: Unsigned Soroban XDR for
//! Wallet Signing".

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

/// Errors surfaced by the Soroban RPC client.
#[derive(Debug, Error)]
pub enum RpcError {
    #[error("rpc transport error: {0}")]
    Transport(String),
    #[error("rpc returned error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("account {0} not found (unfunded or does not exist)")]
    AccountNotFound(String),
    #[error("unexpected rpc response: {0}")]
    Unexpected(String),
}

/// Account state needed to build a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub account_id: String,
    /// Current sequence number as a decimal string (i64 range).
    pub sequence: String,
}

impl AccountInfo {
    /// Sequence number to embed in the transaction: the account's current
    /// sequence incremented by one, as required by Stellar.
    pub fn next_sequence(&self) -> Result<i64, RpcError> {
        let current: i64 = self
            .sequence
            .parse()
            .map_err(|_| RpcError::Unexpected(format!("invalid sequence: {}", self.sequence)))?;
        Ok(current + 1)
    }
}

/// Result of a `simulateTransaction` call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    /// Base64-encoded `SorobanTransactionData` (footprint + resource fees).
    pub transaction_data: String,
    /// Minimum resource fee, as a decimal string.
    pub min_resource_fee: String,
    /// Base64-encoded auth entries required by the invocation.
    #[serde(default)]
    pub auth: Vec<String>,
    /// Latest ledger at simulation time; used to derive `valid_until_ledger`.
    pub latest_ledger: u32,
}

/// A decoded contract error returned by a failed simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractError {
    /// Contract error code, when the failure originated from the contract.
    pub code: Option<u32>,
    /// Human-readable message suitable for surfacing to the UI.
    pub message: String,
}

/// Outcome of a simulation: either the assembled resources or a decoded error.
#[derive(Debug, Clone)]
pub enum SimulationOutcome {
    Success(SimulationResult),
    /// Simulation failed; the builder maps this to HTTP 422.
    Error(ContractError),
}

/// Soroban RPC client.
#[derive(Debug, Clone)]
pub struct SorobanRpc {
    endpoint: String,
    http: reqwest::Client,
}

impl SorobanRpc {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Fetch the account's current sequence number live from the network.
    pub async fn get_account(&self, account_id: &str) -> Result<AccountInfo, RpcError> {
        let params = json!({ "accountId": account_id });
        let result = self.call("getAccount", params).await?;

        let sequence = result
            .get("sequence")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::AccountNotFound(account_id.to_string()))?;

        Ok(AccountInfo {
            account_id: account_id.to_string(),
            sequence: sequence.to_string(),
        })
    }

    /// Return the latest closed ledger sequence.
    pub async fn latest_ledger(&self) -> Result<u32, RpcError> {
        let result = self.call("getLatestLedger", json!({})).await?;
        result
            .get("sequence")
            .and_then(Value::as_u64)
            .map(|s| s as u32)
            .ok_or_else(|| RpcError::Unexpected("missing latest ledger sequence".into()))
    }

    /// Simulate a base64-encoded transaction envelope, returning either the
    /// assembled resources or a decoded contract error.
    pub async fn simulate_transaction(
        &self,
        transaction_xdr: &str,
    ) -> Result<SimulationOutcome, RpcError> {
        let params = json!({ "transaction": transaction_xdr });
        let result = self.call("simulateTransaction", params).await?;

        if let Some(err) = result.get("error").and_then(Value::as_str) {
            return Ok(SimulationOutcome::Error(decode_contract_error(err)));
        }

        let transaction_data = result
            .get("transactionData")
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::Unexpected("missing transactionData".into()))?;
        let min_resource_fee = result
            .get("minResourceFee")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .to_string();
        let auth = result
            .get("results")
            .and_then(Value::as_array)
            .and_then(|r| r.first())
            .and_then(|r| r.get("auth"))
            .and_then(Value::as_array)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let latest_ledger = result
            .get("latestLedger")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;

        Ok(SimulationOutcome::Success(SimulationResult {
            transaction_data: transaction_data.to_string(),
            min_resource_fee,
            auth,
            latest_ledger,
        }))
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let response = self
            .http
            .post(&self.endpoint)
            .json(&body)
            .send()
            .await
            .map_err(|e| RpcError::Transport(e.to_string()))?;

        let payload: Value = response
            .json()
            .await
            .map_err(|e| RpcError::Transport(e.to_string()))?;

        if let Some(error) = payload.get("error") {
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown rpc error")
                .to_string();
            return Err(RpcError::Rpc { code, message });
        }

        payload
            .get("result")
            .cloned()
            .ok_or_else(|| RpcError::Unexpected("missing result".into()))
    }
}

/// Decode a Soroban simulation error string into a contract error code and a
/// human-readable message. Soroban encodes contract failures as
/// `Error(Contract, #<code>)`; anything else is passed through verbatim.
fn decode_contract_error(raw: &str) -> ContractError {
    let code = raw
        .split("#")
        .nth(1)
        .and_then(|s| s.trim_end_matches(')').trim().parse::<u32>().ok());

    let message = match code {
        Some(code) => format!("contract error #{code}: {raw}"),
        None => raw.to_string(),
    };

    ContractError { code, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_sequence_increments_current() {
        let info = AccountInfo {
            account_id: "GABC".into(),
            sequence: "42".into(),
        };
        assert_eq!(info.next_sequence().unwrap(), 43);
    }

    #[test]
    fn decodes_contract_error_code() {
        let err = decode_contract_error("HostError: Error(Contract, #12)");
        assert_eq!(err.code, Some(12));
        assert!(err.message.contains("#12"));
    }

    #[test]
    fn passes_through_non_contract_errors() {
        let err = decode_contract_error("network timeout");
        assert_eq!(err.code, None);
        assert_eq!(err.message, "network timeout");
    }
}
