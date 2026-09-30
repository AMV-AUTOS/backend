impl RpcClient {
    /// Create a client for the given RPC endpoint URL.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Fetch the account's current sequence number live from the network.
    pub async fn get_account(&self, account_id: &str) -> R
    endpoint: String,
    http: reqwest::Client,
}

impl RpcClient {
    /// Create a client for the given RPC endpoint URL.
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
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            http: reqwest::Client::new(),
        }
    }

    /// Read a batch of ledger entries at a fixed ledger sequence.
    ///
    /// `keys` is split into chunks of at most [`MAX_LEDGER_KEYS_PER_CALL`] so
    /// the RPC's per-call limit is never exceeded. When `ledger_seq` is set the
    /// request is pinned to that sequence, giving the reconciler a consistent
    /// snapshot of on-chain state.
    pub async fn get_ledger_entries(
        &self,
        keys: &[LedgerKey],
        ledger_seq: Option<u32>,
    ) -> Result<GetLedgerEntriesResponse, ChainError> {
        let mut all_entries = Vec::with_capacity(keys.len());
        let mut latest_ledger = None;

        for chunk in keys.chunks(MAX_LEDGER_KEYS_PER_CALL) {
            let key_strings: Vec<&str> = chunk.iter().map(|k| k.key.as_str()).collect();
            let mut params = json!({ "keys": key_strings });
            if let Some(seq) = ledger_seq {
                params["ledgerSeq"] = json!(seq);
            }

            let body = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getLedgerEntries",
                "params": params,
            });

            let response = self
                .http
                .post(&self.endpoint)
                .json(&body)
                .send()
                .await
                .map_err(|e| ChainError::Rpc(e.to_string()))?;

            let status = response.status();
            let value: Value = response
                .json()
                .await
                .map_err(|e| ChainError::Rpc(e.to_string()))?;

            if !status.is_success() {
                return Err(ChainError::Rpc(format!(
                    "getLedgerEntries failed with status {status}: {value}"
                )));
            }

            if let Some(err) = value.get("error") {
                return Err(ChainError::Rpc(err.to_string()));
            }

            let result = value.get("result").cloned().unwrap_or(Value::Null);
            let parsed: GetLedgerEntriesResponse = serde_json::from_value(result)
                .map_err(|e| ChainError::Rpc(e.to_string()))?;

            if latest_ledger.is_none() {
                latest_ledger = parsed.latest_ledger;
            }
            all_entries.extend(parsed.entries);
        }

        Ok(GetLedgerEntriesResponse {
            entries: all_entries,
            latest_ledger,
        })
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

    /// Submit a signed transaction envelope. `PENDING` and `DUPLICATE` are
    /// both treated as accepted submissions; the caller tracks the lifecycle
    /// via [`SorobanRpc::get_transaction`].
    pub async fn send_transaction(&self, transaction_xdr: &str) -> Result<SendResult, RpcError> {
        let params = json!({ "transaction": transaction_xdr });
        let result = self.call("sendTransaction", params).await?;

        let status = match result.get("status").and_then(Value::as_str) {
            Some("PENDING") => SendStatus::Pending,
            Some("DUPLICATE") => SendStatus::Duplicate,
            Some("TRY_AGAIN_LATER") => SendStatus::TryAgainLater,
            Some("ERROR") => SendStatus::Error,
            other => {
                return Err(RpcError::Unexpected(format!(
                    "unknown sendTransaction status: {other:?}"
                )))
            }
        };

        Ok(SendResult {
            status,
            hash: result
                .get("hash")
                .and_then(Value::as_str)
                .map(str::to_string),
            latest_ledger: result
                .get("latestLedger")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32,
            error_result_xdr: result
                .get("errorResultXdr")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    /// Poll the status of a previously submitted transaction by hash.
    pub async fn get_transaction(&self, hash: &str) -> Result<GetResult, RpcError> {
        let params = json!({ "hash": hash });
        let result = self.call("getTransaction", params).await?;

        let status = match result.get("status").and_then(Value::as_str) {
            Some("SUCCESS") => GetStatus::Success,
            Some("NOT_FOUND") => GetStatus::NotFound,
            Some("FAILED") => GetStatus::Failed,
            other => {
                return Err(RpcError::Unexpected(format!(
                    "unknown getTransaction status: {other:?}"
                )))
            }
        };

        Ok(GetResult {
            status,
            ledger: result
                .get("ledger")
                .and_then(Value::as_u64)
                .map(|l| l as u32),
            result_xdr: result
                .get("resultXdr")
                .and_then(Value::as_str)
                .map(str::to_string),
            latest_ledger: result
                .get("latestLedger")
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32,
        })
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
        assert!(err.message.contains("contract error #12"));
    }

    #[test]
    fn decodes_non_contract_error() {
        let err = decode_contract_error("HostError: Error(WasmVm, MissingValue)");
        assert_eq!(err.code, None);
        assert_eq!(err.message, "HostError: Error(WasmVm, MissingValue)");
    }
}
