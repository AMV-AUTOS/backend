//! Transaction submission and lifecycle tracking service.
//!
//! Accepts signed Soroban transaction XDR, binds it to a previously built
//! transaction for the same wallet (hash binding), submits it through the
//! Soroban RPC `sendTransaction` method, and tracks it through an explicit
//! state machine persisted in the `chain_txs` table.
//!
//! Lifecycle: `PENDING -> SUCCESS | FAILED | NOT_FOUND | EXPIRED`.
//! `TRY_AGAIN_LATER` and `DUPLICATE` are handled idempotently by hash.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::chain::rpc::{RpcClient, RpcError, SendTransactionResponse, TxStatus};

/// Terminal and non-terminal states a tracked transaction can be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TxState {
    Pending,
    Success,
    Failed,
    NotFound,
    Expired,
}

impl TxState {
    pub fn as_str(&self) -> &'static str {
        match self {
            TxState::Pending => "PENDING",
            TxState::Success => "SUCCESS",
            TxState::Failed => "FAILED",
            TxState::NotFound => "NOT_FOUND",
            TxState::Expired => "EXPIRED",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "PENDING" => Some(TxState::Pending),
            "SUCCESS" => Some(TxState::Success),
            "FAILED" => Some(TxState::Failed),
            "NOT_FOUND" => Some(TxState::NotFound),
            "EXPIRED" => Some(TxState::Expired),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TxState::Success | TxState::Failed | TxState::NotFound | TxState::Expired
        )
    }
}

/// Allowed-transitions table for the persisted state machine.
///
/// A transition is only applied when the current state permits it, which keeps
/// concurrent pollers and duplicate submissions from clobbering a finalised
/// status.
pub fn transition_allowed(from: TxState, to: TxState) -> bool {
    use TxState::*;
    match (from, to) {
        // Non-terminal states may move to any other state.
        (Pending, _) => true,
        (NotFound, Pending) | (NotFound, Success) | (NotFound, Failed) | (NotFound, Expired) => true,
        // Terminal states are immutable.
        (Success, _) | (Failed, _) | (Expired, _) => false,
        // A terminal NotFound may only be re-opened by a later confirmation.
        (NotFound, NotFound) => true,
    }
}

/// Row shape for the `chain_txs` table.
#[derive(Debug, Clone, FromRow, Serialize)]
pub struct ChainTx {
    pub hash: String,
    pub wallet: String,
    pub kind: String,
    pub status: String,
    pub submitted_at: chrono::DateTime<chrono::Utc>,
    pub ledger: Option<i64>,
    pub result_xdr: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Error)]
pub enum SubmitError {
    #[error("malformed signed XDR: {0}")]
    MalformedXdr(String),
    #[error("transaction hash does not match a server-built transaction for this wallet")]
    ForeignTransaction,
    #[error("transaction already finalised as {0}")]
    AlreadyFinalised(String),
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcError),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// Request body for `POST /api/v1/tx/submit`.
#[derive(Debug, Deserialize)]
pub struct SubmitRequest {
    pub wallet: String,
    pub signed_xdr: String,
    /// Optional client-supplied hash; when present it must match the derived hash.
    pub hash: Option<String>,
}

/// Response body for `POST /api/v1/tx/submit` and `GET /api/v1/tx/:hash`.
#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub hash: String,
    pub status: TxState,
    pub ledger: Option<i64>,
    pub result_xdr: Option<String>,
    pub error: Option<String>,
}

/// Service owning submission, persistence and lifecycle polling.
#[derive(Clone)]
pub struct TxSubmitService {
    db: PgPool,
    rpc: Arc<RpcClient>,
    /// In-process guard so the same hash cannot be submitted concurrently.
    inflight: Arc<Mutex<HashMap<String, ()>>>,
}

impl TxSubmitService {
    pub fn new(db: PgPool, rpc: Arc<RpcClient>) -> Self {
        Self {
            db,
            rpc,
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Submit a signed transaction, binding it to a server-built transaction.
    pub async fn submit(&self, req: SubmitRequest) -> Result<SubmitResponse, SubmitError> {
        let (hash, kind, valid_until_ledger) = self.bind_signed_xdr(&req)?;

        if let Some(client_hash) = &req.hash {
            if client_hash != &hash {
                return Err(SubmitError::ForeignTransaction);
            }
        }

        // Serialise concurrent submissions of the same hash.
        {
            let mut inflight = self.inflight.lock().await;
            if inflight.contains_key(&hash) {
                // Another request is already submitting this hash; return the
                // current persisted state instead of double-submitting.
                return self.load(&hash).await;
            }
            inflight.insert(hash.clone(), ());
        }
        let _guard = InflightGuard {
            hash: hash.clone(),
            inflight: self.inflight.clone(),
        };

        // Idempotency: if we already track this hash, do not resubmit.
        if let Some(existing) = self.try_load(&hash).await? {
            if existing.status != TxState::Pending.as_str() {
                return Ok(existing.into());
            }
        } else {
            self.insert_pending(&hash, &req.wallet, &kind, valid_until_ledger)
                .await?;
        }

        let resp = self.rpc.send_transaction(&req.signed_xdr).await?;
        self.apply_send_response(&hash, resp).await
    }

    /// Fetch the current tracked state for a hash.
    pub async fn load(&self, hash: &str) -> Result<SubmitResponse, SubmitError> {
        let row = sqlx::query_as::<_, ChainTx>(
            "SELECT hash, wallet, kind, status, submitted_at, ledger, result_xdr, error \
             FROM chain_txs WHERE hash = $1",
        )
        .bind(hash)
        .fetch_optional(&self.db)
        .await?
        .ok_or_else(|| SubmitError::MalformedXdr(format!("unknown transaction {hash}")))?;
        Ok(row.into())
    }

    async fn try_load(&self, hash: &str) -> Result<Option<SubmitResponse>, SubmitError> {
        let row = sqlx::query_as::<_, ChainTx>(
            "SELECT hash, wallet, kind, status, submitted_at, ledger, result_xdr, error \
             FROM chain_txs WHERE hash = $1",
        )
        .bind(hash)
        .fetch_optional(&self.db)
        .await?;
        Ok(row.map(Into::into))
    }

    /// Validate the signed XDR against a server-built transaction for the
    /// wallet. Arbitrary relaying is rejected.
    fn bind_signed_xdr(&self, req: &SubmitRequest) -> Result<(String, String, i64), SubmitError> {
        let parsed = crate::chain::rpc::parse_signed_transaction(&req.signed_xdr)
            .map_err(SubmitError::MalformedXdr)?;

        // The inner operation must reference the wallet that built it.
        if parsed.source_account != req.wallet {
            return Err(SubmitError::ForeignTransaction);
        }

        // Hash binding: the derived hash must match a server-built transaction
        // recorded for this wallet. `built_txs` is populated by the builder
        // service; absence means the client is relaying a foreign transaction.
        let known = crate::chain::rpc::lookup_built_transaction(&parsed.hash, &req.wallet)
            .ok_or(SubmitError::ForeignTransaction)?;

        Ok((parsed.hash, known.kind, known.valid_until_ledger))
    }

    async fn insert_pending(
        &self,
        hash: &str,
        wallet: &str,
        kind: &str,
        valid_until_ledger: i64,
    ) -> Result<(), SubmitError> {
        sqlx::query(
            "INSERT INTO chain_txs (hash, wallet, kind, status, submitted_at, valid_until_ledger) \
             VALUES ($1, $2, $3, 'PENDING', now(), $4) \
             ON CONFLICT (hash) DO NOTHING",
        )
        .bind(hash)
        .bind(wallet)
        .bind(kind)
        .bind(valid_until_ledger)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    /// Apply the immediate `sendTransaction` response.
    async fn apply_send_response(
        &self,
        hash: &str,
        resp: SendTransactionResponse,
    ) -> Result<SubmitResponse, SubmitError> {
        match resp.status {
            TxStatus::Pending => {
                // Stays PENDING; the background poller finalises it.
                self.load(hash).await
            }
            TxStatus::Duplicate => {
                // Idempotent by hash: keep whatever we already track.
                self.load(hash).await
            }
            TxStatus::TryAgainLater => {
                // Keep PENDING and let the poller retry with backoff.
                self.load(hash).await
            }
            TxStatus::Error => {
                let err = resp.error.unwrap_or_else(|| "submission rejected".into());
                self.transition(hash, TxState::Failed, None, None, Some(err))
                    .await
            }
        }
    }

    /// Atomically transition a transaction, honouring the allowed-transitions
    /// table. Returns the resulting row.
    pub async fn transition(
        &self,
        hash: &str,
        to: TxState,
        ledger: Option<i64>,
        result_xdr: Option<String>,
        error: Option<String>,
    ) -> Result<SubmitResponse, SubmitError> {
        let current = self.load(hash).await?;
        let from = TxState::from_str(&current.status)
            .ok_or_else(|| SubmitError::MalformedXdr(format!("bad status {}", current.status)))?;

        if !transition_allowed(from, to) {
            return Ok(current);
        }

        // Guard against a concurrent poller having already finalised the row.
        let updated = sqlx::query_as::<_, ChainTx>(
            "UPDATE chain_txs \
             SET status = $2, ledger = COALESCE($3, ledger), \
                 result_xdr = COALESCE($4, result_xdr), error = $5 \
             WHERE hash = $1 AND status = $6 \
             RETURNING hash, wallet, kind, status, submitted_at, ledger, result_xdr, error",
        )
        .bind(hash)
        .bind(to.as_str())
        .bind(ledger)
        .bind(result_xdr)
        .bind(error)
        .bind(from.as_str())
        .fetch_optional(&self.db)
        .await?;

        match updated {
            Some(row) => {
                let resp: SubmitResponse = row.into();
                if to.is_terminal() {
                    crate::events::publish_tx_confirmed(&resp);
                }
                Ok(resp)
            }
            // Lost the race; return the authoritative current state.
            None => self.load(hash).await,
        }
    }

    /// Background poller: finalise PENDING transactions with backoff and mark
    /// transactions past `valid_until_ledger` as expired. Resumes from the
    /// table after a restart.
    pub async fn poll_once(&self) -> Result<(), SubmitError> {
        let pending = sqlx::query_as::<_, ChainTx>(
            "SELECT hash, wallet, kind, status, submitted_at, ledger, result_xdr, error \
             FROM chain_txs WHERE status = 'PENDING' ORDER BY submitted_at ASC LIMIT 100",
        )
        .fetch_all(&self.db)
        .await?;

        for tx in pending {
            let status = match self.rpc.get_transaction(&tx.hash).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(hash = %tx.hash, error = %e, "poll failed; will retry with backoff");
                    continue;
                }
            };

            match status {
                TxStatus::Success => {
                    let (ledger, result_xdr) = self.rpc.tx_result(&tx.hash).await?;
                    self.transition(&tx.hash, TxState::Success, ledger, result_xdr, None)
                        .await?;
                }
                TxStatus::Error => {
                    let (ledger, result_xdr) = self.rpc.tx_result(&tx.hash).await?;
                    let decoded = result_xdr
                        .as_deref()
                        .and_then(crate::chain::rpc::decode_contract_error);
                    self.transition(&tx.hash, TxState::Failed, ledger, result_xdr, decoded)
                        .await?;
                }
                TxStatus::NotFound => {
                    if self.past_valid_until(&tx.hash).await? {
                        self.transition(&tx.hash, TxState::Expired, None, None, None)
                            .await?;
                    } else {
                        self.transition(&tx.hash, TxState::NotFound, None, None, None)
                            .await?;
                    }
                }
                TxStatus::Pending | TxStatus::Duplicate | TxStatus::TryAgainLater => {}
            }
        }
        Ok(())
    }

    async fn past_valid_until(&self, hash: &str) -> Result<bool, SubmitError> {
        let latest = self.rpc.latest_ledger().await?;
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT valid_until_ledger FROM chain_txs WHERE hash = $1")
                .bind(hash)
                .fetch_optional(&self.db)
                .await?;
        Ok(matches!(row, Some((v,)) if latest > v))
    }
}

impl From<ChainTx> for SubmitResponse {
    fn from(row: ChainTx) -> Self {
        SubmitResponse {
            hash: row.hash,
            status: TxState::from_str(&row.status).unwrap_or(TxState::Pending),
            ledger: row.ledger,
            result_xdr: row.result_xdr,
            error: row.error,
        }
    }
}

/// Removes the in-flight marker when a submission completes or errors.
struct InflightGuard {
    hash: String,
    inflight: Arc<Mutex<HashMap<String, ()>>>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let hash = self.hash.clone();
        let inflight = self.inflight.clone();
        tokio::spawn(async move {
            inflight.lock().await.remove(&hash);
        });
    }
}

/// Spawn the background poller with exponential backoff.
pub fn spawn_poller(service: TxSubmitService) {
    tokio::spawn(async move {
        let mut backoff = std::time::Duration::from_secs(1);
        loop {
            match service.poll_once().await {
                Ok(()) => backoff = std::time::Duration::from_secs(1),
                Err(e) => {
                    warn!(error = %e, "tx poller error");
                    backoff = (backoff * 2).min(std::time::Duration::from_secs(30));
                }
            }
            info!("tx poller sleeping for {:?}", backoff);
            tokio::time::sleep(backoff).await;
        }
    });
}
