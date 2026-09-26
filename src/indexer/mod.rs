//! Contract event indexer for the Zenith Options Soroban contracts.
//!
//! The indexer polls `getEvents` for the configured Zenith contracts (option
//! factory, vault, settlement), decodes the `ScVal` topics/data into typed
//! domain events, and persists them together with a durable cursor so that
//! ingestion resumes exactly where it stopped.
//!
//! Pipeline: fetch batch -> decode -> one DB transaction (raw events +
//! projections + cursor). The cursor is written in the same transaction as the
//! events it covers, which gives exactly-once persistence: a crash either
//! commits both or neither, so a restart never duplicates or skips events.
//!
//! Backfill mode is bounded by RPC retention: the RPC only retains events for a
//! limited window, so `start_ledger` is clamped to the oldest ledger the RPC
//! still serves. Requesting an older ledger cannot recover data that has already
//! been pruned; the effective start is logged and exposed via `effective_start`.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod decoder;
pub mod projections;

pub use decoder::{DecoderRegistry, DecodeError, DecodedEvent, EventDecoder};
pub use projections::{Projection, ProjectionError};

/// Errors surfaced by the indexer pipeline.
#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("rpc error: {0}")]
    Rpc(String),
    #[error("database error: {0}")]
    Database(String),
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
    #[error("projection error: {0}")]
    Projection(#[from] ProjectionError),
}

/// A raw event as returned by the Soroban RPC `getEvents` method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawEvent {
    /// Ledger sequence in which the event was emitted.
    pub ledger: u32,
    /// Transaction hash that produced the event.
    pub tx_hash: String,
    /// Contract id (C... strkey) that emitted the event.
    pub contract_id: String,
    /// Base64-encoded `ScVal` topics.
    pub topic_xdr: Vec<String>,
    /// Base64-encoded `ScVal` data.
    pub data_xdr: String,
    /// Opaque RPC paging token used to order events within a ledger.
    pub paging_token: String,
}

/// A batch of events returned by the RPC for a single page.
#[derive(Debug, Clone, Default)]
pub struct EventBatch {
    pub events: Vec<RawEvent>,
    /// Cursor to resume from after this batch; `None` when the page is empty.
    pub next_cursor: Option<Cursor>,
}

/// Durable ingestion cursor for a single stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub stream: String,
    pub ledger: u32,
    pub paging_token: String,
}

/// Configuration for the indexer.
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// Logical stream name, e.g. `"zenith"`. Used as the cursor key.
    pub stream: String,
    /// Contract ids to index. Events from other contracts are ignored.
    pub contract_allowlist: Vec<String>,
    /// Ledger to start from when no cursor exists yet.
    pub start_ledger: u32,
    /// Maximum number of events to fetch per RPC page.
    pub batch_size: u32,
    /// When true, run in backfill mode (bounded by RPC retention).
    pub backfill: bool,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            stream: "zenith".to_string(),
            contract_allowlist: Vec::new(),
            start_ledger: 0,
            batch_size: 500,
            backfill: false,
        }
    }
}

/// Minimal RPC surface the indexer depends on.
///
/// Implemented by `src/chain/rpc.rs`; kept as a trait so the pipeline can be
/// tested without a live RPC.
pub trait EventSource: Send + Sync {
    /// Fetch a page of events starting at `cursor` (or `start_ledger` when
    /// `cursor` is `None`).
    fn get_events(
        &self,
        start_ledger: u32,
        cursor: Option<&Cursor>,
        limit: u32,
    ) -> Result<EventBatch, IndexerError>;

    /// Oldest ledger the RPC still retains events for. Backfill cannot go
    /// earlier than this because pruned events are unrecoverable.
    fn oldest_retained_ledger(&self) -> Result<u32, IndexerError>;
}

/// Persistence surface for the indexer.
///
/// Implementations must write the raw events, the typed projections, and the
/// cursor in a single transaction so ingestion is exactly-once.
pub trait IndexerStore: Send + Sync {
    /// Load the durable cursor for `stream`, if any.
    fn load_cursor(&self, stream: &str) -> Result<Option<Cursor>, IndexerError>;

    /// Persist a decoded batch atomically: raw `chain_events` rows, typed
    /// projection rows, and the `indexer_cursors` row for `stream`.
    fn commit_batch(
        &self,
        stream: &str,
        events: &[DecodedEvent],
        cursor: &Cursor,
    ) -> Result<(), IndexerError>;
}

/// Metrics emitted by the indexer.
pub trait IndexerMetrics: Send + Sync {
    /// Number of ledgers between the chain head and the last indexed ledger.
    fn set_lag_ledgers(&self, lag: u32);
    /// Count of events whose topic had no registered decoder.
    fn inc_unknown_event(&self, topic: &str);
}

/// The event indexer.
pub struct Indexer<S, M> {
    config: IndexerConfig,
    source: Arc<dyn EventSource>,
    store: Arc<dyn IndexerStore>,
    registry: DecoderRegistry,
    metrics: Arc<M>,
    _marker: std::marker::PhantomData<S>,
}

impl<S, M> Indexer<S, M>
where
    M: IndexerMetrics,
{
    pub fn new(
        config: IndexerConfig,
        source: Arc<dyn EventSource>,
        store: Arc<dyn IndexerStore>,
        registry: DecoderRegistry,
        metrics: Arc<M>,
    ) -> Self {
        Self {
            config,
            source,
            store,
            registry,
            metrics,
            _marker: std::marker::PhantomData,
        }
    }

    /// Resolve the ledger to start from, clamping to RPC retention in backfill
    /// mode. Returns the effective start ledger.
    pub fn effective_start(&self) -> Result<u32, IndexerError> {
        let oldest = self.source.oldest_retained_ledger()?;
        let requested = self.config.start_ledger.max(oldest);
        if self.config.backfill && requested > self.config.start_ledger {
            tracing::warn!(
                requested = self.config.start_ledger,
                effective = requested,
                oldest_retained = oldest,
                "backfill start clamped to RPC retention window"
            );
        }
        Ok(requested)
    }

    /// Run a single ingestion step: fetch one page, decode, and commit.
    ///
    /// Returns the number of events committed. A `None` cursor is returned by
    /// the source when the page is empty, in which case nothing is written.
    pub fn step(&self) -> Result<usize, IndexerError> {
        let cursor = self.store.load_cursor(&self.config.stream)?;
        let start_ledger = match &cursor {
            Some(c) => c.ledger,
            None => self.effective_start()?,
        };

        let batch = self
            .source
            .get_events(start_ledger, cursor.as_ref(), self.config.batch_size)?;

        if batch.events.is_empty() {
            return Ok(0);
        }

        // The RPC may return events out of order within a ledger; sort by
        // paging token so the cursor advances monotonically.
        let mut events = batch.events;
        events.sort_by(|a, b| a.paging_token.cmp(&b.paging_token));

        let mut decoded = Vec::with_capacity(events.len());
        for raw in &events {
            if !self.config.contract_allowlist.is_empty()
                && !self.config.contract_allowlist.contains(&raw.contract_id)
            {
                continue;
            }
            match self.registry.decode(raw) {
                Ok(event) => decoded.push(event),
                Err(DecodeError::UnknownTopic(topic)) => {
                    // Unknown events are stored raw and counted, never fatal.
                    self.metrics.inc_unknown_event(&topic);
                    decoded.push(DecodedEvent::unknown(raw.clone()));
                }
                Err(err) => return Err(IndexerError::Decode(err)),
            }
        }

        let next = batch.next_cursor.unwrap_or_else(|| {
            let last = events.last().expect("non-empty batch");
            Cursor {
                stream: self.config.stream.clone(),
                ledger: last.ledger,
                paging_token: last.paging_token.clone(),
            }
        });

        self.store.commit_batch(&self.config.stream, &decoded, &next)?;

        if let Some(head) = self.chain_head() {
            self.metrics.set_lag_ledgers(head.saturating_sub(next.ledger));
        }

        Ok(decoded.len())
    }

    /// Best-effort chain head used for the lag metric. Implementations that do
    /// not expose a head return `None`.
    fn chain_head(&self) -> Option<u32> {
        None
    }
}

/// In-memory metric sink useful for tests and as a default.
#[derive(Debug, Default)]
pub struct NoopMetrics;

impl IndexerMetrics for NoopMetrics {
    fn set_lag_ledgers(&self, _lag: u32) {}
    fn inc_unknown_event(&self, _topic: &str) {}
}

/// Convenience alias for the default indexer type.
pub type DefaultIndexer = Indexer<(), NoopMetrics>;

/// Registry of typed decoders keyed by event topic symbol.
pub type Registry = HashMap<String, Box<dyn EventDecoder>>;
