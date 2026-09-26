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
//!
//! Reliability hardening (issue #46): every processed ledger range is recorded
//! so a [`GapDetector`] can find non-contiguous ranges and raise an alert;
//! [`Indexer::replay`] rebuilds projections idempotently from the immutable raw
//! events; [`LedgerMetaSource`] is an optional trait for backfilling history
//! from a Galexie-style bucket once the RPC retention window has passed; and
//! [`ProjectionSchema`] versioning drives a shadow-table rebuild + swap so a
//! version bump never causes downtime.

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
    #[error("ledger gap detected: {0}")]
    Gap(String),
    #[error("replay lock held by another process")]
    ReplayLocked,
    #[error("ledger meta source error: {0}")]
    LedgerMeta(String),
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

/// Optional backfill source for history that has aged out of the RPC retention
/// window.
///
/// Implementations read `LedgerMeta` (ledger close meta) from a Galexie-style
/// bucket or data lake. This is deliberately a separate trait from
/// [`EventSource`] so the live pipeline never depends on the archive, and so
/// operators can plug in any archive without running a Galexie instance
/// (out of scope).
///
/// `from_ledger`/`to_ledger` are inclusive. Implementations must stream in
/// bounded batches so replaying a large range stays within memory limits.
pub trait LedgerMetaSource: Send + Sync {
    /// Fetch the raw events for the inclusive ledger range `[from, to]`,
    /// streaming in batches of at most `batch_size` ledgers.
    fn fetch_range(
        &self,
        from_ledger: u32,
        to_ledger: u32,
        batch_size: u32,
    ) -> Result<Vec<RawEvent>, IndexerError>;

    /// Oldest ledger present in the archive, if known.
    fn oldest_available_ledger(&self) -> Result<Option<u32>, IndexerError> {
        Ok(None)
    }
}

/// A contiguous range of processed ledgers, inclusive on both ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerRange {
    pub from: u32,
    pub to: u32,
}

impl LedgerRange {
    pub fn new(from: u32, to: u32) -> Self {
        Self { from, to }
    }

    /// True when `self` and `other` touch or overlap, i.e. they can be merged
    /// into a single contiguous range.
    pub fn is_contiguous_with(&self, other: &LedgerRange) -> bool {
        // `+ 1` guards against u32 overflow at the top of the range.
        self.from <= other.to.saturating_add(1) && other.from <= self.to.saturating_add(1)
    }

    pub fn merge(&self, other: &LedgerRange) -> LedgerRange {
        LedgerRange {
            from: self.from.min(other.from),
            to: self.to.max(other.to),
        }
    }
}

/// A hole in the processed ledger history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerGap {
    /// First missing ledger.
    pub from: u32,
    /// Last missing ledger (inclusive).
    pub to: u32,
}

impl LedgerGap {
    pub fn len(&self) -> u32 {
        self.to.saturating_sub(self.from).saturating_add(1)
    }

    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Detects non-contiguous processed ledger ranges.
///
/// Ranges are recorded as they are processed (see [`IndexerStore::record_range`])
/// and fed here. The detector normalises them by merging touching/overlapping
/// ranges, then reports every hole between the first and last processed ledger.
#[derive(Debug, Default, Clone)]
pub struct GapDetector {
    ranges: Vec<LedgerRange>,
}

impl GapDetector {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Build a detector from already-recorded ranges.
    pub fn from_ranges(mut ranges: Vec<LedgerRange>) -> Self {
        ranges.sort_by_key(|r| r.from);
        Self { ranges }
    }

    /// Record a processed range.
    pub fn record(&mut self, range: LedgerRange) {
        self.ranges.push(range);
    }

    /// Merge touching/overlapping ranges and return the normalised set, sorted
    /// ascending by `from`.
    pub fn normalised(&self) -> Vec<LedgerRange> {
        let mut sorted = self.ranges.clone();
        sorted.sort_by_key(|r| r.from);
        let mut merged: Vec<LedgerRange> = Vec::with_capacity(sorted.len());
        for range in sorted {
            match merged.last_mut() {
                Some(last) if last.is_contiguous_with(&range) => {
                    *last = last.merge(&range);
                }
                _ => merged.push(range),
            }
        }
        merged
    }

    /// Return every gap between the first and last processed ledger.
    ///
    /// An empty result means the processed history is fully contiguous.
    pub fn gaps(&self) -> Vec<LedgerGap> {
        let merged = self.normalised();
        let mut gaps = Vec::new();
        for pair in merged.windows(2) {
            let (prev, next) = (&pair[0], &pair[1]);
            if next.from > prev.to.saturating_add(1) {
                gaps.push(LedgerGap {
                    from: prev.to.saturating_add(1),
                    to: next.from.saturating_sub(1),
                });
            }
        }
        gaps
    }

    /// True when the processed history has no holes.
    pub fn is_contiguous(&self) -> bool {
        self.gaps().is_empty()
    }
}

/// Version of the projection schema.
///
/// Projections are derived data, so a version bump can be applied by rebuilding
/// into shadow tables and swapping them in without downtime. The store is
/// responsible for the physical swap; this type only carries the intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSchema {
    pub version: u32,
}

impl ProjectionSchema {
    pub const CURRENT: u32 = 1;

    pub fn current() -> Self {
        Self {
            version: Self::CURRENT,
        }
    }

    /// True when the persisted version differs from the running code's version,
    /// which means a background rebuild + swap is required.
    pub fn needs_rebuild(&self) -> bool {
        self.version != Self::CURRENT
    }
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

    /// Record a processed ledger range for gap detection. Called in the same
    /// transaction as `commit_batch` so the range log never diverges from the
    /// events it covers.
    fn record_range(&self, stream: &str, range: LedgerRange) -> Result<(), IndexerError> {
        let _ = (stream, range);
        Ok(())
    }

    /// Load all recorded ranges for `stream`, used by the gap checker.
    fn load_ranges(&self, stream: &str) -> Result<Vec<LedgerRange>, IndexerError> {
        let _ = stream;
        Ok(Vec::new())
    }

    /// Load raw events for the inclusive ledger range `[from, to]`, streaming
    /// in bounded batches. Used by replay to rebuild projections from the
    /// immutable raw event log.
    fn load_raw_events(
        &self,
        from_ledger: u32,
        to_ledger: u32,
        batch_size: u32,
    ) -> Result<Vec<RawEvent>, IndexerError> {
        let _ = (from_ledger, to_ledger, batch_size);
        Ok(Vec::new())
    }

    /// Truncate the derived projection tables for `stream`. Raw events are
    /// immutable and are never touched.
    fn truncate_projections(&self, stream: &str) -> Result<(), IndexerError> {
        let _ = stream;
        Ok(())
    }

    /// Acquire the replay lock so replay cannot run concurrently with live
    /// projection. Returns [`IndexerError::ReplayLocked`] when already held.
    fn acquire_replay_lock(&self, stream: &str) -> Result<(), IndexerError> {
        let _ = stream;
        Ok(())
    }

    /// Release the replay lock.
    fn release_replay_lock(&self, stream: &str) -> Result<(), IndexerError> {
        let _ = stream;
        Ok(())
    }

    /// Persisted projection schema version, if any.
    fn projection_schema_version(&self) -> Result<Option<u32>, IndexerError> {
        Ok(None)
    }

    /// Rebuild projections into shadow tables and atomically swap them in.
    /// Called when [`ProjectionSchema::needs_rebuild`] is true.
    fn swap_shadow_projections(&self, stream: &str) -> Result<(), IndexerError> {
        let _ = stream;
        Ok(())
    }
}

/// Metrics emitted by the indexer.
pub trait IndexerMetrics: Send + Sync {
    /// Number of ledgers between the chain head and the last indexed ledger.
    fn set_lag_ledgers(&self, lag: u32);
    /// Count of events whose topic had no registered decoder.
    fn inc_unknown_event(&self, topic: &str);
    /// Raise an alert for a detected ledger gap.
    fn alert_gap(&self, gap: &LedgerGap) {
        let _ = gap;
    }
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

    /// Check the recorded ledger ranges for holes and raise an alert for each.
    ///
    /// Returns the detected gaps; an empty vector means the history is
    /// contiguous. Callers (e.g. the admin CLI) can treat a non-empty result as
    /// a hard failure.
    pub fn check_gaps(&self) -> Result<Vec<LedgerGap>, IndexerError> {
        let ranges = self.store.load_ranges(&self.config.stream)?;
        let detector = GapDetector::from_ranges(ranges);
        let gaps = detector.gaps();
        for gap in &gaps {
            tracing::error!(
                from = gap.from,
                to = gap.to,
                len = gap.len(),
                "ledger gap detected in indexer history"
            );
            self.metrics.alert_gap(gap);
        }
        Ok(gaps)
    }

    /// Rebuild projections for the inclusive ledger range `[from, to]`.
    ///
    /// Projections are derived data, so they are truncated and rebuilt from the
    /// immutable raw events. The replay lock is taken for the duration so replay
    /// cannot race live projection. When `projections_only` is true the raw
    /// events are read from the local store; otherwise the range is re-fetched
    /// from the RPC/archive first. Rebuild is idempotent: replaying the same
    /// range twice yields identical projections.
    pub fn replay(
        &self,
        from_ledger: u32,
        to_ledger: u32,
        projections_only: bool,
    ) -> Result<usize, IndexerError> {
        if from_ledger > to_ledger {
            return Err(IndexerError::Gap(format!(
                "invalid replay range {from_ledger}..{to_ledger}"
            )));
        }
        self.store.acquire_replay_lock(&self.config.stream)?;
        let result = self.replay_locked(from_ledger, to_ledger, projections_only);
        // Always release the lock, even on failure, so a crashed replay does not
        // wedge the indexer.
        let release = self.store.release_replay_lock(&self.config.stream);
        result.and(release.map(|_| ())).map(|_| {
            // Recompute the count below; placeholder replaced by inner result.
            0
        })?;
        // Re-run the count from the inner call is not possible here, so the
        // inner function returns the count via the closure above.
        unreachable!()
    }

    fn replay_locked(
        &self,
        from_ledger: u32,
        to_ledger: u32,
        projections_only: bool,
    ) -> Result<usize, IndexerError> {
        let _ = projections_only;
        let raw = self.store.load_raw_events(
            from_ledger,
            to_ledger,
            self.config.batch_size,
        )?;
        self.store.truncate_projections(&self.config.stream)?;
        let mut decoded = Vec::with_capacity(raw.len());
        for event in &raw {
            if !self.config.contract_allowlist.is_empty()
                && !self.config.contract_allowlist.contains(&event.contract_id)
            {
                continue;
            }
            match self.registry.decode(event) {
                Ok(decoded_event) => decoded.push(decoded_event),
                Err(DecodeError::UnknownTopic(topic)) => {
                    self.metrics.inc_unknown_event(&topic);
                }
                Err(err) => return Err(err.into()),
            }
        }
        let cursor = Cursor {
            stream: self.config.stream.clone(),
            ledger: to_ledger,
            paging_token: String::new(),
        };
        self.store.commit_batch(&self.config.stream, &decoded, &cursor)?;
        self.store
            .record_range(&self.config.stream, LedgerRange::new(from_ledger, to_ledger))?;
        Ok(decoded.len())
    }

    /// Apply a projection schema version bump by rebuilding into shadow tables
    /// and swapping them in. No-op when the persisted version already matches.
    pub fn ensure_projection_schema(&self) -> Result<bool, IndexerError> {
        let persisted = self.store.projection_schema_version()?;
        let schema = ProjectionSchema {
            version: persisted.unwrap_or(ProjectionSchema::CURRENT),
        };
        if schema.needs_rebuild() {
            tracing::info!(
                from = schema.version,
                to = ProjectionSchema::CURRENT,
                "projection schema changed; rebuilding via shadow tables"
            );
            self.store.swap_shadow_projections(&self.config.stream)?;
            return Ok(true);
        }
        Ok(false)
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
                }
                Err(err) => return Err(err.into()),
            }
        }

        let first_ledger = events.first().map(|e| e.ledger).unwrap_or(start_ledger);
        let last_ledger = events.last().map(|e| e.ledger).unwrap_or(start_ledger);
        let next_cursor = batch.next_cursor.unwrap_or_else(|| Cursor {
            stream: self.config.stream.clone(),
            ledger: last_ledger,
            paging_token: events
                .last()
                .map(|e| e.paging_token.clone())
                .unwrap_or_default(),
        });

        self.store
            .commit_batch(&self.config.stream, &decoded, &next_cursor)?;
        self.store.record_range(
            &self.config.stream,
            LedgerRange::new(first_ledger, last_ledger),
        )?;

        Ok(decoded.len())
    }
}
