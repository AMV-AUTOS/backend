//! Typed decoder registry for Zenith Soroban contract events.
//!
//! The indexer fetches raw contract events from the Soroban RPC `getEvents`
//! endpoint and hands each one to this registry. The registry maps the event's
//! topic symbol (the first `ScVal` topic) to a typed decoder that turns the
//! raw `ScVal` topics/data into a domain event.
//!
//! Design constraints (see issue #45):
//! * Unknown events must never crash the indexer. They are stored raw and
//!   counted in a metric so operators can spot schema drift.
//! * Decoders are pure functions over `ScVal` so they can be unit tested with
//!   real XDR fixtures and reused by the projection layer.
//! * A contract upgrade may change an event schema; decoders therefore return
//!   a `Result` and the caller decides whether to fall back to raw storage.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use stellar_xdr::curr::{ScSymbol, ScVal};

/// Domain events emitted by the Zenith options contracts.
///
/// Each variant carries the decoded payload for one on-chain event. The
/// projection layer persists these into the typed projection tables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DomainEvent {
    /// A new option series was created by the option factory.
    SeriesCreated(SeriesCreated),
    /// A caller minted an option position.
    OptionMinted(OptionMinted),
    /// A holder exercised an option position.
    OptionExercised(OptionExercised),
    /// Collateral was deposited into the vault.
    CollateralDeposited(CollateralDeposited),
    /// A series was settled by the settlement contract.
    Settled(Settled),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesCreated {
    pub series_id: u64,
    pub underlying: String,
    pub strike: i128,
    pub expiry: u64,
    pub is_call: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionMinted {
    pub series_id: u64,
    pub holder: String,
    pub amount: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionExercised {
    pub series_id: u64,
    pub holder: String,
    pub amount: i128,
    pub payout: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollateralDeposited {
    pub series_id: u64,
    pub depositor: String,
    pub amount: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settled {
    pub series_id: u64,
    pub settlement_price: i128,
}

/// Errors produced while decoding a single event.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The event has no topics, so it cannot be routed.
    #[error("event has no topics")]
    MissingTopic,
    /// The first topic is not a symbol, so it cannot be routed.
    #[error("event topic is not a symbol")]
    TopicNotSymbol,
    /// The topic symbol is not registered in the decoder registry.
    #[error("unknown event topic: {0}")]
    UnknownTopic(String),
    /// The payload did not match the expected schema for this topic.
    #[error("malformed payload for {topic}: {reason}")]
    Malformed { topic: String, reason: String },
}

/// A decoder turns raw `ScVal` topics/data into a typed [`DomainEvent`].
///
/// Decoders are `Send + Sync` so the registry can be shared across the
/// indexer's worker tasks without cloning the underlying functions.
pub type DecoderFn = Arc<dyn Fn(&[ScVal], &ScVal) -> Result<DomainEvent, DecodeError> + Send + Sync>;

/// Registry mapping event topic symbols to typed decoders.
///
/// The registry is immutable after construction; build it once at startup and
/// share it with the ingestion pipeline.
#[derive(Clone, Default)]
pub struct DecoderRegistry {
    decoders: HashMap<String, DecoderFn>,
}

impl DecoderRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            decoders: HashMap::new(),
        }
    }

    /// Create a registry pre-populated with the Zenith contract decoders.
    pub fn zenith() -> Self {
        let mut registry = Self::new();
        registry.register("series_created", Arc::new(decode_series_created));
        registry.register("option_minted", Arc::new(decode_option_minted));
        registry.register("option_exercised", Arc::new(decode_option_exercised));
        registry.register("collateral_deposited", Arc::new(decode_collateral_deposited));
        registry.register("settled", Arc::new(decode_settled));
        registry
    }

    /// Register a decoder for a topic symbol.
    pub fn register(&mut self, topic: impl Into<String>, decoder: DecoderFn) {
        self.decoders.insert(topic.into(), decoder);
    }

    /// Decode a raw event into a typed [`DomainEvent`].
    ///
    /// Returns [`DecodeError::UnknownTopic`] for unregistered topics so the
    /// caller can store the event raw and bump the unknown-event metric
    /// instead of crashing the indexer.
    pub fn decode(&self, topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
        let topic = topic_symbol(topics)?;
        let decoder = self
            .decoders
            .get(&topic)
            .ok_or_else(|| DecodeError::UnknownTopic(topic.clone()))?;
        decoder(topics, data)
    }

    /// Whether a topic symbol is registered.
    pub fn contains(&self, topic: &str) -> bool {
        self.decoders.contains_key(topic)
    }
}

/// Extract the routing symbol from an event's topics.
fn topic_symbol(topics: &[ScVal]) -> Result<String, DecodeError> {
    let first = topics.first().ok_or(DecodeError::MissingTopic)?;
    match first {
        ScVal::Symbol(ScSymbol(sym)) => Ok(sym.to_string()),
        _ => Err(DecodeError::TopicNotSymbol),
    }
}

/// Decode a `series_created` event.
fn decode_series_created(_topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
    let fields = expect_map(data, "series_created")?;
    Ok(DomainEvent::SeriesCreated(SeriesCreated {
        series_id: expect_u64(&fields, "series_id", "series_created")?,
        underlying: expect_string(&fields, "underlying", "series_created")?,
        strike: expect_i128(&fields, "strike", "series_created")?,
        expiry: expect_u64(&fields, "expiry", "series_created")?,
        is_call: expect_bool(&fields, "is_call", "series_created")?,
    }))
}

/// Decode an `option_minted` event.
fn decode_option_minted(_topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
    let fields = expect_map(data, "option_minted")?;
    Ok(DomainEvent::OptionMinted(OptionMinted {
        series_id: expect_u64(&fields, "series_id", "option_minted")?,
        holder: expect_string(&fields, "holder", "option_minted")?,
        amount: expect_i128(&fields, "amount", "option_minted")?,
    }))
}

/// Decode an `option_exercised` event.
fn decode_option_exercised(_topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
    let fields = expect_map(data, "option_exercised")?;
    Ok(DomainEvent::OptionExercised(OptionExercised {
        series_id: expect_u64(&fields, "series_id", "option_exercised")?,
        holder: expect_string(&fields, "holder", "option_exercised")?,
        amount: expect_i128(&fields, "amount", "option_exercised")?,
        payout: expect_i128(&fields, "payout", "option_exercised")?,
    }))
}

/// Decode a `collateral_deposited` event.
fn decode_collateral_deposited(_topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
    let fields = expect_map(data, "collateral_deposited")?;
    Ok(DomainEvent::CollateralDeposited(CollateralDeposited {
        series_id: expect_u64(&fields, "series_id", "collateral_deposited")?,
        depositor: expect_string(&fields, "depositor", "collateral_deposited")?,
        amount: expect_i128(&fields, "amount", "collateral_deposited")?,
    }))
}

/// Decode a `settled` event.
fn decode_settled(_topics: &[ScVal], data: &ScVal) -> Result<DomainEvent, DecodeError> {
    let fields = expect_map(data, "settled")?;
    Ok(DomainEvent::Settled(Settled {
        series_id: expect_u64(&fields, "series_id", "settled")?,
        settlement_price: expect_i128(&fields, "settlement_price", "settled")?,
    }))
}

/// A decoded event payload keyed by field name.
type Fields = HashMap<String, ScVal>;

/// Interpret an `ScVal` as a map of field name to value.
fn expect_map(data: &ScVal, topic: &str) -> Result<Fields, DecodeError> {
    match data {
        ScVal::Map(Some(map)) => {
            let mut fields = HashMap::with_capacity(map.len() as usize);
            for entry in map.iter() {
                let key = match &entry.key {
                    ScVal::Symbol(ScSymbol(sym)) => sym.to_string(),
                    ScVal::String(s) => s.to_string(),
                    _ => continue,
                };
                fields.insert(key, entry.val.clone());
            }
            Ok(fields)
        }
        _ => Err(DecodeError::Malformed {
            topic: topic.to_string(),
            reason: "expected a map payload".to_string(),
        }),
    }
}

fn malformed(topic: &str, field: &str) -> DecodeError {
    DecodeError::Malformed {
        topic: topic.to_string(),
        reason: format!("missing or invalid field `{field}`"),
    }
}

fn expect_u64(fields: &Fields, field: &str, topic: &str) -> Result<u64, DecodeError> {
    match fields.get(field) {
        Some(ScVal::U64(v)) => Ok(*v),
        Some(ScVal::U32(v)) => Ok(*v as u64),
        _ => Err(malformed(topic, field)),
    }
}

fn expect_i128(fields: &Fields, field: &str, topic: &str) -> Result<i128, DecodeError> {
    match fields.get(field) {
        Some(ScVal::I128(parts)) => Ok(((parts.hi as i128) << 64) | (parts.lo as i128)),
        Some(ScVal::I64(v)) => Ok(*v as i128),
        _ => Err(malformed(topic, field)),
    }
}

fn expect_bool(fields: &Fields, field: &str, topic: &str) -> Result<bool, DecodeError> {
    match fields.get(field) {
        Some(ScVal::Bool(v)) => Ok(*v),
        _ => Err(malformed(topic, field)),
    }
}

fn expect_string(fields: &Fields, field: &str, topic: &str) -> Result<String, DecodeError> {
    match fields.get(field) {
        Some(ScVal::String(s)) => Ok(s.to_string()),
        Some(ScVal::Symbol(ScSymbol(sym))) => Ok(sym.to_string()),
        _ => Err(malformed(topic, field)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{ScMap, ScMapEntry, StringM, VecM};

    fn symbol(s: &str) -> ScVal {
        ScVal::Symbol(ScSymbol(StringM::try_from(s).unwrap()))
    }

    fn map(entries: Vec<(&str, ScVal)>) -> ScVal {
        let entries: Vec<ScMapEntry> = entries
            .into_iter()
            .map(|(k, v)| ScMapEntry {
                key: symbol(k),
                val: v,
            })
            .collect();
        ScVal::Map(Some(ScMap(VecM::try_from(entries).unwrap())))
    }

    fn i128_val(v: i128) -> ScVal {
        ScVal::I128(stellar_xdr::curr::Int128Parts {
            hi: (v >> 64) as i64,
            lo: v as u64,
        })
    }

    #[test]
    fn decodes_series_created() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("series_created")];
        let data = map(vec![
            ("series_id", ScVal::U64(7)),
            ("underlying", ScVal::String(StringM::try_from("XLM").unwrap())),
            ("strike", i128_val(1_000)),
            ("expiry", ScVal::U64(1_700_000_000)),
            ("is_call", ScVal::Bool(true)),
        ]);
        let decoded = registry.decode(&topics, &data).unwrap();
        assert_eq!(
            decoded,
            DomainEvent::SeriesCreated(SeriesCreated {
                series_id: 7,
                underlying: "XLM".to_string(),
                strike: 1_000,
                expiry: 1_700_000_000,
                is_call: true,
            })
        );
    }

    #[test]
    fn decodes_option_minted() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("option_minted")];
        let data = map(vec![
            ("series_id", ScVal::U64(7)),
            ("holder", ScVal::String(StringM::try_from("GABC").unwrap())),
            ("amount", i128_val(42)),
        ]);
        let decoded = registry.decode(&topics, &data).unwrap();
        assert_eq!(
            decoded,
            DomainEvent::OptionMinted(OptionMinted {
                series_id: 7,
                holder: "GABC".to_string(),
                amount: 42,
            })
        );
    }

    #[test]
    fn decodes_option_exercised() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("option_exercised")];
        let data = map(vec![
            ("series_id", ScVal::U64(7)),
            ("holder", ScVal::String(StringM::try_from("GABC").unwrap())),
            ("amount", i128_val(42)),
            ("payout", i128_val(84)),
        ]);
        let decoded = registry.decode(&topics, &data).unwrap();
        assert_eq!(
            decoded,
            DomainEvent::OptionExercised(OptionExercised {
                series_id: 7,
                holder: "GABC".to_string(),
                amount: 42,
                payout: 84,
            })
        );
    }

    #[test]
    fn decodes_collateral_deposited() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("collateral_deposited")];
        let data = map(vec![
            ("series_id", ScVal::U64(7)),
            ("depositor", ScVal::String(StringM::try_from("GABC").unwrap())),
            ("amount", i128_val(500)),
        ]);
        let decoded = registry.decode(&topics, &data).unwrap();
        assert_eq!(
            decoded,
            DomainEvent::CollateralDeposited(CollateralDeposited {
                series_id: 7,
                depositor: "GABC".to_string(),
                amount: 500,
            })
        );
    }

    #[test]
    fn decodes_settled() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("settled")];
        let data = map(vec![
            ("series_id", ScVal::U64(7)),
            ("settlement_price", i128_val(1_234)),
        ]);
        let decoded = registry.decode(&topics, &data).unwrap();
        assert_eq!(
            decoded,
            DomainEvent::Settled(Settled {
                series_id: 7,
                settlement_price: 1_234,
            })
        );
    }

    #[test]
    fn unknown_topic_is_reported_not_panicked() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("some_future_event")];
        let data = map(vec![("series_id", ScVal::U64(1))]);
        let err = registry.decode(&topics, &data).unwrap_err();
        assert!(matches!(err, DecodeError::UnknownTopic(t) if t == "some_future_event"));
    }

    #[test]
    fn missing_topic_is_reported() {
        let registry = DecoderRegistry::zenith();
        let err = registry.decode(&[], &ScVal::Void).unwrap_err();
        assert!(matches!(err, DecodeError::MissingTopic));
    }

    #[test]
    fn malformed_payload_is_reported() {
        let registry = DecoderRegistry::zenith();
        let topics = vec![symbol("series_created")];
        let data = map(vec![("series_id", ScVal::U64(7))]);
        let err = registry.decode(&topics, &data).unwrap_err();
        assert!(matches!(err, DecodeError::Malformed { .. }));
    }
}
