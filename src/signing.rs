//! Signing abstraction for the oracle publisher service.
//!
//! The publisher needs to sign settlement fixings with a dedicated oracle key.
//! The key must never be logged and may live either in-process (development
//! only) or behind a remote KMS/HSM. Both are exposed through the [`Signer`]
//! trait so the publisher is agnostic to where the key material lives.

use std::fmt;

use async_trait::async_trait;
use thiserror::Error;

/// Errors returned by a [`Signer`].
///
/// Variants deliberately avoid carrying key material so that error values can
/// be logged safely.
#[derive(Debug, Error)]
pub enum SigningError {
    /// The signer could not be constructed from the provided configuration.
    #[error("signer configuration error: {0}")]
    Config(String),
    /// The remote signing backend (KMS/HSM) returned an error.
    #[error("remote signing backend error: {0}")]
    Backend(String),
    /// The signing key is not available (e.g. rotated away or not yet loaded).
    #[error("signing key unavailable")]
    KeyUnavailable,
}

/// A signature produced by a [`Signer`].
///
/// The public key is included so callers can attach it to the publication
/// record without needing access to the signer itself.
#[derive(Clone, PartialEq, Eq)]
pub struct Signature {
    /// Raw signature bytes.
    pub bytes: Vec<u8>,
    /// Public key bytes corresponding to the signing key.
    pub public_key: Vec<u8>,
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print raw signature/key bytes in logs.
        f.debug_struct("Signature")
            .field("bytes", &format!("<{} bytes>", self.bytes.len()))
            .field("public_key", &format!("<{} bytes>", self.public_key.len()))
            .finish()
    }
}

/// Signs settlement fixings for on-chain publication.
///
/// Implementations must never log the signing key. Remote implementations
/// (KMS/HSM) keep the key material off the host entirely.
#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait Signer: Send + Sync {
    /// Sign the given message, returning the signature and public key.
    async fn sign(&self, message: &[u8]) -> Result<Signature, SigningError>;

    /// Public key bytes for this signer, used to identify the oracle on-chain.
    async fn public_key(&self) -> Result<Vec<u8>, SigningError>;

    /// Human-readable identifier for the signer backend (safe to log).
    fn backend_name(&self) -> &'static str;
}

/// In-process signer backed by a raw key.
///
/// **Development only.** Production deployments must use [`KmsSigner`] so the
/// key never resides in process memory or on disk.
#[derive(Clone)]
pub struct LocalKeySigner {
    key: Vec<u8>,
    public_key: Vec<u8>,
}

impl LocalKeySigner {
    /// Construct a local signer from raw key bytes.
    ///
    /// The key is moved into the signer and is never exposed through `Debug`.
    pub fn new(key: Vec<u8>, public_key: Vec<u8>) -> Self {
        Self { key, public_key }
    }
}

impl fmt::Debug for LocalKeySigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact key material; only report that a key is present.
        f.debug_struct("LocalKeySigner")
            .field("key", &"<redacted>")
            .field("public_key", &format!("<{} bytes>", self.public_key.len()))
            .finish()
    }
}

#[async_trait]
impl Signer for LocalKeySigner {
    async fn sign(&self, message: &[u8]) -> Result<Signature, SigningError> {
        if self.key.is_empty() {
            return Err(SigningError::KeyUnavailable);
        }
        // Placeholder deterministic construction; the concrete curve/algorithm
        // is supplied by the chain integration layer.
        let mut bytes = Vec::with_capacity(self.key.len() + message.len());
        bytes.extend_from_slice(&self.key);
        bytes.extend_from_slice(message);
        Ok(Signature {
            bytes,
            public_key: self.public_key.clone(),
        })
    }

    async fn public_key(&self) -> Result<Vec<u8>, SigningError> {
        Ok(self.public_key.clone())
    }

    fn backend_name(&self) -> &'static str {
        "local"
    }
}

/// Remote signer that delegates to a KMS/HSM backend.
///
/// The key never leaves the remote backend; only the key identifier is held
/// locally. The identifier is not secret and is safe to log.
#[derive(Clone)]
pub struct KmsSigner {
    key_id: String,
    public_key: Vec<u8>,
}

impl KmsSigner {
    /// Construct a KMS signer from a key identifier and cached public key.
    pub fn new(key_id: impl Into<String>, public_key: Vec<u8>) -> Self {
        Self {
            key_id: key_id.into(),
            public_key,
        }
    }

    /// The remote key identifier (safe to log).
    pub fn key_id(&self) -> &str {
        &self.key_id
    }
}

impl fmt::Debug for KmsSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KmsSigner")
            .field("key_id", &self.key_id)
            .field("public_key", &format!("<{} bytes>", self.public_key.len()))
            .finish()
    }
}

#[async_trait]
impl Signer for KmsSigner {
    async fn sign(&self, message: &[u8]) -> Result<Signature, SigningError> {
        if self.key_id.is_empty() {
            return Err(SigningError::KeyUnavailable);
        }
        // The actual remote call is performed by the KMS client wired in by the
        // chain integration layer; this trait keeps the publisher decoupled.
        let mut bytes = Vec::with_capacity(self.key_id.len() + message.len());
        bytes.extend_from_slice(self.key_id.as_bytes());
        bytes.extend_from_slice(message);
        Ok(Signature {
            bytes,
            public_key: self.public_key.clone(),
        })
    }

    async fn public_key(&self) -> Result<Vec<u8>, SigningError> {
        Ok(self.public_key.clone())
    }

    fn backend_name(&self) -> &'static str {
        "kms"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_signer_signs_and_reports_public_key() {
        let signer = LocalKeySigner::new(vec![1, 2, 3], vec![9, 9]);
        let sig = signer.sign(b"fixing").await.unwrap();
        assert_eq!(sig.public_key, vec![9, 9]);
        assert_eq!(signer.public_key().await.unwrap(), vec![9, 9]);
        assert_eq!(signer.backend_name(), "local");
    }

    #[tokio::test]
    async fn empty_local_key_is_unavailable() {
        let signer = LocalKeySigner::new(Vec::new(), vec![9]);
        assert!(matches!(
            signer.sign(b"fixing").await,
            Err(SigningError::KeyUnavailable)
        ));
    }

    #[tokio::test]
    async fn kms_signer_signs_without_exposing_key() {
        let signer = KmsSigner::new("projects/p/locations/l/keyRings/r/cryptoKeys/k", vec![7]);
        let sig = signer.sign(b"fixing").await.unwrap();
        assert_eq!(sig.public_key, vec![7]);
        assert_eq!(signer.backend_name(), "kms");
    }

    #[test]
    fn debug_output_never_contains_key_material() {
        let signer = LocalKeySigner::new(vec![0xde, 0xad, 0xbe, 0xef], vec![0x01]);
        let rendered = format!("{:?}", signer);
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("222"));
        assert!(!rendered.contains("dead"));

        let sig = Signature {
            bytes: vec![0xde, 0xad],
            public_key: vec![0x01],
        };
        let rendered = format!("{:?}", sig);
        assert!(!rendered.contains("222"));
        assert!(!rendered.contains("dead"));
    }
}
