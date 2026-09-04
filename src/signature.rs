use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Opaque author identity carried in remote changes. Treated as an opaque
/// string by this crate; the consumer decides the shape (a DID, a device
/// pubkey, a UCAN principal).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorId(pub String);

impl AuthorId {
    pub fn anonymous() -> Self {
        AuthorId(String::new())
    }
}

/// Opaque envelope of remote changes handed to `Store::apply_remote_changes`.
/// The concrete layout is defined in `store` once the scanner/apply modules
/// are ported.
#[derive(Debug, Clone)]
pub struct RemoteChanges {
    /// Placeholder — real fields land with the scanner/apply port.
    pub raw: Vec<u8>,
}

/// Signs and verifies per-column CRDT payloads. Provides a batch-level
/// policy hook that runs inside the apply transaction before any write.
///
/// # Trust contract (plan §4.2)
///
/// - `apply_remote_changes` is all-or-nothing. It runs inside a single
///   `IMMEDIATE` transaction: pre-apply hook, preflight verification of
///   every column change, then writes. Any failure rolls back the whole
///   batch.
/// - This crate does not decide whether an empty signature is acceptable.
///   That is the provider's policy. `NoopSignatureProvider` accepts empty
///   signatures and rejects non-empty ones it cannot verify.
/// - **Consumers using `NoopSignatureProvider` MUST deliver remote changes
///   over an already-authenticated transport.** The no-op provider performs
///   no transport attestation. `haex-vault` uses an MLS group; `holzi` v1
///   uses an authenticated iroh channel between attested devices.
pub trait SignatureProvider: Send + Sync {
    fn sign_column(&self, preimage: &[u8]) -> Result<Vec<u8>>;

    fn verify_column(&self, preimage: &[u8], sig: &[u8], author: &AuthorId) -> Result<()>;

    fn author_id(&self) -> AuthorId;

    /// Row-level / batch-level policy hook. Called by
    /// `apply_remote_changes` before any write, inside the apply
    /// transaction. Returning `Err` rejects the entire batch; the
    /// transaction rolls back.
    fn on_before_apply(&self, changes: &RemoteChanges) -> Result<()> {
        let _ = changes;
        Ok(())
    }
}

/// No-op provider. Signs with empty payloads; accepts empty incoming
/// signatures. See the trust contract on `SignatureProvider` — this
/// provider requires an already-authenticated transport.
pub struct NoopSignatureProvider;

impl SignatureProvider for NoopSignatureProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn verify_column(&self, _preimage: &[u8], sig: &[u8], _author: &AuthorId) -> Result<()> {
        if sig.is_empty() {
            Ok(())
        } else {
            Err(Error::UnexpectedSignatureUnderNoop)
        }
    }

    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn author_id_anonymous_is_empty_string() {
        assert_eq!(AuthorId::anonymous().0, "");
    }

    #[test]
    fn noop_sign_column_returns_empty_payload_regardless_of_input() {
        let p = NoopSignatureProvider;
        assert!(p.sign_column(b"").unwrap().is_empty());
        assert!(p.sign_column(b"some preimage").unwrap().is_empty());
        assert!(p.sign_column(&[0xFF; 4096]).unwrap().is_empty());
    }

    #[test]
    fn noop_verify_column_accepts_empty_signature() {
        let p = NoopSignatureProvider;
        p.verify_column(b"any preimage", &[], &AuthorId("peer".into()))
            .expect("empty sig must be accepted");
    }

    #[test]
    fn noop_verify_column_rejects_non_empty_signature_with_unexpected_variant() {
        let p = NoopSignatureProvider;
        let err = p
            .verify_column(b"preimage", b"not-empty", &AuthorId("peer".into()))
            .unwrap_err();
        assert!(matches!(err, Error::UnexpectedSignatureUnderNoop));
    }

    #[test]
    fn noop_author_id_returns_anonymous() {
        let p = NoopSignatureProvider;
        assert_eq!(p.author_id(), AuthorId::anonymous());
    }

    #[test]
    fn noop_on_before_apply_default_accepts_any_batch() {
        let p = NoopSignatureProvider;
        let changes = RemoteChanges { raw: vec![1, 2, 3] };
        p.on_before_apply(&changes)
            .expect("default on_before_apply must be a no-op");
    }

    #[test]
    fn signature_provider_is_object_safe_via_dyn_dispatch() {
        // Ensures the trait can be stored behind Arc<dyn ...> as
        // `StoreConfig::signature_provider` requires (plan §6).
        let provider: std::sync::Arc<dyn SignatureProvider> =
            std::sync::Arc::new(NoopSignatureProvider);
        assert_eq!(provider.author_id(), AuthorId::anonymous());
    }
}
