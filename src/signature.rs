use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::crdt::scanner::ColumnChange;
use crate::error::{Error, Result};

/// Opaque author identity a provider may attach to its own signed writes.
/// Treated as an opaque string by this crate; the consumer decides the shape
/// (a DID, a device pubkey, a UCAN principal).
///
/// The apply pipeline does **not** pass this to `verify_column` — a peer's
/// author lives inside the opaque `sig: JsonValue` on the incoming
/// [`ColumnChange`], and the provider extracts it itself when verifying.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthorId(pub String);

impl AuthorId {
    pub fn anonymous() -> Self {
        AuthorId(String::new())
    }
}

/// Payload handed to `apply_remote_changes`. A flat list of column-level
/// changes; the apply engine groups by transaction-HLC internally.
///
/// This is a type alias over [`ColumnChange`] so the same record travels
/// scanner → wire → apply without shape shifts. The `sig: Option<JsonValue>`
/// stays opaque to the crate — consumers who need per-column signatures
/// carry their own wire shape end-to-end and decode it inside their
/// [`SignatureProvider`].
pub type RemoteChanges = Vec<ColumnChange>;

/// Signs and verifies per-column CRDT payloads. Provides a batch-level
/// policy hook that runs inside the apply transaction before any write.
///
/// # Trust contract (plan §4.2)
///
/// - `apply_remote_changes` is all-or-nothing. It runs inside a single
///   `IMMEDIATE` transaction: pre-apply hook, preflight verification of
///   every column change, then writes. Any failure rolls back the whole
///   batch.
/// - This crate does not decide whether an empty (or absent) signature is
///   acceptable. That is the provider's policy.
/// - The `sig` argument to [`verify_column`] is the **raw JSON** the change
///   carried through the wire, exactly as the scanner emitted it on the
///   sender side (see [`ColumnChange::sig`]). The provider owns the shape
///   end-to-end and decodes it internally.
/// - **Consumers using [`NoopSignatureProvider`] MUST deliver remote changes
///   over an already-authenticated transport.** The no-op provider performs
///   no transport attestation. `haex-vault` uses an MLS group; `holzi` v1
///   uses an authenticated iroh channel between attested devices.
pub trait SignatureProvider: Send + Sync {
    /// Sign a canonical preimage for a local column write. Called by the
    /// consumer-side sign-on-write path (haex-vault's `column_sig` module);
    /// the returned bytes are the provider's business, but must round-trip
    /// through the same provider's [`verify_column`] on the receiving side.
    fn sign_column(&self, preimage: &[u8]) -> Result<Vec<u8>>;

    /// Verify a column change's signature against its preimage. Called by
    /// [`crate::crdt::apply::apply_remote_changes`] during the preflight
    /// pass, before any write. `sig` is the raw JSON the change carried
    /// through the wire (the same value that [`ColumnChange::sig`] would
    /// carry on the scanner side). Returning `Err` aborts the whole batch.
    fn verify_column(&self, preimage: &[u8], sig: &JsonValue) -> Result<()>;

    /// The identity the provider uses for its own signed writes. Independent
    /// of any peer's author — not passed to [`verify_column`].
    fn author_id(&self) -> AuthorId;

    /// Row-level / batch-level policy hook. Called by
    /// [`crate::crdt::apply::apply_remote_changes`] before any write, inside
    /// the apply transaction and before the per-column preflight. Returning
    /// `Err` rejects the entire batch; the transaction rolls back.
    fn on_before_apply(&self, changes: &RemoteChanges) -> Result<()> {
        let _ = changes;
        Ok(())
    }
}

/// No-op provider. Signs with empty payloads; accepts absent or JSON-`null`
/// signatures and rejects everything else. See the trust contract on
/// [`SignatureProvider`] — this provider requires an already-authenticated
/// transport.
pub struct NoopSignatureProvider;

impl SignatureProvider for NoopSignatureProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn verify_column(&self, _preimage: &[u8], sig: &JsonValue) -> Result<()> {
        // The apply pipeline only calls `verify_column` for changes whose
        // `sig` field is `Some(_)`. Reaching this method with a JSON-null
        // means the peer explicitly sent a null-payload signature; treat it
        // the same as an absent one (accept). Anything else is rejected.
        if sig.is_null() {
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
    use serde_json::json;

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
    fn noop_verify_column_accepts_json_null_signature() {
        let p = NoopSignatureProvider;
        p.verify_column(b"any preimage", &JsonValue::Null)
            .expect("null sig must be accepted");
    }

    #[test]
    fn noop_verify_column_rejects_non_null_signature_with_unexpected_variant() {
        let p = NoopSignatureProvider;
        for sig in [
            json!("not-empty"),
            json!({"authorDid": "did:key:z6M...", "bytes": "AAAA"}),
            json!([1, 2, 3]),
        ] {
            let err = p.verify_column(b"preimage", &sig).unwrap_err();
            assert!(
                matches!(err, Error::UnexpectedSignatureUnderNoop),
                "sig {sig:?} must be rejected as unexpected"
            );
        }
    }

    #[test]
    fn noop_author_id_returns_anonymous() {
        let p = NoopSignatureProvider;
        assert_eq!(p.author_id(), AuthorId::anonymous());
    }

    #[test]
    fn noop_on_before_apply_default_accepts_any_batch() {
        let p = NoopSignatureProvider;
        let changes: RemoteChanges = Vec::new();
        p.on_before_apply(&changes)
            .expect("default on_before_apply must be a no-op");
    }

    #[test]
    fn signature_provider_is_object_safe_via_dyn_dispatch() {
        // Ensures the trait can be stored behind Arc<dyn ...> as
        // `DatabaseConfig::signature_provider` requires (plan §6).
        let provider: std::sync::Arc<dyn SignatureProvider> =
            std::sync::Arc::new(NoopSignatureProvider);
        assert_eq!(provider.author_id(), AuthorId::anonymous());
    }
}
