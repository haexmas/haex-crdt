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
