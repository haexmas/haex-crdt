use super::*;
use crate::crdt::apply::signature_policy::SignatureApplyPolicy;
use crate::signature::{AuthorId, NoopSignatureProvider};
use serde_json::{json, Value as JsonValue};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

fn change_with_sig(idx: usize, sig: Option<JsonValue>) -> ColumnChange {
    ColumnChange {
        table_name: "items".to_string(),
        row_pks: format!(r#"{{"id":"r{idx}"}}"#),
        column_name: "body".to_string(),
        hlc_timestamp: format!("{idx:016}/abcdef"),
        value: json!("v"),
        device_id: String::new(),
        sig,
    }
}

struct AllowingProvider;
impl SignatureProvider for AllowingProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn verify_column(&self, _preimage: &[u8], _sig: &JsonValue) -> Result<()> {
        Ok(())
    }
    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

struct RejectingProvider {
    fail_at: usize,
}
impl SignatureProvider for RejectingProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn verify_column(&self, _preimage: &[u8], sig: &JsonValue) -> Result<()> {
        let idx: usize = sig
            .as_object()
            .and_then(|m| m.get("idx"))
            .and_then(|v| v.as_u64())
            .map(|n| n as usize)
            .unwrap_or(usize::MAX);
        if idx == self.fail_at {
            Err(Error::UnexpectedSignatureUnderNoop)
        } else {
            Ok(())
        }
    }
    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

#[test]
fn empty_batch_passes() {
    verify_all_signatures(&[], &AllowingProvider).unwrap();
}

#[test]
fn all_unsigned_passes_regardless_of_provider() {
    // sig == None never calls the provider — verify by using a provider
    // that would reject anything.
    let batch = vec![change_with_sig(0, None), change_with_sig(1, None)];
    verify_all_signatures(&batch, &RejectingProvider { fail_at: 0 }).unwrap();
}

#[test]
fn noop_provider_accepts_null_sigs() {
    let batch = vec![
        change_with_sig(0, Some(JsonValue::Null)),
        change_with_sig(1, Some(JsonValue::Null)),
    ];
    verify_all_signatures(&batch, &NoopSignatureProvider).unwrap();
}

#[test]
fn noop_provider_rejects_non_null_sig_at_first_offender() {
    let batch = vec![
        change_with_sig(0, Some(JsonValue::Null)),
        change_with_sig(1, Some(json!("not-empty"))),
        change_with_sig(2, Some(JsonValue::Null)),
    ];
    let err = verify_all_signatures(&batch, &NoopSignatureProvider).unwrap_err();
    match err {
        Error::SignatureVerificationFailed {
            first_failed_change,
        } => {
            assert_eq!(first_failed_change, 1, "must report the first offender");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn preflight_reports_first_failure_index_not_signed_only_index() {
    // Batch has an unsigned change interleaved with signed ones. The
    // returned index must refer to the FULL batch position, not the
    // subset-of-signed position — otherwise callers can't locate the
    // change in their own record.
    let batch = vec![
        change_with_sig(0, None),                    // batch pos 0
        change_with_sig(1, Some(json!({"idx": 1}))), // batch pos 1 (signed pos 0)
        change_with_sig(2, None),                    // batch pos 2
        change_with_sig(3, Some(json!({"idx": 3}))), // batch pos 3 (signed pos 1, fail here)
    ];
    let provider = RejectingProvider { fail_at: 3 };
    let err = verify_all_signatures(&batch, &provider).unwrap_err();
    match err {
        Error::SignatureVerificationFailed {
            first_failed_change,
        } => {
            assert_eq!(first_failed_change, 3);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

/// An HLC `offset` beyond the live wall clock. Derived from the clock so
/// a value chosen to sit outside the tolerance cannot rot into it.
fn hlc_ahead_of_now(offset: Duration) -> String {
    let shifted = uhlc::system_time_clock().as_u64() + uhlc::NTP64::from(offset).as_u64();
    format!("{shifted}/abcdef")
}

/// Records whether the consumer's batch hook was reached, so the phase
/// order can be asserted rather than inferred from the source.
#[derive(Default)]
struct RecordingProvider {
    hook_called: AtomicBool,
}
impl SignatureProvider for RecordingProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn verify_column(&self, _preimage: &[u8], _sig: &JsonValue) -> Result<()> {
        Ok(())
    }
    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
    fn on_before_apply(&self, _changes: &RemoteChanges) -> Result<()> {
        self.hook_called.store(true, Ordering::SeqCst);
        Ok(())
    }
}

fn assert_validation_error(err: Error, needle: &str) {
    match err {
        Error::Database(DatabaseError::ValidationError { reason }) => assert!(
            reason.contains(needle),
            "expected a validation error naming {needle:?}, got: {reason}"
        ),
        other => panic!("expected a validation error, got {other:?}"),
    }
}

#[test]
fn preflight_rejects_an_unsafe_table_name() {
    let mut batch = vec![change_with_sig(0, None)];
    batch[0].table_name = "items; DROP TABLE users".to_string();
    let mut policy = SignatureApplyPolicy::new(&NoopSignatureProvider);
    let err = preflight_batch(&batch, &mut policy).unwrap_err();
    assert_validation_error(err, "Invalid table name");
}

#[test]
fn preflight_rejects_an_unsafe_column_name() {
    let mut batch = vec![change_with_sig(0, None)];
    batch[0].column_name = "body\" = 1 --".to_string();
    let mut policy = SignatureApplyPolicy::new(&NoopSignatureProvider);
    let err = preflight_batch(&batch, &mut policy).unwrap_err();
    assert_validation_error(err, "Invalid column name");
}

#[test]
fn an_unsafe_identifier_is_reported_ahead_of_drift_on_the_same_change() {
    // Phase order, pinned on a change that violates both. Reading a
    // clock to reject a change whose table name could not be quoted
    // anyway would answer the wrong question, and would change which
    // error a consumer sees for input that has always been refused as
    // malformed.
    let mut batch = vec![change_with_sig(0, None)];
    batch[0].table_name = "bad;name".to_string();
    batch[0].hlc_timestamp = hlc_ahead_of_now(MAX_REMOTE_HLC_DRIFT + Duration::from_secs(3600));
    let mut policy = SignatureApplyPolicy::new(&NoopSignatureProvider);
    let err = preflight_batch(&batch, &mut policy).unwrap_err();
    assert_validation_error(err, "Invalid table name");
}

#[test]
fn the_input_boundary_runs_before_the_consumer_hook() {
    // The consumer's authorization hook must never be handed a batch the
    // crate has already decided to refuse on shape or clock grounds.
    let mut unsafe_batch = vec![change_with_sig(0, None)];
    unsafe_batch[0].table_name = "bad;name".to_string();
    let provider = RecordingProvider::default();
    let mut policy = SignatureApplyPolicy::new(&provider);
    preflight_batch(&unsafe_batch, &mut policy).unwrap_err();
    assert!(
        !provider.hook_called.load(Ordering::SeqCst),
        "on_before_apply must not see an identifier-unsafe batch"
    );

    let mut drifted = vec![change_with_sig(0, None)];
    drifted[0].hlc_timestamp = hlc_ahead_of_now(MAX_REMOTE_HLC_DRIFT + Duration::from_secs(3600));
    let provider = RecordingProvider::default();
    let mut policy = SignatureApplyPolicy::new(&provider);
    let err = preflight_batch(&drifted, &mut policy).unwrap_err();
    assert!(
        matches!(err, Error::RemoteHlcDriftTooLarge { .. }),
        "expected a drift refusal, got: {err:?}"
    );
    assert!(
        !provider.hook_called.load(Ordering::SeqCst),
        "on_before_apply must not see an over-drift batch"
    );
}

#[test]
fn preflight_accepts_a_clean_batch_and_reaches_the_consumer_hook() {
    let batch = vec![change_with_sig(0, None), change_with_sig(1, None)];
    let provider = RecordingProvider::default();
    let mut policy = SignatureApplyPolicy::new(&provider);
    preflight_batch(&batch, &mut policy).expect("a clean batch must pass every phase");
    assert!(
        provider.hook_called.load(Ordering::SeqCst),
        "a batch that clears the boundary must reach the consumer hook"
    );
}
