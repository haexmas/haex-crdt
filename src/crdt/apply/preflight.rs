//! Pre-transaction preflight — every check that can refuse a batch, run to
//! completion *before* [`super::apply_remote_changes`] opens its
//! transaction, per plan §4.2's all-or-nothing trust contract.
//!
//! [`preflight_batch`] is the whole phase. It runs the core's own checks,
//! then hands off to the policy's own batch-level veto:
//!
//! 1. **Identifier safety** — every change's `table_name` and `column_name`
//!    must be a safe SQL identifier, so everything downstream may build SQL
//!    without re-checking.
//! 2. **HLC validity and clock drift** — a complete HLC must parse, and no
//!    change may carry an HLC further than [`MAX_REMOTE_HLC_DRIFT`] beyond
//!    local now.
//! 3. **[`crate::crdt::apply::ApplyPolicy::preflight`]** — the policy's own
//!    whole-batch check, with no transaction open yet.
//!    [`crate::crdt::apply::SignatureApplyPolicy`] reproduces today's
//!    `SignatureProvider`-based sequence exactly:
//!    [`SignatureProvider::on_before_apply`] then [`verify_all_signatures`].
//!
//! # What a consumer may rely on
//!
//! An `Err` from any of the four means **nothing was written** — not
//! "written, then rolled back". No transaction exists yet at that point, so
//! there is no partial state to reconcile, no rollback to depend on, and no
//! window in which a crash could expose a half-applied batch. That is what
//! makes quarantine-and-retry safe: a consumer can park the refused batch,
//! ask the user, and resubmit it verbatim later without first inspecting
//! local state to work out how far the previous attempt got.
//!
//! Adding a new whole-batch check means adding it here, not to the write
//! loop, so that guarantee keeps holding.
//!
//! # Why signatures are not verified inline
//!
//! Verifying as each column is about to be written would let earlier writes
//! land before a later verification failure aborts the batch. The
//! transaction's rollback would then be doing the crate's crash-safety work
//! — correct, but a bad shape: the [`SignatureProvider::verify_column`]
//! calls would happen while the DB is being mutated, and any timing- or
//! trace-based side effect a real provider might emit would refer to a
//! partly-applied state that never lands.
//!
//! Verifying up front also lets
//! [`crate::error::Error::SignatureVerificationFailed`] name the offending
//! change by index without the caller having to correlate against an
//! interleaved apply log.

use crate::crdt::apply::policy::ApplyPolicy;
use crate::crdt::apply::preimage::column_sig_preimage;
use crate::crdt::hlc::{remote_hlc_drift, MAX_REMOTE_HLC_DRIFT};
use crate::crdt::scanner::ColumnChange;
use crate::crdt::trigger::is_safe_identifier;
use crate::db::error::DatabaseError;
use crate::error::{Error, Result};
use crate::signature::{RemoteChanges, SignatureProvider};
use std::str::FromStr;
use uhlc::Timestamp;

/// Run the whole pre-transaction phase over `changes`, returning the first
/// refusal. See the module docs for the phases and for what an `Err` from
/// this function guarantees to the caller.
pub fn preflight_batch(changes: &RemoteChanges, policy: &mut dyn ApplyPolicy) -> Result<()> {
    check_identifiers_and_drift(changes)?;
    policy.preflight(changes)?;
    Ok(())
}

/// Phases 1 and 2: refuse identifier-unsafe, malformed, and
/// clock-implausible input at the crate boundary, per change, in batch order.
///
/// The two identifier checks come first, ahead of the drift check, because
/// they decide whether the change is even well-formed enough to be talked
/// about in a SQL statement; reading a clock to reject a change whose table
/// name could not be quoted anyway would be answering the wrong question.
fn check_identifiers_and_drift(changes: &[ColumnChange]) -> Result<()> {
    for change in changes {
        if !is_safe_identifier(&change.table_name) {
            return Err(DatabaseError::ValidationError {
                reason: format!(
                    "Invalid table name '{}' in remote change",
                    change.table_name
                ),
            }
            .into());
        }
        if !is_safe_identifier(&change.column_name) {
            return Err(DatabaseError::ValidationError {
                reason: format!(
                    "Invalid column name '{}' in table '{}'",
                    change.column_name, change.table_name
                ),
            }
            .into());
        }
        // A complete HLC must pass uhlc's parser before it can reach the
        // write loop. The numeric comparator intentionally has a forgiving
        // fallback for malformed strings, but that fallback can rank a
        // malformed full timestamp as newest while `advance_past_remote`
        // rejects it after the transaction commits.
        if change.hlc_timestamp.contains('/') {
            Timestamp::from_str(&change.hlc_timestamp).map_err(|error| {
                Error::Hlc(format!(
                    "Invalid remote HLC timestamp '{}': {error:?}",
                    change.hlc_timestamp
                ))
            })?;
        }

        // Clock-drift gate. Beyond the tolerance an HLC is not a reading of
        // anybody's clock, so the batch's LWW ordering is meaningless and
        // there is nothing in it worth salvaging — refuse the whole call
        // rather than skip the change. Refusing from here, before the
        // transaction is opened, is also what stops the post-commit
        // `advance_past_remote` in the engine from being a drift risk:
        // every HLC that reaches it has already cleared this gate against
        // the same clock.
        //
        // Strings without the full `<time>/<node>` shape are deliberately not
        // refused here. Drift is undefined for them, and
        // `compare_hlc_strings` reads them as ancient so they lose LWW and
        // land in `skipped_stale`.
        if let Some(drift) = remote_hlc_drift(&change.hlc_timestamp) {
            if drift > MAX_REMOTE_HLC_DRIFT {
                return Err(Error::RemoteHlcDriftTooLarge {
                    hlc: change.hlc_timestamp.clone(),
                    drift,
                    limit: MAX_REMOTE_HLC_DRIFT,
                });
            }
        }
    }
    Ok(())
}

/// Verify every `sig`-carrying change in `changes` against the provider,
/// in list order. Returns the index of the first change whose sig fails
/// (mapped into [`Error::SignatureVerificationFailed`]) or `Ok(())` if all
/// signed changes verify.
///
/// Changes with `sig: None` are skipped — verify policy for absent
/// signatures is entirely the provider's business. `NoopSignatureProvider`
/// accepts absent sigs; a strict provider would raise on absence in
/// [`SignatureProvider::on_before_apply`] before this pass runs.
pub fn verify_all_signatures(
    changes: &[ColumnChange],
    provider: &dyn SignatureProvider,
) -> Result<()> {
    for (idx, change) in changes.iter().enumerate() {
        let Some(sig) = &change.sig else {
            continue;
        };
        let preimage = column_sig_preimage(change);
        if provider.verify_column(&preimage, sig).is_err() {
            return Err(Error::SignatureVerificationFailed {
                first_failed_change: idx,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
            // `DatabaseError::ValidationError` flattens through
            // `From<DatabaseError> for Error`, so the variant is `Message`.
            Error::Message(msg) => assert!(
                msg.contains(needle),
                "expected a validation error naming {needle:?}, got: {msg}"
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
        drifted[0].hlc_timestamp =
            hlc_ahead_of_now(MAX_REMOTE_HLC_DRIFT + Duration::from_secs(3600));
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
}
