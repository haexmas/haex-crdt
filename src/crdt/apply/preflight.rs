//! Signature preflight — the pass that MUST run to completion before any
//! write, per plan §4.2's all-or-nothing trust contract.
//!
//! Skipping preflight and verifying inline (as each column is about to be
//! written) would let earlier writes land before a later verification
//! failure aborts the batch. The transaction's rollback would then be
//! doing the crate's crash-safety work — correct, but a bad shape: the
//! `SignatureProvider::verify_column` calls happen while the DB is being
//! mutated, and any timing- or trace-based side effect a real provider
//! might emit would refer to a partly-applied state that never lands.
//!
//! Doing preflight first also lets [`crate::error::Error::SignatureVerificationFailed`]
//! name the offending change by index without the caller having to correlate
//! against an interleaved apply log.

use crate::crdt::apply::preimage::column_sig_preimage;
use crate::crdt::scanner::ColumnChange;
use crate::error::{Error, Result};
use crate::signature::SignatureProvider;

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
    use crate::signature::{AuthorId, NoopSignatureProvider};
    use serde_json::{json, Value as JsonValue};

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
            Error::SignatureVerificationFailed { first_failed_change } => {
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
            change_with_sig(0, None),                         // batch pos 0
            change_with_sig(1, Some(json!({"idx": 1}))),      // batch pos 1 (signed pos 0)
            change_with_sig(2, None),                         // batch pos 2
            change_with_sig(3, Some(json!({"idx": 3}))),      // batch pos 3 (signed pos 1, fail here)
        ];
        let provider = RejectingProvider { fail_at: 3 };
        let err = verify_all_signatures(&batch, &provider).unwrap_err();
        match err {
            Error::SignatureVerificationFailed { first_failed_change } => {
                assert_eq!(first_failed_change, 3);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
