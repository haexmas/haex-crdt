//! Signature preflight tests: the trust contract from plan §4.2.
//!
//! The engine must call `on_before_apply` first, then verify every
//! sig-carrying change, and only then start writing. Any failure aborts the
//! whole batch and nothing lands — and, because both passes complete before
//! the transaction is opened, without needing a rollback to make that true.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value as JsonValue};

use super::{change, create_crdt_table, make_fixture};
use crate::crdt::apply::{
    apply_remote_changes, column_sig_preimage, column_sig_preimage_from_parts, SignatureApplyPolicy,
};
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::scanner::ColumnChange;
use crate::error::{Error, Result};
use crate::signature::{AuthorId, NoopSignatureProvider, RemoteChanges, SignatureProvider};

const HLC1: &str = "0000000000000001/abcdef0000000000000000000000";
const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";

fn signed(change: ColumnChange, sig: JsonValue) -> ColumnChange {
    ColumnChange {
        sig: Some(sig),
        ..change
    }
}

// ---------- rejecting-at-N provider ----------------------------------------

struct RejectAt {
    fail_at: usize,
    calls: Arc<AtomicUsize>,
}
impl SignatureProvider for RejectAt {
    fn sign_column(&self, _p: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn verify_column(&self, _p: &[u8], sig: &JsonValue) -> Result<()> {
        let n = self.calls.fetch_add(1, Ordering::Relaxed);
        let target = sig
            .as_object()
            .and_then(|m| m.get("idx"))
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);
        if target == Some(self.fail_at) || n == self.fail_at {
            Err(Error::UnexpectedSignatureUnderNoop)
        } else {
            Ok(())
        }
    }
    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

// ---------- on_before_apply-hook provider ---------------------------------

struct HookProvider {
    hook_fired: Arc<AtomicBool>,
    reject: bool,
}
impl SignatureProvider for HookProvider {
    fn sign_column(&self, _p: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn verify_column(&self, _p: &[u8], _sig: &JsonValue) -> Result<()> {
        Ok(())
    }
    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
    fn on_before_apply(&self, _changes: &RemoteChanges) -> Result<()> {
        self.hook_fired.store(true, Ordering::Relaxed);
        if self.reject {
            Err(Error::UnexpectedSignatureUnderNoop)
        } else {
            Ok(())
        }
    }
}

// --- tests ----------------------------------------------------------------

#[test]
fn on_before_apply_rejection_aborts_the_batch_before_any_write() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let fired = Arc::new(AtomicBool::new(false));
    let provider = HookProvider {
        hook_fired: fired.clone(),
        reject: true,
    };
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v"))],
        &hlc,
        &mut SignatureApplyPolicy::new(&provider),
    )
    .unwrap_err();
    assert!(matches!(err, Error::UnexpectedSignatureUnderNoop));
    assert!(fired.load(Ordering::Relaxed), "hook must have fired");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "no write may have landed");
}

#[test]
fn preflight_rejection_reports_offender_index_and_leaves_db_untouched() {
    // First sig ok, second sig fails — the whole batch must roll back and
    // the error names the second change's index.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let calls = Arc::new(AtomicUsize::new(0));
    let provider = RejectAt {
        fail_at: 1,
        calls: calls.clone(),
    };
    let batch = vec![
        signed(
            change("items", "r1", "body", HLC1, json!("a")),
            json!({"idx": 0}),
        ),
        signed(
            change("items", "r2", "body", HLC2, json!("b")),
            json!({"idx": 1}),
        ),
    ];
    let err = apply_remote_changes(
        &mut conn,
        batch,
        &hlc,
        &mut SignatureApplyPolicy::new(&provider),
    )
    .unwrap_err();
    match err {
        Error::SignatureVerificationFailed {
            first_failed_change,
        } => {
            assert_eq!(first_failed_change, 1);
        }
        other => panic!("wrong variant: {other:?}"),
    }
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "sig-fail must roll back the first change too");
}

#[test]
fn noop_provider_rejects_batch_carrying_non_null_signatures() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let batch = vec![signed(
        change("items", "r1", "body", HLC1, json!("v")),
        json!("real-sig"),
    )];
    let err = apply_remote_changes(
        &mut conn,
        batch,
        &hlc,
        &mut SignatureApplyPolicy::new(&NoopSignatureProvider),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        Error::SignatureVerificationFailed {
            first_failed_change: 0
        }
    ));
}

#[test]
fn accepted_sig_lands_in_column_sigs_json_map() {
    // A verified sig must persist in the column-sigs map so a downstream
    // peer can relay the change without re-signing.
    struct AcceptAll;
    impl SignatureProvider for AcceptAll {
        fn sign_column(&self, _p: &[u8]) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        fn verify_column(&self, _p: &[u8], _sig: &JsonValue) -> Result<()> {
            Ok(())
        }
        fn author_id(&self) -> AuthorId {
            AuthorId::anonymous()
        }
    }

    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let sig = json!({"authorDid": "did:key:zAlice", "sig": "base64bytes"});
    let batch = vec![signed(
        change("items", "r1", "body", HLC1, json!("v")),
        sig.clone(),
    )];
    apply_remote_changes(
        &mut conn,
        batch,
        &hlc,
        &mut SignatureApplyPolicy::new(&AcceptAll),
    )
    .unwrap();

    let sigs_json: String = conn
        .query_row(
            &format!("SELECT {COLUMN_SIGS_COLUMN} FROM items WHERE id = 'r1'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    let sigs: serde_json::Map<String, JsonValue> = serde_json::from_str(&sigs_json).unwrap();
    assert_eq!(sigs.get("body"), Some(&sig));

    // An unsigned newer value must not retain a signature for the old value.
    apply_remote_changes(
        &mut conn,
        vec![change(
            "items",
            "r1",
            "body",
            HLC2,
            json!("unsigned-new-value"),
        )],
        &hlc,
        &mut SignatureApplyPolicy::new(&NoopSignatureProvider),
    )
    .unwrap();
    let sigs_json: String = conn
        .query_row(
            &format!("SELECT {COLUMN_SIGS_COLUMN} FROM items WHERE id = 'r1'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    let sigs: serde_json::Map<String, JsonValue> = serde_json::from_str(&sigs_json).unwrap();
    assert!(!sigs.contains_key("body"));
}

#[test]
fn preimage_is_deterministic_and_field_sensitive() {
    // Sanity: identical fields → identical preimage; a swapped field breaks
    // it. The apply engine's verifier relies on the sender using the same
    // preimage, so a stable byte layout is load-bearing.
    let c1 = change("items", "r1", "body", HLC1, json!("v"));
    let c2 = change("items", "r1", "body", HLC1, json!("v"));
    assert_eq!(column_sig_preimage(&c1), column_sig_preimage(&c2));

    let c3 = change("items", "r1", "title", HLC1, json!("v"));
    assert_ne!(column_sig_preimage(&c1), column_sig_preimage(&c3));
}

struct LegacyBlobSignatureProvider {
    legacy_preimage: Vec<u8>,
}

impl SignatureProvider for LegacyBlobSignatureProvider {
    fn sign_column(&self, _preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn verify_column(&self, preimage: &[u8], _sig: &JsonValue) -> Result<()> {
        if preimage == self.legacy_preimage {
            Ok(())
        } else {
            Err(Error::UnexpectedSignatureUnderNoop)
        }
    }

    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

#[test]
fn legacy_blob_value_signature_is_accepted_after_encoding_migration() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body BLOB");
    let value = json!({"$blob_hex": "deadbeef"});
    let legacy_preimage =
        column_sig_preimage_from_parts("items", r#"{"id":"r1"}"#, "body", HLC1, &json!("3q2+7w=="));
    let provider = LegacyBlobSignatureProvider { legacy_preimage };

    apply_remote_changes(
        &mut conn,
        vec![signed(
            change("items", "r1", "body", HLC1, value),
            json!("legacy"),
        )],
        &hlc,
        &mut SignatureApplyPolicy::new(&provider),
    )
    .expect("legacy BLOB value signature must remain valid");

    let stored: Vec<u8> = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(stored, b"\xde\xad\xbe\xef");
}

#[test]
fn legacy_blob_primary_key_signature_is_accepted_after_encoding_migration() {
    let (mut conn, hlc, _dev) = make_fixture();
    conn.execute_batch(&format!(
        "CREATE TABLE items (
             id BLOB PRIMARY KEY NOT NULL,
             body TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .unwrap();
    let row_pks = r#"{"id":{"$blob_hex":"deadbeef"}}"#;
    let legacy_preimage = column_sig_preimage_from_parts(
        "items",
        r#"{"id":"3q2+7w=="}"#,
        "body",
        HLC1,
        &json!("value"),
    );
    let provider = LegacyBlobSignatureProvider { legacy_preimage };

    apply_remote_changes(
        &mut conn,
        vec![signed(
            ColumnChange {
                table_name: "items".to_string(),
                row_pks: row_pks.to_string(),
                column_name: "body".to_string(),
                hlc_timestamp: HLC1.to_string(),
                value: json!("value"),
                device_id: String::new(),
                sig: None,
            },
            json!("legacy"),
        )],
        &hlc,
        &mut SignatureApplyPolicy::new(&provider),
    )
    .expect("legacy BLOB primary-key signature must remain valid");

    let stored: Vec<u8> = conn
        .query_row("SELECT id FROM items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(stored, b"\xde\xad\xbe\xef");
}
