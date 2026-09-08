//! `ApplyPolicy` hook tests: malformed policy output, row- vs column-level
//! skip, `after_row` / `before_commit` failure semantics, the
//! `on_insert_constraint` hook on a real NOT NULL / UNIQUE violation, and
//! that a skip anywhere in the pipeline never advances the local clock.
//!
//! `SignatureApplyPolicy`-mediated behavior (drift, reserved columns,
//! `_no_sync`, unknown columns, stale/superseded, delete propagation) is
//! covered by `lww.rs`, `sig.rs`, `delete.rs`, `reserved_columns.rs`, and
//! `drift.rs` — every one of those now routes through `SignatureApplyPolicy`,
//! so their unchanged assertions are exactly the proof that the adapter
//! reproduces today's direct-`SignatureProvider` behavior.

use std::time::Duration;

use rusqlite::types::Value as SqlValue;
use rusqlite::Transaction;
use serde_json::json;

use super::{change, create_crdt_table, hlc_ahead_of_now, make_fixture};
use crate::crdt::apply::{
    apply_remote_changes, ApplyOutcome, ApplyPolicy, ColumnDecision, ConstraintDecision,
    RowDecision, RowInput, RowWrite, SignatureWrite, SkipReason,
};
use crate::crdt::hlc::hlc_is_newer;
use crate::crdt::scanner::ColumnChange;
use crate::error::{Error, Result};
use crate::signature::RemoteChanges;
use crate::table_names::TABLE_CRDT_CONFIGS;

mod regressions;

const HLC1: &str = "0000000000000001/abcdef0000000000000000000000";
const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";

fn text_value(change: &ColumnChange) -> SqlValue {
    SqlValue::Text(change.value.as_str().unwrap_or_default().to_string())
}

/// Accept every eligible column unconditionally, preserving whatever
/// signature-map entry already exists. The baseline several test policies
/// start from before overriding one specific hook.
fn accept_all(row: &RowInput<'_>) -> Result<RowDecision> {
    let mut decisions = Vec::with_capacity(row.eligible_indices.len());
    for &idx in row.eligible_indices {
        let change = row.changes[idx].change;
        decisions.push(ColumnDecision::Accept {
            value: text_value(change),
            signature: SignatureWrite::Keep,
        });
    }
    Ok(RowDecision::Columns(decisions))
}

struct AcceptAllPolicy;
impl ApplyPolicy for AcceptAllPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        accept_all(&row)
    }
}

// -----------------------------------------------------------------------
// Malformed policy output
// -----------------------------------------------------------------------

struct WrongCountPolicy;
impl ApplyPolicy for WrongCountPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, _row: RowInput<'_>) -> Result<RowDecision> {
        // Always wrong: this row has at least one eligible column.
        Ok(RowDecision::Columns(vec![]))
    }
}

#[test]
fn wrong_column_decision_count_is_rejected_with_an_error() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let mut policy = WrongCountPolicy;
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v"))],
        &hlc,
        &mut policy,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Message(_)),
        "expected a validation error naming the mismatch, got {err:?}"
    );

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "malformed policy output must abort the whole batch, not write partially"
    );
}

// -----------------------------------------------------------------------
// Row-level vs column-level skip
// -----------------------------------------------------------------------

struct SkipRowPolicy;
impl ApplyPolicy for SkipRowPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, _row: RowInput<'_>) -> Result<RowDecision> {
        Ok(RowDecision::Skip)
    }
}

#[test]
fn row_level_policy_skip_drops_the_whole_row_and_does_not_advance_the_clock() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let mut policy = SkipRowPolicy;
    let poisoned = hlc_ahead_of_now(&hlc, Duration::from_secs(6 * 60 * 60));
    let outcome = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", &poisoned, json!("v"))],
        &hlc,
        &mut policy,
    )
    .expect("a row-level policy skip must not fail the batch");

    assert_eq!(outcome.report.applied, 0);
    assert_eq!(outcome.report.skipped_policy, 1);
    assert_eq!(outcome.skipped.len(), 1);
    assert_eq!(outcome.skipped[0].input_index, 0);
    assert_eq!(outcome.skipped[0].reason, SkipReason::Policy);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);

    let next = hlc.new_timestamp().expect("timestamp").to_string();
    assert!(
        hlc_is_newer(&poisoned, &next),
        "a policy-skipped row must not advance the local clock"
    );
}

struct SkipNamedColumnPolicy {
    column_to_skip: &'static str,
}
impl ApplyPolicy for SkipNamedColumnPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        let mut decisions = Vec::with_capacity(row.eligible_indices.len());
        for &idx in row.eligible_indices {
            let change = row.changes[idx].change;
            if change.column_name == self.column_to_skip {
                decisions.push(ColumnDecision::Skip);
            } else {
                decisions.push(ColumnDecision::Accept {
                    value: text_value(change),
                    signature: SignatureWrite::Keep,
                });
            }
        }
        Ok(RowDecision::Columns(decisions))
    }
}

#[test]
fn column_level_policy_skip_drops_only_that_column() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT, title TEXT");

    let mut policy = SkipNamedColumnPolicy {
        column_to_skip: "title",
    };
    let outcome = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC1, json!("hello")),
            change("items", "r1", "title", HLC1, json!("world")),
        ],
        &hlc,
        &mut policy,
    )
    .unwrap();

    assert_eq!(outcome.report.applied, 1, "only body may land");
    assert_eq!(outcome.report.skipped_policy, 1);
    let skipped_title = outcome
        .skipped
        .iter()
        .find(|s| s.input_index == 1)
        .expect("the title change must be recorded as skipped");
    assert_eq!(skipped_title.reason, SkipReason::Policy);

    let (body, title): (String, Option<String>) = conn
        .query_row("SELECT body, title FROM items WHERE id = 'r1'", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(body, "hello");
    assert_eq!(title, None, "the column-skipped value must not land");
}

// -----------------------------------------------------------------------
// `after_row` failure
// -----------------------------------------------------------------------

struct FailAfterRowPolicy;
impl ApplyPolicy for FailAfterRowPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        accept_all(&row)
    }
    fn after_row(&mut self, _tx: &Transaction<'_>, _written: RowWrite<'_>) -> Result<()> {
        Err(Error::Message("deliberate after_row failure".to_string()))
    }
}

#[test]
fn after_row_failure_aborts_the_whole_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let mut policy = FailAfterRowPolicy;
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v"))],
        &hlc,
        &mut policy,
    )
    .unwrap_err();
    assert!(matches!(err, Error::Message(_)));

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "after_row failing must roll back the write it was called for"
    );
}

// -----------------------------------------------------------------------
// `on_insert_constraint`: a real NOT NULL / UNIQUE violation
// -----------------------------------------------------------------------

struct SkipOnConstraintPolicy;
impl ApplyPolicy for SkipOnConstraintPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        accept_all(&row)
    }
    fn on_insert_constraint(
        &mut self,
        _tx: &Transaction<'_>,
        _attempted: RowWrite<'_>,
        _error: &rusqlite::Error,
    ) -> Result<ConstraintDecision> {
        Ok(ConstraintDecision::SkipRow)
    }
}

#[test]
fn insert_not_null_violation_skip_row_continues_the_batch_and_does_not_advance_the_clock() {
    let (mut conn, hlc, _dev) = make_fixture();
    // `body` is NOT NULL with no default; the change below only touches
    // `note`, so the generated INSERT omits `body` entirely and SQLite
    // raises a real NOT NULL constraint violation.
    create_crdt_table(&conn, "items", "body TEXT NOT NULL, note TEXT");

    let mut policy = SkipOnConstraintPolicy;
    let poisoned = hlc_ahead_of_now(&hlc, Duration::from_secs(6 * 60 * 60));
    let outcome = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "note", &poisoned, json!("hi"))],
        &hlc,
        &mut policy,
    )
    .expect("SkipRow must not fail the batch");

    assert_eq!(outcome.report.applied, 0);
    assert_eq!(outcome.report.skipped_insert_constraint, 1);
    assert_eq!(outcome.skipped[0].reason, SkipReason::InsertNotNull);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "the failed insert must not have landed");

    let next = hlc.new_timestamp().expect("timestamp").to_string();
    assert!(
        hlc_is_newer(&poisoned, &next),
        "a constraint-skipped row must not advance the local clock"
    );
}

#[test]
fn insert_not_null_violation_aborts_by_default() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT NOT NULL, note TEXT");

    let mut policy = AcceptAllPolicy;
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "note", HLC1, json!("hi"))],
        &hlc,
        &mut policy,
    )
    .unwrap_err();
    assert!(
        matches!(err, Error::Sqlite(_)),
        "the default hook must abort with the original SQL error, got {err:?}"
    );

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn insert_unique_violation_skip_row_continues_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "code TEXT UNIQUE");

    let mut policy = SkipOnConstraintPolicy;
    let outcome = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "code", HLC1, json!("dup")),
            change("items", "r2", "code", HLC2, json!("dup")),
        ],
        &hlc,
        &mut policy,
    )
    .expect("SkipRow must not fail the batch");

    assert_eq!(outcome.report.applied, 1, "only the first row may land");
    assert_eq!(outcome.report.skipped_insert_constraint, 1);
    let reason = outcome
        .skipped
        .iter()
        .find(|s| s.input_index == 1)
        .map(|s| s.reason);
    assert_eq!(reason, Some(SkipReason::InsertUnique));

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "only the first row may exist");
}

// -----------------------------------------------------------------------
// `before_commit` failure rolls back everything, including a policy's own
// side-table writes made earlier in the same transaction
// -----------------------------------------------------------------------

struct SideEffectThenFailBeforeCommitPolicy;
impl ApplyPolicy for SideEffectThenFailBeforeCommitPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn begin(&mut self, tx: &Transaction<'_>, _changes: &RemoteChanges) -> Result<()> {
        tx.execute_batch("CREATE TABLE policy_side (id TEXT)")?;
        tx.execute("INSERT INTO policy_side (id) VALUES ('begin')", [])?;
        Ok(())
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        accept_all(&row)
    }
    fn after_row(&mut self, tx: &Transaction<'_>, _written: RowWrite<'_>) -> Result<()> {
        tx.execute("INSERT INTO policy_side (id) VALUES ('after_row')", [])?;
        Ok(())
    }
    fn before_commit(
        &mut self,
        _tx: &Transaction<'_>,
        _changes: &RemoteChanges,
        _outcome: &ApplyOutcome,
    ) -> Result<()> {
        Err(Error::Message(
            "deliberate before_commit failure".to_string(),
        ))
    }
}

#[test]
fn before_commit_failure_rolls_back_everything_including_policy_side_effects() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let mut policy = SideEffectThenFailBeforeCommitPolicy;
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v"))],
        &hlc,
        &mut policy,
    )
    .unwrap_err();
    assert!(matches!(err, Error::Message(_)));

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "the merge itself must roll back");

    let side_table_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'policy_side'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        side_table_exists, 0,
        "a before_commit failure must roll back everything the policy wrote \
         earlier in the same transaction, including its own side table"
    );
}

// -----------------------------------------------------------------------
// FK-disabled and trigger-state restoration on an error path
// -----------------------------------------------------------------------

struct FailBeginPolicy;
impl ApplyPolicy for FailBeginPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn begin(&mut self, _tx: &Transaction<'_>, _changes: &RemoteChanges) -> Result<()> {
        Err(Error::Message("deliberate begin failure".to_string()))
    }
    fn prepare_row(&mut self, _tx: &Transaction<'_>, _row: RowInput<'_>) -> Result<RowDecision> {
        unreachable!("begin fails before any row is processed")
    }
}

#[test]
fn fk_and_trigger_state_are_restored_after_an_error_from_begin() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

    let mut policy = FailBeginPolicy;
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v"))],
        &hlc,
        &mut policy,
    )
    .unwrap_err();
    assert!(matches!(err, Error::Message(_)));

    let fk_enabled: bool = conn
        .query_row("PRAGMA foreign_keys", [], |r| r.get(0))
        .unwrap();
    assert!(
        fk_enabled,
        "foreign_keys must be restored after an error from a policy hook"
    );

    let triggers_enabled: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        triggers_enabled, "1",
        "trigger state must not be left disabled after a mid-transaction error"
    );
}
