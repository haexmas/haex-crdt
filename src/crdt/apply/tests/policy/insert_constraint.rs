//! `on_insert_constraint`: real NOT NULL / UNIQUE / PRIMARY KEY violations,
//! split out of `policy.rs` to keep that file inside the repo's file-size
//! cap.

use super::*;

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

/// Simulates a consumer whose `prepare_row` hook writes a stub row for this
/// exact PK *before* the core's own INSERT runs for the same row — e.g. an
/// identity-resolution stub insert, as haex-vault's `prepare_row` does. The
/// core's own INSERT then collides on the row's PRIMARY KEY, not on a
/// UNIQUE column.
struct StubRowThenAcceptPolicy;
impl ApplyPolicy for StubRowThenAcceptPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        let id = row.row_pks["id"].as_str().expect("id pk is a string");
        tx.execute(
            &format!("INSERT INTO \"{}\" (id) VALUES (?)", row.table_name),
            [id],
        )?;
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
fn insert_primary_key_violation_from_a_policy_created_row_skip_row_continues_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "note TEXT");

    let mut policy = StubRowThenAcceptPolicy;
    let outcome = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "note", HLC1, json!("hi"))],
        &hlc,
        &mut policy,
    )
    .expect("on_insert_constraint returning SkipRow must not hard-abort the batch");

    assert_eq!(outcome.report.applied, 0);
    assert_eq!(outcome.report.skipped_insert_constraint, 1);
    assert_eq!(outcome.skipped[0].reason, SkipReason::InsertPrimaryKey);

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 1,
        "only the policy's own stub row may exist; the core's INSERT must not have landed"
    );
}

#[test]
fn primary_key_and_unique_violations_are_distinguished_by_extended_code_not_message_text() {
    // PRIMARY KEY: a policy-created stub row collides with the exact PK the
    // core's own INSERT targets. SQLite renders this with the same message
    // text as a UNIQUE violation ("UNIQUE constraint failed: ...") — only
    // the extended error code (1555 vs 2067) tells them apart.
    let (mut pk_conn, pk_hlc, _pk_dev) = make_fixture();
    create_crdt_table(&pk_conn, "items", "note TEXT");
    let mut pk_policy = StubRowThenAcceptPolicy;
    let pk_outcome = apply_remote_changes(
        &mut pk_conn,
        vec![change("items", "r1", "note", HLC1, json!("hi"))],
        &pk_hlc,
        &mut pk_policy,
    )
    .expect("SkipRow must not fail the batch");
    assert_eq!(
        pk_outcome.skipped[0].reason,
        SkipReason::InsertPrimaryKey,
        "a PK collision must classify as InsertPrimaryKey, not InsertUnique"
    );

    // UNIQUE: a genuine business-unique column collision on a *different*
    // row's PK.
    let (mut unique_conn, unique_hlc, _unique_dev) = make_fixture();
    create_crdt_table(&unique_conn, "items", "code TEXT UNIQUE");
    let mut unique_policy = SkipOnConstraintPolicy;
    let unique_outcome = apply_remote_changes(
        &mut unique_conn,
        vec![
            change("items", "r1", "code", HLC1, json!("dup")),
            change("items", "r2", "code", HLC2, json!("dup")),
        ],
        &unique_hlc,
        &mut unique_policy,
    )
    .expect("SkipRow must not fail the batch");
    let reason = unique_outcome
        .skipped
        .iter()
        .find(|s| s.input_index == 1)
        .map(|s| s.reason);
    assert_eq!(
        reason,
        Some(SkipReason::InsertUnique),
        "a same-value-different-row collision must still classify as InsertUnique"
    );
}
