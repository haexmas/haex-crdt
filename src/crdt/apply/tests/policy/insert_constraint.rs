//! `on_insert_constraint`: real NOT NULL / UNIQUE / PRIMARY KEY violations,
//! split out of `policy.rs` to keep that file inside the repo's file-size
//! cap.

use super::*;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};

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
fn insert_constraint_violation_aborts_by_default() {
    for primary_key in [false, true] {
        let (mut conn, hlc, _dev) = make_fixture();
        let (columns, expected_code, mut policy): (_, _, Box<dyn ApplyPolicy>) = if primary_key {
            (
                "body TEXT NOT NULL DEFAULT 'stub', note TEXT",
                rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY,
                Box::new(StubRowThenAcceptPolicy {
                    skip_constraint: false,
                }),
            )
        } else {
            (
                "body TEXT NOT NULL, note TEXT",
                rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL,
                Box::new(AcceptAllPolicy),
            )
        };
        create_crdt_table(&conn, "items", columns);
        let future = hlc_ahead_of_now(&hlc, Duration::from_secs(6 * 60 * 60));
        let err = apply_remote_changes(
            &mut conn,
            vec![
                change("items", "r0", "body", HLC1, json!("earlier write")),
                change("items", "r1", "note", &future, json!("hi")),
            ],
            &hlc,
            policy.as_mut(),
        )
        .unwrap_err();
        assert!(
            matches!(&err, Error::Sqlite(rusqlite::Error::SqliteFailure(code, _))
                if code.extended_code == expected_code),
            "the default hook must abort with the original SQL error, got {err:?}"
        );

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 0,
            "abort must roll back earlier writes and policy stubs"
        );
        assert!(hlc_is_newer(
            &future,
            &hlc.new_timestamp().unwrap().to_string()
        ));
    }
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
/// UNIQUE column. Only `r1` gets a stub so other rows can test batch progress.
struct StubRowThenAcceptPolicy {
    skip_constraint: bool,
}
impl ApplyPolicy for StubRowThenAcceptPolicy {
    fn preflight(&mut self, _changes: &RemoteChanges) -> Result<()> {
        Ok(())
    }
    fn prepare_row(&mut self, tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        let id = row.row_pks["id"].as_str().expect("id pk is a string");
        if id == "r1" {
            tx.execute(
                &format!("INSERT INTO \"{}\" (id) VALUES (?)", row.table_name),
                [id],
            )?;
        }
        accept_all(&row)
    }
    fn on_insert_constraint(
        &mut self,
        tx: &Transaction<'_>,
        attempted: RowWrite<'_>,
        error: &rusqlite::Error,
    ) -> Result<ConstraintDecision> {
        if self.skip_constraint {
            Ok(ConstraintDecision::SkipRow)
        } else {
            // Exercise the actual trait default without duplicating its verdict.
            AcceptAllPolicy.on_insert_constraint(tx, attempted, error)
        }
    }
}

#[test]
fn insert_primary_key_violation_from_a_policy_created_row_skip_row_continues_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "note TEXT, title TEXT");

    let mut policy = StubRowThenAcceptPolicy {
        skip_constraint: true,
    };
    let future = hlc_ahead_of_now(&hlc, Duration::from_secs(6 * 60 * 60));
    let outcome = apply_remote_changes(
        &mut conn,
        vec![
            // Groups use their minimum HLC: the conflicted row runs first,
            // while its second column would poison the clock if folded.
            change("items", "r1", "note", HLC1, json!("hi")),
            change("items", "r1", "title", &future, json!("skipped future")),
            change("items", "r2", "note", HLC2, json!("later row")),
        ],
        &hlc,
        &mut policy,
    )
    .expect("on_insert_constraint returning SkipRow must not hard-abort the batch");

    assert_eq!(
        outcome.report.applied, 1,
        "the row after the conflict must land"
    );
    assert_eq!(outcome.report.skipped_insert_constraint, 2);
    let mut skipped: Vec<_> = outcome
        .skipped
        .iter()
        .map(|s| (s.input_index, s.reason))
        .collect();
    skipped.sort_by_key(|(index, _)| *index);
    assert_eq!(
        skipped,
        vec![
            (0, SkipReason::InsertPrimaryKey),
            (1, SkipReason::InsertPrimaryKey)
        ]
    );

    let stub: (
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        String,
    ) = conn
        .query_row(
            &format!(
                "SELECT note, title, {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN}, \
                 {COLUMN_SIGS_COLUMN} FROM items WHERE id = 'r1'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        stub,
        (None, None, None, "{}".into(), "{}".into()),
        "the skipped INSERT must leave the policy stub and its metadata untouched"
    );
    let later: (String, String) = conn
        .query_row(
            &format!("SELECT note, {HLC_TIMESTAMP_COLUMN} FROM items WHERE id = 'r2'"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(later, ("later row".into(), HLC2.into()));
    assert!(
        hlc_is_newer(&future, &hlc.new_timestamp().unwrap().to_string()),
        "a primary-key-skipped row must not advance the clock"
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
    let mut pk_policy = StubRowThenAcceptPolicy {
        skip_constraint: true,
    };
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
