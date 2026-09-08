//! The pre-transaction clock-drift gate.
//!
//! Within [`MAX_REMOTE_HLC_DRIFT`] a remote timestamp is stored *and* the
//! local clock is advanced past it; beyond it the whole batch is refused
//! before the transaction opens. Both halves are load-bearing and pinned
//! here: advancing is what lets a device whose clock trails the space win
//! LWW with its own subsequent writes, and refusing pre-transaction is what
//! makes a refusal mean "nothing landed" rather than "something might have".

use std::time::Duration;

use serde_json::json;

use super::{change, create_crdt_table, hlc_ahead_of_now, hlc_behind_now, make_fixture};
use crate::crdt::apply::apply_remote_changes;
use crate::crdt::columns::HLC_TIMESTAMP_COLUMN;
use crate::crdt::hlc::{hlc_is_newer, MAX_REMOTE_HLC_DRIFT};
use crate::error::Error;
use crate::signature::NoopSignatureProvider;

const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";
/// The margin test values sit either side of [`MAX_REMOTE_HLC_DRIFT`] by.
/// Wide enough that the wall clock cannot cross it while a test runs.
const MARGIN: Duration = Duration::from_secs(60 * 60);

fn row_count(conn: &rusqlite::Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
        .unwrap()
}

fn body(conn: &rusqlite::Connection) -> String {
    conn.query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap()
}

#[test]
fn an_over_drift_change_is_refused_before_anything_is_written() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let poisoned = hlc_ahead_of_now(&hlc, MAX_REMOTE_HLC_DRIFT + MARGIN);
    let err = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("hello")),
            change("items", "r2", "body", &poisoned, json!("theirs")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect_err("a batch carrying an over-drift HLC must be refused");

    assert!(
        matches!(err, Error::RemoteHlcDriftTooLarge { .. }),
        "expected a drift refusal, got: {err:?}"
    );
    // The point of gating before the transaction opens: an `Err` from this
    // call means nothing landed, including the batch's innocent siblings.
    // Asserting the `Err` alone would pass just as well if the gate sat
    // inside the write loop and rolled back after a partial apply.
    assert_eq!(
        row_count(&conn),
        0,
        "a refused batch must leave no row behind, not even its valid changes"
    );
}

#[test]
fn the_drift_refusal_names_the_timestamp_and_the_measured_drift() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let poisoned = hlc_ahead_of_now(&hlc, MAX_REMOTE_HLC_DRIFT + MARGIN);
    let err = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", &poisoned, json!("theirs"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect_err("refused");

    // The consumer quarantines the batch and asks the user about it, so the
    // error has to carry enough to name the peer's clock as the problem.
    match err {
        Error::RemoteHlcDriftTooLarge { hlc, drift, limit } => {
            assert_eq!(hlc, poisoned, "the offending timestamp must be named");
            assert_eq!(limit, MAX_REMOTE_HLC_DRIFT);
            let expected = MAX_REMOTE_HLC_DRIFT + MARGIN;
            assert!(
                drift <= expected && drift + Duration::from_secs(5) > expected,
                "drift must be the real measurement, give or take the elapsed \
                 wall clock: {drift:?}"
            );
        }
        other => panic!("expected RemoteHlcDriftTooLarge, got {other:?}"),
    }
}

#[test]
fn a_timestamp_just_inside_the_tolerance_is_accepted_and_advances_the_clock() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let ahead = hlc_ahead_of_now(&hlc, MAX_REMOTE_HLC_DRIFT - MARGIN);
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", &ahead, json!("theirs"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect("a timestamp inside the tolerance must be accepted");

    assert_eq!(report.applied, 1);
    assert_eq!(body(&conn), "theirs");
    // Advancing is the reason the tolerance is generous rather than zero:
    // this device's clock now covers the peer's, so its own next write wins
    // LWW against it instead of silently losing to it for the next 11 hours.
    let next = hlc.new_timestamp().expect("timestamp").to_string();
    assert!(
        hlc_is_newer(&next, &ahead),
        "the local clock must have advanced past the accepted remote HLC: \
         {next} vs {ahead}"
    );
}

#[test]
fn a_timestamp_far_in_the_past_is_accepted() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // The gate is one-sided, mirroring uhlc: only future drift is refused.
    // A peer whose clock trails by a month is not lying about the time, it
    // just loses every LWW race it enters.
    let behind = hlc_behind_now(&hlc, Duration::from_secs(30 * 24 * 60 * 60));
    let report = apply_remote_changes(
        &mut conn,
        vec![change(
            "items",
            "r1",
            "body",
            &behind,
            json!("stale but valid"),
        )],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect("a past timestamp is never a drift refusal");

    assert_eq!(report.applied, 1);
    assert_eq!(body(&conn), "stale but valid");
}

#[test]
fn a_malformed_timestamp_is_not_a_drift_refusal() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Drift is undefined for a string that is not a timestamp, so the gate
    // passes it through to the pipeline's existing handling: the LWW
    // comparator reads it as ancient and it loses. Pinned so the gate is
    // never "improved" into a parse check, which would turn every corrupt
    // row on a peer into a total sync stop.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", "not-a-timestamp", json!("x"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect("a malformed HLC must not be refused as drift");

    assert_eq!(report.applied, 0);
    assert_eq!(report.skipped_stale, 1);
    assert_eq!(row_count(&conn), 0);
}

#[test]
fn an_over_drift_change_the_write_loop_would_drop_still_fails_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Where the drift policy overrides skip-don't-reject. The reserved-column
    // guard would drop this change and count it, precisely so one poisoned
    // change cannot cost a peer its whole batch — but the drift gate runs
    // first and refuses the batch anyway.
    //
    // That is deliberate, not an oversight. The two rules answer different
    // questions: skip-don't-reject says an unusable *column* must not cost
    // the batch, while an over-drift HLC says the *clock* the batch is
    // ordered by is unusable, which invalidates its LWW ordering wholesale.
    // The residual denial-of-service — a peer stopping our sync by attaching
    // an over-drift HLC to a change we would have dropped — is accepted:
    // it is indistinguishable from the honest case the gate exists for, and
    // the consumer's answer to both is to quarantine the batch and ask.
    let poisoned = hlc_ahead_of_now(&hlc, MAX_REMOTE_HLC_DRIFT + MARGIN);
    let err = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("hello")),
            change(
                "items",
                "r1",
                HLC_TIMESTAMP_COLUMN,
                &poisoned,
                json!("whatever"),
            ),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .expect_err("the drift gate runs before the reserved-column guard");

    assert!(
        matches!(err, Error::RemoteHlcDriftTooLarge { .. }),
        "expected a drift refusal, got: {err:?}"
    );
    assert_eq!(row_count(&conn), 0, "nothing may land from a refused batch");
}
