//! Columns apply refuses to accept from a peer: `_no_sync` (the consumer's
//! never-ship rule), the crate's own three metadata columns, and PKs.
//!
//! This is the inbound mirror of the scanner's `partition_columns`. The
//! tests pin the exploits the filter closes, not just the filter's
//! presence — on the INSERT path the staged remote columns precede the
//! crate's own in the column list, and SQLite takes the FIRST value for a
//! duplicated column, so an unfiltered remote metadata column wins over
//! the crate's computed one.

use serde_json::json;

use super::{change, create_crdt_table, make_fixture};
use crate::crdt::apply::apply_remote_changes;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::HlcService;
use crate::signature::NoopSignatureProvider;

const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";
const HLC3: &str = "0000000000000003/abcdef0000000000000000000000";
/// Newer than every legitimate HLC in these tests, which is all they
/// compare it against. Deliberately NOT out of uhlc's drift tolerance: the
/// time part parses as decimal NTP64 units, so this is decades in the
/// *past*, and `advance_past_remote` accepts it. See
/// [`far_future_hlc`] for a genuinely out-of-tolerance value.
const HLC_ATTACKER: &str = "9999999999999999/dead000000000000000000000000";

/// An HLC an hour beyond the local clock — past uhlc's drift tolerance, so
/// `advance_past_remote` refuses it. Derived from the live clock rather
/// than hardcoded so the test cannot rot into tolerance.
fn far_future_hlc(hlc: &HlcService) -> String {
    let ts = hlc.new_timestamp().unwrap();
    // NTP64 counts 2^32 units per second.
    let future = ts.get_time().as_u64() + 3600 * (1u64 << 32);
    format!("{future}/{}", ts.get_id())
}

fn row_hlc(conn: &rusqlite::Connection, id: &str) -> String {
    conn.query_row(
        &format!("SELECT {HLC_TIMESTAMP_COLUMN} FROM items WHERE id = ?1"),
        [id],
        |r| r.get(0),
    )
    .unwrap()
}

fn column_hlcs(conn: &rusqlite::Connection, id: &str) -> String {
    conn.query_row(
        &format!("SELECT {COLUMN_HLCS_COLUMN} FROM items WHERE id = ?1"),
        [id],
        |r| r.get(0),
    )
    .unwrap()
}

fn column_sigs(conn: &rusqlite::Connection, id: &str) -> String {
    conn.query_row(
        &format!("SELECT {COLUMN_SIGS_COLUMN} FROM items WHERE id = ?1"),
        [id],
        |r| r.get(0),
    )
    .unwrap()
}

// -----------------------------------------------------------------------
// INSERT path — the exploitable one
// -----------------------------------------------------------------------

#[test]
fn insert_keeps_the_computed_row_hlc_over_a_remote_one() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Fresh row, so this takes the INSERT path. The peer names the row-HLC
    // column alongside a legitimate change.
    let changes = vec![
        change("items", "r1", "body", HLC2, json!("hello")),
        change(
            "items",
            "r1",
            HLC_TIMESTAMP_COLUMN,
            HLC_ATTACKER,
            json!(HLC_ATTACKER),
        ),
    ];
    let report = apply_remote_changes(&mut conn, changes, &hlc, &NoopSignatureProvider).unwrap();

    assert_eq!(
        row_hlc(&conn, "r1"),
        HLC2,
        "the row HLC must be the crate's computed value, not the peer's"
    );
    assert_eq!(report.applied, 1, "only the body change may apply");
    assert_eq!(report.skipped_reserved_column, 1);
}

#[test]
fn insert_keeps_the_computed_column_hlc_map_over_a_remote_one() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let changes = vec![
        change("items", "r1", "body", HLC2, json!("hello")),
        change(
            "items",
            "r1",
            COLUMN_HLCS_COLUMN,
            HLC_ATTACKER,
            json!(format!("{{\"body\":\"{HLC_ATTACKER}\"}}")),
        ),
    ];
    let report = apply_remote_changes(&mut conn, changes, &hlc, &NoopSignatureProvider).unwrap();

    let map = column_hlcs(&conn, "r1");
    assert!(
        map.contains(HLC2) && !map.contains(HLC_ATTACKER),
        "per-column HLCs must be the crate's, not the peer's: {map}"
    );
    assert!(
        !map.contains(COLUMN_HLCS_COLUMN),
        "no per-column HLC entry may be created for a metadata column: {map}"
    );
    assert_eq!(report.skipped_reserved_column, 1);
}

#[test]
fn insert_rejects_a_remote_signature_map() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // A forged sigs map on a fresh row is the forgery vector: a consumer
    // that treats the map as authorization evidence would read the peer's
    // entry as verified.
    let forged = r#"{"body":"forged-signature"}"#;
    let changes = vec![
        change("items", "r1", "body", HLC2, json!("hello")),
        change(
            "items",
            "r1",
            COLUMN_SIGS_COLUMN,
            HLC_ATTACKER,
            json!(forged),
        ),
    ];
    let report = apply_remote_changes(&mut conn, changes, &hlc, &NoopSignatureProvider).unwrap();

    let sigs = column_sigs(&conn, "r1");
    assert!(
        !sigs.contains("forged-signature"),
        "a peer must not be able to populate the signature map: {sigs}"
    );
    assert_eq!(report.skipped_reserved_column, 1);
}

#[test]
fn insert_ignores_a_remote_primary_key_column() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let changes = vec![
        change("items", "r1", "body", HLC2, json!("hello")),
        change("items", "r1", "id", HLC3, json!("hijacked")),
    ];
    let report = apply_remote_changes(&mut conn, changes, &hlc, &NoopSignatureProvider).unwrap();

    let ids: Vec<String> = conn
        .prepare("SELECT id FROM items")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        ids,
        vec!["r1".to_string()],
        "row identity comes from row_pks; a change naming a PK must not set it"
    );
    assert_eq!(report.skipped_reserved_column, 1);
}

// -----------------------------------------------------------------------
// UPDATE path — currently neutralised only by assignment ordering
// -----------------------------------------------------------------------

#[test]
fn update_ignores_remote_metadata_columns() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC2, json!("v2"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // The row now exists, so this takes the UPDATE path, where the crate's
    // own SET assignments are appended last and last-wins protects the
    // value. Assert it rather than rely on that ordering: the peer's change
    // must not reach the statement at all, so it also must not leave an
    // entry in the per-column HLC map.
    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC3, json!("v3")),
            change(
                "items",
                "r1",
                HLC_TIMESTAMP_COLUMN,
                HLC_ATTACKER,
                json!(HLC_ATTACKER),
            ),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    assert_eq!(row_hlc(&conn, "r1"), HLC3);
    let map = column_hlcs(&conn, "r1");
    assert!(
        !map.contains(HLC_TIMESTAMP_COLUMN) && !map.contains(HLC_ATTACKER),
        "the peer's metadata change must leave no trace: {map}"
    );
    assert_eq!(report.skipped_reserved_column, 1);
}

#[test]
fn update_ignores_a_remote_primary_key_column() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC2, json!("v2"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // On the UPDATE path a PK assignment is NOT neutralised by ordering:
    // `SET "id" = ?` would repoint the row the WHERE clause just matched.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "id", HLC3, json!("hijacked"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    let ids: Vec<String> = conn
        .prepare("SELECT id FROM items")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        ids,
        vec!["r1".to_string()],
        "a remote PK assignment must not rewrite row identity"
    );
    assert_eq!(report.skipped_reserved_column, 1);
}

// -----------------------------------------------------------------------
// `_no_sync` columns — skip and count, never reject the batch
// -----------------------------------------------------------------------

#[test]
fn remote_no_sync_column_is_skipped_while_the_rest_of_the_batch_applies() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT, last_pull_cursor_no_sync TEXT");

    // The anti-DoS property: one poisoned change must not cost the batch.
    // Rejecting would hand any peer a way to stop the victim's sync
    // entirely by including one such change every time.
    let report = apply_remote_changes(
        &mut conn,
        vec![
            change(
                "items",
                "r1",
                "last_pull_cursor_no_sync",
                HLC3,
                json!("theirs"),
            ),
            change("items", "r1", "body", HLC2, json!("hello")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "hello", "the sibling change must still land");
    let cursor: Option<String> = conn
        .query_row(
            "SELECT last_pull_cursor_no_sync FROM items WHERE id = 'r1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cursor, None, "this device's cursor must not be clobbered");
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped_no_sync_column, 1);
    assert_eq!(
        report.skipped_reserved_column, 0,
        "a consumer column must not be counted as a crate-reserved one"
    );
}

#[test]
fn unknown_column_still_counts_as_unknown() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Ordering guard: the reserved/`_no_sync` checks run after the
    // unknown-column check, so schema drift keeps reporting as drift even
    // when the absent column happens to carry a reserved-looking name.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "gone_no_sync", HLC2, json!("x"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    assert_eq!(report.skipped_unknown_column, 1);
    assert_eq!(report.skipped_no_sync_column, 0);
}

#[test]
fn a_dropped_change_with_an_out_of_tolerance_hlc_does_not_fail_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // End-to-end anti-DoS: the clock advance must cover what apply
    // accepted, not what it received. Advancing past the received maximum
    // lets a peer attach an out-of-tolerance HLC to a change this guard
    // drops and poison the whole call — the exact denial-of-service the
    // skip-don't-reject rule exists to deny, reachable through the very
    // changes we drop.
    let poisoned = far_future_hlc(&hlc);
    let report = apply_remote_changes(
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
    .expect("a change apply dropped must not fail the batch");

    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped_reserved_column, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "hello", "the sibling change must still land");
    assert_eq!(
        row_hlc(&conn, "r1"),
        HLC2,
        "the dropped change must leave no trace, in the row or in the clock"
    );
}

#[test]
fn remote_no_trigger_column_is_accepted() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT, updated_at_no_trigger TEXT");

    // The "must accept" half of the mirror. `_no_trigger` governs what
    // fires a trigger, `_no_sync` what participates in sync at all: the
    // scanner ships `_no_trigger` columns under the row HLC, so apply must
    // take them. Collapsing the guard into
    // `ends_with("_no_sync") || ends_with("_no_trigger")` would break every
    // consumer's bookkeeping sync with an otherwise green suite.
    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("hello")),
            change(
                "items",
                "r1",
                "updated_at_no_trigger",
                HLC2,
                json!("2026-01-01"),
            ),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    let stored: String = conn
        .query_row(
            "SELECT updated_at_no_trigger FROM items WHERE id = 'r1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored, "2026-01-01");
    assert_eq!(report.applied, 2);
    assert_eq!(report.skipped_no_sync_column, 0);
    assert_eq!(report.skipped_reserved_column, 0);
}
