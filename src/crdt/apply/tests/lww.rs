//! LWW write-loop tests: winner/loser per column, insert vs update, HLC
//! advancement, schema-drift skips.

use serde_json::json;

use super::{change, create_crdt_table, make_fixture};
use crate::crdt::apply::apply_remote_changes;
use crate::crdt::scanner::ColumnChange;
use crate::signature::NoopSignatureProvider;

const HLC1: &str = "0000000000000001/abcdef0000000000000000000000";
const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";
const HLC3: &str = "0000000000000003/abcdef0000000000000000000000";

#[test]
fn insert_creates_a_new_row_with_incoming_hlc() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let changes = vec![change("items", "r1", "body", HLC2, json!("hello"))];
    let report = apply_remote_changes(&mut conn, changes, &hlc, &NoopSignatureProvider).unwrap();

    assert_eq!(report.applied, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "hello");
    let row_hlc: String = conn
        .query_row("SELECT haex_hlc FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(row_hlc, HLC2);
}

#[test]
fn lww_winner_overwrites_older_value_and_loser_is_dropped() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Seed with HLC2.
    apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC2, json!("v2"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Newer HLC3 must win.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC3, json!("v3"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "v3");

    // Older HLC1 must lose and be counted as stale.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("v1"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(report.skipped_stale, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "v3", "older HLC must NOT overwrite newer value");
}

#[test]
fn per_column_lww_lets_older_and_newer_updates_coexist_within_one_row() {
    // Column-level LWW: a single incoming batch touching two different
    // columns of the same row applies each column's write independently.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT, title TEXT");

    apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("body-v2")),
            change("items", "r1", "title", HLC2, json!("title-v2")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Older `body` (loses) + newer `title` (wins) in one batch.
    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC1, json!("body-v1")),
            change("items", "r1", "title", HLC3, json!("title-v3")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped_stale, 1);

    let (body, title): (String, String) = conn
        .query_row("SELECT body, title FROM items WHERE id = 'r1'", [], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .unwrap();
    assert_eq!(body, "body-v2", "older `body` write must have lost");
    assert_eq!(title, "title-v3", "newer `title` write must have won");
}

#[test]
fn unknown_column_is_counted_and_does_not_abort_the_batch() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("v")),
            // `future_field` isn't in the local schema (peer runs a newer
            // schema version). The good change still lands.
            change("items", "r1", "future_field", HLC2, json!("v")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped_unknown_column, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "v");
}

#[test]
fn unknown_table_counts_every_incoming_column_change_as_skipped() {
    let (mut conn, hlc, _dev) = make_fixture();
    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("not_installed", "r1", "a", HLC2, json!("x")),
            change("not_installed", "r1", "b", HLC2, json!("y")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(report.skipped_unknown_table, 2);
}

#[test]
fn hlc_service_advances_past_the_highest_received_timestamp() {
    // After applying, the local HLC must be at least the highest incoming
    // timestamp. Without this, a following local write could get a stale HLC
    // and lose the next sync round.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC3, json!("v"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    let next = hlc.new_timestamp().unwrap().to_string();
    // Lexicographic compare on same-length HLCs equals HLC compare.
    assert!(
        next.as_str() > HLC3,
        "local HLC {next} must be past incoming {HLC3}"
    );
}

#[test]
fn incoming_write_does_not_regress_row_hlc_when_older_than_stored() {
    // A late-arriving older column write must not lower `haex_hlc`, because
    // that would let an older delete shadow a newer local write on the next
    // apply pass (delete-resurrection contract).
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT, title TEXT");

    apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC3, json!("body-3")),
            change("items", "r1", "title", HLC1, json!("title-1")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Row HLC must equal max(HLC3, HLC1) = HLC3.
    let row_hlc: String = conn
        .query_row("SELECT haex_hlc FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(row_hlc, HLC3);
}

#[test]
fn newer_duplicate_column_change_replaces_older_staged_value() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    let report = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", HLC2, json!("old")),
            change("items", "r1", "body", HLC3, json!("new")),
        ],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    assert_eq!(report.applied, 1);
    let (body, column_hlcs): (String, String) = conn
        .query_row(
            "SELECT body, haex_column_hlcs FROM items WHERE id = 'r1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(body, "new");
    assert_eq!(
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&column_hlcs)
            .unwrap()
            .get("body")
            .and_then(serde_json::Value::as_str),
        Some(HLC3)
    );
}

#[test]
fn nullable_column_hlcs_are_treated_as_empty_on_update() {
    let (mut conn, hlc, _dev) = make_fixture();
    conn.execute(
        "CREATE TABLE items (
            id TEXT PRIMARY KEY NOT NULL,
            body TEXT,
            haex_hlc TEXT,
            haex_column_hlcs TEXT,
            haex_column_sigs TEXT NOT NULL DEFAULT '{}'
        )",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO items (id, body, haex_hlc, haex_column_hlcs) VALUES (?1, ?2, ?3, NULL)",
        rusqlite::params!["r1", "old", HLC1],
    )
    .unwrap();

    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC2, json!("new"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    assert_eq!(report.applied, 1);
    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "new");
}

#[test]
fn composite_primary_keys_are_validated_and_bound_by_name() {
    let (mut conn, hlc, _dev) = make_fixture();
    conn.execute(
        "CREATE TABLE items (
            left_key TEXT NOT NULL,
            right_key INTEGER NOT NULL,
            body TEXT,
            haex_hlc TEXT,
            haex_column_hlcs TEXT NOT NULL DEFAULT '{}',
            haex_column_sigs TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY (left_key, right_key)
        )",
        [],
    )
    .unwrap();

    let valid = ColumnChange {
        table_name: "items".to_string(),
        row_pks: r#"{"right_key":7,"left_key":"left"}"#.to_string(),
        column_name: "body".to_string(),
        hlc_timestamp: HLC2.to_string(),
        value: json!("bound correctly"),
        device_id: String::new(),
        sig: None,
    };
    let report =
        apply_remote_changes(&mut conn, vec![valid], &hlc, &NoopSignatureProvider).unwrap();
    assert_eq!(report.applied, 1);

    let body: String = conn
        .query_row(
            "SELECT body FROM items WHERE left_key = 'left' AND right_key = 7",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(body, "bound correctly");

    let incomplete = ColumnChange {
        row_pks: r#"{"left_key":"left"}"#.to_string(),
        hlc_timestamp: HLC3.to_string(),
        value: json!("must be ignored"),
        ..change("items", "unused", "body", HLC3, json!("unused"))
    };
    let extra = ColumnChange {
        row_pks: r#"{"left_key":"left","right_key":7,"extra":"x"}"#.to_string(),
        ..incomplete.clone()
    };
    let report = apply_remote_changes(
        &mut conn,
        vec![incomplete, extra],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(report.skipped_unknown_table, 2);
}
