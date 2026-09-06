//! Delete-log propagation + resurrection guards.
//!
//! Delete-log rows arrive as ordinary column changes into `haex_deleted_rows`.
//! After the write loop, the engine takes their ids and issues a `DELETE`
//! on the target table — unless the target row is strictly newer (LWW
//! keeps it) or an insert in the same batch would resurrect a shadowed row.

use serde_json::json;

use super::{change, create_crdt_table, make_fixture};
use crate::crdt::apply::apply_remote_changes;
use crate::crdt::columns::DELETED_ROWS_TABLE;
use crate::signature::NoopSignatureProvider;

const HLC1: &str = "0000000000000001/abcdef0000000000000000000000";
const HLC2: &str = "0000000000000002/abcdef0000000000000000000000";
const HLC3: &str = "0000000000000003/abcdef0000000000000000000000";

fn insert_target(conn: &mut rusqlite::Connection, hlc: &crate::crdt::hlc::HlcService) {
    apply_remote_changes(
        conn,
        vec![change("items", "r1", "body", HLC1, json!("original"))],
        hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
}

fn delete_log_change(
    delete_id: &str,
    hlc: &str,
    col: &str,
    val: serde_json::Value,
) -> crate::crdt::scanner::ColumnChange {
    crate::crdt::scanner::ColumnChange {
        table_name: DELETED_ROWS_TABLE.to_string(),
        row_pks: format!(r#"{{"id":"{delete_id}"}}"#),
        column_name: col.to_string(),
        hlc_timestamp: hlc.to_string(),
        value: val,
        device_id: String::new(),
        sig: None,
    }
}

// A haex_deleted_rows entry is one row with several columns: id (PK),
// table_name, row_pks (JSON of the target row's PKs). The scanner emits one
// ColumnChange per data column; the apply engine reassembles them into a
// single row-insert on haex_deleted_rows.
fn delete_log_batch(
    delete_id: &str,
    target_table: &str,
    target_pk_id: &str,
    delete_hlc: &str,
) -> Vec<crate::crdt::scanner::ColumnChange> {
    let target_pks = format!(r#"{{"id":"{target_pk_id}"}}"#);
    vec![
        delete_log_change(delete_id, delete_hlc, "table_name", json!(target_table)),
        delete_log_change(delete_id, delete_hlc, "row_pks", json!(target_pks)),
    ]
}

#[test]
fn delete_log_entry_fans_out_to_target_row() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    insert_target(&mut conn, &hlc);

    apply_remote_changes(
        &mut conn,
        delete_log_batch("del-1", "items", "r1", HLC2),
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Target row must be gone; delete-log entry stays.
    let items: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(items, 0, "delete-log propagation must remove target row");

    let deletes: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE} WHERE id = 'del-1'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(deletes, 1, "delete-log entry must stay for peer relay");
}

#[test]
fn delete_log_does_not_propagate_when_target_row_is_strictly_newer() {
    // Simulate resurrection: after the delete-log entry lands, the row
    // must not vanish if its haex_hlc is strictly newer than the delete.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Row inserted at HLC3.
    apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC3, json!("kept"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Delete at HLC2 arrives — older than the row → propagation must skip.
    let report = apply_remote_changes(
        &mut conn,
        delete_log_batch("del-1", "items", "r1", HLC2),
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.skipped_delete_target_newer, 1);

    let items: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(items, 1, "newer row must survive an older delete");
}

#[test]
fn insert_shadowed_by_prior_delete_is_suppressed_and_counted() {
    // A delete-log entry arrives first (HLC2). Then an older insert for the
    // same row (HLC1). Applying the insert would resurrect the row —
    // apply_row must suppress it via the shadow map.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    // Land the delete-log entry.
    apply_remote_changes(
        &mut conn,
        delete_log_batch("del-1", "items", "r1", HLC2),
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    // Now an older insert for r1.
    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC1, json!("resurrected"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 0, "resurrection insert must be suppressed");
    assert_eq!(report.skipped_shadowed_by_delete, 1);

    let items: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(items, 0, "shadowed insert must not land");
}

#[test]
fn insert_strictly_newer_than_all_deletes_wins_and_lands() {
    // Reverse of the shadow test: insert HLC newer than any delete → insert
    // must land, no suppression.
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");

    apply_remote_changes(
        &mut conn,
        delete_log_batch("del-1", "items", "r1", HLC1),
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    let report = apply_remote_changes(
        &mut conn,
        vec![change("items", "r1", "body", HLC3, json!("legit-repost"))],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped_shadowed_by_delete, 0);

    let body: String = conn
        .query_row("SELECT body FROM items WHERE id = 'r1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(body, "legit-repost");
}

#[test]
fn null_delete_hlc_is_skipped_without_aborting_apply() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    insert_target(&mut conn, &hlc);
    conn.execute(
        &format!(
            "INSERT INTO {DELETED_ROWS_TABLE}
             (id, table_name, row_pks, haex_hlc, haex_column_hlcs)
             VALUES ('del-null', 'items', '{{\"id\":\"r1\"}}', NULL, '{{}}')"
        ),
        [],
    )
    .unwrap();

    // The unknown column leaves the pre-existing delete-log row untouched,
    // while collect_inbound_delete_log_ids still exercises propagation.
    let report = apply_remote_changes(
        &mut conn,
        vec![delete_log_change(
            "del-null",
            HLC2,
            "unknown_column",
            json!("ignored"),
        )],
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();
    assert_eq!(report.skipped_unknown_column, 1);

    let items: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(items, 1);
}

#[test]
fn target_delete_error_rolls_back_the_delete_log_apply() {
    let (mut conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    insert_target(&mut conn, &hlc);
    conn.execute_batch(
        "CREATE TRIGGER reject_item_delete
         BEFORE DELETE ON items
         BEGIN
             SELECT RAISE(ABORT, 'delete rejected');
         END;",
    )
    .unwrap();

    assert!(apply_remote_changes(
        &mut conn,
        delete_log_batch("del-rollback", "items", "r1", HLC2),
        &hlc,
        &NoopSignatureProvider,
    )
    .is_err());

    let items: i64 = conn
        .query_row("SELECT COUNT(*) FROM items WHERE id = 'r1'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let deletes: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE} WHERE id = 'del-rollback'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        items, 1,
        "failed propagation must not remove the target row"
    );
    assert_eq!(deletes, 0, "failed propagation must roll back the log row");
}

#[test]
fn delete_target_without_crdt_metadata_is_skipped() {
    let (mut conn, hlc, _dev) = make_fixture();
    conn.execute(
        "CREATE TABLE plain_items (id TEXT PRIMARY KEY NOT NULL, body TEXT)",
        [],
    )
    .unwrap();

    apply_remote_changes(
        &mut conn,
        delete_log_batch("del-plain", "plain_items", "r1", HLC2),
        &hlc,
        &NoopSignatureProvider,
    )
    .unwrap();

    let deletes: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE} WHERE id = 'del-plain'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(deletes, 1, "malformed target must not abort the batch");
}
