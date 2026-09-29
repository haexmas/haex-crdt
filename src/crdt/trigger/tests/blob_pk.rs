//! The BEFORE-DELETE trigger's `row_pks` for BLOB keys. SQLite's
//! `json_object` refuses a BLOB argument outright, so without the tagged
//! spelling a row with a BLOB key could not be deleted at all. Kept in its
//! own file because `tests.rs` is already well past the repo's file-size
//! cap.

use super::*;

/// Composite key of a BLOB and a TEXT column, so one delete pins both the
/// tagged and the plain spelling in schema order.
fn setup_blob_pk_table() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);
    conn.execute_batch(&format!(
        "CREATE TABLE keyed (
             k BLOB NOT NULL,
             label TEXT NOT NULL,
             body TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             PRIMARY KEY (k, label)
         );"
    ))
    .expect("create keyed table");
    let tx = conn.unchecked_transaction().unwrap();
    setup_triggers_for_table(&tx, "keyed", false).unwrap();
    tx.commit().unwrap();
    conn
}

fn delete_log_row_pks(conn: &Connection) -> String {
    conn.query_row(
        &format!("SELECT row_pks FROM {DELETED_ROWS_TABLE}"),
        [],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn delete_of_blob_keyed_row_logs_tagged_hex_key() {
    let conn = setup_blob_pk_table();
    conn.execute(
        &format!(
            "INSERT INTO keyed (k, label, body, {HLC_TIMESTAMP_COLUMN}) \
             VALUES (x'DEADBEEF', 'a', 'b', 'hlc-1')"
        ),
        [],
    )
    .unwrap();

    conn.execute("DELETE FROM keyed WHERE k = x'DEADBEEF'", [])
        .expect("a BLOB key must not make the delete trigger fail");

    assert_eq!(
        delete_log_row_pks(&conn),
        r#"{"k":{"$blob_hex":"deadbeef"},"label":"a"}"#
    );
}

#[test]
fn delete_of_text_key_spelling_the_tag_logs_a_plain_string() {
    // Only `typeof = 'blob'` is tagged: a TEXT key that reads like the tag
    // stays a JSON string, so it cannot be mistaken for a BLOB later.
    let conn = setup_blob_pk_table();
    conn.execute(
        &format!(
            "INSERT INTO keyed (k, label, {HLC_TIMESTAMP_COLUMN}) \
             VALUES ('{{\"$blob_hex\":\"00\"}}', 'a', 'hlc-1')"
        ),
        [],
    )
    .unwrap();

    conn.execute("DELETE FROM keyed", []).unwrap();

    assert_eq!(
        delete_log_row_pks(&conn),
        r#"{"k":"{\"$blob_hex\":\"00\"}","label":"a"}"#
    );
}
