//! The `after_hlc` prefilter must order HLC times numerically at any width:
//! a 20-digit time is newer than a 19-digit cursor.

use super::*;

const CURSOR: &str = "9999999999999999999/1";
const NEWER: &str = "10000000000000000000/1";

#[test]
fn a_row_with_a_wider_time_than_the_cursor_is_scanned() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );
    conn.execute(
        &format!(
            "UPDATE items SET {HLC_TIMESTAMP_COLUMN} = ?1, {COLUMN_HLCS_COLUMN} = json_object('name', ?1)"
        ),
        [NEWER],
    )
    .unwrap();

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        Some(CURSOR),
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    assert!(
        changes.iter().any(|c| c.column_name == "name"),
        "the newer 20-digit row must pass the prefilter: {changes:?}"
    );

    let none = scan_table_for_local_changes(
        &conn,
        "items",
        Some(NEWER),
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    assert!(
        none.is_empty(),
        "a row at the cursor is not newer: {none:?}"
    );
}
