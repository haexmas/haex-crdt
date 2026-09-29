//! The update trigger on a row whose column-HLC map is NULL — a row
//! inserted before the HLC existed, so the insert trigger never initialised
//! it. `json_set` on NULL stays NULL, which used to drop every column HLC
//! the row would ever get. Kept in its own file because `tests.rs` is
//! already well past the repo's file-size cap.

use super::*;

#[test]
fn update_records_column_hlc_when_map_was_null() {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);
    // Nullable map, as the CRDT transformer declares it.
    conn.execute_batch(&format!(
        "CREATE TABLE items (
             id TEXT PRIMARY KEY NOT NULL,
             name TEXT,
             body TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT,
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    setup_triggers_for_table(&tx, "items", false).unwrap();
    tx.commit().unwrap();

    conn.execute(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')",
        [],
    )
    .unwrap();
    conn.execute(
        &format!("UPDATE items SET name = 'z', {HLC_TIMESTAMP_COLUMN} = 'hlc-2' WHERE id = 'i1'"),
        [],
    )
    .unwrap();

    let map: Option<String> = conn
        .query_row(
            &format!("SELECT {COLUMN_HLCS_COLUMN} FROM items WHERE id = 'i1'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(map.as_deref(), Some(r#"{"name":"hlc-2"}"#));
}
