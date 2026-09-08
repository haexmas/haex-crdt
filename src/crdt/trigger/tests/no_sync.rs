//! The column-level `_no_sync` rule: a column that never ships must not be
//! tracked either. Kept in its own file because `tests.rs` is already well
//! past the repo's file-size cap.

use super::*;

/// Fixture carrying one tracked column and one of each suffix, so the two
/// rules are pinned against each other rather than one at a time.
fn setup_both_suffixes() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);
    conn.execute_batch(&format!(
        "CREATE TABLE items (
             id TEXT PRIMARY KEY NOT NULL,
             value TEXT,
             local_meta_no_trigger TEXT,
             last_pull_cursor_no_sync TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("create items table");
    let tx = conn.unchecked_transaction().unwrap();
    setup_triggers_for_table(&tx, "items", false).unwrap();
    tx.commit().unwrap();
    conn
}

#[test]
fn no_sync_suffixed_column_is_not_tracked() {
    let conn = setup_both_suffixes();
    let tracked = tracked_columns_of(&update_trigger_ddl(&conn, "items"));
    // A column that can never ship must not advance the row's CRDT
    // bookkeeping either, so `_no_sync` implies `_no_trigger`'s effect.
    assert_eq!(
        tracked,
        vec!["\"value\"".to_string()],
        "only the plain column may be tracked"
    );
}

#[test]
fn update_of_no_sync_suffixed_column_does_not_mark_dirty() {
    let conn = setup_both_suffixes();
    conn.execute(
        &format!(
            "INSERT INTO items (id, value, last_pull_cursor_no_sync, {HLC_TIMESTAMP_COLUMN})
             VALUES ('r1', 'v', 'cursor-1', 'hlc-1')"
        ),
        [],
    )
    .unwrap();
    // Clear the dirty entry left by the INSERT so the UPDATE assertion is
    // unambiguous.
    conn.execute(
        &format!("DELETE FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"),
        [],
    )
    .unwrap();

    conn.execute(
        &format!(
            "UPDATE items SET last_pull_cursor_no_sync = 'cursor-2', \
             {HLC_TIMESTAMP_COLUMN} = 'hlc-2' WHERE id = 'r1'"
        ),
        [],
    )
    .unwrap();

    let dirty: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        dirty, 0,
        "writing a `_no_sync` column must not queue a sync round for a \
         change that can never travel"
    );
}
