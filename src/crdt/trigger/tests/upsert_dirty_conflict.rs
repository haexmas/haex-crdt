//! Regression for issue #28: `INSERT ... ON CONFLICT DO UPDATE` on a
//! CRDT-tracked table with exactly one tracked column must not fail on the
//! second call with a UNIQUE-constraint violation on
//! `haex_crdt_dirty_tables_no_sync.table_name`.

use super::*;

/// Fixture with a single tracked column (`value`) — the shape that
/// reproduces the bug. Multi-column fixtures elsewhere in these tests do not
/// reproduce it, so the shape itself is load-bearing.
fn setup_single_tracked_column() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);
    conn.execute_batch(&format!(
        "CREATE TABLE prefs (
             pk TEXT PRIMARY KEY NOT NULL,
             value TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("create prefs table");
    let tx = conn.unchecked_transaction().unwrap();
    setup_triggers_for_table(&tx, "prefs", false).unwrap();
    tx.commit().unwrap();
    conn
}

/// Repeated upserts refresh the dirty-table timestamp without duplicating its row.
#[test]
fn upsert_twice_on_single_tracked_column_table_succeeds() {
    let conn = setup_single_tracked_column();

    conn.execute(
        &format!(
            "INSERT INTO prefs (pk, value, {HLC_TIMESTAMP_COLUMN}) \
             VALUES ('a', 'v1', 'hlc-1') \
             ON CONFLICT (pk) DO UPDATE SET \
                 value = excluded.value, \
                 {HLC_TIMESTAMP_COLUMN} = excluded.{HLC_TIMESTAMP_COLUMN}"
        ),
        [],
    )
    .expect("first upsert should succeed");

    conn.execute(
        &format!(
            "UPDATE {TABLE_CRDT_DIRTY_TABLES} SET last_modified = 'sentinel' \
             WHERE table_name = 'prefs'"
        ),
        [],
    )
    .expect("set dirty-table timestamp sentinel");

    conn.execute(
        &format!(
            "INSERT INTO prefs (pk, value, {HLC_TIMESTAMP_COLUMN}) \
             VALUES ('a', 'v2', 'hlc-2') \
             ON CONFLICT (pk) DO UPDATE SET \
                 value = excluded.value, \
                 {HLC_TIMESTAMP_COLUMN} = excluded.{HLC_TIMESTAMP_COLUMN}"
        ),
        [],
    )
    .expect("second upsert must not fail with UNIQUE constraint on dirty_tables");

    let last_modified: String = conn
        .query_row(
            &format!(
                "SELECT last_modified FROM {TABLE_CRDT_DIRTY_TABLES} \
                 WHERE table_name = 'prefs'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_ne!(
        last_modified, "sentinel",
        "repeat upsert refreshes the dirty-table timestamp"
    );

    let dirty: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'prefs'"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        dirty, 1,
        "table stays marked dirty exactly once across repeat upserts"
    );
}
