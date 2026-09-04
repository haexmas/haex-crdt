//! Tests for the CRDT trigger installer.
//!
//! Four tests port directly from haex-vault (column-management on ALTER
//! TABLE); the remaining tests exercise the trimmed trigger surface end-to-end
//! against an in-memory SQLite database with test UDFs registered for
//! `gen_uuid` and `current_hlc`.

use super::*;
use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, Ordering};

fn register_test_udfs(conn: &Connection) {
    static UUID_COUNTER: AtomicU64 = AtomicU64::new(0);
    static HLC_COUNTER: AtomicU64 = AtomicU64::new(0);

    conn.create_scalar_function(
        UUID_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        |_| {
            Ok(format!(
                "test-uuid-{}",
                UUID_COUNTER.fetch_add(1, Ordering::Relaxed)
            ))
        },
    )
    .expect("register gen_uuid");

    conn.create_scalar_function(
        HLC_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        |_| {
            Ok(format!(
                "hlc-{:016}",
                HLC_COUNTER.fetch_add(1, Ordering::Relaxed)
            ))
        },
    )
    .expect("register current_hlc");
}

/// Create the CRDT bookkeeping tables the triggers read (`configs`) and write
/// (`dirty_tables`), plus the delete-event log the BEFORE-DELETE trigger
/// appends to. Shapes mirror the migration in `haex-vault` so a schema
/// divergence shows up immediately.
fn setup_crdt_bookkeeping(conn: &Connection) {
    conn.execute_batch(&format!(
        "CREATE TABLE {TABLE_CRDT_CONFIGS} (
             key TEXT PRIMARY KEY NOT NULL,
             value TEXT,
             type TEXT
         );
         CREATE TABLE {TABLE_CRDT_DIRTY_TABLES} (
             table_name TEXT PRIMARY KEY NOT NULL,
             last_modified TEXT
         );
         INSERT INTO {TABLE_CRDT_CONFIGS} (key, value, type)
         VALUES ('triggers_enabled', '1', 'boolean');

         CREATE TABLE {DELETED_ROWS_TABLE} (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("setup CRDT bookkeeping tables");
}

// -------------------------------------------------------------------------
// ensure_crdt_columns — direct port from haex-vault
// -------------------------------------------------------------------------

#[test]
fn ensure_crdt_columns_adds_all_three_when_missing() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)",
        [],
    )
    .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    let result = ensure_crdt_columns(&tx, "test_table").unwrap();
    assert!(result, "should have added columns");
    tx.commit().unwrap();

    let columns = get_table_schema(&conn, "test_table").unwrap();
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&HLC_TIMESTAMP_COLUMN));
    assert!(names.contains(&COLUMN_HLCS_COLUMN));
    assert!(names.contains(&COLUMN_SIGS_COLUMN));
}

#[test]
fn ensure_crdt_columns_is_idempotent_when_columns_present() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE test_table (
                id TEXT PRIMARY KEY,
                name TEXT,
                {HLC_TIMESTAMP_COLUMN} TEXT,
                {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
                {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
            )"
        ),
        [],
    )
    .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    let result = ensure_crdt_columns(&tx, "test_table").unwrap();
    assert!(!result, "should not have added any columns");
    tx.commit().unwrap();
}

#[test]
fn ensure_crdt_columns_backfills_partial_state() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE test_table (
                id TEXT PRIMARY KEY,
                name TEXT,
                {HLC_TIMESTAMP_COLUMN} TEXT
            )"
        ),
        [],
    )
    .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    let result = ensure_crdt_columns(&tx, "test_table").unwrap();
    assert!(result, "should have added missing columns");
    tx.commit().unwrap();

    let names: Vec<String> = get_table_schema(&conn, "test_table")
        .unwrap()
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert!(names.contains(&HLC_TIMESTAMP_COLUMN.to_string()));
    assert!(names.contains(&COLUMN_HLCS_COLUMN.to_string()));
    assert!(names.contains(&COLUMN_SIGS_COLUMN.to_string()));
}

#[test]
fn ensure_crdt_columns_returns_false_on_nonexistent_table() {
    let conn = Connection::open_in_memory().unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let result = ensure_crdt_columns(&tx, "nonexistent_table").unwrap();
    assert!(!result);
}

// -------------------------------------------------------------------------
// End-to-end trigger firing (new coverage; haex-vault leaves this to
// commands_tests and integration tests we do not port here).
// -------------------------------------------------------------------------

fn setup_trigger_fixture() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);

    conn.execute_batch(&format!(
        "CREATE TABLE items (
             id TEXT PRIMARY KEY NOT NULL,
             name TEXT,
             body TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("create items table");

    let tx = conn.unchecked_transaction().unwrap();
    assert!(matches!(
        setup_triggers_for_table(&tx, "items", false).unwrap(),
        TriggerSetupResult::Success
    ));
    tx.commit().unwrap();

    conn
}

#[test]
fn setup_returns_table_not_found_when_table_missing() {
    let conn = Connection::open_in_memory().unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    assert!(matches!(
        setup_triggers_for_table(&tx, "no_such_table", false).unwrap(),
        TriggerSetupResult::TableNotFound
    ));
}

#[test]
fn setup_errors_when_hlc_column_missing() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY)", [])
        .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let err = setup_triggers_for_table(&tx, "items", false).unwrap_err();
    assert!(matches!(err, CrdtSetupError::HlcColumnMissing { .. }));
}

#[test]
fn setup_errors_when_primary_key_missing() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE items (
                id TEXT,
                {HLC_TIMESTAMP_COLUMN} TEXT
            )"
        ),
        [],
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    let err = setup_triggers_for_table(&tx, "items", false).unwrap_err();
    assert!(matches!(err, CrdtSetupError::PrimaryKeyMissing { .. }));
}

#[test]
fn insert_populates_column_hlcs_and_marks_table_dirty() {
    let conn = setup_trigger_fixture();

    conn.execute(
        &format!("INSERT INTO items (id, name, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', 'b', 'hlc-seed')"),
        [],
    )
    .unwrap();

    let (hlcs_json,): (String,) = conn
        .query_row(
            &format!("SELECT {COLUMN_HLCS_COLUMN} FROM items WHERE id = 'i1'"),
            [],
            |r| Ok((r.get(0)?,)),
        )
        .unwrap();
    // The map should carry each tracked (non-PK, non-meta) column keyed to
    // the inserted HLC.
    let parsed: serde_json::Value = serde_json::from_str(&hlcs_json).unwrap();
    assert_eq!(parsed["name"], "hlc-seed");
    assert_eq!(parsed["body"], "hlc-seed");

    let dirty: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 1);
}

#[test]
fn update_advances_only_changed_columns_hlc() {
    let conn = setup_trigger_fixture();

    conn.execute(
        &format!("INSERT INTO items (id, name, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', 'b', 'hlc-1')"),
        [],
    )
    .unwrap();
    // Only `body` changes; `name` stays the same.
    conn.execute(
        &format!("UPDATE items SET body = 'b2', {HLC_TIMESTAMP_COLUMN} = 'hlc-2' WHERE id = 'i1'"),
        [],
    )
    .unwrap();

    let (hlcs_json,): (String,) = conn
        .query_row(
            &format!("SELECT {COLUMN_HLCS_COLUMN} FROM items WHERE id = 'i1'"),
            [],
            |r| Ok((r.get(0)?,)),
        )
        .unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&hlcs_json).unwrap();
    assert_eq!(parsed["name"], "hlc-1", "unchanged column keeps its HLC");
    assert_eq!(parsed["body"], "hlc-2", "changed column advances its HLC");
}

#[test]
fn update_that_touches_only_meta_column_does_not_mark_dirty() {
    let conn = setup_trigger_fixture();
    conn.execute(
        &format!("INSERT INTO items (id, name, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', 'b', 'hlc-1')"),
        [],
    )
    .unwrap();
    // Clear the dirty entry from the INSERT to measure just the UPDATE.
    conn.execute(
        &format!("DELETE FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"),
        [],
    )
    .unwrap();

    // Bump only haex_hlc (a meta column, not in cols_to_track). No tracked
    // column changed, so the dirty entry must not reappear.
    conn.execute(
        &format!("UPDATE items SET {HLC_TIMESTAMP_COLUMN} = 'hlc-2' WHERE id = 'i1'"),
        [],
    )
    .unwrap();

    let dirty: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 0, "meta-only UPDATE must not re-mark items dirty");
}

#[test]
fn delete_records_event_row_and_marks_deleted_rows_dirty() {
    let conn = setup_trigger_fixture();
    conn.execute(
        &format!("INSERT INTO items (id, name, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', 'b', 'hlc-1')"),
        [],
    )
    .unwrap();

    conn.execute("DELETE FROM items WHERE id = 'i1'", [])
        .unwrap();

    let (table_name, row_pks): (String, String) = conn
        .query_row(
            &format!("SELECT table_name, row_pks FROM {DELETED_ROWS_TABLE}"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(table_name, "items");
    assert_eq!(row_pks, r#"{"id":"i1"}"#);

    let dirty: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = ?"
            ),
            [DELETED_ROWS_TABLE],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(dirty, 1);
}

#[test]
fn triggers_disabled_flag_suppresses_all_three_triggers() {
    let conn = setup_trigger_fixture();
    conn.execute(
        &format!("UPDATE {TABLE_CRDT_CONFIGS} SET value = '0' WHERE key = 'triggers_enabled'"),
        [],
    )
    .unwrap();

    // INSERT then UPDATE then DELETE: none should touch dirty_tables or
    // deleted_rows.
    conn.execute(
        &format!("INSERT INTO items (id, name, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', 'b', 'hlc-1')"),
        [],
    )
    .unwrap();
    conn.execute(
        &format!("UPDATE items SET body = 'b2', {HLC_TIMESTAMP_COLUMN} = 'hlc-2' WHERE id = 'i1'"),
        [],
    )
    .unwrap();
    conn.execute("DELETE FROM items WHERE id = 'i1'", [])
        .unwrap();

    let dirty: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(dirty, 0);

    let delete_events: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE}"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(delete_events, 0);
}

#[test]
fn recreate_triggers_replaces_existing_triggers_without_error() {
    let conn = setup_trigger_fixture();
    let tx = conn.unchecked_transaction().unwrap();
    // Calling with recreate=true drops the existing triggers first; a second
    // install must therefore succeed without a "trigger already exists" error.
    assert!(matches!(
        setup_triggers_for_table(&tx, "items", true).unwrap(),
        TriggerSetupResult::Success
    ));
    tx.commit().unwrap();
}

#[test]
fn drop_triggers_removes_all_three_installed_triggers() {
    let conn = setup_trigger_fixture();

    let count_before: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'z_dirty_items_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_before, 3, "expected 3 triggers after setup");

    let tx = conn.unchecked_transaction().unwrap();
    drop_triggers_for_table(&tx, "items").unwrap();
    tx.commit().unwrap();

    let count_after: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'z_dirty_items_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_after, 0);
}

#[test]
fn setup_on_deleted_rows_table_does_not_install_self_referencing_delete_trigger() {
    // haex_deleted_rows is the delete-event log; a DELETE trigger on it would
    // recursively log its own cleanup DELETEs. Guard against regressions.
    let conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);

    let tx = conn.unchecked_transaction().unwrap();
    assert!(matches!(
        setup_triggers_for_table(&tx, DELETED_ROWS_TABLE, false).unwrap(),
        TriggerSetupResult::Success
    ));
    tx.commit().unwrap();

    let delete_trigger_name = format!("z_dirty_{DELETED_ROWS_TABLE}_delete");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            [&delete_trigger_name],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "no DELETE trigger on the delete-event log itself");
}

#[test]
fn ensure_crdt_columns_and_triggers_installs_both_on_bare_table() {
    let conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_crdt_bookkeeping(&conn);
    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)", [])
        .unwrap();

    let tx = conn.unchecked_transaction().unwrap();
    let (cols_added, triggers_created) =
        ensure_crdt_columns_and_triggers(&tx, "items").unwrap();
    tx.commit().unwrap();

    assert!(cols_added);
    assert!(triggers_created);

    let trigger_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'z_dirty_items_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(trigger_count, 3);
}

#[test]
fn is_safe_identifier_accepts_alnum_underscore_hyphen_rejects_empty_and_symbols() {
    assert!(is_safe_identifier("plain"));
    assert!(is_safe_identifier("with_underscore"));
    assert!(is_safe_identifier("with-hyphen"));
    assert!(is_safe_identifier("digits123"));

    assert!(!is_safe_identifier(""));
    assert!(!is_safe_identifier("with space"));
    assert!(!is_safe_identifier("quotes\""));
    assert!(!is_safe_identifier("semi;colon"));
}

#[test]
fn crdt_setup_error_converts_into_database_error_crdt_setup_variant() {
    let err = CrdtSetupError::PrimaryKeyMissing {
        table_name: "t".to_string(),
    };
    let db_err: DatabaseError = err.into();
    assert!(matches!(db_err, DatabaseError::CrdtSetup(_)));
}
