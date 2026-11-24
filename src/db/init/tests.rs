use super::*;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;
use rusqlite::functions::FunctionFlags;
use std::sync::atomic::{AtomicU64, Ordering};

fn register_test_udfs(conn: &Connection) {
    use crate::crdt::columns::{HLC_FUNCTION_NAME, UUID_FUNCTION_NAME};
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

fn setup_bookkeeping(conn: &Connection) {
    use crate::crdt::columns::DELETED_ROWS_TABLE;
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
         CREATE TABLE {DELETED_ROWS_TABLE} (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("bookkeeping tables");
}

fn create_synced_table(conn: &Connection, name: &str) {
    conn.execute_batch(&format!(
        "CREATE TABLE {name} (
             id TEXT PRIMARY KEY NOT NULL,
             body TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .unwrap_or_else(|e| panic!("create table {name}: {e}"));
}

fn create_no_sync_table(conn: &Connection, name: &str) {
    conn.execute_batch(&format!(
        "CREATE TABLE {name} (id TEXT PRIMARY KEY NOT NULL, message TEXT);"
    ))
    .unwrap_or_else(|e| panic!("create table {name}: {e}"));
}

// --- discover_crdt_tables ---

#[test]
fn discover_lists_only_tables_carrying_the_row_hlc_column() {
    let conn = Connection::open_in_memory().unwrap();
    create_synced_table(&conn, "items");
    create_no_sync_table(&conn, "haex_logs_no_sync");

    let tables = discover_crdt_tables(&conn).unwrap();
    assert!(tables.contains(&"items".to_string()));
    assert!(!tables.contains(&"haex_logs_no_sync".to_string()));
}

#[test]
fn discover_returns_sorted_names() {
    let conn = Connection::open_in_memory().unwrap();
    create_synced_table(&conn, "beta");
    create_synced_table(&conn, "alpha");
    create_synced_table(&conn, "gamma");

    let tables = discover_crdt_tables(&conn).unwrap();
    assert_eq!(tables, vec!["alpha", "beta", "gamma"]);
}

#[test]
fn discover_returns_empty_when_no_crdt_tables() {
    let conn = Connection::open_in_memory().unwrap();
    create_no_sync_table(&conn, "haex_logs_no_sync");
    assert!(discover_crdt_tables(&conn).unwrap().is_empty());
}

// --- ensure_triggers_initialized ---

#[test]
fn first_time_initialization_installs_triggers_and_stores_version() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    let was_already = ensure_triggers_initialized(&mut conn, 3).unwrap();
    assert!(!was_already, "first-time init returns false");

    let version: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?"),
            params![CONFIG_KEY_TRIGGER_VERSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(version, "3");

    let trigger_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'z_dirty_items_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(trigger_count, 3);

    let enabled: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?"),
            params![CONFIG_KEY_TRIGGERS_ENABLED],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(enabled, "1");
}

#[test]
fn re_run_at_same_version_is_a_noop_and_returns_true() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    assert!(!ensure_triggers_initialized(&mut conn, 3).unwrap());
    // Second call at same version: was_already = true, no rewrite.
    assert!(ensure_triggers_initialized(&mut conn, 3).unwrap());
}

#[test]
fn same_version_initialization_reseeds_enabled_triggers() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    assert!(!ensure_triggers_initialized(&mut conn, 3).unwrap());
    conn.execute(
        &format!("UPDATE {TABLE_CRDT_CONFIGS} SET value = '0' WHERE key = ?"),
        params![CONFIG_KEY_TRIGGERS_ENABLED],
    )
    .unwrap();

    assert!(ensure_triggers_initialized(&mut conn, 3).unwrap());
    let enabled: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?"),
            params![CONFIG_KEY_TRIGGERS_ENABLED],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(enabled, "1");
}

#[test]
fn upgrade_to_higher_version_recreates_triggers_and_bumps_stored_version() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    assert!(!ensure_triggers_initialized(&mut conn, 3).unwrap());
    assert!(!ensure_triggers_initialized(&mut conn, 5).unwrap());

    let version: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?"),
            params![CONFIG_KEY_TRIGGER_VERSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(version, "5");
}

#[test]
fn downgrade_below_stored_version_is_a_noop() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    assert!(!ensure_triggers_initialized(&mut conn, 5).unwrap());
    // Requesting an older version returns Ok(true) without rewriting.
    assert!(ensure_triggers_initialized(&mut conn, 3).unwrap());

    let version: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?"),
            params![CONFIG_KEY_TRIGGER_VERSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(version, "5", "must not downgrade the stored version");
}

// --- ensure_triggers_for_all_tables ---

#[test]
fn incremental_install_creates_missing_triggers_only() {
    let mut conn = Connection::open_in_memory().unwrap();
    register_test_udfs(&conn);
    setup_bookkeeping(&conn);
    create_synced_table(&conn, "items");

    // First pass: bootstrap installs triggers on `items`.
    ensure_triggers_initialized(&mut conn, 1).unwrap();

    // A new CRDT table shows up (e.g. from a later schema migration).
    create_synced_table(&conn, "new_table");

    let created = ensure_triggers_for_all_tables(&mut conn).unwrap();
    assert_eq!(created, 1, "only the new table needed triggers");

    // A second call is a full no-op.
    let created_again = ensure_triggers_for_all_tables(&mut conn).unwrap();
    assert_eq!(created_again, 0);
}
