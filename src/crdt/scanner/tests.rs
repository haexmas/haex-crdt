//! Tests for the scanner module.

use super::*;
use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN, UUID_FUNCTION_NAME,
};
use crate::crdt::hlc::{device_uuid_to_hlc_node, HlcService};
use crate::crdt::trigger::{ensure_crdt_columns_and_triggers, setup_triggers_for_table};
use crate::db::connection_context::ConnectionContext;
use crate::db::core::init::{install_tx_hlc_hooks, register_current_hlc_udf};
use crate::table_names::{TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES};
use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;
use serde_json::json;
use std::collections::HashSet;
use uuid::Uuid;

mod filters;
mod pagination;

// -----------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------

fn setup_bookkeeping(conn: &Connection) {
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
         INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value)
         VALUES ('triggers_enabled', 'system', '1');

         CREATE TABLE haex_deleted_rows (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );
         CREATE TABLE haex_hlc_state (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             timestamp TEXT NOT NULL
         );"
    ))
    .expect("bookkeeping");
}

fn register_udfs(conn: &Connection, hlc: HlcService) {
    let ctx = ConnectionContext::new();
    conn.create_scalar_function(
        UUID_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        |_| Ok(Uuid::new_v4().to_string()),
    )
    .expect("gen_uuid");
    register_current_hlc_udf(conn, hlc, ctx.clone()).expect("current_hlc");
    install_tx_hlc_hooks(conn, ctx).expect("hooks");
}

fn create_crdt_table(conn: &Connection, name: &str, extra_cols: &str) {
    conn.execute(
        &format!(
            "CREATE TABLE {name} (
                 id TEXT PRIMARY KEY NOT NULL{extra_sep}{extra_cols}
             )",
            extra_sep = if extra_cols.is_empty() {
                ""
            } else {
                ",\n                 "
            }
        ),
        [],
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    ensure_crdt_columns_and_triggers(&tx, name).unwrap();
    tx.commit().unwrap();
}

fn insert_row_via_transformer(conn: &Connection, hlc_service: &HlcService, sql: &str) -> String {
    use crate::crdt::transformer::CrdtTransformer;
    use crate::db::core::strip_main_schema_prefix;

    let ts = hlc_service.new_timestamp().unwrap();
    hlc_service.update_with_timestamp(&ts).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    HlcService::persist_timestamp(&tx, &ts).unwrap();
    let mut stmt = crate::db::core::parse_single_statement(sql).unwrap();
    let transformer = CrdtTransformer::new();
    transformer
        .transform_execute_statement(&mut stmt, &ts)
        .unwrap();
    let rewritten = strip_main_schema_prefix(&stmt.to_string());
    tx.execute(&rewritten, []).unwrap();
    tx.commit().unwrap();
    ts.to_string()
}

fn make_fixture() -> (Connection, HlcService, Uuid) {
    let conn = Connection::open_in_memory().unwrap();
    let device_uuid = Uuid::new_v4();
    let hlc = HlcService::new_with_uuid(device_uuid);
    register_udfs(&conn, hlc.clone());
    setup_bookkeeping(&conn);
    (conn, hlc, device_uuid)
}

// -----------------------------------------------------------------------
// scan_dirty_tables
// -----------------------------------------------------------------------

#[test]
fn scan_dirty_tables_returns_empty_when_none_marked() {
    let (conn, _hlc, _dev) = make_fixture();
    assert!(scan_dirty_tables(&conn).unwrap().is_empty());
}

#[test]
fn scan_dirty_tables_returns_tables_the_trigger_marked() {
    let (conn, hlc, _dev) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    create_crdt_table(&conn, "notes", "body TEXT");

    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, body) VALUES ('i1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO notes (id, body) VALUES ('n1', 'a')",
    );

    let dirty = scan_dirty_tables(&conn).unwrap();
    assert!(dirty.contains(&"items".to_string()));
    assert!(dirty.contains(&"notes".to_string()));
}

// -----------------------------------------------------------------------
// scan_table_for_local_changes — happy paths
// -----------------------------------------------------------------------

#[test]
fn scan_returns_empty_for_missing_table() {
    let (conn, _hlc, dev) = make_fixture();
    assert!(scan_table_for_local_changes(
        &conn,
        "no_such",
        None,
        &dev.to_string(),
        ScanFilters::default()
    )
    .unwrap()
    .is_empty());
}

#[test]
fn scan_rejects_table_without_primary_key() {
    let (conn, _hlc, dev) = make_fixture();
    // No PK, but with the row-level HLC column so it looks CRDT-flavoured.
    conn.execute(
        &format!(
            "CREATE TABLE t (a TEXT, {HLC_TIMESTAMP_COLUMN} TEXT, \
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}')"
        ),
        [],
    )
    .unwrap();
    let err =
        scan_table_for_local_changes(&conn, "t", None, &dev.to_string(), ScanFilters::default())
            .unwrap_err();
    assert!(matches!(err, DatabaseError::ExecutionError { .. }));
}

#[test]
fn scan_rejects_table_without_required_crdt_metadata() {
    let (conn, _hlc, dev) = make_fixture();
    conn.execute(
        "CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, body TEXT)",
        [],
    )
    .unwrap();

    let err =
        scan_table_for_local_changes(&conn, "t", None, &dev.to_string(), ScanFilters::default())
            .unwrap_err();
    match err {
        DatabaseError::ExecutionError { reason, table, .. } => {
            assert_eq!(table.as_deref(), Some("t"));
            assert!(reason.contains(HLC_TIMESTAMP_COLUMN));
            assert!(reason.contains(COLUMN_HLCS_COLUMN));
        }
        other => panic!("expected a descriptive metadata error, got {other:?}"),
    }
}

#[test]
fn scan_emits_one_change_per_data_column_after_insert() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT, body TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    // Two data columns: name + body.
    let cols: HashSet<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(cols.len(), 2);
    assert!(cols.contains("name"));
    assert!(cols.contains("body"));
    // Row key is canonical JSON in schema-declaration order.
    for c in &changes {
        assert_eq!(c.row_pks, r#"{"id":"i1"}"#);
        assert_eq!(c.table_name, "items");
        assert_eq!(c.device_id, dev.to_string());
    }
}

#[test]
fn scan_excludes_pks_and_crdt_meta_from_emitted_columns() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    for c in &changes {
        assert_ne!(c.column_name, "id", "PK must not emit");
        assert_ne!(c.column_name, HLC_TIMESTAMP_COLUMN);
        assert_ne!(c.column_name, COLUMN_HLCS_COLUMN);
        assert_ne!(c.column_name, COLUMN_SIGS_COLUMN);
    }
}

#[test]
fn scan_skips_consumer_no_trigger_suffixed_columns() {
    let (conn, hlc, dev) = make_fixture();
    // A consumer bookkeeping column opted out of change tracking by the
    // `_no_trigger` suffix. The installer never tracks it, so it never
    // gets an entry in the per-column HLC map — without the suffix rule
    // the per-column loop falls back to the row-level HLC and ships it on
    // every scan.
    create_crdt_table(&conn, "items", "name TEXT, updated_at_no_trigger TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name, updated_at_no_trigger) VALUES ('i1', 'a', '2026-01-01')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    let cols: Vec<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["name"],
        "only the tracked sibling column may emit; `_no_trigger` columns must not"
    );
}

// -----------------------------------------------------------------------
// after_hlc cursor
// -----------------------------------------------------------------------

#[test]
fn scan_with_cursor_only_emits_columns_newer_than_it() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT, body TEXT");
    let t1 = insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')",
    );
    // Update only the `body` column so its per-column HLC advances past t1.
    let _t2 =
        insert_row_via_transformer(&conn, &hlc, "UPDATE items SET body = 'b2' WHERE id = 'i1'");

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        Some(&t1),
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    // Only the body-column change is newer than t1; the name column's HLC
    // remained at t1 and is filtered out by the strict `>` check.
    let cols: Vec<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(cols, vec!["body"]);
    assert_eq!(changes[0].value, json!("b2"));
}

// -----------------------------------------------------------------------
// Sig pass-through
// -----------------------------------------------------------------------

#[test]
fn sig_is_none_when_column_sigs_are_absent() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );
    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    assert!(changes.iter().all(|c| c.sig.is_none()));
}

#[test]
fn sig_passes_through_as_raw_json_when_present() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );
    // A downstream PostWriteHook would normally write here. Simulate by
    // directly setting a mixed shape: a plain sig for `name`, an
    // arbitrary nested shape for a phantom column to prove the crate
    // does not enforce a schema.
    conn.execute(
        &format!("UPDATE items SET {COLUMN_SIGS_COLUMN} = ?1 WHERE id = 'i1'"),
        [r#"{"name": "sig-bytes-b64", "phantom": {"space_a": "nested"}}"#],
    )
    .unwrap();
    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters::default(),
    )
    .unwrap();
    let for_name = changes.iter().find(|c| c.column_name == "name").unwrap();
    assert_eq!(for_name.sig, Some(json!("sig-bytes-b64")));
}
