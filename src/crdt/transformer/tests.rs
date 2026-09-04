//! Tests for the CRDT transformer module.
//!
//! Since the delete-log refactor, the transformer no longer injects
//! `haex_tombstone` columns or tombstone filters. These tests verify the
//! remaining responsibilities:
//! - CREATE TABLE gets `haex_hlc` + `haex_column_hlcs` added
//! - CREATE UNIQUE INDEX stays untouched (no partial rewrite)
//! - DELETE stays a DELETE
//! - UPDATE gets the HLC timestamp assignment
//! - SELECT passes through, including recursion into subqueries

use crate::crdt::transformer::CrdtTransformer;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use uhlc::HLC;

fn parse_and_transform_execute(sql: &str) -> String {
    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).unwrap();
    let transformer = CrdtTransformer::new();
    let hlc = HLC::default();
    let timestamp = hlc.new_timestamp();

    transformer
        .transform_execute_statement(&mut statements[0], &timestamp)
        .unwrap();

    statements[0].to_string()
}

#[test]
fn test_select_no_longer_adds_tombstone_filter() {
    let result = parse_and_transform_execute("SELECT * FROM items");
    assert!(!result.contains("haex_tombstone"), "Got: {result}");
}

#[test]
fn test_delete_stays_delete() {
    let result = parse_and_transform_execute("DELETE FROM items WHERE id = 'x'");
    assert!(
        result.to_uppercase().starts_with("DELETE"),
        "DELETE must not be rewritten into UPDATE anymore. Got: {result}"
    );
    assert!(!result.contains("haex_tombstone"), "Got: {result}");
}

#[test]
fn test_update_adds_hlc_assignment() {
    let result = parse_and_transform_execute("UPDATE items SET name = 'foo' WHERE id = 'x'");
    assert!(
        result.contains("haex_hlc"),
        "UPDATE must add haex_hlc assignment. Got: {result}"
    );
    assert!(!result.contains("haex_tombstone"), "Got: {result}");
}

#[test]
fn test_create_table_adds_crdt_columns() {
    let result = parse_and_transform_execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)");
    assert!(result.contains("haex_hlc"), "Got: {result}");
    assert!(result.contains("haex_column_hlcs"), "Got: {result}");
    assert!(
        !result.contains("haex_tombstone"),
        "haex_tombstone must not be added anymore. Got: {result}"
    );
}

#[test]
fn transform_ddl_injects_haex_column_sigs_on_space_scoped_tables() {
    // Phase 1 of the shared-space authenticity design: every CRDT-sync
    // (space-scoped) table must carry a per-column signature map so that
    // shared-space receivers can verify authorship before applying an
    // incoming column change. Parallel to `haex_column_hlcs`, but with an
    // explicit `NOT NULL DEFAULT '{}'` so legacy INSERT statements (and
    // pre-existing rows after ALTER TABLE) still get a valid empty map.
    let transformer = CrdtTransformer::new();
    let out = transformer
        .transform_ddl_statement("CREATE TABLE foo (id TEXT PRIMARY KEY, val TEXT)")
        .expect("transform_ddl_statement must not error on well-formed CREATE TABLE");

    assert!(
        out.contains("haex_column_sigs"),
        "space-scoped CREATE TABLE must gain `haex_column_sigs`. Got: {out}"
    );
    assert!(
        out.contains("DEFAULT '{}'"),
        "`haex_column_sigs` must default to an empty JSON object. Got: {out}"
    );
    // Sig column must not accidentally displace the HLC columns.
    assert!(out.contains("haex_hlc"), "Got: {out}");
    assert!(out.contains("haex_column_hlcs"), "Got: {out}");
}

#[test]
fn transform_ddl_skips_haex_column_sigs_for_no_sync_tables() {
    // `_no_sync` tables are excluded from CRDT sync entirely — no HLC
    // columns, no sig column. Regression guard mirrored on
    // `haex_logs_no_sync_is_not_a_crdt_sync_table`.
    let transformer = CrdtTransformer::new();
    let out = transformer
        .transform_ddl_statement("CREATE TABLE my_cache_no_sync (id TEXT PRIMARY KEY, value TEXT)")
        .expect("transform_ddl_statement must not error on well-formed CREATE TABLE");

    assert!(
        !out.contains("haex_column_sigs"),
        "_no_sync tables must not gain `haex_column_sigs`. Got: {out}"
    );
}

#[test]
fn test_create_unique_index_is_not_rewritten_to_partial() {
    let result = parse_and_transform_execute("CREATE UNIQUE INDEX idx_items_name ON items(name)");
    assert!(
        !result.to_uppercase().contains("WHERE"),
        "UNIQUE index must stay full (no partial rewrite). Got: {result}"
    );
    assert!(!result.contains("haex_tombstone"), "Got: {result}");
}

#[test]
fn test_create_table_no_sync_skipped() {
    let result = parse_and_transform_execute(
        "CREATE TABLE my_cache_no_sync (id TEXT PRIMARY KEY, value TEXT)",
    );
    assert!(
        !result.contains("haex_hlc"),
        "_no_sync tables must not get CRDT columns. Got: {result}"
    );
}

#[test]
fn haex_logs_no_sync_is_not_a_crdt_sync_table() {
    // Regression guard for docs/plans/2026-07-21-haex-logs-no-sync.md.
    // If this table were CRDT-synced, every log row would be pushed to the
    // owner's other devices, which would log the receipt and push it back —
    // the amplification loop that motivated the rename. discover_crdt_tables
    // selects tables by presence of the `haex_hlc` column, so keeping that
    // column absent is the load-bearing invariant.
    let sql = "CREATE TABLE `haex_logs_no_sync` (\
        `id` text PRIMARY KEY NOT NULL,\
        `timestamp` text NOT NULL,\
        `level` text NOT NULL,\
        `source` text NOT NULL,\
        `extension_id` text,\
        `message` text NOT NULL,\
        `metadata` text,\
        `device_id` text NOT NULL,\
        FOREIGN KEY (`extension_id`) REFERENCES `haex_extensions`(`id`) ON UPDATE no action ON DELETE cascade\
    )";
    let transformer = CrdtTransformer::new();
    let result = transformer
        .transform_ddl_statement(sql)
        .expect("transform_ddl_statement must not error on well-formed CREATE TABLE");
    assert!(
        !result.contains("haex_hlc"),
        "haex_logs_no_sync must not gain `haex_hlc` — that column is what discover_crdt_tables keys on. Got: {result}"
    );
    assert!(
        !result.contains("haex_column_hlcs"),
        "haex_logs_no_sync must not gain `haex_column_hlcs`. Got: {result}"
    );
}

#[test]
fn test_insert_into_sync_table_gets_hlc_column() {
    let result = parse_and_transform_execute("INSERT INTO items (id, name) VALUES ('a', 'b')");
    // InsertTransformer adds haex_hlc as a literal column/value
    assert!(result.contains("haex_hlc"), "Got: {result}");
}

#[test]
fn test_delete_from_sync_table_stays_delete() {
    let result = parse_and_transform_execute("DELETE FROM items WHERE id = 'a'");
    assert!(result.to_uppercase().contains("DELETE"));
    assert!(
        !result.to_uppercase().contains("UPDATE"),
        "DELETE must not be rewritten. Got: {result}"
    );
}
