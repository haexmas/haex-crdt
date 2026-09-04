//! Cross-cutting tests for the `core` module: end-to-end coverage that the
//! `current_hlc()` UDF + commit/rollback/update hooks preserve the
//! transaction-scoped HLC invariant when driven through a real SQLite
//! connection. Per-file behavior is tested in the leaf modules; this file
//! exercises the integration between them.

use super::*;
use crate::crdt::hlc::HlcService;
use crate::db::connection_context::ConnectionContext;
use rusqlite::Connection;
use uuid::Uuid;

fn setup_hlc_connection(_label: &str) -> Connection {
    // `_label` is unused in the current constructor but kept in the signature
    // so each call reads self-documentingly at the callsite.
    let conn = Connection::open_in_memory().expect("in-memory connection");
    let hlc = HlcService::new_with_uuid(Uuid::new_v4());
    let ctx = ConnectionContext::new();
    register_current_hlc_udf(&conn, hlc, ctx.clone()).expect("register current_hlc");
    install_tx_hlc_hooks(&conn, ctx).expect("install tx-hlc hooks");
    conn
}

#[test]
fn current_hlc_differs_across_separate_autocommit_transactions() {
    let conn = setup_hlc_connection("hlc-across-stmts");
    let first: String = conn
        .query_row("SELECT current_hlc()", [], |row| row.get(0))
        .unwrap();
    // Any non-query statement forces the auto-commit transaction to close.
    conn.execute_batch("CREATE TABLE _tick (id INTEGER);")
        .unwrap();
    let second: String = conn
        .query_row("SELECT current_hlc()", [], |row| row.get(0))
        .unwrap();
    assert_ne!(
        first, second,
        "current_hlc() must differ across separate auto-commit transactions"
    );
}

#[test]
fn writes_within_one_explicit_tx_share_one_hlc() {
    // The transaction-scope invariant only applies to writes: multiple
    // INSERT/UPDATE/DELETE statements inside one tx must share one HLC.
    let mut conn = setup_hlc_connection("hlc-explicit-tx-writes");
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, hlc TEXT);")
        .unwrap();
    let tx = conn.transaction().expect("begin tx");
    tx.execute("INSERT INTO t (id, hlc) VALUES (1, current_hlc())", [])
        .unwrap();
    tx.execute("INSERT INTO t (id, hlc) VALUES (2, current_hlc())", [])
        .unwrap();
    tx.commit().unwrap();
    let (a, b): (String, String) = conn
        .query_row(
            "SELECT (SELECT hlc FROM t WHERE id=1), (SELECT hlc FROM t WHERE id=2)",
            [],
            |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())),
        )
        .unwrap();
    assert_eq!(
        a, b,
        "two writes within one explicit transaction must share one HLC"
    );
}

#[test]
fn readonly_probe_does_not_poison_next_write_tx() {
    // Regression guard: a bare `SELECT current_hlc()` outside any write must
    // not dictate the HLC that a later write transaction receives.
    let conn = setup_hlc_connection("hlc-no-poison");
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, hlc TEXT);")
        .unwrap();
    let probed: String = conn
        .query_row("SELECT current_hlc()", [], |row| row.get(0))
        .unwrap();
    conn.execute("INSERT INTO t (id, hlc) VALUES (1, current_hlc())", [])
        .unwrap();
    let persisted: String = conn
        .query_row("SELECT hlc FROM t WHERE id=1", [], |row| row.get(0))
        .unwrap();
    assert_ne!(
        probed, persisted,
        "the probed value must not be reused by the subsequent write"
    );
}

#[test]
fn current_hlc_reset_on_rollback() {
    let mut conn = setup_hlc_connection("hlc-rollback");
    let tx = conn.transaction().expect("begin tx");
    let a: String = tx
        .query_row("SELECT current_hlc()", [], |row| row.get(0))
        .unwrap();
    tx.rollback().unwrap();
    let b: String = conn
        .query_row("SELECT current_hlc()", [], |row| row.get(0))
        .unwrap();
    assert_ne!(a, b, "current_hlc() must be fresh after a rollback");
}

#[test]
fn strip_main_schema_preserves_string_literals() {
    let sql = "SELECT * FROM main.users WHERE notes LIKE '%main.table%'";
    let result = strip_main_schema_prefix(sql);
    assert!(!result.contains("main.users"));
    assert!(result.contains("%main.table%"));
}
