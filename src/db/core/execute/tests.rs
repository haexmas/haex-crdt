//! Tests for `execute`, `execute_with_crdt`, the size guard, the touched-
//! column extractor, the meta-column write guard, and the PostWriteSigner
//! integration.

use super::*;
use crate::crdt::hlc::HlcService;
use crate::crdt::trigger::{ensure_crdt_columns_and_triggers, setup_triggers_for_table};
use crate::db::connection_context::ConnectionContext;
use crate::db::core::init::{install_tx_hlc_hooks, register_current_hlc_udf};
use crate::db::execute_hook::{NoopPostWriteSigner, PostWriteSigner, WriteContext};
use crate::db::init::ensure_triggers_initialized;
use crate::db::DbConnection;
use crate::table_names::{TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES};
use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;
use serde_json::json;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// -------------------------------------------------------------------------
// write_payload_too_large
// -------------------------------------------------------------------------

#[test]
fn oversized_payload_is_flagged() {
    let big = "x".repeat(1_000);
    let params = vec![json!(big)];
    let limit = 100;

    match write_payload_too_large(&params, limit) {
        Some(bytes) => assert!(bytes > limit, "{bytes} should exceed {limit}"),
        None => panic!("oversized payload should be flagged"),
    }
}

#[test]
fn under_limit_payload_passes() {
    let params = vec![json!("small"), json!(42)];
    assert_eq!(write_payload_too_large(&params, 10_000), None);
}

#[test]
fn multi_row_insert_params_sum_over_limit() {
    let chunk = "y".repeat(50);
    let params: Vec<serde_json::Value> = (0..10).map(|_| json!(chunk)).collect();
    let limit = 200;
    assert!(write_payload_too_large(&params, limit).is_some());
}

#[test]
fn empty_params_pass() {
    let params: Vec<serde_json::Value> = vec![];
    assert_eq!(write_payload_too_large(&params, 100), None);
}

// -------------------------------------------------------------------------
// extract_touched_for_signing
// -------------------------------------------------------------------------

fn parse_stmt(sql: &str) -> Statement {
    parse_single_statement(sql).unwrap()
}

#[test]
fn extract_touched_lowercases_table_and_columns_for_insert() {
    let stmt = parse_stmt("INSERT INTO MyTable (Foo, Bar) VALUES (1, 2)");
    let (table, cols) = extract_touched_for_signing(&stmt).unwrap();
    assert_eq!(table.as_str(), "mytable");
    match cols {
        TouchedColumns::Explicit(names) => {
            assert_eq!(names, vec!["foo".to_string(), "bar".to_string()]);
        }
        TouchedColumns::AllColumns => panic!("expected Explicit"),
    }
}

#[test]
fn extract_touched_marks_columnless_insert_as_all_columns() {
    let stmt = parse_stmt("INSERT INTO items VALUES (1, 'x')");
    let (table, cols) = extract_touched_for_signing(&stmt).unwrap();
    assert_eq!(table.as_str(), "items");
    assert!(cols.is_all_columns());
}

#[test]
fn extract_touched_lowercases_update_target_and_assignment_names() {
    let stmt = parse_stmt("UPDATE Items SET Name = 'x' WHERE id = 1");
    let (table, cols) = extract_touched_for_signing(&stmt).unwrap();
    assert_eq!(table.as_str(), "items");
    assert_eq!(cols.explicit(), &["name".to_string()]);
}

#[test]
fn extract_touched_returns_none_for_select_delete_and_ddl() {
    assert!(extract_touched_for_signing(&parse_stmt("SELECT * FROM t")).is_none());
    assert!(extract_touched_for_signing(&parse_stmt("DELETE FROM t WHERE id = 1")).is_none());
    assert!(extract_touched_for_signing(&parse_stmt("CREATE TABLE t (id INTEGER)")).is_none());
}

// -------------------------------------------------------------------------
// End-to-end fixture: an in-memory DB with UDFs, bookkeeping, one CRDT
// table + triggers, and a wrapping `DbConnection`.
// -------------------------------------------------------------------------

fn register_udfs(conn: &Connection, hlc: HlcService, ctx: ConnectionContext) {
    use crate::crdt::columns::UUID_FUNCTION_NAME;
    conn.create_scalar_function(
        UUID_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        |_| Ok(Uuid::new_v4().to_string()),
    )
    .expect("register gen_uuid");
    register_current_hlc_udf(conn, hlc, ctx.clone()).expect("register current_hlc");
    install_tx_hlc_hooks(conn, ctx).expect("install hooks");
}

fn setup_bookkeeping_tables(conn: &Connection) {
    use crate::crdt::columns::{
        COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN,
    };
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
         );
         CREATE TABLE haex_hlc_state (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             timestamp TEXT NOT NULL
         );"
    ))
    .expect("bookkeeping tables");
}

struct Fixture {
    connection: DbConnection,
    hlc_service: HlcService,
}

fn setup_fixture() -> Fixture {
    let conn = Connection::open_in_memory().expect("open in-memory db");
    let hlc = HlcService::new_with_uuid(Uuid::new_v4());
    let ctx = ConnectionContext::new();
    register_udfs(&conn, hlc.clone(), ctx);
    setup_bookkeeping_tables(&conn);

    // Business table + CRDT triggers.
    conn.execute(
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, name TEXT, body TEXT)",
        [],
    )
    .unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        ensure_crdt_columns_and_triggers(&tx, "items").unwrap();
        tx.commit().unwrap();
    }

    // Same connection wrapped for the executor path.
    let connection = DbConnection(Arc::new(Mutex::new(Some(conn))));
    Fixture {
        connection,
        hlc_service: hlc,
    }
}

// -------------------------------------------------------------------------
// execute (no-CRDT path)
// -------------------------------------------------------------------------

#[test]
fn execute_without_crdt_bypasses_triggers_and_leaves_dirty_tables_empty() {
    let fx = setup_fixture();
    // Seed the config row so triggers can consult it (bookkeeping table exists
    // but is empty by default; the execute path INSERT-OR-UPDATEs it).
    execute(
        "INSERT INTO items (id, name, body, haex_hlc) VALUES ('i1', 'a', 'b', 'seed-hlc')"
            .to_string(),
        vec![],
        &fx.connection,
    )
    .expect("execute must succeed");

    // dirty_tables should be empty because the trigger was suppressed by the
    // execute path flipping triggers_enabled to '0' inside its transaction.
    with_connection(&fx.connection, |conn| {
        let count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES}"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "dirty_tables must stay empty in the no-CRDT path");
        let value: String = conn
            .query_row(
                &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(value, "1", "triggers must be re-enabled before commit");
        Ok(())
    })
    .unwrap();
}

#[test]
fn execute_returning_yields_row_values() {
    let fx = setup_fixture();
    let rows = execute(
        "INSERT INTO items (id, name, body, haex_hlc) VALUES ('i1', 'a', 'b', 'h') RETURNING id, name"
            .to_string(),
        vec![],
        &fx.connection,
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], json!("i1"));
    assert_eq!(rows[0][1], json!("a"));
}

// -------------------------------------------------------------------------
// execute_with_crdt
// -------------------------------------------------------------------------

#[test]
fn execute_with_crdt_populates_haex_hlc_and_marks_table_dirty() {
    let fx = setup_fixture();
    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap();

    with_connection(&fx.connection, |conn| {
        let hlc: Option<String> = conn
            .query_row("SELECT haex_hlc FROM items WHERE id = 'i1'", [], |r| r.get(0))
            .unwrap();
        assert!(hlc.is_some(), "haex_hlc must be populated");

        let dirty: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 1, "items must be marked dirty");
        Ok(())
    })
    .unwrap();
}

#[test]
fn execute_with_crdt_rejects_write_to_haex_hlc_meta_column() {
    let fx = setup_fixture();
    let err = execute_with_crdt(
        "INSERT INTO items (id, name, haex_hlc) VALUES ('i1', 'a', 'attacker-hlc')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap_err();
    match err {
        DatabaseError::CrdtMetaColumnWriteForbidden { column } => {
            assert_eq!(column, "haex_hlc");
        }
        other => panic!("expected CrdtMetaColumnWriteForbidden, got {other:?}"),
    }
}

#[test]
fn execute_with_crdt_rejects_write_to_haex_column_hlcs_meta_column() {
    let fx = setup_fixture();
    let err = execute_with_crdt(
        "UPDATE items SET haex_column_hlcs = '{\"forged\":\"x\"}' WHERE id = 'i1'".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, DatabaseError::CrdtMetaColumnWriteForbidden { .. }));
}

#[test]
fn execute_with_crdt_rejects_oversized_payload_before_any_write() {
    let fx = setup_fixture();
    let huge = "x".repeat(MAX_CRDT_TRANSACTION_BYTES + 10);
    let err = execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES (?, ?, ?)".to_string(),
        vec![json!("i1"), json!("n"), json!(huge)],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap_err();
    assert!(matches!(err, DatabaseError::TransactionTooLarge { .. }));

    // Nothing was written.
    with_connection(&fx.connection, |conn| {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
        Ok(())
    })
    .unwrap();
}

#[test]
fn execute_with_crdt_returns_returning_rows() {
    let fx = setup_fixture();
    let rows = execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b') RETURNING id, name".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], json!("i1"));
    assert_eq!(rows[0][1], json!("a"));
}

#[test]
fn execute_with_crdt_update_touches_only_named_columns() {
    let fx = setup_fixture();
    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap();
    execute_with_crdt(
        "UPDATE items SET body = 'b2' WHERE id = 'i1'".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap();

    // Both columns tracked, but only `body` had its HLC advanced.
    with_connection(&fx.connection, |conn| {
        let hlcs: String = conn
            .query_row(
                "SELECT haex_column_hlcs FROM items WHERE id = 'i1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&hlcs).unwrap();
        assert!(parsed["body"].is_string());
        assert!(parsed["name"].is_string());
        assert_ne!(parsed["body"], parsed["name"], "body HLC advanced past name HLC");
        Ok(())
    })
    .unwrap();
}

// -------------------------------------------------------------------------
// PostWriteSigner integration
// -------------------------------------------------------------------------

type SpyCalls = Arc<Mutex<Vec<(String, Vec<String>)>>>;

struct SpySigner {
    calls: SpyCalls,
}

impl PostWriteSigner for SpySigner {
    fn on_after_write(
        &self,
        _tx: &Transaction,
        ctx: &WriteContext<'_>,
    ) -> Result<(), DatabaseError> {
        if let Some((table, cols)) = &ctx.touched {
            self.calls
                .lock()
                .unwrap()
                .push((table.as_str().to_string(), cols.explicit().to_vec()));
        }
        Ok(())
    }
}

struct FailingSigner;

impl PostWriteSigner for FailingSigner {
    fn on_after_write(
        &self,
        _tx: &Transaction,
        _ctx: &WriteContext<'_>,
    ) -> Result<(), DatabaseError> {
        Err(DatabaseError::StatementError {
            reason: "signer said no".to_string(),
        })
    }
}

/// A signer that writes to the DB from inside its callback, proving that the
/// callback receives a live transaction it can operate on.
struct RowWritingSigner {
    row_id: String,
}

impl PostWriteSigner for RowWritingSigner {
    fn on_after_write(
        &self,
        tx: &Transaction,
        _ctx: &WriteContext<'_>,
    ) -> Result<(), DatabaseError> {
        tx.execute(
            "UPDATE items SET haex_column_sigs = json_set(haex_column_sigs, '$.body', ?1) WHERE id = ?2",
            ["signer-sig-for-body", self.row_id.as_str()],
        )?;
        Ok(())
    }
}

#[test]
fn no_signers_registered_is_a_valid_call() {
    let fx = setup_fixture();
    // Baseline: `execute_with_crdt` used with an empty signer slice must not
    // panic and must still write the row.
    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[],
    )
    .unwrap();
}

#[test]
fn noop_signer_is_invoked_and_returns_ok() {
    let fx = setup_fixture();
    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[Arc::new(NoopPostWriteSigner)],
    )
    .expect("noop signer must not reject the write");
}

#[test]
fn spy_signer_receives_touched_table_and_columns() {
    let fx = setup_fixture();
    let calls = Arc::new(Mutex::new(Vec::<(String, Vec<String>)>::new()));
    let spy: Arc<dyn PostWriteSigner> = Arc::new(SpySigner {
        calls: calls.clone(),
    });

    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[spy],
    )
    .unwrap();

    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].0, "items");
    assert_eq!(recorded[0].1, vec!["id", "name", "body"]);
}

#[test]
fn signers_run_in_registration_order() {
    let fx = setup_fixture();
    let calls = Arc::new(Mutex::new(Vec::<String>::new()));

    struct OrderMarker {
        label: String,
        log: Arc<Mutex<Vec<String>>>,
    }
    impl PostWriteSigner for OrderMarker {
        fn on_after_write(
            &self,
            _tx: &Transaction,
            _ctx: &WriteContext<'_>,
        ) -> Result<(), DatabaseError> {
            self.log.lock().unwrap().push(self.label.clone());
            Ok(())
        }
    }

    let first: Arc<dyn PostWriteSigner> = Arc::new(OrderMarker {
        label: "first".into(),
        log: calls.clone(),
    });
    let second: Arc<dyn PostWriteSigner> = Arc::new(OrderMarker {
        label: "second".into(),
        log: calls.clone(),
    });

    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[first, second],
    )
    .unwrap();
    assert_eq!(calls.lock().unwrap().clone(), vec!["first", "second"]);
}

#[test]
fn signer_error_aborts_transaction_and_rolls_back_write() {
    let fx = setup_fixture();
    let err = execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[Arc::new(FailingSigner)],
    )
    .unwrap_err();
    match err {
        DatabaseError::StatementError { reason } => assert_eq!(reason, "signer said no"),
        other => panic!("unexpected error: {other:?}"),
    }

    with_connection(&fx.connection, |conn| {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "the row must have been rolled back");
        Ok(())
    })
    .unwrap();
}

#[test]
fn signer_can_write_to_the_transaction_it_receives() {
    let fx = setup_fixture();
    // Ensure triggers config is present so execute_with_crdt's transformer/triggers see it.
    with_connection(&fx.connection, |conn| {
        ensure_triggers_initialized(conn, 1).unwrap();
        Ok(())
    })
    .unwrap();

    let signer: Arc<dyn PostWriteSigner> = Arc::new(RowWritingSigner {
        row_id: "i1".to_string(),
    });
    execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[signer],
    )
    .unwrap();

    with_connection(&fx.connection, |conn| {
        let sigs_json: String = conn
            .query_row(
                "SELECT haex_column_sigs FROM items WHERE id = 'i1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&sigs_json).unwrap();
        assert_eq!(parsed["body"], json!("signer-sig-for-body"));
        Ok(())
    })
    .unwrap();
}

#[test]
fn later_signer_is_skipped_when_earlier_signer_errors() {
    let fx = setup_fixture();
    let downstream = Arc::new(Mutex::new(false));
    struct MarkCalled {
        flag: Arc<Mutex<bool>>,
    }
    impl PostWriteSigner for MarkCalled {
        fn on_after_write(
            &self,
            _tx: &Transaction,
            _ctx: &WriteContext<'_>,
        ) -> Result<(), DatabaseError> {
            *self.flag.lock().unwrap() = true;
            Ok(())
        }
    }

    let _ = execute_with_crdt(
        "INSERT INTO items (id, name, body) VALUES ('i1', 'a', 'b')".to_string(),
        vec![],
        &fx.connection,
        &fx.hlc_service,
        &[
            Arc::new(FailingSigner),
            Arc::new(MarkCalled {
                flag: downstream.clone(),
            }),
        ],
    );
    assert!(!*downstream.lock().unwrap(), "downstream signer must not have run");
}

// -------------------------------------------------------------------------
// setup_triggers_for_table via `execute_with_crdt`-driven flow — sanity
// integration that ties trigger setup + write execution together.
// -------------------------------------------------------------------------

#[test]
fn integration_setup_triggers_then_write_populates_dirty_and_column_hlcs() {
    // Different fixture: table without triggers first, then install them,
    // then write via execute_with_crdt.
    let conn = Connection::open_in_memory().unwrap();
    let hlc = HlcService::new_with_uuid(Uuid::new_v4());
    let ctx = ConnectionContext::new();
    register_udfs(&conn, hlc.clone(), ctx);
    setup_bookkeeping_tables(&conn);

    conn.execute("CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)", [])
        .unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        crate::crdt::trigger::ensure_crdt_columns(&tx, "items").unwrap();
        setup_triggers_for_table(&tx, "items", false).unwrap();
        // Seed triggers_enabled = 1.
        tx.execute(
            &format!(
                "INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value) VALUES ('triggers_enabled', 'system', '1')"
            ),
            [],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    let connection = DbConnection(Arc::new(Mutex::new(Some(conn))));

    execute_with_crdt(
        "INSERT INTO items (id, name) VALUES ('i1', 'a')".to_string(),
        vec![],
        &connection,
        &hlc,
        &[],
    )
    .unwrap();

    with_connection(&connection, |c| {
        let dirty: i64 = c
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'items'"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dirty, 1);

        let hlcs: String = c
            .query_row(
                "SELECT haex_column_hlcs FROM items WHERE id = 'i1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&hlcs).unwrap();
        assert!(parsed["name"].is_string(), "name HLC must be present");
        Ok(())
    })
    .unwrap();
}
