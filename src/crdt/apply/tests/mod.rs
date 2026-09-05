//! Shared fixture + submodules for the apply-pipeline tests.
//!
//! Tests are split by concern so each file stays legible and under the
//! 500-LoC cap: `lww` covers the write loop, `sig` covers the preflight
//! contract, `delete` covers the delete-log fan-out + shadowing.

mod delete;
mod lww;
mod sig;

use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;
use uuid::Uuid;

use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN,
    UUID_FUNCTION_NAME,
};
use crate::crdt::hlc::HlcService;
use crate::crdt::scanner::ColumnChange;
use crate::db::connection_context::ConnectionContext;
use crate::db::core::{install_tx_hlc_hooks, register_current_hlc_udf};
use crate::table_names::{TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES};

pub(super) fn make_fixture() -> (Connection, HlcService, Uuid) {
    let dev = Uuid::new_v4();
    let hlc = HlcService::new_with_uuid(dev);
    let conn = Connection::open_in_memory().unwrap();
    setup_bookkeeping(&conn);
    register_udfs(&conn, hlc.clone());
    (conn, hlc, dev)
}

pub(super) fn setup_bookkeeping(conn: &Connection) {
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
         CREATE TABLE {DELETED_ROWS_TABLE} (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .expect("bookkeeping");
}

pub(super) fn register_udfs(conn: &Connection, hlc: HlcService) {
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

pub(super) fn create_crdt_table(conn: &Connection, name: &str, extra_cols: &str) {
    let sep = if extra_cols.is_empty() { "" } else { "," };
    conn.execute_batch(&format!(
        "CREATE TABLE {name} (
             id TEXT PRIMARY KEY NOT NULL{sep} {extra_cols}
             , {HLC_TIMESTAMP_COLUMN} TEXT
             , {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
             , {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

pub(super) fn change(
    table: &str,
    pk_id: &str,
    col: &str,
    hlc: &str,
    value: serde_json::Value,
) -> ColumnChange {
    ColumnChange {
        table_name: table.to_string(),
        row_pks: format!(r#"{{"id":"{pk_id}"}}"#),
        column_name: col.to_string(),
        hlc_timestamp: hlc.to_string(),
        value,
        device_id: String::new(),
        sig: None,
    }
}
