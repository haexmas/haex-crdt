//! `haex-crdt` — SQLite + SQLCipher storage with column-level LWW CRDT sync.
//!
//! This crate is the extraction target for the CRDT/storage layer currently
//! living inside `haex-vault`. See the extraction plan
//! (`holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md`) for scope,
//! trait boundaries, and the sequence by which modules move here.
//!
//! Public surface is a working skeleton: types and trait shapes are in place;
//! the CRDT implementation itself (triggers, scanner, apply pipeline, HLC
//! service) is ported from `haex-vault` in a follow-up step.
//!
//! # Public dependency version contract
//!
//! The post-write hook API exposes types from `rusqlite`, `sqlparser`, and
//! `uhlc`. These crates are re-exported below so consumers can use the exact
//! versions resolved by `haex-crdt`. Changing those versions may require
//! downstream consumers to migrate their hook implementations.

pub mod crdt;
pub mod database;
pub mod db;
pub mod device_id;
pub mod error;
pub mod migration;
pub mod signature;
pub mod table_names;

/// Re-export used by [`PostWriteHook`] implementations to name transactions.
pub use rusqlite;
/// Re-export used by [`WriteContext`] consumers to inspect transformed SQL.
pub use sqlparser;
/// Re-export used by [`WriteContext`] consumers to inspect transaction HLCs.
pub use uhlc;

pub use device_id::{DeviceIdProvider, StaticDeviceId};
pub use error::{Error, MigrationJournal, Result};
pub use migration::{MigrationName, MigrationSource, StaticMigrationSource};
pub use signature::{AuthorId, NoopSignatureProvider, RemoteChanges, SignatureProvider};

pub use crdt::apply::{
    apply_remote_changes, column_sig_preimage, column_sig_preimage_from_parts,
    delete_shadows_insert, should_propagate_delete, ApplyOutcome, ApplyPolicy, ApplyReport,
    ColumnDecision, ConstraintDecision, IndexedChange, RowDecision, RowInput, RowWrite,
    SignatureApplyPolicy, SignatureWrite, SkipReason, SkippedChange, WrittenColumn,
};
pub use crdt::cleanup::{
    cleanup_deleted_rows, compute_cutoff, get_crdt_stats, with_fk_disabled, CleanupResult,
    CrdtStats, ForeignKeyGuard, RetentionPolicy,
};
pub use crdt::hlc::{
    compare_hlc_strings, device_uuid_to_hlc_node, hlc_is_from_node, hlc_is_newer, hlc_max, hlc_min,
    hlc_node_id_suffix, parse_hlc_node_hex, remote_hlc_drift, HlcError, HlcService,
    MAX_REMOTE_HLC_DRIFT,
};
pub use crdt::scanner::{
    paginate_changes, scan_dirty_tables, scan_table_for_local_changes, ColumnChange, Paginable,
    ScanFilters, PULL_PAGE_BUDGET,
};
pub use crdt::trigger::{
    drop_triggers_for_table, ensure_crdt_columns, ensure_crdt_columns_and_triggers,
    get_table_schema, is_safe_identifier, setup_triggers_for_table, ColumnInfo, CrdtSetupError,
    TriggerSetupResult,
};

pub use db::connection_context::ConnectionContext;
pub use db::core::{
    convert_value_ref_to_json, execute, execute_with_crdt, extract_primary_table_name_from_sql,
    extract_table_names_from_sql, extract_table_names_from_statement, install_tx_hlc_hooks,
    open_and_init_db, parse_single_statement, parse_sql_statements, register_current_hlc_udf,
    select, select_with_crdt, statement_has_returning, strip_main_schema_prefix, with_connection,
    write_payload_too_large, ValueConverter, DRIZZLE_STATEMENT_BREAKPOINT,
    MAX_CRDT_TRANSACTION_BYTES,
};
pub use db::execute_hook::{
    NoopPostWriteHook, PostWriteHook, TouchedColumns, TouchedTable, WriteContext,
};
pub use db::init::{
    discover_crdt_tables, ensure_triggers_for_all_tables, ensure_triggers_initialized,
    CONFIG_KEY_TRIGGERS_ENABLED, CONFIG_KEY_TRIGGER_VERSION,
};
pub use db::lock::{DatabaseLock, DatabaseLockError};
pub use db::migrations::{run_migrations, MigrationReport, CRATE_MIGRATIONS};
pub use db::row::{get_bool, get_string};
pub use db::DbConnection;
pub use table_names::{
    TABLE_APP_MIGRATIONS, TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES, TABLE_CRDT_MIGRATIONS,
};

pub use database::{
    Database, DatabaseConfig, InstallCrdtOptions, SqlCipherKey, DEFAULT_TRIGGER_VERSION,
};
