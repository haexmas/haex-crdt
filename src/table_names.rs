//! Reserved names for CRDT bookkeeping tables and SQLite user-defined
//! functions. `haex-vault` currently generates these from a build script;
//! this crate hard-codes them, so no build step is required.
//!
//! Every table declared here carries the `_no_sync` suffix. Per the v0.1.1
//! integration decision D-1, that suffix is the sole rule the CRDT layer
//! uses to exclude a table from sync — the crate's own bookkeeping follows
//! the same convention user tables must follow.
//!
//! Additional table names land here as their owning modules are ported.

/// Storage for CRDT-scoped configuration rows (HLC timestamps, etc.).
pub const TABLE_CRDT_CONFIGS: &str = "haex_crdt_configs_no_sync";

/// Set of tables that have unpushed local CRDT changes.
pub const TABLE_CRDT_DIRTY_TABLES: &str = "haex_crdt_dirty_tables_no_sync";

/// Journal of applied crate-owned CRDT bookkeeping migrations (see plan §4.3).
/// Compiled-in list lives in `db::migrations::bootstrap::CRATE_MIGRATIONS`;
/// reconciled on every open against that list and never against the consumer's
/// `MigrationSource`.
pub const TABLE_CRDT_MIGRATIONS: &str = "haex_crdt_migrations_no_sync";

/// Journal of applied consumer-owned schema migrations (see plan §4.3).
/// Reconciled on every open against the consumer's [`crate::MigrationSource`]
/// and never against the crate's compiled-in list.
pub const TABLE_APP_MIGRATIONS: &str = "haex_app_migrations_no_sync";
