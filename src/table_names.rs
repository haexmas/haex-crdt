//! Reserved names for CRDT bookkeeping tables and SQLite user-defined
//! functions. `haex-vault` currently generates these from a build script;
//! this crate hard-codes them, so no build step is required.
//!
//! Additional table names land here as their owning modules are ported.

/// Storage for CRDT-scoped configuration rows (HLC timestamps, etc.).
pub const TABLE_CRDT_CONFIGS: &str = "haex_crdt_configs";

/// Set of tables that have unpushed local CRDT changes.
pub const TABLE_CRDT_DIRTY_TABLES: &str = "haex_crdt_dirty_tables";
