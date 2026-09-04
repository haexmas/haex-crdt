//! Bootstraps CRDT triggers on all discovered CRDT-managed tables.
//!
//! A table is considered CRDT-managed iff it carries the `haex_hlc` column
//! (see [`discover_crdt_tables`]). This is a security-load-bearing
//! invariant: any table that must stay device-local MUST be created without
//! CRDT metadata columns. The `_no_sync` name suffix is only a convention;
//! the discovery query keys on the column alone.
//!
//! [`ensure_triggers_initialized`] is the one-shot bootstrap called on open.
//! It stores the applied trigger version in
//! [`crate::table_names::TABLE_CRDT_CONFIGS`] under the key
//! [`CONFIG_KEY_TRIGGER_VERSION`] and recreates triggers whenever the caller
//! passes a version greater than the stored one. Callers advance the version
//! whenever the trigger-generation code changes shape (see the port history
//! in `crdt::trigger::setup_triggers_for_table`).
//!
//! [`ensure_triggers_for_all_tables`] is the incremental variant, meant for
//! callers who just applied a schema migration and want to install triggers
//! on any new CRDT-managed tables without touching the version bookkeeping.

use crate::crdt::trigger::{setup_triggers_for_table, TriggerSetupResult};
use crate::db::error::DatabaseError;
use crate::table_names::TABLE_CRDT_CONFIGS;
use rusqlite::{params, Connection};

/// Config key storing the applied trigger version in [`TABLE_CRDT_CONFIGS`].
pub const CONFIG_KEY_TRIGGER_VERSION: &str = "trigger_version";

/// Config key storing the `triggers_enabled` gate the triggers themselves
/// consult on every fire; seeded to `'1'` by [`ensure_triggers_initialized`].
pub const CONFIG_KEY_TRIGGERS_ENABLED: &str = "triggers_enabled";

/// Discovers CRDT-managed tables by scanning `sqlite_master` for tables that
/// carry a `haex_hlc` column. The `_no_sync` naming convention is enforced by
/// callers omitting CRDT columns on those tables at CREATE-TABLE time; the
/// query itself does not filter by name.
pub fn discover_crdt_tables(conn: &Connection) -> Result<Vec<String>, DatabaseError> {
    let mut stmt = conn.prepare(
        "SELECT m.name as table_name
         FROM sqlite_master m
         JOIN pragma_table_info(m.name) p
         WHERE m.type = 'table'
           AND p.name = 'haex_hlc'
         GROUP BY m.name
         ORDER BY m.name",
    )?;

    let tables: Result<Vec<String>, _> = stmt.query_map([], |row| row.get(0))?.collect();
    Ok(tables?)
}

/// Ensures CRDT triggers are installed at exactly `trigger_version`.
///
/// Behavior against the version stored in [`TABLE_CRDT_CONFIGS`]:
/// - stored == `trigger_version` → no-op, returns `Ok(true)` (already at version)
/// - stored < `trigger_version` → drops + recreates every table's triggers,
///   writes the new version, returns `Ok(false)`
/// - stored > `trigger_version` → returns `Ok(true)` unchanged (the caller
///   presumably rolled back the crate but the DB has a newer trigger shape;
///   we do not downgrade)
/// - no version stored → first-time install, returns `Ok(false)`
///
/// Seeds `triggers_enabled = '1'` on every call (idempotent).
pub fn ensure_triggers_initialized(
    conn: &mut Connection,
    trigger_version: i32,
) -> Result<bool, DatabaseError> {
    let tx = conn.transaction()?;

    let check_sql =
        format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?");
    let current_version: Option<i32> = tx
        .query_row(&check_sql, params![CONFIG_KEY_TRIGGER_VERSION], |row| {
            let val: String = row.get(0)?;
            Ok(val.parse().unwrap_or(1))
        })
        .ok();

    tx.execute(
        &format!(
            "INSERT OR REPLACE INTO {TABLE_CRDT_CONFIGS} (key, type, value) \
             VALUES (?, 'system', '1')"
        ),
        params![CONFIG_KEY_TRIGGERS_ENABLED],
    )?;

    let needs_update = match current_version {
        Some(v) if v >= trigger_version => {
            tx.commit()?;
            return Ok(true);
        }
        Some(_) => true,
        None => false,
    };

    let crdt_tables = discover_crdt_tables(&tx)?;

    for table_name in crdt_tables {
        setup_triggers_for_table(&tx, &table_name, needs_update)?;
    }

    tx.execute(
        &format!(
            "INSERT OR REPLACE INTO {TABLE_CRDT_CONFIGS} (key, type, value) \
             VALUES (?, 'system', ?)"
        ),
        params![CONFIG_KEY_TRIGGER_VERSION, trigger_version.to_string()],
    )?;

    tx.commit()?;
    Ok(false)
}

/// Idempotently installs triggers on every discovered CRDT-managed table that
/// does not already carry the INSERT trigger. Does not touch the trigger-
/// version bookkeeping. Intended for callers who just applied a schema
/// migration that added new CRDT-managed tables. Returns the number of tables
/// that received a fresh trigger set.
pub fn ensure_triggers_for_all_tables(conn: &mut Connection) -> Result<usize, DatabaseError> {
    let tx = conn.transaction()?;

    let crdt_tables = discover_crdt_tables(&tx)?;
    let mut triggers_created = 0;

    for table_name in &crdt_tables {
        let trigger_name = format!("z_dirty_{table_name}_insert");
        let has_trigger: bool = tx
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'trigger' AND name = ?",
                [&trigger_name],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !has_trigger
            && matches!(
                setup_triggers_for_table(&tx, table_name, false)?,
                TriggerSetupResult::Success
            )
        {
            triggers_created += 1;
        }
    }

    tx.commit()?;
    Ok(triggers_created)
}

#[cfg(test)]
mod tests {
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
    fn discover_lists_only_tables_carrying_haex_hlc() {
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
}
