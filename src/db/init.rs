//! Bootstraps CRDT triggers on all discovered CRDT-managed tables.
//!
//! A table is considered CRDT-managed iff it carries the row-level HLC
//! column ([`crate::crdt::columns::HLC_TIMESTAMP_COLUMN`]); see
//! [`discover_crdt_tables`]. This is a security-load-bearing invariant:
//! any table that must stay device-local MUST be created without CRDT
//! metadata columns. The `_no_sync` name suffix is only a convention; the
//! discovery query keys on the column alone.
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

use crate::crdt::columns::HLC_TIMESTAMP_COLUMN;
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
/// carry the row-level HLC column. The `_no_sync` naming convention is
/// enforced by callers omitting CRDT columns on those tables at
/// CREATE-TABLE time; the query itself does not filter by name.
pub fn discover_crdt_tables(conn: &Connection) -> Result<Vec<String>, DatabaseError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT m.name as table_name
         FROM sqlite_master m
         JOIN pragma_table_info(m.name) p
         WHERE m.type = 'table'
           AND p.name = '{HLC_TIMESTAMP_COLUMN}'
         GROUP BY m.name
         ORDER BY m.name"
    ))?;

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

    let check_sql = format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?");
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
mod tests;
