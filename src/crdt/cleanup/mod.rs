//! Delete-log retention and CRDT table statistics.
//!
//! Ported from haex-vault's `src-tauri/src/crdt/cleanup.rs`, trimmed to the
//! CRDT-generic surface. Owner-domain pruning of
//! [`crate::crdt::columns::DELETED_ROWS_TABLE`] is kept; haex-vault's
//! shared-space delete-log and the two anchor-table writes
//! (`haex_space_compaction_anchors`, `haex_vault_settings`) stay in
//! haex-vault — they persist per-space anti-resurrection anchors into
//! haex-vault-owned tables the crate does not know about.
//!
//! Consumers that need an anti-resurrection anchor (see ADR 0002 §6.5)
//! pass a `before_prune` closure to [`cleanup_deleted_rows`]. The closure
//! runs BEFORE the actual DELETE, inside the same transaction, and
//! receives the max HLC of entries about to be pruned. Advancing an
//! anchor after the delete instead would leave a resurrection window on
//! crash-recovery — the crate refuses that shape.

use crate::crdt::columns::DELETED_ROWS_TABLE;
use crate::db::error::DatabaseError;
use crate::table_names::TABLE_CRDT_CONFIGS;
use rusqlite::{Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uhlc::Timestamp;

// -----------------------------------------------------------------------
// Foreign-key pragma helpers (used by cleanup + apply)
// -----------------------------------------------------------------------

/// RAII guard that turns off `PRAGMA foreign_keys` for the duration of a
/// block and restores its previous state when the guard goes out of scope.
///
/// Use this instead of manual `pragma_update(..., "OFF") ... pragma_update(..., "ON")`
/// pairs: if anything between the calls returns early via `?` the manual
/// version leaves FK checks disabled on a shared `Connection`, silently
/// breaking referential integrity for subsequent queries on the same
/// connection.
pub struct ForeignKeyGuard<'a> {
    conn: &'a Connection,
    was_enabled: bool,
}

impl<'a> ForeignKeyGuard<'a> {
    /// Disables foreign-key enforcement until the returned guard is dropped.
    pub fn disable(conn: &'a Connection) -> Result<Self, rusqlite::Error> {
        let was_enabled = foreign_keys_enabled(conn)?;
        conn.execute("PRAGMA foreign_keys = OFF", [])?;
        Ok(Self { conn, was_enabled })
    }
}

impl Drop for ForeignKeyGuard<'_> {
    fn drop(&mut self) {
        let _ = restore_foreign_keys(self.conn, self.was_enabled);
    }
}

/// Runs `f` with `PRAGMA foreign_keys` turned off, restoring its previous
/// state when `f` returns — even on `Err` or panic. Use this
/// instead of manual OFF/ON pairs in code paths that open a transaction:
/// `Connection::transaction` requires `&mut`, which conflicts with the
/// RAII guard's shared borrow.
///
/// Generic over the error type so callers can use their own error enum
/// (e.g. [`DatabaseError`]) as long as it implements `From<rusqlite::Error>`.
///
/// Panic-safety: `f` runs under `catch_unwind`. If it panics, the FK
/// pragma state is restored and the original payload is re-raised — without
/// this, a panic inside `f` would leave the shared connection with FK
/// disabled and later non-CRDT queries would silently skip
/// referential-integrity checks.
pub fn with_fk_disabled<R, E, F>(conn: &mut Connection, f: F) -> Result<R, E>
where
    F: FnOnce(&mut Connection) -> Result<R, E>,
    E: From<rusqlite::Error>,
{
    let was_enabled = foreign_keys_enabled(conn).map_err(E::from)?;
    conn.execute("PRAGMA foreign_keys = OFF", [])
        .map_err(E::from)?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(conn)));
    let _ = restore_foreign_keys(conn, was_enabled);
    match result {
        Ok(r) => r,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

// -----------------------------------------------------------------------
// Retention + result types
// -----------------------------------------------------------------------

/// Which delete-log entries [`cleanup_deleted_rows`] should prune.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Prune entries whose HLC time-part is more than `days` older than the
    /// current HLC stored in `haex_crdt_configs_no_sync` under key `hlc_timestamp`.
    /// If no HLC is recorded yet, the pass is a no-op.
    TimeBasedDays { days: u32 },
    /// Hard-delete every delete-log entry with an anchorable, non-NULL HLC.
    /// Entries without an HLC remain until they can be handled safely.
    All,
}

/// Result of a single [`cleanup_deleted_rows`] call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanupResult {
    /// Number of rows removed from `haex_deleted_rows`.
    pub rows_deleted: usize,
    /// Max HLC of the entries that were pruned. `None` when the pass was a
    /// no-op (no entries matched the policy).
    pub max_pruned_hlc: Option<String>,
}

// -----------------------------------------------------------------------
// Cleanup entry point
// -----------------------------------------------------------------------

/// Prunes old delete-log entries per `policy`.
///
/// Runs inside a single transaction. The optional `before_prune` hook
/// runs BEFORE the DELETE inside the same transaction, receiving the max
/// HLC of entries about to be pruned (`None` when the pass would delete
/// nothing). Use it to advance an anti-resurrection anchor into a table
/// the caller owns — advancing after the delete instead would leave a
/// resurrection window on crash-recovery (see haex-vault's ADR 0002 §6.5).
///
/// Foreign-key enforcement is disabled for the duration and its previous state
/// is restored on exit — `haex_deleted_rows` intentionally has no FK back
/// to the source rows, but callers may add their own FK-heavy tables and
/// the cascade path is safer with FK off.
///
/// Consumers who do not need anti-resurrection anchoring pass
/// `|_, _| Ok(())`.
pub fn cleanup_deleted_rows<F>(
    conn: &mut Connection,
    policy: RetentionPolicy,
    before_prune: F,
) -> Result<CleanupResult, DatabaseError>
where
    F: FnOnce(&Transaction, Option<&str>) -> Result<(), DatabaseError>,
{
    with_fk_disabled(conn, |conn| {
        let tx = conn.transaction()?;

        // The hook may update the stored HLC. Fix the cutoff before invoking it
        // so the reported maximum and the DELETE always cover the same rows.
        let cutoff = compute_cutoff(&tx, policy)?;
        let max_pruned_hlc = read_max_prunable_hlc(&tx, policy, cutoff)?;

        before_prune(&tx, max_pruned_hlc.as_deref())?;

        let rows_deleted = match policy {
            RetentionPolicy::All => {
                let sql =
                    format!("DELETE FROM \"{DELETED_ROWS_TABLE}\" WHERE haex_hlc IS NOT NULL");
                tx.execute(&sql, [])?
            }
            RetentionPolicy::TimeBasedDays { .. } => {
                let Some(cutoff) = cutoff else {
                    tx.commit()?;
                    return Ok(CleanupResult {
                        rows_deleted: 0,
                        max_pruned_hlc: None,
                    });
                };
                let sql = format!(
                    "DELETE FROM \"{DELETED_ROWS_TABLE}\" \
                     WHERE haex_hlc IS NOT NULL \
                       AND CAST(substr(haex_hlc, 1, instr(haex_hlc, '/') - 1) AS INTEGER) < ?1"
                );
                tx.execute(&sql, [cutoff])?
            }
        };

        tx.commit()?;
        Ok(CleanupResult {
            rows_deleted,
            max_pruned_hlc,
        })
    })
}

// -----------------------------------------------------------------------
// Stats
// -----------------------------------------------------------------------

/// Snapshot of the CRDT layer's contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrdtStats {
    /// Live rows across every CRDT-managed table (identified by the
    /// presence of a `haex_hlc` column). Excludes `_no_sync` tables,
    /// SQLite internals, and the delete-log table itself.
    pub live_row_count: i64,
    /// Number of CRDT-managed tables discovered.
    pub crdt_table_count: i64,
    /// Rows currently in `haex_deleted_rows`.
    pub delete_log_row_count: i64,
}

/// Walks `sqlite_master` for CRDT-managed tables and counts live rows plus
/// delete-log entries.
pub fn get_crdt_stats(conn: &Connection) -> Result<CrdtStats, DatabaseError> {
    let mut live_row_count: i64 = 0;
    let mut crdt_table_count: i64 = 0;

    let mut stmt = conn.prepare(
        "SELECT m.name FROM sqlite_master m \
         WHERE m.type = 'table' \
         AND m.name NOT LIKE 'sqlite_%' \
         AND m.name NOT LIKE '%_no_sync' \
         AND m.name != ?1 \
         AND EXISTS (SELECT 1 FROM pragma_table_info(m.name) WHERE name = 'haex_hlc')",
    )?;

    let table_names: Vec<String> = stmt
        .query_map([DELETED_ROWS_TABLE], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?;
    drop(stmt);

    for table_name in table_names {
        crdt_table_count += 1;
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM \"{table_name}\""),
            [],
            |row| row.get(0),
        )?;
        live_row_count += count;
    }

    let delete_log_row_count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM \"{DELETED_ROWS_TABLE}\""),
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    Ok(CrdtStats {
        live_row_count,
        crdt_table_count,
        delete_log_row_count,
    })
}

// -----------------------------------------------------------------------
// Private helpers
// -----------------------------------------------------------------------

/// Reads the max HLC of entries that would be pruned by `policy`, or
/// `None` if the pass would delete nothing.
fn read_max_prunable_hlc(
    tx: &Transaction,
    policy: RetentionPolicy,
    cutoff: Option<i64>,
) -> Result<Option<String>, DatabaseError> {
    match policy {
        RetentionPolicy::All => {
            let hlc: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT haex_hlc FROM \"{DELETED_ROWS_TABLE}\" \
                         WHERE haex_hlc IS NOT NULL \
                         ORDER BY \
                           CAST(substr(haex_hlc, 1, instr(haex_hlc, '/') - 1) AS INTEGER) DESC \
                         LIMIT 1"
                    ),
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(hlc)
        }
        RetentionPolicy::TimeBasedDays { .. } => {
            let Some(cutoff) = cutoff else {
                return Ok(None);
            };
            let hlc: Option<String> = tx
                .query_row(
                    &format!(
                        "SELECT haex_hlc FROM \"{DELETED_ROWS_TABLE}\" \
                         WHERE haex_hlc IS NOT NULL \
                           AND CAST(substr(haex_hlc, 1, instr(haex_hlc, '/') - 1) AS INTEGER) < ?1 \
                         ORDER BY \
                           CAST(substr(haex_hlc, 1, instr(haex_hlc, '/') - 1) AS INTEGER) DESC \
                         LIMIT 1"
                    ),
                    [cutoff],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(hlc)
        }
    }
}

/// For `TimeBasedDays`: computes the cutoff HLC time-part from the
/// current HLC in `haex_crdt_configs_no_sync`. Returns `None` when no HLC has
/// been recorded yet or when the cutoff would overflow `i64` (SQLite
/// stores integers signed 64-bit; an `as i64` cast on `u64 > i64::MAX`
/// would wrap negative and silently skew the comparison).
fn compute_cutoff(tx: &Transaction, policy: RetentionPolicy) -> Result<Option<i64>, DatabaseError> {
    let days = match policy {
        RetentionPolicy::TimeBasedDays { days } => days,
        RetentionPolicy::All => return Ok(None),
    };

    let current_hlc_str: Option<String> = tx
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?1 AND type = 'hlc'"),
            ["hlc_timestamp"],
            |row| row.get(0),
        )
        .optional()?;
    let Some(current_hlc_str) = current_hlc_str else {
        return Ok(None);
    };

    let current_timestamp =
        Timestamp::from_str(&current_hlc_str).map_err(|e| DatabaseError::HlcError {
            reason: format!("cleanup: invalid HLC in config '{current_hlc_str}': {e:?}"),
        })?;

    Ok(compute_cutoff_hlc_num(
        current_timestamp.get_time().as_u64(),
        days,
    ))
}

/// Converts a current HLC time and retention window into SQLite's signed cutoff.
fn compute_cutoff_hlc_num(current_hlc_num: u64, retention_days: u32) -> Option<i64> {
    let ns_per_day: u64 = 24 * 60 * 60 * 1_000_000_000;
    let retention_ns = u64::from(retention_days).saturating_mul(ns_per_day);
    let cutoff = current_hlc_num.saturating_sub(retention_ns);
    i64::try_from(cutoff).ok()
}

/// Returns whether SQLite foreign-key enforcement is currently enabled.
fn foreign_keys_enabled(conn: &Connection) -> Result<bool, rusqlite::Error> {
    conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
        .map(|value| value != 0)
}

/// Restores SQLite foreign-key enforcement to a previously captured state.
fn restore_foreign_keys(conn: &Connection, enabled: bool) -> Result<(), rusqlite::Error> {
    let pragma = if enabled {
        "PRAGMA foreign_keys = ON"
    } else {
        "PRAGMA foreign_keys = OFF"
    };
    conn.execute(pragma, []).map(|_| ())
}

#[cfg(test)]
mod tests;
