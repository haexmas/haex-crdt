//! CRDT trigger installer.
//!
//! Ported from `haex-vault`'s `src-tauri/src/crdt/trigger.rs`, trimmed to the
//! CRDT-generic surface: INSERT/UPDATE/DELETE triggers that populate the
//! per-column HLC map, append delete-events to [`DELETED_ROWS_TABLE`], and
//! mark dirty tables in [`TABLE_CRDT_DIRTY_TABLES`] so the scanner picks them
//! up on the next sync cycle.
//!
//! Business tables carry no soft-delete column. Deletes are hard-deletes on
//! the source row plus one event row in [`DELETED_ROWS_TABLE`] — see the
//! BEFORE-DELETE trigger.
//!
//! Haex-vault-specific concerns (shared-space register cascade, per-space
//! delete-log fanout, MLS-specific skip lists, consumer-schema column
//! conventions like `last_push_hlc_timestamp` / `updated_at`) are **not**
//! ported here — extraction plan §3 keeps sync-transport features in
//! `haex-vault`, and consumer-schema opinions belong to the consumer.

use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_FUNCTION_NAME,
    HLC_TIMESTAMP_COLUMN, UUID_FUNCTION_NAME,
};
use crate::db::error::DatabaseError;
use crate::table_names::{TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES};
use rusqlite::{Connection, Result as RusqliteResult, Row, Transaction};
use serde::Serialize;
use thiserror::Error;

const INSERT_TRIGGER_TPL: &str = "z_dirty_{TABLE_NAME}_insert";
const UPDATE_TRIGGER_TPL: &str = "z_dirty_{TABLE_NAME}_update";
const DELETE_TRIGGER_TPL: &str = "z_dirty_{TABLE_NAME}_delete";

#[derive(Debug, Error)]
/// Errors encountered while inspecting a table or installing its CRDT triggers.
pub enum CrdtSetupError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("table '{table_name}' is missing the required hlc column '{column_name}'")]
    HlcColumnMissing {
        table_name: String,
        column_name: String,
    },

    #[error("table '{table_name}' has no primary key")]
    PrimaryKeyMissing { table_name: String },

    /// A table column name would be unsafe to interpolate into trigger SQL.
    #[error("column '{name}' is not a safe SQL identifier")]
    UnsafeIdentifier { name: String },
}

impl From<CrdtSetupError> for DatabaseError {
    fn from(err: CrdtSetupError) -> Self {
        DatabaseError::CrdtSetup(err.to_string())
    }
}

#[derive(Debug, Serialize)]
/// Outcome of attempting to install CRDT triggers for a table.
pub enum TriggerSetupResult {
    /// The table was found and the requested triggers were installed.
    Success,
    /// No table with the requested name exists.
    TableNotFound,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
/// Schema metadata used to derive CRDT trigger expressions.
pub struct ColumnInfo {
    /// Column name as reported by SQLite.
    pub name: String,
    #[serde(rename = "type")]
    /// Declared SQLite column type.
    pub column_type: String,
    /// Whether SQLite marks this column as part of the primary key.
    pub is_pk: bool,
}

impl ColumnInfo {
    /// Reads a column description from a `PRAGMA table_info` result row.
    pub fn from_row(row: &Row) -> RusqliteResult<Self> {
        Ok(ColumnInfo {
            name: row.get("name")?,
            column_type: row.get("type")?,
            is_pk: row.get::<_, i64>("pk")? > 0,
        })
    }
}

/// Returns whether `name` can safely be interpolated into generated SQL.
pub fn is_safe_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

/// Installs CRDT triggers (INSERT / UPDATE / BEFORE-DELETE) on `table_name`.
///
/// The table must already carry the three CRDT metadata columns (see
/// [`ensure_crdt_columns`]) and have at least one primary-key column.
///
/// # Column skip rule (D-4, revised)
///
/// One suffix, defined in [`crate::crdt::columns`]: `_no_sync` — never
/// shipped, and therefore not tracked either. A column that cannot travel
/// must not advance the row's CRDT bookkeeping, or a write to it would mark
/// the row dirty and queue a sync round for a change that can never leave.
/// Consumers who want a column tracked simply do not name it with the
/// suffix.
///
/// The three structural CRDT metadata columns ([`HLC_TIMESTAMP_COLUMN`],
/// [`COLUMN_HLCS_COLUMN`], [`COLUMN_SIGS_COLUMN`]) carry the suffix too, so
/// the same rule catches them — no hardcoded exemption here, and none in
/// [`crate::crdt::scanner::scan_table_for_local_changes`] either, which
/// applies the identical predicate to decide what ships.
///
/// Primary-key columns are also skipped.
///
/// The BEFORE-DELETE trigger records the delete as an event row in
/// [`DELETED_ROWS_TABLE`] on every hard-delete; that table itself is exempt
/// (a self-referencing DELETE trigger would loop on cleanup).
pub fn setup_triggers_for_table(
    tx: &Transaction,
    table_name: &str,
    recreate: bool,
) -> Result<TriggerSetupResult, CrdtSetupError> {
    let columns = get_table_schema(tx, table_name)?;

    if columns.is_empty() {
        return Ok(TriggerSetupResult::TableNotFound);
    }

    if !columns.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN) {
        return Err(CrdtSetupError::HlcColumnMissing {
            table_name: table_name.to_string(),
            column_name: HLC_TIMESTAMP_COLUMN.to_string(),
        });
    }

    let pks: Vec<String> = columns
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.clone())
        .collect();

    if pks.is_empty() {
        return Err(CrdtSetupError::PrimaryKeyMissing {
            table_name: table_name.to_string(),
        });
    }

    // D-4 (revised): skip PKs and `_no_sync` columns — a column that never
    // ships must not advance the row's bookkeeping either, or writing it
    // would mark the row dirty for a change that can never travel. The three
    // structural CRDT metadata columns carry the suffix, so this catches
    // them without a separate exemption. Same predicate as the scanner's
    // `partition_columns`, deliberately: what fires a trigger and what ships
    // are now one question.
    let cols_to_track: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_pk && !c.name.ends_with("_no_sync"))
        .map(|c| c.name.clone())
        .collect();

    for column_name in pks.iter().chain(cols_to_track.iter()) {
        if !is_safe_identifier(column_name) {
            return Err(CrdtSetupError::UnsafeIdentifier {
                name: column_name.clone(),
            });
        }
    }

    let insert_trigger_sql = generate_insert_trigger_sql(table_name, &cols_to_track, &pks);
    let update_trigger_sql = generate_update_trigger_sql(table_name, &cols_to_track, &pks);

    if recreate {
        drop_triggers_for_table(tx, table_name)?;
    }

    tx.execute_batch(&insert_trigger_sql)?;
    tx.execute_batch(&update_trigger_sql)?;

    if table_name != DELETED_ROWS_TABLE {
        let delete_trigger_sql = generate_delete_trigger_sql(table_name, &pks);
        tx.execute_batch(&delete_trigger_sql)?;
    }

    Ok(TriggerSetupResult::Success)
}

/// Returns SQLite's column metadata for `table_name`.
pub fn get_table_schema(conn: &Connection, table_name: &str) -> RusqliteResult<Vec<ColumnInfo>> {
    if !is_safe_identifier(table_name) {
        return Err(rusqlite::Error::InvalidParameterName(format!(
            "Invalid or unsafe table name provided: {table_name}"
        )));
    }

    let sql = format!("PRAGMA table_info(\"{table_name}\");");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], ColumnInfo::from_row)?;
    rows.collect()
}

/// Drops all CRDT trigger names associated with `table_name` if they exist.
pub fn drop_triggers_for_table(tx: &Transaction, table_name: &str) -> Result<(), CrdtSetupError> {
    if !is_safe_identifier(table_name) {
        return Err(rusqlite::Error::InvalidParameterName(format!(
            "Invalid or unsafe table name provided: {table_name}"
        ))
        .into());
    }

    let drop_insert = drop_trigger_sql(&INSERT_TRIGGER_TPL.replace("{TABLE_NAME}", table_name));
    let drop_update = drop_trigger_sql(&UPDATE_TRIGGER_TPL.replace("{TABLE_NAME}", table_name));
    let drop_delete = drop_trigger_sql(&DELETE_TRIGGER_TPL.replace("{TABLE_NAME}", table_name));

    let sql_batch = format!("{drop_insert}\n{drop_update}\n{drop_delete}");
    tx.execute_batch(&sql_batch)?;
    Ok(())
}

fn generate_insert_trigger_sql(
    table_name: &str,
    cols_to_track: &[String],
    primary_key_columns: &[String],
) -> String {
    let trigger_name = INSERT_TRIGGER_TPL.replace("{TABLE_NAME}", table_name);

    let json_pairs: Vec<String> = cols_to_track
        .iter()
        .map(|col| format!("'{col}', NEW.\"{HLC_TIMESTAMP_COLUMN}\""))
        .collect();
    let json_object = if json_pairs.is_empty() {
        "'{}'".to_string()
    } else {
        format!("json_object({})", json_pairs.join(", "))
    };

    let pk_where = if primary_key_columns.is_empty() {
        "rowid = NEW.rowid".to_string()
    } else {
        primary_key_columns
            .iter()
            .map(|pk| format!("\"{pk}\" = NEW.\"{pk}\""))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    format!(
        "CREATE TRIGGER IF NOT EXISTS \"{trigger_name}\"
            AFTER INSERT ON \"{table_name}\"
            FOR EACH ROW
            WHEN NEW.{HLC_TIMESTAMP_COLUMN} IS NOT NULL
                AND COALESCE((SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'), '1') = '1'
            BEGIN
            UPDATE \"{table_name}\"
            SET {COLUMN_HLCS_COLUMN} = {json_object}
            WHERE {pk_where};

            INSERT OR REPLACE INTO {TABLE_CRDT_DIRTY_TABLES} (table_name, last_modified)
            VALUES ('{table_name}', datetime('now'));
            END;"
    )
}

fn drop_trigger_sql(trigger_name: &str) -> String {
    format!("DROP TRIGGER IF EXISTS \"{trigger_name}\";")
}

fn generate_update_trigger_sql(
    table_name: &str,
    cols_to_track: &[String],
    primary_key_columns: &[String],
) -> String {
    let trigger_name = UPDATE_TRIGGER_TPL.replace("{TABLE_NAME}", table_name);

    let pk_where = if primary_key_columns.is_empty() {
        "rowid = NEW.rowid".to_string()
    } else {
        primary_key_columns
            .iter()
            .map(|pk| format!("\"{pk}\" = NEW.\"{pk}\""))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    let mut update_statements: Vec<String> = Vec::new();
    for col in cols_to_track {
        update_statements.push(format!(
            "UPDATE \"{table_name}\"
            SET {COLUMN_HLCS_COLUMN} = json_set({COLUMN_HLCS_COLUMN}, '$.{col}', NEW.\"{HLC_TIMESTAMP_COLUMN}\")
            WHERE {pk_where} AND NEW.\"{col}\" IS NOT OLD.\"{col}\";"
        ));
    }
    let all_updates = update_statements.join("\n            ");

    let any_tracked_changed = if cols_to_track.is_empty() {
        "0".to_string()
    } else {
        cols_to_track
            .iter()
            .map(|col| format!("NEW.\"{col}\" IS NOT OLD.\"{col}\""))
            .collect::<Vec<_>>()
            .join(" OR ")
    };

    // D-4: constrain the trigger to only fire when at least one *tracked*
    // column is UPDATE'd. `AFTER UPDATE OF <cols>` is SQLite's built-in
    // column-scoped trigger form; an UPDATE that touches only skipped
    // columns (`_no_sync`-suffixed, structural metadata, or PKs) then does
    // not fire the trigger at all.
    //
    // Empty tracked list is degenerate — the trigger body is inert anyway
    // (the SELECT ... WHERE (0) guard) — so fall back to bare `AFTER UPDATE
    // ON` since `UPDATE OF` with no column list is a syntax error.
    let update_of_clause = if cols_to_track.is_empty() {
        format!("AFTER UPDATE ON \"{table_name}\"")
    } else {
        let column_list = cols_to_track
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!("AFTER UPDATE OF {column_list} ON \"{table_name}\"")
    };

    format!(
        "CREATE TRIGGER IF NOT EXISTS \"{trigger_name}\"
            {update_of_clause}
            FOR EACH ROW
            WHEN NEW.{HLC_TIMESTAMP_COLUMN} IS NOT NULL
                AND COALESCE((SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'), '1') = '1'
            BEGIN
            {all_updates}

            INSERT OR REPLACE INTO {TABLE_CRDT_DIRTY_TABLES} (table_name, last_modified)
            SELECT '{table_name}', datetime('now')
            WHERE ({any_tracked_changed});
            END;"
    )
}

fn generate_delete_trigger_sql(table_name: &str, pks: &[String]) -> String {
    let trigger_name = DELETE_TRIGGER_TPL.replace("{TABLE_NAME}", table_name);

    let row_pks_json = pks
        .iter()
        .map(|name| format!("'{name}', OLD.\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "CREATE TRIGGER IF NOT EXISTS \"{trigger_name}\"
            BEFORE DELETE ON \"{table_name}\"
            FOR EACH ROW
            WHEN COALESCE((SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'), '1') = '1'
            BEGIN
            INSERT INTO {DELETED_ROWS_TABLE} (id, table_name, row_pks, {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN})
            VALUES ({UUID_FUNCTION_NAME}(), '{table_name}', json_object({row_pks_json}), {HLC_FUNCTION_NAME}(), '{{}}');
            INSERT OR REPLACE INTO {TABLE_CRDT_DIRTY_TABLES} (table_name, last_modified)
            VALUES ('{DELETED_ROWS_TABLE}', datetime('now'));
            END;"
    )
}

/// Adds the three CRDT metadata columns to `table_name` if any are missing.
/// Returns `true` when a column was added.
pub fn ensure_crdt_columns(tx: &Transaction, table_name: &str) -> Result<bool, CrdtSetupError> {
    let columns = get_table_schema(tx, table_name)?;

    if columns.is_empty() {
        return Ok(false);
    }

    let has_hlc = columns.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN);
    let has_column_hlcs = columns.iter().any(|c| c.name == COLUMN_HLCS_COLUMN);
    let has_column_sigs = columns.iter().any(|c| c.name == COLUMN_SIGS_COLUMN);

    let mut added_any = false;

    if !has_hlc {
        tx.execute(
            &format!("ALTER TABLE \"{table_name}\" ADD COLUMN \"{HLC_TIMESTAMP_COLUMN}\" TEXT"),
            [],
        )?;
        added_any = true;
    }

    if !has_column_hlcs {
        tx.execute(
            &format!(
                "ALTER TABLE \"{table_name}\" ADD COLUMN \"{COLUMN_HLCS_COLUMN}\" TEXT NOT NULL DEFAULT '{{}}'"
            ),
            [],
        )?;
        added_any = true;
    }

    if !has_column_sigs {
        tx.execute(
            &format!(
                "ALTER TABLE \"{table_name}\" ADD COLUMN \"{COLUMN_SIGS_COLUMN}\" TEXT NOT NULL DEFAULT '{{}}'"
            ),
            [],
        )?;
        added_any = true;
    }

    Ok(added_any)
}

/// Combines [`ensure_crdt_columns`] and [`setup_triggers_for_table`], installing
/// any missing trigger from the required set. The delete-event log requires
/// only INSERT and UPDATE triggers because it must not install a self-referential
/// DELETE trigger. Returns `(columns_added, triggers_created)`.
pub fn ensure_crdt_columns_and_triggers(
    tx: &Transaction,
    table_name: &str,
) -> Result<(bool, bool), CrdtSetupError> {
    let columns_added = ensure_crdt_columns(tx, table_name)?;

    let trigger_names = if table_name == DELETED_ROWS_TABLE {
        vec![INSERT_TRIGGER_TPL, UPDATE_TRIGGER_TPL]
    } else {
        vec![INSERT_TRIGGER_TPL, UPDATE_TRIGGER_TPL, DELETE_TRIGGER_TPL]
    };
    let has_all_triggers = trigger_names.iter().all(|template| {
        let trigger_name = template.replace("{TABLE_NAME}", table_name);
        tx.query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type = 'trigger' AND name = ?",
            [&trigger_name],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
    });

    let triggers_created = if !has_all_triggers {
        matches!(
            setup_triggers_for_table(tx, table_name, false)?,
            TriggerSetupResult::Success
        )
    } else {
        false
    };

    Ok((columns_added, triggers_created))
}

#[cfg(test)]
mod tests;
