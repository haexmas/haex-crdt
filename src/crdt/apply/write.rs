//! The two SQL writers the apply loop ends in, wrapped in a savepoint so a
//! NOT NULL / UNIQUE INSERT failure can be recovered per-row, plus the
//! signature-map helpers `SignatureApplyPolicy` uses to reproduce today's
//! flat `[column]` replace-or-remove shape.
//!
//! Split out of `engine.rs` to keep both files inside the repo's file-size
//! cap. Note the column ordering in [`write_insert`]: the staged remote
//! columns precede the crate's own, and SQLite takes the first value for a
//! column named twice — which is why the row loop must filter the crate's
//! own column names out of remote input before staging them.

use rusqlite::types::Value as SqlValue;
use rusqlite::Transaction;

use crate::crdt::apply::policy_types::SignatureWrite;
use crate::crdt::apply::row::StagedColumn;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::trigger::ColumnInfo;
use crate::db::core::ValueConverter;
use crate::db::error::DatabaseError;

/// A row's INSERT/UPDATE either landed, or failed with a raw `rusqlite`
/// error the caller must classify (only an INSERT's NOT NULL / UNIQUE
/// failure is recoverable — see [`classify_insert_constraint`]).
pub(super) enum WriteOutcome {
    Written,
    SqlFailure(rusqlite::Error),
}

const ROW_SAVEPOINT: &str = "haex_apply_row";

/// Wrap `write_fn` in a SQLite savepoint: on success, release it; on
/// failure, return the raw error for the caller to classify (constraint
/// failures are handled by the caller rolling back the savepoint itself,
/// since only an INSERT's NOT NULL/UNIQUE failure is recoverable and the
/// caller is the one that knows which statement kind this was).
pub(super) fn in_row_savepoint(
    tx: &Transaction<'_>,
    write_fn: impl FnOnce() -> rusqlite::Result<()>,
) -> Result<WriteOutcome, DatabaseError> {
    tx.execute_batch(&format!("SAVEPOINT {ROW_SAVEPOINT}"))?;
    match write_fn() {
        Ok(()) => {
            tx.execute_batch(&format!("RELEASE SAVEPOINT {ROW_SAVEPOINT}"))?;
            Ok(WriteOutcome::Written)
        }
        Err(e) => Ok(WriteOutcome::SqlFailure(e)),
    }
}

/// Roll back and release the row savepoint after a classified INSERT
/// constraint failure, before the policy's `on_insert_constraint` hook runs.
pub(super) fn rollback_row_savepoint(tx: &Transaction<'_>) -> Result<(), DatabaseError> {
    tx.execute_batch(&format!(
        "ROLLBACK TO SAVEPOINT {ROW_SAVEPOINT}; RELEASE SAVEPOINT {ROW_SAVEPOINT}"
    ))?;
    Ok(())
}

/// Classify a SQL error from an INSERT statement as a recoverable NOT NULL /
/// UNIQUE constraint violation, or `None` for anything else (which always
/// aborts the batch regardless of any policy hook).
pub(super) fn classify_insert_constraint(
    err: &rusqlite::Error,
) -> Option<super::report::SkipReason> {
    use super::report::SkipReason;
    if let rusqlite::Error::SqliteFailure(ffi_err, _) = err {
        if ffi_err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL {
            return Some(SkipReason::InsertNotNull);
        }
        if ffi_err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE {
            return Some(SkipReason::InsertUnique);
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_update(
    tx: &Transaction<'_>,
    table_name: &str,
    staged: &[StagedColumn<'_>],
    column_hlcs_json: &str,
    max_hlc_for_row: &str,
    where_clause: &str,
    pk_values: &[serde_json::Value],
    has_sigs_column: bool,
) -> Result<WriteOutcome, DatabaseError> {
    let mut set_parts: Vec<String> = staged
        .iter()
        .map(|s| format!("\"{}\" = ?", s.change.column_name))
        .collect();
    set_parts.push(format!("{COLUMN_HLCS_COLUMN} = ?"));
    set_parts.push(format!("{HLC_TIMESTAMP_COLUMN} = ?"));
    if has_sigs_column {
        set_parts.push(format!("{COLUMN_SIGS_COLUMN} = ?"));
    }
    let sql = format!(
        "UPDATE \"{table_name}\" SET {} WHERE {where_clause}",
        set_parts.join(", ")
    );

    let mut params: Vec<SqlValue> = Vec::with_capacity(staged.len() + 3 + pk_values.len());
    for s in staged {
        params.push(s.value.clone());
    }
    params.push(SqlValue::Text(column_hlcs_json.to_string()));
    params.push(SqlValue::Text(max_hlc_for_row.to_string()));
    if has_sigs_column {
        params.push(SqlValue::Text(merge_sigs_json(
            tx,
            table_name,
            where_clause,
            pk_values,
            staged,
        )?));
    }
    for v in ValueConverter::convert_params(pk_values)? {
        params.push(v);
    }

    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
    in_row_savepoint(tx, || tx.execute(&sql, &*param_refs).map(|_| ()))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_insert(
    tx: &Transaction<'_>,
    table_name: &str,
    schema: &[ColumnInfo],
    row_pks: &serde_json::Map<String, serde_json::Value>,
    staged: &[StagedColumn<'_>],
    column_hlcs_json: &str,
    max_hlc_for_row: &str,
    has_sigs_column: bool,
) -> Result<WriteOutcome, DatabaseError> {
    let mut columns: Vec<String> = Vec::new();
    let mut values: Vec<SqlValue> = Vec::new();

    let pk_columns: Vec<&ColumnInfo> = schema.iter().filter(|c| c.is_pk).collect();
    let pk_json_values: Vec<serde_json::Value> = pk_columns
        .iter()
        .map(|c| row_pks[&c.name].clone())
        .collect();
    for (c, v) in pk_columns
        .iter()
        .zip(ValueConverter::convert_params(&pk_json_values)?)
    {
        columns.push(c.name.clone());
        values.push(v);
    }
    for s in staged {
        columns.push(s.change.column_name.clone());
        values.push(s.value.clone());
    }
    columns.push(COLUMN_HLCS_COLUMN.to_string());
    columns.push(HLC_TIMESTAMP_COLUMN.to_string());
    values.push(SqlValue::Text(column_hlcs_json.to_string()));
    values.push(SqlValue::Text(max_hlc_for_row.to_string()));
    if has_sigs_column {
        let sigs_json = build_sigs_json_for_insert(staged);
        columns.push(COLUMN_SIGS_COLUMN.to_string());
        values.push(SqlValue::Text(sigs_json));
    }

    let placeholders = vec!["?"; columns.len()].join(", ");
    let quoted: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
    let sql = format!(
        "INSERT INTO \"{table_name}\" ({}) VALUES ({placeholders})",
        quoted.join(", ")
    );
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        values.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
    in_row_savepoint(tx, || tx.execute(&sql, &*param_refs).map(|_| ()))
}

/// Serialise the column-signature JSON map for a fresh INSERT — only staged
/// columns whose [`SignatureWrite`] is `Replace(Some(_))` land in the map;
/// `Keep` and `Replace(None)` both contribute nothing, since a fresh row has
/// no prior entry to keep.
fn build_sigs_json_for_insert(staged: &[StagedColumn<'_>]) -> String {
    let mut map = serde_json::Map::new();
    for s in staged {
        if let SignatureWrite::Replace(Some(sig)) = &s.signature {
            map.insert(s.change.column_name.clone(), sig.clone());
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// Merge staged sigs into the row's existing column-signature JSON for an
/// UPDATE, per column: `Replace(Some(_))` inserts, `Replace(None)` removes,
/// `Keep` leaves that column's existing entry untouched.
fn merge_sigs_json(
    tx: &Transaction<'_>,
    table_name: &str,
    where_clause: &str,
    pk_values: &[serde_json::Value],
    staged: &[StagedColumn<'_>],
) -> Result<String, DatabaseError> {
    let sql = format!("SELECT {COLUMN_SIGS_COLUMN} FROM \"{table_name}\" WHERE {where_clause}");
    let sql_params = ValueConverter::convert_params(pk_values)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> = sql_params
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();
    let mut stmt = tx.prepare(&sql)?;
    let existing: String = stmt
        .query_row(&*param_refs, |r| r.get::<_, String>(0))
        .unwrap_or_else(|_| "{}".to_string());
    let mut map: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&existing).unwrap_or_default();
    for s in staged {
        match &s.signature {
            SignatureWrite::Replace(Some(sig)) => {
                map.insert(s.change.column_name.clone(), sig.clone());
            }
            SignatureWrite::Replace(None) => {
                map.remove(&s.change.column_name);
            }
            SignatureWrite::Keep => {}
        }
    }
    Ok(serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string()))
}
