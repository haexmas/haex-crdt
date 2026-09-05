//! Write path. Two entry points:
//!
//! - [`execute`] runs a statement without CRDT rewriting. Triggers are
//!   suppressed for the duration of the statement by flipping
//!   `triggers_enabled` in the CRDT config table to `'0'` and then restoring
//!   its previous state inside the same transaction. Sync-facing readers
//!   never see the flag on `'0'`.
//!
//! - [`execute_with_crdt`] runs a statement through
//!   [`crate::crdt::transformer::CrdtTransformer`] with the transaction-
//!   scoped HLC, executes it, and invokes every registered
//!   [`crate::db::execute_hook::PostWriteSigner`] inside the same
//!   transaction before commit. The invariant: either the write + the
//!   dirty-tables entry + the delete-event log row + every signer's derived
//!   rows commit together, or nothing does.

use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_FUNCTION_NAME, HLC_TIMESTAMP_COLUMN,
};
use crate::crdt::hlc::HlcService;
use crate::crdt::transformer::CrdtTransformer;
use crate::db::core::connection::with_connection;
use crate::db::core::extract::extract_primary_table_name_from_sql;
use crate::db::core::parsing::{parse_single_statement, statement_has_returning};
use crate::db::core::prefix::strip_main_schema_prefix;
use crate::db::core::value::{convert_value_ref_to_json, ValueConverter};
use crate::db::error::DatabaseError;
use crate::db::execute_hook::{PostWriteSigner, TouchedColumns, TouchedTable, WriteContext};
use crate::db::DbConnection;
use crate::table_names::TABLE_CRDT_CONFIGS;
use rusqlite::types::Value as RusqliteValue;
use rusqlite::{params_from_iter, OptionalExtension, ToSql, Transaction};
use serde_json::Value as JsonValue;
use sqlparser::ast::{AssignmentTarget, ObjectName, Statement, TableFactor, TableObject};
use std::str::FromStr;
use std::sync::Arc;
use uhlc::Timestamp;

/// Maximum serialized size of a single CRDT transaction (ADR 0001).
///
/// One `execute_with_crdt` call parses one statement and runs it in its own
/// `conn.transaction()` — nothing nests `execute_with_crdt` calls — so one
/// call is exactly one SQLite transaction (one HLC). Enforcing the cap per
/// call is equivalent to a per-transaction byte counter, with no extra
/// plumbing. Larger payloads must use file storage, never CRDT columns.
pub const MAX_CRDT_TRANSACTION_BYTES: usize = 100 * 1024 * 1024;

/// Returns `Some(bytes)` if the serialized size of `params` exceeds `limit`,
/// else `None`. Fail-closed: an unmeasurable payload counts as over-limit.
///
/// `limit` is a parameter (not the const) so tests can inject a tiny limit
/// instead of allocating `MAX_CRDT_TRANSACTION_BYTES`.
pub fn write_payload_too_large(params: &[JsonValue], limit: usize) -> Option<usize> {
    let bytes = serde_json::to_vec(&params)
        .map(|v| v.len())
        .unwrap_or(usize::MAX);
    (bytes > limit).then_some(bytes)
}

/// Executes a statement through the CRDT transformer and invokes every
/// registered [`PostWriteSigner`] inside the same transaction.
///
/// - `signers` are called in registration order after the main write, before
///   `tx.commit()`. The first `Err` aborts the transaction; later signers
///   are skipped.
/// - CRDT meta-column writes (`haex_hlc`, `haex_column_hlcs`,
///   `haex_column_sigs`) are hard-rejected: the transformer would silently
///   clobber `haex_hlc`, and a caller-supplied `haex_column_hlcs` would
///   feed a forged HLC into any signer's preimage.
/// - Batches whose serialized params exceed [`MAX_CRDT_TRANSACTION_BYTES`]
///   are rejected before any write happens.
pub fn execute_with_crdt(
    sql: String,
    params: Vec<JsonValue>,
    connection: &DbConnection,
    hlc_service: &HlcService,
    signers: &[Arc<dyn PostWriteSigner>],
) -> Result<Vec<Vec<JsonValue>>, DatabaseError> {
    if let Some(bytes) = write_payload_too_large(&params, MAX_CRDT_TRANSACTION_BYTES) {
        return Err(DatabaseError::TransactionTooLarge {
            bytes,
            limit: MAX_CRDT_TRANSACTION_BYTES,
        });
    }

    let statement = parse_single_statement(&sql)?;
    let has_returning = statement_has_returning(&statement);
    let touched = extract_touched_for_signing(&statement);

    // Reject caller-supplied writes to CRDT meta columns. The transformer
    // would otherwise clobber `haex_hlc` silently, and a caller-supplied
    // `haex_column_hlcs` would feed a forged HLC into the sig-preimage of
    // any signer running after this write — an attacker could then mint a
    // valid signature over an arbitrary HLC. Hard rejection is the only
    // safe choice.
    if let Some(bad) = touched
        .as_ref()
        .and_then(|(_, cols)| cols.explicit().iter().find(|c| is_crdt_meta_column(c)))
    {
        return Err(DatabaseError::CrdtMetaColumnWriteForbidden {
            column: bad.clone(),
        });
    }

    with_connection(connection, |conn| {
        let tx = conn.transaction()?;

        let (result, tx_hlc, transformed_statement) = if has_returning {
            let (ts, transformed, rows) = query_internal(&tx, hlc_service, statement, &params)?;
            (rows, ts, transformed)
        } else {
            let (ts, transformed) = execute_internal(&tx, hlc_service, statement, &params)?;
            (vec![], ts, transformed)
        };

        if !signers.is_empty() {
            let ctx = WriteContext {
                statement: &transformed_statement,
                touched,
                hlc: &tx_hlc,
            };
            for signer in signers {
                signer.on_after_write(&tx, &ctx)?;
            }
        }

        tx.commit()?;
        Ok(result)
    })
}

/// Executes a statement without CRDT rewriting. Suppresses triggers for the
/// duration of the write by flipping [`TABLE_CRDT_CONFIGS`]'s
/// `triggers_enabled` row to `'0'` inside the transaction and restoring its
/// previous value (or absence) before commit. Concurrent sync-facing
/// connections never see the flag on `'0'` because SQLite's transaction
/// isolation shields the intermediate value.
pub fn execute(
    sql: String,
    params: Vec<JsonValue>,
    connection: &DbConnection,
) -> Result<Vec<Vec<JsonValue>>, DatabaseError> {
    let params_converted: Vec<RusqliteValue> = params
        .iter()
        .map(ValueConverter::json_to_rusqlite_value)
        .collect::<Result<Vec<_>, _>>()?;
    let params_sql: Vec<&dyn ToSql> = params_converted.iter().map(|v| v as &dyn ToSql).collect();

    let has_returning = {
        let stmt = parse_single_statement(&sql)?;
        statement_has_returning(&stmt)
    };

    with_connection(connection, |conn| {
        let tx = conn.transaction()?;

        let previous_triggers_enabled: Option<Option<String>> = tx
            .query_row(
                &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'"),
                [],
                |row| row.get(0),
            )
            .optional()?;

        let disable_sql = format!(
            "INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value) VALUES ('triggers_enabled', 'system', '0') \
             ON CONFLICT(key) DO UPDATE SET value = '0'"
        );
        tx.execute(&disable_sql, [])?;

        let result = if has_returning {
            let mut result_vec: Vec<Vec<JsonValue>> = Vec::new();
            let mut stmt = tx.prepare(&sql)?;
            let num_columns = stmt.column_count();
            let mut rows = stmt.query(&params_sql[..])?;

            while let Some(row) = rows.next()? {
                let mut row_values: Vec<JsonValue> = Vec::with_capacity(num_columns);
                for i in 0..num_columns {
                    let value_ref = row.get_ref(i)?;
                    let json_val = convert_value_ref_to_json(value_ref)?;
                    row_values.push(json_val);
                }
                result_vec.push(row_values);
            }
            drop(rows);
            drop(stmt);
            result_vec
        } else {
            tx.execute(&sql, &params_sql[..]).map_err(|e| {
                let table_name = extract_primary_table_name_from_sql(&sql).unwrap_or(None);
                DatabaseError::ExecutionError {
                    sql: sql.clone(),
                    reason: e.to_string(),
                    table: table_name,
                }
            })?;
            vec![]
        };

        match previous_triggers_enabled {
            Some(previous_value) => {
                tx.execute(
                    &format!(
                        "UPDATE {TABLE_CRDT_CONFIGS} SET value = ?1 WHERE key = 'triggers_enabled'"
                    ),
                    [previous_value],
                )?;
            }
            None => {
                tx.execute(
                    &format!("DELETE FROM {TABLE_CRDT_CONFIGS} WHERE key = 'triggers_enabled'"),
                    [],
                )?;
            }
        }

        tx.commit()?;
        Ok(result)
    })
}

// ---- private helpers -----------------------------------------------------

/// Reads the transaction-scoped HLC, aligns [`HlcService`] with it, and
/// persists it to the HLC row in [`TABLE_CRDT_CONFIGS`]. Every write inside
/// one transaction goes through this once at the top so the transformer's SQL
/// literal and the `current_hlc()` UDF (used by triggers) return the same value.
fn tx_scoped_hlc(tx: &Transaction, hlc_service: &HlcService) -> Result<Timestamp, DatabaseError> {
    let hlc_str: String = tx
        .query_row(&format!("SELECT {HLC_FUNCTION_NAME}()"), [], |row| {
            row.get(0)
        })
        .map_err(|e| DatabaseError::HlcError {
            reason: format!("Failed to read {HLC_FUNCTION_NAME}(): {e}"),
        })?;

    let timestamp = Timestamp::from_str(&hlc_str).map_err(|e| DatabaseError::HlcError {
        reason: format!("Invalid HLC from UDF: {e:?}"),
    })?;

    hlc_service
        .update_with_timestamp(&timestamp)
        .map_err(|e| DatabaseError::HlcError {
            reason: e.to_string(),
        })?;

    HlcService::persist_timestamp(tx, &timestamp).map_err(|e| DatabaseError::HlcError {
        reason: e.to_string(),
    })?;

    Ok(timestamp)
}

/// Runs a non-RETURNING write through the CRDT transformer + main-prefix
/// stripping and executes it against `tx`. Returns the tx-scoped HLC and
/// transformed statement so the caller can hand both to [`PostWriteSigner`]s.
fn execute_internal(
    tx: &Transaction,
    hlc_service: &HlcService,
    mut statement: Statement,
    params: &[JsonValue],
) -> Result<(Timestamp, Statement), DatabaseError> {
    let sql_params = ValueConverter::convert_params(params)?;
    let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|p| p as &dyn ToSql).collect();

    let transformer = CrdtTransformer::new();
    let hlc_timestamp = tx_scoped_hlc(tx, hlc_service)?;

    transformer.transform_execute_statement(&mut statement, &hlc_timestamp)?;

    let raw_sql = statement.to_string();
    let sql_str = strip_main_schema_prefix(&raw_sql);

    tx.execute(&sql_str, &param_refs[..])
        .map_err(|e| DatabaseError::ExecutionError {
            sql: sql_str.clone(),
            table: None,
            reason: format!("Execute failed: {e}"),
        })?;

    Ok((hlc_timestamp, statement))
}

/// RETURNING variant of [`execute_internal`]. Returns the HLC, transformed
/// statement, and rows so the caller can supply signer context and results.
fn query_internal(
    tx: &Transaction,
    hlc_service: &HlcService,
    mut statement: Statement,
    params: &[JsonValue],
) -> Result<(Timestamp, Statement, Vec<Vec<JsonValue>>), DatabaseError> {
    let sql_params = ValueConverter::convert_params(params)?;
    let param_refs: Vec<&dyn ToSql> = sql_params.iter().map(|p| p as &dyn ToSql).collect();

    let transformer = CrdtTransformer::new();
    let hlc_timestamp = tx_scoped_hlc(tx, hlc_service)?;

    transformer.transform_execute_statement(&mut statement, &hlc_timestamp)?;

    let raw_sql = statement.to_string();
    let sql_str = strip_main_schema_prefix(&raw_sql);

    let mut stmt = tx
        .prepare(&sql_str)
        .map_err(|e| DatabaseError::ExecutionError {
            sql: sql_str.clone(),
            table: None,
            reason: e.to_string(),
        })?;
    let num_columns = stmt.column_names().len();

    let mut rows = stmt
        .query(params_from_iter(param_refs.iter()))
        .map_err(|e| DatabaseError::ExecutionError {
            sql: sql_str.clone(),
            table: None,
            reason: e.to_string(),
        })?;

    let mut result_vec: Vec<Vec<JsonValue>> = Vec::new();
    while let Some(row) = rows.next().map_err(|e| DatabaseError::ExecutionError {
        sql: sql_str.clone(),
        table: None,
        reason: e.to_string(),
    })? {
        let mut row_values: Vec<JsonValue> = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            let value_ref = row.get_ref(i).map_err(|e| DatabaseError::ExecutionError {
                sql: sql_str.clone(),
                table: None,
                reason: e.to_string(),
            })?;
            row_values.push(convert_value_ref_to_json(value_ref)?);
        }
        result_vec.push(row_values);
    }

    Ok((hlc_timestamp, statement, result_vec))
}

/// Extracts `(target_table, touched_columns)` for statements that carry
/// column writes; returns `None` for statements the signer does not handle
/// (SELECT / DELETE / DDL). Table names and column names are case-folded to
/// lowercase — SQL identifiers are case-insensitive and downstream `==`
/// comparisons are safer when the values arrive canonicalised.
pub(crate) fn extract_touched_for_signing(
    stmt: &Statement,
) -> Option<(TouchedTable, TouchedColumns)> {
    match stmt {
        Statement::Insert(insert) => {
            let name = match &insert.table {
                TableObject::TableName(n) => object_name_last(n)?,
                _ => return None,
            };
            if insert.columns.is_empty() {
                return Some((TouchedTable::from_raw(&name), TouchedColumns::AllColumns));
            }
            let cols: Vec<String> = insert
                .columns
                .iter()
                .filter_map(object_name_last)
                .map(|c| c.to_ascii_lowercase())
                .collect();
            Some((
                TouchedTable::from_raw(&name),
                TouchedColumns::Explicit(cols),
            ))
        }
        Statement::Update(update) => {
            let name = match &update.table.relation {
                TableFactor::Table { name, .. } => object_name_last(name)?,
                _ => return None,
            };
            let mut cols = Vec::new();
            for assignment in &update.assignments {
                let targets: &[ObjectName] = match &assignment.target {
                    AssignmentTarget::ColumnName(target) => std::slice::from_ref(target),
                    AssignmentTarget::Tuple(targets) => targets,
                };
                cols.extend(
                    targets
                        .iter()
                        .filter_map(object_name_last)
                        .map(|column| column.to_ascii_lowercase()),
                );
            }
            Some((
                TouchedTable::from_raw(&name),
                TouchedColumns::Explicit(cols),
            ))
        }
        _ => None,
    }
}

/// Returns the unqualified final identifier from an object name.
fn object_name_last(obj: &ObjectName) -> Option<String> {
    obj.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
}

/// True iff `col` is one of the CRDT meta columns whose value must be
/// produced by the CRDT layer, not by the caller.
/// Whether a column is maintained exclusively by the CRDT write path.
fn is_crdt_meta_column(col: &str) -> bool {
    col == HLC_TIMESTAMP_COLUMN || col == COLUMN_HLCS_COLUMN || col == COLUMN_SIGS_COLUMN
}

#[cfg(test)]
mod tests;
