//! CRDT write helpers behind [`crate::Database::write`]: parse one
//! statement, reject caller writes to CRDT meta columns, and stamp the
//! statement with the transaction-scoped HLC through
//! [`crate::crdt::transformer::CrdtTransformer`].

use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_FUNCTION_NAME, HLC_TIMESTAMP_COLUMN,
};
use crate::crdt::hlc::HlcService;
use crate::crdt::transformer::CrdtTransformer;
use crate::db::core::parsing::parse_single_statement;
use crate::db::core::prefix::strip_main_schema_prefix;
use crate::db::error::DatabaseError;
use rusqlite::Transaction;
use sqlparser::ast::{AssignmentTarget, ObjectName, Statement, TableFactor};
use std::str::FromStr;
use uhlc::Timestamp;

/// Default maximum serialized size of a single CRDT transaction (ADR 0001),
/// the default of [`crate::DatabaseConfig::max_transaction_bytes`]. Larger
/// payloads must use file storage, never CRDT columns.
pub const MAX_CRDT_TRANSACTION_BYTES: usize = 100 * 1024 * 1024;

/// Parses one statement for the CRDT write path.
///
/// Rejects caller-supplied writes to CRDT meta columns. The transformer
/// would otherwise clobber the row-level HLC silently, and a caller-supplied
/// column-HLC map would feed a forged HLC into any signature preimage built
/// over this write — an attacker could then mint a valid signature over an
/// arbitrary HLC. Hard rejection is the only safe choice. `ON CONFLICT … DO
/// UPDATE` assignments are checked by the transformer itself.
pub(crate) fn parse_crdt_write(sql: &str) -> Result<Statement, DatabaseError> {
    let statement = parse_single_statement(sql)?;
    if let Some(column) = explicitly_written_columns(&statement)
        .into_iter()
        .find(|c| is_crdt_meta_column(c))
    {
        return Err(DatabaseError::CrdtMetaColumnWriteForbidden { column });
    }
    Ok(statement)
}

/// Stamps `statement` with the transaction-scoped HLC through the CRDT
/// transformer and returns the HLC, the SQL to run and — for a `CREATE
/// TABLE` or `ALTER TABLE` on a synced table — the (lowercased) name of the
/// table whose schema the statement changes.
pub(crate) fn transform_write(
    tx: &Transaction,
    hlc_service: &HlcService,
    statement: &mut Statement,
) -> Result<(Timestamp, String, Option<String>), DatabaseError> {
    let hlc_timestamp = tx_scoped_hlc(tx, hlc_service)?;
    let schema_changed =
        CrdtTransformer::new().transform_execute_statement(statement, &hlc_timestamp)?;
    let sql = strip_main_schema_prefix(&statement.to_string());
    Ok((hlc_timestamp, sql, schema_changed))
}

/// Reads the transaction-scoped HLC, aligns [`HlcService`] with it, and
/// persists it to the HLC row in `haex_crdt_configs_no_sync`. Every write
/// inside one transaction goes through this so the transformer's SQL literal
/// and the `current_hlc()` UDF (used by triggers) return the same value.
fn tx_scoped_hlc(tx: &Transaction, hlc_service: &HlcService) -> Result<Timestamp, DatabaseError> {
    let hlc_str: String = tx
        .query_row(&format!("SELECT {HLC_FUNCTION_NAME}()"), [], |row| {
            row.get(0)
        })
        .map_err(|source| DatabaseError::SqliteStep {
            step: format!("read {HLC_FUNCTION_NAME}()"),
            source,
        })?;

    let timestamp = Timestamp::from_str(&hlc_str).map_err(|e| DatabaseError::InvalidHlc {
        reason: format!("from {HLC_FUNCTION_NAME}(): {e:?}"),
    })?;

    hlc_service
        .update_with_timestamp(&timestamp)
        .map_err(DatabaseError::from)?;

    HlcService::persist_timestamp(tx, &timestamp).map_err(DatabaseError::from)?;

    Ok(timestamp)
}

/// The columns an INSERT names or an UPDATE assigns, case-folded to
/// lowercase (SQL identifiers are case-insensitive). Empty for a column-less
/// INSERT and for statements that write no named columns (SELECT, DELETE,
/// DDL).
pub(crate) fn explicitly_written_columns(stmt: &Statement) -> Vec<String> {
    let names: Vec<&ObjectName> = match stmt {
        Statement::Insert(insert) => insert.columns.iter().collect(),
        Statement::Update(update) => {
            if !matches!(update.table.relation, TableFactor::Table { .. }) {
                return Vec::new();
            }
            update
                .assignments
                .iter()
                .flat_map(|assignment| match &assignment.target {
                    AssignmentTarget::ColumnName(target) => std::slice::from_ref(target),
                    AssignmentTarget::Tuple(targets) => targets.as_slice(),
                })
                .collect()
        }
        _ => Vec::new(),
    };
    names
        .into_iter()
        .filter_map(object_name_last)
        .map(|column| column.to_ascii_lowercase())
        .collect()
}

/// Returns the unqualified final identifier from an object name.
fn object_name_last(obj: &ObjectName) -> Option<String> {
    obj.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.clone())
}

/// Whether a column is maintained exclusively by the CRDT write path.
fn is_crdt_meta_column(col: &str) -> bool {
    col == HLC_TIMESTAMP_COLUMN || col == COLUMN_HLCS_COLUMN || col == COLUMN_SIGS_COLUMN
}

#[cfg(test)]
mod tests;
