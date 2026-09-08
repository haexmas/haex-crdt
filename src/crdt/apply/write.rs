//! The two SQL writers the apply loop ends in, plus the signature-map
//! helpers they need.
//!
//! Split out of `engine.rs` to keep both files inside the repo's file-size
//! cap. Note the column ordering in [`write_insert`]: the staged remote
//! columns precede the crate's own, and SQLite takes the first value for a
//! column named twice — which is why `engine`'s write loop must filter the
//! crate's own column names out of remote input before staging them.

use rusqlite::types::Value as SqlValue;
use rusqlite::Transaction;
use serde_json::Value as JsonValue;

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::trigger::ColumnInfo;
use crate::db::core::ValueConverter;
use crate::db::error::DatabaseError;

#[allow(clippy::too_many_arguments)]
pub(super) fn write_update(
    tx: &Transaction<'_>,
    table_name: &str,
    staged: &[(String, SqlValue, String, Option<JsonValue>)],
    column_hlcs_json: &str,
    max_hlc_for_row: &str,
    where_clause: &str,
    pk_values: &[JsonValue],
    has_sigs_column: bool,
) -> std::result::Result<(), DatabaseError> {
    let mut set_parts: Vec<String> = staged
        .iter()
        .map(|(col, _, _, _)| format!("\"{col}\" = ?"))
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
    for (_, val, _, _) in staged {
        params.push(val.clone());
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
    tx.execute(&sql, &*param_refs)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn write_insert(
    tx: &Transaction<'_>,
    table_name: &str,
    schema: &[ColumnInfo],
    row_pks: &serde_json::Map<String, JsonValue>,
    staged: &[(String, SqlValue, String, Option<JsonValue>)],
    column_hlcs_json: &str,
    max_hlc_for_row: &str,
    has_sigs_column: bool,
) -> std::result::Result<(), DatabaseError> {
    let mut columns: Vec<String> = Vec::new();
    let mut values: Vec<SqlValue> = Vec::new();

    let pk_columns: Vec<&ColumnInfo> = schema.iter().filter(|c| c.is_pk).collect();
    let pk_json_values: Vec<JsonValue> = pk_columns
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
    for (col, val, _, _) in staged {
        columns.push(col.clone());
        values.push(val.clone());
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
    tx.execute(&sql, &*param_refs)?;
    Ok(())
}

/// Serialise the column-signature JSON map for a fresh INSERT — only the
/// staged columns that carry a `sig` land in the map.
fn build_sigs_json_for_insert(staged: &[(String, SqlValue, String, Option<JsonValue>)]) -> String {
    let mut map = serde_json::Map::new();
    for (col, _, _, sig) in staged {
        if let Some(s) = sig {
            map.insert(col.clone(), s.clone());
        }
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// Merge staged sigs into the row's existing column-signature JSON for an
/// UPDATE. A signed value replaces the column's previous signature; an
/// unsigned value removes it because the old signature no longer describes
/// the current column value.
fn merge_sigs_json(
    tx: &Transaction<'_>,
    table_name: &str,
    where_clause: &str,
    pk_values: &[JsonValue],
    staged: &[(String, SqlValue, String, Option<JsonValue>)],
) -> std::result::Result<String, DatabaseError> {
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
    let mut map: serde_json::Map<String, JsonValue> =
        serde_json::from_str(&existing).unwrap_or_default();
    for (col, _, _, sig) in staged {
        match sig {
            Some(s) => {
                map.insert(col.clone(), s.clone());
            }
            None => {
                map.remove(col);
            }
        }
    }
    Ok(serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string()))
}
