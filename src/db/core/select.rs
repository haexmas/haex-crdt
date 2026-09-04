//! Read-only SQL entry points.
//!
//! - [`select`] executes a plain `SELECT` and returns rows as
//!   `Vec<Vec<serde_json::Value>>`. Rejects any non-Query statement.
//! - [`select_with_crdt`] runs the same statement through
//!   [`crate::crdt::transformer::CrdtTransformer`] and
//!   [`crate::db::core::prefix::strip_main_schema_prefix`] before executing.
//!
//! Both functions rely on [`crate::db::core::connection::with_connection`]
//! for locking and connection-slot handling. Params are handed in as
//! `serde_json::Value` and converted via [`super::value::ValueConverter`].

use crate::db::core::connection::with_connection;
use crate::db::core::parsing::parse_single_statement;
use crate::db::core::prefix::strip_main_schema_prefix;
use crate::db::core::value::{convert_value_ref_to_json, ValueConverter};
use crate::db::error::DatabaseError;
use crate::db::DbConnection;
use rusqlite::types::Value as RusqliteValue;
use rusqlite::ToSql;
use serde_json::Value as JsonValue;
use sqlparser::ast::Statement;

pub fn select(
    sql: String,
    params: Vec<JsonValue>,
    connection: &DbConnection,
) -> Result<Vec<Vec<JsonValue>>, DatabaseError> {
    let statement = parse_single_statement(&sql)?;

    if !matches!(statement, Statement::Query(_)) {
        return Err(DatabaseError::StatementError {
            reason: "Only SELECT statements are allowed in select function".to_string(),
        });
    }

    let params_converted: Vec<RusqliteValue> = params
        .iter()
        .map(ValueConverter::json_to_rusqlite_value)
        .collect::<Result<Vec<_>, _>>()?;
    let params_sql: Vec<&dyn ToSql> = params_converted.iter().map(|v| v as &dyn ToSql).collect();

    with_connection(connection, |conn| {
        let mut stmt = conn.prepare(&sql)?;
        let num_columns = stmt.column_count();
        let mut rows = stmt.query(&params_sql[..])?;
        let mut result_vec: Vec<Vec<JsonValue>> = Vec::new();

        while let Some(row) = rows.next()? {
            let mut row_values: Vec<JsonValue> = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let value_ref = row.get_ref(i)?;
                let json_val = convert_value_ref_to_json(value_ref)?;
                row_values.push(json_val);
            }
            result_vec.push(row_values);
        }
        Ok(result_vec)
    })
}

pub fn select_with_crdt(
    sql: String,
    params: Vec<JsonValue>,
    connection: &DbConnection,
) -> Result<Vec<Vec<JsonValue>>, DatabaseError> {
    use crate::crdt::transformer::CrdtTransformer;

    let statement = parse_single_statement(&sql)?;

    let transformed_sql = if let Statement::Query(mut query) = statement {
        let transformer = CrdtTransformer::new();
        transformer.transform_query(&mut query);
        strip_main_schema_prefix(&query.to_string())
    } else {
        return Err(DatabaseError::StatementError {
            reason: "Only SELECT statements are allowed in select_with_crdt".to_string(),
        });
    };

    let params_converted: Vec<RusqliteValue> = params
        .iter()
        .map(ValueConverter::json_to_rusqlite_value)
        .collect::<Result<Vec<_>, _>>()?;
    let params_sql: Vec<&dyn ToSql> = params_converted.iter().map(|v| v as &dyn ToSql).collect();

    with_connection(connection, |conn| {
        let mut stmt = conn.prepare(&transformed_sql)?;
        let num_columns = stmt.column_count();
        let mut rows = stmt.query(&params_sql[..])?;
        let mut result_vec: Vec<Vec<JsonValue>> = Vec::new();

        while let Some(row) = rows.next()? {
            let mut row_values: Vec<JsonValue> = Vec::with_capacity(num_columns);
            for i in 0..num_columns {
                let value_ref = row.get_ref(i)?;
                let json_val = convert_value_ref_to_json(value_ref)?;
                row_values.push(json_val);
            }
            result_vec.push(row_values);
        }
        Ok(result_vec)
    })
}
