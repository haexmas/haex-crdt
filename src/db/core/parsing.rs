//! SQL statement parsing helpers built on top of `sqlparser` with the SQLite
//! dialect. All callers in the crate route AST access through these two
//! entry points so parse-error surfacing stays consistent.

use crate::db::error::DatabaseError;
use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

/// Parses exactly one SQL statement, erroring if the input is empty or
/// unparseable. Only whitespace, comments and semicolons may follow the
/// statement; a second statement is a [`DatabaseError::MultipleStatements`]
/// error instead of being dropped.
pub fn parse_single_statement(sql: &str) -> Result<Statement, DatabaseError> {
    let dialect = SQLiteDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|source| DatabaseError::ParseError {
            sql: sql.to_string(),
            source,
        })?;

    let mut statements = statements.into_iter();
    let first = statements
        .next()
        .ok_or_else(|| DatabaseError::EmptyStatement {
            sql: sql.to_string(),
        })?;
    if statements.next().is_some() {
        return Err(DatabaseError::MultipleStatements {
            sql: sql.to_string(),
        });
    }
    Ok(first)
}

/// Parses one or more SQL statements while preserving the original SQL text,
/// including whitespace inside string literals.
pub fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, DatabaseError> {
    let dialect = SQLiteDialect {};

    Parser::parse_sql(&dialect, sql).map_err(|source| DatabaseError::ParseError {
        sql: sql.to_string(),
        source,
    })
}

/// AST-level check for a `RETURNING` clause on INSERT/UPDATE/DELETE. SELECT
/// always returns `false`.
pub fn statement_has_returning(statement: &Statement) -> bool {
    match statement {
        Statement::Insert(insert) => insert.returning.is_some(),
        Statement::Update(update) => update.returning.is_some(),
        Statement::Delete(delete) => delete.returning.is_some(),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
