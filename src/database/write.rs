//! Transactional write and read paths on [`Database`].
//!
//! [`Database::write`] opens one SQLite transaction and hands the caller a
//! [`CrdtTransaction`]. Every write in it runs through the CRDT transformer,
//! so the caller never stamps the HLC by hand, and all writes share the
//! transaction HLC: the whole closure is one transaction group for sync.
//! Tables ending in `_no_sync` pass through the transformer untouched, so the
//! same call writes synced and device-local tables.
//!
//! [`Database::read`] runs a closure on a restricted query view while
//! `PRAGMA query_only` and a SQLite authorizer prevent writes around the
//! transformer.

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{
    Connection, OptionalExtension, Params, Row, ToSql, Transaction, TransactionBehavior,
};
use sqlparser::ast::Statement;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use super::Database;
use crate::crdt::hlc::HlcService;
use crate::db::core::execute::{parse_crdt_write, transform_write};
use crate::db::error::DatabaseError;
use crate::error::Result;

/// One SQLite transaction on the CRDT write path, created by
/// [`Database::write`].
///
/// Writes are counted against [`Database::max_transaction_bytes`]: the
/// serialized size of all parameters of all writes in the transaction. The
/// write that would cross the limit fails with
/// [`DatabaseError::TransactionTooLarge`] before it runs.
pub struct CrdtTransaction<'a> {
    tx: Transaction<'a>,
    hlc: &'a HlcService,
    written_bytes: usize,
    max_bytes: usize,
}

/// Read-only view of the database connection passed to [`Database::read`].
///
/// The view exposes query operations only. The underlying connection also has
/// a SQLite authorizer installed while the callback runs, so SQL that attempts
/// to write, change pragmas, or alter transaction state is rejected.
pub struct ReadOnlyConnection<'c> {
    conn: &'c Connection,
}

impl ReadOnlyConnection<'_> {
    /// Runs a query expected to return one row.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn.query_row(sql, params, f)
    }

    /// Runs a query and maps every returned row.
    pub fn query_map<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map(params, f)?.collect();
        rows
    }
}

impl CrdtTransaction<'_> {
    /// Runs one statement. Writes are stamped with the transaction HLC.
    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<usize> {
        let sql = self.prepare(sql, params)?;
        Ok(self
            .tx
            .execute(&sql, params)
            .map_err(|e| execution_error(&sql, e))?)
    }

    /// Runs one statement and maps every returned row: a `SELECT`, or a
    /// write with `RETURNING`.
    pub fn query_map<T, F>(&mut self, sql: &str, params: &[&dyn ToSql], f: F) -> Result<Vec<T>>
    where
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let sql = self.prepare(sql, params)?;
        let mut stmt = self
            .tx
            .prepare(&sql)
            .map_err(|e| execution_error(&sql, e))?;
        let rows = stmt
            .query_map(params, f)
            .map_err(|e| execution_error(&sql, e))?;
        Ok(rows
            .collect::<rusqlite::Result<Vec<T>>>()
            .map_err(|e| execution_error(&sql, e))?)
    }

    /// Like [`Self::query_map`] for at most one row; `None` when there is
    /// none.
    pub fn query_row<T, F>(&mut self, sql: &str, params: &[&dyn ToSql], f: F) -> Result<Option<T>>
    where
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        let sql = self.prepare(sql, params)?;
        Ok(self
            .tx
            .query_row(&sql, params, f)
            .optional()
            .map_err(|e| execution_error(&sql, e))?)
    }

    /// Parses `sql`, rejects writes to CRDT meta columns, and for a write
    /// charges its parameters against the size limit and stamps the HLC.
    /// Returns the SQL to run.
    fn prepare(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<String> {
        let (mut statement, _touched) = parse_crdt_write(sql)?;
        if matches!(statement, Statement::Query(_)) {
            return Ok(sql.to_string());
        }
        self.charge(params)?;
        let (_hlc, sql) = transform_write(&self.tx, self.hlc, &mut statement)?;
        Ok(sql)
    }

    /// Adds parameter sizes to the transaction's cumulative byte count using
    /// saturating arithmetic. Returns an error if conversion fails or the
    /// updated count exceeds the limit; bytes already charged remain counted.
    fn charge(&mut self, params: &[&dyn ToSql]) -> Result<()> {
        for param in params {
            let bytes = param_bytes(*param).map_err(DatabaseError::from)?;
            self.written_bytes = self.written_bytes.saturating_add(bytes);
        }
        if self.written_bytes > self.max_bytes {
            return Err(DatabaseError::TransactionTooLarge {
                bytes: self.written_bytes,
                limit: self.max_bytes,
            }
            .into());
        }
        Ok(())
    }
}

impl Database {
    /// Runs `f` in one `IMMEDIATE` transaction on the CRDT write path and
    /// commits when it returns `Ok`. On `Err` or a panic the transaction
    /// rolls back and nothing is written.
    pub fn write<R>(&self, f: impl FnOnce(&mut CrdtTransaction<'_>) -> Result<R>) -> Result<R> {
        self.with_locked_conn(|conn| {
            let tx = conn
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(DatabaseError::from)?;
            let mut crdt_tx = CrdtTransaction {
                tx,
                hlc: &self.inner.hlc,
                written_bytes: 0,
                max_bytes: self.inner.max_transaction_bytes,
            };
            let value = f(&mut crdt_tx)?;
            crdt_tx.tx.commit().map_err(DatabaseError::from)?;
            Ok(value)
        })
    }

    /// Runs `f` on a read-only view of the connection. SQL writes, PRAGMA
    /// changes, and transaction control inside `f` are rejected.
    pub fn read<R>(&self, f: impl FnOnce(&ReadOnlyConnection<'_>) -> Result<R>) -> Result<R> {
        self.with_locked_conn(|conn| {
            let guard = QueryOnly::enable(conn)?;
            let result = {
                let read_only = match ReadOnlyConnection::enable(conn) {
                    Ok(read_only) => read_only,
                    Err(error) => {
                        drop(guard);
                        return Err(error);
                    }
                };
                catch_unwind(AssertUnwindSafe(|| f(&read_only)))
            };
            let _ = conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
            drop(guard);
            match result {
                Ok(result) => result,
                Err(payload) => resume_unwind(payload),
            }
        })
    }

    /// The write-size limit per transaction from
    /// [`super::DatabaseConfig::max_transaction_bytes`].
    pub fn max_transaction_bytes(&self) -> usize {
        self.inner.max_transaction_bytes
    }
}

/// Holds `PRAGMA query_only` on for its lifetime.
struct QueryOnly<'c>(&'c Connection);

impl<'c> QueryOnly<'c> {
    /// Enables `PRAGMA query_only` and returns a guard that disables it on drop.
    /// Returns an error if SQLite cannot enable the pragma.
    fn enable(conn: &'c Connection) -> Result<Self> {
        conn.pragma_update(None, "query_only", true)
            .map_err(DatabaseError::from)?;
        Ok(QueryOnly(conn))
    }
}

impl<'c> ReadOnlyConnection<'c> {
    fn enable(conn: &'c Connection) -> Result<Self> {
        conn.authorizer(Some(|context: AuthContext<'_>| match context.action {
            AuthAction::Read { .. }
            | AuthAction::Select
            | AuthAction::Function { .. }
            | AuthAction::Recursive => Authorization::Allow,
            _ => Authorization::Deny,
        }))
        .map_err(DatabaseError::from)?;
        Ok(ReadOnlyConnection { conn })
    }
}

impl Drop for QueryOnly<'_> {
    /// Attempts to disable `PRAGMA query_only`, ignoring reset errors because
    /// `Drop` cannot return them. A failed reset leaves the connection read-only.
    fn drop(&mut self) {
        // Drop cannot report the error. If resetting fails the connection
        // stays read-only, so later writes fail loudly rather than silently.
        let _ = self.0.pragma_update(None, "query_only", false);
    }
}

/// Wraps a SQLite failure with the executed SQL, leaving the table
/// unspecified.
fn execution_error(sql: &str, source: rusqlite::Error) -> DatabaseError {
    DatabaseError::ExecutionError {
        sql: sql.to_string(),
        table: None,
        source,
    }
}

/// Size of one parameter as SQLite stores it. Output that cannot be measured
/// counts as over any limit, the same fail-closed stance as
/// [`crate::db::core::execute::write_payload_too_large`].
fn param_bytes(param: &dyn ToSql) -> rusqlite::Result<usize> {
    Ok(match param.to_sql()? {
        ToSqlOutput::Borrowed(value) => value_bytes(value),
        ToSqlOutput::Owned(value) => value_bytes(ValueRef::from(&value)),
        _ => usize::MAX,
    })
}

/// Returns the payload size used for transaction accounting: zero for NULL,
/// eight bytes for numbers, and the byte length for text and BLOB values.
fn value_bytes(value: ValueRef<'_>) -> usize {
    match value {
        ValueRef::Null => 0,
        ValueRef::Integer(_) | ValueRef::Real(_) => 8,
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.len(),
    }
}
