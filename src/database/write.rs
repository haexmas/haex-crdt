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
//!
//! [`Database::write_guarded`] and [`Database::read_guarded`] use the same
//! transaction and view with a consumer [`SqlGuard`] around each statement;
//! see [`super::guard`] and, for schema changes, [`super::schema`].

use rusqlite::hooks::{AuthContext, Authorization};
use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{
    Connection, OptionalExtension, Params, Row, ToSql, Transaction, TransactionBehavior,
};
use sqlparser::ast::Statement;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use super::guard::{
    install_read_only, run_guarded, statement_error, GuardScope, GuardedWriteOptions, QueryRows,
    SqlGuard,
};
use super::schema::{self, SchemaChange};
use super::Database;
use crate::crdt::hlc::HlcService;
use crate::db::core::execute::{parse_crdt_write, transform_write};
use crate::db::core::strip_main_schema_prefix;
use crate::db::error::DatabaseError;
use crate::error::Result;

/// One SQLite transaction on the CRDT write path, created by
/// [`Database::write`] or [`Database::write_guarded`].
///
/// Writes are counted against [`Database::max_transaction_bytes`]: the
/// serialized size of all parameters of all writes in the transaction. The
/// write that would cross the limit fails with
/// [`DatabaseError::TransactionTooLarge`] before it runs.
///
/// Once SQLite has rolled the transaction back — after an interrupt or a
/// failure it cannot recover from — every further call and the commit fail
/// with [`DatabaseError::TransactionAborted`].
pub struct CrdtTransaction<'a> {
    tx: Transaction<'a>,
    hlc: &'a HlcService,
    written_bytes: usize,
    max_bytes: usize,
    guard: Option<&'a SqlGuard>,
    options: GuardedWriteOptions,
    aborted: Option<String>,
}

/// Read-only view of the database connection passed to [`Database::read`]
/// and [`Database::read_guarded`].
///
/// The view exposes query operations only. The underlying connection also has
/// a SQLite authorizer installed while the callback runs, so SQL that attempts
/// to write, change pragmas, or alter transaction state is rejected.
pub struct ReadOnlyConnection<'c> {
    conn: &'c Connection,
    guard: Option<&'c SqlGuard>,
}

/// The SQL to run for one statement and the schema change it makes.
struct PreparedStatement {
    sql: String,
    schema: Option<SchemaChange>,
}

impl ReadOnlyConnection<'_> {
    /// Runs a query expected to return one row.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.guarded(|| self.conn.query_row(sql, params, f))
    }

    /// Runs a query and maps every returned row.
    pub fn query_map<T, P, F>(&self, sql: &str, params: P, f: F) -> rusqlite::Result<Vec<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.guarded(|| Ok(query_rows(self.conn, sql, params, f)?.rows))
    }

    /// Like [`Self::query_map`], with the column names (also when no row
    /// comes back) and typed errors: [`DatabaseError::SqlGuardDenied`],
    /// [`DatabaseError::SqlGuardInterrupted`],
    /// [`DatabaseError::MultipleStatements`].
    pub fn query_with_columns<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<QueryRows<T>>
    where
        P: Params,
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        let run = || query_rows(self.conn, sql, params, f);
        let result = match self.guard {
            None => run(),
            Some(guard) => {
                let (result, interrupted) = run_guarded(self.conn, guard, GuardScope::Read, run)
                    .map_err(DatabaseError::from)?;
                if interrupted {
                    return Err(DatabaseError::SqlGuardInterrupted {
                        sql: sql.to_string(),
                    }
                    .into());
                }
                result
            }
        };
        Ok(result.map_err(|source| statement_error(sql, source))?)
    }

    /// Runs `run` inside the guard window of [`Database::read_guarded`], or
    /// directly under [`Database::read`].
    fn guarded<T>(&self, run: impl FnOnce() -> rusqlite::Result<T>) -> rusqlite::Result<T> {
        let Some(guard) = self.guard else {
            return run();
        };
        run_guarded(self.conn, guard, GuardScope::Read, run).and_then(|(result, _)| result)
    }
}

impl CrdtTransaction<'_> {
    /// Runs one statement. Writes are stamped with the transaction HLC.
    pub fn execute(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<usize> {
        self.run(sql, params, |conn, sql| conn.execute(sql, params))
    }

    /// Runs one statement and maps every returned row: a `SELECT`, or a
    /// write with `RETURNING`.
    pub fn query_map<T, F>(&mut self, sql: &str, params: &[&dyn ToSql], f: F) -> Result<Vec<T>>
    where
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        Ok(self.query_with_columns(sql, params, f)?.rows)
    }

    /// Like [`Self::query_map`], with the column names, which are also
    /// present when no row comes back.
    pub fn query_with_columns<T, F>(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        f: F,
    ) -> Result<QueryRows<T>>
    where
        F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.run(sql, params, |conn, sql| query_rows(conn, sql, params, f))
    }

    /// Like [`Self::query_map`] for at most one row; `None` when there is
    /// none.
    pub fn query_row<T, F>(&mut self, sql: &str, params: &[&dyn ToSql], f: F) -> Result<Option<T>>
    where
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.run(sql, params, |conn, sql| {
            conn.query_row(sql, params, f).optional()
        })
    }

    /// The rowid of the last row inserted by this connection
    /// (`last_insert_rowid()`), read in this transaction.
    pub fn last_insert_rowid(&self) -> i64 {
        self.tx.last_insert_rowid()
    }

    /// Copies rows for a table rebuild in schema mode: `sql` is `INSERT INTO
    /// T (cols) SELECT exprs FROM S` (one source table, no joins, no
    /// wildcard, no `ON CONFLICT` or `RETURNING`). When both tables carry
    /// CRDT columns, those columns are copied unchanged and the triggers are
    /// off during the copy, so no row is stamped anew and nothing is marked
    /// dirty. Tables without CRDT columns are copied as written.
    ///
    /// Fails with [`DatabaseError::UnsupportedStatement`] outside
    /// [`GuardedWriteOptions::schema_mode`] or for any other shape.
    pub fn copy_rows_verbatim(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<usize> {
        self.ensure_open()?;
        let guard = match self.guard {
            Some(guard) if self.options.schema_mode => guard,
            _ => {
                return Err(DatabaseError::UnsupportedStatement {
                    sql: sql.to_string(),
                    reason: "copy_rows_verbatim needs a guarded write in schema mode".to_string(),
                }
                .into())
            }
        };
        let mut statement = parse_crdt_write(sql)?;
        schema::rewrite_rebuild_copy(&self.tx, &mut statement)?;
        self.charge(params)?;
        let sql = strip_main_schema_prefix(&statement.to_string());

        let previous = schema::begin_verbatim_copy(&self.tx)?;
        let (result, interrupted) = run_guarded(&self.tx, guard, GuardScope::Write, || {
            self.tx.execute(&sql, params)
        })
        .map_err(DatabaseError::from)
        .inspect_err(|_| schema::rollback(&self.tx))?;
        let copied = self
            .finish(&sql, result, interrupted)
            .and_then(|copied| schema::end_verbatim_copy(&self.tx, &previous).map(|_| copied));
        copied.inspect_err(|_| schema::rollback(&self.tx))
    }

    /// Prepares `sql`, runs it through `exec` — inside the guard window when
    /// there is a guard — and keeps the triggers in step with a schema
    /// change.
    fn run<T>(
        &mut self,
        sql: &str,
        params: &[&dyn ToSql],
        exec: impl FnOnce(&Connection, &str) -> rusqlite::Result<T>,
    ) -> Result<T> {
        self.ensure_open()?;
        let prepared = self.prepare(sql, params)?;
        let Some(guard) = self.guard else {
            let result = exec(&self.tx, &prepared.sql);
            return self.finish(&prepared.sql, result, false);
        };
        if let Some(change) = &prepared.schema {
            schema::begin(&self.tx, change)?;
        }
        let outcome = run_guarded(&self.tx, guard, GuardScope::Write, || {
            exec(&self.tx, &prepared.sql)
        });
        let result = outcome
            .map_err(|source| DatabaseError::from(source).into())
            .and_then(|(result, interrupted)| self.finish(&prepared.sql, result, interrupted));
        let Some(change) = &prepared.schema else {
            return result;
        };
        let result = result.and_then(|value| schema::complete(&self.tx, change).map(|_| value));
        if result.is_err() {
            schema::rollback(&self.tx);
            self.note_rollback();
        }
        result
    }

    /// Parses `sql`, rejects writes to CRDT meta columns, and for a write
    /// charges its parameters against the size limit and stamps the HLC.
    /// Returns the SQL to run and, in a guarded write, the schema change it
    /// makes.
    fn prepare(&mut self, sql: &str, params: &[&dyn ToSql]) -> Result<PreparedStatement> {
        let mut statement = parse_crdt_write(sql)?;
        if matches!(statement, Statement::Query(_)) {
            return Ok(PreparedStatement {
                sql: sql.to_string(),
                schema: None,
            });
        }
        self.charge(params)?;
        if self.guard.is_none() {
            let (_hlc, sql, _) = transform_write(&self.tx, self.hlc, &mut statement)?;
            return Ok(PreparedStatement { sql, schema: None });
        }
        schema::check_rename(&statement)?;
        if self.options.local && !schema::transforms_in_local_mode(&self.tx, &statement)? {
            return Ok(PreparedStatement {
                sql: strip_main_schema_prefix(&statement.to_string()),
                schema: None,
            });
        }
        let (_hlc, sql, changed) = transform_write(&self.tx, self.hlc, &mut statement)?;
        let schema = changed.and_then(|table| SchemaChange::of(&statement, table));
        Ok(PreparedStatement { sql, schema })
    }

    /// Turns the result of the caller's statement into the crate's result:
    /// an interrupt aborts the transaction, and a failure after which SQLite
    /// rolled the transaction back marks it aborted.
    fn finish<T>(
        &mut self,
        sql: &str,
        result: rusqlite::Result<T>,
        interrupted: bool,
    ) -> Result<T> {
        if interrupted {
            self.aborted = Some("the progress callback interrupted a statement".to_string());
            return Err(DatabaseError::SqlGuardInterrupted {
                sql: sql.to_string(),
            }
            .into());
        }
        match result {
            Ok(value) => Ok(value),
            Err(source) => {
                self.note_rollback();
                Err(statement_error(sql, source).into())
            }
        }
    }

    /// Marks the transaction aborted when SQLite has already rolled it back.
    fn note_rollback(&mut self) {
        if self.aborted.is_none() && self.tx.is_autocommit() {
            self.aborted = Some("SQLite rolled the transaction back".to_string());
        }
    }

    fn ensure_open(&self) -> Result<()> {
        match &self.aborted {
            Some(reason) => Err(DatabaseError::TransactionAborted {
                reason: reason.clone(),
            }
            .into()),
            None => Ok(()),
        }
    }

    /// Adds parameter sizes to the transaction's cumulative byte count using
    /// saturating arithmetic. Returns an error if conversion fails or the
    /// updated count exceeds the limit; bytes already charged remain counted.
    fn charge(&mut self, params: &[&dyn ToSql]) -> Result<()> {
        let bytes = serialized_parameter_bytes(params).map_err(DatabaseError::from)?;
        self.written_bytes = self.written_bytes.saturating_add(bytes);
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
        self.with_locked_conn(|conn| self.run_write(conn, None, GuardedWriteOptions::default(), f))
    }

    /// Runs `f` on a read-only view of the connection. SQL writes, PRAGMA
    /// changes, and transaction control inside `f` are rejected.
    pub fn read<R>(&self, f: impl FnOnce(&ReadOnlyConnection<'_>) -> Result<R>) -> Result<R> {
        self.run_read(None, f)
    }

    /// The write-size limit per transaction from
    /// [`super::DatabaseConfig::max_transaction_bytes`].
    pub fn max_transaction_bytes(&self) -> usize {
        self.inner.max_transaction_bytes
    }

    /// The transaction behind [`Self::write`] and [`Self::write_guarded_with`]:
    /// the caller holds the connection lock; in schema mode foreign keys are
    /// already off.
    pub(super) fn run_write<R>(
        &self,
        conn: &mut Connection,
        guard: Option<&SqlGuard>,
        options: GuardedWriteOptions,
        f: impl FnOnce(&mut CrdtTransaction<'_>) -> Result<R>,
    ) -> Result<R> {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(DatabaseError::from)?;
        let mut crdt_tx = CrdtTransaction {
            tx,
            hlc: &self.inner.hlc,
            written_bytes: 0,
            max_bytes: self.inner.max_transaction_bytes,
            guard,
            options,
            aborted: None,
        };
        let value = f(&mut crdt_tx)?;
        crdt_tx.ensure_open()?;
        if options.schema_mode {
            schema::foreign_key_check(&crdt_tx.tx)?;
        }
        crdt_tx.tx.commit().map_err(DatabaseError::from)?;
        Ok(value)
    }

    /// The read view behind [`Self::read`] and [`Self::read_guarded`].
    pub(super) fn run_read<R>(
        &self,
        guard: Option<&SqlGuard>,
        f: impl FnOnce(&ReadOnlyConnection<'_>) -> Result<R>,
    ) -> Result<R> {
        self.with_locked_conn(|conn| {
            let query_only = QueryOnly::enable(conn)?;
            let result = {
                let read_only = match ReadOnlyConnection::enable(conn, guard) {
                    Ok(read_only) => read_only,
                    Err(error) => {
                        drop(query_only);
                        return Err(error);
                    }
                };
                catch_unwind(AssertUnwindSafe(|| f(&read_only)))
            };
            let _ = conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
            drop(query_only);
            match result {
                Ok(result) => result,
                Err(payload) => resume_unwind(payload),
            }
        })
    }
}

/// Prepares `sql` and maps every row, keeping the column names.
fn query_rows<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    f: F,
) -> rusqlite::Result<QueryRows<T>>
where
    P: Params,
    F: FnMut(&Row<'_>) -> rusqlite::Result<T>,
{
    let mut statement = conn.prepare(sql)?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let rows = statement
        .query_map(params, f)?
        .collect::<rusqlite::Result<Vec<T>>>()?;
    Ok(QueryRows { columns, rows })
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
    fn enable(conn: &'c Connection, guard: Option<&'c SqlGuard>) -> Result<Self> {
        install_read_only(conn).map_err(DatabaseError::from)?;
        Ok(ReadOnlyConnection { conn, guard })
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

/// The canonical byte accounting for [`crate::DatabaseConfig::max_transaction_bytes`]:
/// the sum of the stored sizes of all bind parameters — text and BLOB length,
/// 8 bytes for an integer or real, 0 for NULL. SQL text, column names and HLC
/// metadata do not count. A consumer that applies a transaction group from
/// elsewhere measures it with the same function, so local writes and
/// received groups are checked against the same size. Output that cannot be
/// measured counts as over any limit.
pub fn serialized_parameter_bytes(params: &[&dyn ToSql]) -> rusqlite::Result<usize> {
    params.iter().try_fold(0usize, |total, param| {
        Ok(total.saturating_add(param_bytes(*param)?))
    })
}

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
