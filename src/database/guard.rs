//! Guarded execution of SQL from a less trusted caller.
//!
//! [`Database::write_guarded`] and [`Database::read_guarded`] work like
//! [`Database::write`] and [`Database::read`], with a [`SqlGuard`] that the
//! consumer supplies: a SQLite authorizer and an optional progress callback.
//! Both are installed only while the caller's own statement is prepared and
//! stepped. The statements the crate runs around it — reading and persisting
//! the transaction HLC, recreating triggers, toggling the trigger gate,
//! the foreign key check — run without them, so the authorizer only ever
//! judges the caller's SQL. Trigger bodies are compiled into the caller's
//! statement, so the authorizer sees them too, with
//! [`AuthContext::accessor`] set to the trigger name (`z_dirty_<table>_*`).
//!
//! - In [`Database::read_guarded`] the authorizer is combined with the
//!   read-only rule of [`Database::read`]: a statement passes only when both
//!   allow it.
//! - A denial is [`DatabaseError::SqlGuardDenied`]; a progress callback that
//!   returns `true` interrupts the statement with
//!   [`DatabaseError::SqlGuardInterrupted`], and the guarded write can no
//!   longer commit ([`DatabaseError::TransactionAborted`]): everything rolls
//!   back.
//! - [`SqlGuard::max_value_bytes`] lowers the connection's limit for one
//!   value or row while the caller's statement runs; a larger value is
//!   [`DatabaseError::ValueTooLarge`], before SQLite allocates it.
//! - SQL with a second statement after the first is refused with
//!   [`DatabaseError::MultipleStatements`]; whitespace and comments are fine.
//! - [`CrdtTransaction::query_with_columns`] and
//!   [`ReadOnlyConnection::query_with_columns`] return the column names even
//!   when no row comes back.
//!
//! [`Database::write_guarded_with`] adds [`GuardedWriteOptions`]: schema mode
//! for migrations and local mode for device-local development tables. Schema
//! changes are described in [`super::schema`].

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::{Connection, ErrorCode};

use super::{CrdtTransaction, Database, ReadOnlyConnection};
use crate::crdt::cleanup::with_fk_disabled;
use crate::db::error::DatabaseError;
use crate::error::Result;

/// Decides every access SQLite reports while it prepares the caller's
/// statement. See [`rusqlite::hooks::AuthAction`] for the actions.
pub type SqlAuthorizer = Arc<dyn Fn(&AuthContext<'_>) -> Authorization + Send + Sync>;

/// Called every N virtual machine instructions of the caller's statement;
/// returning `true` interrupts it.
pub type SqlProgress = Arc<dyn Fn() -> bool + Send + Sync>;

/// The authorizer and progress callback of a guarded read or write.
#[derive(Clone)]
pub struct SqlGuard {
    /// Judges each table, column, function and pragma access of the caller's
    /// statement, including the trigger bodies it fires.
    pub authorizer: SqlAuthorizer,
    /// `(instructions, callback)`: SQLite calls `callback` every
    /// `instructions` virtual machine steps of the caller's statement; `true`
    /// interrupts it. Use it for a runtime limit.
    pub progress: Option<(i32, SqlProgress)>,
    /// Upper bound for one string or BLOB value, and for one row, while the
    /// caller's statement runs (`SQLITE_LIMIT_LENGTH`): a larger value is
    /// [`DatabaseError::ValueTooLarge`]. It only lowers
    /// [`crate::DatabaseConfig::max_value_bytes`], never raises it. `None`
    /// keeps the database's limit.
    pub max_value_bytes: Option<usize>,
}

/// A byte count as a value for `sqlite3_limit` (SQLite caps it further at
/// its compile-time maximum).
pub(crate) fn limit_value(bytes: usize) -> i32 {
    i32::try_from(bytes).unwrap_or(i32::MAX)
}

/// Modes of [`Database::write_guarded_with`]. The default is a plain
/// guarded write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GuardedWriteOptions {
    /// Schema mode for migrations: foreign keys are switched off before the
    /// transaction begins (so a `DROP TABLE` in a table rebuild cannot
    /// cascade into child tables), `PRAGMA foreign_key_check` runs before the
    /// commit (any violation rolls back with
    /// [`DatabaseError::ForeignKeyCheckFailed`]), and the previous setting is
    /// restored afterwards. Also enables
    /// [`CrdtTransaction::copy_rows_verbatim`].
    pub schema_mode: bool,
    /// Local mode for device-local tables (developer mode): `CREATE TABLE`
    /// gets no CRDT columns and no triggers, and writes to a table without
    /// CRDT columns pass through without stamping. Tables that do carry CRDT
    /// columns are still stamped.
    pub local: bool,
}

/// Rows of a query together with its column names, which are also present
/// when no row came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRows<T> {
    /// Column names in result order.
    pub columns: Vec<String>,
    /// The mapped rows.
    pub rows: Vec<T>,
}

/// Which rule the authorizer of a guarded statement is combined with.
#[derive(Clone, Copy)]
pub(crate) enum GuardScope {
    /// Only the guard decides; nothing is installed outside the statement.
    Write,
    /// The read-only rule of [`Database::read`] and the guard must both
    /// allow; the read-only rule stays installed outside the statement.
    Read,
}

impl Database {
    /// [`Database::write`] with `guard` around every statement of the
    /// caller. See the [module docs](crate::database::guard).
    pub fn write_guarded<R>(
        &self,
        guard: &SqlGuard,
        f: impl FnOnce(&mut CrdtTransaction<'_>) -> Result<R>,
    ) -> Result<R> {
        self.write_guarded_with(guard, GuardedWriteOptions::default(), f)
    }

    /// [`Database::write_guarded`] in schema mode and/or local mode, see
    /// [`GuardedWriteOptions`]. In schema mode the foreign key setting is
    /// switched and restored while the connection lock is held.
    pub fn write_guarded_with<R>(
        &self,
        guard: &SqlGuard,
        options: GuardedWriteOptions,
        f: impl FnOnce(&mut CrdtTransaction<'_>) -> Result<R>,
    ) -> Result<R> {
        self.with_locked_conn(|conn| {
            if options.schema_mode {
                with_fk_disabled(conn, |conn| self.run_write(conn, Some(guard), options, f))
            } else {
                self.run_write(conn, Some(guard), options, f)
            }
        })
    }

    /// [`Database::read`] with `guard` around every statement of the
    /// caller, combined with the read-only rule.
    pub fn read_guarded<R>(
        &self,
        guard: &SqlGuard,
        f: impl FnOnce(&ReadOnlyConnection<'_>) -> Result<R>,
    ) -> Result<R> {
        self.run_read(Some(guard), f)
    }
}

/// The authorizer rule of [`Database::read`]: reads, function calls and
/// recursive CTEs only.
pub(crate) fn read_only_rule(context: &AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Read { .. }
        | AuthAction::Select
        | AuthAction::Function { .. }
        | AuthAction::Recursive => Authorization::Allow,
        _ => Authorization::Deny,
    }
}

/// Installs the read-only rule as the connection's authorizer.
pub(crate) fn install_read_only(conn: &Connection) -> rusqlite::Result<()> {
    conn.authorizer(Some(|context: AuthContext<'_>| read_only_rule(&context)))
}

/// Runs `run` — the caller's statement — with `guard` installed on `conn`,
/// then puts back what [`GuardScope`] prescribes. Returns the statement's
/// result and whether the progress callback asked for an interrupt.
pub(crate) fn run_guarded<T>(
    conn: &Connection,
    guard: &SqlGuard,
    scope: GuardScope,
    run: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<(rusqlite::Result<T>, bool)> {
    let interrupted = Arc::new(AtomicBool::new(false));
    let previous_limit = match install(conn, guard, scope, &interrupted) {
        Ok(previous_limit) => previous_limit,
        Err(error) => {
            let _ = uninstall(conn, scope, None);
            return Err(error);
        }
    };
    let outcome = catch_unwind(AssertUnwindSafe(run));
    let restored = uninstall(conn, scope, previous_limit);
    let result = match outcome {
        Ok(result) => result,
        Err(payload) => resume_unwind(payload),
    };
    restored?;
    Ok((result, interrupted.load(Ordering::SeqCst)))
}

/// Installs the guard. Returns the value limit to put back, when the guard
/// lowered it; that happens last, so a failed install leaves it untouched.
fn install(
    conn: &Connection,
    guard: &SqlGuard,
    scope: GuardScope,
    interrupted: &Arc<AtomicBool>,
) -> rusqlite::Result<Option<i32>> {
    let authorizer = Arc::clone(&guard.authorizer);
    match scope {
        GuardScope::Write => {
            conn.authorizer(Some(move |context: AuthContext<'_>| authorizer(&context)))?
        }
        GuardScope::Read => conn.authorizer(Some(move |context: AuthContext<'_>| {
            match read_only_rule(&context) {
                Authorization::Allow => authorizer(&context),
                denied => denied,
            }
        }))?,
    }
    if let Some((instructions, callback)) = &guard.progress {
        let callback = Arc::clone(callback);
        let flag = Arc::clone(interrupted);
        conn.progress_handler(
            *instructions,
            Some(move || {
                let stop = callback();
                if stop {
                    flag.store(true, Ordering::SeqCst);
                }
                stop
            }),
        )?;
    }
    let Some(bytes) = guard.max_value_bytes else {
        return Ok(None);
    };
    let current = conn.limit(Limit::SQLITE_LIMIT_LENGTH)?;
    conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, current.min(limit_value(bytes)))?;
    Ok(Some(current))
}

fn uninstall(
    conn: &Connection,
    scope: GuardScope,
    previous_limit: Option<i32>,
) -> rusqlite::Result<()> {
    let limit = previous_limit.map_or(Ok(0), |previous| {
        conn.set_limit(Limit::SQLITE_LIMIT_LENGTH, previous)
    });
    conn.progress_handler(0, None::<fn() -> bool>)?;
    match scope {
        GuardScope::Write => conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?,
        GuardScope::Read => install_read_only(conn)?,
    }
    limit.map(drop)
}

/// Maps a SQLite failure of a statement to a typed error: a denial by the
/// authorizer, a statement tail, a value over the length limit, or else an
/// execution error carrying the SQL.
pub(crate) fn statement_error(sql: &str, source: rusqlite::Error) -> DatabaseError {
    if matches!(source, rusqlite::Error::MultipleStatement) {
        return DatabaseError::MultipleStatements {
            sql: sql.to_string(),
        };
    }
    if source.sqlite_error_code() == Some(ErrorCode::AuthorizationForStatementDenied) {
        return DatabaseError::SqlGuardDenied {
            sql: sql.to_string(),
            source,
        };
    }
    if source.sqlite_error_code() == Some(ErrorCode::TooBig) {
        return DatabaseError::ValueTooLarge {
            sql: sql.to_string(),
        };
    }
    DatabaseError::ExecutionError {
        sql: sql.to_string(),
        table: None,
        source,
    }
}
