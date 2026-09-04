//! Opens a SQLCipher-encrypted database file and wires the CRDT UDFs plus the
//! per-transaction HLC hooks on the resulting connection.
//!
//! Two UDFs are registered on every connection this crate hands out:
//!
//! - `gen_uuid()` — fresh UUIDv4 per call (used by the delete-event trigger).
//! - `current_hlc()` — the transaction-scoped HLC. Marked `INNOCUOUS` so it is
//!   legal from a trigger/view context under `trusted_schema=OFF`.
//!   Cross-statement stability inside a write transaction is provided by the
//!   [`crate::db::connection_context::ConnectionContext`] cache plus the
//!   commit/rollback/update hooks installed here.

use crate::crdt::columns::{HLC_FUNCTION_NAME, UUID_FUNCTION_NAME};
use crate::crdt::hlc::HlcService;
use crate::db::connection_context::ConnectionContext;
use crate::db::error::DatabaseError;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OpenFlags};
use uuid::Uuid;

/// Opens (or creates) the SQLCipher database at `path`, applies the encryption
/// key, enables foreign-key enforcement + WAL journaling, and installs the
/// CRDT UDFs and per-transaction hooks. The `HlcService` and
/// `ConnectionContext` are captured by the `current_hlc()` UDF, so callers
/// must pass instances that outlive the connection.
pub fn open_and_init_db(
    path: &str,
    key: &str,
    create: bool,
    hlc_service: HlcService,
    context: ConnectionContext,
) -> Result<Connection, DatabaseError> {
    let flags = if create {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };

    let conn =
        Connection::open_with_flags(path, flags).map_err(|e| DatabaseError::ConnectionFailed {
            path: path.to_string(),
            reason: e.to_string(),
        })?;

    conn.pragma_update(None, "key", key)
        .map_err(|e| DatabaseError::PragmaError {
            pragma: "key".to_string(),
            reason: e.to_string(),
        })?;

    // Foreign-key enforcement (required for PRAGMA defer_foreign_keys to work).
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(|e| DatabaseError::PragmaError {
            pragma: "foreign_keys".to_string(),
            reason: e.to_string(),
        })?;

    let fk_enabled: i32 = conn
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .map_err(|e| DatabaseError::PragmaError {
            pragma: "foreign_keys (verify)".to_string(),
            reason: e.to_string(),
        })?;
    if fk_enabled != 1 {
        return Err(DatabaseError::PragmaError {
            pragma: "foreign_keys".to_string(),
            reason: format!("PRAGMA foreign_keys returned {fk_enabled}, expected 1"),
        });
    }

    conn.create_scalar_function(
        UUID_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        |_ctx| Ok(Uuid::new_v4().to_string()),
    )
    .map_err(|e| DatabaseError::DatabaseError {
        reason: format!("Failed to register {UUID_FUNCTION_NAME} function: {e}"),
    })?;

    register_current_hlc_udf(&conn, hlc_service, context.clone())?;
    install_tx_hlc_hooks(&conn, context)?;

    let journal_mode: String = conn
        .query_row("PRAGMA journal_mode=WAL;", [], |row| row.get(0))
        .map_err(|e| DatabaseError::PragmaError {
            pragma: "journal_mode=WAL".to_string(),
            reason: e.to_string(),
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        return Err(DatabaseError::PragmaError {
            pragma: "journal_mode=WAL".to_string(),
            reason: format!("expected WAL, got '{journal_mode}'"),
        });
    }

    Ok(conn)
}

/// Registers `current_hlc()` on the given connection. Exposed so tests that
/// build bare in-memory connections can wire the UDF with the same
/// `INNOCUOUS` flag set the crate uses in production.
pub fn register_current_hlc_udf(
    conn: &Connection,
    hlc_service: HlcService,
    context: ConnectionContext,
) -> Result<(), DatabaseError> {
    conn.create_scalar_function(
        HLC_FUNCTION_NAME,
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_INNOCUOUS,
        move |_ctx| {
            context
                .current_or_new_tx_hlc(&hlc_service)
                .map(|ts| ts.to_string())
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
        },
    )
    .map_err(|e| DatabaseError::DatabaseError {
        reason: format!("Failed to register {HLC_FUNCTION_NAME} function: {e}"),
    })
}

/// Wires commit_hook, rollback_hook and update_hook so the per-transaction
/// HLC slot behaves correctly:
///
/// - `commit_hook` / `rollback_hook` clear the slot at end-of-transaction.
/// - `update_hook` flips the write-pending flag on the first row-level
///   INSERT/UPDATE/DELETE, so a stray read-only `SELECT current_hlc()`
///   cannot poison the HLC of a later write.
pub fn install_tx_hlc_hooks(
    conn: &Connection,
    context: ConnectionContext,
) -> Result<(), DatabaseError> {
    let ctx_commit = context.clone();
    conn.commit_hook(Some(move || {
        ctx_commit.reset_tx_slot();
        false
    }))
    .map_err(|e| DatabaseError::DatabaseError {
        reason: format!("Failed to install commit_hook: {e}"),
    })?;

    let ctx_rollback = context.clone();
    conn.rollback_hook(Some(move || {
        ctx_rollback.reset_tx_slot();
    }))
    .map_err(|e| DatabaseError::DatabaseError {
        reason: format!("Failed to install rollback_hook: {e}"),
    })?;

    let ctx_update = context;
    conn.update_hook(Some(
        move |_action, _db: &str, _table: &str, _row_id: i64| {
            ctx_update.mark_write_pending();
        },
    ))
    .map_err(|e| DatabaseError::DatabaseError {
        reason: format!("Failed to install update_hook: {e}"),
    })?;
    Ok(())
}
