//! Database layer: SQL execution, connection wrapping, CRDT-aware helpers.
//!
//! C1 lands the read-only surface: connection newtype, per-connection HLC
//! slot, SQL parsing helpers, `main.` schema prefix stripping, table-name
//! extraction, `select` / `select_with_crdt`, and connection init with the
//! `gen_uuid` / `current_hlc` UDFs plus commit/rollback/update hooks.
//!
//! C2 adds `execute` / `execute_with_crdt` with the post-write signing hook
//! trait; sync-transport concerns stay in `haex-vault`.

pub mod connection_context;
pub mod core;
pub mod error;
pub mod execute_hook;
pub mod init;
pub mod migrations;
pub mod row;

use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Thin lockable wrapper around an optional SQLite connection.
///
/// The `Option` slot lets the owning process mount/unmount a database file
/// without dropping the `Arc` handle held by other components; `None` signals
/// "no connection currently attached", which the `with_connection` helper
/// surfaces as `DatabaseError::ConnectionError`.
pub struct DbConnection(pub Arc<Mutex<Option<Connection>>>);

impl DbConnection {
    /// Wraps an already-open connection. Consumers that mount/unmount at
    /// runtime construct the newtype directly with their own `Arc<Mutex<...>>`.
    pub fn new(connection: Connection) -> Self {
        DbConnection(Arc::new(Mutex::new(Some(connection))))
    }

    /// Empty slot — no connection attached. Useful as a placeholder before
    /// the owning process opens the actual database file.
    pub fn empty() -> Self {
        DbConnection(Arc::new(Mutex::new(None)))
    }
}
