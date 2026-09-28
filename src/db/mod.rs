//! Database layer: connection init with the `gen_uuid` / `current_hlc` UDFs
//! and the transaction hooks, the per-connection HLC slot, SQL parsing
//! helpers, `main.` schema prefix stripping, table-name extraction, the CRDT
//! write helpers behind [`crate::Database::write`], migrations and the file
//! lock.

pub mod connection_context;
pub mod core;
pub mod error;
pub mod init;
pub mod lock;
pub mod migrations;
