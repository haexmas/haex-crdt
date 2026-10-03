//! Errors of the database layer.
//!
//! Every variant that wraps a failure from SQLite, the HLC service, the SQL
//! parser, JSON serialization, the filesystem or the trigger installer keeps
//! that failure as its `source`, so callers can inspect it (for example the
//! SQLite error code through [`DatabaseError::sqlite_error`]) instead of
//! parsing a message.

use std::io;

use sqlparser::parser::ParserError;
use thiserror::Error;

use crate::crdt::hlc::HlcError;
use crate::crdt::trigger::CrdtSetupError;

#[derive(Error, Debug)]
pub enum DatabaseError {
    /// The SQL could not be parsed.
    #[error("Failed to parse SQL: {source} - SQL: {sql}")]
    ParseError {
        sql: String,
        #[source]
        source: ParserError,
    },

    /// The SQL contains no statement.
    #[error("No SQL statement found - SQL: {sql}")]
    EmptyStatement { sql: String },

    #[error("Statement Error: {reason}")]
    StatementError { reason: String },

    /// A SQLite call failed.
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// A SQLite call during a named setup step failed.
    #[error("{step}: {source}")]
    SqliteStep {
        step: String,
        #[source]
        source: rusqlite::Error,
    },

    /// Executing a statement failed.
    #[error("Execution error on table {table:?}: {source} - SQL: {sql}")]
    ExecutionError {
        sql: String,
        table: Option<String>,
        #[source]
        source: rusqlite::Error,
    },

    /// A table cannot take part in CRDT scanning in its current shape.
    #[error("Table '{table}' is not usable for CRDT: {reason}")]
    InvalidTable { table: String, reason: String },

    /// The SQL statement is not supported on this path.
    #[error("Unsupported statement. '{reason}'. - SQL: {sql}")]
    UnsupportedStatement { reason: String, sql: String },

    /// The HLC service failed.
    #[error("HLC error: {0}")]
    Hlc(#[from] HlcError),

    /// A stored or generated HLC string is not a valid timestamp.
    #[error("Invalid HLC: {reason}")]
    InvalidHlc { reason: String },

    /// No connection is attached to the connection slot.
    #[error("Connection error: {reason}")]
    ConnectionError { reason: String },

    /// JSON serialization failed.
    #[error("Serialization error ({context}): {source}")]
    SerializationError {
        context: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("Mutex Poisoned error: {reason}")]
    MutexPoisoned { reason: String },

    #[error("Datenbankverbindung fehlgeschlagen für Pfad '{path}': {source}")]
    ConnectionFailed {
        path: String,
        #[source]
        source: rusqlite::Error,
    },

    /// Setting or reading a PRAGMA failed in SQLite.
    #[error("PRAGMA-Befehl '{pragma}' konnte nicht gesetzt werden: {source}")]
    PragmaFailed {
        pragma: String,
        #[source]
        source: rusqlite::Error,
    },

    /// A PRAGMA took effect with an unexpected value.
    #[error("PRAGMA-Befehl '{pragma}' konnte nicht gesetzt werden: {reason}")]
    PragmaError { pragma: String, reason: String },

    #[error("Datei-I/O-Fehler für Pfad '{path}': {source}")]
    IoError {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("CRDT setup failed: {0}")]
    CrdtSetup(#[from] CrdtSetupError),

    /// Another process already has this vault open — holding a `.lock` file
    /// on the vault DB path. Surface this as a distinct variant (rather than
    /// a generic IoError) so the UI can display a user-facing
    /// "vault already open in another window" message instead of a raw
    /// filesystem error.
    #[error("Vault at '{path}' is already open in another instance")]
    VaultAlreadyOpenElsewhere {
        path: String,
        #[source]
        source: io::Error,
    },

    #[error("Validation error: {reason}")]
    ValidationError { reason: String },

    /// A CRDT transaction exceeded the maximum serialized size (ADR 0001).
    #[error("CRDT transaction too large: {bytes} bytes exceeds the {limit} byte limit; use file storage for large payloads")]
    TransactionTooLarge { bytes: usize, limit: usize },

    /// The caller tried to write to a CRDT meta column directly (the
    /// row-level HLC, column-HLC map, or column-signature map — see the
    /// constants in `crate::crdt::columns`). Those columns are managed
    /// exclusively by the CRDT transformer and the signing passes — a
    /// caller-supplied value would either be silently clobbered by the
    /// transformer (row-level HLC) or, worse, would feed a forged HLC/sig
    /// into the sig preimage (column-HLC map — sig-forgery vector). Reject
    /// the whole statement — no silent stripping.
    #[error("CRDT meta column write is forbidden: '{column}' is managed by the CRDT layer and must not be set by callers")]
    CrdtMetaColumnWriteForbidden { column: String },

    /// The SQL holds more than one statement. Only whitespace and comments
    /// may follow the first one; a second statement is refused instead of
    /// being dropped.
    #[error("Only one SQL statement is allowed - SQL: {sql}")]
    MultipleStatements { sql: String },

    /// The [`crate::SqlGuard`] authorizer (or, in
    /// [`crate::Database::read_guarded`], the read-only rule) refused the
    /// statement while SQLite prepared it.
    #[error("Statement not authorized: {source} - SQL: {sql}")]
    SqlGuardDenied {
        sql: String,
        #[source]
        source: rusqlite::Error,
    },

    /// The [`crate::SqlGuard`] progress callback asked SQLite to stop the
    /// statement. The surrounding guarded write cannot commit any more.
    #[error("Statement interrupted by the progress callback - SQL: {sql}")]
    SqlGuardInterrupted { sql: String },

    /// A string or BLOB value, or a row, would have exceeded the length
    /// limit: [`crate::DatabaseConfig::max_value_bytes`], or the lower
    /// [`crate::SqlGuard::max_value_bytes`] of a guarded statement.
    #[error("Value or row larger than the length limit - SQL: {sql}")]
    ValueTooLarge { sql: String },

    /// The write transaction was rolled back — by an interrupt or by SQLite
    /// after a failed statement — and can neither run statements nor commit.
    #[error("Transaction aborted: {reason}")]
    TransactionAborted { reason: String },

    /// `PRAGMA foreign_key_check` found rows without a parent at the end of
    /// a schema-mode write; the transaction was rolled back.
    #[error("Foreign key check failed in tables {tables:?}")]
    ForeignKeyCheckFailed { tables: Vec<String> },
}

impl DatabaseError {
    /// The SQLite error behind this error, if there is one, so a caller can
    /// read its code (`rusqlite::Error::sqlite_error_code`).
    pub fn sqlite_error(&self) -> Option<&rusqlite::Error> {
        match self {
            DatabaseError::Sqlite(source)
            | DatabaseError::SqliteStep { source, .. }
            | DatabaseError::ExecutionError { source, .. }
            | DatabaseError::SqlGuardDenied { source, .. }
            | DatabaseError::ConnectionFailed { source, .. }
            | DatabaseError::PragmaFailed { source, .. }
            | DatabaseError::Hlc(HlcError::Database(source))
            | DatabaseError::CrdtSetup(CrdtSetupError::Database(source)) => Some(source),
            _ => None,
        }
    }
}
