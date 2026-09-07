use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "details", rename_all_fields = "camelCase")]
pub enum DatabaseError {
    /// Der SQL-Code konnte nicht geparst werden.
    #[error("Failed to parse SQL: {reason} - SQL: {sql}")]
    ParseError { reason: String, sql: String },

    /// Parameter-Fehler (falsche Anzahl, ungültiger Typ, etc.)
    #[error("Parameter count mismatch: SQL has {expected} placeholders but {provided} provided. SQL Statement: {sql}")]
    ParameterMismatchError {
        expected: usize,
        provided: usize,
        sql: String,
    },

    #[error("No table provided in SQL Statement: {sql}")]
    NoTableError { sql: String },

    #[error("Statement Error: {reason}")]
    StatementError { reason: String },

    #[error("Failed to prepare statement: {reason}")]
    PrepareError { reason: String },

    #[error("Database error: {reason}")]
    DatabaseError { reason: String },

    /// Ein Fehler ist während der Ausführung in der Datenbank aufgetreten.
    #[error("Execution error on table {table:?}: {reason} - SQL: {sql}")]
    ExecutionError {
        sql: String,
        reason: String,
        table: Option<String>,
    },
    /// Ein Fehler ist beim Verwalten der Transaktion aufgetreten.
    #[error("Transaction error: {reason}")]
    TransactionError { reason: String },

    /// Ein SQL-Statement wird vom Proxy nicht unterstützt.
    #[error("Unsupported statement. '{reason}'. - SQL: {sql}")]
    UnsupportedStatement { reason: String, sql: String },

    /// Fehler im HLC-Service
    #[error("HLC error: {reason}")]
    HlcError { reason: String },

    /// Fehler beim Sperren der Datenbankverbindung
    #[error("Lock error: {reason}")]
    LockError { reason: String },

    /// Fehler bei der Datenbankverbindung
    #[error("Connection error: {reason}")]
    ConnectionError { reason: String },

    /// Fehler bei der JSON-Serialisierung
    #[error("Serialization error: {reason}")]
    SerializationError { reason: String },

    /// Permission-bezogener Fehler für Extensions
    #[error("Permission error for extension '{extension_id}': {reason} (operation: {operation:?}, resource: {resource:?})")]
    PermissionError {
        extension_id: String,
        operation: Option<String>,
        resource: Option<String>,
        reason: String,
    },

    #[error("Query error: {reason}")]
    QueryError { reason: String },

    #[error("Row processing error: {reason}")]
    RowProcessingError { reason: String },

    #[error("Mutex Poisoned error: {reason}")]
    MutexPoisoned { reason: String },

    #[error("Datenbankverbindung fehlgeschlagen für Pfad '{path}': {reason}")]
    ConnectionFailed { path: String, reason: String },

    #[error("PRAGMA-Befehl '{pragma}' konnte nicht gesetzt werden: {reason}")]
    PragmaError { pragma: String, reason: String },

    #[error("Fehler beim Auflösen des Dateipfads: {reason}")]
    PathResolutionError { reason: String },

    #[error("Datei-I/O-Fehler für Pfad '{path}': {reason}")]
    IoError { path: String, reason: String },

    #[error("CRDT setup failed: {0}")]
    CrdtSetup(String),

    #[error("Migration error: {reason}")]
    MigrationError { reason: String },

    #[error("Vault '{vault_name}' already exists")]
    VaultAlreadyExists { vault_name: String },

    /// Another process already has this vault open — holding a `.lock` file
    /// on the vault DB path. Surface this as a distinct variant (rather than
    /// a generic IoError) so the UI can display a user-facing
    /// "vault already open in another window" message instead of a raw
    /// filesystem error.
    #[error("Vault at '{path}' is already open in another instance")]
    VaultAlreadyOpenElsewhere { path: String, reason: String },

    /// This process already has a vault mounted in AppState — typically
    /// because the caller forgot to invoke `close_database` before
    /// `create_encrypted_database` / `open_encrypted_database` for a
    /// different vault. Surfaced as a distinct variant so frontends and
    /// test fixtures can recover by calling `close_database` and retrying,
    /// instead of having to grep for substrings in a generic error reason.
    #[error("Cannot mount '{requested_path}': another vault ('{existing_path}') is still mounted in this process; close it first.")]
    VaultAlreadyMountedInProcess {
        existing_path: String,
        requested_path: String,
    },

    #[error("Validation error: {reason}")]
    ValidationError { reason: String },

    #[error("Limit exceeded: {reason}")]
    LimitExceeded { reason: String },

    /// A single CRDT transaction exceeded the maximum serialized size (ADR 0001).
    /// One `execute_with_crdt` call is exactly one transaction (one HLC), so this
    /// is enforced as a per-call write-size guard at that chokepoint.
    #[error("CRDT transaction too large: {bytes} bytes exceeds the {limit} byte limit; use file storage for large payloads")]
    TransactionTooLarge { bytes: usize, limit: usize },

    /// Shared-space integrity violation I1 (ADR 0002 §4b): a row was inserted
    /// into the share register `haex_shared_space_sync` whose `table_name`
    /// points at an internal `haex_*` (or `sqlite_*`) system table. The
    /// register may never target internal tables — reject the transaction.
    #[error("I1 integrity violation: share register may not target system table '{table}'")]
    I1RegisterTargetsSystemTable { table: String },

    /// Shared-space integrity violation I2 (ADR 0002 §4b / §6): the vault does
    /// not hold a signing key for the declared `space_id`, so it cannot
    /// legitimately author the share entry. Signing for a foreign space would
    /// let a foreign-authored row leak into it — reject the transaction.
    #[error("I2 integrity violation: vault has no signing key for space '{space_id}' — cannot author share")]
    I2ForeignShareInsert { space_id: String },

    /// The caller of `execute_with_crdt` tried to write to a CRDT meta column
    /// directly (the row-level HLC, column-HLC map, or column-signature map
    /// — see the constants in `crate::crdt::columns`). Those columns are
    /// managed exclusively by the CRDT transformer + the F1/F2 signing
    /// passes — a caller-supplied value would either be silently clobbered
    /// by the transformer (row-level HLC) or, worse, would feed a forged
    /// HLC/sig into the sig preimage (column-HLC map — sig-forgery
    /// vector). Reject the whole statement — no silent stripping.
    #[error("CRDT meta column write is forbidden: '{column}' is managed by the CRDT layer and must not be set by callers")]
    CrdtMetaColumnWriteForbidden { column: String },

    /// Task B.3 sign-on-write: a local INSERT into `haex_shared_space_sync`
    /// declared `authored_by_did = '{claimed}'`, but this vault's own
    /// signing key for `space_id` derives DID `{derived}`. Only the DID that
    /// comes out of the space's own key may author a registry row — a
    /// caller may never claim foreign authorship.
    #[error("registry row declares authoredByDid='{claimed}' but this vault's key for space '{space_id}' derives DID='{derived}' — cannot author as a foreign identity")]
    RegistryRowForeignAuthoredByDid {
        space_id: String,
        claimed: String,
        derived: String,
    },

    /// Task B.3 sign-on-write: a caller-issued UPDATE on
    /// `haex_shared_space_sync` tried to change `authored_by_did`.
    /// Authorship is immutable after the row is created — only the
    /// sign-on-write pass may populate it, once, on INSERT.
    #[error("registry row UPDATE on '{table}' may not change authoredByDid — authorship is immutable after creation")]
    RegistryRowAuthoredByDidImmutable { table: String },

    /// Task B.3 sign-on-write: a caller-issued write to
    /// `haex_shared_space_sync.row_sig` supplied a value directly. That
    /// column is derived exclusively by the sign-on-write pass from the
    /// row's 12 signed fields — a caller-supplied value could replay or
    /// forge a signature without holding the space's signing key.
    #[error("registry row_sig write is forbidden: '{column}' is derived by the sign-on-write pass and must not be set by callers")]
    RegistryRowSigColumnWriteForbidden { column: String },
}

impl From<rusqlite::Error> for DatabaseError {
    fn from(err: rusqlite::Error) -> Self {
        DatabaseError::DatabaseError {
            reason: err.to_string(),
        }
    }
}

impl From<String> for DatabaseError {
    fn from(reason: String) -> Self {
        DatabaseError::DatabaseError { reason }
    }
}

impl DatabaseError {
    /// Extract extension ID if this error is related to an extension
    pub fn extension_id(&self) -> Option<&str> {
        match self {
            DatabaseError::PermissionError { extension_id, .. } => Some(extension_id.as_str()),
            _ => None,
        }
    }

    /// Check if this is a permission-related error
    pub fn is_permission_error(&self) -> bool {
        matches!(self, DatabaseError::PermissionError { .. })
    }

    /// Get operation if available
    pub fn operation(&self) -> Option<&str> {
        match self {
            DatabaseError::PermissionError {
                operation: Some(op),
                ..
            } => Some(op.as_str()),
            _ => None,
        }
    }

    /// Get resource if available
    pub fn resource(&self) -> Option<&str> {
        match self {
            DatabaseError::PermissionError {
                resource: Some(res),
                ..
            } => Some(res.as_str()),
            _ => None,
        }
    }
}
