use thiserror::Error;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

/// Which journal a migration lookup was reconciling against. Used by
/// `Error::MigrationMissingFromSource` so a caller can distinguish a
/// crate-owned bookkeeping migration from a consumer-owned schema migration
/// without inspecting the migration name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationJournal {
    /// `haex_crdt_migrations_no_sync` — CRDT bookkeeping migrations compiled into this crate.
    CrateOwned,
    /// `haex_app_migrations_no_sync` — schema migrations supplied by the consumer's `MigrationSource`.
    ConsumerOwned,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    #[error("uhlc error: {0}")]
    Hlc(String),

    // ---- device identity contract (plan §4.1) --------------------------------
    /// The `DeviceIdProvider` returned a `Uuid` that does not match the one
    /// recorded in [`crate::TABLE_CRDT_CONFIGS`] on first open. Recovery is the consumer's
    /// decision — this crate never silently rewrites HLC state.
    #[error("device id mismatch: recorded {expected}, supplied {supplied}")]
    DeviceIdMismatch { expected: Uuid, supplied: Uuid },

    // ---- signature contract (plan §4.2) --------------------------------------
    /// `apply_remote_changes` preflight verification found a change whose
    /// signature does not verify. The batch has been rolled back; no writes
    /// remain. `first_failed_change` names the offending column change so
    /// the caller can diagnose the source.
    #[error("signature verification failed at change #{first_failed_change}")]
    SignatureVerificationFailed { first_failed_change: usize },

    /// A `NoopSignatureProvider` was asked to verify a non-empty signature.
    /// The no-op provider accepts only empty signatures; a non-empty payload
    /// implies the peer signed with a real provider and the local vault must
    /// upgrade to a real provider before applying.
    #[error("unexpected non-empty signature under NoopSignatureProvider")]
    UnexpectedSignatureUnderNoop,

    // ---- migration contract (plan §4.3) --------------------------------------
    /// A migration named in the journal is not returned by the current
    /// `MigrationSource`. `journal` distinguishes the CRDT bookkeeping
    /// journal from the consumer's schema journal so a valid crate-owned
    /// migration is never wrongly reported as missing from a consumer source.
    #[error("migration `{name}` missing from {journal:?} source")]
    MigrationMissingFromSource {
        journal: MigrationJournal,
        name: String,
    },

    /// A migration's SQL content on disk differs from the SHA-256 digest
    /// stored in the journal when it was applied. Applied migration content
    /// is frozen; drift aborts open rather than re-running or silently
    /// continuing.
    #[error("migration `{name}` content drifted; expected {expected}, found {found}")]
    MigrationContentDrift {
        name: String,
        expected: String,
        found: String,
    },

    // ---- install_crdt contract (plan §6) -------------------------------------
    /// `install_crdt` refused to run because the three CRDT metadata columns
    /// are already present on the table. Pass
    /// `InstallCrdtOptions::allow_reinstall = true` to reinstall triggers
    /// without backfill.
    #[error("crdt already installed on `{table}`")]
    CrdtAlreadyInstalled { table: String },

    // ---- catch-all -----------------------------------------------------------
    #[error("{0}")]
    Message(String),
}

impl From<crate::db::error::DatabaseError> for Error {
    fn from(err: crate::db::error::DatabaseError) -> Self {
        // The db layer already renders a rich Display; keep the string here
        // so callers see the same message the db layer would surface.
        Error::Message(err.to_string())
    }
}
