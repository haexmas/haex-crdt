use std::time::Duration;
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
    /// recorded in `haex_hlc_state` on first open. Recovery is the consumer's
    /// decision — this crate never silently rewrites HLC state.
    #[error("device id mismatch: recorded {expected}, supplied {supplied}")]
    DeviceIdMismatch { expected: Uuid, supplied: Uuid },

    // ---- signature contract (plan §4.2) --------------------------------------
    /// `apply_remote_changes` preflight verification found a change whose
    /// signature does not verify. Preflight completes before the apply
    /// transaction is opened, so no write was ever attempted — nothing to
    /// roll back, and nothing of this batch is in local state.
    /// `first_failed_change` names the offending column change, by its
    /// index in the batch as submitted, so the caller can diagnose the
    /// source.
    #[error("signature verification failed at change #{first_failed_change}")]
    SignatureVerificationFailed { first_failed_change: usize },

    /// A `NoopSignatureProvider` was asked to verify a non-empty signature.
    /// The no-op provider accepts only empty signatures; a non-empty payload
    /// implies the peer signed with a real provider and the local vault must
    /// upgrade to a real provider before applying.
    #[error("unexpected non-empty signature under NoopSignatureProvider")]
    UnexpectedSignatureUnderNoop,

    // ---- remote clock drift contract (plan §4.2) -----------------------------
    /// A change in an `apply_remote_changes` batch carried an HLC timestamp
    /// more than [`crate::MAX_REMOTE_HLC_DRIFT`] beyond local now. A
    /// timestamp that far ahead is not a clock reading, so the whole batch
    /// is refused — refused *before* the transaction opens, so no part of it
    /// landed and the caller may retry or quarantine it wholesale.
    ///
    /// Whole-batch rather than per-change on purpose: partial application
    /// would leave the caller unable to say what its local state now
    /// contains. It therefore outranks the write loop's skip-don't-reject
    /// rule, which keeps a single unusable *column* from costing a batch —
    /// an unusable *clock* invalidates the batch's entire LWW ordering, not
    /// one column of it.
    ///
    /// `hlc` names the offending timestamp and `drift` how far beyond
    /// `limit` it lay, so a consumer can quarantine the batch and tell the
    /// user which peer's clock to look at.
    #[error("remote HLC `{hlc}` lies {drift:?} beyond local now, past the {limit:?} tolerance; the batch was refused before any write")]
    RemoteHlcDriftTooLarge {
        hlc: String,
        drift: Duration,
        limit: Duration,
    },

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

    /// A legacy CRDT table or metadata column could not be migrated without
    /// risking data loss. Opening stops before the current schema is created
    /// so the consumer can resolve the conflict explicitly.
    #[error("legacy CRDT schema is incompatible: {reason}")]
    MigrationCompatibility { reason: String },

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
