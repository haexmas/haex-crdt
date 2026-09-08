//! The transaction-aware extension point for `apply_remote_changes`.
//!
//! # What this trait is for
//!
//! The crate keeps owning every generic merge decision: LWW, row identity,
//! delete-log shadow/resurrection/propagation, the SQL writers, the
//! reserved-column and `_no_sync` guards, HLC drift, and the local clock
//! advance. `ApplyPolicy` is where a consumer plugs in the parts that are
//! legitimately its own business: selective per-row admission (a
//! DB-dependent authorization or membership check), exact value decoding
//! from whatever wire encoding it uses, its own signature-metadata shape,
//! and work that must land atomically with the merge (recovery markers,
//! consumer-owned deletes, cursor updates).
//!
//! The callback only ever sees already-parsed [`crate::ColumnChange`] data
//! and a raw `&Transaction`. The crate gains no concept of space, registry,
//! membership, or any other consumer-specific notion by having this trait —
//! those concepts live entirely in whatever a policy implementation chooses
//! to do with the transaction access it is given.
//!
//! # Trust contract — read before implementing
//!
//! **A policy is trusted application code, not a security sandbox.** Handing
//! it `&Transaction` permits arbitrary SQL against the whole database, not
//! just the row being processed. That access comes with rules:
//!
//! - A hook must **not** call `commit`, `rollback`, or otherwise end the
//!   transaction it was given — the engine owns the transaction's lifetime.
//! - A hook must **not** write the crate's own CRDT bookkeeping columns
//!   ([`crate::crdt::columns::HLC_TIMESTAMP_COLUMN`],
//!   [`crate::crdt::columns::COLUMN_HLCS_COLUMN`]) or tables (the delete-log,
//!   the CRDT config/dirty-tables bookkeeping) directly. It may write
//!   consumer-owned side tables and consumer-owned signature metadata (see
//!   [`super::SignatureWrite`] for the one column the crate does recognize
//!   generically).
//! - A hook must **not** re-enter `apply_remote_changes` — nested batches are
//!   not supported and will deadlock or corrupt state depending on the
//!   caller's connection setup.
//! - A hook must **not** invoke the consumer's local sign-on-write machinery.
//!   That is a different chokepoint (local writes, not remote merge) with
//!   different invariants; conflating the two would let a remote peer's
//!   input drive locally-authoritative signing.
//! - No network calls, event emission, or other externally-visible /
//!   irreversible action belongs in a hook. Every hook may run multiple
//!   times across a retried call, and any hook's effects are discarded if
//!   the batch later aborts — irreversible side effects would then have
//!   fired for work that never landed.
//!
//! # What the policy cannot do
//!
//! A policy only ever accepts or skips what the core has already identified
//! as eligible for a row — see [`super::RowInput::eligible_indices`]. It
//! cannot supply a new input index, a new column name, a new PK, or a new
//! HLC; it cannot bypass the reserved-column or `_no_sync` guards. The core
//! still runs LWW selection, same-batch supersession, and the delete-shadow
//! check on whatever a policy accepts — acceptance in `prepare_row` is
//! necessary, not sufficient, for a column to actually be written.

use rusqlite::Transaction;

use crate::crdt::apply::report::ApplyOutcome;
use crate::error::Result;
use crate::signature::RemoteChanges;

use super::policy_types::{ConstraintDecision, RowDecision, RowInput, RowWrite};

/// The transaction-aware extension point for `apply_remote_changes`. See the
/// module docs for the trust contract every implementation must honor.
pub trait ApplyPolicy {
    /// Whole-batch policy, run once, after the core's structural preflight
    /// (identifier safety, HLC validity and drift) and before any
    /// transaction is opened. An `Err` here means nothing was written — see
    /// [`crate::crdt::apply::preflight_batch`].
    ///
    /// No `&Connection` is available: this runs before the transaction
    /// exists, so a DB-dependent check (anything that needs to read
    /// persisted state) belongs in [`Self::prepare_row`] instead.
    fn preflight(&mut self, changes: &RemoteChanges) -> Result<()>;

    /// Runs once per call, right after the transaction opens and trigger
    /// state is disabled, before the per-row loop starts. The place for
    /// schema preparation (e.g. installing missing CRDT columns/triggers) or
    /// for computing batch-scoped state once rather than per row.
    ///
    /// Default: no-op. A missing table is not this hook's problem to fix —
    /// the row loop classifies it as `MissingTable` / `MissingCrdtMetadata`
    /// on its own; if a policy's own repair attempt here fails, remembering
    /// that and skipping every row for the affected table from
    /// [`Self::prepare_row`] is the policy's responsibility, not a case the
    /// core special-cases.
    fn begin(&mut self, tx: &Transaction<'_>, changes: &RemoteChanges) -> Result<()> {
        let _ = (tx, changes);
        Ok(())
    }

    /// Selective admission and exact value decoding for one row. Called once
    /// per `(table, row_pks)` group in the batch, after the core's own
    /// structural checks (schema exists and carries both CRDT metadata
    /// columns, the PK map matches exactly, per-column
    /// reserved/`_no_sync`/unknown classification) have already run.
    ///
    /// Never LWW: whatever this returns is only a candidate. The core still
    /// compares each accepted column's HLC against the stored map, resolves
    /// same-batch duplicates, and applies the delete-shadow check before a
    /// single byte is written.
    fn prepare_row(&mut self, tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision>;

    /// Runs after a row's value write has actually succeeded (not merely
    /// staged) — the place to merge the winning columns into a policy's own
    /// signature-metadata storage, using only the columns genuinely
    /// written, never ones only proposed in `prepare_row`.
    ///
    /// Default: no-op.
    fn after_row(&mut self, tx: &Transaction<'_>, written: RowWrite<'_>) -> Result<()> {
        let _ = (tx, written);
        Ok(())
    }

    /// Runs only for a NOT NULL or UNIQUE violation raised by the row's own
    /// INSERT statement, after that INSERT's savepoint has already been
    /// rolled back. Any other SQL error always aborts the batch regardless
    /// of this hook.
    ///
    /// Default: [`ConstraintDecision::Abort`].
    fn on_insert_constraint(
        &mut self,
        tx: &Transaction<'_>,
        attempted: RowWrite<'_>,
        error: &rusqlite::Error,
    ) -> Result<ConstraintDecision> {
        let _ = (tx, attempted, error);
        Ok(ConstraintDecision::Abort)
    }

    /// Runs once per call, after the generic owner-delete propagation and
    /// before trigger state is restored and the transaction commits. The
    /// place for a policy's own atomic-with-the-merge work: recovery
    /// markers keyed by `outcome.skipped`'s indexed detail, a policy's own
    /// additional deletes, cursor updates. Any later failure — including
    /// one from this hook — rolls back everything written so far in the
    /// same transaction.
    ///
    /// Default: no-op.
    fn before_commit(
        &mut self,
        tx: &Transaction<'_>,
        changes: &RemoteChanges,
        outcome: &ApplyOutcome,
    ) -> Result<()> {
        let _ = (tx, changes, outcome);
        Ok(())
    }
}
