//! Outcome counters returned by [`super::apply_remote_changes`]. Reset per
//! call — no cumulative state.
//!
//! The engine's write loop is skip-heavy by design: LWW losers, deletes that
//! shadow inserts, unknown tables or columns from schema-version skew — each
//! has its own counter so consumers can log the shape of a sync round and
//! tell "5 rows moved" apart from "5 rows dropped for schema drift".
//!
//! [`ApplyOutcome`] wraps [`ApplyReport`] with per-change detail: every
//! skipped [`crate::ColumnChange`] is named by its index in the original,
//! unfiltered batch, plus why it was skipped. The counters stay the
//! log-friendly aggregate; `skipped` is the new indexed detail a consumer
//! needs to correlate a skip back to its own bookkeeping (e.g. a recovery
//! marker keyed by row identity).

/// Per-call apply outcome. All fields count individual `ColumnChange` records
/// unless noted otherwise.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Column writes actually applied (INSERT or UPDATE landed a value).
    pub applied: usize,
    /// LWW losers: incoming HLC was not strictly newer than the stored HLC
    /// for the same column, so the local value stays.
    pub skipped_stale: usize,
    /// Inserts shadowed by a delete-log entry at the same or newer HLC —
    /// applying would resurrect a deleted row, so we skip.
    pub skipped_shadowed_by_delete: usize,
    /// Column referred to a table not present in the local schema (typical
    /// during a rolling upgrade where a newer peer writes into a table this
    /// consumer hasn't installed yet).
    pub skipped_unknown_table: usize,
    /// Column not present in the local table's schema (rolling upgrade where
    /// a newer peer's schema has extra columns). Checked before the two
    /// counters below, so a `_no_sync` or reserved name that is absent
    /// locally is counted here rather than there.
    pub skipped_unknown_column: usize,
    /// Column carried a `_no_sync` name, which the local scanner would never
    /// ship (a misconfigured peer, or one predating the column-level rule).
    /// The crate's own metadata columns carry that suffix too but are
    /// counted under [`Self::skipped_reserved_column`], which is checked
    /// first.
    pub skipped_no_sync_column: usize,
    /// Column named something the crate owns the value of: one of its three
    /// structural metadata columns, or a primary key (row identity comes
    /// from `row_pks`). Unlike [`Self::skipped_no_sync_column`] this points
    /// at a badly broken peer or an attack, not at misconfiguration.
    pub skipped_reserved_column: usize,
    /// Delete-log entries whose target row is newer locally (resurrection
    /// check) and therefore NOT propagated into a DELETE.
    pub skipped_delete_target_newer: usize,
    /// Row- or column-level skip decided by the [`super::ApplyPolicy`]:
    /// [`super::RowDecision::Skip`] (whole row, every still-eligible column)
    /// or [`super::ColumnDecision::Skip`] (one column).
    pub skipped_policy: usize,
    /// INSERT failed a NOT NULL, UNIQUE, or PRIMARY KEY constraint and the
    /// policy's [`super::ApplyPolicy::on_insert_constraint`] returned
    /// [`super::ConstraintDecision::SkipRow`]. See [`SkipReason::InsertNotNull`]
    /// / [`SkipReason::InsertUnique`] / [`SkipReason::InsertPrimaryKey`] in
    /// `skipped` for which kind.
    pub skipped_insert_constraint: usize,
    /// A column-accepting decision lost to a *later* change on the same
    /// column within the same call (last-in-HLC-order wins). Distinct from
    /// [`Self::skipped_stale`], which counts a loss against the value already
    /// persisted from a previous call.
    pub skipped_superseded_in_batch: usize,
}

/// [`ApplyReport`] plus the indexed detail a consumer needs to correlate a
/// skip back to its own bookkeeping. Additive over the plain counters, not a
/// replacement — `report` is unchanged from what `apply_remote_changes`
/// returned before this type existed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub report: ApplyReport,
    pub skipped: Vec<SkippedChange>,
}

/// One skipped `ColumnChange`, named by its position in the original,
/// unfiltered batch passed to `apply_remote_changes` — not the position
/// after any internal reordering or filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkippedChange {
    pub input_index: usize,
    pub reason: SkipReason,
}

/// Why one `ColumnChange` did not land. See [`ApplyReport`]'s fields for the
/// aggregate counter each reason folds into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// The table does not exist locally.
    MissingTable,
    /// The table exists but does not carry both CRDT metadata columns.
    MissingCrdtMetadata,
    /// The row's PK map failed to parse, or did not name exactly the
    /// table's PK columns.
    InvalidRowIdentity,
    /// The column is not present in the local table's schema.
    UnknownColumn,
    /// The column is one the crate owns the value of (metadata column or a
    /// PK) and a peer may never set directly.
    ReservedColumn,
    /// The column carries a `_no_sync` name, which never ships.
    NoSyncColumn,
    /// Lost LWW against the value already persisted from a previous call.
    Stale,
    /// Lost LWW against a later change to the same column within this call.
    SupersededInBatch,
    /// An insert was shadowed by a delete-log entry at the same or newer
    /// HLC.
    ShadowedByDelete,
    /// The [`super::ApplyPolicy`] skipped this row or column.
    Policy,
    /// INSERT failed a NOT NULL constraint and the policy chose to skip the
    /// row rather than abort the batch.
    InsertNotNull,
    /// INSERT failed a UNIQUE constraint and the policy chose to skip the
    /// row rather than abort the batch.
    InsertUnique,
    /// INSERT failed a PRIMARY KEY constraint — the row's own identity
    /// already exists — and the policy chose to skip the row rather than
    /// abort the batch. Distinct from [`Self::InsertUnique`]: a PK collision
    /// means this exact row already exists, while a UNIQUE collision means a
    /// different row already claims some other business-unique value.
    /// SQLite renders both with the same message text ("UNIQUE constraint
    /// failed: ..."); only the extended error code tells them apart.
    InsertPrimaryKey,
}
