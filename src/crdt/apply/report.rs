//! Outcome counters returned by [`super::apply_remote_changes`]. Reset per
//! call — no cumulative state.
//!
//! The engine's write loop is skip-heavy by design: LWW losers, deletes that
//! shadow inserts, unknown tables or columns from schema-version skew — each
//! has its own counter so consumers can log the shape of a sync round and
//! tell "5 rows moved" apart from "5 rows dropped for schema drift".

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
    pub skipped_no_sync_column: usize,
    /// Column named something the crate owns the value of: one of its three
    /// structural metadata columns, or a primary key (row identity comes
    /// from `row_pks`). Unlike [`Self::skipped_no_sync_column`] this points
    /// at a badly broken peer or an attack, not at misconfiguration.
    pub skipped_reserved_column: usize,
    /// Delete-log entries whose target row is newer locally (resurrection
    /// check) and therefore NOT propagated into a DELETE.
    pub skipped_delete_target_newer: usize,
}
