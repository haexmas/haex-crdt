//! Read-only contexts and decision types passed across the [`super::ApplyPolicy`]
//! boundary. These carry no behavior of their own — see `policy.rs` for the
//! trait and its usage contract.

use serde_json::Value as JsonValue;

use crate::crdt::scanner::ColumnChange;
use crate::crdt::trigger::ColumnInfo;

/// One change from the original batch, still at its original position.
pub struct IndexedChange<'a> {
    /// Index into the original, unfiltered batch passed to
    /// `apply_remote_changes` — stable identity for correlating a decision
    /// or a skip back to the caller's own record of the batch.
    pub input_index: usize,
    pub change: &'a ColumnChange,
}

/// Everything [`super::ApplyPolicy::prepare_row`] needs to decide one row.
///
/// `changes` is the row's **full** original change group — including columns
/// the core has already classified as ineligible (unknown, reserved,
/// `_no_sync`) — so a row-level gate can inspect everything the peer sent,
/// even columns it will never be allowed to write. `eligible_indices` names
/// which entries of `changes` the core has cleared for consideration; a
/// [`RowDecision::Columns`] response must supply exactly one
/// [`ColumnDecision`] per entry in `eligible_indices`, in the same order.
#[derive(Clone, Copy)]
pub struct RowInput<'a> {
    pub table_name: &'a str,
    /// Original bytes of the row's PK JSON — do not silently reserialize.
    pub row_pks_json: &'a str,
    pub row_pks: &'a serde_json::Map<String, JsonValue>,
    pub schema: &'a [ColumnInfo],
    /// Whether the row already exists locally (decides INSERT vs UPDATE).
    pub exists: bool,
    pub changes: &'a [IndexedChange<'a>],
    /// Indices into `changes` (not `input_index`) that the core has cleared
    /// for consideration.
    pub eligible_indices: &'a [usize],
}

/// A policy's verdict for one row.
pub enum RowDecision {
    /// Drop the whole row. Every entry named by `eligible_indices` is
    /// recorded as [`crate::crdt::apply::SkipReason::Policy`] unless the core
    /// already classified it otherwise.
    Skip,
    /// Admit the row for further (core) selection. Must contain exactly one
    /// [`ColumnDecision`] per entry in `eligible_indices`, in the same order
    /// — a mismatched length is rejected with an error.
    Columns(Vec<ColumnDecision>),
}

/// A policy's verdict for one eligible column.
pub enum ColumnDecision {
    /// Drop this column only; the rest of the row's decisions still apply.
    Skip,
    /// Admit this column's decoded value for LWW selection. Acceptance here
    /// is not final — the core still runs LWW comparison, same-batch
    /// supersession, and the delete-shadow check before anything is
    /// written.
    Accept {
        value: rusqlite::types::Value,
        signature: SignatureWrite,
    },
}

/// How a column's entry in the crate's signature-map column should be
/// written, if that column exists locally. Meaningless if the local schema
/// has no signature-map column.
pub enum SignatureWrite {
    /// Preserve the column's existing signature-map entry unchanged (empty
    /// on a fresh INSERT, since there is nothing to keep). A policy that
    /// owns a different metadata shape entirely uses this and writes its own
    /// storage itself, e.g. in `after_row`.
    Keep,
    /// `Some` replaces the column's entry; `None` removes it.
    Replace(Option<JsonValue>),
}

/// One column actually written by the core, handed to
/// [`super::ApplyPolicy::after_row`] / `on_insert_constraint`.
pub struct WrittenColumn<'a> {
    pub input_index: usize,
    pub change: &'a ColumnChange,
    /// The value that was written (or, for `on_insert_constraint`, attempted).
    pub value: &'a rusqlite::types::Value,
}

/// A row's write, after core selection. Passed to `after_row` once the write
/// has actually succeeded, or to `on_insert_constraint` for the write that
/// was attempted and rolled back.
pub struct RowWrite<'a> {
    pub table_name: &'a str,
    pub row_pks_json: &'a str,
    pub row_pks: &'a serde_json::Map<String, JsonValue>,
    pub schema: &'a [ColumnInfo],
    pub columns: &'a [WrittenColumn<'a>],
    pub row_hlc: &'a str,
}

/// A policy's verdict after a NOT NULL / UNIQUE INSERT failure.
pub enum ConstraintDecision {
    /// Drop the row (recorded as `SkipReason::InsertNotNull` /
    /// `InsertUnique`) and continue with the rest of the batch.
    SkipRow,
    /// Abort the whole batch, surfacing the original SQL error.
    Abort,
}
