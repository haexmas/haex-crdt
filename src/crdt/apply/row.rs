//! Row-selection logic: LWW winner selection among policy-accepted columns,
//! same-batch supersession, and the delete-shadow check. Split out of
//! `engine.rs` per the file-size cap — this module is the pure decision
//! logic; `engine.rs` still owns the structural checks that decide
//! *eligibility* and the actual SQL write that follows selection.

use std::collections::HashSet;

use rusqlite::types::Value as SqlValue;
use rusqlite::Transaction;
use serde_json::Value as JsonValue;

use crate::crdt::apply::delete_propagation::{insert_suppressed_by_deletes, DeleteShadowMap};
use crate::crdt::apply::policy_types::{ColumnDecision, RowInput, SignatureWrite};
use crate::crdt::apply::report::{ApplyOutcome, ApplyReport, SkipReason, SkippedChange};
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::hlc_is_newer;
use crate::crdt::scanner::ColumnChange;
use crate::db::error::DatabaseError;
use crate::error::{Error, Result};

/// One column that survived policy acceptance, LWW comparison, and
/// same-batch supersession — a winner about to be written.
pub(super) struct StagedColumn<'a> {
    pub input_index: usize,
    pub change: &'a ColumnChange,
    pub value: SqlValue,
    pub signature: SignatureWrite,
}

/// Validate a policy's [`super::RowDecision::Columns`] length against
/// `eligible_indices`, then run LWW selection (against the stored
/// `column_hlcs`) and same-batch supersession (last-in-`changes`-order
/// wins) over the accepted columns. Populates `outcome`'s skip detail and
/// counters for every column that does not survive. Returns the surviving
/// columns in `changes` order.
#[allow(clippy::too_many_arguments)]
pub(super) fn select_staged_columns<'a>(
    table_name: &str,
    decisions: Vec<ColumnDecision>,
    row: &RowInput<'a>,
    column_hlcs: &serde_json::Map<String, JsonValue>,
    report: &mut ApplyReport,
    skipped: &mut Vec<SkippedChange>,
) -> Result<Vec<StagedColumn<'a>>> {
    if decisions.len() != row.eligible_indices.len() {
        return Err(DatabaseError::ValidationError {
            reason: format!(
                "policy returned {} column decisions for {} eligible columns in table '{}'",
                decisions.len(),
                row.eligible_indices.len(),
                table_name
            ),
        }
        .into());
    }

    let mut staged: Vec<StagedColumn<'a>> = Vec::new();
    for (decision, &changes_idx) in decisions.into_iter().zip(row.eligible_indices.iter()) {
        let indexed_change = &row.changes[changes_idx];
        let change = indexed_change.change;

        let (value, signature) = match decision {
            ColumnDecision::Skip => {
                report.skipped_policy += 1;
                skipped.push(SkippedChange {
                    input_index: indexed_change.input_index,
                    reason: SkipReason::Policy,
                });
                continue;
            }
            ColumnDecision::Accept { value, signature } => (value, signature),
        };

        let running_hlc: String = staged
            .iter()
            .find(|s| s.change.column_name == change.column_name)
            .map(|s| s.change.hlc_timestamp.clone())
            .unwrap_or_else(|| {
                column_hlcs
                    .get(&change.column_name)
                    .and_then(JsonValue::as_str)
                    .unwrap_or("")
                    .to_string()
            });

        // Preflight leaves incomplete timestamps to the skip path. A
        // numeric-only string can beat an empty stored HLC in the tolerant
        // comparator, but cannot be folded into the clock after writing.
        if !change.hlc_timestamp.contains('/') || !hlc_is_newer(&change.hlc_timestamp, &running_hlc)
        {
            let has_batch_competitor = staged
                .iter()
                .any(|s| s.change.column_name == change.column_name);
            let reason = if has_batch_competitor {
                report.skipped_superseded_in_batch += 1;
                SkipReason::SupersededInBatch
            } else {
                report.skipped_stale += 1;
                SkipReason::Stale
            };
            skipped.push(SkippedChange {
                input_index: indexed_change.input_index,
                reason,
            });
            continue;
        }

        if let Some(pos) = staged
            .iter()
            .position(|s| s.change.column_name == change.column_name)
        {
            let evicted = staged.remove(pos);
            report.skipped_superseded_in_batch += 1;
            skipped.push(SkippedChange {
                input_index: evicted.input_index,
                reason: SkipReason::SupersededInBatch,
            });
        }
        staged.push(StagedColumn {
            input_index: indexed_change.input_index,
            change,
            value,
            signature,
        });
    }

    Ok(staged)
}

/// The owner delete-shadow check for a fresh row: true if `staged`'s columns
/// must NOT be inserted because a delete-log entry for the same row carries
/// a shadowing HLC. Mirrors today's `apply_row` insert-path check exactly —
/// `max_hlc_for_row` is the maximum HLC among the surviving staged columns.
pub(super) fn insert_shadowed(
    table_name: &str,
    row_pks: &serde_json::Map<String, JsonValue>,
    max_hlc_for_row: &str,
    shadow: &DeleteShadowMap,
) -> bool {
    let empty: Vec<(serde_json::Map<String, JsonValue>, String)> = Vec::new();
    let candidates = shadow.get(table_name).unwrap_or(&empty);
    insert_suppressed_by_deletes(row_pks, max_hlc_for_row, candidates)
}

/// Maximum HLC among the staged columns — panics only if `staged` is empty,
/// which callers must guarantee (mirrors today's contract that this is only
/// computed once at least one column survived selection).
pub(super) fn max_hlc(staged: &[StagedColumn<'_>]) -> String {
    staged
        .iter()
        .map(|s| s.change.hlc_timestamp.as_str())
        .max_by(|a, b| crate::crdt::hlc::compare_hlc_strings(a, b))
        .expect("staged must be non-empty")
        .to_string()
}

/// One row's worth of skip bookkeeping, all attributed to the same reason —
/// used for the structural (pre-policy) skip sites where the whole group
/// shares one verdict.
pub(super) fn skip_whole_group(
    outcome: &mut ApplyOutcome,
    group: &[(usize, ColumnChange)],
    reason: SkipReason,
) {
    outcome.report.skipped_unknown_table += group.len();
    for (input_index, _) in group {
        outcome.skipped.push(SkippedChange {
            input_index: *input_index,
            reason,
        });
    }
}

/// Core per-column eligibility classification — the inbound mirror of the
/// scanner's `partition_columns`, unchanged from today's `apply_row`.
/// Reserved is checked first so the three metadata columns (which also end
/// in `_no_sync`) are not swallowed by the suffix check.
pub(super) fn classify_eligibility(
    column_name: &str,
    existing_columns: &HashSet<&str>,
    expected_pks: &HashSet<&str>,
) -> std::result::Result<(), SkipReason> {
    if !existing_columns.contains(column_name) {
        return Err(SkipReason::UnknownColumn);
    }
    if column_name == HLC_TIMESTAMP_COLUMN
        || column_name == COLUMN_HLCS_COLUMN
        || column_name == COLUMN_SIGS_COLUMN
        || expected_pks.contains(column_name)
    {
        return Err(SkipReason::ReservedColumn);
    }
    if column_name.ends_with("_no_sync") {
        return Err(SkipReason::NoSyncColumn);
    }
    Ok(())
}

/// `(row_level_hlc, parsed_column_hlcs)` for a row that already exists.
pub(super) type ExistingHlcs = Option<(String, serde_json::Map<String, JsonValue>)>;

pub(super) fn fetch_existing_hlcs(
    tx: &Transaction<'_>,
    table_name: &str,
    where_clause: &str,
    pk_values: &[JsonValue],
) -> Result<ExistingHlcs> {
    use crate::db::core::ValueConverter;

    let sql = format!(
        "SELECT {COLUMN_HLCS_COLUMN}, {HLC_TIMESTAMP_COLUMN} FROM \"{table_name}\" WHERE {where_clause}"
    );
    let sql_params = ValueConverter::convert_params(pk_values)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> = sql_params
        .iter()
        .map(|v| v as &dyn rusqlite::ToSql)
        .collect();
    let mut stmt = tx.prepare(&sql)?;
    match stmt.query_row(&*param_refs, |row| {
        let hlcs: Option<String> = row.get(0)?;
        let row_hlc: Option<String> = row.get(1)?;
        Ok((hlcs, row_hlc))
    }) {
        Ok((hlcs_str, row_hlc)) => Ok(Some((
            row_hlc.unwrap_or_default(),
            hlcs_str
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default(),
        ))),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(Error::Sqlite(e)),
    }
}
