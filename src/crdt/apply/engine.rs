//! Apply-remote-changes entry point (plan §4.2).
//!
//! One `apply_remote_changes` call has a preflight phase and one atomic write
//! transaction. The HLC advance happens after that transaction commits and
//! may therefore return an error after the batch has landed:
//!
//! ```text
//! preflight_batch(&changes, policy)?         // no transaction open yet
//! with_fk_disabled(conn):
//!     IMMEDIATE tx {
//!         disable triggers
//!         policy.begin(tx, &changes)
//!         for the delete-log groups, then the remaining groups
//!         (each phase loads the delete-shadow map first), HLC-ordered:
//!             core structural checks + eligibility classification
//!             policy.prepare_row(tx, row) -> RowDecision
//!             LWW-filter + same-batch-supersede the accepted columns
//!             delete-shadow check (insert path)
//!             INSERT (savepoint) or UPDATE (savepoint)
//!             policy.after_row(tx, written) on a successful write
//!         (after the delete-log groups: propagate inbound delete-log
//!          entries into target tables)
//!         policy.before_commit(tx, &changes, &outcome)
//!         enable triggers
//!         commit
//!     }
//!     hlc_service.advance_past_remote(max_accepted_hlc)
//! ```
//!
//! Every skip goes into an [`ApplyReport`] counter, and — since the
//! `ApplyPolicy` widening — into [`ApplyOutcome::skipped`] by original batch
//! index. No silent drops.

use std::collections::HashSet;
use std::str::FromStr;

use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde_json::Value as JsonValue;
use uhlc::Timestamp;

use crate::crdt::apply::delete_propagation::{
    load_delete_shadow_map, propagate_deleted_rows_to_target_tables, DeleteShadowMap,
};
use crate::crdt::apply::grouping::{
    build_pk_where_from_map, group_by_hlc_key, group_by_row_key_hlc_ordered,
};
use crate::crdt::apply::policy::ApplyPolicy;
use crate::crdt::apply::policy_types::{
    ConstraintDecision, IndexedChange, RowDecision, RowInput, RowWrite, WrittenColumn,
};
use crate::crdt::apply::preflight::preflight_batch;
use crate::crdt::apply::report::{ApplyOutcome, SkipReason, SkippedChange};
use crate::crdt::apply::row::{
    classify_eligibility, fetch_existing_hlcs, insert_shadowed, max_hlc, select_staged_columns,
    skip_whole_group, StagedColumn,
};
use crate::crdt::apply::write::{
    classify_insert_constraint, rollback_row_savepoint, write_insert, write_update, WriteOutcome,
};
use crate::crdt::cleanup::with_fk_disabled;
use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN,
};
use crate::crdt::hlc::{hlc_is_newer, HlcService};
use crate::crdt::scanner::ColumnChange;
use crate::crdt::trigger::get_table_schema;
use crate::db::core::ValueConverter;
use crate::db::error::DatabaseError;
use crate::error::{Error, Result};
use crate::signature::RemoteChanges;
use crate::table_names::TABLE_CRDT_CONFIGS;

/// Apply a batch of remote column changes atomically. See the module docs
/// for the sequence and the plan §4.2 trust contract, and `policy.rs` for
/// the extension points and their usage contract.
pub fn apply_remote_changes(
    conn: &mut Connection,
    changes: RemoteChanges,
    hlc_service: &HlcService,
    policy: &mut dyn ApplyPolicy,
) -> Result<ApplyOutcome> {
    // Every check that can refuse this batch runs here, to completion,
    // before a transaction exists — so a refusal provably wrote nothing.
    preflight_batch(&changes, policy)?;

    // Tag every change with its position in the original, unfiltered batch
    // before any reordering, so `ApplyOutcome::skipped` can name it later.
    let indexed: Vec<(usize, ColumnChange)> = changes.iter().cloned().enumerate().collect();
    let ordered_indexed: Vec<(usize, ColumnChange)> =
        group_by_hlc_key(indexed, |(_, c)| c.hlc_timestamp.as_str())
            .into_iter()
            .flat_map(|(_hlc, group)| group.into_iter())
            .collect();
    let row_groups = group_by_row_key_hlc_ordered(
        ordered_indexed,
        |(_, c)| (c.table_name.clone(), c.row_pks.clone()),
        |(_, c)| c.hlc_timestamp.as_str(),
    );

    let mut outcome = ApplyOutcome::default();
    // The clock must cover what landed, not what arrived — see the
    // module-level rationale retained from before this widening.
    let mut max_accepted_hlc: Option<String> = None;

    with_fk_disabled(conn, |conn| -> Result<()> {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        toggle_triggers(&tx, "0")?;
        policy.begin(&tx, &changes)?;

        // Delete-log rows land and fan out first, so a delete and an insert
        // reusing its UNIQUE value in one batch cannot collide; the shadow map
        // is reloaded afterwards so it covers the entries just written.
        let (log_groups, data_groups): (Vec<_>, Vec<_>) = row_groups
            .into_iter()
            .partition(|((table_name, _), _)| table_name == DELETED_ROWS_TABLE);

        let log_shadow = load_delete_shadow_map(&tx)?;
        for ((table_name, row_pks_str), group) in log_groups {
            process_row_group(
                &tx,
                policy,
                &table_name,
                &row_pks_str,
                group,
                &log_shadow,
                &mut outcome,
                &mut max_accepted_hlc,
            )?;
        }

        let inbound_delete_ids = collect_inbound_delete_log_ids(&changes, &outcome);
        let delete_targets = collect_inbound_delete_targets(&tx, &inbound_delete_ids)?;
        let shadow = load_delete_shadow_map(&tx)?;
        let (target_updates, remaining_groups): (Vec<_>, Vec<_>) = data_groups
            .into_iter()
            .partition(|((table_name, row_pks), _)| {
                canonical_row_key(table_name, row_pks)
                    .is_some_and(|key| delete_targets.contains(&key))
            });

        // Apply updates to rows targeted by this batch's deletes first. The
        // fan-out then sees their final row HLC and preserves a newer update,
        // including the row's unchanged columns. Other writes wait until the
        // deletes have released any UNIQUE values they may reuse.
        for ((table_name, row_pks_str), group) in target_updates {
            process_row_group(
                &tx,
                policy,
                &table_name,
                &row_pks_str,
                group,
                &shadow,
                &mut outcome,
                &mut max_accepted_hlc,
            )?;
        }

        propagate_deleted_rows_to_target_tables(&tx, &inbound_delete_ids, &mut outcome.report)?;

        let shadow = load_delete_shadow_map(&tx)?;
        for ((table_name, row_pks_str), group) in remaining_groups {
            process_row_group(
                &tx,
                policy,
                &table_name,
                &row_pks_str,
                group,
                &shadow,
                &mut outcome,
                &mut max_accepted_hlc,
            )?;
        }
        policy.before_commit(&tx, &changes, &outcome)?;
        toggle_triggers(&tx, "1")?;
        tx.commit()?;
        Ok(())
    })?;

    // Runs after the commit, so an `Err` here does not mean nothing landed —
    // see `Error::PostCommitClockAdvance`.
    if let Some(hlc) = max_accepted_hlc {
        if let Err(source) = hlc_service.advance_past_remote(&hlc) {
            return Err(Error::PostCommitClockAdvance {
                outcome: Box::new(outcome),
                source,
            });
        }
    }

    Ok(outcome)
}

fn collect_inbound_delete_targets(
    tx: &Transaction<'_>,
    delete_ids: &HashSet<String>,
) -> Result<HashSet<(String, String)>> {
    let mut targets = HashSet::new();
    for id in delete_ids {
        let target = tx.query_row(
            &format!("SELECT table_name, row_pks FROM \"{DELETED_ROWS_TABLE}\" WHERE id = ?1"),
            [id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        );
        let (table_name, row_pks) = match target {
            Ok(target) => target,
            Err(rusqlite::Error::QueryReturnedNoRows) => continue,
            Err(error) => return Err(Error::Sqlite(error)),
        };
        if let Some(key) = canonical_row_key(&table_name, &row_pks) {
            targets.insert(key);
        }
    }
    Ok(targets)
}

fn canonical_row_key(table_name: &str, row_pks: &str) -> Option<(String, String)> {
    let row_pks = serde_json::from_str::<serde_json::Map<String, JsonValue>>(row_pks).ok()?;
    Some((
        table_name.to_string(),
        serde_json::to_string(&row_pks).ok()?,
    ))
}

/// Sets the `triggers_enabled` gate every CRDT trigger consults to `value`
/// (`'0'` or `'1'`).
pub(crate) fn toggle_triggers(tx: &Transaction<'_>, value: &str) -> Result<()> {
    tx.execute(
        &format!(
            "INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value) \
             VALUES ('triggers_enabled', 'system', ?1) \
             ON CONFLICT(key) DO UPDATE SET value = ?1"
        ),
        [value],
    )?;
    Ok(())
}

/// Delete-log rows arrive as ordinary column changes into
/// [`DELETED_ROWS_TABLE`]; collect their `id`s after admission and writing.
/// Admitted replays that lost LWW still propagate the stored tombstone, but
/// changes rejected by the policy or core must not cause a target DELETE.
fn collect_inbound_delete_log_ids(
    changes: &[ColumnChange],
    outcome: &ApplyOutcome,
) -> HashSet<String> {
    let rejected: HashSet<usize> = outcome
        .skipped
        .iter()
        .filter(|skipped| {
            !matches!(
                skipped.reason,
                SkipReason::Stale | SkipReason::SupersededInBatch
            )
        })
        .map(|skipped| skipped.input_index)
        .collect();
    let mut ids: HashSet<String> = HashSet::new();
    for (input_index, change) in changes.iter().enumerate() {
        if change.table_name != DELETED_ROWS_TABLE || rejected.contains(&input_index) {
            continue;
        }
        if let Ok(map) = serde_json::from_str::<serde_json::Map<String, JsonValue>>(&change.row_pks)
        {
            if let Some(JsonValue::String(id)) = map.get("id") {
                ids.insert(id.clone());
            }
        }
    }
    ids
}

#[allow(clippy::too_many_arguments)]
fn process_row_group(
    tx: &Transaction<'_>,
    policy: &mut dyn ApplyPolicy,
    table_name: &str,
    row_pks_str: &str,
    group: Vec<(usize, ColumnChange)>,
    shadow: &DeleteShadowMap,
    outcome: &mut ApplyOutcome,
    max_accepted_hlc: &mut Option<String>,
) -> Result<()> {
    let schema = get_table_schema(tx, table_name)?;
    if schema.is_empty() {
        skip_whole_group(outcome, &group, SkipReason::MissingTable);
        return Ok(());
    }
    if !schema.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN)
        || !schema.iter().any(|c| c.name == COLUMN_HLCS_COLUMN)
    {
        skip_whole_group(outcome, &group, SkipReason::MissingCrdtMetadata);
        return Ok(());
    }

    let row_pks: serde_json::Map<String, JsonValue> = match serde_json::from_str(row_pks_str) {
        Ok(m) => m,
        Err(_) => {
            skip_whole_group(outcome, &group, SkipReason::InvalidRowIdentity);
            return Ok(());
        }
    };

    let expected_pks: HashSet<&str> = schema
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.as_str())
        .collect();
    let provided_pks: HashSet<&str> = row_pks.keys().map(|k| k.as_str()).collect();
    if expected_pks.is_empty() || expected_pks != provided_pks {
        skip_whole_group(outcome, &group, SkipReason::InvalidRowIdentity);
        return Ok(());
    }

    let (where_clause, pk_values) = match build_pk_where_from_map(&row_pks) {
        Some(parts) => parts,
        None => {
            skip_whole_group(outcome, &group, SkipReason::InvalidRowIdentity);
            return Ok(());
        }
    };
    // A PK value that cannot be bound (a malformed `{"$blob_hex":…}` tag)
    // names no row, the same as unparseable `row_pks`.
    if ValueConverter::convert_params(&pk_values).is_err() {
        skip_whole_group(outcome, &group, SkipReason::InvalidRowIdentity);
        return Ok(());
    }

    let existing = fetch_existing_hlcs(tx, table_name, &where_clause, &pk_values)?;
    let row_exists = existing.is_some();
    let (current_row_hlc, column_hlcs) =
        existing.unwrap_or_else(|| (String::new(), serde_json::Map::new()));

    let existing_columns: HashSet<&str> = schema.iter().map(|c| c.name.as_str()).collect();
    let has_sigs_column = existing_columns.contains(COLUMN_SIGS_COLUMN);

    let indexed_changes: Vec<IndexedChange<'_>> = group
        .iter()
        .map(|(input_index, change)| IndexedChange {
            input_index: *input_index,
            change,
        })
        .collect();

    let mut eligible_indices: Vec<usize> = Vec::new();
    for (i, ic) in indexed_changes.iter().enumerate() {
        match classify_eligibility(&ic.change.column_name, &existing_columns, &expected_pks) {
            Ok(()) => eligible_indices.push(i),
            Err(reason) => {
                match reason {
                    SkipReason::UnknownColumn => outcome.report.skipped_unknown_column += 1,
                    SkipReason::ReservedColumn => outcome.report.skipped_reserved_column += 1,
                    SkipReason::NoSyncColumn => outcome.report.skipped_no_sync_column += 1,
                    _ => unreachable!("classify_eligibility only returns the three column reasons"),
                }
                outcome.skipped.push(SkippedChange {
                    input_index: ic.input_index,
                    reason,
                });
            }
        }
    }

    let row_input = RowInput {
        table_name,
        row_pks_json: row_pks_str,
        row_pks: &row_pks,
        schema: &schema,
        exists: row_exists,
        changes: &indexed_changes,
        eligible_indices: &eligible_indices,
    };

    let decision = policy.prepare_row(tx, row_input)?;
    let decisions = match decision {
        RowDecision::Skip => {
            outcome.report.skipped_policy += eligible_indices.len();
            for &i in &eligible_indices {
                outcome.skipped.push(SkippedChange {
                    input_index: indexed_changes[i].input_index,
                    reason: SkipReason::Policy,
                });
            }
            return Ok(());
        }
        RowDecision::Columns(decisions) => decisions,
    };

    let staged = select_staged_columns(
        table_name,
        decisions,
        &row_input,
        &column_hlcs,
        &mut outcome.report,
        &mut outcome.skipped,
    )?;

    if staged.is_empty() {
        return Ok(());
    }

    let mut max_hlc_for_row = max_hlc(&staged);

    if !row_exists && insert_shadowed(table_name, &row_pks, &max_hlc_for_row, shadow) {
        outcome.report.skipped_shadowed_by_delete += staged.len();
        for s in &staged {
            outcome.skipped.push(SkippedChange {
                input_index: s.input_index,
                reason: SkipReason::ShadowedByDelete,
            });
        }
        return Ok(());
    }

    if row_exists && hlc_is_newer(&current_row_hlc, &max_hlc_for_row) {
        max_hlc_for_row = current_row_hlc.clone();
    }

    let mut column_hlcs_after = column_hlcs.clone();
    for s in &staged {
        column_hlcs_after.insert(
            s.change.column_name.clone(),
            JsonValue::String(s.change.hlc_timestamp.clone()),
        );
    }
    let column_hlcs_json = serde_json::to_string(&column_hlcs_after).map_err(|source| {
        DatabaseError::SerializationError {
            context: "column HLCs".to_string(),
            source,
        }
    })?;

    let write_outcome = if row_exists {
        write_update(
            tx,
            table_name,
            &staged,
            &column_hlcs_json,
            &max_hlc_for_row,
            &where_clause,
            &pk_values,
            has_sigs_column,
        )?
    } else {
        write_insert(
            tx,
            table_name,
            &schema,
            &row_pks,
            &staged,
            &column_hlcs_json,
            &max_hlc_for_row,
            has_sigs_column,
        )?
    };

    match write_outcome {
        WriteOutcome::Written => {
            let written_columns = build_written_columns(&staged);
            let row_write = RowWrite {
                table_name,
                row_pks_json: row_pks_str,
                row_pks: &row_pks,
                schema: &schema,
                columns: &written_columns,
                row_hlc: &max_hlc_for_row,
            };
            policy.after_row(tx, row_write)?;

            // Only now — write and `after_row` both succeeded — does this
            // row's HLCs enter local state and the clock.
            outcome.report.applied += staged.len();
            for s in &staged {
                fold_max_accepted_hlc(max_accepted_hlc, &s.change.hlc_timestamp);
            }
        }
        WriteOutcome::SqlFailure(error) => {
            let constraint_reason = if !row_exists {
                classify_insert_constraint(&error)
            } else {
                None
            };
            let Some(reason) = constraint_reason else {
                return Err(Error::Sqlite(error));
            };

            rollback_row_savepoint(tx)?;
            let written_columns = build_written_columns(&staged);
            let attempted = RowWrite {
                table_name,
                row_pks_json: row_pks_str,
                row_pks: &row_pks,
                schema: &schema,
                columns: &written_columns,
                row_hlc: &max_hlc_for_row,
            };
            match policy.on_insert_constraint(tx, attempted, &error)? {
                ConstraintDecision::SkipRow => {
                    outcome.report.skipped_insert_constraint += staged.len();
                    for s in &staged {
                        outcome.skipped.push(SkippedChange {
                            input_index: s.input_index,
                            reason,
                        });
                    }
                }
                ConstraintDecision::Abort => {
                    return Err(Error::Sqlite(error));
                }
            }
        }
    }

    Ok(())
}

fn build_written_columns<'a>(staged: &'a [StagedColumn<'a>]) -> Vec<WrittenColumn<'a>> {
    staged
        .iter()
        .map(|s| WrittenColumn {
            input_index: s.input_index,
            change: s.change,
            value: &s.value,
        })
        .collect()
}

fn fold_max_accepted_hlc(max_accepted_hlc: &mut Option<String>, hlc: &str) {
    let incoming =
        Timestamp::from_str(hlc).expect("preflight must reject malformed full HLC timestamps");
    let is_newer = match max_accepted_hlc.as_deref() {
        Some(current) => {
            let current = Timestamp::from_str(current)
                .expect("max_accepted_hlc must contain a valid HLC timestamp");
            incoming > current
        }
        None => true,
    };
    if is_newer {
        *max_accepted_hlc = Some(hlc.to_string());
    }
}
