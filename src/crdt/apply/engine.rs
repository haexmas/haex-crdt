//! Apply-remote-changes entry point (plan §4.2).
//!
//! One `apply_remote_changes` call is one all-or-nothing sync round:
//!
//! ```text
//! with_fk_disabled(conn):
//!     IMMEDIATE tx {
//!         provider.on_before_apply(&changes)?
//!         verify_all_signatures(&changes, provider)?
//!         disable triggers
//!         load delete-shadow map
//!         for each (table, row) in HLC-ordered groups:
//!             LWW-filter columns
//!             INSERT (with resurrection guard) or UPDATE
//!         propagate inbound delete-log entries into target tables
//!         enable triggers
//!         commit
//!     }
//!     hlc_service.advance_past_remote(max_hlc)
//! ```
//!
//! Every skip goes into an [`ApplyReport`] counter — no silent drops.

use std::collections::HashSet;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, Transaction};
use serde_json::Value as JsonValue;

use crate::crdt::apply::delete_propagation::{
    insert_suppressed_by_deletes, load_delete_shadow_map, propagate_deleted_rows_to_target_tables,
};
use crate::crdt::apply::grouping::{
    build_pk_where_from_map, group_by_transaction_hlc, group_row_changes_in_hlc_order,
};
use crate::crdt::apply::preflight::verify_all_signatures;
use crate::crdt::apply::report::ApplyReport;
use crate::crdt::apply::write::{write_insert, write_update};
use crate::crdt::cleanup::with_fk_disabled;
use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN,
};
use crate::crdt::hlc::{hlc_is_newer, HlcService};
use crate::crdt::scanner::ColumnChange;
use crate::crdt::trigger::{get_table_schema, is_safe_identifier};
use crate::db::core::ValueConverter;
use crate::db::error::DatabaseError;
use crate::error::Result;
use crate::signature::{RemoteChanges, SignatureProvider};
use crate::table_names::TABLE_CRDT_CONFIGS;

/// Apply a batch of remote column changes atomically. See the module docs
/// for the sequence and the plan §4.2 trust contract.
pub fn apply_remote_changes(
    conn: &mut Connection,
    changes: RemoteChanges,
    hlc_service: &HlcService,
    provider: &dyn SignatureProvider,
) -> Result<ApplyReport> {
    // Pre-tx sanity: refuse identifier-unsafe input at the boundary so the
    // rest of the code can build SQL without re-checking.
    for change in &changes {
        if !is_safe_identifier(&change.table_name) {
            return Err(DatabaseError::ValidationError {
                reason: format!(
                    "Invalid table name '{}' in remote change",
                    change.table_name
                ),
            }
            .into());
        }
        if !is_safe_identifier(&change.column_name) {
            return Err(DatabaseError::ValidationError {
                reason: format!(
                    "Invalid column name '{}' in table '{}'",
                    change.column_name, change.table_name
                ),
            }
            .into());
        }
    }

    provider.on_before_apply(&changes)?;
    verify_all_signatures(&changes, provider)?;

    // Reorder for the write loop: transaction-HLC groups sorted ascending,
    // flattened back to a single vec. Same content, deterministic order.
    let ordered: Vec<ColumnChange> = group_by_transaction_hlc(changes)
        .into_iter()
        .flat_map(|(_hlc, group)| group.into_iter())
        .collect();
    let max_hlc: Option<String> = ordered.last().map(|c| c.hlc_timestamp.clone());

    let mut report = ApplyReport::default();
    with_fk_disabled(conn, |conn| -> std::result::Result<(), DatabaseError> {
        let tx = conn.transaction()?;
        toggle_triggers(&tx, "0")?;
        let shadow = load_delete_shadow_map(&tx)?;
        let inbound_delete_ids = collect_inbound_delete_log_ids(&ordered);

        for ((_table, row_pks_str), row_changes) in group_row_changes_in_hlc_order(ordered.clone())
        {
            apply_row(&tx, &row_pks_str, row_changes, &shadow, &mut report)?;
        }

        propagate_deleted_rows_to_target_tables(&tx, &inbound_delete_ids, &mut report)?;
        toggle_triggers(&tx, "1")?;
        tx.commit()?;
        Ok(())
    })?;

    if let Some(hlc) = max_hlc {
        hlc_service
            .advance_past_remote(&hlc)
            .map_err(|e| DatabaseError::HlcError {
                reason: e.to_string(),
            })?;
    }

    Ok(report)
}

fn toggle_triggers(tx: &Transaction<'_>, value: &str) -> std::result::Result<(), DatabaseError> {
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
/// [`DELETED_ROWS_TABLE`]; collect their `id`s so the post-loop propagation
/// pass knows which target rows to fan out to.
fn collect_inbound_delete_log_ids(changes: &[ColumnChange]) -> HashSet<String> {
    let mut ids: HashSet<String> = HashSet::new();
    for change in changes {
        if change.table_name != DELETED_ROWS_TABLE {
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

fn apply_row(
    tx: &Transaction<'_>,
    row_pks_str: &str,
    row_changes: Vec<ColumnChange>,
    shadow: &crate::crdt::apply::delete_propagation::DeleteShadowMap,
    report: &mut ApplyReport,
) -> std::result::Result<(), DatabaseError> {
    let first = &row_changes[0];
    let table_name = first.table_name.clone();

    let schema = get_table_schema(tx, &table_name).map_err(DatabaseError::from)?;
    if schema.is_empty() {
        report.skipped_unknown_table += row_changes.len();
        return Ok(());
    }
    // A CRDT-managed table must carry the two core metadata columns; a plain
    // table without them cannot participate in LWW and is treated as unknown
    // for reporting purposes (per plan §3, consumers install CRDT on their
    // tables before pointing sync at them).
    if !schema.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN)
        || !schema.iter().any(|c| c.name == COLUMN_HLCS_COLUMN)
    {
        report.skipped_unknown_table += row_changes.len();
        return Ok(());
    }

    let row_pks: serde_json::Map<String, JsonValue> = match serde_json::from_str(row_pks_str) {
        Ok(m) => m,
        Err(e) => {
            return Err(DatabaseError::SerializationError {
                reason: format!("Failed to parse row PKs: {e}"),
            });
        }
    };

    // A partial or extra PK map must never reach the SQL builders. A partial
    // map would broaden the WHERE clause; an extra key would target a column
    // that is not part of the row identity. Treat malformed remote identity
    // data as an unknown row and account for every skipped change.
    let expected_pks: HashSet<&str> = schema
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.as_str())
        .collect();
    let provided_pks: HashSet<&str> = row_pks.keys().map(|k| k.as_str()).collect();
    if expected_pks.is_empty() || expected_pks != provided_pks {
        report.skipped_unknown_table += row_changes.len();
        return Ok(());
    }

    let (where_clause, pk_values) = match build_pk_where_from_map(&row_pks) {
        Some(parts) => parts,
        None => {
            report.skipped_unknown_table += row_changes.len();
            return Ok(());
        }
    };

    let existing = fetch_existing_hlcs(tx, &table_name, &where_clause, &pk_values)?;
    let row_exists = existing.is_some();
    let (current_row_hlc, mut column_hlcs) =
        existing.unwrap_or_else(|| (String::new(), serde_json::Map::new()));

    let existing_columns: HashSet<&str> = schema.iter().map(|c| c.name.as_str()).collect();
    let has_sigs_column = existing_columns.contains(COLUMN_SIGS_COLUMN);

    let mut staged: Vec<(String, SqlValue, String, Option<JsonValue>)> = Vec::new();
    let mut max_hlc_for_row = first.hlc_timestamp.clone();
    for change in &row_changes {
        if !existing_columns.contains(change.column_name.as_str()) {
            report.skipped_unknown_column += 1;
            continue;
        }
        // Inbound mirror of the scanner's `partition_columns`: the set of
        // columns apply accepts from a peer is exactly the set the scanner
        // is willing to ship. That identity is the invariant — when the two
        // sides drift, a column that can never leave one device can still
        // be written on another.
        //
        // Both checks skip and count rather than fail the batch. Rejecting
        // would hand any peer a denial-of-service primitive: one poisoned
        // change per batch and the victim's sync stops entirely, which is
        // worse than the single write it prevents. Skipping degrades to
        // "that column never travels", which is what the rule promises
        // anyway, and the counters keep a broken peer diagnosable.
        if change.column_name.ends_with("_no_sync") {
            report.skipped_no_sync_column += 1;
            continue;
        }
        // Columns whose value the crate itself owns. Load-bearing, not
        // cosmetic: `write_insert` pushes the staged remote columns BEFORE
        // the crate's own, and SQLite takes the FIRST value for a column
        // named twice in an INSERT — so an unfiltered metadata column wins
        // over the computed row HLC, per-column HLC map, or signature map.
        // A remote PK assignment is worse on the UPDATE path, where it
        // would repoint the very row the WHERE clause just matched; row
        // identity comes from `row_pks` alone.
        if change.column_name == HLC_TIMESTAMP_COLUMN
            || change.column_name == COLUMN_HLCS_COLUMN
            || change.column_name == COLUMN_SIGS_COLUMN
            || expected_pks.contains(change.column_name.as_str())
        {
            report.skipped_reserved_column += 1;
            continue;
        }
        let current_col_hlc = column_hlcs
            .get(&change.column_name)
            .and_then(JsonValue::as_str)
            .unwrap_or("");
        if !hlc_is_newer(&change.hlc_timestamp, current_col_hlc) {
            report.skipped_stale += 1;
            continue;
        }
        let sql_value = ValueConverter::json_to_rusqlite_value(&change.value)?;
        column_hlcs.insert(
            change.column_name.clone(),
            JsonValue::String(change.hlc_timestamp.clone()),
        );
        if hlc_is_newer(&change.hlc_timestamp, &max_hlc_for_row) {
            max_hlc_for_row = change.hlc_timestamp.clone();
        }
        let entry = (
            change.column_name.clone(),
            sql_value,
            change.hlc_timestamp.clone(),
            change.sig.clone(),
        );
        match staged
            .iter_mut()
            .find(|(column, _, _, _)| *column == change.column_name)
        {
            Some(slot) => *slot = entry,
            None => staged.push(entry),
        }
    }

    if staged.is_empty() {
        return Ok(());
    }

    if !row_exists {
        let empty: Vec<(serde_json::Map<String, JsonValue>, String)> = Vec::new();
        let candidates = shadow.get(&table_name).unwrap_or(&empty);
        if insert_suppressed_by_deletes(&row_pks, &max_hlc_for_row, candidates) {
            report.skipped_shadowed_by_delete += staged.len();
            return Ok(());
        }
    }

    let column_hlcs_json =
        serde_json::to_string(&column_hlcs).map_err(|e| DatabaseError::SerializationError {
            reason: format!("Failed to serialize column HLCs: {e}"),
        })?;

    if row_exists {
        // Never regress the row-level HLC: an older column-late-arrival
        // legally has an older HLC, but the row HLC feeds the delete-
        // resurrection comparison — a regression would let an older delete
        // shadow a newer local write.
        if hlc_is_newer(&current_row_hlc, &max_hlc_for_row) {
            max_hlc_for_row = current_row_hlc.clone();
        }
        write_update(
            tx,
            &table_name,
            &staged,
            &column_hlcs_json,
            &max_hlc_for_row,
            &where_clause,
            &pk_values,
            has_sigs_column,
        )?;
    } else {
        write_insert(
            tx,
            &table_name,
            &schema,
            &row_pks,
            &staged,
            &column_hlcs_json,
            &max_hlc_for_row,
            has_sigs_column,
        )?;
    }
    report.applied += staged.len();
    Ok(())
}

/// `(row_level_hlc, parsed_column_hlcs)` for a row that already exists.
type ExistingHlcs = Option<(String, serde_json::Map<String, JsonValue>)>;

fn fetch_existing_hlcs(
    tx: &Transaction<'_>,
    table_name: &str,
    where_clause: &str,
    pk_values: &[JsonValue],
) -> std::result::Result<ExistingHlcs, DatabaseError> {
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
        Err(e) => Err(DatabaseError::from(e)),
    }
}
