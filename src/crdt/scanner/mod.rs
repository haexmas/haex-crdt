//! Outbound scanner: reads local CRDT changes from a table since a cursor.
//!
//! Ported from haex-vault's `src-tauri/src/crdt/scanner.rs`, trimmed to
//! the CRDT-generic surface. haex-vault's shared-space whitelists
//! (`SPACE_SCOPED_CRDT_TABLES`, `MEMBERSHIP_SYSTEM_TABLES`), the
//! `is_registered_for_space` register lookup, and the per-space scanners
//! (`scan_space_scoped_tables_for_local_changes` etc.) stay in haex-vault
//! — they belong to the sync-transport tier, which extraction plan §3
//! keeps out of this crate.
//!
//! The consumer decides what to scan and what to filter. The crate ships:
//!
//! - [`scan_dirty_tables`] — list tables the trigger installer marked
//!   dirty.
//! - [`scan_table_for_local_changes`] — read per-column changes since a
//!   cursor from one table, with optional origin-node and PK allow-list
//!   filters (the two filters that are content-agnostic).
//! - [`paginate_changes`] — pack changes into transaction-HLC groups that
//!   fit a byte budget without splitting a group across pages.
//! - [`LocalColumnChange`] — the change record.
//!
//! # `sig` is opaque
//!
//! `LocalColumnChange::sig` is `Option<JsonValue>`: the raw JSON entry
//! from `haex_column_sigs[column_name]` if present, else `None`. The
//! crate does not decode a shape here — consumers with a signature
//! provider decode into their own type. This is what makes the scanner
//! agnostic to haex-vault's per-space `{col: {space: sig}}` nesting vs a
//! consumer that stores `{col: sig}` flat.

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::{hlc_is_from_node, hlc_is_newer};
use crate::crdt::trigger::{get_table_schema, ColumnInfo};
use crate::db::core::convert_value_ref_to_json;
use crate::db::core::execute::MAX_CRDT_TRANSACTION_BYTES;
use crate::db::error::DatabaseError;
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

/// The serve-side per-page byte budget for a paginated pull. Sized equal to
/// [`MAX_CRDT_TRANSACTION_BYTES`] so a single page always has room for the
/// largest legal transaction (the ≥1 rule in [`paginate_changes`] guarantees
/// even an at-cap group is emitted).
pub const PULL_PAGE_BUDGET: usize = MAX_CRDT_TRANSACTION_BYTES;

/// One column-level change ready for outbound transmission by a
/// consumer-defined sync layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalColumnChange {
    pub table_name: String,
    /// JSON string of PK values in schema-declaration order, e.g.
    /// `{"id":"abc-123"}`. See [`scan_table_for_local_changes`] for the
    /// canonical encoding contract.
    pub row_pks: String,
    pub column_name: String,
    pub hlc_timestamp: String,
    pub value: JsonValue,
    pub device_id: String,
    /// Raw entry from `haex_column_sigs[column_name]` if present. The
    /// crate does not decode this — consumers own the shape via their
    /// [`crate::signature::SignatureProvider`] (haex-vault stores
    /// per-space nested `{space: sig}`, plain deployments store the sig
    /// directly).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<JsonValue>,
}

/// Lists tables the trigger installer has marked dirty (see
/// [`crate::crdt::trigger::setup_triggers_for_table`]). Returns the names
/// ordered by ascending `last_modified`, with `table_name` ascending as the
/// tie-breaker.
pub fn scan_dirty_tables(conn: &Connection) -> Result<Vec<String>, DatabaseError> {
    let mut stmt = conn.prepare(&format!(
        "SELECT table_name FROM {TABLE_CRDT_DIRTY_TABLES} ORDER BY last_modified ASC, table_name ASC"
    ))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let out: Vec<String> = rows.collect::<Result<_, _>>()?;
    Ok(out)
}

/// Reads per-column changes since `after_hlc` from `table_name`, returning
/// one [`LocalColumnChange`] per (row, changed column) pair.
///
/// # Filters
///
/// - `after_hlc` — exclusive lower bound on the per-column HLC. `None`
///   emits every column with a usable HLC (fresh scan / full snapshot).
///   Row-level `haex_hlc` is used as fallback when a column is missing
///   from `haex_column_hlcs`; empty-string HLCs are treated as absent.
/// - `origin_node_filter` — when `Some(node_id)`, emits only columns
///   whose HLC's node-id matches. Use to skip columns freshly applied
///   from remote peers so they are not pushed back (ping-pong prevention).
/// - `row_pks_filter` — when `Some(&set)`, emits only rows whose
///   canonical PK JSON is in the set. Applied BEFORE parsing the HLC/sig
///   blobs so a large table with few allow-listed rows pays deserialisation
///   cost only on the matches.
///
/// # PK JSON encoding
///
/// `row_pks` is a canonical JSON object with keys in **schema-declaration
/// order** (the order [`get_table_schema`] returns PK columns). This
/// matches the wire form used across the CRDT ecosystem — any consumer
/// that maintains a PK allow-list (e.g. from a shared-space register)
/// must produce PK JSON in the same order or `HashSet::contains` will
/// miss composite-PK rows declared in non-alphabetical order like
/// `(col_b, col_a)`.
pub fn scan_table_for_local_changes(
    conn: &Connection,
    table_name: &str,
    after_hlc: Option<&str>,
    device_id: &str,
    origin_node_filter: Option<u128>,
    row_pks_filter: Option<&HashSet<String>>,
) -> Result<Vec<LocalColumnChange>, DatabaseError> {
    let schema = get_table_schema(conn, table_name)?;
    if schema.is_empty() {
        return Ok(Vec::new());
    }

    let (pk_columns, data_columns) = partition_columns(&schema);

    if pk_columns.is_empty() {
        return Err(DatabaseError::ExecutionError {
            sql: format!("PRAGMA table_info(\"{table_name}\")"),
            reason: format!("Table '{table_name}' has no primary key"),
            table: Some(table_name.to_string()),
        });
    }

    let mut select_columns: Vec<&str> = Vec::new();
    for col in &pk_columns {
        select_columns.push(&col.name);
    }
    for col in &data_columns {
        select_columns.push(&col.name);
    }
    let has_hlc_timestamp = schema.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN);
    let has_column_hlcs = schema.iter().any(|c| c.name == COLUMN_HLCS_COLUMN);
    if !has_hlc_timestamp || !has_column_hlcs {
        let missing_columns: Vec<&str> = [
            (HLC_TIMESTAMP_COLUMN, has_hlc_timestamp),
            (COLUMN_HLCS_COLUMN, has_column_hlcs),
        ]
        .into_iter()
        .filter_map(|(column, present)| (!present).then_some(column))
        .collect();
        return Err(DatabaseError::ExecutionError {
            sql: format!("PRAGMA table_info(\"{table_name}\")"),
            reason: format!(
                "Table '{table_name}' is missing required CRDT metadata column(s): {}",
                missing_columns.join(", ")
            ),
            table: Some(table_name.to_string()),
        });
    }

    select_columns.push(HLC_TIMESTAMP_COLUMN);
    select_columns.push(COLUMN_HLCS_COLUMN);
    let has_column_sigs = schema.iter().any(|c| c.name == COLUMN_SIGS_COLUMN);
    if has_column_sigs {
        select_columns.push(COLUMN_SIGS_COLUMN);
    }

    let column_list: String = select_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");

    let (where_sql, params) = if let Some(hlc) = after_hlc {
        // Admit rows whose row-level HLC is absent (NULL) or empty in addition
        // to those strictly newer than the cursor. A corrupt/legacy row can
        // carry `haex_hlc = ''` while still holding a valid per-column HLC in
        // `haex_column_hlcs`; a bare `"haex_hlc" > ?` prefilter drops it before
        // the per-column fallback below can emit that valid change, so the row
        // could only ever converge on a full scan. The per-column loop re-checks
        // each HLC against `after_hlc`, so widening here cannot leak stale
        // columns — rows with no usable HLC are still skipped.
        (
            format!(
                " WHERE (\"{col}\" > ?1 OR \"{col}\" IS NULL OR \"{col}\" = '')",
                col = HLC_TIMESTAMP_COLUMN
            ),
            vec![hlc.to_string()],
        )
    } else {
        (String::new(), Vec::new())
    };

    let query = format!("SELECT {column_list} FROM \"{table_name}\"{where_sql}");
    let mut stmt = conn.prepare(&query)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let mut rows = stmt.query(param_refs.as_slice())?;

    let mut changes: Vec<LocalColumnChange> = Vec::new();
    while let Some(row) = rows.next()? {
        emit_row_changes(
            row,
            &select_columns,
            &pk_columns,
            &data_columns,
            table_name,
            after_hlc,
            device_id,
            origin_node_filter,
            row_pks_filter,
            &mut changes,
        )?;
    }

    Ok(changes)
}

/// Packs whole transaction-HLC groups into pages that fit `page_budget`
/// bytes, returning `(page, has_more)`. Pure and deterministic — no I/O.
///
/// HLC equals one source transaction, so all changes sharing an
/// `hlc_timestamp` belong to one transaction and are never split across a
/// page boundary. Groups are emitted in ascending HLC order (matching the
/// scanner's global ordering), so a client can resume the next page at
/// the max HLC of the page just received — the cursor stays HLC-only.
///
/// **≥1 rule**: if the page is still empty when the first group alone
/// exceeds the budget, that group is included anyway (with `has_more =
/// true` if later groups exist) — otherwise an at-or-over-budget
/// transaction could never traverse the wire. Bounded above by
/// [`MAX_CRDT_TRANSACTION_BYTES`] because `execute_with_crdt` rejects
/// oversized writes at commit time.
pub fn paginate_changes(
    changes: Vec<LocalColumnChange>,
    page_budget: usize,
) -> (Vec<LocalColumnChange>, bool) {
    if changes.is_empty() {
        return (Vec::new(), false);
    }

    let mut groups: HashMap<String, Vec<LocalColumnChange>> = HashMap::new();
    for change in changes {
        groups
            .entry(change.hlc_timestamp.clone())
            .or_default()
            .push(change);
    }
    let mut ordered: Vec<(String, Vec<LocalColumnChange>)> = groups.into_iter().collect();
    ordered.sort_by(|a, b| crate::crdt::hlc::compare_hlc_strings(&a.0, &b.0));

    let mut page: Vec<LocalColumnChange> = Vec::new();
    let mut running: usize = 0;
    let mut has_more = false;

    for (idx, (_hlc, group)) in ordered.into_iter().enumerate() {
        let group_size = serde_json::to_vec(&group)
            .map(|v| v.len())
            .unwrap_or(usize::MAX);
        let fits = running.saturating_add(group_size) <= page_budget;
        if fits || idx == 0 {
            running = running.saturating_add(group_size);
            page.extend(group);
        } else {
            has_more = true;
            break;
        }
    }

    (page, has_more)
}

// -----------------------------------------------------------------------
// Private helpers
// -----------------------------------------------------------------------

/// Splits a table schema into PK columns and syncable data columns. Data
/// columns exclude PKs and the three CRDT metadata columns — consumer-
/// schema conventions like `updated_at` are the consumer's concern (same
/// stance as [`crate::crdt::trigger::setup_triggers_for_table`]).
fn partition_columns(schema: &[ColumnInfo]) -> (Vec<&ColumnInfo>, Vec<&ColumnInfo>) {
    let pk_columns: Vec<&ColumnInfo> = schema.iter().filter(|c| c.is_pk).collect();
    let data_columns: Vec<&ColumnInfo> = schema
        .iter()
        .filter(|c| {
            !c.is_pk
                && c.name != HLC_TIMESTAMP_COLUMN
                && c.name != COLUMN_HLCS_COLUMN
                && c.name != COLUMN_SIGS_COLUMN
        })
        .collect();
    (pk_columns, data_columns)
}

/// Per-row column-change emitter. Reads column values from `row`, builds
/// the canonical PK JSON in schema-declaration order, applies the
/// `row_pks_filter` allow-list if any, then for each data column emits a
/// change when its per-column HLC (or the row-level fallback) is strictly
/// newer than `after_hlc` and — if `origin_node_filter` is set — was
/// written by this node.
#[allow(clippy::too_many_arguments)]
fn emit_row_changes(
    row: &rusqlite::Row<'_>,
    select_columns: &[&str],
    pk_columns: &[&ColumnInfo],
    data_columns: &[&ColumnInfo],
    table_name: &str,
    after_hlc: Option<&str>,
    device_id: &str,
    origin_node_filter: Option<u128>,
    row_pks_filter: Option<&HashSet<String>>,
    out: &mut Vec<LocalColumnChange>,
) -> Result<(), DatabaseError> {
    let mut row_map: HashMap<&str, JsonValue> = HashMap::new();
    for (i, col_name) in select_columns.iter().enumerate() {
        let value_ref = row.get_ref(i)?;
        let json_val = convert_value_ref_to_json(value_ref)?;
        row_map.insert(col_name, json_val);
    }

    // Canonical PK JSON in schema-declaration order. We cannot use
    // `serde_json::Map` here — without the `preserve_order` feature it is
    // a `BTreeMap` and sorts keys alphabetically, which silently breaks
    // composite-PK equality against register-side JSON built in schema
    // order. Construct the JSON string explicitly instead.
    let mut pk_json = String::from("{");
    let mut first = true;
    for pk in pk_columns {
        let val = row_map
            .get(pk.name.as_str())
            .cloned()
            .unwrap_or(JsonValue::Null);
        if !first {
            pk_json.push(',');
        }
        first = false;
        let key_json =
            serde_json::to_string(&pk.name).map_err(|e| DatabaseError::QueryError {
                reason: format!("serialize pk column name '{}': {e}", pk.name),
            })?;
        let val_json = serde_json::to_string(&val).map_err(|e| DatabaseError::QueryError {
            reason: format!("serialize pk column value for '{}': {e}", pk.name),
        })?;
        pk_json.push_str(&key_json);
        pk_json.push(':');
        pk_json.push_str(&val_json);
    }
    pk_json.push('}');

    if let Some(wanted) = row_pks_filter {
        if !wanted.contains(&pk_json) {
            return Ok(());
        }
    }

    let column_hlcs: HashMap<String, String> = match row_map.get(COLUMN_HLCS_COLUMN) {
        Some(JsonValue::String(s)) => serde_json::from_str(s).unwrap_or_default(),
        _ => HashMap::new(),
    };
    let column_sigs_map: serde_json::Map<String, JsonValue> = row_map
        .get(COLUMN_SIGS_COLUMN)
        .and_then(JsonValue::as_str)
        .and_then(|raw| serde_json::from_str::<JsonValue>(raw).ok())
        .and_then(|v| match v {
            JsonValue::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default();

    let row_hlc = match row_map.get(HLC_TIMESTAMP_COLUMN) {
        Some(JsonValue::String(s)) if !s.is_empty() => Some(s.as_str()),
        _ => None,
    };

    for col in data_columns {
        // Treat an empty per-column HLC as absent so it falls back to the
        // row HLC; if both are empty/missing the column has no usable
        // timestamp and is skipped.
        let col_hlc = column_hlcs
            .get(&col.name)
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty());

        let hlc_to_use = match col_hlc.or(row_hlc) {
            Some(h) => h,
            None => continue,
        };

        let passes_hlc = match after_hlc {
            Some(threshold) => hlc_is_newer(hlc_to_use, threshold),
            None => true,
        };
        let passes_origin = match origin_node_filter {
            Some(our_node) => hlc_is_from_node(hlc_to_use, our_node),
            None => true,
        };

        if passes_hlc && passes_origin {
            let value = row_map
                .get(col.name.as_str())
                .cloned()
                .unwrap_or(JsonValue::Null);
            let sig = column_sigs_map.get(&col.name).cloned();

            out.push(LocalColumnChange {
                table_name: table_name.to_string(),
                row_pks: pk_json.clone(),
                column_name: col.name.clone(),
                hlc_timestamp: hlc_to_use.to_string(),
                value,
                device_id: device_id.to_string(),
                sig,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
