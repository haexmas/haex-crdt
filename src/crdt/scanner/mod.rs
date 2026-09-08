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
//!   cursor from one table, restricted by a [`ScanFilters`] carrying the
//!   optional origin-node, PK allow-list and single-column equality
//!   filters (the filters that need no knowledge of what the data means).
//! - [`paginate_changes`] — pack changes into transaction-HLC groups that
//!   fit a byte budget without splitting a group across pages.
//! - [`ColumnChange`] — the change record.
//!
//! # `sig` is opaque
//!
//! `ColumnChange::sig` is `Option<JsonValue>`: the raw JSON entry
//! from the column-signature map keyed by `column_name` if present, else
//! `None`. The crate does not decode a shape here — consumers with a
//! signature provider decode into their own type. This is what makes the
//! scanner agnostic to haex-vault's per-space `{col: {space: sig}}`
//! nesting vs a consumer that stores `{col: sig}` flat.

mod emit;

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::scanner::emit::emit_row_changes;
use crate::crdt::trigger::{get_table_schema, is_safe_identifier, ColumnInfo};
use crate::db::core::execute::MAX_CRDT_TRANSACTION_BYTES;
use crate::db::error::DatabaseError;
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

/// The serve-side per-page byte budget for a paginated pull. Sized equal to
/// [`MAX_CRDT_TRANSACTION_BYTES`], the cap `execute_with_crdt` rejects one
/// write's serialized parameters against, so a page is dimensioned for a
/// transaction at that cap.
///
/// It is not a guarantee that any group fits, in three ways: a group of
/// change records re-serializes more than the write's parameters did; the
/// cap is enforced on the `execute_with_crdt` path only, so transactions
/// arriving through `apply_remote_changes` or a raw-connection write using
/// the `current_hlc()` UDF are never size-checked at all; and for a
/// consumer's own [`Paginable`] type the crate has never seen the
/// `Serialize` impl. What guarantees progress regardless is the ≥1 rule in
/// [`paginate_changes`], which emits an over-budget group rather than
/// stalling on it.
pub const PULL_PAGE_BUDGET: usize = MAX_CRDT_TRANSACTION_BYTES;

/// One column-level change ready for outbound transmission by a
/// consumer-defined sync layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnChange {
    pub table_name: String,
    /// JSON string of PK values in schema-declaration order, e.g.
    /// `{"id":"abc-123"}`. See [`scan_table_for_local_changes`] for the
    /// canonical encoding contract.
    pub row_pks: String,
    pub column_name: String,
    pub hlc_timestamp: String,
    pub value: JsonValue,
    pub device_id: String,
    /// Raw entry from the column-signature map keyed by `column_name` if
    /// present. The crate does not decode this — consumers own the shape
    /// via their [`crate::signature::SignatureProvider`] (haex-vault
    /// stores per-space nested `{space: sig}`, plain deployments store the
    /// sig directly).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sig: Option<JsonValue>,
}

/// A change record [`paginate_changes`] can pack into pages: it exposes the
/// HLC identifying the source transaction the change belongs to.
///
/// Pagination needs nothing else from a change record, so the trait keeps
/// the algorithm's invariants — the ≥1 rule, never splitting a
/// transaction-HLC group, ascending HLC order, and with it the HLC-only
/// cursor — in one place. [`ColumnChange`] implements it; a consumer whose
/// change type carries more than the crate's (a decoded signature, a
/// routing key, …) implements it too instead of re-deriving those
/// invariants.
///
/// [`Serialize`] is a supertrait because the packing rule measures the
/// serialized size of each transaction group.
pub trait Paginable: Serialize {
    /// HLC of the transaction that produced this change.
    fn transaction_hlc(&self) -> &str;
}

impl Paginable for ColumnChange {
    fn transaction_hlc(&self) -> &str {
        &self.hlc_timestamp
    }
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

/// The content-agnostic filters [`scan_table_for_local_changes`] applies.
///
/// All three default to "no restriction", so a full scan is
/// `ScanFilters::default()` and a single restriction is
/// `ScanFilters { column_eq: Some(("tenant_id", id)), ..Default::default() }`.
///
/// `Copy`, because [`scan_table_for_local_changes`] takes it by value and
/// a consumer draining several tables passes the same filters to each.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScanFilters<'a> {
    /// When `Some(node_id)`, emits only columns whose HLC's node-id
    /// matches. Use to skip columns freshly applied from remote peers so
    /// they are not pushed back (ping-pong prevention).
    pub origin_node: Option<u128>,
    /// When `Some(&set)`, emits only rows whose canonical PK JSON is in
    /// the set. Applied BEFORE parsing the HLC/sig blobs so a large table
    /// with few allow-listed rows pays deserialisation cost only on the
    /// matches. See [`scan_table_for_local_changes`] for the PK JSON
    /// encoding contract the set entries must follow.
    pub row_pks: Option<&'a HashSet<String>>,
    /// When `Some((column, value))`, restricts the scan to rows where
    /// `column` equals `value`. Composes with the scan's `after_hlc`
    /// cursor via `AND`.
    ///
    /// Any column the table actually has is a legal target — including a
    /// PK, a `_no_trigger` column, and a CRDT metadata column. Which
    /// columns may be *filtered on* is deliberately independent of which
    /// ones get emitted, so a consumer can scope a scan by bookkeeping
    /// that is itself opted out of change tracking.
    ///
    /// This cannot be a post-filter on the returned changes: if a row's
    /// filter-column HLC is at or below the cursor, no change is emitted
    /// for that column, so a caller inspecting the result set has nothing
    /// to match the row on. The restriction has to reach the `WHERE`
    /// clause.
    ///
    /// `value` is bound as a SQL parameter; it is never interpolated. The
    /// column NAME is interpolated, so it must pass
    /// [`is_safe_identifier`] — a name that does not is an `Err`, since
    /// "this name cannot be filtered on" is not "no rows matched". If the
    /// table has no column of that name the scan returns no rows at all:
    /// fail closed, because a consumer whose filter target is missing must
    /// not silently receive the whole table.
    ///
    /// The value always binds as TEXT, so matching relies on the column's
    /// type affinity: an INTEGER-affinity column coerces `"5"` and matches
    /// integer `5`, but a no-affinity column holding integer `5` does not
    /// — that scan returns `Ok(vec![])`, indistinguishable from "no such
    /// rows". A NULL filter value is not expressible (`= NULL` never
    /// matches in SQL anyway).
    pub column_eq: Option<(&'a str, &'a str)>,
}

/// Reads per-column changes since `after_hlc` from `table_name`, returning
/// one [`ColumnChange`] per (row, changed column) pair.
///
/// Primary-key columns and the crate's three structural metadata columns
/// are never emitted. Every other column is, `_no_trigger` ones included —
/// that suffix governs what fires a trigger, not what ships. See
/// [`partition_columns`] for why the two are distinct.
///
/// # Filters
///
/// - `after_hlc` — exclusive lower bound on the per-column HLC. `None`
///   emits every column with a usable HLC (fresh scan / full snapshot).
///   The row-level HLC is used as fallback when a column is missing
///   from the column-HLC map; empty-string HLCs are treated as absent.
/// - `filters` — the three content-agnostic row/column restrictions; see
///   [`ScanFilters`] and its fields for the semantics of each.
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
    filters: ScanFilters<'_>,
) -> Result<Vec<ColumnChange>, DatabaseError> {
    // Pre-query sanity: refuse identifier-unsafe input at the boundary, so
    // the error a caller sees for a bad filter name does not depend on the
    // table's shape (same stance as `apply_remote_changes`). The gate needs
    // no schema, and the name is interpolated into the WHERE clause below.
    //
    // Schema membership is NOT this gate: SQLite happily reports a column
    // named `bucket" OR 1=1 OR "bucket_no_trigger`, and a `_no_trigger`
    // name reaches this filter without passing any other check in the crate
    // — `partition_columns` keeps it out of the SELECT list, and the trigger
    // installer strips the suffix before its own identifier check.
    // Interpolated, such a name turns the restriction into a tautology and
    // ships every row for a value that matches none.
    if let Some((filter_column, _)) = filters.column_eq {
        if !is_safe_identifier(filter_column) {
            return Err(DatabaseError::ValidationError {
                reason: format!(
                    "Unsafe filter column name '{filter_column}' for table '{table_name}'"
                ),
            });
        }
    }

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

    // Predicates and their bound values are built together so the `?N`
    // indices always match the parameter vector's order.
    let mut predicates: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(hlc) = after_hlc {
        // Admit rows whose row-level HLC is absent (NULL) or empty in addition
        // to those strictly newer than the cursor. A corrupt/legacy row can
        // carry an empty row-level HLC while still holding a valid per-column
        // HLC in the column-HLC map; a bare `<row_hlc> > ?` prefilter drops
        // it before the per-column fallback below can emit that valid change,
        // so the row could only ever converge on a full scan. The per-column
        // loop re-checks each HLC against `after_hlc`, so widening here cannot
        // leak stale columns — rows with no usable HLC are still skipped.
        params.push(hlc.to_string());
        predicates.push(format!(
            "(\"{col}\" > ?{n} OR \"{col}\" IS NULL OR \"{col}\" = '')",
            col = HLC_TIMESTAMP_COLUMN,
            n = params.len()
        ));
    }

    if let Some((filter_column, filter_value)) = filters.column_eq {
        // The name already passed the identifier gate at the top of the
        // function. Membership is the separate fail-closed rule: an unknown
        // column means "no matching rows", never "the whole table".
        if !schema.iter().any(|c| c.name == filter_column) {
            return Ok(Vec::new());
        }
        params.push(filter_value.to_string());
        predicates.push(format!("\"{filter_column}\" = ?{n}", n = params.len()));
    }

    let where_sql = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };

    let query = format!("SELECT {column_list} FROM \"{table_name}\"{where_sql}");
    let mut stmt = conn.prepare(&query)?;
    let param_refs: Vec<&dyn rusqlite::ToSql> =
        params.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
    let mut rows = stmt.query(param_refs.as_slice())?;

    let mut changes: Vec<ColumnChange> = Vec::new();
    while let Some(row) = rows.next()? {
        emit_row_changes(
            row,
            &select_columns,
            &pk_columns,
            &data_columns,
            table_name,
            after_hlc,
            device_id,
            filters.origin_node,
            filters.row_pks,
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
/// transaction could never traverse the wire. A group's serialized size is
/// therefore unbounded, and [`MAX_CRDT_TRANSACTION_BYTES`] does not bound
/// it even for [`ColumnChange`]: that cap counts one write's serialized
/// parameters while each change record re-serializes its table name, PK
/// JSON, column name, HLC, device id and sig, and it is checked on the
/// `execute_with_crdt` path only — a transaction applied by
/// `apply_remote_changes` or written straight through a raw connection
/// never passes it. For a consumer's own [`Paginable`] type, whose
/// `Serialize` impl the crate has never seen, there is nothing to relate a
/// group's size to at all. The ≥1 rule is what keeps pagination making
/// progress in every one of those cases.
///
/// Generic over [`Paginable`] so a consumer's own change type gets the same
/// invariants rather than a re-derived copy of them.
pub fn paginate_changes<T: Paginable>(changes: Vec<T>, page_budget: usize) -> (Vec<T>, bool) {
    if changes.is_empty() {
        return (Vec::new(), false);
    }

    let mut groups: HashMap<String, Vec<T>> = HashMap::new();
    for change in changes {
        let group_hlc = change.transaction_hlc().to_string();
        groups.entry(group_hlc).or_default().push(change);
    }
    let mut ordered: Vec<(String, Vec<T>)> = groups.into_iter().collect();
    ordered.sort_by(|a, b| crate::crdt::hlc::compare_hlc_strings(&a.0, &b.0));

    let mut page: Vec<T> = Vec::new();
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

/// Splits a table schema into PK columns and syncable data columns.
///
/// Data columns exclude PKs and the crate's own three structural metadata
/// columns ([`HLC_TIMESTAMP_COLUMN`], [`COLUMN_HLCS_COLUMN`],
/// [`COLUMN_SIGS_COLUMN`]) — those carry the CRDT's own bookkeeping, so
/// shipping them as data changes would be meaningless. Naming them here is
/// the crate describing its own internals, not a consumer exception list.
///
/// Everything else is emitted, **including `_no_trigger` columns**. The two
/// suffixes govern different questions and must not be conflated:
///
/// - `_no_trigger` decides what fires a trigger, i.e. what *drives* sync. A
///   `_no_trigger` column has no entry in the per-column HLC map, so it
///   never causes a row to be scanned — but when the row's tracked columns
///   do sync, the column's current value rides along under the row-level
///   HLC. That is the intended semantics, not a leak.
/// - `_no_sync` decides what participates in sync at all, and is a
///   *table*-level suffix (see [`crate::db::init`]). There is deliberately
///   no column-level equivalent yet: a consumer that must keep a column off
///   the wire filters it out of the returned changes.
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

#[cfg(test)]
mod tests;
