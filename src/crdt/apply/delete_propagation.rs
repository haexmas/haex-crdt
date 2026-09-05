//! Delete-log fan-out and resurrection defense.
//!
//! Delete-log entries live in [`DELETED_ROWS_TABLE`] as ordinary CRDT rows;
//! they arrive at a peer through the regular apply pipeline like any other
//! column change. What this module adds is the second half: taking the
//! delete-log rows the pipeline just wrote and issuing the corresponding
//! `DELETE` against the target business tables — while respecting LWW
//! semantics so a row that was inserted or updated *after* the delete does
//! not vanish (see [`should_propagate_delete`]).
//!
//! The mirror problem — an insert arriving *after* a shadowing delete —
//! is handled at the insert site via [`insert_suppressed_by_deletes`] so
//! the shadow decision uses a preloaded snapshot rather than a per-insert
//! query.

use std::collections::{HashMap, HashSet};

use rusqlite::{params, Transaction};
use serde_json::Value as JsonValue;

use crate::crdt::columns::DELETED_ROWS_TABLE;
use crate::crdt::hlc::compare_hlc_strings;
use crate::crdt::trigger::{get_table_schema, is_safe_identifier};
use crate::db::error::DatabaseError;

use super::grouping::build_pk_where_from_map;
use super::report::ApplyReport;

/// Decide whether to honour a delete-log entry, given the HLC of the entry
/// and the HLC of the target row currently in the table (if any).
///
/// CRDT semantics: a delete is a timestamped operation. If the target row
/// carries a `haex_hlc` strictly newer than the delete-log entry, the row
/// was inserted or updated *after* the delete and must be kept — that is a
/// "resurrection" and must NOT be dropped. Row-absent → propagate is a
/// no-op DELETE (safe).
pub fn should_propagate_delete(delete_log_hlc: &str, target_row_hlc: Option<&str>) -> bool {
    match target_row_hlc {
        None => true,
        Some(target) => {
            compare_hlc_strings(target, delete_log_hlc) != std::cmp::Ordering::Greater
        }
    }
}

/// True if a delete-log entry at `delete_hlc` shadows an insert at
/// `insert_hlc` — the insert is NOT strictly newer, so applying it would
/// resurrect a deleted row. Sibling of [`should_propagate_delete`] with the
/// tie-break favoring the delete (`>=` shadows).
pub fn delete_shadows_insert(delete_hlc: &str, insert_hlc: &str) -> bool {
    compare_hlc_strings(insert_hlc, delete_hlc) != std::cmp::Ordering::Greater
}

/// Snapshot of every delete-log entry currently on disk, indexed by target
/// table name. Values are `(parsed_row_pks_map, delete_hlc)` pairs so the
/// insert-site check can compare against a live snapshot without re-parsing
/// the JSON `row_pks` per candidate.
///
/// Entries with NULL `haex_hlc` are skipped: they cannot participate in an
/// HLC comparison so treating them as absent is safer than assigning a
/// default that could shadow an unrelated insert.
pub type DeleteShadowMap = HashMap<String, Vec<(serde_json::Map<String, JsonValue>, String)>>;

/// Load the whole delete-log into a per-table shadow map once, before the
/// per-row insert loop starts. Loading lazily per-table on first absent row
/// scales with the number of touched tables; loading once collapses the
/// whole apply pass to a single sweep. The caller's transaction (`tx`) is
/// the only writer to [`DELETED_ROWS_TABLE`] during apply, so the snapshot
/// stays consistent for the duration of one call.
pub fn load_delete_shadow_map(tx: &Transaction<'_>) -> Result<DeleteShadowMap, DatabaseError> {
    let mut map: DeleteShadowMap = HashMap::new();
    let mut stmt = tx
        .prepare(&format!(
            "SELECT table_name, row_pks, haex_hlc FROM \"{DELETED_ROWS_TABLE}\""
        ))
        .map_err(DatabaseError::from)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(DatabaseError::from)?;
    for r in rows {
        let (table_name, pks_str, del_hlc) = r.map_err(DatabaseError::from)?;
        if let (Some(del_hlc), Ok(pks_map)) = (
            del_hlc,
            serde_json::from_str::<serde_json::Map<String, JsonValue>>(&pks_str),
        ) {
            map.entry(table_name).or_default().push((pks_map, del_hlc));
        }
    }
    Ok(map)
}

/// True if an insert for `insert_pks` at `insert_hlc` must be suppressed
/// because a delete-log entry for the same row (parsed-map equality —
/// serializer- and order-agnostic) carries a shadowing HLC.
///
/// Match is type-strict (`serde_json::Value` equality): a PK serialized as
/// a JSON number on one side and a string on the other will NOT match. In
/// practice the delete-tracked tables use TEXT (UUID) PKs, so this is safe
/// today. A miss here fails toward NOT suppressing (status-quo
/// resurrection), never toward a wrong suppression of a valid write.
pub fn insert_suppressed_by_deletes(
    insert_pks: &serde_json::Map<String, JsonValue>,
    insert_hlc: &str,
    candidates: &[(serde_json::Map<String, JsonValue>, String)],
) -> bool {
    candidates
        .iter()
        .any(|(del_pks, del_hlc)| del_pks == insert_pks && delete_shadows_insert(del_hlc, insert_hlc))
}

/// Apply pending delete-log entries to their target tables.
///
/// For each id in `delete_log_ids`, reads `(table_name, row_pks, delete_hlc)`
/// from [`DELETED_ROWS_TABLE`] and issues a `DELETE` on the target table.
/// The caller **MUST** have set `triggers_enabled = '0'` first, so the DELETE
/// does not re-append to the delete-log.
///
/// Per-row failures are tolerated: a single malformed delete-log entry
/// must not abort the whole batch (would wedge the sync cursor
/// permanently). Errors surface as skipped counters on the returned
/// `ApplyReport` delta so callers can log them.
pub fn propagate_deleted_rows_to_target_tables(
    tx: &Transaction<'_>,
    delete_log_ids: &HashSet<String>,
    report: &mut ApplyReport,
) -> Result<(), DatabaseError> {
    for id in delete_log_ids {
        let entry = tx
            .query_row(
                &format!(
                    "SELECT table_name, row_pks, haex_hlc FROM \"{DELETED_ROWS_TABLE}\" WHERE id = ?1"
                ),
                params![id],
                |row| {
                    let table_name: String = row.get(0)?;
                    let row_pks: String = row.get(1)?;
                    let delete_hlc: String = row.get(2)?;
                    Ok((table_name, row_pks, delete_hlc))
                },
            );
        let (target_table, row_pks_json, delete_hlc) = match entry {
            Ok(r) => r,
            Err(rusqlite::Error::QueryReturnedNoRows) => continue,
            Err(e) => return Err(DatabaseError::from(e)),
        };

        if !is_safe_identifier(&target_table) {
            continue;
        }

        let row_pks: serde_json::Map<String, JsonValue> =
            match serde_json::from_str(&row_pks_json) {
                Ok(m) => m,
                Err(_) => continue,
            };

        // Defense-in-depth: refuse to propagate unless row_pks names exactly
        // the target table's PK columns. A composite PK with only some keys
        // present would build a partial WHERE and over-DELETE every row
        // matching the partial key.
        let schema = match get_table_schema(tx, &target_table) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let expected_pks: HashSet<&str> = schema
            .iter()
            .filter(|c| c.is_pk)
            .map(|c| c.name.as_str())
            .collect();
        let provided_pks: HashSet<&str> = row_pks.keys().map(|k| k.as_str()).collect();
        if expected_pks.is_empty() || expected_pks != provided_pks {
            continue;
        }

        let (where_clause, values) = match build_pk_where_from_map(&row_pks) {
            Some(parts) => parts,
            None => continue,
        };
        let sql_params: Vec<rusqlite::types::Value> = values
            .iter()
            .map(json_to_sql_value)
            .collect();
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            sql_params.iter().map(|v| v as &dyn rusqlite::ToSql).collect();

        // Resurrection check: if the target row was inserted or updated
        // after this delete-log entry, keep it.
        let select_hlc_sql =
            format!("SELECT haex_hlc FROM \"{target_table}\" WHERE {where_clause}");
        let target_row_hlc: Option<String> = match tx
            .query_row(&select_hlc_sql, param_refs.as_slice(), |row| row.get(0))
        {
            Ok(hlc) => Some(hlc),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => return Err(DatabaseError::from(e)),
        };
        if !should_propagate_delete(&delete_hlc, target_row_hlc.as_deref()) {
            report.skipped_delete_target_newer += 1;
            continue;
        }

        let delete_sql = format!("DELETE FROM \"{target_table}\" WHERE {where_clause}");
        // A single failed DELETE (constraint, table gone) must not abort
        // the whole batch — swallow the error so the pull cursor advances.
        let _ = tx.execute(&delete_sql, param_refs.as_slice());
    }
    Ok(())
}

/// Convert `serde_json::Value` PK values into `rusqlite::types::Value`. Only
/// the PK-viable variants are covered — bool/object/array PKs are extremely
/// unusual and fall back to text via `to_string`, which is safe for the
/// equality comparisons this module builds but may not round-trip. Delete-
/// tracked tables use TEXT UUIDs today so this is a defensive fallback.
fn json_to_sql_value(v: &JsonValue) -> rusqlite::types::Value {
    use rusqlite::types::Value;
    match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Real(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => Value::Text(s.clone()),
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---------- should_propagate_delete --------------------------------

    const H1: &str = "0000000000000001/abcdef";
    const H2: &str = "0000000000000002/abcdef";

    #[test]
    fn propagate_when_target_row_absent() {
        assert!(should_propagate_delete(H1, None));
    }

    #[test]
    fn propagate_when_target_row_older() {
        assert!(should_propagate_delete(H2, Some(H1)));
    }

    #[test]
    fn propagate_when_target_row_equal_hlc() {
        // Tie goes to the delete — matches `delete_shadows_insert` (>=).
        assert!(should_propagate_delete(H1, Some(H1)));
    }

    #[test]
    fn do_not_propagate_when_target_row_strictly_newer() {
        assert!(!should_propagate_delete(H1, Some(H2)));
    }

    // ---------- delete_shadows_insert ----------------------------------

    #[test]
    fn shadow_when_insert_older_than_delete() {
        assert!(delete_shadows_insert(H2, H1));
    }

    #[test]
    fn shadow_on_equal_hlc() {
        assert!(delete_shadows_insert(H1, H1));
    }

    #[test]
    fn no_shadow_when_insert_strictly_newer() {
        assert!(!delete_shadows_insert(H1, H2));
    }

    // ---------- insert_suppressed_by_deletes ---------------------------

    #[test]
    fn insert_suppressed_by_matching_pk_and_shadowing_hlc() {
        let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
        let candidates = vec![(pks.clone(), H2.to_string())];
        assert!(insert_suppressed_by_deletes(&pks, H1, &candidates));
    }

    #[test]
    fn insert_not_suppressed_when_pks_differ() {
        let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
        let other = serde_json::Map::from_iter([("id".to_string(), json!("row-b"))]);
        let candidates = vec![(other, H2.to_string())];
        assert!(!insert_suppressed_by_deletes(&pks, H1, &candidates));
    }

    #[test]
    fn insert_not_suppressed_when_it_is_strictly_newer_than_all_deletes() {
        let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
        let candidates = vec![(pks.clone(), H1.to_string())];
        assert!(!insert_suppressed_by_deletes(&pks, H2, &candidates));
    }
}
