//! Groupers that keep the write loop deterministic under any input order.
//!
//! The scanner and any sync transport are free to hand the apply engine a
//! flat `Vec<ColumnChange>` in whatever order fell out of their queues. The
//! engine needs two grouping views:
//!
//! - **By transaction-HLC** — every change written inside one sender-side
//!   transaction shares its HLC. Grouping by HLC and iterating groups in
//!   ascending HLC order applies cross-table transactions (e.g. parent +
//!   child insert) together, in causal order.
//! - **By (table, row_pks)** — all column changes touching one row are
//!   written in one INSERT/UPDATE per row, and rows themselves are ordered
//!   by their earliest HLC so the per-row iteration reflects the same
//!   causal order as the HLC grouping.
//!
//! The second helper also builds the SQL `WHERE …` clause from a row's
//! primary-key JSON map; that lives here rather than in the write loop so
//! its all-or-nothing safety stance is testable in isolation.

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::crdt::hlc::{compare_hlc_strings, hlc_min};
use crate::crdt::trigger::is_safe_identifier;

/// Groups arbitrary items into ascending-HLC-ordered buckets, keyed by
/// `hlc_of`. Generic so the same grouping logic serves both a plain
/// `ColumnChange` batch and an index-tagged one — see the engine's use for
/// the latter; a plain-`ColumnChange` batch groups with `hlc_of: |c|
/// c.hlc_timestamp.as_str()`. All writes issued inside the same sender-side
/// transaction share a timestamp, so `hlc_timestamp` is the semantic
/// grouping key.
pub fn group_by_hlc_key<T>(items: Vec<T>, hlc_of: impl Fn(&T) -> &str) -> Vec<(String, Vec<T>)> {
    let mut groups: HashMap<String, Vec<T>> = HashMap::new();
    for item in items {
        groups
            .entry(hlc_of(&item).to_string())
            .or_default()
            .push(item);
    }
    let mut ordered: Vec<(String, Vec<T>)> = groups.into_iter().collect();
    ordered.sort_by(|a, b| compare_hlc_strings(&a.0, &b.0));
    ordered
}

/// Groups arbitrary items by a `(String, String)` row key and returns rows in
/// ascending order of their earliest HLC, with a stable tie-break on the key
/// itself. Generic so the same grouping logic serves both a plain
/// `ColumnChange` batch (`key_of: |c| (c.table_name.clone(),
/// c.row_pks.clone())`) and an index-tagged one. Plain `HashMap` iteration is
/// unordered — a remote batch that spans several transactions would apply
/// rows in nondeterministic order and future logic that observes the
/// per-row sequence would see inconsistent results across runs.
pub fn group_by_row_key_hlc_ordered<T>(
    items: impl IntoIterator<Item = T>,
    key_of: impl Fn(&T) -> (String, String),
    hlc_of: impl Fn(&T) -> &str,
) -> Vec<((String, String), Vec<T>)> {
    let mut map: HashMap<(String, String), Vec<T>> = HashMap::new();
    for item in items {
        map.entry(key_of(&item)).or_default().push(item);
    }
    let mut entries: Vec<((String, String), Vec<T>)> = map.into_iter().collect();
    entries.sort_by(|a, b| {
        let a_min = hlc_min(a.1.iter().map(&hlc_of));
        let b_min = hlc_min(b.1.iter().map(&hlc_of));
        let primary = match (a_min, b_min) {
            (Some(am), Some(bm)) => compare_hlc_strings(am, bm),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        // Stable tie-break on the group key so equal-min-HLC rows have a
        // deterministic order across runs.
        primary.then_with(|| a.0.cmp(&b.0))
    });
    entries
}

/// Build a `WHERE …` clause that matches a row by its CRDT primary-key map.
///
/// Returns `Some((where_clause, values))` if every PK column name is a safe
/// identifier; returns `None` if **any** column name fails the safety check
/// or the map is empty. Skipping individual columns is wrong: with a partial
/// `WHERE` the resulting DELETE/UPDATE matches *more* than the intended row
/// (potentially every row if every column was unsafe). All-or-nothing is
/// the only correct stance.
///
/// `JsonValue::Null` PK values are emitted as `"col" IS NULL` (not `= ?`)
/// so they participate in the match. Non-null values are placeholders and
/// contribute to the returned `values` list in map iteration order.
pub fn build_pk_where_from_map(
    row_pks: &serde_json::Map<String, JsonValue>,
) -> Option<(String, Vec<JsonValue>)> {
    if row_pks.is_empty() {
        return None;
    }
    let mut where_parts: Vec<String> = Vec::with_capacity(row_pks.len());
    let mut values: Vec<JsonValue> = Vec::with_capacity(row_pks.len());
    for (col_name, value) in row_pks {
        if !is_safe_identifier(col_name) {
            return None;
        }
        match value {
            JsonValue::Null => {
                where_parts.push(format!("\"{col_name}\" IS NULL"));
            }
            _ => {
                where_parts.push(format!("\"{col_name}\" = ?"));
                values.push(value.clone());
            }
        }
    }
    Some((where_parts.join(" AND "), values))
}

#[cfg(test)]
mod tests;
