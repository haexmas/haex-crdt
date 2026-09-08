//! Per-row change emission for [`super::scan_table_for_local_changes`].
//!
//! Split out of `mod.rs` to keep both files inside the repo's file-size
//! cap; it holds the row-level half of the scan — value decoding, the
//! canonical PK JSON encoding, and the per-column HLC comparison.

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::{hlc_is_from_node, hlc_is_newer};
use crate::crdt::scanner::ColumnChange;
use crate::crdt::trigger::ColumnInfo;
use crate::db::core::convert_value_ref_to_json;
use crate::db::error::DatabaseError;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

/// Per-row column-change emitter. Reads column values from `row`, builds
/// the canonical PK JSON in schema-declaration order, applies the
/// `row_pks_filter` allow-list if any, then for each data column emits a
/// change when its per-column HLC (or the row-level fallback) is strictly
/// newer than `after_hlc` and — if `origin_node_filter` is set — was
/// written by this node.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_row_changes(
    row: &rusqlite::Row<'_>,
    select_columns: &[&str],
    pk_columns: &[&ColumnInfo],
    data_columns: &[&ColumnInfo],
    table_name: &str,
    after_hlc: Option<&str>,
    device_id: &str,
    origin_node_filter: Option<u128>,
    row_pks_filter: Option<&HashSet<String>>,
    out: &mut Vec<ColumnChange>,
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
        let key_json = serde_json::to_string(&pk.name).map_err(|e| DatabaseError::QueryError {
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

            out.push(ColumnChange {
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
