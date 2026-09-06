//! Canonical byte preimage for column signatures.
//!
//! Both the local sign-on-write path and the remote verify path must feed
//! identical bytes into the [`SignatureProvider`](crate::SignatureProvider)
//! — otherwise every signature fails to verify. This module owns that byte
//! layout so consumers on either side can produce the same preimage from
//! the same [`ColumnChange`] fields.
//!
//! # Format
//!
//! Length-prefixed concatenation of the five signed fields, in fixed order:
//!
//! ```text
//! [u32 len][table_name bytes]
//! [u32 len][row_pks bytes]
//! [u32 len][column_name bytes]
//! [u32 len][hlc_timestamp bytes]
//! [u32 len][value bytes]                // canonical JSON of `value`
//! ```
//!
//! Length prefixes are big-endian `u32`. Canonical JSON for `value` recursively
//! sorts object keys before serialization, independent of `serde_json`'s map
//! implementation or feature configuration.
//!
//! Rationale for length-prefixing (vs. a delimiter): field payloads may
//! contain arbitrary bytes including delimiter candidates. Prefixes make the
//! preimage parseable and unambiguous for future re-verification.

use serde_json::Value as JsonValue;

use crate::crdt::scanner::ColumnChange;

/// Build the canonical signature preimage for `change` — the byte sequence
/// the provider signs on the local side and verifies on the remote side.
pub fn column_sig_preimage(change: &ColumnChange) -> Vec<u8> {
    let value = canonicalize_json(&change.value);
    let value_bytes = serde_json::to_vec(&value).unwrap_or_default();
    length_prefixed([
        change.table_name.as_bytes(),
        change.row_pks.as_bytes(),
        change.column_name.as_bytes(),
        change.hlc_timestamp.as_bytes(),
        &value_bytes,
    ])
}

/// Recursively sort JSON object keys so signatures do not depend on map
/// insertion order or on the enabled `serde_json` features.
fn canonicalize_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort();
            let mut canonical = serde_json::Map::new();
            for key in keys {
                canonical.insert(key.clone(), canonicalize_json(&object[key]));
            }
            JsonValue::Object(canonical)
        }
        JsonValue::Array(values) => {
            JsonValue::Array(values.iter().map(canonicalize_json).collect())
        }
        scalar => scalar.clone(),
    }
}

fn length_prefixed(parts: [&[u8]; 5]) -> Vec<u8> {
    let total = parts.iter().map(|p| 4 + p.len()).sum();
    let mut buf = Vec::with_capacity(total);
    for part in parts {
        buf.extend_from_slice(&(part.len() as u32).to_be_bytes());
        buf.extend_from_slice(part);
    }
    buf
}

/// Convenience: build the preimage from the individual fields, without
/// requiring a `ColumnChange` value. Local sign-on-write callers hold the
/// fields separately and don't want to construct a scanner record just to
/// sign one column.
pub fn column_sig_preimage_from_parts(
    table_name: &str,
    row_pks: &str,
    column_name: &str,
    hlc_timestamp: &str,
    value: &JsonValue,
) -> Vec<u8> {
    let canonical_value = canonicalize_json(value);
    let value_bytes = serde_json::to_vec(&canonical_value).unwrap_or_default();
    length_prefixed([
        table_name.as_bytes(),
        row_pks.as_bytes(),
        column_name.as_bytes(),
        hlc_timestamp.as_bytes(),
        &value_bytes,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn change(table: &str, pks: &str, col: &str, hlc: &str, value: JsonValue) -> ColumnChange {
        ColumnChange {
            table_name: table.to_string(),
            row_pks: pks.to_string(),
            column_name: col.to_string(),
            hlc_timestamp: hlc.to_string(),
            value,
            device_id: String::new(),
            sig: None,
        }
    }

    #[test]
    fn preimage_is_stable_across_calls() {
        let c = change("items", r#"{"id":"a"}"#, "body", "1/abc", json!("hello"));
        assert_eq!(column_sig_preimage(&c), column_sig_preimage(&c));
    }

    #[test]
    fn preimage_from_parts_matches_from_change() {
        let value = json!({"nested": [1, 2, 3]});
        let c = change("items", r#"{"id":"a"}"#, "meta", "1/abc", value.clone());
        assert_eq!(
            column_sig_preimage(&c),
            column_sig_preimage_from_parts("items", r#"{"id":"a"}"#, "meta", "1/abc", &value),
        );
    }

    #[test]
    fn field_swap_produces_different_preimage() {
        // A preimage that ignored field boundaries could hash-collide when
        // two fields are swapped. Length prefixes prevent that.
        let a = change("t", "r", "col_a", "1", json!("x"));
        let b = change("t", "r", "col_b", "1", json!("x"));
        assert_ne!(column_sig_preimage(&a), column_sig_preimage(&b));
    }

    #[test]
    fn value_serialization_key_order_is_stable() {
        let one = change("t", "r", "c", "1", json!({"b": 2, "a": 1}));
        let two = change("t", "r", "c", "1", json!({"a": 1, "b": 2}));
        assert_eq!(column_sig_preimage(&one), column_sig_preimage(&two));
    }

    #[test]
    fn nested_object_key_order_is_canonicalized() {
        let mut first_nested = serde_json::Map::new();
        first_nested.insert("z".to_string(), json!({"b": 2, "a": 1}));
        first_nested.insert("a".to_string(), json!([json!({"d": 4, "c": 3})]));
        let mut second_nested = serde_json::Map::new();
        second_nested.insert("a".to_string(), json!([json!({"c": 3, "d": 4})]));
        second_nested.insert("z".to_string(), json!({"a": 1, "b": 2}));

        let one = change("t", "r", "c", "1", JsonValue::Object(first_nested));
        let two = change("t", "r", "c", "1", JsonValue::Object(second_nested));
        assert_eq!(column_sig_preimage(&one), column_sig_preimage(&two));
        assert_eq!(
            column_sig_preimage(&one),
            column_sig_preimage_from_parts("t", "r", "c", "1", &two.value),
        );
    }

    #[test]
    fn preimage_shape_length_prefixes_each_field() {
        // Minimal end-to-end shape check — five fields, each prefixed with
        // a big-endian u32 length. Total = 5 * 4 + sum(field lengths).
        let c = change("aa", "bb", "cc", "dd", json!("e"));
        let value_bytes = serde_json::to_vec(&json!("e")).unwrap();
        let expected_len = 5 * 4 + 2 + 2 + 2 + 2 + value_bytes.len();
        assert_eq!(column_sig_preimage(&c).len(), expected_len);
    }
}
