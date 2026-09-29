//! Bidirectional conversion between `serde_json::Value` (the value format of
//! [`crate::ColumnChange`]) and `rusqlite`'s `SqlValue` / `ValueRef`.
//!
//! - SQLite has no boolean type; JSON booleans map to `INTEGER 0/1` and back
//!   to numbers on read.
//! - JSON arrays and objects are stored as JSON-encoded text, except the
//!   tagged BLOB object below.
//! - BLOBs travel as the tagged object `{"$blob_hex":"<lowercase hex>"}`
//!   (see [`BLOB_HEX_TAG`]) and decode back to a BLOB. TEXT stays a plain
//!   JSON string, so a TEXT value that happens to spell the tag is never
//!   mistaken for a BLOB.

use crate::db::error::DatabaseError;
use rusqlite::types::{Value as SqlValue, ValueRef};
use serde_json::Value as JsonValue;

/// Key of the single-entry JSON object a BLOB value is encoded as:
/// `{"$blob_hex":"deadbeef"}`. The same shape appears in column values
/// ([`crate::ColumnChange::value`]) and inside `row_pks`.
///
/// Hex rather than base64 because the BEFORE-DELETE trigger has to build
/// the same `row_pks` in SQL, and SQLite has `hex()` built in but no base64.
/// The digits are lowercase — the trigger wraps `hex()` in `lower()` — and
/// decoding accepts only lowercase, so one BLOB has exactly one JSON
/// spelling and `row_pks` strings stay comparable byte for byte.
pub const BLOB_HEX_TAG: &str = "$blob_hex";

pub struct ValueConverter;

impl ValueConverter {
    pub fn json_to_rusqlite_value(json_val: &JsonValue) -> Result<SqlValue, DatabaseError> {
        match json_val {
            JsonValue::Null => Ok(SqlValue::Null),
            JsonValue::Bool(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(SqlValue::Integer(i))
                } else if let Some(u) = n.as_u64() {
                    match i64::try_from(u) {
                        Ok(i) => Ok(SqlValue::Integer(i)),
                        Err(_) => Ok(SqlValue::Text(n.to_string())),
                    }
                } else if let Some(f) = n.as_f64() {
                    Ok(SqlValue::Real(f))
                } else {
                    Ok(SqlValue::Text(n.to_string()))
                }
            }
            JsonValue::String(s) => Ok(SqlValue::Text(s.clone())),
            JsonValue::Object(map) if map.len() == 1 && map.contains_key(BLOB_HEX_TAG) => {
                decode_blob_tag(&map[BLOB_HEX_TAG])
            }
            JsonValue::Array(_) | JsonValue::Object(_) => serde_json::to_string(json_val)
                .map(SqlValue::Text)
                .map_err(|source| DatabaseError::SerializationError {
                    context: "JSON param".to_string(),
                    source,
                }),
        }
    }

    pub fn convert_params(params: &[JsonValue]) -> Result<Vec<SqlValue>, DatabaseError> {
        params.iter().map(Self::json_to_rusqlite_value).collect()
    }

    /// Converts an owned `SqlValue` to JSON by delegating to
    /// [`convert_value_ref_to_json`].
    pub fn rusqlite_value_to_json(sql_value: &SqlValue) -> JsonValue {
        let value_ref = match sql_value {
            SqlValue::Null => ValueRef::Null,
            SqlValue::Integer(n) => ValueRef::Integer(*n),
            SqlValue::Real(f) => ValueRef::Real(*f),
            SqlValue::Text(s) => ValueRef::Text(s.as_bytes()),
            SqlValue::Blob(b) => ValueRef::Blob(b),
        };
        convert_value_ref_to_json(value_ref).unwrap_or(JsonValue::Null)
    }
}

/// Converts a `rusqlite::ValueRef` into a `serde_json::Value`. BLOBs become
/// the tagged object `{"$blob_hex":"<lowercase hex>"}` (see
/// [`BLOB_HEX_TAG`]) because JSON cannot carry raw bytes.
pub fn convert_value_ref_to_json(value_ref: ValueRef) -> Result<JsonValue, DatabaseError> {
    let json_val = match value_ref {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Integer(i) => JsonValue::Number(i.into()),
        ValueRef::Real(f) => JsonValue::Number(
            serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
        ),
        ValueRef::Text(t) => {
            let s = String::from_utf8_lossy(t).to_string();
            JsonValue::String(s)
        }
        ValueRef::Blob(b) => {
            let mut tagged = serde_json::Map::with_capacity(1);
            tagged.insert(BLOB_HEX_TAG.to_string(), JsonValue::String(encode_hex(b)));
            JsonValue::Object(tagged)
        }
    };
    Ok(json_val)
}

/// Lowercase hex encoding, the spelling SQLite's `lower(hex(x))` produces.
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decodes the payload of a `{"$blob_hex": …}` object. A malformed payload
/// is an error rather than JSON text: the tag says the sender meant a BLOB.
fn decode_blob_tag(payload: &JsonValue) -> Result<SqlValue, DatabaseError> {
    payload
        .as_str()
        .and_then(decode_hex)
        .map(SqlValue::Blob)
        .ok_or_else(|| DatabaseError::ValidationError {
            reason: format!("'{BLOB_HEX_TAG}' must hold an even-length lowercase hex string"),
        })
}

/// Inverse of [`encode_hex`]; `None` for odd length or any character outside
/// `0-9a-f` (uppercase included, to keep one spelling per BLOB).
fn decode_hex(s: &str) -> Option<Vec<u8>> {
    fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    let (pairs, odd) = s.as_bytes().as_chunks::<2>();
    if !odd.is_empty() {
        return None;
    }
    pairs
        .iter()
        .map(|[hi, lo]| Some((nibble(*hi)? << 4) | nibble(*lo)?))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_null_maps_to_sql_null() {
        let v = ValueConverter::json_to_rusqlite_value(&json!(null)).unwrap();
        assert!(matches!(v, SqlValue::Null));
    }

    #[test]
    fn json_bool_encoded_as_integer_zero_or_one() {
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json!(true)).unwrap(),
            SqlValue::Integer(1)
        );
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json!(false)).unwrap(),
            SqlValue::Integer(0)
        );
    }

    #[test]
    fn json_i64_maps_to_sql_integer() {
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json!(42_i64)).unwrap(),
            SqlValue::Integer(42)
        );
    }

    #[test]
    fn json_f64_maps_to_sql_real() {
        let v = ValueConverter::json_to_rusqlite_value(&json!(3.5_f64)).unwrap();
        assert!(matches!(v, SqlValue::Real(x) if (x - 3.5).abs() < f64::EPSILON));
    }

    #[test]
    fn large_unsigned_integer_is_preserved_as_text() {
        let number = serde_json::Number::from(u64::MAX);
        let v = ValueConverter::json_to_rusqlite_value(&JsonValue::Number(number)).unwrap();
        assert_eq!(v, SqlValue::Text(u64::MAX.to_string()));
    }

    #[test]
    fn json_string_maps_to_sql_text() {
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json!("hi")).unwrap(),
            SqlValue::Text("hi".to_string())
        );
    }

    #[test]
    fn json_array_and_object_are_stored_as_json_text() {
        let arr = ValueConverter::json_to_rusqlite_value(&json!([1, 2, 3])).unwrap();
        assert!(matches!(arr, SqlValue::Text(ref s) if s == "[1,2,3]"));

        let obj = ValueConverter::json_to_rusqlite_value(&json!({"k": "v"})).unwrap();
        assert!(matches!(obj, SqlValue::Text(ref s) if s == r#"{"k":"v"}"#));
    }

    #[test]
    fn convert_params_preserves_order_and_types() {
        let out = ValueConverter::convert_params(&[json!(1), json!("x"), json!(null), json!(true)])
            .unwrap();
        assert_eq!(
            out,
            vec![
                SqlValue::Integer(1),
                SqlValue::Text("x".to_string()),
                SqlValue::Null,
                SqlValue::Integer(1),
            ]
        );
    }

    #[test]
    fn value_ref_null_maps_to_json_null() {
        assert_eq!(
            convert_value_ref_to_json(ValueRef::Null).unwrap(),
            json!(null)
        );
    }

    #[test]
    fn value_ref_integer_maps_to_json_number() {
        assert_eq!(
            convert_value_ref_to_json(ValueRef::Integer(42)).unwrap(),
            json!(42)
        );
    }

    #[test]
    fn value_ref_text_decodes_utf8_lossy() {
        assert_eq!(
            convert_value_ref_to_json(ValueRef::Text(b"hello")).unwrap(),
            json!("hello")
        );
    }

    #[test]
    fn value_ref_blob_returned_as_tagged_hex_object() {
        let out = convert_value_ref_to_json(ValueRef::Blob(&[0xDE, 0xAD, 0xBE, 0xEF])).unwrap();
        assert_eq!(out, json!({"$blob_hex": "deadbeef"}));
    }

    #[test]
    fn tagged_hex_object_decodes_to_sql_blob() {
        let v = ValueConverter::json_to_rusqlite_value(&json!({"$blob_hex": "00ff10"})).unwrap();
        assert_eq!(v, SqlValue::Blob(vec![0x00, 0xFF, 0x10]));
        let empty = ValueConverter::json_to_rusqlite_value(&json!({"$blob_hex": ""})).unwrap();
        assert_eq!(empty, SqlValue::Blob(Vec::new()));
    }

    #[test]
    fn blob_roundtrips_through_json() {
        let bytes: Vec<u8> = (0..=255).collect();
        let json = ValueConverter::rusqlite_value_to_json(&SqlValue::Blob(bytes.clone()));
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json).unwrap(),
            SqlValue::Blob(bytes)
        );
    }

    #[test]
    fn malformed_blob_tag_is_rejected() {
        for bad in [
            json!({"$blob_hex": "abc"}),
            json!({"$blob_hex": "DEADBEEF"}),
            json!({"$blob_hex": "zz"}),
            json!({"$blob_hex": 12}),
        ] {
            assert!(
                matches!(
                    ValueConverter::json_to_rusqlite_value(&bad),
                    Err(DatabaseError::ValidationError { .. })
                ),
                "{bad} must not decode"
            );
        }
    }

    #[test]
    fn tag_lookalikes_stay_text() {
        // A string spelling the tag is TEXT, and so is an object that carries
        // the tag next to another key.
        let s = r#"{"$blob_hex":"deadbeef"}"#;
        assert_eq!(
            ValueConverter::json_to_rusqlite_value(&json!(s)).unwrap(),
            SqlValue::Text(s.to_string())
        );
        let two_keys = json!({"$blob_hex": "00", "x": 1});
        assert!(matches!(
            ValueConverter::json_to_rusqlite_value(&two_keys).unwrap(),
            SqlValue::Text(_)
        ));
    }

    #[test]
    fn rusqlite_value_to_json_roundtrips_via_value_ref() {
        assert_eq!(
            ValueConverter::rusqlite_value_to_json(&SqlValue::Text("abc".to_string())),
            json!("abc")
        );
        assert_eq!(
            ValueConverter::rusqlite_value_to_json(&SqlValue::Integer(7)),
            json!(7)
        );
        assert_eq!(
            ValueConverter::rusqlite_value_to_json(&SqlValue::Null),
            json!(null)
        );
    }
}
