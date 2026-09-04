//! Bidirectional conversion between `serde_json::Value` (the crate's public
//! row / param format) and `rusqlite`'s `SqlValue` / `ValueRef`.
//!
//! - SQLite has no boolean type; JSON booleans map to `INTEGER 0/1` and back
//!   to numbers on read (callers use `db::row::get_bool` to decode).
//! - JSON arrays and objects are stored as JSON-encoded text.
//! - BLOBs come back as base64-encoded strings (JSON has no binary type).

use crate::db::error::DatabaseError;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use rusqlite::types::{Value as SqlValue, ValueRef};
use serde_json::Value as JsonValue;

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
            JsonValue::Array(_) | JsonValue::Object(_) => serde_json::to_string(json_val)
                .map(SqlValue::Text)
                .map_err(|e| DatabaseError::SerializationError {
                    reason: format!("Failed to serialize JSON param: {e}"),
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

/// Converts a `rusqlite::ValueRef` into a `serde_json::Value`. BLOBs are
/// base64-encoded (STANDARD alphabet) because JSON cannot carry raw bytes.
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
        ValueRef::Blob(b) => JsonValue::String(STANDARD.encode(b)),
    };
    Ok(json_val)
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
        let out =
            ValueConverter::convert_params(&[json!(1), json!("x"), json!(null), json!(true)])
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
        assert_eq!(convert_value_ref_to_json(ValueRef::Null).unwrap(), json!(null));
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
    fn value_ref_blob_returned_as_base64_string() {
        let out = convert_value_ref_to_json(ValueRef::Blob(&[0xDE, 0xAD, 0xBE, 0xEF])).unwrap();
        // base64 STANDARD encoding of [0xDE, 0xAD, 0xBE, 0xEF] = "3q2+7w=="
        assert_eq!(out, json!("3q2+7w=="));
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
