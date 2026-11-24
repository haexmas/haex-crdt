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
mod tests;
