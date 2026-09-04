//! Row-parsing helpers for `select` / `select_with_crdt` results.
//!
//! Rows returned from the crate's select functions are `Vec<serde_json::Value>`
//! where each row is an array. These helpers give type-safe access by index.

use serde_json::Value as JsonValue;

/// Reads a string at `idx`, returning `""` if missing or of a non-string type.
pub fn get_string(row: &[JsonValue], idx: usize) -> String {
    row.get(idx)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Reads a boolean at `idx` from SQLite's 0/1 integer encoding. Returns
/// `false` for missing or non-integer values.
pub fn get_bool(row: &[JsonValue], idx: usize) -> bool {
    row.get(idx)
        .and_then(|v| v.as_i64())
        .map(|v| v != 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn get_string_returns_str_when_present() {
        let row = vec![json!("hello"), json!(42)];
        assert_eq!(get_string(&row, 0), "hello");
    }

    #[test]
    fn get_string_returns_empty_on_missing_index() {
        let row = vec![json!("hello")];
        assert_eq!(get_string(&row, 5), "");
    }

    #[test]
    fn get_string_returns_empty_on_non_string_type() {
        let row = vec![json!(42), json!(true), json!(null)];
        assert_eq!(get_string(&row, 0), "");
        assert_eq!(get_string(&row, 1), "");
        assert_eq!(get_string(&row, 2), "");
    }

    #[test]
    fn get_bool_reads_sqlite_integer_encoding() {
        let row = vec![json!(0), json!(1), json!(2)];
        assert!(!get_bool(&row, 0));
        assert!(get_bool(&row, 1));
        // SQLite booleans are only 0/1, but any non-zero integer follows the
        // conventional truthiness rule.
        assert!(get_bool(&row, 2));
    }

    #[test]
    fn get_bool_returns_false_on_missing_or_non_integer() {
        let row = vec![json!("true"), json!(null)];
        assert!(!get_bool(&row, 0));
        assert!(!get_bool(&row, 1));
        assert!(!get_bool(&row, 5));
    }
}
