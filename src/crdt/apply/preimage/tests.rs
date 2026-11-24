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
fn blob_value_preimage_differs_from_text_spelling_the_tag() {
    // The tagged BLOB object is a JSON object, the lookalike a JSON
    // string, so the signed bytes keep the storage type apart.
    let blob = change("t", "r", "c", "1", json!({"$blob_hex": "deadbeef"}));
    let text = change("t", "r", "c", "1", json!(r#"{"$blob_hex":"deadbeef"}"#));
    assert_eq!(
        column_sig_preimage(&blob),
        column_sig_preimage(&blob.clone())
    );
    assert_ne!(column_sig_preimage(&blob), column_sig_preimage(&text));
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
