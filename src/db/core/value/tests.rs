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
        ValueConverter::convert_params(&[json!(1), json!("x"), json!(null), json!(true)]).unwrap();
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
