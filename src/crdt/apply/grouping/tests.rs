use super::*;
use crate::crdt::scanner::ColumnChange;
use serde_json::json;

fn change(table: &str, pk: &str, col: &str, hlc: &str) -> ColumnChange {
    ColumnChange {
        table_name: table.to_string(),
        row_pks: pk.to_string(),
        column_name: col.to_string(),
        hlc_timestamp: hlc.to_string(),
        value: JsonValue::Null,
        device_id: String::new(),
        sig: None,
    }
}

// HLC strings share format `time-part/node-hex`. Time-part is decimal ns
// — use fixed-width numeric prefixes so the relative order is unambiguous.
const HLC1: &str = "0000000000000001/abcdef";
const HLC2: &str = "0000000000000002/abcdef";
const HLC3: &str = "0000000000000003/abcdef";
const HLC4: &str = "0000000000000004/abcdef";

fn tx_hlc(c: &ColumnChange) -> &str {
    c.hlc_timestamp.as_str()
}

fn row_key(c: &ColumnChange) -> (String, String) {
    (c.table_name.clone(), c.row_pks.clone())
}

// ---------- group_by_hlc_key (ColumnChange case) -------------------

#[test]
fn tx_grouping_collapses_same_hlc_into_one_group() {
    let changes = vec![
        change("t", r#"{"id":"a"}"#, "c1", HLC1),
        change("t", r#"{"id":"b"}"#, "c1", HLC1),
    ];
    let grouped = group_by_hlc_key(changes, tx_hlc);
    assert_eq!(grouped.len(), 1);
    assert_eq!(grouped[0].1.len(), 2);
}

#[test]
fn tx_grouping_orders_groups_ascending_by_hlc() {
    let changes = vec![
        change("t", r#"{"id":"a"}"#, "c", HLC3),
        change("t", r#"{"id":"b"}"#, "c", HLC1),
        change("t", r#"{"id":"c"}"#, "c", HLC2),
    ];
    let grouped = group_by_hlc_key(changes, tx_hlc);
    let hlcs: Vec<&str> = grouped.iter().map(|(h, _)| h.as_str()).collect();
    assert_eq!(hlcs, vec![HLC1, HLC2, HLC3]);
}

// ---------- group_by_row_key_hlc_ordered (ColumnChange case) -------

#[test]
fn row_grouping_orders_by_min_hlc_per_row() {
    // Row A has changes at HLC4 + HLC1; Row B has one at HLC2.
    // min(A)=HLC1 < min(B)=HLC2 → A first, even though A holds HLC4.
    let changes = vec![
        change("t", r#"{"id":"a"}"#, "c1", HLC4),
        change("t", r#"{"id":"b"}"#, "c", HLC2),
        change("t", r#"{"id":"a"}"#, "c2", HLC1),
    ];
    let ordered = group_by_row_key_hlc_ordered(changes, row_key, tx_hlc);
    assert_eq!(ordered.len(), 2);
    assert_eq!(ordered[0].0 .1, r#"{"id":"a"}"#);
    assert_eq!(ordered[0].1.len(), 2);
    assert_eq!(ordered[1].0 .1, r#"{"id":"b"}"#);
}

#[test]
fn row_grouping_is_deterministic_across_input_orderings() {
    let baseline: Vec<ColumnChange> = (0..12)
        .map(|i| {
            let hlc = format!("{i:016}/abcdef");
            change("t", &format!(r#"{{"id":"r{i}"}}"#), "c", &hlc)
        })
        .collect();
    let baseline_keys: Vec<String> = group_by_row_key_hlc_ordered(baseline, row_key, tx_hlc)
        .into_iter()
        .map(|(k, _)| k.1)
        .collect();

    let reversed: Vec<ColumnChange> = (0..12)
        .rev()
        .map(|i| {
            let hlc = format!("{i:016}/abcdef");
            change("t", &format!(r#"{{"id":"r{i}"}}"#), "c", &hlc)
        })
        .collect();
    let reversed_keys: Vec<String> = group_by_row_key_hlc_ordered(reversed, row_key, tx_hlc)
        .into_iter()
        .map(|(k, _)| k.1)
        .collect();

    assert_eq!(
        baseline_keys, reversed_keys,
        "iteration order must be deterministic and HLC-driven"
    );
}

// ---------- build_pk_where_from_map --------------------------------

fn pk_map(pairs: &[(&str, JsonValue)]) -> serde_json::Map<String, JsonValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

#[test]
fn where_returns_none_for_empty_map() {
    assert!(build_pk_where_from_map(&serde_json::Map::new()).is_none());
}

#[test]
fn where_handles_safe_identifiers_with_values() {
    let map = pk_map(&[("id", json!("x")), ("group_id", json!("g"))]);
    let (clause, values) = build_pk_where_from_map(&map).expect("safe");
    assert!(clause.contains("\"id\" = ?"));
    assert!(clause.contains("\"group_id\" = ?"));
    assert!(clause.contains(" AND "));
    assert_eq!(values.len(), 2);
}

#[test]
fn where_uses_is_null_for_null_values() {
    let map = pk_map(&[("id", json!("x")), ("optional", JsonValue::Null)]);
    let (clause, values) = build_pk_where_from_map(&map).expect("safe");
    assert!(clause.contains("\"optional\" IS NULL"));
    assert_eq!(values.len(), 1, "null values do not bind a placeholder");
}

#[test]
fn where_returns_none_when_any_column_is_unsafe() {
    // All-or-nothing: a partial WHERE from remaining columns would
    // over-match. `evil; DROP TABLE` alone in the map would still fail.
    let map = pk_map(&[("id", json!("x")), ("evil; DROP TABLE", json!("y"))]);
    assert!(build_pk_where_from_map(&map).is_none());
}

#[test]
fn where_returns_none_when_only_unsafe_columns() {
    let map = pk_map(&[("evil; --", json!("y"))]);
    assert!(build_pk_where_from_map(&map).is_none());
}
