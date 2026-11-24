use super::*;
use serde_json::json;

// ---------- should_propagate_delete --------------------------------

const H1: &str = "0000000000000001/abcdef";
const H2: &str = "0000000000000002/abcdef";

#[test]
fn propagate_when_target_row_absent() {
    assert!(should_propagate_delete(H1, None));
}

#[test]
fn propagate_when_target_row_older() {
    assert!(should_propagate_delete(H2, Some(H1)));
}

#[test]
fn propagate_when_target_row_equal_hlc() {
    // Tie goes to the delete — matches `delete_shadows_insert` (>=).
    assert!(should_propagate_delete(H1, Some(H1)));
}

#[test]
fn do_not_propagate_when_target_row_strictly_newer() {
    assert!(!should_propagate_delete(H1, Some(H2)));
}

// ---------- delete_shadows_insert ----------------------------------

#[test]
fn shadow_when_insert_older_than_delete() {
    assert!(delete_shadows_insert(H2, H1));
}

#[test]
fn shadow_on_equal_hlc() {
    assert!(delete_shadows_insert(H1, H1));
}

#[test]
fn no_shadow_when_insert_strictly_newer() {
    assert!(!delete_shadows_insert(H1, H2));
}

// ---------- insert_suppressed_by_deletes ---------------------------

#[test]
fn insert_suppressed_by_matching_pk_and_shadowing_hlc() {
    let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
    let candidates = vec![(pks.clone(), H2.to_string())];
    assert!(insert_suppressed_by_deletes(&pks, H1, &candidates));
}

#[test]
fn insert_not_suppressed_when_pks_differ() {
    let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
    let other = serde_json::Map::from_iter([("id".to_string(), json!("row-b"))]);
    let candidates = vec![(other, H2.to_string())];
    assert!(!insert_suppressed_by_deletes(&pks, H1, &candidates));
}

#[test]
fn insert_not_suppressed_when_it_is_strictly_newer_than_all_deletes() {
    let pks = serde_json::Map::from_iter([("id".to_string(), json!("row-a"))]);
    let candidates = vec![(pks.clone(), H1.to_string())];
    assert!(!insert_suppressed_by_deletes(&pks, H2, &candidates));
}
