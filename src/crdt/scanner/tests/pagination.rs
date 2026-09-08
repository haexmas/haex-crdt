//! Tests for [`paginate_changes`] and the [`Paginable`] trait it is
//! generic over.

use super::*;
use serde::Serialize;

fn change(hlc: &str, table: &str, col: &str, value: &str) -> ColumnChange {
    ColumnChange {
        table_name: table.to_string(),
        row_pks: r#"{"id":"r"}"#.to_string(),
        column_name: col.to_string(),
        hlc_timestamp: hlc.to_string(),
        value: json!(value),
        device_id: "dev".to_string(),
        sig: None,
    }
}

#[test]
fn paginate_empty_input_returns_empty_and_no_more() {
    let (page, has_more) = paginate_changes(Vec::<ColumnChange>::new(), 1000);
    assert!(page.is_empty());
    assert!(!has_more);
}

#[test]
fn paginate_packs_multiple_hlc_groups_when_they_fit() {
    let changes = vec![
        change("100/n", "t", "a", "aa"),
        change("100/n", "t", "b", "bb"),
        change("200/n", "t", "a", "cc"),
    ];
    let (page, has_more) = paginate_changes(changes.clone(), 10_000);
    assert_eq!(page.len(), 3);
    assert!(!has_more);
}

#[test]
fn paginate_never_splits_a_transaction_hlc_group() {
    // A tiny budget: two large-ish groups. The first fits (≥1 rule); the
    // second exceeds the remaining budget and defers as one atomic group.
    let big_group = vec![
        change("100/n", "t", "a", &"x".repeat(50)),
        change("100/n", "t", "b", &"x".repeat(50)),
    ];
    let follow_up = vec![change("200/n", "t", "a", &"y".repeat(50))];
    let mut all = big_group.clone();
    all.extend(follow_up);

    let (page, has_more) = paginate_changes(all, 200);
    // The first group's two changes stay together; the follow-up defers.
    let hlcs: HashSet<&str> = page.iter().map(|c| c.hlc_timestamp.as_str()).collect();
    assert_eq!(hlcs.len(), 1);
    assert!(hlcs.contains("100/n"));
    assert!(has_more);
}

#[test]
fn paginate_ge_one_rule_admits_first_group_even_when_over_budget() {
    // First group alone exceeds the budget: it must still be emitted, and
    // has_more must be true if later groups exist.
    let over = vec![
        change("100/n", "t", "a", &"x".repeat(500)),
        change("200/n", "t", "a", "b"),
    ];
    let (page, has_more) = paginate_changes(over, 50);
    // Only the first group appears.
    let hlcs: HashSet<&str> = page.iter().map(|c| c.hlc_timestamp.as_str()).collect();
    assert!(hlcs.contains("100/n"));
    assert!(!hlcs.contains("200/n"));
    assert!(has_more);
}

#[test]
fn paginate_orders_groups_ascending_by_hlc() {
    // Insertion order is deliberately shuffled — the output must be sorted
    // by HLC ascending.
    let mixed = vec![
        change("300/n", "t", "a", "c"),
        change("100/n", "t", "a", "a"),
        change("200/n", "t", "a", "b"),
    ];
    let (page, _) = paginate_changes(mixed, 10_000);
    let hlcs: Vec<&str> = page.iter().map(|c| c.hlc_timestamp.as_str()).collect();
    assert_eq!(hlcs, vec!["100/n", "200/n", "300/n"]);
}

#[test]
fn paginate_ge_one_rule_reports_no_more_when_the_over_budget_group_is_the_only_one() {
    // Same ≥1 admission as above, but with nothing following it: the group
    // is emitted AND has_more must stay false, or a client would ask for a
    // page that does not exist and loop forever on the same cursor.
    let only = vec![change("100/n", "t", "a", &"x".repeat(500))];
    let (page, has_more) = paginate_changes(only, 50);
    assert_eq!(page.len(), 1);
    assert!(!has_more, "no later group exists, so this page is the last");
}

#[test]
fn paginate_accepts_a_consumer_defined_paginable_type() {
    // The regression test for the generic: a change type the crate knows
    // nothing about still gets the grouping and ordering invariants.
    #[derive(Debug, Serialize)]
    struct ScopedChange {
        hlc: String,
        payload: String,
    }
    impl Paginable for ScopedChange {
        fn hlc_timestamp(&self) -> &str {
            &self.hlc
        }
    }

    let changes = vec![
        ScopedChange {
            hlc: "200/n".to_string(),
            payload: "c".to_string(),
        },
        ScopedChange {
            hlc: "100/n".to_string(),
            payload: "a".to_string(),
        },
        ScopedChange {
            hlc: "100/n".to_string(),
            payload: "b".to_string(),
        },
    ];

    let (page, has_more) = paginate_changes(changes, 10_000);
    let hlcs: Vec<&str> = page.iter().map(|c| c.hlc.as_str()).collect();
    assert_eq!(hlcs, vec!["100/n", "100/n", "200/n"]);
    assert!(!has_more);
}
