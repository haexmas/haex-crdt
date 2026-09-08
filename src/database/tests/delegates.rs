//! Method delegates on `Database` — `apply_remote_changes`,
//! `scan_table_for_local_changes`, `cleanup_deleted_rows`.

use serde_json::json;

use super::super::*;
use super::{source, Fixture};
use crate::crdt::cleanup::RetentionPolicy;
use crate::crdt::hlc::device_uuid_to_hlc_node;
use crate::crdt::scanner::ColumnChange;

#[test]
fn store_apply_remote_changes_uses_the_configured_signature_provider() {
    let fx = Fixture::with_source(source(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let db = Database::open(fx.config).unwrap();
    let hlc = "9999999999999999/abcdef0000000000000000000000";
    let changes = vec![ColumnChange {
        table_name: "items".to_string(),
        row_pks: r#"{"id":"r1"}"#.to_string(),
        column_name: "body".to_string(),
        hlc_timestamp: hlc.to_string(),
        value: json!("hello"),
        device_id: String::new(),
        sig: None,
    }];
    let report = db.apply_remote_changes(changes).unwrap();
    assert_eq!(report.applied, 1);
}

#[test]
fn store_scan_after_apply_returns_the_applied_change() {
    let fx = Fixture::with_source(source(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let db = Database::open(fx.config.clone()).unwrap();

    // Apply a change whose HLC comes from a foreign node id — the scanner
    // is then invoked without the ping-pong origin filter, so it must
    // return the applied row regardless of author.
    let foreign_hlc = "9999999999999999/deadbeefdeadbeefdeadbeefdeadbe";
    db.apply_remote_changes(vec![ColumnChange {
        table_name: "items".to_string(),
        row_pks: r#"{"id":"r1"}"#.to_string(),
        column_name: "body".to_string(),
        hlc_timestamp: foreign_hlc.to_string(),
        value: json!("v"),
        device_id: String::new(),
        sig: None,
    }])
    .unwrap();

    let changes = db
        .scan_table_for_local_changes("items", None, ScanFilters::default())
        .unwrap();
    assert!(
        changes.iter().any(|c| c.column_name == "body"),
        "scan must return the body change; got {changes:?}"
    );
    // Sanity: the origin_node filter for our own device must drop the row
    // (author was the foreign node) — proves the filter is wired.
    let node = device_uuid_to_hlc_node(&fx.device.to_string()).unwrap();
    let self_only = db
        .scan_table_for_local_changes(
            "items",
            None,
            ScanFilters {
                origin_node: Some(node),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        self_only.is_empty(),
        "foreign-authored change must be filtered out; got {self_only:?}",
    );
}

#[test]
fn store_cleanup_delegate_reports_zero_pruned_on_empty_log() {
    let fx = Fixture::new();
    let db = Database::open(fx.config).unwrap();
    let report = db
        .cleanup_deleted_rows(RetentionPolicy::All, |_, _| Ok(()))
        .unwrap();
    assert_eq!(report.rows_deleted, 0);
}
