//! A case-only edit of a `COLLATE NOCASE` column is still a change: the
//! update trigger must compare values binary, or the edit is never stamped
//! and never synced.

use super::super::*;
use super::{source, Fixture};

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, name TEXT COLLATE NOCASE);",
);

#[test]
fn a_case_only_edit_of_a_nocase_column_is_scanned() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    let db = Database::open(fx.config.clone()).unwrap();
    db.write(|tx| tx.execute("INSERT INTO items (id, name) VALUES ('r1', 'abc')", &[]))
        .unwrap();
    db.scan_dirty_tables().unwrap();
    let before = db
        .scan_table_for_local_changes("items", None, ScanFilters::default())
        .unwrap();
    let name_before = before.iter().find(|c| c.column_name == "name").unwrap();
    let hlc_before = name_before.hlc_timestamp.clone();

    db.write(|tx| tx.execute("UPDATE items SET name = 'ABC' WHERE id = 'r1'", &[]))
        .unwrap();

    let after = db
        .scan_table_for_local_changes("items", None, ScanFilters::default())
        .unwrap();
    let name = after.iter().find(|c| c.column_name == "name").unwrap();
    assert_ne!(name.hlc_timestamp, hlc_before, "the edit must be stamped");
    assert_eq!(name.value, serde_json::json!("ABC"));
}
