//! The HLC must stay ahead of every remote timestamp that was applied, also
//! across a restart: otherwise a local edit made after reopening can carry a
//! lower HLC than the remote value it replaces and lose last-write-wins.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::json;

use super::super::*;
use super::{source, Fixture};
use crate::crdt::columns::HLC_TIMESTAMP_COLUMN;
use crate::crdt::hlc::compare_hlc_strings;
use crate::crdt::scanner::ColumnChange;

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT);",
);

fn remote_hlc(seconds_ahead: u64) -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + seconds_ahead;
    format!("{}/abcdef0000000000000000000000", secs << 32)
}

fn row_hlc(db: &Database) -> String {
    db.with_locked_conn(|conn| {
        conn.query_row(
            &format!("SELECT {HLC_TIMESTAMP_COLUMN} FROM items WHERE id = 'r1'"),
            [],
            |r| r.get(0),
        )
        .map_err(crate::Error::from)
    })
    .unwrap()
}

#[test]
fn local_edit_after_reopen_is_newer_than_an_applied_remote_hlc() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    let db = Database::open(fx.config.clone()).unwrap();
    let remote = remote_hlc(60);
    db.apply_remote_changes(vec![ColumnChange {
        table_name: "items".to_string(),
        row_pks: r#"{"id":"r1"}"#.to_string(),
        column_name: "title".to_string(),
        hlc_timestamp: remote.clone(),
        value: json!("remote"),
        device_id: String::new(),
        sig: None,
    }])
    .unwrap();
    drop(db);

    let db = Database::open(DatabaseConfig {
        create_if_missing: false,
        ..fx.config
    })
    .unwrap();
    db.write(|tx| tx.execute("UPDATE items SET title = 'local' WHERE id = 'r1'", &[]))
        .unwrap();

    assert_eq!(
        compare_hlc_strings(&row_hlc(&db), &remote),
        std::cmp::Ordering::Greater,
        "a local edit after reopen must be newer than the remote value it replaced"
    );
}
