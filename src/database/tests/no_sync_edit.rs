//! An edit confined to a `_no_sync` column is device-local state: it must not
//! make the row look newer than a remote delete.

use super::super::*;
use super::{source, Fixture};
use crate::crdt::columns::{DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN};

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT, cache_no_sync TEXT);",
);

fn item_count(db: &Database) -> i64 {
    db.with_locked_conn(|conn| {
        conn.query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .map_err(crate::Error::from)
    })
    .unwrap()
}

fn sync(from: &Database, to: &Database, table: &str) {
    let changes = from
        .scan_table_for_local_changes(table, None, ScanFilters::default())
        .unwrap();
    to.apply_remote_changes(changes).unwrap();
}

fn advance_past_delete(delete_source: &Database, replica: &Database) {
    let delete_hlc = delete_source
        .with_locked_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {HLC_TIMESTAMP_COLUMN} FROM {DELETED_ROWS_TABLE} \
                     WHERE table_name = 'items' LIMIT 1"
                ),
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(crate::Error::from)
        })
        .unwrap();
    replica.inner.hlc.advance_past_remote(&delete_hlc).unwrap();
}

fn replicas() -> (Fixture, Database, Fixture, Database) {
    let fa = Fixture::with_source(source(&[ITEMS]));
    let fb = Fixture::with_source(source(&[ITEMS]));
    let a = Database::open(fa.config.clone()).unwrap();
    let b = Database::open(fb.config.clone()).unwrap();
    (fa, a, fb, b)
}

#[test]
fn local_only_edit_does_not_shield_a_row_from_a_remote_delete() {
    let (_fa, a, _fb, b) = replicas();
    a.write(|tx| tx.execute("INSERT INTO items (id, title) VALUES ('r1', 'a')", &[]))
        .unwrap();
    sync(&a, &b, "items");

    a.write(|tx| tx.execute("DELETE FROM items WHERE id = 'r1'", &[]))
        .unwrap();
    advance_past_delete(&a, &b);
    b.write(|tx| tx.execute("UPDATE items SET cache_no_sync = 'x' WHERE id = 'r1'", &[]))
        .unwrap();

    sync(&a, &b, DELETED_ROWS_TABLE);

    assert_eq!(item_count(&b), 0, "the remote delete must reach the row");
}

#[test]
fn edit_of_a_synced_column_after_the_delete_still_keeps_the_row() {
    let (_fa, a, _fb, b) = replicas();
    a.write(|tx| tx.execute("INSERT INTO items (id, title) VALUES ('r1', 'a')", &[]))
        .unwrap();
    sync(&a, &b, "items");

    a.write(|tx| tx.execute("DELETE FROM items WHERE id = 'r1'", &[]))
        .unwrap();
    advance_past_delete(&a, &b);
    b.write(|tx| tx.execute("UPDATE items SET title = 'newer' WHERE id = 'r1'", &[]))
        .unwrap();

    sync(&a, &b, DELETED_ROWS_TABLE);

    assert_eq!(
        item_count(&b),
        1,
        "a newer synced edit wins over the delete"
    );
}
