//! `INSERT OR REPLACE` deletes the old row without firing the delete
//! trigger, so no delete-log entry reaches the peers.

use super::super::*;
use super::{source, Fixture};
use crate::db::error::DatabaseError;
use crate::error::Error;

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT);",
);

#[test]
fn insert_or_replace_and_replace_into_are_refused() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    let db = Database::open(fx.config.clone()).unwrap();
    db.write(|tx| tx.execute("INSERT INTO items (id, title) VALUES ('r1', 'a')", &[]))
        .unwrap();
    for sql in [
        "INSERT OR REPLACE INTO items (id, title) VALUES ('r1', 'b')",
        "REPLACE INTO items (id, title) VALUES ('r1', 'b')",
    ] {
        let err = db.write(|tx| tx.execute(sql, &[])).unwrap_err();
        assert!(
            matches!(
                err,
                Error::Database(DatabaseError::UnsupportedStatement { .. })
            ),
            "`{sql}`: got {err:?}"
        );
    }
    let title: String = db
        .with_locked_conn(|conn| {
            conn.query_row("SELECT title FROM items WHERE id = 'r1'", [], |r| r.get(0))
                .map_err(Error::from)
        })
        .unwrap();
    assert_eq!(title, "a");
}
