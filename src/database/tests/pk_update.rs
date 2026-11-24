//! Changing a primary key through UPDATE would fork the row across peers.

use super::super::*;
use super::{source, Fixture};
use crate::db::error::DatabaseError;
use crate::error::Error;

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT);",
);

#[test]
fn updating_a_primary_key_column_is_refused_but_other_columns_are_not() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    let db = Database::open(fx.config.clone()).unwrap();
    db.write(|tx| tx.execute("INSERT INTO items (id, title) VALUES ('r1', 'a')", &[]))
        .unwrap();
    let err = db
        .write(|tx| tx.execute("UPDATE items SET id = 'r2' WHERE id = 'r1'", &[]))
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::Database(DatabaseError::UnsupportedStatement { .. })
        ),
        "got {err:?}"
    );
    db.write(|tx| tx.execute("UPDATE items SET title = 'b' WHERE id = 'r1'", &[]))
        .unwrap();
}
