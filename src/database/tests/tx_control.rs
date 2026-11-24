//! `Database::write` owns the transaction: a statement that commits, rolls
//! back or nests one would defeat the all-or-nothing guarantee.

use super::super::*;
use super::{source, Fixture};
use crate::db::error::DatabaseError;
use crate::error::Error;

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT);",
);

#[test]
fn transaction_control_statements_are_refused_and_nothing_persists() {
    for sql in [
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
        "SAVEPOINT sp",
        "RELEASE SAVEPOINT sp",
    ] {
        let fx = Fixture::with_source(source(&[ITEMS]));
        let db = Database::open(fx.config.clone()).unwrap();
        let result = db.write(|tx| {
            tx.execute("INSERT INTO items (id, title) VALUES ('r1', 'a')", &[])?;
            tx.execute(sql, &[])?;
            Err::<(), _>(Error::Database(DatabaseError::ValidationError {
                reason: "caller aborts".into(),
            }))
        });
        assert!(result.is_err());
        let count: i64 = db
            .with_locked_conn(|conn| {
                conn.query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
                    .map_err(Error::from)
            })
            .unwrap();
        assert_eq!(count, 0, "`{sql}` must not let the insert survive");
    }
}

#[test]
fn a_refused_transaction_statement_names_itself() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    let db = Database::open(fx.config.clone()).unwrap();
    let err = db.write(|tx| tx.execute("COMMIT", &[])).unwrap_err();
    assert!(
        matches!(
            err,
            Error::Database(DatabaseError::UnsupportedStatement { .. })
        ),
        "got {err:?}"
    );
}
