//! `Database::write` / `Database::read`: one HLC per transaction, rollback,
//! size limit, BLOB parameters, meta-column guard, read-only guard.

use rusqlite::params;

use super::super::*;
use super::{source, Fixture};
use crate::crdt::columns::HLC_TIMESTAMP_COLUMN;
use crate::crdt::scanner::ScanFilters;
use crate::db::error::DatabaseError;
use crate::error::Error;

const ITEMS: &str = "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT, data BLOB);";
const CACHE: &str = "CREATE TABLE cache_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);";

fn open() -> (Fixture, Database) {
    let fx = Fixture::with_source(source(&[("0001_items", ITEMS), ("0002_cache", CACHE)]));
    let db = Database::open(fx.config.clone()).unwrap();
    (fx, db)
}

fn row_hlc(db: &Database, id: &str) -> Option<String> {
    db.read(|conn| {
        Ok(conn
            .query_row(
                &format!("SELECT {HLC_TIMESTAMP_COLUMN} FROM items WHERE id = ?1"),
                [id],
                |row| row.get(0),
            )
            .map_err(DatabaseError::from)?)
    })
    .unwrap()
}

fn count(db: &Database, table: &str) -> i64 {
    db.read(|conn| {
        Ok(conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .map_err(DatabaseError::from)?)
    })
    .unwrap()
}

#[test]
fn writes_in_one_transaction_share_one_hlc_and_one_transaction_group() {
    let (_fx, db) = open();
    db.write(|tx| {
        tx.execute(
            "INSERT INTO items (id, body) VALUES (?1, ?2)",
            params!["a", "first"],
        )?;
        tx.execute(
            "INSERT INTO items (id, body) VALUES (?1, ?2)",
            params!["b", "second"],
        )?;
        tx.execute(
            "UPDATE items SET body = ?1 WHERE id = ?2",
            params!["first!", "a"],
        )?;
        Ok(())
    })
    .unwrap();

    let hlc_a = row_hlc(&db, "a").expect("a is stamped");
    let hlc_b = row_hlc(&db, "b").expect("b is stamped");
    assert_eq!(hlc_a, hlc_b);

    let changes = db
        .scan_table_for_local_changes("items", None, ScanFilters::default())
        .unwrap();
    assert!(!changes.is_empty());
    assert!(changes.iter().all(|c| c.hlc_timestamp == hlc_a));
}

#[test]
fn separate_transactions_get_separate_hlcs() {
    let (_fx, db) = open();
    for id in ["a", "b"] {
        db.write(|tx| {
            tx.execute("INSERT INTO items (id) VALUES (?1)", params![id])?;
            Ok(())
        })
        .unwrap();
    }
    assert_ne!(row_hlc(&db, "a"), row_hlc(&db, "b"));
}

#[test]
fn an_error_in_the_closure_rolls_back_every_write() {
    let (_fx, db) = open();
    db.write(|tx| {
        tx.execute("INSERT INTO items (id) VALUES ('keep')", params![])?;
        Ok(())
    })
    .unwrap();

    let result: crate::Result<()> = db.write(|tx| {
        tx.execute("INSERT INTO items (id) VALUES ('gone')", params![])?;
        tx.execute("DELETE FROM items WHERE id = 'keep'", params![])?;
        Err(DatabaseError::ValidationError {
            reason: "caller aborts".to_string(),
        }
        .into())
    });

    assert!(result.is_err());
    assert_eq!(count(&db, "items"), 1);
    assert_eq!(count(&db, "haex_deleted_rows"), 0);
}

#[test]
fn no_sync_tables_are_written_without_crdt_stamping() {
    let (_fx, db) = open();
    db.write(|tx| {
        tx.execute(
            "INSERT INTO cache_no_sync (id, body) VALUES (?1, ?2)",
            params!["c", "local"],
        )?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count(&db, "cache_no_sync"), 1);
}

#[test]
fn blob_parameters_are_stored_unchanged() {
    let (_fx, db) = open();
    let bytes: Vec<u8> = vec![0, 159, 146, 150, 255];
    db.write(|tx| {
        tx.execute(
            "INSERT INTO items (id, data) VALUES (?1, ?2)",
            params!["bin", bytes],
        )?;
        Ok(())
    })
    .unwrap();
    let stored: Vec<u8> = db
        .read(|conn| {
            Ok(conn
                .query_row("SELECT data FROM items WHERE id = 'bin'", [], |row| {
                    row.get(0)
                })
                .map_err(DatabaseError::from)?)
        })
        .unwrap();
    assert_eq!(stored, bytes);
}

#[test]
fn the_size_limit_counts_all_writes_of_a_transaction() {
    let mut fx = Fixture::with_source(source(&[("0001_items", ITEMS)]));
    fx.config.max_transaction_bytes = 10;
    let db = Database::open(fx.config.clone()).unwrap();
    assert_eq!(db.max_transaction_bytes(), 10);

    let err = db
        .write(|tx| {
            tx.execute("INSERT INTO items (id) VALUES (?1)", params!["abcd"])?;
            tx.execute("INSERT INTO items (id) VALUES (?1)", params!["efgh"])?;
            tx.execute("INSERT INTO items (id) VALUES (?1)", params!["ijkl"])?;
            Ok(())
        })
        .unwrap_err();

    assert!(
        matches!(
            err,
            Error::Database(DatabaseError::TransactionTooLarge {
                bytes: 12,
                limit: 10
            })
        ),
        "got {err:?}"
    );
    assert_eq!(count(&db, "items"), 0);
}

#[test]
fn writes_to_crdt_meta_columns_are_rejected() {
    let (_fx, db) = open();
    let err = db
        .write(|tx| {
            tx.execute(
                &format!("INSERT INTO items (id, {HLC_TIMESTAMP_COLUMN}) VALUES (?1, ?2)"),
                params!["x", "forged"],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(
            &err,
            Error::Database(DatabaseError::CrdtMetaColumnWriteForbidden { column })
                if column == HLC_TIMESTAMP_COLUMN
        ),
        "got {err:?}"
    );
}

#[test]
fn reads_inside_a_write_see_earlier_writes_and_returning_is_stamped() {
    let (_fx, db) = open();
    let (seen, returned) = db
        .write(|tx| {
            tx.execute("INSERT INTO items (id, body) VALUES ('r', 'x')", params![])?;
            let seen: Option<String> = tx.query_row(
                "SELECT body FROM items WHERE id = ?1",
                params!["r"],
                |row| row.get(0),
            )?;
            let returned: Vec<String> = tx.query_map(
                "UPDATE items SET body = 'y' WHERE id = 'r' RETURNING body",
                params![],
                |row| row.get(0),
            )?;
            Ok((seen, returned))
        })
        .unwrap();
    assert_eq!(seen.as_deref(), Some("x"));
    assert_eq!(returned, vec!["y".to_string()]);
    assert!(row_hlc(&db, "r").is_some());
}

#[test]
fn query_row_returns_none_for_no_row() {
    let (_fx, db) = open();
    let found: Option<String> = db
        .write(|tx| {
            tx.query_row(
                "SELECT body FROM items WHERE id = 'missing'",
                params![],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert!(found.is_none());
}

#[test]
fn read_refuses_writes_and_leaves_the_connection_writable_afterwards() {
    let (_fx, db) = open();
    let attempt = db.read(|conn| {
        Ok(conn
            .query_row("INSERT INTO items (id) VALUES ('sneaky')", [], |_| Ok(()))
            .map_err(DatabaseError::from)?)
    });
    assert!(attempt.is_err());
    assert_eq!(count(&db, "items"), 0);

    db.write(|tx| {
        tx.execute("INSERT INTO items (id) VALUES ('ok')", params![])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count(&db, "items"), 1);
}

#[test]
fn read_cannot_disable_the_write_guard() {
    let (_fx, db) = open();
    let attempt = db.read(|conn| {
        conn.query_row("PRAGMA query_only = OFF", [], |_| Ok(()))
            .map_err(DatabaseError::from)?;
        Ok(conn
            .query_row("INSERT INTO items (id) VALUES ('sneaky')", [], |_| Ok(()))
            .map_err(DatabaseError::from)?)
    });

    assert!(attempt.is_err());
    assert_eq!(count(&db, "items"), 0);
}

#[test]
fn callback_panics_do_not_poison_the_connection() {
    let (_fx, db) = open();
    let write_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: crate::Result<()> = db.write(|tx| {
            tx.execute("INSERT INTO items (id) VALUES ('rolled_back')", &[])?;
            panic!("simulated write callback panic");
        });
    }));
    assert!(write_panic.is_err());

    let read_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: crate::Result<()> = db.read(|_| panic!("simulated read callback panic"));
    }));
    assert!(read_panic.is_err());

    assert_eq!(count(&db, "items"), 0);
    db.write(|tx| {
        tx.execute("INSERT INTO items (id) VALUES ('usable')", &[])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count(&db, "items"), 1);
}

#[test]
fn a_constraint_violation_keeps_its_sqlite_error_code() {
    let (_fx, db) = open();
    let insert = |id: &str| {
        db.write(|tx| {
            tx.execute("INSERT INTO items (id) VALUES (?1)", params![id])?;
            Ok(())
        })
    };
    insert("dup").unwrap();
    let err = insert("dup").unwrap_err();

    let code = err
        .sqlite_error()
        .and_then(rusqlite::Error::sqlite_error_code);
    assert_eq!(
        code,
        Some(rusqlite::ErrorCode::ConstraintViolation),
        "got {err:?}"
    );
}

#[derive(Debug, thiserror::Error)]
#[error("consumer failure {0}")]
struct ConsumerFailure(u8);

#[test]
fn a_consumer_error_from_the_closure_can_be_downcast() {
    let (_fx, db) = open();
    let err = db
        .write(|_tx| -> crate::Result<()> { Err(Error::consumer(ConsumerFailure(7))) })
        .unwrap_err();

    match err {
        Error::Consumer(inner) => {
            let failure = inner
                .downcast_ref::<ConsumerFailure>()
                .expect("the consumer's own error type survives");
            assert_eq!(failure.0, 7);
        }
        other => panic!("expected Error::Consumer, got {other:?}"),
    }
}
