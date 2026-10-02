//! `Database::write_guarded` / `Database::read_guarded` (spec 017, T005):
//! the authorizer runs only around the caller's statement, a denial and an
//! interrupt are typed errors, a statement tail is refused, an interrupt
//! rolls the transaction back, and a query reports its column names even
//! when it returns no row.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::params;

use super::super::*;
use super::{source, Fixture};
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::db::error::DatabaseError;
use crate::error::Error;
use crate::table_names::{TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES};

const ITEMS: &str = "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);";
const SECRET: &str = "CREATE TABLE secret (id TEXT PRIMARY KEY NOT NULL, value TEXT);";

/// A recursive query that runs long enough for the progress callback to fire.
const SLOW_QUERY: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
                          WHERE x < 10000000) SELECT count(*) FROM c";

type AuthLog = Arc<Mutex<Vec<(String, Option<String>)>>>;

fn open() -> (Fixture, Database) {
    let fx = Fixture::with_source(source(&[("0001_items", ITEMS), ("0002_secret", SECRET)]));
    let db = Database::open(fx.config.clone()).unwrap();
    (fx, db)
}

fn allow_all() -> SqlGuard {
    SqlGuard {
        authorizer: Arc::new(|_: &AuthContext<'_>| Authorization::Allow),
        progress: None,
    }
}

/// Allows everything and records each call as `(action, accessor)`.
fn recording() -> (SqlGuard, AuthLog) {
    let log: AuthLog = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&log);
    let guard = SqlGuard {
        authorizer: Arc::new(move |ctx: &AuthContext<'_>| {
            sink.lock().unwrap().push((
                format!("{:?}", ctx.action),
                ctx.accessor.map(str::to_string),
            ));
            Authorization::Allow
        }),
        progress: None,
    };
    (guard, log)
}

/// Denies every access to `table` and allows the rest.
fn deny_table(table: &'static str) -> SqlGuard {
    SqlGuard {
        authorizer: Arc::new(move |ctx: &AuthContext<'_>| match ctx.action {
            AuthAction::Read { table_name, .. }
            | AuthAction::Insert { table_name }
            | AuthAction::Update { table_name, .. }
            | AuthAction::Delete { table_name }
                if table_name.eq_ignore_ascii_case(table) =>
            {
                Authorization::Deny
            }
            _ => Authorization::Allow,
        }),
        progress: None,
    }
}

fn database_error(err: Error) -> DatabaseError {
    match err {
        Error::Database(err) => err,
        other => panic!("expected a database error, got {other:?}"),
    }
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
fn a_guarded_write_is_stamped_like_a_plain_write() {
    let (_fx, db) = open();
    let rowid = db
        .write_guarded(&allow_all(), |tx| {
            tx.execute(
                "INSERT INTO items (id, body) VALUES (?1, ?2)",
                params!["a", "x"],
            )?;
            Ok(tx.last_insert_rowid())
        })
        .unwrap();
    assert!(rowid > 0);

    let (hlc, map): (String, String) = db
        .read(|conn| {
            Ok(conn
                .query_row(
                    &format!(
                        "SELECT {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN} FROM items WHERE id = 'a'"
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DatabaseError::from)?)
        })
        .unwrap();
    let map: serde_json::Value = serde_json::from_str(&map).unwrap();
    assert_eq!(map["body"], serde_json::Value::String(hlc));
}

#[test]
fn the_authorizer_is_not_invoked_for_the_crates_own_statements() {
    let (_fx, db) = open();
    let (guard, log) = recording();
    db.write_guarded(&guard, |tx| {
        tx.execute("INSERT INTO items (id, body) VALUES ('a', 'x')", params![])?;
        Ok(())
    })
    .unwrap();

    let log = log.lock().unwrap();
    assert!(
        log.iter()
            .any(|(action, accessor)| accessor.is_none() && action.contains("\"items\"")),
        "the caller's statement is authorized: {log:?}"
    );
    let own = log.iter().find(|(action, accessor)| {
        accessor.is_none()
            && (action.contains(TABLE_CRDT_CONFIGS) || action.contains("current_hlc"))
    });
    assert!(own.is_none(), "crate statement authorized: {own:?}");
}

#[test]
fn trigger_bodies_are_authorized_with_the_trigger_as_accessor() {
    let (_fx, db) = open();
    let (guard, log) = recording();
    db.write_guarded(&guard, |tx| {
        tx.execute("INSERT INTO items (id, body) VALUES ('a', 'x')", params![])?;
        Ok(())
    })
    .unwrap();

    let log = log.lock().unwrap();
    let dirty: Vec<_> = log
        .iter()
        .filter(|(action, _)| action.contains(TABLE_CRDT_DIRTY_TABLES))
        .collect();
    assert!(
        !dirty.is_empty(),
        "the insert trigger was compiled: {log:?}"
    );
    assert!(
        dirty
            .iter()
            .all(|(_, accessor)| accessor.as_deref() == Some("z_dirty_items_insert")),
        "{dirty:?}"
    );
}

#[test]
fn a_denial_is_a_typed_error_on_write_and_read() {
    let (_fx, db) = open();
    let guard = deny_table("secret");

    let err = db
        .write_guarded(&guard, |tx| {
            tx.execute(
                "INSERT INTO secret (id, value) VALUES ('s', 'v')",
                params![],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(database_error(err), DatabaseError::SqlGuardDenied { .. }),
        "write"
    );

    let err = db
        .read_guarded(&guard, |conn| {
            conn.query_with_columns("SELECT value FROM secret", [], |row| {
                row.get::<_, String>(0)
            })?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(database_error(err), DatabaseError::SqlGuardDenied { .. }),
        "read"
    );

    // The guard is gone afterwards: plain writes to the table work again.
    db.write(|tx| {
        tx.execute(
            "INSERT INTO secret (id, value) VALUES ('s', 'v')",
            params![],
        )?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count(&db, "secret"), 1);
}

#[test]
fn read_guarded_keeps_the_read_only_rule() {
    let (_fx, db) = open();
    let err = db
        .read_guarded(&allow_all(), |conn| {
            conn.query_with_columns(
                "INSERT INTO items (id) VALUES ('x') RETURNING id",
                [],
                |row| row.get::<_, String>(0),
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(database_error(err), DatabaseError::SqlGuardDenied { .. }),
        "the read-only rule still applies"
    );
    assert_eq!(count(&db, "items"), 0);

    let ids = db
        .read_guarded(&allow_all(), |conn| {
            Ok(conn.query_map("SELECT id FROM items", [], |row| row.get::<_, String>(0))?)
        })
        .unwrap();
    assert!(ids.is_empty());
}

#[test]
fn a_statement_tail_is_refused_on_write() {
    let (_fx, db) = open();
    let err = db
        .write_guarded(&allow_all(), |tx| {
            tx.execute(
                "INSERT INTO items (id) VALUES ('a'); DELETE FROM items",
                params![],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::MultipleStatements { .. }
        ),
        "tail refused"
    );
    assert_eq!(count(&db, "items"), 0);

    // Whitespace and comments after the statement are not a tail.
    db.write_guarded(&allow_all(), |tx| {
        tx.execute("INSERT INTO items (id) VALUES ('a'); -- done\n ", params![])?;
        tx.query_with_columns("SELECT id FROM items; /* trailing */", params![], |row| {
            row.get::<_, String>(0)
        })?;
        Ok(())
    })
    .unwrap();
    assert_eq!(count(&db, "items"), 1);
}

#[test]
fn a_statement_tail_is_refused_on_read() {
    let (_fx, db) = open();
    let err = db
        .read_guarded(&allow_all(), |conn| {
            conn.query_with_columns("SELECT 1; SELECT 2", [], |row| row.get::<_, i64>(0))?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::MultipleStatements { .. }
        ),
        "tail refused"
    );
}

#[test]
fn a_progress_interrupt_rolls_back_the_transaction() {
    let (_fx, db) = open();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let guard = SqlGuard {
        progress: Some((100, Arc::new(move || flag.load(Ordering::SeqCst)))),
        ..allow_all()
    };

    let err = db
        .write_guarded(&guard, |tx| {
            tx.execute("INSERT INTO items (id) VALUES ('a')", params![])?;
            stop.store(true, Ordering::SeqCst);
            let interrupted = tx
                .query_with_columns(SLOW_QUERY, params![], |row| row.get::<_, i64>(0))
                .unwrap_err();
            assert!(
                matches!(
                    &interrupted,
                    Error::Database(DatabaseError::SqlGuardInterrupted { .. })
                ),
                "got {interrupted:?}"
            );
            // Even a caller that swallows the error cannot commit.
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::TransactionAborted { .. }
        ),
        "commit refused"
    );
    assert_eq!(count(&db, "items"), 0);

    // The progress callback is gone afterwards.
    let total: i64 = db
        .write(|tx| {
            Ok(tx
                .query_row(SLOW_QUERY, params![], |row| row.get(0))?
                .unwrap_or(0))
        })
        .unwrap();
    assert_eq!(total, 10_000_000);
}

#[test]
fn a_progress_interrupt_is_a_typed_error_on_read() {
    let (_fx, db) = open();
    let guard = SqlGuard {
        progress: Some((100, Arc::new(|| true))),
        ..allow_all()
    };

    let err = db
        .read_guarded(&guard, |conn| {
            conn.query_with_columns(SLOW_QUERY, [], |row| row.get::<_, i64>(0))
        })
        .unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::SqlGuardInterrupted { .. }
        ),
        "interrupt reported as such"
    );
}

#[test]
fn a_query_with_no_rows_still_reports_its_columns() {
    let (_fx, db) = open();
    let written = db
        .write_guarded(&allow_all(), |tx| {
            tx.query_with_columns("SELECT id, body FROM items", params![], |row| {
                row.get::<_, String>(0)
            })
        })
        .unwrap();
    assert_eq!(written.columns, vec!["id", "body"]);
    assert!(written.rows.is_empty());

    let read = db
        .read_guarded(&allow_all(), |conn| {
            conn.query_with_columns("SELECT body, id AS key FROM items", [], |row| {
                row.get::<_, String>(0)
            })
        })
        .unwrap();
    assert_eq!(read.columns, vec!["body", "key"]);
    assert!(read.rows.is_empty());
}
