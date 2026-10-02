//! Schema changes inside a guarded write (spec 017, T007): triggers follow
//! the schema in the same transaction, schema mode keeps foreign keys from
//! cascading and checks them before the commit, a table rebuild keeps the
//! CRDT metadata of every row, and local mode creates plain tables.

use std::sync::Arc;

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::params;
use serde_json::Value as JsonValue;

use super::super::*;
use super::{source, Fixture};
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN};
use crate::db::error::DatabaseError;
use crate::error::Error;
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;

const ITEMS: &str = "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);";
const NOTES: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, \
                     item_id TEXT NOT NULL REFERENCES items(id) ON DELETE CASCADE, text TEXT);";

const SCHEMA: GuardedWriteOptions = GuardedWriteOptions {
    schema_mode: true,
    local: false,
};
const LOCAL: GuardedWriteOptions = GuardedWriteOptions {
    schema_mode: true,
    local: true,
};

fn open() -> (Fixture, Database) {
    let fx = Fixture::with_source(source(&[("0001_items", ITEMS), ("0002_notes", NOTES)]));
    let db = Database::open(fx.config.clone()).unwrap();
    (fx, db)
}

fn allow_all() -> SqlGuard {
    SqlGuard {
        authorizer: Arc::new(|_: &AuthContext<'_>| Authorization::Allow),
        progress: None,
    }
}

fn database_error(err: Error) -> DatabaseError {
    match err {
        Error::Database(err) => err,
        other => panic!("expected a database error, got {other:?}"),
    }
}

/// Runs each statement in one guarded write.
fn run(db: &Database, options: GuardedWriteOptions, statements: &[&str]) -> crate::Result<()> {
    db.write_guarded_with(&allow_all(), options, |tx| {
        for sql in statements {
            tx.execute(sql, params![])?;
        }
        Ok(())
    })
}

fn columns(db: &Database, table: &str) -> Vec<String> {
    db.read_guarded(&allow_all(), |conn| {
        Ok(conn
            .query_with_columns(&format!("SELECT * FROM {table} LIMIT 0"), [], |_| Ok(()))?
            .columns)
    })
    .unwrap()
}

fn triggers(db: &Database, table: &str) -> Vec<String> {
    db.read(|conn| {
        Ok(conn
            .query_map(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?1 \
                 ORDER BY name",
                [table],
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

/// `(row HLC, column-HLC map)` of one row.
fn meta(db: &Database, table: &str, id: &str) -> (String, JsonValue) {
    let (hlc, map): (String, String) = db
        .read(|conn| {
            Ok(conn
                .query_row(
                    &format!(
                        "SELECT {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN} FROM {table} WHERE id = ?1"
                    ),
                    [id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(DatabaseError::from)?)
        })
        .unwrap();
    (hlc, serde_json::from_str(&map).unwrap())
}

fn crdt_triggers(table: &str) -> Vec<String> {
    ["delete", "insert", "update"]
        .iter()
        .map(|kind| format!("z_dirty_{table}_{kind}"))
        .collect()
}

#[test]
fn add_column_then_insert_gets_a_column_hlc() {
    let (_fx, db) = open();
    run(
        &db,
        GuardedWriteOptions::default(),
        &["ALTER TABLE items ADD COLUMN extra TEXT"],
    )
    .unwrap();
    run(
        &db,
        GuardedWriteOptions::default(),
        &["INSERT INTO items (id, body, extra) VALUES ('a', 'x', 'y')"],
    )
    .unwrap();

    let (hlc, map) = meta(&db, "items", "a");
    assert_eq!(map["extra"], JsonValue::String(hlc.clone()));
    assert_eq!(map["body"], JsonValue::String(hlc.clone()));

    run(
        &db,
        GuardedWriteOptions::default(),
        &["UPDATE items SET extra = 'z' WHERE id = 'a'"],
    )
    .unwrap();
    let (later, map) = meta(&db, "items", "a");
    assert_ne!(later, hlc);
    assert_eq!(map["extra"], JsonValue::String(later));
    assert_eq!(map["body"], JsonValue::String(hlc));
}

#[test]
fn drop_column_on_a_synced_table_recreates_its_triggers() {
    let (_fx, db) = open();
    run(
        &db,
        GuardedWriteOptions::default(),
        &["ALTER TABLE items ADD COLUMN extra TEXT"],
    )
    .unwrap();
    run(
        &db,
        GuardedWriteOptions::default(),
        &["ALTER TABLE items DROP COLUMN extra"],
    )
    .unwrap();
    assert_eq!(triggers(&db, "items"), crdt_triggers("items"));

    run(
        &db,
        GuardedWriteOptions::default(),
        &["INSERT INTO items (id, body) VALUES ('a', 'x')"],
    )
    .unwrap();
    let (_, map) = meta(&db, "items", "a");
    assert!(map.get("extra").is_none());
    assert!(map.get("body").is_some());
}

#[test]
fn a_drizzle_rebuild_keeps_hlcs_and_writes_no_delete_markers() {
    let (_fx, db) = open();
    db.write(|tx| {
        tx.execute(
            "INSERT INTO items (id, body) VALUES ('a', 'x'), ('b', 'y')",
            params![],
        )?;
        tx.execute(
            "INSERT INTO notes (id, item_id, text) VALUES ('n', 'a', 't')",
            params![],
        )?;
        Ok(())
    })
    .unwrap();
    let before = (meta(&db, "items", "a"), meta(&db, "items", "b"));
    let markers = count(&db, DELETED_ROWS_TABLE);

    db.write_guarded_with(&allow_all(), SCHEMA, |tx| {
        tx.execute(
            "CREATE TABLE `__new_items` (`id` text PRIMARY KEY NOT NULL, `body` text, `extra` text)",
            params![],
        )?;
        tx.copy_rows_verbatim(
            "INSERT INTO `__new_items`(\"id\", \"body\") SELECT \"id\", \"body\" FROM `items`",
            params![],
        )?;
        tx.execute("DROP TABLE `items`", params![])?;
        tx.execute("ALTER TABLE `__new_items` RENAME TO `items`", params![])?;
        Ok(())
    })
    .unwrap();

    assert_eq!((meta(&db, "items", "a"), meta(&db, "items", "b")), before);
    assert_eq!(count(&db, "notes"), 1, "the drop did not cascade");
    assert_eq!(count(&db, DELETED_ROWS_TABLE), markers, "no delete markers");
    assert_eq!(triggers(&db, "items"), crdt_triggers("items"));
    assert!(triggers(&db, "__new_items").is_empty());
    let leftover: i64 = db
        .read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'z_dirty___new_items%'",
                    [],
                    |row| row.get(0),
                )
                .map_err(DatabaseError::from)?)
        })
        .unwrap();
    assert_eq!(leftover, 0, "old-name triggers are gone");

    // The rebuilt table is a synced table again, with the new column tracked.
    run(
        &db,
        GuardedWriteOptions::default(),
        &["INSERT INTO items (id, body, extra) VALUES ('c', 'z', 'e')"],
    )
    .unwrap();
    let (hlc, map) = meta(&db, "items", "c");
    assert_eq!(map["extra"], JsonValue::String(hlc));

    // Foreign keys are enforced again after schema mode.
    let err = db
        .write(|tx| {
            tx.execute(
                "INSERT INTO notes (id, item_id) VALUES ('m', 'missing')",
                params![],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(err.sqlite_error().is_some(), "got {err:?}");
}

#[test]
fn schema_mode_rolls_back_on_a_foreign_key_violation() {
    let (_fx, db) = open();
    let err = run(
        &db,
        SCHEMA,
        &["INSERT INTO notes (id, item_id) VALUES ('m', 'missing')"],
    )
    .unwrap_err();
    assert!(
        matches!(&database_error(err), DatabaseError::ForeignKeyCheckFailed { tables } if tables == &["notes".to_string()]),
        "typed error"
    );
    assert_eq!(count(&db, "notes"), 0);

    let err = db
        .write(|tx| {
            tx.execute(
                "INSERT INTO notes (id, item_id) VALUES ('m', 'missing')",
                params![],
            )?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        err.sqlite_error().is_some(),
        "foreign keys restored: {err:?}"
    );
}

#[test]
fn a_no_sync_table_gets_neither_crdt_columns_nor_a_delete_trigger() {
    let (_fx, db) = open();
    run(
        &db,
        GuardedWriteOptions::default(),
        &["CREATE TABLE scratch_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT)"],
    )
    .unwrap();
    assert_eq!(columns(&db, "scratch_no_sync"), vec!["id", "body"]);
    assert!(triggers(&db, "scratch_no_sync").is_empty());

    run(
        &db,
        GuardedWriteOptions::default(),
        &[
            "INSERT INTO scratch_no_sync (id, body) VALUES ('a', 'x')",
            "DELETE FROM scratch_no_sync",
        ],
    )
    .unwrap();
    assert_eq!(count(&db, DELETED_ROWS_TABLE), 0);
}

#[test]
fn local_mode_creates_plain_tables_and_writes_them_untouched() {
    let (_fx, db) = open();
    run(
        &db,
        LOCAL,
        &["CREATE TABLE dev_items (id TEXT PRIMARY KEY NOT NULL, body TEXT)"],
    )
    .unwrap();
    assert_eq!(columns(&db, "dev_items"), vec!["id", "body"]);
    assert!(triggers(&db, "dev_items").is_empty());

    run(
        &db,
        LOCAL,
        &[
            "ALTER TABLE dev_items ADD COLUMN extra TEXT",
            "INSERT INTO dev_items (id, body) VALUES ('a', 'x')",
            "UPDATE dev_items SET body = 'y'",
            "DELETE FROM dev_items",
            "INSERT INTO items (id, body) VALUES ('s', 'synced')",
        ],
    )
    .unwrap();
    assert!(triggers(&db, "dev_items").is_empty());
    assert_eq!(count(&db, DELETED_ROWS_TABLE), 0);
    let dirty: i64 = db
        .read(|conn| {
            Ok(conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {TABLE_CRDT_DIRTY_TABLES} WHERE table_name = 'dev_items'"),
                    [],
                    |row| row.get(0),
                )
                .map_err(DatabaseError::from)?)
        })
        .unwrap();
    assert_eq!(dirty, 0);

    // A synced table written in local mode is still stamped.
    let (hlc, map) = meta(&db, "items", "s");
    assert_eq!(map["body"], JsonValue::String(hlc));
}

#[test]
fn a_rename_across_the_no_sync_boundary_is_refused() {
    let (_fx, db) = open();
    let err = run(&db, SCHEMA, &["ALTER TABLE items RENAME TO items_no_sync"]).unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::UnsupportedStatement { .. }
        ),
        "refused"
    );
    assert_eq!(triggers(&db, "items"), crdt_triggers("items"));
}

#[test]
fn copy_rows_verbatim_needs_schema_mode() {
    let (_fx, db) = open();
    let err = db
        .write_guarded(&allow_all(), |tx| {
            tx.copy_rows_verbatim("INSERT INTO notes (id) SELECT id FROM items", params![])?;
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(
            database_error(err),
            DatabaseError::UnsupportedStatement { .. }
        ),
        "refused"
    );
}

#[test]
fn a_denied_alter_keeps_the_tables_triggers() {
    let (_fx, db) = open();
    let guard = SqlGuard {
        authorizer: Arc::new(|ctx: &AuthContext<'_>| match ctx.action {
            AuthAction::AlterTable { .. } => Authorization::Deny,
            _ => Authorization::Allow,
        }),
        progress: None,
    };
    db.write_guarded(&guard, |tx| {
        let err = tx
            .execute("ALTER TABLE items ADD COLUMN extra TEXT", params![])
            .unwrap_err();
        assert!(
            matches!(&err, Error::Database(DatabaseError::SqlGuardDenied { .. })),
            "got {err:?}"
        );
        Ok(())
    })
    .unwrap();
    assert_eq!(triggers(&db, "items"), crdt_triggers("items"));
    assert_eq!(columns(&db, "items").len(), 5);
}
