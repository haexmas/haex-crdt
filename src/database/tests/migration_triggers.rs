//! A table created by a migration that runs after the trigger version was
//! already stored must still get its CRDT triggers: on reopen, and through
//! the public `apply_migrations`.

use std::sync::{Arc, Mutex};

use super::super::*;
use super::{source, Fixture};
use crate::migration::{MigrationName, MigrationSource};

const ITEMS: (&str, &str) = (
    "0001_items",
    "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, title TEXT);",
);
const NOTES: (&str, &str) = (
    "0002_notes",
    "CREATE TABLE notes (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
);

fn trigger_count(db: &Database, table: &str) -> i64 {
    db.with_locked_conn(|conn| {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ?1",
            [table],
            |r| r.get(0),
        )
        .map_err(crate::Error::from)
    })
    .unwrap()
}

fn insert_note(db: &Database) {
    db.write(|tx| tx.execute("INSERT INTO notes (id, body) VALUES ('n1', 'x')", &[]))
        .unwrap();
}

#[test]
fn table_created_by_a_later_migration_gets_triggers_on_reopen() {
    let fx = Fixture::with_source(source(&[ITEMS]));
    drop(Database::open(fx.config.clone()).unwrap());

    let db = Database::open(DatabaseConfig {
        create_if_missing: false,
        migration_source: source(&[ITEMS, NOTES]),
        ..fx.config
    })
    .unwrap();

    assert_eq!(trigger_count(&db, "notes"), 3);
    insert_note(&db);
    assert!(db
        .scan_dirty_tables()
        .unwrap()
        .contains(&"notes".to_string()));
}

struct GrowingSource(Mutex<Vec<(&'static str, &'static str)>>);

impl MigrationSource for GrowingSource {
    fn load_migration(&self, name: &MigrationName) -> crate::Result<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(n, _)| *n == name.0)
            .map(|(_, sql)| (*sql).to_string())
            .ok_or_else(|| crate::Error::MigrationMissingFromSource {
                journal: crate::error::MigrationJournal::ConsumerOwned,
                name: name.0.clone(),
            })
    }

    fn list_migrations(&self) -> crate::Result<Vec<MigrationName>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _)| MigrationName::from(*n))
            .collect())
    }
}

#[test]
fn apply_migrations_installs_triggers_on_tables_it_created() {
    let growing = Arc::new(GrowingSource(Mutex::new(vec![ITEMS])));
    let fx = Fixture::with_source(growing.clone());
    let db = Database::open(fx.config).unwrap();
    assert_eq!(trigger_count(&db, "items"), 3);

    growing.0.lock().unwrap().push(NOTES);
    db.apply_migrations().unwrap();

    assert_eq!(trigger_count(&db, "notes"), 3);
    insert_note(&db);
    assert!(db
        .scan_dirty_tables()
        .unwrap()
        .contains(&"notes".to_string()));
}
