//! `Database::observe_committed_changes` reports the tables of every committed transaction,
//! whichever path wrote them, and nothing for a transaction that rolled back.

#![cfg(feature = "raw-connection")]

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use haex_crdt::{
    Database, DatabaseConfig, MigrationName, NoopSignatureProvider, SqlCipherKey, StaticDeviceId,
    StaticMigrationSource, DEFAULT_TRIGGER_VERSION,
};
use tempfile::TempDir;
use uuid::Uuid;

type Seen = Arc<Mutex<Vec<BTreeSet<String>>>>;

/// A store with two device-local tables, `alpha_no_sync` and `beta_no_sync`, and an observer that
/// collects every report.
fn open_observed() -> (TempDir, Database, Seen) {
    let dir = TempDir::new().expect("tempdir");
    let mut migrations = BTreeMap::new();
    migrations.insert(
        MigrationName::from("0001_tables"),
        "CREATE TABLE alpha_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);\n\
         --> statement-breakpoint\n\
         CREATE TABLE beta_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);"
            .to_string(),
    );
    let db = Database::open(DatabaseConfig {
        path: dir.path().join("observed.db"),
        key: SqlCipherKey::new("observe-test-key"),
        create_if_missing: true,
        bootstrap: Arc::new(StaticDeviceId(Uuid::new_v4())),
        signature_provider: Arc::new(NoopSignatureProvider),
        migration_source: Arc::new(StaticMigrationSource(migrations)),
        trigger_version: DEFAULT_TRIGGER_VERSION,
        max_transaction_bytes: haex_crdt::MAX_CRDT_TRANSACTION_BYTES,
    })
    .expect("open");
    let seen: Seen = Arc::default();
    let sink = Arc::clone(&seen);
    db.observe_committed_changes(move |tables| sink.lock().expect("seen").push(tables.clone()));
    (dir, db, seen)
}

fn names(tables: &[&str]) -> BTreeSet<String> {
    tables.iter().map(|t| (*t).to_string()).collect()
}

#[test]
fn a_write_reports_each_table_it_changed_once() {
    let (_dir, db, seen) = open_observed();

    db.write(|tx| {
        tx.execute(
            "INSERT INTO alpha_no_sync (id, body) VALUES ('a', '1')",
            &[],
        )?;
        tx.execute(
            "INSERT INTO alpha_no_sync (id, body) VALUES ('b', '2')",
            &[],
        )?;
        tx.execute("INSERT INTO beta_no_sync (id, body) VALUES ('c', '3')", &[])?;
        Ok(())
    })
    .expect("write");

    assert_eq!(
        *seen.lock().unwrap(),
        vec![names(&["alpha_no_sync", "beta_no_sync"])]
    );
}

#[test]
fn a_rolled_back_write_reports_nothing_and_leaves_no_trace_in_the_next_report() {
    let (_dir, db, seen) = open_observed();

    let failed: haex_crdt::Result<()> = db.write(|tx| {
        tx.execute(
            "INSERT INTO alpha_no_sync (id, body) VALUES ('a', '1')",
            &[],
        )?;
        Err(haex_crdt::db::error::DatabaseError::ValidationError {
            reason: "abort".to_string(),
        }
        .into())
    });
    assert!(failed.is_err());
    assert!(seen.lock().unwrap().is_empty());

    db.write(|tx| {
        tx.execute("INSERT INTO beta_no_sync (id, body) VALUES ('b', '2')", &[])?;
        Ok(())
    })
    .expect("write");
    assert_eq!(*seen.lock().unwrap(), vec![names(&["beta_no_sync"])]);
}

#[test]
fn raw_sql_and_a_read_are_told_apart() {
    let (_dir, db, seen) = open_observed();

    db.with_connection(|conn| {
        conn.execute("INSERT INTO alpha_no_sync (id, body) VALUES ('a', '1')", [])
            .map_err(haex_crdt::db::error::DatabaseError::from)?;
        Ok(())
    })
    .expect("raw write");
    assert_eq!(*seen.lock().unwrap(), vec![names(&["alpha_no_sync"])]);

    db.read(|conn| {
        conn.query_row("SELECT COUNT(*) FROM alpha_no_sync", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(haex_crdt::db::error::DatabaseError::from)?;
        Ok(())
    })
    .expect("read");
    assert_eq!(seen.lock().unwrap().len(), 1, "a read reports nothing");
}

#[test]
fn a_panicking_observer_does_not_break_the_write() {
    let (_dir, db, _seen) = open_observed();
    db.observe_committed_changes(|_| panic!("observer bug"));

    db.write(|tx| {
        tx.execute(
            "INSERT INTO alpha_no_sync (id, body) VALUES ('a', '1')",
            &[],
        )?;
        Ok(())
    })
    .expect("write survives the observer");
}
