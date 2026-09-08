use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use rusqlite::Connection;

use super::bootstrap::CRATE_MIGRATIONS;
use super::engine::run_migrations;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::error::{Error, MigrationJournal};
use crate::migration::{MigrationName, StaticMigrationSource};
use crate::table_names::{
    TABLE_APP_MIGRATIONS, TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES, TABLE_CRDT_MIGRATIONS,
};

fn empty_source() -> StaticMigrationSource {
    StaticMigrationSource(BTreeMap::new())
}

fn source_from(entries: &[(&str, &str)]) -> StaticMigrationSource {
    let mut m = BTreeMap::new();
    for (n, c) in entries {
        m.insert(MigrationName::from(*n), (*c).to_string());
    }
    StaticMigrationSource(m)
}

fn table_columns(conn: &Connection, table: &str) -> Vec<String> {
    conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
        [name],
        |r| r.get::<_, bool>(0),
    )
    .unwrap()
}

fn journal_names(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT migration_name FROM {table} ORDER BY migration_name ASC"
        ))
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

// --- initial run -----------------------------------------------------------

#[test]
fn initial_run_creates_both_journal_tables_and_applies_crate_bootstrap() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = empty_source();

    let report = run_migrations(&mut conn, &src).unwrap();

    assert_eq!(report.crate_applied, CRATE_MIGRATIONS.len());
    assert_eq!(report.consumer_applied, 0);
    assert!(table_exists(&conn, TABLE_CRDT_MIGRATIONS));
    assert!(table_exists(&conn, TABLE_APP_MIGRATIONS));
    // Bootstrap must materialize the CRDT bookkeeping schema.
    assert!(table_exists(&conn, TABLE_CRDT_CONFIGS));
    assert!(table_exists(&conn, TABLE_CRDT_DIRTY_TABLES));
    assert!(table_exists(&conn, "haex_deleted_rows"));
}

#[test]
fn second_run_is_noop_when_nothing_changed() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = empty_source();

    let first = run_migrations(&mut conn, &src).unwrap();
    let second = run_migrations(&mut conn, &src).unwrap();

    assert!(first.crate_applied > 0);
    assert_eq!(second.crate_applied, 0);
    assert_eq!(second.consumer_applied, 0);
}

// --- consumer migrations ---------------------------------------------------

#[test]
fn consumer_migration_creates_its_table_and_records_in_app_journal() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = source_from(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]);

    let report = run_migrations(&mut conn, &src).unwrap();

    assert_eq!(report.consumer_applied, 1);
    assert!(table_exists(&conn, "items"));
    assert_eq!(
        journal_names(&conn, TABLE_APP_MIGRATIONS),
        vec!["0001_items"]
    );
}

#[test]
fn consumer_ddl_receives_crdt_metadata_columns() {
    // Plan §4.3: consumer-owned migrations pass through CrdtTransformer,
    // which injects the three CRDT metadata columns
    // (HLC_TIMESTAMP_COLUMN / COLUMN_HLCS_COLUMN / COLUMN_SIGS_COLUMN)
    // into any CREATE TABLE that isn't marked `_no_sync`.
    let mut conn = Connection::open_in_memory().unwrap();
    let src = source_from(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]);

    run_migrations(&mut conn, &src).unwrap();

    let cols: Vec<String> = conn
        .prepare("PRAGMA table_info('items')")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        cols.iter().any(|c| c == HLC_TIMESTAMP_COLUMN),
        "columns: {cols:?}"
    );
    assert!(
        cols.iter().any(|c| c == COLUMN_HLCS_COLUMN),
        "columns: {cols:?}"
    );
    assert!(
        cols.iter().any(|c| c == COLUMN_SIGS_COLUMN),
        "columns: {cols:?}"
    );
}

#[test]
fn no_sync_table_bypasses_crdt_transformer() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = source_from(&[(
        "0001_local_cache",
        "CREATE TABLE local_cache_no_sync (id TEXT PRIMARY KEY NOT NULL, blob BLOB);",
    )]);

    run_migrations(&mut conn, &src).unwrap();

    let cols: Vec<String> = conn
        .prepare("PRAGMA table_info('local_cache_no_sync')")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        !cols.iter().any(|c| c.starts_with("haex_")),
        "no_sync must be untouched, got {cols:?}"
    );
}

#[test]
fn multi_statement_migration_splits_on_breakpoint() {
    let mut conn = Connection::open_in_memory().unwrap();
    let sql = "CREATE TABLE a_no_sync (id INTEGER PRIMARY KEY);\n\
               --> statement-breakpoint\n\
               CREATE TABLE b_no_sync (id INTEGER PRIMARY KEY);";
    let src = source_from(&[("0001_two_tables", sql)]);

    run_migrations(&mut conn, &src).unwrap();

    assert!(table_exists(&conn, "a_no_sync"));
    assert!(table_exists(&conn, "b_no_sync"));
}

#[test]
fn empty_statements_between_breakpoints_are_skipped() {
    let mut conn = Connection::open_in_memory().unwrap();
    // A stray leading breakpoint and blank lines must not turn into empty
    // statements the driver would reject.
    let sql = "--> statement-breakpoint\n\
               \n\
               CREATE TABLE t_no_sync (id INTEGER PRIMARY KEY);\n\
               --> statement-breakpoint\n\
               \n";
    let src = source_from(&[("0001_stray_breaks", sql)]);

    run_migrations(&mut conn, &src).unwrap();
    assert!(table_exists(&conn, "t_no_sync"));
}

// --- drift detection -------------------------------------------------------

#[test]
fn content_drift_on_consumer_migration_aborts_open() {
    let mut conn = Connection::open_in_memory().unwrap();
    let original = source_from(&[(
        "0001_items",
        "CREATE TABLE items_no_sync (id TEXT PRIMARY KEY);",
    )]);
    run_migrations(&mut conn, &original).unwrap();

    let edited = source_from(&[(
        "0001_items",
        "CREATE TABLE items_no_sync (id INTEGER PRIMARY KEY);",
    )]);
    let err = run_migrations(&mut conn, &edited).unwrap_err();
    match err {
        Error::MigrationContentDrift {
            name,
            expected,
            found,
        } => {
            assert_eq!(name, "0001_items");
            assert_ne!(expected, found);
        }
        other => panic!("expected MigrationContentDrift, got {other:?}"),
    }
}

#[test]
fn content_drift_on_a_crate_migration_aborts_open() {
    // The crate-owned journal is reconciled independently of the consumer's,
    // so its drift check needs its own coverage. Corrupting the stored digest
    // stands in for the case that would produce it in the wild: a shipped
    // migration edited after the fact, which `bootstrap::CRATE_MIGRATIONS`
    // forbids from the first tagged release containing it.
    let mut conn = Connection::open_in_memory().unwrap();
    run_migrations(&mut conn, &empty_source()).unwrap();

    let (name, _) = CRATE_MIGRATIONS[0];
    conn.execute(
        &format!("UPDATE {TABLE_CRDT_MIGRATIONS} SET sha256_digest = ?1 WHERE migration_name = ?2"),
        rusqlite::params!["0".repeat(64), name],
    )
    .unwrap();

    let err = run_migrations(&mut conn, &empty_source()).unwrap_err();
    match err {
        Error::MigrationContentDrift { name: drifted, .. } => assert_eq!(drifted, name),
        other => panic!("expected MigrationContentDrift, got {other:?}"),
    }
}

#[test]
fn content_drift_never_rolls_back_previously_applied_content() {
    // Drift must abort the run before touching anything; already-applied
    // migrations stay in the journal so a second correct run resumes cleanly.
    let mut conn = Connection::open_in_memory().unwrap();
    let src = source_from(&[("0001_a", "CREATE TABLE a_no_sync (id INTEGER PRIMARY KEY);")]);
    run_migrations(&mut conn, &src).unwrap();

    let drifted = source_from(&[("0001_a", "CREATE TABLE a_no_sync (id TEXT PRIMARY KEY);")]);
    let _ = run_migrations(&mut conn, &drifted).unwrap_err();

    assert_eq!(journal_names(&conn, TABLE_APP_MIGRATIONS), vec!["0001_a"]);
    assert!(table_exists(&conn, "a_no_sync"));
}

// --- missing-from-source reconciliation ------------------------------------

#[test]
fn journaled_migration_missing_from_consumer_source_aborts_with_scoped_error() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = source_from(&[(
        "0001_items",
        "CREATE TABLE items_no_sync (id TEXT PRIMARY KEY);",
    )]);
    run_migrations(&mut conn, &src).unwrap();

    // Consumer removed the migration on next release — plan §4.3 says abort,
    // never silently continue.
    let dropped = empty_source();
    let err = run_migrations(&mut conn, &dropped).unwrap_err();
    match err {
        Error::MigrationMissingFromSource { journal, name } => {
            assert_eq!(journal, MigrationJournal::ConsumerOwned);
            assert_eq!(name, "0001_items");
        }
        other => panic!("expected MigrationMissingFromSource(ConsumerOwned), got {other:?}"),
    }
}

// --- journal isolation -----------------------------------------------------

#[test]
fn crate_and_consumer_journals_never_collide_on_same_name() {
    // A consumer migration named identically to a crate-owned one lives in a
    // different journal — the two-journal design (plan §4.3) prevents both
    // from being reported as duplicate.
    let mut conn = Connection::open_in_memory().unwrap();
    let (crate_name, _) = CRATE_MIGRATIONS[0];
    let src = source_from(&[(
        crate_name,
        "CREATE TABLE consumer_shadow_no_sync (id INTEGER PRIMARY KEY);",
    )]);

    let report = run_migrations(&mut conn, &src).unwrap();
    assert!(report.crate_applied > 0);
    assert_eq!(report.consumer_applied, 1);
    assert!(table_exists(&conn, "consumer_shadow_no_sync"));

    let crate_journal = journal_names(&conn, TABLE_CRDT_MIGRATIONS);
    let app_journal = journal_names(&conn, TABLE_APP_MIGRATIONS);
    assert!(crate_journal.contains(&crate_name.to_string()));
    assert!(app_journal.contains(&crate_name.to_string()));
}

#[test]
fn consumer_migrations_apply_in_lexicographic_order() {
    let mut conn = Connection::open_in_memory().unwrap();
    // Provide out-of-order names to prove ordering comes from source list,
    // not insertion order. StaticMigrationSource is BTreeMap-backed so
    // list_migrations returns lexicographic order.
    let src = source_from(&[
        ("0002_b", "CREATE TABLE b_no_sync (id INTEGER PRIMARY KEY);"),
        ("0001_a", "CREATE TABLE a_no_sync (id INTEGER PRIMARY KEY);"),
        ("0010_z", "CREATE TABLE z_no_sync (id INTEGER PRIMARY KEY);"),
    ]);

    run_migrations(&mut conn, &src).unwrap();

    assert_eq!(
        journal_names(&conn, TABLE_APP_MIGRATIONS),
        vec!["0001_a", "0002_b", "0010_z"]
    );
}

#[test]
fn concurrent_connections_apply_each_migration_only_once() {
    let database = tempfile::NamedTempFile::new().unwrap();
    let path = database.path().to_owned();
    let start = Arc::new(Barrier::new(2));

    let first_start = Arc::clone(&start);
    let first_path = path.clone();
    let first = thread::spawn(move || {
        let mut conn = Connection::open(first_path).unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        first_start.wait();
        run_migrations(
            &mut conn,
            &source_from(&[(
                "0001_shared",
                "CREATE TABLE shared_no_sync (id INTEGER PRIMARY KEY);",
            )]),
        )
    });

    let second_start = Arc::clone(&start);
    let second_path = path.clone();
    let second = thread::spawn(move || {
        let mut conn = Connection::open(second_path).unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        second_start.wait();
        run_migrations(
            &mut conn,
            &source_from(&[(
                "0001_shared",
                "CREATE TABLE shared_no_sync (id INTEGER PRIMARY KEY);",
            )]),
        )
    });

    let first_report = first.join().unwrap().unwrap();
    let second_report = second.join().unwrap().unwrap();
    assert_eq!(
        first_report.consumer_applied + second_report.consumer_applied,
        1
    );

    let conn = Connection::open(path).unwrap();
    assert!(table_exists(&conn, "shared_no_sync"));
    assert_eq!(
        journal_names(&conn, TABLE_APP_MIGRATIONS),
        vec!["0001_shared"]
    );
}

// --- transactional isolation of a single migration -------------------------

#[test]
fn failing_statement_rolls_back_the_whole_migration() {
    let mut conn = Connection::open_in_memory().unwrap();
    // Second statement is invalid; the whole migration must roll back, so
    // neither the created table nor the journal row survives.
    let sql = "CREATE TABLE tx_test_no_sync (id INTEGER PRIMARY KEY);\n\
               --> statement-breakpoint\n\
               THIS IS NOT VALID SQL;";
    let src = source_from(&[("0001_bad", sql)]);

    let err = run_migrations(&mut conn, &src).unwrap_err();
    assert!(matches!(err, Error::Sqlite(_)), "got {err:?}");

    assert!(!table_exists(&conn, "tx_test_no_sync"));
    assert!(!journal_names(&conn, TABLE_APP_MIGRATIONS).contains(&"0001_bad".to_string()));
    // Crate journal must still carry bootstrap — earlier applications survive.
    assert!(!journal_names(&conn, TABLE_CRDT_MIGRATIONS).is_empty());
}

// --- bootstrap invariants --------------------------------------------------

// --- naming convention invariant (v0.1.1 D-1) ------------------------------

#[test]
/// D-1: `_no_sync` is the sole rule that excludes a table from CRDT sync.
/// The crate's own bookkeeping tables must carry that suffix so they follow
/// the same convention user tables do; the migration bootstrap must
/// materialize them under those suffixed names.
fn crate_bookkeeping_table_names_end_with_no_sync_and_bootstrap_creates_them() {
    for (label, value) in [
        ("TABLE_CRDT_CONFIGS", TABLE_CRDT_CONFIGS),
        ("TABLE_CRDT_DIRTY_TABLES", TABLE_CRDT_DIRTY_TABLES),
        ("TABLE_CRDT_MIGRATIONS", TABLE_CRDT_MIGRATIONS),
        ("TABLE_APP_MIGRATIONS", TABLE_APP_MIGRATIONS),
    ] {
        assert!(
            value.ends_with("_no_sync"),
            "{label} must end with `_no_sync`; got {value:?}",
        );
    }

    let mut conn = Connection::open_in_memory().unwrap();
    run_migrations(&mut conn, &empty_source()).unwrap();

    assert!(table_exists(&conn, TABLE_CRDT_CONFIGS));
    assert!(table_exists(&conn, TABLE_CRDT_DIRTY_TABLES));
    assert!(table_exists(&conn, TABLE_CRDT_MIGRATIONS));
    assert!(table_exists(&conn, TABLE_APP_MIGRATIONS));
}

#[test]
fn crate_bootstrap_records_a_stable_digest() {
    let mut conn = Connection::open_in_memory().unwrap();
    let src = empty_source();
    run_migrations(&mut conn, &src).unwrap();

    let (name, _) = CRATE_MIGRATIONS[0];
    let digest: String = conn
        .query_row(
            &format!("SELECT sha256_digest FROM {TABLE_CRDT_MIGRATIONS} WHERE migration_name = ?"),
            [name],
            |r| r.get(0),
        )
        .unwrap();
    // SHA-256 hex is 64 lowercase hex chars.
    assert_eq!(digest.len(), 64);
    assert!(digest
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
}

#[test]
fn legacy_crdt_identifiers_are_migrated_before_current_bootstrap() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE haex_crdt_configs (
             key TEXT PRIMARY KEY NOT NULL, type TEXT NOT NULL, value TEXT NOT NULL
         );
         CREATE TABLE haex_crdt_dirty_tables (
             table_name TEXT PRIMARY KEY NOT NULL, last_modified TEXT
         );
         CREATE TABLE haex_crdt_migrations (
             migration_name TEXT PRIMARY KEY NOT NULL,
             sha256_digest TEXT NOT NULL,
             applied_at TEXT NOT NULL DEFAULT (datetime('now'))
         );
         CREATE TABLE haex_app_migrations (
             migration_name TEXT PRIMARY KEY NOT NULL,
             sha256_digest TEXT NOT NULL,
             applied_at TEXT NOT NULL DEFAULT (datetime('now'))
         );
         CREATE TABLE haex_deleted_rows (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             haex_hlc TEXT,
             haex_column_hlcs TEXT NOT NULL DEFAULT '{}',
             haex_column_sigs TEXT NOT NULL DEFAULT '{}'
         );
         CREATE TABLE legacy_items (
             id TEXT PRIMARY KEY NOT NULL,
             body TEXT,
             haex_hlc TEXT,
             haex_column_hlcs TEXT NOT NULL DEFAULT '{}',
             haex_column_sigs TEXT NOT NULL DEFAULT '{}'
         );
         INSERT INTO haex_crdt_configs (key, type, value)
             VALUES ('hlc_timestamp', 'hlc', '0000000000000001/1');
         INSERT INTO haex_crdt_dirty_tables (table_name, last_modified)
             VALUES ('legacy_items', '2026-09-07 12:00:00');
         INSERT INTO legacy_items
             (id, body, haex_hlc, haex_column_hlcs, haex_column_sigs)
             VALUES ('row-1', 'preserve me', 'hlc-1', '{\"body\":\"hlc-1\"}', '{}');",
    )
    .unwrap();

    run_migrations(&mut conn, &empty_source()).unwrap();

    for legacy in [
        "haex_crdt_configs",
        "haex_crdt_dirty_tables",
        "haex_crdt_migrations",
        "haex_app_migrations",
    ] {
        assert!(
            !table_exists(&conn, legacy),
            "legacy table still exists: {legacy}"
        );
    }
    assert!(table_exists(&conn, TABLE_CRDT_CONFIGS));
    assert!(table_exists(&conn, TABLE_CRDT_DIRTY_TABLES));
    assert!(table_exists(&conn, TABLE_CRDT_MIGRATIONS));
    assert!(table_exists(&conn, TABLE_APP_MIGRATIONS));

    let config: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = 'hlc_timestamp'"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(config, "0000000000000001/1");

    let columns = conn
        .prepare("PRAGMA table_info(legacy_items)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(columns.iter().any(|name| name == HLC_TIMESTAMP_COLUMN));
    assert!(columns.iter().any(|name| name == COLUMN_HLCS_COLUMN));
    assert!(columns.iter().any(|name| name == COLUMN_SIGS_COLUMN));

    let row: (String, String, String) = conn
        .query_row(
            &format!(
                "SELECT body, {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN} \
                 FROM legacy_items WHERE id = 'row-1'"
            ),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        row,
        (
            "preserve me".into(),
            "hlc-1".into(),
            "{\"body\":\"hlc-1\"}".into()
        )
    );

    let deleted_columns = conn
        .prepare("PRAGMA table_info(haex_deleted_rows)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(deleted_columns
        .iter()
        .any(|name| name == HLC_TIMESTAMP_COLUMN));
    assert!(deleted_columns
        .iter()
        .any(|name| name == COLUMN_HLCS_COLUMN));
    assert!(deleted_columns
        .iter()
        .any(|name| name == COLUMN_SIGS_COLUMN));
}

#[test]
fn conflicting_legacy_and_current_tables_abort_without_merging() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE {TABLE_CRDT_CONFIGS} (
                 key TEXT PRIMARY KEY NOT NULL, type TEXT NOT NULL, value TEXT NOT NULL
             );
             CREATE TABLE haex_crdt_configs (
                 key TEXT PRIMARY KEY NOT NULL, type TEXT NOT NULL, value TEXT NOT NULL
             );
             INSERT INTO {TABLE_CRDT_CONFIGS} VALUES ('device_id', 'system', 'current');
             INSERT INTO haex_crdt_configs VALUES ('device_id', 'system', 'legacy');"
    ))
    .unwrap();

    let err = run_migrations(&mut conn, &empty_source()).unwrap_err();
    assert!(matches!(err, Error::MigrationCompatibility { .. }));
    assert!(table_exists(&conn, TABLE_CRDT_CONFIGS));
    assert!(table_exists(&conn, "haex_crdt_configs"));
}

#[test]
fn a_table_carrying_both_a_legacy_and_a_current_column_aborts_without_renaming() {
    // The column analogue of the case above. SQLite cannot hold two columns
    // of one name, so the rename would fail with a raw "duplicate column
    // name"; report it as a compatibility problem instead and leave both
    // columns in place for inspection.
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE items (
             id TEXT PRIMARY KEY NOT NULL,
             body TEXT,
             haex_hlc TEXT,
             {HLC_TIMESTAMP_COLUMN} TEXT
         );"
    ))
    .unwrap();

    let err = run_migrations(&mut conn, &empty_source()).unwrap_err();
    assert!(
        matches!(err, Error::MigrationCompatibility { .. }),
        "expected MigrationCompatibility, got {err:?}"
    );
    let columns = table_columns(&conn, "items");
    assert!(columns.iter().any(|name| name == "haex_hlc"));
    assert!(columns.iter().any(|name| name == HLC_TIMESTAMP_COLUMN));
}
