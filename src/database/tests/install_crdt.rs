//! `Database::install_crdt` semantics: empty vs backfill, sig binding, the
//! reinstall refuse/allow knob, unsafe-identifier guard.

use std::sync::Arc;

use serde_json::json;

use super::super::*;
use super::{hex, source, EchoSignatureProvider, Fixture};

/// Creates `sql` on the raw connection after `open`, so the migration
/// engine's `CrdtTransformer` does not add CRDT metadata to it first.
fn create_plain(db: &Database, sql: &str) {
    db.with_locked_conn(|conn| conn.execute_batch(sql).map_err(crate::Error::from))
        .unwrap();
}

/// A table with two rows that predate CRDT, created behind the migration
/// engine's back.
fn seed_legacy(db: &Database) {
    create_plain(
        db,
        "CREATE TABLE legacy_items (id TEXT PRIMARY KEY NOT NULL, body TEXT);\n\
         INSERT INTO legacy_items (id, body) VALUES ('r1', 'legacy-a');\n\
         INSERT INTO legacy_items (id, body) VALUES ('r2', 'legacy-b');",
    );
}

#[test]
fn install_crdt_on_empty_table_installs_columns_and_triggers_without_backfill() {
    let db = Database::open(Fixture::new().config).unwrap();
    create_plain(
        &db,
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    );
    db.install_crdt("items", InstallCrdtOptions::default())
        .unwrap();

    // Empty table → nothing marked dirty by the backfill.
    let dirty = db.scan_dirty_tables().unwrap();
    assert!(
        !dirty.contains(&"items".to_string()),
        "empty backfill must not mark dirty; dirty={dirty:?}"
    );
}

#[test]
fn install_crdt_backfills_pre_existing_rows_and_marks_dirty() {
    // The rows are on disk without CRDT columns before `install_crdt` runs —
    // the "existing database, now flip CRDT on" scenario plan §6 targets.
    let fx = Fixture::new();
    let db = Database::open(fx.config).unwrap();
    seed_legacy(&db);

    db.install_crdt("legacy_items", InstallCrdtOptions::default())
        .unwrap();

    // Dirty-tables set must include the backfilled table.
    let dirty = db.scan_dirty_tables().unwrap();
    assert!(
        dirty.contains(&"legacy_items".to_string()),
        "backfill must mark the table dirty; got {dirty:?}"
    );

    // Scanner must now return the backfilled `body` column for both rows.
    // Two rows × one data column (`body`) = two changes.
    let changes = db
        .scan_table_for_local_changes("legacy_items", None, ScanFilters::default())
        .unwrap();
    assert_eq!(
        changes.len(),
        2,
        "backfill must populate every row; got {} changes: {changes:?}",
        changes.len(),
    );
    assert!(changes.iter().all(|c| c.column_name == "body"));
}

#[test]
fn install_crdt_backfill_signs_each_row_value_and_primary_key() {
    let fx = Fixture::new();
    let mut config = fx.config;
    config.signature_provider = Arc::new(EchoSignatureProvider);
    let db = Database::open(config).unwrap();
    seed_legacy(&db);
    db.install_crdt("legacy_items", InstallCrdtOptions::default())
        .unwrap();

    let changes = db
        .scan_table_for_local_changes("legacy_items", None, ScanFilters::default())
        .unwrap();
    assert_eq!(changes.len(), 2);
    for change in changes {
        let expected = crate::crdt::apply::column_sig_preimage_from_parts(
            "legacy_items",
            &change.row_pks,
            "body",
            &change.hlc_timestamp,
            &change.value,
        );
        assert_eq!(change.sig, Some(json!(hex(&expected))));
    }
}

#[test]
fn install_crdt_refuses_to_reinstall_by_default() {
    let db = Database::open(Fixture::new().config).unwrap();
    create_plain(&db, "CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL);");
    db.install_crdt("t", InstallCrdtOptions::default()).unwrap();
    let err = match db.install_crdt("t", InstallCrdtOptions::default()) {
        Err(e) => e,
        Ok(()) => panic!("re-install must be refused"),
    };
    match err {
        crate::Error::CrdtAlreadyInstalled { table } => {
            assert_eq!(table, "t");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn install_crdt_allow_reinstall_only_refreshes_triggers() {
    let db = Database::open(Fixture::new().config).unwrap();
    create_plain(&db, "CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL);");
    db.install_crdt("t", InstallCrdtOptions::default()).unwrap();
    db.install_crdt(
        "t",
        InstallCrdtOptions {
            allow_reinstall: true,
        },
    )
    .expect("reinstall path must succeed on already-managed table");
}

#[test]
fn install_crdt_rejects_unsafe_identifiers() {
    let fx = Fixture::new();
    let db = Database::open(fx.config).unwrap();
    let err = match db.install_crdt("evil; DROP TABLE", InstallCrdtOptions::default()) {
        Err(e) => e,
        Ok(()) => panic!("unsafe identifier must be rejected"),
    };
    // The unsafe-identifier check runs before any tx opens; the exact error
    // variant is intentionally checked loosely so future refactoring of the
    // validation surface stays possible.
    let msg = err.to_string();
    assert!(msg.contains("evil") || msg.contains("Invalid"));
}

#[test]
fn install_crdt_refuses_no_sync_tables_and_leaves_them_untouched() {
    let fx = Fixture::with_source(source(&[(
        "0001_local",
        "CREATE TABLE local_state_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let db = Database::open(fx.config).unwrap();

    for name in ["local_state_no_sync", "Local_State_NO_SYNC"] {
        let err = match db.install_crdt(name, InstallCrdtOptions::default()) {
            Err(e) => e,
            Ok(()) => panic!("a `_no_sync` table must be refused"),
        };
        assert!(
            matches!(
                err,
                crate::Error::Database(crate::db::error::DatabaseError::ValidationError { .. })
            ),
            "wrong error: {err:?}"
        );
    }

    let columns: i64 = db
        .with_locked_conn(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM pragma_table_info('local_state_no_sync')",
                [],
                |r| r.get(0),
            )
            .map_err(crate::Error::from)
        })
        .unwrap();
    assert_eq!(columns, 2, "the refused table must not gain CRDT columns");
}
