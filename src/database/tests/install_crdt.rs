//! `Database::install_crdt` semantics: empty vs backfill, sig binding, the
//! reinstall refuse/allow knob, unsafe-identifier guard.

use std::sync::Arc;

use serde_json::json;

use super::super::*;
use super::{hex, source, EchoSignatureProvider, Fixture};

#[test]
fn install_crdt_on_empty_table_installs_columns_and_triggers_without_backfill() {
    let fx = Fixture::with_source(source(&[(
        "0001_items_plain",
        "CREATE TABLE items_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let db = Database::open(fx.config).unwrap();
    db.install_crdt("items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    // Empty table → nothing marked dirty by the backfill.
    let dirty = db.scan_dirty_tables().unwrap();
    assert!(
        !dirty.contains(&"items_no_sync".to_string()),
        "empty backfill must not mark dirty; dirty={dirty:?}"
    );
}

#[test]
fn install_crdt_backfills_pre_existing_rows_and_marks_dirty() {
    // Put both the CREATE TABLE and the pre-existing INSERTs into the
    // migration itself, so at Database::open the rows are already on disk
    // without CRDT columns — exactly the "existing database, now flip CRDT
    // on" scenario plan §6 targets. Uses `_no_sync` so the consumer-side
    // `CrdtTransformer` in the migration engine leaves the schema alone.
    let fx = Fixture::with_source(source(&[(
        "0001_seed_legacy",
        "CREATE TABLE legacy_items_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);\n\
         --> statement-breakpoint\n\
         INSERT INTO legacy_items_no_sync (id, body) VALUES ('r1', 'legacy-a');\n\
         --> statement-breakpoint\n\
         INSERT INTO legacy_items_no_sync (id, body) VALUES ('r2', 'legacy-b');",
    )]));
    let db = Database::open(fx.config).unwrap();

    db.install_crdt("legacy_items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    // Dirty-tables set must include the backfilled table.
    let dirty = db.scan_dirty_tables().unwrap();
    assert!(
        dirty.contains(&"legacy_items_no_sync".to_string()),
        "backfill must mark the table dirty; got {dirty:?}"
    );

    // Scanner must now return the backfilled `body` column for both rows.
    // Two rows × one data column (`body`) = two changes.
    let changes = db
        .scan_table_for_local_changes("legacy_items_no_sync", None, ScanFilters::default())
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
    let fx = Fixture::with_source(source(&[(
        "0001_seed_legacy",
        "CREATE TABLE legacy_items_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);\n\
         --> statement-breakpoint\n\
         INSERT INTO legacy_items_no_sync (id, body) VALUES ('r1', 'legacy-a');\n\
         --> statement-breakpoint\n\
         INSERT INTO legacy_items_no_sync (id, body) VALUES ('r2', 'legacy-b');",
    )]));
    let mut config = fx.config;
    config.signature_provider = Arc::new(EchoSignatureProvider);
    let db = Database::open(config).unwrap();
    db.install_crdt("legacy_items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    let changes = db
        .scan_table_for_local_changes("legacy_items_no_sync", None, ScanFilters::default())
        .unwrap();
    assert_eq!(changes.len(), 2);
    for change in changes {
        let expected = crate::crdt::apply::column_sig_preimage_from_parts(
            "legacy_items_no_sync",
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
    let fx = Fixture::with_source(source(&[(
        "0001_items_plain",
        "CREATE TABLE t_no_sync (id TEXT PRIMARY KEY NOT NULL);",
    )]));
    let db = Database::open(fx.config).unwrap();
    db.install_crdt("t_no_sync", InstallCrdtOptions::default())
        .unwrap();
    let err = match db.install_crdt("t_no_sync", InstallCrdtOptions::default()) {
        Err(e) => e,
        Ok(()) => panic!("re-install must be refused"),
    };
    match err {
        crate::Error::CrdtAlreadyInstalled { table } => {
            assert_eq!(table, "t_no_sync");
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn install_crdt_allow_reinstall_only_refreshes_triggers() {
    let fx = Fixture::with_source(source(&[(
        "0001_items_plain",
        "CREATE TABLE t_no_sync (id TEXT PRIMARY KEY NOT NULL);",
    )]));
    let db = Database::open(fx.config).unwrap();
    db.install_crdt("t_no_sync", InstallCrdtOptions::default())
        .unwrap();
    db.install_crdt(
        "t_no_sync",
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
