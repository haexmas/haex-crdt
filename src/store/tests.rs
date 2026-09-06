//! `Store` open lifecycle + method delegates + `install_crdt` backfill.
//!
//! Tests use a temp-file SQLCipher DB rather than `:memory:` — `Store::open`
//! insists on WAL journaling which `:memory:` cannot provide.

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::crdt::cleanup::RetentionPolicy;
use crate::crdt::hlc::device_uuid_to_hlc_node;
use crate::crdt::scanner::ColumnChange;
use crate::device_id::StaticDeviceId;
use crate::error::Result;
use crate::migration::{MigrationName, StaticMigrationSource};
use crate::signature::{AuthorId, NoopSignatureProvider, SignatureProvider};

fn source(entries: &[(&str, &str)]) -> Arc<StaticMigrationSource> {
    let mut m = BTreeMap::new();
    for (n, c) in entries {
        m.insert(MigrationName::from(*n), (*c).to_string());
    }
    Arc::new(StaticMigrationSource(m))
}

struct Fixture {
    _tmp: TempDir,
    config: StoreConfig,
    device: Uuid,
}

struct EchoSignatureProvider;

impl SignatureProvider for EchoSignatureProvider {
    fn sign_column(&self, preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(preimage.to_vec())
    }

    fn verify_column(&self, _preimage: &[u8], _sig: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl Fixture {
    fn with_source(migration_source: Arc<dyn crate::migration::MigrationSource>) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("store.db");
        let device = Uuid::new_v4();
        let config = StoreConfig {
            path,
            key: SqlCipherKey::new("test-key"),
            create_if_missing: true,
            device_id: Arc::new(StaticDeviceId(device)),
            signature_provider: Arc::new(NoopSignatureProvider),
            migration_source,
            trigger_version: DEFAULT_TRIGGER_VERSION,
        };
        Fixture {
            _tmp: tmp,
            config,
            device,
        }
    }

    fn new() -> Self {
        Self::with_source(source(&[]))
    }
}

// ---------- open lifecycle -------------------------------------------------

#[test]
fn open_fresh_bootstraps_bookkeeping_and_records_device_id() {
    let fx = Fixture::new();
    let store = Store::open(fx.config.clone()).unwrap();
    assert_eq!(store.device_id(), fx.device);
}

#[test]
fn reopen_with_same_device_id_succeeds() {
    let fx = Fixture::new();
    Store::open(fx.config.clone()).unwrap();
    // A second open on the same path with the same provider must succeed.
    let cfg = StoreConfig {
        create_if_missing: false,
        ..fx.config.clone()
    };
    let store = Store::open(cfg).unwrap();
    assert_eq!(store.device_id(), fx.device);
}

#[test]
fn reopen_with_different_device_id_returns_device_id_mismatch() {
    let fx = Fixture::new();
    Store::open(fx.config.clone()).unwrap();

    let other = Uuid::new_v4();
    let mut cfg = fx.config.clone();
    cfg.create_if_missing = false;
    cfg.device_id = Arc::new(StaticDeviceId(other));
    let err = match Store::open(cfg) {
        Err(e) => e,
        Ok(_) => panic!("open must reject a mismatched device id"),
    };
    match err {
        crate::Error::DeviceIdMismatch { expected, supplied } => {
            assert_eq!(expected, fx.device);
            assert_eq!(supplied, other);
        }
        other => panic!("wrong variant: {other:?}"),
    }
}

#[test]
fn concurrent_first_opens_with_same_device_id_converge() {
    let fx = Fixture::new();
    let start = Arc::new(Barrier::new(2));
    let first_config = fx.config.clone();
    let second_config = fx.config.clone();

    let first_start = Arc::clone(&start);
    let first = thread::spawn(move || {
        first_start.wait();
        Store::open(first_config).map(|store| store.device_id())
    });
    let second_start = Arc::clone(&start);
    let second = thread::spawn(move || {
        second_start.wait();
        Store::open(second_config).map(|store| store.device_id())
    });

    assert_eq!(first.join().unwrap().unwrap(), fx.device);
    assert_eq!(second.join().unwrap().unwrap(), fx.device);
}

#[test]
fn concurrent_first_opens_with_different_device_ids_reject_the_loser() {
    let fx = Fixture::new();
    let other_device = Uuid::new_v4();
    let first_config = fx.config.clone();
    let mut other_config = fx.config.clone();
    other_config.device_id = Arc::new(StaticDeviceId(other_device));
    let start = Arc::new(Barrier::new(2));

    let first_start = Arc::clone(&start);
    let first = thread::spawn(move || {
        first_start.wait();
        Store::open(first_config).map(|store| store.device_id())
    });
    let second_start = Arc::clone(&start);
    let second = thread::spawn(move || {
        second_start.wait();
        Store::open(other_config).map(|store| store.device_id())
    });

    let first_result = first.join().unwrap();
    let second_result = second.join().unwrap();
    let successful_device = match (first_result, second_result) {
        (Ok(first_id), Err(crate::Error::DeviceIdMismatch { supplied, .. })) => {
            assert_eq!(supplied, other_device);
            first_id
        }
        (Err(crate::Error::DeviceIdMismatch { supplied, .. }), Ok(second_id)) => {
            assert_eq!(supplied, fx.device);
            second_id
        }
        (first, second) => {
            panic!("expected one successful open and one mismatch: {first:?}, {second:?}")
        }
    };
    assert!(successful_device == fx.device || successful_device == other_device);
}

#[cfg(unix)]
#[test]
fn open_rejects_non_utf8_database_paths() {
    use std::os::unix::ffi::OsStringExt;

    let fx = Fixture::new();
    let mut config = fx.config;
    config.path = std::path::PathBuf::from(std::ffi::OsString::from_vec(vec![b'd', b'b', 0xFF]));

    let error = match Store::open(config) {
        Err(error) => error,
        Ok(_) => panic!("non-UTF-8 database path must be rejected"),
    };
    assert!(error.to_string().contains("valid UTF-8"));
}

#[test]
fn open_applies_consumer_migrations() {
    let fx = Fixture::with_source(source(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let store = Store::open(fx.config).unwrap();
    // apply_migrations at open is idempotent — a second call is a no-op.
    let report = store.apply_migrations().unwrap();
    assert_eq!(report.crate_applied, 0);
    assert_eq!(report.consumer_applied, 0);
}

// ---------- install_crdt ---------------------------------------------------

#[test]
fn install_crdt_on_empty_table_installs_columns_and_triggers_without_backfill() {
    let fx = Fixture::with_source(source(&[(
        "0001_items_plain",
        "CREATE TABLE items_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let store = Store::open(fx.config).unwrap();
    store
        .install_crdt("items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    // Empty table → nothing marked dirty by the backfill.
    let dirty = store.scan_dirty_tables().unwrap();
    assert!(
        !dirty.contains(&"items_no_sync".to_string()),
        "empty backfill must not mark dirty; dirty={dirty:?}"
    );
}

#[test]
fn install_crdt_backfills_pre_existing_rows_and_marks_dirty() {
    // Put both the CREATE TABLE and the pre-existing INSERTs into the
    // migration itself, so at Store::open the rows are already on disk
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
    let store = Store::open(fx.config).unwrap();

    store
        .install_crdt("legacy_items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    // Dirty-tables set must include the backfilled table.
    let dirty = store.scan_dirty_tables().unwrap();
    assert!(
        dirty.contains(&"legacy_items_no_sync".to_string()),
        "backfill must mark the table dirty; got {dirty:?}"
    );

    // Scanner must now return the backfilled `body` column for both rows.
    // Two rows × one data column (`body`) = two changes.
    let changes = store
        .scan_table_for_local_changes("legacy_items_no_sync", None, None, None)
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
    let store = Store::open(config).unwrap();
    store
        .install_crdt("legacy_items_no_sync", InstallCrdtOptions::default())
        .unwrap();

    let changes = store
        .scan_table_for_local_changes("legacy_items_no_sync", None, None, None)
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
    let store = Store::open(fx.config).unwrap();
    store
        .install_crdt("t_no_sync", InstallCrdtOptions::default())
        .unwrap();
    let err = match store.install_crdt("t_no_sync", InstallCrdtOptions::default()) {
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
    let store = Store::open(fx.config).unwrap();
    store
        .install_crdt("t_no_sync", InstallCrdtOptions::default())
        .unwrap();
    store
        .install_crdt(
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
    let store = Store::open(fx.config).unwrap();
    let err = match store.install_crdt("evil; DROP TABLE", InstallCrdtOptions::default()) {
        Err(e) => e,
        Ok(()) => panic!("unsafe identifier must be rejected"),
    };
    // The unsafe-identifier check runs before any tx opens; the exact error
    // variant is intentionally checked loosely so future refactoring of the
    // validation surface stays possible.
    let msg = err.to_string();
    assert!(msg.contains("evil") || msg.contains("Invalid"));
}

// ---------- apply_remote_changes delegate ----------------------------------

#[test]
fn store_apply_remote_changes_uses_the_configured_signature_provider() {
    let fx = Fixture::with_source(source(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let store = Store::open(fx.config).unwrap();
    let hlc = "9999999999999999/abcdef0000000000000000000000";
    let changes = vec![ColumnChange {
        table_name: "items".to_string(),
        row_pks: r#"{"id":"r1"}"#.to_string(),
        column_name: "body".to_string(),
        hlc_timestamp: hlc.to_string(),
        value: json!("hello"),
        device_id: String::new(),
        sig: None,
    }];
    let report = store.apply_remote_changes(changes).unwrap();
    assert_eq!(report.applied, 1);
}

#[test]
fn store_scan_after_apply_returns_the_applied_change() {
    let fx = Fixture::with_source(source(&[(
        "0001_items",
        "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, body TEXT);",
    )]));
    let store = Store::open(fx.config.clone()).unwrap();

    // Apply a change whose HLC comes from a foreign node id — the scanner
    // is then invoked without the ping-pong origin filter, so it must
    // return the applied row regardless of author.
    let foreign_hlc = "9999999999999999/deadbeefdeadbeefdeadbeefdeadbe";
    store
        .apply_remote_changes(vec![ColumnChange {
            table_name: "items".to_string(),
            row_pks: r#"{"id":"r1"}"#.to_string(),
            column_name: "body".to_string(),
            hlc_timestamp: foreign_hlc.to_string(),
            value: json!("v"),
            device_id: String::new(),
            sig: None,
        }])
        .unwrap();

    let changes = store
        .scan_table_for_local_changes("items", None, None, None)
        .unwrap();
    assert!(
        changes.iter().any(|c| c.column_name == "body"),
        "scan must return the body change; got {changes:?}"
    );
    // Sanity: the origin_node filter for our own device must drop the row
    // (author was the foreign node) — proves the filter is wired.
    let node = device_uuid_to_hlc_node(&fx.device.to_string()).unwrap();
    let self_only = store
        .scan_table_for_local_changes("items", None, Some(node), None)
        .unwrap();
    assert!(
        self_only.is_empty(),
        "foreign-authored change must be filtered out; got {self_only:?}",
    );
}

#[test]
fn store_cleanup_delegate_reports_zero_pruned_on_empty_log() {
    let fx = Fixture::new();
    let store = Store::open(fx.config).unwrap();
    let report = store
        .cleanup_deleted_rows(RetentionPolicy::All, |_, _| Ok(()))
        .unwrap();
    assert_eq!(report.rows_deleted, 0);
}
