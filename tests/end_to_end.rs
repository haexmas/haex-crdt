//! End-to-end acceptance test per extraction plan §8.
//!
//! Opens **two independent [`Database`]s** on **two separate SQLCipher files**
//! in one `tempdir`, each with its own durable `DeviceIdProvider`, and walks
//! through the sequence the plan defines:
//!
//! - (a) `Database::open` succeeds on both fresh files
//! - (b) Crate-owned bookkeeping migrations run (journaled in
//!   `haex_crdt_migrations_no_sync`)
//! - (c) Consumer-owned toy migration runs (journaled in
//!   `haex_app_migrations_no_sync`)
//! - (d) Store A is pre-populated *before* `install_crdt` runs so the
//!   backfill contract is exercised on it; the scanner then returns the
//!   backfilled rows
//! - (d′) Store B calls `install_crdt` on the empty toy table so the CRDT
//!   metadata columns, triggers, and dirty-tables wiring are in place
//!   before the apply pass
//! - (e) Local write on A → `scan_table_for_local_changes(A)` returns it →
//!   `apply_remote_changes(B)` succeeds → readback from B carries A's
//!   HLC and author metadata → reopening either store with a different
//!   device provider is accepted and returns the new provider's UUID
//!   (the crate no longer arbitrates device IDs; the consumer owns
//!   uniqueness per (DB × replica))
//!
//! This is an integration test on the local path (workspace path dependency);
//! the true "consumable from a tagged git commit" check that plan §8 gates
//! step 3 → step 4 with lands with the `v0.1.0` tag in Batch H, when a
//! standalone throwaway crate can pull `haex-crdt` from that tag.

#![cfg(feature = "raw-connection")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use haex_crdt::crdt::columns::HLC_TIMESTAMP_COLUMN;
use haex_crdt::rusqlite::params;
use haex_crdt::{
    device_uuid_to_hlc_node, hlc_is_from_node, Database, DatabaseConfig, DeviceIdProvider,
    InstallCrdtOptions, MigrationName, NoopSignatureProvider, ScanFilters, SqlCipherKey,
    StaticDeviceId, StaticMigrationSource, DEFAULT_TRIGGER_VERSION,
};
use tempfile::TempDir;
use uuid::Uuid;

/// Wraps the plan §8 migration set: one consumer migration creating a plain
/// `toy_no_sync` table that both stores share. Suffixed `_no_sync` so the
/// migration engine's `CrdtTransformer` leaves the CREATE TABLE verbatim —
/// the CRDT metadata is added later via `install_crdt` per the plan's
/// (d)/(d′) flow.
fn migration_source() -> Arc<StaticMigrationSource> {
    let mut m = BTreeMap::new();
    m.insert(
        MigrationName::from("0001_toy"),
        "CREATE TABLE toy_no_sync (id TEXT PRIMARY KEY NOT NULL, body TEXT);".to_string(),
    );
    Arc::new(StaticMigrationSource(m))
}

/// Builds the database configuration shared by both acceptance-test stores.
fn config(
    path: PathBuf,
    device_id: Arc<dyn DeviceIdProvider>,
    source: Arc<dyn haex_crdt::MigrationSource>,
) -> DatabaseConfig {
    DatabaseConfig {
        path,
        key: SqlCipherKey::new("acceptance-test-key"),
        create_if_missing: true,
        device_id,
        signature_provider: Arc::new(NoopSignatureProvider),
        migration_source: source,
        trigger_version: DEFAULT_TRIGGER_VERSION,
    }
}

/// Runs a scalar count query through the public raw-connection hook.
fn count(db: &Database, sql: &'static str) -> i64 {
    db.with_connection(|conn| {
        conn.query_row(sql, [], |r| r.get::<_, i64>(0))
            .map_err(map_err)
    })
    .expect("count query")
}

/// Converts a raw SQLite error into the crate's public error type.
fn map_err(e: haex_crdt::rusqlite::Error) -> haex_crdt::Error {
    haex_crdt::Error::Message(e.to_string())
}

#[test]
/// Verifies the complete two-device migration, backfill, sync, and identity flow.
fn two_devices_sync_backfilled_and_fresh_writes_end_to_end() {
    // ---------- setup ------------------------------------------------------

    let tmp = TempDir::new().expect("tempdir");
    let path_a = tmp.path().join("device_a.db");
    let path_b = tmp.path().join("device_b.db");
    let device_a = Uuid::new_v4();
    let device_b = Uuid::new_v4();
    assert_ne!(device_a, device_b);
    let source = migration_source();

    let provider_a: Arc<dyn DeviceIdProvider> = Arc::new(StaticDeviceId(device_a));
    let provider_b: Arc<dyn DeviceIdProvider> = Arc::new(StaticDeviceId(device_b));

    // ---------- (a) open succeeds on both --------------------------------

    let db_a = Database::open(config(
        path_a.clone(),
        Arc::clone(&provider_a),
        source.clone(),
    ))
    .expect("open device_a");
    let db_b = Database::open(config(
        path_b.clone(),
        Arc::clone(&provider_b),
        source.clone(),
    ))
    .expect("open device_b");
    assert_eq!(db_a.device_id(), device_a);
    assert_eq!(db_b.device_id(), device_b);

    // ---------- (b) crate-owned migrations journaled ---------------------

    let crate_journal_a = count(&db_a, "SELECT COUNT(*) FROM haex_crdt_migrations_no_sync");
    let crate_journal_b = count(&db_b, "SELECT COUNT(*) FROM haex_crdt_migrations_no_sync");
    assert!(
        crate_journal_a > 0 && crate_journal_b > 0,
        "crate bootstrap migrations must run on both stores",
    );

    // ---------- (c) consumer migration journaled --------------------------

    let toy_migration_a = db_a
        .with_connection(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM haex_app_migrations_no_sync WHERE migration_name = ?1",
                ["0001_toy"],
                |r| r.get::<_, i64>(0),
            )
            .map_err(map_err)
        })
        .expect("count consumer migration on a");
    let toy_migration_b = db_b
        .with_connection(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM haex_app_migrations_no_sync WHERE migration_name = ?1",
                ["0001_toy"],
                |r| r.get::<_, i64>(0),
            )
            .map_err(map_err)
        })
        .expect("count consumer migration on b");
    assert_eq!(toy_migration_a, 1);
    assert_eq!(toy_migration_b, 1);

    // ---------- (d) pre-populate A, then install_crdt (backfill) --------

    db_a.with_connection(|conn| {
        conn.execute(
            "INSERT INTO toy_no_sync (id, body) VALUES (?1, ?2)",
            params!["legacy-1", "legacy body one"],
        )
        .map(|_| ())
        .map_err(map_err)
    })
    .expect("seed legacy row on device_a");
    db_a.with_connection(|conn| {
        conn.execute(
            "INSERT INTO toy_no_sync (id, body) VALUES (?1, ?2)",
            params!["legacy-2", "legacy body two"],
        )
        .map(|_| ())
        .map_err(map_err)
    })
    .expect("seed second legacy row on device_a");

    db_a.install_crdt("toy_no_sync", InstallCrdtOptions::default())
        .expect("install_crdt on device_a with legacy rows");

    // Backfill contract: scanner now returns one change per (legacy row ×
    // data column). Two rows × one data column (`body`) = two changes.
    let backfilled = db_a
        .scan_table_for_local_changes("toy_no_sync", None, ScanFilters::default())
        .expect("scan backfilled changes on device_a");
    assert_eq!(
        backfilled.len(),
        2,
        "backfill must surface every legacy row's data column; got {backfilled:?}",
    );
    assert!(backfilled.iter().all(|c| c.column_name == "body"));

    // ---------- (d′) install_crdt on empty B ------------------------------

    db_b.install_crdt("toy_no_sync", InstallCrdtOptions::default())
        .expect("install_crdt on empty toy table for device_b");

    // ---------- (e) fresh write on A → scan → apply on B → readback ------

    // Local write via the raw connection, using the transaction-scoped
    // `current_hlc()` UDF so the write carries the store's device node id
    // and fires the CRDT INSERT trigger (which requires the row-level HLC
    // to be non-NULL to populate the column-HLC map and mark the table
    // dirty).
    db_a.with_connection(|conn| {
        conn.execute(
            &format!(
                "INSERT INTO toy_no_sync (id, body, {HLC_TIMESTAMP_COLUMN}) \
                 VALUES (?1, ?2, current_hlc())"
            ),
            params!["fresh-1", "fresh body from device_a"],
        )
        .map(|_| ())
        .map_err(map_err)
    })
    .expect("fresh write on device_a");

    // Scan A again — must include the fresh row alongside the backfilled ones.
    let all_local = db_a
        .scan_table_for_local_changes("toy_no_sync", None, ScanFilters::default())
        .expect("scan after fresh write");
    let fresh_change = all_local
        .iter()
        .find(|c| c.row_pks.contains("fresh-1"))
        .expect("scan must include the fresh row");
    assert_eq!(fresh_change.column_name, "body");
    assert_eq!(
        fresh_change.value,
        serde_json::json!("fresh body from device_a")
    );
    let node_a = device_uuid_to_hlc_node(&device_a.to_string()).expect("device_a → hlc node");
    assert!(
        hlc_is_from_node(&fresh_change.hlc_timestamp, node_a),
        "fresh write's HLC must carry device_a's node id; got {}",
        fresh_change.hlc_timestamp,
    );

    // Apply everything scanned on A into B — backfilled + fresh land together.
    let report = db_b
        .apply_remote_changes(all_local.clone())
        .expect("apply_remote_changes on device_b");
    assert!(
        report.report.applied > 0,
        "apply must land at least one change; report={report:?}",
    );
    assert_eq!(report.report.skipped_stale, 0);

    // Readback from B — the fresh row must be present with A's payload and
    // A's HLC. A raw query bypasses LWW filtering so we see exactly what
    // apply wrote.
    let (body_on_b, hlc_on_b) = db_b
        .with_connection(|conn| {
            conn.query_row(
                &format!("SELECT body, {HLC_TIMESTAMP_COLUMN} FROM toy_no_sync WHERE id = ?1"),
                ["fresh-1"],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .map_err(map_err)
        })
        .expect("readback fresh row on device_b");
    assert_eq!(body_on_b, "fresh body from device_a");
    assert!(
        hlc_is_from_node(&hlc_on_b, node_a),
        "readback HLC on device_b must still be tagged with device_a's node; got {hlc_on_b}",
    );
    assert_eq!(hlc_on_b, fresh_change.hlc_timestamp);

    // Legacy row from the backfill also landed.
    let legacy_on_b: String = db_b
        .with_connection(|conn| {
            conn.query_row(
                "SELECT body FROM toy_no_sync WHERE id = ?1",
                ["legacy-1"],
                |r| r.get::<_, String>(0),
            )
            .map_err(map_err)
        })
        .expect("readback legacy row on device_b");
    assert_eq!(legacy_on_b, "legacy body one");

    // ---------- provider-authoritative device id -------------------------
    // The crate no longer arbitrates device IDs on the same DB file. A
    // reopen with a different provider is accepted and returns exactly
    // that provider's UUID. Enforcing uniqueness per (DB × replica) is
    // the consumer's job — see the DeviceIdProvider contract docs.
    drop(db_a);
    drop(db_b);

    let db_a_with_b = Database::open(config(path_a, Arc::clone(&provider_b), source.clone()))
        .expect("crate accepts a different provider on the same file");
    assert_eq!(db_a_with_b.device_id(), device_b);
    drop(db_a_with_b);

    let db_b_with_a = Database::open(config(path_b, Arc::clone(&provider_a), source))
        .expect("crate accepts a different provider on the same file");
    assert_eq!(db_b_with_a.device_id(), device_a);
}
