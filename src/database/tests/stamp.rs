//! Open lifecycle step 6: rows the bootstrap hook writes before the HLC
//! exists are stamped with an HLC and a full column-HLC map, so the scanner
//! ships them whole. Shape mirrors holzi's device registry, the row that
//! motivated the pass.

use std::collections::BTreeSet;

use rusqlite::OptionalExtension;
use serde_json::Value as JsonValue;

use super::*;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::device_id::DatabaseBootstrap;
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;

const KNOWN_DEVICES: &str = "CREATE TABLE known_devices (
    installation_uuid TEXT PRIMARY KEY NOT NULL,
    vault_device_uuid TEXT NOT NULL UNIQUE,
    alias TEXT,
    first_seen TEXT NOT NULL,
    local_note_no_sync TEXT
);";

/// Registers this installation once, the way a consumer would in its hook.
struct RegisteringBootstrap {
    uuid: Uuid,
}

impl DatabaseBootstrap for RegisteringBootstrap {
    fn bootstrap(&self, tx: &rusqlite::Transaction<'_>) -> crate::Result<Uuid> {
        tx.execute(
            "INSERT OR IGNORE INTO known_devices \
             (installation_uuid, vault_device_uuid, alias, first_seen, local_note_no_sync) \
             VALUES ('inst-1', ?1, 'laptop', '2026-09-29', 'local')",
            [self.uuid.to_string()],
        )
        .map_err(crate::Error::from)?;
        Ok(self.uuid)
    }
}

fn fixture() -> Fixture {
    let mut fx = Fixture::with_source(source(&[("0001_known_devices", KNOWN_DEVICES)]));
    fx.config.bootstrap = Arc::new(RegisteringBootstrap { uuid: fx.device });
    fx
}

/// `(row HLC, parsed column-HLC map)` of the registered row.
fn metadata(db: &Database) -> (Option<String>, Option<JsonValue>) {
    db.with_locked_conn(|conn| {
        conn.query_row(
            &format!(
                "SELECT {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN} FROM known_devices \
                 WHERE installation_uuid = 'inst-1'"
            ),
            [],
            |r| {
                let map: Option<String> = r.get(1)?;
                Ok((r.get(0)?, map.map(|m| serde_json::from_str(&m).unwrap())))
            },
        )
        .map_err(crate::Error::from)
    })
    .unwrap()
}

#[test]
fn bootstrap_rows_are_stamped_and_scanned_whole() {
    let db = Database::open(fixture().config).unwrap();

    let (row_hlc, map) = metadata(&db);
    let row_hlc = row_hlc.expect("open must stamp the row HLC");
    assert_eq!(
        map,
        Some(serde_json::json!({
            "vault_device_uuid": row_hlc,
            "alias": row_hlc,
            "first_seen": row_hlc,
        })),
        "every tracked column carries the stamp; PK and `_no_sync` columns do not"
    );
    assert!(db
        .scan_dirty_tables()
        .unwrap()
        .contains(&"known_devices".to_string()));

    let changes = db
        .scan_table_for_local_changes("known_devices", None, ScanFilters::default())
        .unwrap();
    let columns: BTreeSet<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(
        columns,
        BTreeSet::from(["alias", "first_seen", "vault_device_uuid"])
    );
    assert!(changes.iter().all(|c| c.hlc_timestamp == row_hlc));
}

#[test]
fn reopen_does_not_restamp() {
    let fx = fixture();
    let db = Database::open(fx.config.clone()).unwrap();
    let first = metadata(&db);
    let dirty_at = |db: &Database| {
        db.with_locked_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT last_modified FROM {TABLE_CRDT_DIRTY_TABLES} \
                     WHERE table_name = 'known_devices'"
                ),
                [],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .map_err(crate::Error::from)
        })
        .unwrap()
    };
    // Drain the dirty mark so a restamp would show up as a new one.
    db.with_locked_conn(|conn| {
        conn.execute(&format!("DELETE FROM {TABLE_CRDT_DIRTY_TABLES}"), [])
            .map_err(crate::Error::from)
    })
    .unwrap();
    drop(db);

    let db = Database::open(DatabaseConfig {
        create_if_missing: false,
        ..fx.config
    })
    .unwrap();
    assert_eq!(metadata(&db), first, "a stamped row keeps its HLC");
    assert_eq!(
        dirty_at(&db),
        None,
        "nothing to stamp, nothing marked dirty"
    );
}
