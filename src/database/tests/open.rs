//! `Database::open` lifecycle tests: fresh vs reopen, concurrent opens (now
//! serialized by the fs2 vault lock), non-UTF-8 path rejection, migration
//! idempotence.

use std::sync::{Arc, Barrier, Mutex};
use std::thread;

use uuid::Uuid;

use super::super::*;
use super::{assert_already_open, source, Fixture};
use crate::device_id::{DatabaseBootstrap, StaticDeviceId};
use crate::table_names::TABLE_CRDT_CONFIGS;

struct OneShotBootstrap(Mutex<Option<Uuid>>);

impl DatabaseBootstrap for OneShotBootstrap {
    fn bootstrap(&self, _tx: &rusqlite::Transaction<'_>) -> crate::Result<Uuid> {
        self.0.lock().unwrap().take().ok_or_else(|| {
            crate::Error::Hlc("bootstrap hook called more than once".to_string())
        })
    }
}

#[test]
fn open_calls_bootstrap_once_and_returns_its_uuid() {
    let mut fx = Fixture::new();
    fx.config.bootstrap = Arc::new(OneShotBootstrap(Mutex::new(Some(fx.device))));
    let db = Database::open(fx.config).unwrap();
    assert_eq!(db.device_id(), fx.device);
}

/// The hook is meant to be the place where a consumer inserts its
/// per-installation device registry row. Verify the tx it gets is a real,
/// writable transaction: the write must land in the DB and be visible on a
/// subsequent open.
struct WritingBootstrap {
    uuid: Uuid,
}

impl DatabaseBootstrap for WritingBootstrap {
    fn bootstrap(&self, tx: &rusqlite::Transaction<'_>) -> crate::Result<Uuid> {
        tx.execute(
            "CREATE TABLE IF NOT EXISTS bootstrap_marker (n INTEGER PRIMARY KEY)",
            [],
        )
        .map_err(|e| crate::Error::Message(e.to_string()))?;
        tx.execute(
            "INSERT INTO bootstrap_marker (n) SELECT COALESCE(MAX(n), 0) + 1 FROM bootstrap_marker",
            [],
        )
        .map_err(|e| crate::Error::Message(e.to_string()))?;
        Ok(self.uuid)
    }
}

#[test]
fn bootstrap_hook_may_write_and_writes_are_committed() {
    let mut fx = Fixture::new();
    let uuid = fx.device;
    fx.config.bootstrap = Arc::new(WritingBootstrap { uuid });
    let db = Database::open(fx.config.clone()).unwrap();
    let count = db
        .with_connection(|conn| {
            conn.query_row("SELECT COUNT(*) FROM bootstrap_marker", [], |r| {
                r.get::<_, i64>(0)
            })
            .map_err(|e| crate::Error::Message(e.to_string()))
        })
        .unwrap();
    assert_eq!(count, 1, "hook's write must be visible via db handle");
    drop(db);

    // Reopen: hook runs again and adds another row, so the marker table
    // must show two entries — the first open's write survived the crate's
    // commit.
    let cfg = DatabaseConfig {
        create_if_missing: false,
        ..fx.config
    };
    let db = Database::open(cfg).unwrap();
    let count = db
        .with_connection(|conn| {
            conn.query_row("SELECT COUNT(*) FROM bootstrap_marker", [], |r| {
                r.get::<_, i64>(0)
            })
            .map_err(|e| crate::Error::Message(e.to_string()))
        })
        .unwrap();
    assert_eq!(count, 2, "first-open write must persist across reopen");
}

/// A hook that returns an error must roll back its own writes and cause the
/// whole open to fail.
struct FailingBootstrap;

impl DatabaseBootstrap for FailingBootstrap {
    fn bootstrap(&self, tx: &rusqlite::Transaction<'_>) -> crate::Result<Uuid> {
        tx.execute(
            "CREATE TABLE IF NOT EXISTS failing_marker (n INTEGER PRIMARY KEY)",
            [],
        )
        .map_err(|e| crate::Error::Message(e.to_string()))?;
        tx.execute("INSERT INTO failing_marker (n) VALUES (1)", [])
            .map_err(|e| crate::Error::Message(e.to_string()))?;
        Err(crate::Error::Hlc("hook chose to fail".to_string()))
    }
}

#[test]
fn bootstrap_hook_error_rolls_back_and_fails_open() {
    let mut fx = Fixture::new();
    fx.config.bootstrap = Arc::new(FailingBootstrap);
    let err = match Database::open(fx.config.clone()) {
        Err(e) => e,
        Ok(_) => panic!("failing bootstrap must fail open"),
    };
    assert!(matches!(err, crate::Error::Hlc(_)));

    // A subsequent open with a good hook must not see the failing hook's
    // writes — they were rolled back with its transaction.
    fx.config.bootstrap = Arc::new(StaticDeviceId(fx.device));
    let db = Database::open(fx.config).unwrap();
    let exists = db
        .with_connection(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='failing_marker'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(|e| crate::Error::Message(e.to_string()))
        })
        .unwrap();
    assert_eq!(exists, 0, "failed hook's table must not have been committed");
}

#[test]
fn reopen_with_same_provider_returns_same_uuid() {
    let fx = Fixture::new();
    Database::open(fx.config.clone()).unwrap();
    let cfg = DatabaseConfig {
        create_if_missing: false,
        ..fx.config.clone()
    };
    let db = Database::open(cfg).unwrap();
    assert_eq!(db.device_id(), fx.device);
}

#[test]
fn reopen_with_different_provider_returns_that_providers_uuid() {
    // Post-cleanup semantics: the crate does not arbitrate device IDs. If a
    // consumer legitimately serves a different UUID for the same DB file
    // (per-installation UUID lookup pattern), `Database::open` accepts it and
    // returns exactly what the provider gave. Uniqueness per (DB × replica)
    // is the consumer's job.
    let fx = Fixture::new();
    let db = Database::open(fx.config.clone()).unwrap();
    db.with_locked_conn(|conn| {
        conn.execute(
            &format!(
                "INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value) VALUES ('device_id', 'system', ?1)"
            ),
            [fx.device.to_string()],
        )?;
        Ok(())
    })
    .unwrap();
    drop(db);

    let other = Uuid::new_v4();
    let mut cfg = fx.config.clone();
    cfg.create_if_missing = false;
    cfg.bootstrap = Arc::new(StaticDeviceId(other));
    let db = Database::open(cfg).unwrap();
    assert_eq!(db.device_id(), other);
}

#[test]
fn concurrent_first_opens_serialize_via_vault_lock() {
    // With the fs2 advisory lock in place, exactly one racing `open` may
    // succeed; the other must fail with `VaultAlreadyOpenElsewhere`. This
    // replaces the pre-lock "both converge" behaviour, which was correct
    // for its era (SQLite file-locks + WAL kept them from corrupting each
    // other) but couldn't stop two live `Database` handles from co-existing
    // — the very invariant the lock enforces.
    let fx = Fixture::new();
    let start = Arc::new(Barrier::new(2));
    let first_config = fx.config.clone();
    let second_config = fx.config.clone();

    let first_start = Arc::clone(&start);
    let first = thread::spawn(move || {
        first_start.wait();
        Database::open(first_config)
    });
    let second_start = Arc::clone(&start);
    let second = thread::spawn(move || {
        second_start.wait();
        Database::open(second_config)
    });

    let first_result = first.join().unwrap();
    let second_result = second.join().unwrap();

    match (&first_result, &second_result) {
        (Ok(db), Err(err)) | (Err(err), Ok(db)) => {
            assert_eq!(db.device_id(), fx.device);
            assert_already_open(err);
        }
        _ => panic!("lock must produce exactly one Ok + one AlreadyOpen"),
    }
}

#[test]
fn concurrent_first_opens_with_different_device_ids_reject_the_loser() {
    // Post-lock semantics: whichever supplier acquired the lock first opens
    // the DB with its provider's UUID; the loser sees `AlreadyOpen` at the
    // file lock, without ever running open. The crate no longer arbitrates
    // between device IDs on the same file — see
    // `reopen_with_different_provider_returns_that_providers_uuid`.
    let fx = Fixture::new();
    let other_device = Uuid::new_v4();
    let first_config = fx.config.clone();
    let mut other_config = fx.config.clone();
    other_config.bootstrap = Arc::new(StaticDeviceId(other_device));
    let start = Arc::new(Barrier::new(2));

    let first_start = Arc::clone(&start);
    let first = thread::spawn(move || {
        first_start.wait();
        Database::open(first_config)
    });
    let second_start = Arc::clone(&start);
    let second = thread::spawn(move || {
        second_start.wait();
        Database::open(other_config)
    });

    let first_result = first.join().unwrap();
    let second_result = second.join().unwrap();

    match (&first_result, &second_result) {
        (Ok(db), Err(err)) => {
            assert_eq!(db.device_id(), fx.device);
            assert_already_open(err);
        }
        (Err(err), Ok(db)) => {
            assert_eq!(db.device_id(), other_device);
            assert_already_open(err);
        }
        _ => panic!("lock must produce exactly one Ok + one AlreadyOpen"),
    }
}

#[test]
fn second_open_while_first_alive_returns_already_open() {
    // Sequential probe of the same invariant the concurrent tests cover:
    // hold the first `Database` alive in scope, then attempt a second
    // `Database::open` on the same file — the lock must reject it.
    let fx = Fixture::new();
    let first = Database::open(fx.config.clone()).unwrap();
    let second_config = DatabaseConfig {
        create_if_missing: false,
        ..fx.config.clone()
    };
    let err = match Database::open(second_config) {
        Err(e) => e,
        Ok(_) => panic!("second open must fail while first is still alive"),
    };
    assert_already_open(&err);

    drop(first);
    let cfg = DatabaseConfig {
        create_if_missing: false,
        ..fx.config
    };
    let _revived = Database::open(cfg).expect("post-drop reopen must succeed");
}

#[test]
fn clones_share_the_underlying_lock() {
    // Every clone of the same `Database` shares one lock — dropping some
    // clones while others live must not release the lock.
    let fx = Fixture::new();
    let db = Database::open(fx.config.clone()).unwrap();
    let clone = db.clone();
    drop(db);

    // `clone` is still alive → lock still held → second open still refused.
    let contention_config = DatabaseConfig {
        create_if_missing: false,
        ..fx.config.clone()
    };
    let err = match Database::open(contention_config) {
        Err(e) => e,
        Ok(_) => panic!("clone must keep the lock held"),
    };
    assert_already_open(&err);

    drop(clone);
    let cfg = DatabaseConfig {
        create_if_missing: false,
        ..fx.config
    };
    let _revived = Database::open(cfg).expect("post-drop reopen must succeed");
}

#[cfg(unix)]
#[test]
fn open_rejects_non_utf8_database_paths() {
    use std::os::unix::ffi::OsStringExt;

    let fx = Fixture::new();
    let mut config = fx.config;
    config.path = std::path::PathBuf::from(std::ffi::OsString::from_vec(vec![b'd', b'b', 0xFF]));

    let error = match Database::open(config) {
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
    let db = Database::open(fx.config).unwrap();
    // apply_migrations at open is idempotent — a second call is a no-op.
    let report = db.apply_migrations().unwrap();
    assert_eq!(report.crate_applied, 0);
    assert_eq!(report.consumer_applied, 0);
}
