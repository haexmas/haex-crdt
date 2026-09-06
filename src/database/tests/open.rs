//! `Database::open` lifecycle tests: fresh vs reopen, device-id contract,
//! concurrent opens (now serialized by the fs2 vault lock), non-UTF-8 path
//! rejection, migration idempotence.

use std::sync::{Arc, Barrier};
use std::thread;

use uuid::Uuid;

use super::super::*;
use super::{assert_already_open, source, Fixture};
use crate::device_id::StaticDeviceId;

#[test]
fn open_fresh_bootstraps_bookkeeping_and_records_device_id() {
    let fx = Fixture::new();
    let db = Database::open(fx.config.clone()).unwrap();
    assert_eq!(db.device_id(), fx.device);
}

#[test]
fn reopen_with_same_device_id_succeeds() {
    let fx = Fixture::new();
    Database::open(fx.config.clone()).unwrap();
    // A second open on the same path with the same provider must succeed —
    // the previous open's `Database` is dropped inline so the lock is free.
    let cfg = DatabaseConfig {
        create_if_missing: false,
        ..fx.config.clone()
    };
    let db = Database::open(cfg).unwrap();
    assert_eq!(db.device_id(), fx.device);
}

#[test]
fn reopen_with_different_device_id_returns_device_id_mismatch() {
    let fx = Fixture::new();
    Database::open(fx.config.clone()).unwrap();

    let other = Uuid::new_v4();
    let mut cfg = fx.config.clone();
    cfg.create_if_missing = false;
    cfg.device_id = Arc::new(StaticDeviceId(other));
    let err = match Database::open(cfg) {
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
    // Post-lock semantics: whichever supplier acquired the lock first
    // becomes the device-id winner; the loser sees `AlreadyOpen` and never
    // reaches the device-id arbitration. `DeviceIdMismatch` is now reserved
    // for the sequential re-open path — see
    // `reopen_with_different_device_id_returns_device_id_mismatch`.
    let fx = Fixture::new();
    let other_device = Uuid::new_v4();
    let first_config = fx.config.clone();
    let mut other_config = fx.config.clone();
    other_config.device_id = Arc::new(StaticDeviceId(other_device));
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
