use super::*;
use rusqlite::Connection;
use std::sync::Arc;

/// Opens the in-memory database used by bootstrap contract tests.
fn open_conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

#[test]
fn static_device_id_returns_wrapped_uuid() {
    let uuid = Uuid::new_v4();
    let hook = StaticDeviceId(uuid);
    let mut conn = open_conn();
    let tx = conn.transaction().unwrap();
    assert_eq!(hook.bootstrap(&tx).unwrap(), uuid);
}

#[test]
fn static_device_id_is_stable_across_multiple_calls() {
    let uuid = Uuid::new_v4();
    let hook = StaticDeviceId(uuid);
    let mut conn = open_conn();
    for _ in 0..3 {
        let tx = conn.transaction().unwrap();
        assert_eq!(hook.bootstrap(&tx).unwrap(), uuid);
        tx.commit().unwrap();
    }
}

#[test]
fn database_bootstrap_is_object_safe_via_dyn_dispatch() {
    // Ensures the trait can be stored behind Arc<dyn ...> as
    // `DatabaseConfig::bootstrap` requires.
    let uuid = Uuid::new_v4();
    let hook: Arc<dyn DatabaseBootstrap> = Arc::new(StaticDeviceId(uuid));
    let mut conn = open_conn();
    let tx = conn.transaction().unwrap();
    assert_eq!(hook.bootstrap(&tx).unwrap(), uuid);
}
