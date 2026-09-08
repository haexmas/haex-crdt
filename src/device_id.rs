use rusqlite::Transaction;
use uuid::Uuid;

use crate::error::Result;

/// Runs the consumer's bootstrap phase for one `Database::open` and returns
/// the device UUID that will be used as this open's HLC node id.
///
/// # When it runs
///
/// [`crate::Database::open`] calls [`bootstrap`](Self::bootstrap) **after**
/// crate-owned and consumer-owned migrations have been applied, and
/// **before** HLC initialization or CRDT trigger installation. It hands the
/// implementation a fresh transaction; the crate commits it on `Ok`, or
/// rolls it back on `Err`. Anything the hook writes lands in the same
/// atomic step that decides the device UUID.
///
/// # What the hook may do
///
/// - Read from the consumer's own tables (freshly migrated).
/// - Insert or update consumer-owned rows — for example, look up a
///   per-installation device row in a consumer registry table, or insert a
///   fresh one on first open.
/// - Perform side-effecting I/O outside the DB (read a file that pins an
///   installation-scoped UUID, mint one, and fsync it). The crate does not
///   observe that I/O; it only observes the returned UUID and any writes
///   committed inside the transaction.
///
/// # What the hook MUST NOT do
///
/// - Commit or roll back `tx`. Ownership stays with `Database::open`.
/// - Touch CRDT bookkeeping tables (`haex_crdt_*_no_sync`) or CRDT-tracked
///   tables that carry `_no_trigger` metadata columns; those are populated
///   only after HLC and trigger initialization run. The bootstrap phase is
///   for unsigned, non-CRDT setup only.
/// - Rely on `crate::current_hlc()` or any HLC-derived value; HLC is not
///   initialized yet.
///
/// # Invariants
///
/// - Across opens of the *same* DB file by the *same* logical replica, the
///   implementation MUST return the same `Uuid` — otherwise HLC causality
///   on that replica is broken.
/// - Enforcing uniqueness per (DB file × replica) is the consumer's job.
///   Consumers that legitimately serve different UUIDs to the same DB file
///   for different logical replicas (per-installation UUID lookup, as in
///   haex-vault and holzi) are directly supported: the crate does not
///   store or arbitrate the device UUID.
pub trait DatabaseBootstrap: Send + Sync {
    /// Performs consumer-owned initialization and returns this open's device UUID.
    fn bootstrap(&self, tx: &Transaction<'_>) -> Result<Uuid>;
}

/// Test / simple-consumer implementation. Returns a fixed UUID and does not
/// touch the transaction. Consumers with real per-installation lookup
/// (haex-vault, holzi) implement the trait themselves.
pub struct StaticDeviceId(pub Uuid);

impl DatabaseBootstrap for StaticDeviceId {
    fn bootstrap(&self, _tx: &Transaction<'_>) -> Result<Uuid> {
        Ok(self.0)
    }
}

#[cfg(test)]
mod tests {
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
}
