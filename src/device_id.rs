use uuid::Uuid;

use crate::error::Result;

/// Supplies the persistent device UUID that scopes this store's HLC state.
///
/// # Contract (plan §4.1)
///
/// - The returned `Uuid` MUST be durable per physical device and stable
///   across process restarts, OS reboots, and library upgrades within the
///   same install. `uhlc::ID` uniqueness invariants depend on this.
/// - The provider MUST NOT return a freshly generated `Uuid` on each call.
///   Consumers that don't yet have a persisted device UUID are responsible
///   for minting and persisting one **before** handing a provider to
///   `haex-crdt`.
/// - `Database::open` records the `device_id` observed on first successful
///   open in `haex_crdt_configs`. On subsequent opens, if the supplied
///   provider returns a different `Uuid`, the outcome is the consumer's
///   choice via [`crate::DeviceIdPolicy`]: `Reject` (the default) returns
///   `Error::DeviceIdMismatch` rather than silently rewriting the record,
///   and `AdoptOnMismatch` takes the supplied UUID over.
///
/// A consumer that mints its UUID per (host, database) rather than per
/// physical device satisfies the durability requirement within one host
/// while producing a fresh UUID whenever the file moves. That is the case
/// `AdoptOnMismatch` exists for: the mismatch reports a relocated database,
/// not a provider bug. Such a consumer is responsible for retiring the copy
/// the file came from — the crate cannot see the other host.
pub trait DeviceIdProvider: Send + Sync {
    fn device_id(&self) -> Result<Uuid>;
}

/// Test / simple-consumer implementation. Consumers with real device-id
/// persistence (a keystore lookup, a Tauri store lookup, etc.) implement the
/// trait themselves.
pub struct StaticDeviceId(pub Uuid);

impl DeviceIdProvider for StaticDeviceId {
    fn device_id(&self) -> Result<Uuid> {
        Ok(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn static_device_id_returns_wrapped_uuid() {
        let uuid = Uuid::new_v4();
        let provider = StaticDeviceId(uuid);
        assert_eq!(provider.device_id().unwrap(), uuid);
    }

    #[test]
    fn static_device_id_is_stable_across_multiple_calls() {
        // The trait contract forbids returning a fresh UUID per call.
        let uuid = Uuid::new_v4();
        let provider = StaticDeviceId(uuid);
        let first = provider.device_id().unwrap();
        let second = provider.device_id().unwrap();
        let third = provider.device_id().unwrap();
        assert_eq!(first, second);
        assert_eq!(second, third);
    }

    #[test]
    fn device_id_provider_is_object_safe_via_dyn_dispatch() {
        // Ensures the trait can be stored behind Arc<dyn ...> as
        // `DatabaseConfig::device_id` requires (plan §6).
        let uuid = Uuid::new_v4();
        let provider: Arc<dyn DeviceIdProvider> = Arc::new(StaticDeviceId(uuid));
        assert_eq!(provider.device_id().unwrap(), uuid);
    }
}
