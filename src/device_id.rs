use uuid::Uuid;

use crate::error::Result;

/// Supplies the device UUID used as this open's HLC node id.
///
/// # Contract
///
/// - The returned `Uuid` scopes HLC causality for the current `Database::open`
///   call and every operation on the resulting handle. `uhlc::ID` uniqueness
///   invariants apply for the lifetime of that handle.
/// - Across opens of the *same* DB file by the *same* logical replica, the
///   consumer MUST return the same `Uuid` — otherwise HLC causality on that
///   replica is broken.
/// - Consumers that legitimately serve different UUIDs to the same DB file
///   for different logical replicas (e.g. a per-installation UUID lookup, as
///   in haex-vault) are directly supported: `Database::open` no longer stores
///   or arbitrates the device UUID and simply uses what the provider returns.
///   Enforcing uniqueness per (DB file × replica) is the consumer's job.
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
