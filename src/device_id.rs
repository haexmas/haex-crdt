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
/// - `Store::open` records the `device_id` observed on first successful
///   open in `haex_hlc_state`. On subsequent opens, if the supplied
///   provider returns a different `Uuid`, `Store::open` returns
///   `Error::DeviceIdMismatch` rather than silently rewriting HLC state.
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
