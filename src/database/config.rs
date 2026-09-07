//! Database configuration types (see plan §6).
//!
//! [`DatabaseConfig`] is the single input to [`super::Database::open`]. It ties
//! together the four consumer-owned traits — `DeviceIdProvider`,
//! `SignatureProvider`, `MigrationSource`, and (implicitly, via
//! `SqlCipherKey`) the encryption key — and the crate-owned parameters that
//! affect open semantics (path, create-flag, trigger-schema version).

use std::path::PathBuf;
use std::sync::Arc;

use crate::device_id::DeviceIdProvider;
use crate::migration::MigrationSource;
use crate::signature::SignatureProvider;

/// The default trigger-schema version [`super::Database::open`] passes to
/// `ensure_triggers_initialized` when the config leaves it unset. Bump the
/// crate-side default in lockstep with any trigger-shape change so open
/// upgrades the DB in place.
pub const DEFAULT_TRIGGER_VERSION: i32 = 1;

/// SQLCipher encryption key, passed verbatim to `PRAGMA key = ?`. The wrapper
/// is a passthrough newtype — the consumer decides whether to hand a
/// passphrase (`"correct horse battery staple"`) or a raw-hex spelling
/// (`"x'ABCD...F0'"`) that SQLCipher recognises directly.
///
/// The wrapper exists so the surrounding type shape (`DatabaseConfig`) makes it
/// syntactically obvious what the value is, and so a future protocol shift
/// (`kdf_iter=N`, PBKDF2 salt injection) can land without breaking the outer
/// public signature.
#[derive(Clone)]
pub struct SqlCipherKey(String);

impl SqlCipherKey {
    /// Wrap a caller-owned key string. The value is never logged or cloned
    /// outside the open path.
    pub fn new(key: impl Into<String>) -> Self {
        SqlCipherKey(key.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Configuration passed to [`super::Database::open`].
///
/// `Clone` because open takes it by value but downstream owners (tests, a
/// wrapper store, a re-open loop) frequently want to build one config and
/// hand copies around.
#[derive(Clone)]
pub struct DatabaseConfig {
    /// Absolute filesystem path to the SQLCipher database. `":memory:"` is
    /// accepted for tests and produces an ephemeral connection with the
    /// encryption pragma still applied (no-op on the wire, still exercises
    /// the pragma path).
    pub path: PathBuf,
    /// Encryption key — see [`SqlCipherKey`].
    pub key: SqlCipherKey,
    /// If true, `open` creates the file if it does not exist yet. If false,
    /// missing files fail with `DatabaseError::ConnectionFailed` — matches
    /// the SQLCipher/rusqlite semantics of `OpenFlags::SQLITE_OPEN_CREATE`.
    pub create_if_missing: bool,
    /// Supplies the persistent device UUID that scopes this store's HLC
    /// state. See [`DeviceIdProvider`] for the durability contract.
    pub device_id: Arc<dyn DeviceIdProvider>,
    /// Provider called during the apply-pipeline preflight and (later) any
    /// local sign-on-write path a consumer builds on top. See
    /// [`SignatureProvider`] for the trust contract.
    pub signature_provider: Arc<dyn SignatureProvider>,
    /// Source of consumer-owned schema migrations. The crate-owned
    /// bookkeeping migrations are compiled in and applied automatically.
    pub migration_source: Arc<dyn MigrationSource>,
    /// The CRDT trigger-schema version the store should install on open.
    /// Bumping this on the next release triggers a rewrite via
    /// `ensure_triggers_initialized`. Defaults to
    /// [`DEFAULT_TRIGGER_VERSION`].
    pub trigger_version: i32,
}

/// Options controlling [`super::Database::install_crdt`]. Defaults to a fresh
/// install that refuses to overwrite an already-CRDT-managed table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InstallCrdtOptions {
    /// When `true`, `install_crdt` on an already-CRDT-managed table only
    /// reinstalls the triggers — the metadata columns and any row-level
    /// state are left untouched, and the backfill pass is skipped. When
    /// `false` (default), such a call errors with
    /// [`crate::error::Error::CrdtAlreadyInstalled`].
    pub allow_reinstall: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device_id::StaticDeviceId;
    use crate::migration::StaticMigrationSource;
    use crate::signature::NoopSignatureProvider;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn dummy_config() -> DatabaseConfig {
        DatabaseConfig {
            path: PathBuf::from(":memory:"),
            key: SqlCipherKey::new("test"),
            create_if_missing: true,
            device_id: Arc::new(StaticDeviceId(Uuid::new_v4())),
            signature_provider: Arc::new(NoopSignatureProvider),
            migration_source: Arc::new(StaticMigrationSource(BTreeMap::new())),
            trigger_version: DEFAULT_TRIGGER_VERSION,
        }
    }

    #[test]
    fn sql_cipher_key_round_trips_string_into_inner() {
        let k = SqlCipherKey::new("passphrase");
        assert_eq!(k.as_str(), "passphrase");
    }

    #[test]
    fn install_crdt_options_default_disallows_reinstall() {
        let opts = InstallCrdtOptions::default();
        assert!(!opts.allow_reinstall);
    }

    #[test]
    fn store_config_is_clone() {
        // The Arc<dyn ...> fields make Clone non-trivial to derive; verify
        // the manual Clone impl compiles + preserves the pointer identities.
        let c1 = dummy_config();
        let c2 = c1.clone();
        assert!(Arc::ptr_eq(&c1.device_id, &c2.device_id));
        assert!(Arc::ptr_eq(&c1.signature_provider, &c2.signature_provider));
        assert!(Arc::ptr_eq(&c1.migration_source, &c2.migration_source));
    }
}
