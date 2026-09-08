//! `Database` test suite. Split into submodules to stay under the crate's
//! 500-LoC per-file budget:
//!
//! - [`open`] — open lifecycle, reopen, concurrent-open (lock), non-UTF-8,
//!   migration idempotence
//! - [`install_crdt`] — `install_crdt` semantics: empty, backfill, sig
//!   binding, reinstall refuse/allow, unsafe identifier
//! - [`delegates`] — method delegates (apply/scan/cleanup)
//!
//! Tests use a temp-file SQLCipher DB rather than `:memory:` —
//! `Database::open` insists on WAL journaling which `:memory:` cannot provide.

use std::collections::BTreeMap;
use std::sync::Arc;

use tempfile::TempDir;
use uuid::Uuid;

use super::*;
use crate::device_id::StaticDeviceId;
use crate::error::Result;
use crate::migration::{MigrationName, StaticMigrationSource};
use crate::signature::{AuthorId, NoopSignatureProvider, SignatureProvider};

mod delegates;
mod install_crdt;
mod open;

pub(super) fn source(entries: &[(&str, &str)]) -> Arc<StaticMigrationSource> {
    let mut m = BTreeMap::new();
    for (n, c) in entries {
        m.insert(MigrationName::from(*n), (*c).to_string());
    }
    Arc::new(StaticMigrationSource(m))
}

pub(super) struct Fixture {
    pub _tmp: TempDir,
    pub config: DatabaseConfig,
    pub device: Uuid,
}

impl Fixture {
    pub fn with_source(migration_source: Arc<dyn crate::migration::MigrationSource>) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("database.db");
        let device = Uuid::new_v4();
        let config = DatabaseConfig {
            path,
            key: SqlCipherKey::new("test-key"),
            create_if_missing: true,
            bootstrap: Arc::new(StaticDeviceId(device)),
            signature_provider: Arc::new(NoopSignatureProvider),
            migration_source,
            trigger_version: DEFAULT_TRIGGER_VERSION,
        };
        Fixture {
            _tmp: tmp,
            config,
            device,
        }
    }

    pub fn new() -> Self {
        Self::with_source(source(&[]))
    }
}

/// `SignatureProvider` used by the backfill-sig-binding test — round-trips
/// the preimage into the sig so the persisted value can be reproduced by
/// re-running the same preimage function.
pub(super) struct EchoSignatureProvider;

impl SignatureProvider for EchoSignatureProvider {
    fn sign_column(&self, preimage: &[u8]) -> Result<Vec<u8>> {
        Ok(preimage.to_vec())
    }

    fn verify_column(&self, _preimage: &[u8], _sig: &serde_json::Value) -> Result<()> {
        Ok(())
    }

    fn author_id(&self) -> AuthorId {
        AuthorId::anonymous()
    }
}

pub(super) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Assert that `err` is `VaultAlreadyOpenElsewhere` (matched by message
/// substring so the check survives future error-variant edits).
pub(super) fn assert_already_open(err: &crate::Error) {
    let msg = err.to_string();
    assert!(
        msg.contains("already open"),
        "expected VaultAlreadyOpenElsewhere, got {err:?}",
    );
}
