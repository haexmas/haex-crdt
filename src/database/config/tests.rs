use super::*;
use crate::device_id::StaticDeviceId;
use crate::migration::StaticMigrationSource;
use crate::signature::NoopSignatureProvider;
use std::collections::BTreeMap;
use uuid::Uuid;

fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}

fn dummy_config() -> DatabaseConfig {
    DatabaseConfig {
        path: PathBuf::from(":memory:"),
        key: SqlCipherKey::new("test"),
        create_if_missing: true,
        bootstrap: Arc::new(StaticDeviceId(Uuid::new_v4())),
        signature_provider: Arc::new(NoopSignatureProvider),
        migration_source: Arc::new(StaticMigrationSource(BTreeMap::new())),
        trigger_version: DEFAULT_TRIGGER_VERSION,
        max_transaction_bytes: crate::MAX_CRDT_TRANSACTION_BYTES,
        max_value_bytes: MAX_VALUE_BYTES,
    }
}

#[test]
fn sql_cipher_key_round_trips_string_into_inner() {
    let k = SqlCipherKey::new("passphrase");
    assert_eq!(k.as_str(), "passphrase");
}

#[test]
fn sql_cipher_key_erases_on_drop() {
    // A compile-time assertion: the type promises to erase its buffer when dropped.
    assert_zeroize_on_drop::<SqlCipherKey>();
}

#[test]
fn sql_cipher_key_clone_is_an_independent_erasing_copy() {
    let k1 = SqlCipherKey::new("passphrase");
    let k2 = k1.clone();
    assert_eq!(k1.as_str(), "passphrase");
    assert_eq!(k2.as_str(), "passphrase");
    drop(k1);
    assert_eq!(k2.as_str(), "passphrase");
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
    assert!(Arc::ptr_eq(&c1.bootstrap, &c2.bootstrap));
    assert!(Arc::ptr_eq(&c1.signature_provider, &c2.signature_provider));
    assert!(Arc::ptr_eq(&c1.migration_source, &c2.migration_source));
}
