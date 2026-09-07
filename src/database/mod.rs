//! Public `Database` facade (plan §6).
//!
//! [`Database`] ties together the crate's four consumer-owned traits
//! (`DeviceIdProvider`, `SignatureProvider`, `MigrationSource`, plus the
//! SQLCipher key) and exposes the CRDT operations as method calls. Consumers
//! do not need to touch the internal helpers unless they explicitly opt into
//! `with_connection` (see the crate `raw-connection` feature).
//!
//! # Usage scope
//!
//! One [`Database`] handle owns exactly one [`rusqlite::Connection`] behind
//! an internal `Mutex`. The intended shape is **one `Database` per DB file
//! per process**, shared across threads and async tasks via
//! [`Database::clone`] — the internal `Arc` makes clones cheap, and every
//! clone routes through the same lock.
//!
//! Opening the same DB file from **two processes** is rejected. As the very
//! first step of [`Database::open`], the fs2-backed [`crate::DatabaseLock`]
//! acquires an exclusive advisory lock on `<path>.lock`. A second process
//! (or a second in-process `Database::open` while a live handle still holds
//! the lock) fails immediately with
//! [`crate::db::error::DatabaseError::VaultAlreadyOpenElsewhere`]. Different
//! DB files remain independently openable — the lock only serializes on the
//! same canonicalized path, and the OS releases it automatically on process
//! exit so a crashed process cannot strand a database.
//!
//! # Open lifecycle
//!
//! [`Database::open`] runs, in order:
//! 1. Open the SQLCipher connection (`create_if_missing` decides whether a
//!    missing file is created or errors).
//! 2. Apply crate-owned CRDT bookkeeping migrations, then consumer
//!    migrations (see [`crate::run_migrations`]).
//! 3. Initialize the HLC service from the persisted row in
//!    `haex_crdt_configs_no_sync` — or seed it on first open.
//! 4. Enforce the device-id contract (plan §4.1): the first-open device UUID
//!    is stored in `haex_crdt_configs_no_sync`; every subsequent open rejects a
//!    provider that returns a different UUID with
//!    [`crate::Error::DeviceIdMismatch`].
//! 5. Ensure CRDT triggers are at the requested `trigger_version`.

pub mod config;
mod install;

pub use config::{DatabaseConfig, InstallCrdtOptions, SqlCipherKey, DEFAULT_TRIGGER_VERSION};

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use crate::crdt::apply::{apply_remote_changes, ApplyReport};
use crate::crdt::cleanup::{cleanup_deleted_rows, CleanupResult, RetentionPolicy};
use crate::crdt::hlc::HlcService;
use crate::crdt::scanner::{scan_dirty_tables, scan_table_for_local_changes, ColumnChange};
use crate::db::connection_context::ConnectionContext;
use crate::db::core::open_and_init_db;
use crate::db::error::DatabaseError;
use crate::db::init::ensure_triggers_initialized;
use crate::db::lock::{DatabaseLock, DatabaseLockError};
use crate::db::migrations::{run_migrations, MigrationReport};
use crate::error::{Error, Result};
use crate::signature::{RemoteChanges, SignatureProvider};
use crate::table_names::TABLE_CRDT_CONFIGS;

/// Config-row key under which the device UUID recorded on first open lives
/// (see plan §4.1). Public so consumers who need to peek at raw
/// bookkeeping via `with_connection` can find the row.
pub const CONFIG_KEY_DEVICE_ID: &str = "device_id";

/// The public facade — one `Database` per opened SQLCipher database. Cloning
/// shares the underlying connection, so a `Database` handed to multiple threads
/// or long-lived tasks always sees the same locked write path.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

struct DatabaseInner {
    conn: Mutex<Connection>,
    hlc: HlcService,
    signature_provider: Arc<dyn SignatureProvider>,
    #[allow(dead_code)] // kept for future re-check / diagnostics
    migration_source: Arc<dyn MigrationSource>,
    device_uuid: Uuid,
    /// Advisory file lock guarding the DB from cross-process concurrent
    /// mounts. Held for the lifetime of every clone of this `Database`;
    /// dropping the last clone releases the OS-level lock via `Drop`.
    /// See [`crate::db::lock`].
    #[allow(dead_code)] // held for its Drop side effect
    lock: DatabaseLock,
}

// Trait re-import to keep the `Arc<dyn ...>` field readable above without a
// full path.
use crate::migration::MigrationSource;

impl Database {
    /// Open (or create) the SQLCipher store described by `config`. See the
    /// module docs for the full open lifecycle.
    pub fn open(config: DatabaseConfig) -> Result<Self> {
        let path_str = config
            .path
            .to_str()
            .ok_or_else(|| DatabaseError::ValidationError {
                reason: "database path must be valid UTF-8".to_string(),
            })?;

        // Acquire the advisory file lock BEFORE opening SQLite so a
        // second process racing us gets a clean `VaultAlreadyOpenElsewhere`
        // instead of colliding on the WAL pragma or the device-id
        // arbitration write.
        let lock = DatabaseLock::try_acquire(&config.path).map_err(map_lock_error)?;

        let hlc = HlcService::new();
        let ctx = ConnectionContext::new();
        let mut conn = open_and_init_db(
            path_str,
            config.key.as_str(),
            config.create_if_missing,
            hlc.clone(),
            ctx,
        )?;

        run_migrations(&mut conn, config.migration_source.as_ref())?;

        hlc.initialize_in_place(&conn, config.device_id.as_ref())
            .map_err(|e| DatabaseError::HlcError {
                reason: e.to_string(),
            })?;

        let supplied_uuid = config
            .device_id
            .device_id()
            .map_err(|e| DatabaseError::HlcError {
                reason: e.to_string(),
            })?;
        reconcile_device_id(&conn, supplied_uuid)?;

        ensure_triggers_initialized(&mut conn, config.trigger_version)?;

        Ok(Database {
            inner: Arc::new(DatabaseInner {
                conn: Mutex::new(conn),
                hlc,
                signature_provider: config.signature_provider,
                migration_source: config.migration_source,
                device_uuid: supplied_uuid,
                lock,
            }),
        })
    }

    /// The HLC service this store owns. Callers that want to observe the
    /// current transaction-HLC or seed an out-of-band timestamp can grab it
    /// here without touching the raw connection.
    pub fn hlc(&self) -> &HlcService {
        &self.inner.hlc
    }

    /// The device UUID observed on first successful open and re-checked on
    /// every subsequent open. Convenience for consumers that want to log or
    /// display it alongside sync progress.
    pub fn device_id(&self) -> Uuid {
        self.inner.device_uuid
    }

    /// Re-run the migration engine. `Database::open` already calls this once,
    /// so callers only need it after they mutate the connection out-of-band
    /// (e.g. a test that resets state) or when driving a redundant retry.
    pub fn apply_migrations(&self) -> Result<MigrationReport> {
        self.with_locked_conn(|conn| run_migrations(conn, self.inner.migration_source.as_ref()))
    }

    /// Install CRDT metadata + triggers on `table_name`, backfilling any
    /// pre-existing rows so they immediately participate in LWW (plan §6).
    ///
    /// See [`InstallCrdtOptions`] for the reinstall knob.
    pub fn install_crdt(&self, table_name: &str, opts: InstallCrdtOptions) -> Result<()> {
        let provider = Arc::clone(&self.inner.signature_provider);
        self.with_locked_conn(|conn| {
            install::install_crdt(conn, table_name, opts, &self.inner.hlc, provider.as_ref())
        })
    }

    /// List every table the trigger installer has marked dirty since the
    /// last drain. Wraps [`scan_dirty_tables`] with the store's connection
    /// lock so callers don't have to hold it themselves.
    pub fn scan_dirty_tables(&self) -> Result<Vec<String>> {
        self.with_locked_conn(|conn| scan_dirty_tables(conn).map_err(Error::from))
    }

    /// Scan one table for local changes newer than `after_hlc`. See
    /// [`scan_table_for_local_changes`] for the filter semantics; this method
    /// injects the store's device id automatically so scanner-side authoring
    /// attribution matches what the apply pipeline will see on the receiver.
    pub fn scan_table_for_local_changes(
        &self,
        table_name: &str,
        after_hlc: Option<&str>,
        origin_node_filter: Option<u128>,
        row_pks_filter: Option<&HashSet<String>>,
    ) -> Result<Vec<ColumnChange>> {
        let device_str = self.inner.device_uuid.to_string();
        self.with_locked_conn(|conn| {
            scan_table_for_local_changes(
                conn,
                table_name,
                after_hlc,
                &device_str,
                origin_node_filter,
                row_pks_filter,
            )
            .map_err(Error::from)
        })
    }

    /// Merge a remote batch into the local DB. See
    /// [`apply_remote_changes`] for the trust contract (plan §4.2).
    pub fn apply_remote_changes(&self, changes: RemoteChanges) -> Result<ApplyReport> {
        let provider = Arc::clone(&self.inner.signature_provider);
        self.with_locked_conn(|conn| {
            apply_remote_changes(conn, changes, &self.inner.hlc, provider.as_ref())
        })
    }

    /// Purge delete-log rows per [`RetentionPolicy`]. `before_prune` runs
    /// inside the tx before the actual DELETE, so a consumer can advance
    /// per-space sync anchors atomically alongside the retention pass.
    pub fn cleanup_deleted_rows<F>(
        &self,
        policy: RetentionPolicy,
        before_prune: F,
    ) -> Result<CleanupResult>
    where
        F: FnOnce(&rusqlite::Transaction, Option<&str>) -> std::result::Result<(), DatabaseError>,
    {
        self.with_locked_conn(|conn| {
            cleanup_deleted_rows(conn, policy, before_prune).map_err(Error::from)
        })
    }

    /// Escape hatch — the raw `rusqlite::Connection` under the store's lock.
    /// Behind the `raw-connection` feature per plan §6: both consumers must
    /// link the same `rusqlite` crate instance for the callback types to
    /// match.
    #[cfg(feature = "raw-connection")]
    pub fn with_connection<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> Result<R>,
    {
        let guard = self
            .inner
            .conn
            .lock()
            .map_err(|_| DatabaseError::MutexPoisoned {
                reason: "Database connection mutex poisoned".to_string(),
            })?;
        f(&guard)
    }

    // ---- internal helpers ------------------------------------------------

    fn with_locked_conn<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Connection) -> Result<R>,
    {
        let mut guard = self
            .inner
            .conn
            .lock()
            .map_err(|_| DatabaseError::MutexPoisoned {
                reason: "Database connection mutex poisoned".to_string(),
            })?;
        f(&mut guard)
    }
}

/// Enforce the device-id contract from plan §4.1: first-open records the
/// UUID, later opens reject a mismatched provider with
/// [`Error::DeviceIdMismatch`]. Stored under the reserved config key
/// [`CONFIG_KEY_DEVICE_ID`] in the `haex_crdt_configs_no_sync` table (already
/// materialised by the crate bootstrap migration).
fn reconcile_device_id(conn: &Connection, supplied: Uuid) -> Result<()> {
    // The insert is the arbitration point for concurrent first opens. The
    // config key is unique, so exactly one value can win; every opener then
    // compares against the value that actually persisted rather than against
    // its own earlier observation of an empty table.
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO {TABLE_CRDT_CONFIGS} (key, type, value) \
             VALUES (?1, 'system', ?2)"
        ),
        params![CONFIG_KEY_DEVICE_ID, supplied.to_string()],
    )
    .map_err(DatabaseError::from)?;

    let recorded: String = conn
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?1"),
            params![CONFIG_KEY_DEVICE_ID],
            |r| r.get(0),
        )
        .optional()
        .map_err(DatabaseError::from)?
        .ok_or_else(|| DatabaseError::ValidationError {
            reason: "device_id config row disappeared after initialization".to_string(),
        })?;

    let expected = Uuid::parse_str(&recorded).map_err(|e| DatabaseError::ValidationError {
        reason: format!("stored device_id is not a UUID: {e}"),
    })?;
    if expected != supplied {
        return Err(Error::DeviceIdMismatch { expected, supplied });
    }
    Ok(())
}

/// Map a [`DatabaseLockError`] into the crate's top-level [`Error`], routing
/// genuine contention into the distinct
/// [`DatabaseError::VaultAlreadyOpenElsewhere`] variant so UI callers can
/// render a "database already open elsewhere" message rather than a raw
/// filesystem error.
fn map_lock_error(err: DatabaseLockError) -> Error {
    match err {
        DatabaseLockError::AlreadyHeld { path, source } => {
            DatabaseError::VaultAlreadyOpenElsewhere {
                path,
                reason: source.to_string(),
            }
            .into()
        }
        DatabaseLockError::Io { path, source } => DatabaseError::IoError {
            path,
            reason: source.to_string(),
        }
        .into(),
    }
}

#[cfg(test)]
mod tests;
