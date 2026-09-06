//! Per-database advisory file lock.
//!
//! Prevents the same DB file from being opened by two processes concurrently,
//! which would corrupt CRDT HLC clocks and cause SQLite WAL races. Different
//! databases remain independently openable — only the same file is mutually
//! exclusive.
//!
//! Uses `fs2`'s cross-platform advisory `try_lock_exclusive()` — flock on
//! Unix, LockFileEx on Windows. The lock is tied to the `File` handle;
//! closing the file (on Drop) releases it automatically. On abrupt process
//! termination the OS drops the handle for us, so stale locks cannot strand
//! a database permanently unreachable.
//!
//! Ported from haex-vault's `vault_lock.rs`, renamed for this crate's naming
//! (`Database` is the top-level type; `Vault` is a haex-vault-specific term).

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;

/// Suffix appended to the database path for the lock file. Kept separate from
/// the DB so SQLite's own WAL/SHM files are not confused with it.
const LOCK_SUFFIX: &str = ".lock";

/// Holds an acquired exclusive advisory lock on a database's `.lock` file for
/// the lifetime of this handle. Dropping it releases the lock.
#[derive(Debug)]
pub struct DatabaseLock {
    /// Keeps the OS-level lock alive. Must stay owned for the whole duration
    /// of the open database — closing this handle releases the advisory lock.
    handle: File,
    lock_path: PathBuf,
    /// The database file path this lock is guarding (canonicalized). Retained
    /// so callers can verify which database is currently mounted.
    database_path: PathBuf,
}

impl DatabaseLock {
    /// Database file path that this lock is guarding (canonicalized).
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// True if this lock is guarding the same underlying DB file as `path`.
    ///
    /// Both sides are normalized so different spellings of the same database
    /// (`./x.db` vs `/abs/x.db`, or a symlink alias) resolve to the same
    /// identity. Without this, callers comparing `database_path()` against a
    /// raw caller path would falsely conclude two distinct databases are
    /// mounted.
    pub fn matches(&self, path: &Path) -> bool {
        match normalize_database_path(path) {
            Ok(normalized) => self.database_path == normalized,
            // If the candidate path can't be normalized (e.g. parent dir gone)
            // it can't match a successfully-acquired lock — treat as different.
            Err(_) => false,
        }
    }

    /// Try to acquire an exclusive advisory lock on `<database_path>.lock`.
    ///
    /// Returns `Ok(DatabaseLock)` if we got the lock, or an error if another
    /// process already holds it (or we cannot create the lock file at all).
    ///
    /// `database_path` is canonicalized so two callers using different
    /// spellings of the same DB (relative vs absolute, or via a symlink
    /// alias) acquire the same lock file. Without normalization a symlink
    /// could bypass the exclusivity this module is meant to enforce.
    ///
    /// The lock file is created on first acquire and kept around — this is
    /// fine because advisory locks are scoped to open file handles, not to
    /// the file's content.
    pub fn try_acquire(database_path: &Path) -> Result<Self, DatabaseLockError> {
        let normalized = normalize_database_path(database_path)?;
        let lock_path = lock_path_for(&normalized);

        // Create the parent dir if missing (mirrors `Database::open`'s
        // `create_if_missing` semantics for the DB file itself).
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| DatabaseLockError::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }

        let handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| DatabaseLockError::Io {
                path: lock_path.display().to_string(),
                source,
            })?;

        handle
            .try_lock_exclusive()
            .map_err(|source| classify_try_lock_error(&lock_path, source))?;

        Ok(Self {
            handle,
            lock_path,
            database_path: normalized,
        })
    }
}

/// Resolve `database_path` to an absolute, symlink-resolved form so two
/// callers using different spellings (relative paths, symlinks, `..`
/// segments) all derive the same lock file identity.
///
/// Falls back to canonicalizing the parent dir + joining the filename when
/// the database file itself doesn't exist yet — that's the expected state
/// for `Database::open` with `create_if_missing = true`, which acquires the
/// lock BEFORE any SQLite file is written.
fn normalize_database_path(database_path: &Path) -> Result<PathBuf, DatabaseLockError> {
    if let Ok(canonical) = std::fs::canonicalize(database_path) {
        return Ok(canonical);
    }

    // File doesn't exist yet — normalize the parent dir instead. This still
    // catches the symlinked-dir case while permitting create-then-open.
    let parent = database_path
        .parent()
        .ok_or_else(|| DatabaseLockError::Io {
            path: database_path.display().to_string(),
            source: io::Error::new(
                io::ErrorKind::InvalidInput,
                "database path has no parent directory",
            ),
        })?;
    let file_name = database_path
        .file_name()
        .ok_or_else(|| DatabaseLockError::Io {
            path: database_path.display().to_string(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "database path has no file name"),
        })?;

    // Empty parent (".") canonicalizes via the current working dir, which is
    // exactly the behaviour we want for relative inputs like `x.db`.
    let parent_for_canon: &Path = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };

    let parent_canonical =
        std::fs::canonicalize(parent_for_canon).map_err(|source| DatabaseLockError::Io {
            path: parent_for_canon.display().to_string(),
            source,
        })?;

    Ok(parent_canonical.join(file_name))
}

/// Classify an error returned by `try_lock_exclusive`. Only genuine lock
/// contention becomes [`DatabaseLockError::AlreadyHeld`]; everything else
/// (permission denied, unsupported filesystem, etc.) is a real I/O failure
/// and must not be surfaced as "database already open".
///
/// Uses `fs2::lock_contended_error()` as the canonical sentinel rather than
/// hard-coding `ErrorKind::WouldBlock` — the raw OS errno differs across
/// platforms (EWOULDBLOCK on Unix vs ERROR_LOCK_VIOLATION on Windows), and
/// Rust's `ErrorKind` mapping for the Windows case has shifted between
/// releases. Matching against fs2's own sentinel is the only way to stay
/// correct on every target.
fn classify_try_lock_error(lock_path: &Path, source: io::Error) -> DatabaseLockError {
    let contended = fs2::lock_contended_error();
    let is_contended = match (source.raw_os_error(), contended.raw_os_error()) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => source.kind() == contended.kind(),
    };
    if is_contended {
        DatabaseLockError::AlreadyHeld {
            path: lock_path.display().to_string(),
            source,
        }
    } else {
        DatabaseLockError::Io {
            path: lock_path.display().to_string(),
            source,
        }
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        // Best-effort release. The OS would also release on handle close,
        // but doing it explicitly lets us catch double-drop bugs in tests.
        let _ = fs2::FileExt::unlock(&self.handle);
        // Do NOT delete the lock file: deletion creates a TOCTOU race where a
        // concurrent acquirer may recreate+open the file between our unlock
        // and remove, ending up with two live handles both holding exclusive
        // locks. The file is cheap (0 bytes on disk) so we leave it in place.
        let _ = &self.lock_path; // keep field live (linter)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseLockError {
    /// Another handle already holds the lock — typically another process (or
    /// another test thread) has the same DB open. Maps to
    /// [`crate::db::error::DatabaseError::VaultAlreadyOpenElsewhere`] at the
    /// `Database::open` boundary.
    #[error("database is already open in another instance (lock at '{path}')")]
    AlreadyHeld { path: String, source: io::Error },

    /// The lock file could not be prepared (missing parent directory,
    /// permission denied, unsupported filesystem, etc.). Never a "someone
    /// else has it" — a real environmental failure.
    #[error("failed to prepare lock file at '{path}': {source}")]
    Io { path: String, source: io::Error },
}

fn lock_path_for(database_path: &Path) -> PathBuf {
    let mut lock = database_path.to_path_buf();
    // Extend the existing filename rather than replace the extension so the
    // dotted DB name + `.db.lock` is unambiguous next to `.db-shm`/`.db-wal`.
    let filename = lock
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    let mut extended = filename;
    extended.push(LOCK_SUFFIX);
    lock.set_file_name(extended);
    lock
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn second_acquire_same_path_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("my.db");

        let first = DatabaseLock::try_acquire(&db).expect("first acquire");
        let second = DatabaseLock::try_acquire(&db);
        assert!(matches!(second, Err(DatabaseLockError::AlreadyHeld { .. })));

        drop(first);

        // After release, acquisition works again — confirms the lock isn't
        // stranded by a lingering file.
        let _third = DatabaseLock::try_acquire(&db).expect("post-release acquire");
    }

    #[test]
    fn different_databases_can_lock_independently() {
        let dir = tempfile::tempdir().expect("tempdir");
        let a = DatabaseLock::try_acquire(&dir.path().join("a.db")).expect("a");
        let b = DatabaseLock::try_acquire(&dir.path().join("b.db")).expect("b");
        drop((a, b));
    }

    #[test]
    fn lock_path_for_appends_suffix() {
        let p = lock_path_for(Path::new("/x/my.db"));
        assert_eq!(p, Path::new("/x/my.db.lock"));
    }

    #[test]
    fn classify_fs2_contended_as_already_held() {
        // Use fs2's own sentinel so the test mirrors what `try_lock_exclusive`
        // actually returns on contention on this platform.
        let source = fs2::lock_contended_error();
        let mapped = classify_try_lock_error(Path::new("/x/y.db.lock"), source);
        assert!(
            matches!(mapped, DatabaseLockError::AlreadyHeld { .. }),
            "fs2::lock_contended_error must classify as AlreadyHeld, got {mapped:?}",
        );
    }

    #[test]
    fn classify_permission_denied_as_io() {
        // Regression: previously any try_lock error became AlreadyHeld, so a
        // permission-restricted FS produced a misleading "database already
        // open" message the user could not resolve.
        let source = io::Error::new(io::ErrorKind::PermissionDenied, "EACCES");
        let mapped = classify_try_lock_error(Path::new("/x/y.db.lock"), source);
        assert!(
            matches!(mapped, DatabaseLockError::Io { .. }),
            "PermissionDenied must classify as Io, got {mapped:?}",
        );
    }

    #[test]
    fn classify_unsupported_as_io() {
        // Some network filesystems return Unsupported (or a raw os error not
        // mapped to WouldBlock). These are environmental failures, not
        // contention.
        let source = io::Error::new(io::ErrorKind::Unsupported, "ENOTSUP");
        let mapped = classify_try_lock_error(Path::new("/x/y.db.lock"), source);
        assert!(
            matches!(mapped, DatabaseLockError::Io { .. }),
            "Unsupported must classify as Io, got {mapped:?}",
        );
    }

    #[test]
    fn matches_resolves_distinct_spellings_of_same_path() {
        // Regression: previously the lock keyed on the caller's raw spelling,
        // so opening the same DB via `./x.db` and `<absdir>/x.db` produced
        // two locks targeting one file.
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("x.db");
        std::fs::write(&db, b"").expect("touch db");

        let lock = DatabaseLock::try_acquire(&db).expect("acquire");

        // Spelling 1: parent dir with a `./.` redirection
        let with_dot = dir.path().join(".").join("x.db");
        assert!(lock.matches(&with_dot), "dot-redirected path must match");

        // Spelling 2: absolute path normalized via canonicalize
        let canonical = std::fs::canonicalize(&db).expect("canonicalize");
        assert!(lock.matches(&canonical), "canonical path must match");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_alias_acquires_same_lock() {
        // Without canonicalization, opening `alias.db -> real.db` would
        // produce a separate `alias.db.lock` while both SQLite handles point
        // at the same file — defeating the advisory lock.
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real.db");
        let alias = dir.path().join("alias.db");
        std::fs::write(&real, b"").expect("touch real");
        std::os::unix::fs::symlink(&real, &alias).expect("symlink");

        let first = DatabaseLock::try_acquire(&real).expect("acquire real");
        let second = DatabaseLock::try_acquire(&alias);
        assert!(
            matches!(second, Err(DatabaseLockError::AlreadyHeld { .. })),
            "symlink alias must contend on the canonical lock, got {second:?}",
        );

        drop(first);
    }
}
