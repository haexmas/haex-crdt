use std::sync::{Arc, Barrier};

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

/// A dangling final symlink and its absent target must contend on one lock.
#[cfg(unix)]
#[test]
fn dangling_symlink_alias_contends_before_target_exists() {
    let dir = tempfile::tempdir().expect("tempdir");
    let real = dir.path().join("real.db");
    let alias = dir.path().join("alias.db");
    std::os::unix::fs::symlink(&real, &alias).expect("symlink");
    assert!(
        !real.exists(),
        "the target must be absent before both opens"
    );

    let start = Arc::new(Barrier::new(2));
    let alias_start = Arc::clone(&start);
    let alias_thread = std::thread::spawn(move || {
        alias_start.wait();
        DatabaseLock::try_acquire(&alias)
    });
    let real_start = Arc::clone(&start);
    let real_thread = std::thread::spawn(move || {
        real_start.wait();
        DatabaseLock::try_acquire(&real)
    });

    let alias_result = alias_thread.join().expect("alias thread");
    let real_result = real_thread.join().expect("real thread");

    match (&alias_result, &real_result) {
        (Ok(_), Err(DatabaseLockError::AlreadyHeld { .. }))
        | (Err(DatabaseLockError::AlreadyHeld { .. }), Ok(_)) => {}
        (alias_result, real_result) => panic!(
            "dangling symlink and target must share one lock; got {alias_result:?} + {real_result:?}"
        ),
    }
}
