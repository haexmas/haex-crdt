//! Tests for the cleanup module.
//!
//! Two clusters: FK-guard mechanics (RAII + closure form with panic-safety)
//! and delete-log retention (RetentionPolicy branches, the before_prune
//! hook contract, stats). All tests run against in-memory SQLite; there is
//! no filesystem interaction.

use super::*;
use crate::crdt::columns::{
    COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN,
};
use rusqlite::Connection;

// -----------------------------------------------------------------------
// FK-guard mechanics
// -----------------------------------------------------------------------

fn fk_state(conn: &Connection) -> bool {
    conn.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
        .unwrap()
        == 1
}

#[test]
fn foreign_key_guard_disables_on_construction() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
    assert!(fk_state(&conn));

    let _guard = ForeignKeyGuard::disable(&conn).unwrap();
    assert!(!fk_state(&conn));
}

#[test]
fn foreign_key_guard_reenables_on_drop_via_block_end() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
    {
        let _guard = ForeignKeyGuard::disable(&conn).unwrap();
        assert!(!fk_state(&conn));
    }
    assert!(fk_state(&conn));
}

#[test]
fn foreign_key_guard_reenables_on_early_return_via_question_mark() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

    fn body(conn: &Connection) -> Result<(), rusqlite::Error> {
        let _guard = ForeignKeyGuard::disable(conn)?;
        conn.execute("INVALID SQL", [])?;
        Ok(())
    }
    assert!(body(&conn).is_err());
    assert!(
        fk_state(&conn),
        "FK must be re-enabled even when body returns Err via `?`"
    );
}

#[test]
fn with_fk_disabled_observes_fk_off_inside_body() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
    let _: Result<(), rusqlite::Error> = with_fk_disabled(&mut conn, |c| {
        assert!(!fk_state(c));
        Ok(())
    });
}

#[test]
fn with_fk_disabled_reenables_on_ok_and_err() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

    let _: Result<(), rusqlite::Error> = with_fk_disabled(&mut conn, |_| Ok(()));
    assert!(fk_state(&conn));

    let err: Result<(), rusqlite::Error> = with_fk_disabled(&mut conn, |c| {
        c.execute("INVALID SQL", [])?;
        Ok(())
    });
    assert!(err.is_err());
    assert!(fk_state(&conn));
}

#[test]
fn with_fk_disabled_reenables_on_panic() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();

    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), rusqlite::Error> =
            with_fk_disabled(&mut conn, |_| panic!("simulated"));
    }));
    assert!(payload.is_err(), "panic must propagate");
    assert!(fk_state(&conn), "FK must be re-enabled after a panic");
}

#[test]
fn with_fk_disabled_transaction_pattern_works() {
    // Verifies the crate's real call shape works: open a tx, do stuff, commit.
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("PRAGMA foreign_keys = ON", []).unwrap();
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .unwrap();

    let result: Result<(), rusqlite::Error> = with_fk_disabled(&mut conn, |c| {
        let tx = c.transaction()?;
        tx.execute("INSERT INTO t (id) VALUES (1)", [])?;
        tx.commit()?;
        Ok(())
    });
    result.unwrap();
    assert!(fk_state(&conn));
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

// -----------------------------------------------------------------------
// Delete-log retention
// -----------------------------------------------------------------------

/// One nanosecond per unit of `n`. HLC time-part is a `u64` nanosecond
/// count; encoding two entries N days apart requires knowing this.
const NS_PER_DAY: u64 = 24 * 60 * 60 * 1_000_000_000;

/// Encodes an HLC string in uhlc's on-the-wire format: `<decimal time>/<hex node>`.
/// The SQL `CAST(substr(...) AS INTEGER)` in the cleanup module keys on the
/// leading decimal digits — hex time-parts would silently cast to 0 and
/// break ordering. Zero-pad to 16 digits so string comparisons over the
/// full HLC also line up numerically for reader ergonomics.
fn hlc_at(ns: u64) -> String {
    format!("{ns:016}/deadbeef")
}

fn setup_cleanup_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE {TABLE_CRDT_CONFIGS} (
             key TEXT PRIMARY KEY NOT NULL,
             value TEXT,
             type TEXT
         );
         CREATE TABLE {DELETED_ROWS_TABLE} (
             id TEXT PRIMARY KEY NOT NULL,
             table_name TEXT NOT NULL,
             row_pks TEXT NOT NULL,
             {HLC_TIMESTAMP_COLUMN} TEXT,
             {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
             {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
         );"
    ))
    .unwrap();
    conn
}

fn insert_delete_log_row(conn: &Connection, id: &str, hlc: &str) {
    conn.execute(
        &format!(
            "INSERT INTO {DELETED_ROWS_TABLE} (id, table_name, row_pks, {HLC_TIMESTAMP_COLUMN}) \
             VALUES (?1, 'items', '{{\"id\":\"x\"}}', ?2)"
        ),
        [id, hlc],
    )
    .unwrap();
}

fn set_current_hlc(conn: &Connection, hlc: &str) {
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {TABLE_CRDT_CONFIGS} (key, type, value) \
             VALUES ('hlc_timestamp', 'hlc', ?1)"
        ),
        [hlc],
    )
    .unwrap();
}

fn count_delete_log(conn: &Connection) -> i64 {
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE}"),
        [],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn retention_all_deletes_every_delete_log_row() {
    let mut conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(1));
    insert_delete_log_row(&conn, "b", &hlc_at(2));
    insert_delete_log_row(&conn, "c", &hlc_at(3));
    assert_eq!(count_delete_log(&conn), 3);

    let result =
        cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |_, _| Ok(())).unwrap();
    assert_eq!(result.rows_deleted, 3);
    assert_eq!(count_delete_log(&conn), 0);
}

#[test]
fn retention_all_reports_max_pruned_hlc() {
    let mut conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(5));
    insert_delete_log_row(&conn, "b", &hlc_at(10));
    insert_delete_log_row(&conn, "c", &hlc_at(3));

    let result =
        cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |_, _| Ok(())).unwrap();
    assert_eq!(result.max_pruned_hlc, Some(hlc_at(10)));
}

#[test]
fn retention_all_on_empty_log_is_a_noop_with_none_max() {
    let mut conn = setup_cleanup_db();
    let result =
        cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |_, _| Ok(())).unwrap();
    assert_eq!(result.rows_deleted, 0);
    assert_eq!(result.max_pruned_hlc, None);
}

#[test]
fn retention_time_based_prunes_only_entries_older_than_cutoff() {
    let mut conn = setup_cleanup_db();
    // "Now" is day 10; retention is 3 days → cutoff = day 7.
    set_current_hlc(&conn, &hlc_at(10 * NS_PER_DAY));
    insert_delete_log_row(&conn, "old_1", &hlc_at(NS_PER_DAY));
    insert_delete_log_row(&conn, "old_2", &hlc_at(5 * NS_PER_DAY));
    insert_delete_log_row(&conn, "borderline", &hlc_at(7 * NS_PER_DAY - 1));
    insert_delete_log_row(&conn, "fresh_1", &hlc_at(8 * NS_PER_DAY));
    insert_delete_log_row(&conn, "fresh_2", &hlc_at(9 * NS_PER_DAY));

    let result = cleanup_deleted_rows(
        &mut conn,
        RetentionPolicy::TimeBasedDays { days: 3 },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(result.rows_deleted, 3, "old_1, old_2, borderline");
    assert_eq!(count_delete_log(&conn), 2);
    assert_eq!(result.max_pruned_hlc, Some(hlc_at(7 * NS_PER_DAY - 1)));
}

#[test]
fn retention_time_based_is_a_noop_when_no_current_hlc_recorded() {
    let mut conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(1));
    // No `hlc_timestamp` row in the config table.

    let result = cleanup_deleted_rows(
        &mut conn,
        RetentionPolicy::TimeBasedDays { days: 1 },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(result.rows_deleted, 0);
    assert_eq!(result.max_pruned_hlc, None);
    assert_eq!(count_delete_log(&conn), 1);
}

#[test]
fn retention_time_based_all_entries_fresher_than_cutoff_is_noop() {
    let mut conn = setup_cleanup_db();
    set_current_hlc(&conn, &hlc_at(10 * NS_PER_DAY));
    insert_delete_log_row(&conn, "fresh", &hlc_at(9 * NS_PER_DAY));

    let result = cleanup_deleted_rows(
        &mut conn,
        RetentionPolicy::TimeBasedDays { days: 3 },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(result.rows_deleted, 0);
    assert_eq!(result.max_pruned_hlc, None);
    assert_eq!(count_delete_log(&conn), 1);
}

#[test]
fn retention_time_based_days_zero_is_equivalent_to_all() {
    let mut conn = setup_cleanup_db();
    set_current_hlc(&conn, &hlc_at(10 * NS_PER_DAY));
    insert_delete_log_row(&conn, "a", &hlc_at(1));
    insert_delete_log_row(&conn, "b", &hlc_at(10 * NS_PER_DAY - 1));

    let result = cleanup_deleted_rows(
        &mut conn,
        RetentionPolicy::TimeBasedDays { days: 0 },
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(result.rows_deleted, 2);
}

#[test]
fn before_prune_hook_receives_max_pruned_hlc_before_delete() {
    let mut conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(5));
    insert_delete_log_row(&conn, "b", &hlc_at(10));

    let observed = std::sync::Mutex::new(None::<String>);
    let observed_count_at_hook = std::sync::Mutex::new(-1i64);

    cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |tx, max_hlc| {
        *observed.lock().unwrap() = max_hlc.map(String::from);
        let count: i64 = tx
            .query_row(
                &format!("SELECT COUNT(*) FROM {DELETED_ROWS_TABLE}"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        *observed_count_at_hook.lock().unwrap() = count;
        Ok(())
    })
    .unwrap();

    assert_eq!(observed.lock().unwrap().as_deref(), Some(hlc_at(10).as_str()));
    assert_eq!(
        *observed_count_at_hook.lock().unwrap(),
        2,
        "the hook must run BEFORE the DELETE — rows must still be present"
    );
}

#[test]
fn before_prune_hook_error_aborts_transaction_and_leaves_log_intact() {
    let mut conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(1));

    let err = cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |_, _| {
        Err(DatabaseError::StatementError {
            reason: "hook said no".to_string(),
        })
    })
    .unwrap_err();
    match err {
        DatabaseError::StatementError { reason } => assert_eq!(reason, "hook said no"),
        other => panic!("unexpected error: {other:?}"),
    }
    assert_eq!(count_delete_log(&conn), 1, "delete must have rolled back");
}

#[test]
fn before_prune_hook_can_write_to_transaction_and_commits_atomically() {
    let mut conn = setup_cleanup_db();
    // A caller-owned anchor table the hook writes into.
    conn.execute(
        "CREATE TABLE my_anchor (id INTEGER PRIMARY KEY CHECK (id = 1), max_hlc TEXT NOT NULL)",
        [],
    )
    .unwrap();
    insert_delete_log_row(&conn, "a", &hlc_at(42));

    cleanup_deleted_rows(&mut conn, RetentionPolicy::All, |tx, max_hlc| {
        let hlc = max_hlc.expect("test setup guarantees a max");
        tx.execute(
            "INSERT OR REPLACE INTO my_anchor (id, max_hlc) VALUES (1, ?1)",
            [hlc],
        )?;
        Ok(())
    })
    .unwrap();

    let stored: String = conn
        .query_row("SELECT max_hlc FROM my_anchor WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(stored, hlc_at(42));
    assert_eq!(count_delete_log(&conn), 0);
}

// -----------------------------------------------------------------------
// Stats
// -----------------------------------------------------------------------

fn create_synced_table(conn: &Connection, name: &str) {
    conn.execute(
        &format!(
            "CREATE TABLE {name} (
                 id TEXT PRIMARY KEY NOT NULL,
                 body TEXT,
                 {HLC_TIMESTAMP_COLUMN} TEXT,
                 {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
                 {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}'
             )"
        ),
        [],
    )
    .unwrap();
}

#[test]
fn stats_counts_live_rows_across_crdt_tables_only() {
    let conn = setup_cleanup_db();
    create_synced_table(&conn, "items");
    create_synced_table(&conn, "notes");
    // A no-sync table must be excluded even though it has the same
    // columns — the discovery walk keys on both the column and the
    // `_no_sync` suffix rule.
    conn.execute(
        "CREATE TABLE local_cache_no_sync (id TEXT PRIMARY KEY, value TEXT)",
        [],
    )
    .unwrap();

    conn.execute(
        &format!(
            "INSERT INTO items (id, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'a', '1/n')"
        ),
        [],
    )
    .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO items (id, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('i2', 'b', '1/n')"
        ),
        [],
    )
    .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO notes (id, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('n1', 'x', '1/n')"
        ),
        [],
    )
    .unwrap();

    let stats = get_crdt_stats(&conn).unwrap();
    assert_eq!(stats.crdt_table_count, 2);
    assert_eq!(stats.live_row_count, 3);
    assert_eq!(stats.delete_log_row_count, 0);
}

#[test]
fn stats_reports_delete_log_row_count() {
    let conn = setup_cleanup_db();
    insert_delete_log_row(&conn, "a", &hlc_at(1));
    insert_delete_log_row(&conn, "b", &hlc_at(2));

    let stats = get_crdt_stats(&conn).unwrap();
    assert_eq!(stats.delete_log_row_count, 2);
    assert_eq!(stats.crdt_table_count, 0, "delete-log table itself excluded");
}

#[test]
fn stats_empty_db_reports_zeroes() {
    let conn = setup_cleanup_db();
    let stats = get_crdt_stats(&conn).unwrap();
    assert_eq!(stats.crdt_table_count, 0);
    assert_eq!(stats.live_row_count, 0);
    assert_eq!(stats.delete_log_row_count, 0);
}
