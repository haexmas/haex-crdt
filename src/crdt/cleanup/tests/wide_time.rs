//! Cleanup compares HLC times numerically at any width: SQLite integers are
//! signed 64-bit, so a cast would stop pruning once the clock passes 2^63.

use super::*;

const PAST_I64: &str = "10000000000000000000/deadbeef";

#[test]
fn time_based_cleanup_still_prunes_once_the_clock_passes_i64_max() {
    let mut conn = setup_cleanup_db();
    set_current_hlc(&conn, PAST_I64);
    insert_delete_log_row(&conn, "old", "5000000000000000000/deadbeef");
    insert_delete_log_row(&conn, "wide", "9999000000000000000/deadbeef");
    insert_delete_log_row(&conn, "new", PAST_I64);

    let result = cleanup_deleted_rows(
        &mut conn,
        RetentionPolicy::TimeBasedDays { days: 1 },
        |_, _| Ok(()),
    )
    .unwrap();

    assert_eq!(result.rows_deleted, 2, "old and wide are older than a day");
    assert_eq!(
        result.max_pruned_hlc.as_deref(),
        Some("9999000000000000000/deadbeef")
    );
    assert_eq!(count_delete_log(&conn), 1);
}
