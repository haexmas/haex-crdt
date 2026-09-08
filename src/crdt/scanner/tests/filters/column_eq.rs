//! `column_eq`: the SQL-level row restriction — its identifier gate,
//! its fail-closed missing-column rule, and what may be a target.

use super::super::*;

#[test]
fn column_eq_filter_restricts_scan_to_matching_rows() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "bucket TEXT, name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i1', 'b1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i2', 'b2', 'b')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("bucket", "b1")),
            ..Default::default()
        },
    )
    .unwrap();
    let pks: HashSet<&str> = changes.iter().map(|c| c.row_pks.as_str()).collect();
    assert_eq!(pks.len(), 1, "only the b1 row may be scanned: {changes:?}");
    assert!(pks.contains(r#"{"id":"i1"}"#));
}

#[test]
fn column_eq_filter_naming_an_absent_column_yields_no_rows() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );

    // Fail closed: a filter whose target column does not exist means "no
    // matching rows", never "the whole table".
    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("no_such_column", "whatever")),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(changes.is_empty(), "must fail closed, got: {changes:?}");
}

#[test]
fn column_eq_filter_composes_with_after_hlc_cursor() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "bucket TEXT, name TEXT");
    let t1 = insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i1', 'b1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i2', 'b2', 'b')",
    );
    // Both rows change after t1; only the b1 one may survive the filter.
    insert_row_via_transformer(&conn, &hlc, "UPDATE items SET name = 'a2' WHERE id = 'i1'");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "UPDATE items SET name = 'b_updated' WHERE id = 'i2'",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        Some(&t1),
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("bucket", "b1")),
            ..Default::default()
        },
    )
    .unwrap();
    let cols: Vec<(&str, &JsonValue)> = changes
        .iter()
        .map(|c| (c.column_name.as_str(), &c.value))
        .collect();
    assert_eq!(
        cols,
        vec![("name", &json!("a2"))],
        "only i1's post-cursor name change may surface: {changes:?}"
    );
}

#[test]
fn column_eq_filter_value_with_sql_metacharacters_matches_literally() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "bucket TEXT, name TEXT");
    // A value carrying a quote and a LIKE wildcard: it must bind as a
    // parameter and compare literally, not be interpolated or pattern-
    // matched.
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i1', 'o''brien_%', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket, name) VALUES ('i2', 'obrien_x', 'b')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("bucket", "o'brien_%")),
            ..Default::default()
        },
    )
    .unwrap();
    let pks: HashSet<&str> = changes.iter().map(|c| c.row_pks.as_str()).collect();
    assert_eq!(pks.len(), 1, "literal match only: {changes:?}");
    assert!(pks.contains(r#"{"id":"i1"}"#));
}

#[test]
fn column_eq_filter_rejects_an_identifier_unsafe_column_name() {
    let (conn, hlc, dev) = make_fixture();
    // A column whose NAME closes the quoted identifier and opens a
    // tautology. Nothing else in the crate validates it: the `_no_trigger`
    // suffix keeps it out of `partition_columns`' SELECT list, and the
    // trigger installer strips `_no_trigger` names before its own
    // identifier check — the filter is the only path from this name into
    // SQL.
    create_crdt_table(
        &conn,
        "items",
        r#"bucket TEXT, "bucket"" OR 1=1 OR ""bucket_no_trigger" TEXT"#,
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket) VALUES ('i1', 'b1')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket) VALUES ('i2', 'b2')",
    );

    // Interpolated unquoted this yields
    //   WHERE "bucket" OR 1=1 OR "bucket_no_trigger" = ?1
    // — valid SQL with one bind param that matches every row, so a filter
    // value matching nothing would ship the whole table.
    let err = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some((r#"bucket" OR 1=1 OR "bucket_no_trigger"#, "matches-nothing")),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, DatabaseError::ValidationError { .. }),
        "an unsafe filter identifier must be an error, not a silent scan: {err:?}"
    );
}

#[test]
fn missing_crdt_metadata_outranks_an_absent_filter_column() {
    let (conn, _hlc, dev) = make_fixture();
    // A table with no CRDT metadata at all is a table-shape error, not "no
    // matching rows": the likeliest consumer bug behind it is a forgotten
    // `install_crdt`, and `Ok(vec![])` would hide it behind a scan that
    // silently ships nothing forever. The precedence otherwise lives only
    // in statement order, so pin it.
    conn.execute(
        "CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, body TEXT)",
        [],
    )
    .unwrap();

    let err = scan_table_for_local_changes(
        &conn,
        "t",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("no_such_column", "x")),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, DatabaseError::ExecutionError { .. }),
        "the metadata error must outrank the absent-column fail-closed rule: {err:?}"
    );
}

#[test]
fn column_eq_filter_rejects_an_unsafe_name_before_any_schema_check() {
    let (conn, _hlc, dev) = make_fixture();
    // The identifier gate sits at the boundary, so an unusable filter name
    // is reported as such regardless of the table's shape — this table has
    // no CRDT metadata at all and would otherwise raise the table-shape
    // error instead.
    conn.execute(
        "CREATE TABLE t (id TEXT PRIMARY KEY NOT NULL, body TEXT)",
        [],
    )
    .unwrap();

    let err = scan_table_for_local_changes(
        &conn,
        "t",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some((r#"body" OR 1=1 OR "body"#, "matches-nothing")),
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, DatabaseError::ValidationError { .. }),
        "an unusable filter name must not depend on table state: {err:?}"
    );
}

#[test]
fn column_eq_filter_may_target_a_no_trigger_column() {
    let (conn, hlc, dev) = make_fixture();
    // A consumer may scope a scan by bookkeeping that is itself opted out
    // of change tracking. Membership is checked against the whole schema,
    // so any column the table has is a legal filter target.
    create_crdt_table(&conn, "items", "bucket_no_trigger TEXT, name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket_no_trigger, name) VALUES ('i1', 'b1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket_no_trigger, name) VALUES ('i2', 'b2', 'b')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("bucket_no_trigger", "b1")),
            ..Default::default()
        },
    )
    .unwrap();
    let mut cols: Vec<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    cols.sort_unstable();
    assert_eq!(
        cols,
        vec!["bucket_no_trigger", "name"],
        "filtering on a `_no_trigger` column neither withholds it nor the \
         row's tracked columns: {changes:?}"
    );
    assert!(
        changes.iter().all(|c| c.row_pks == r#"{"id":"i1"}"#),
        "only the matching row may be returned: {changes:?}"
    );
}

#[test]
fn column_eq_filter_may_target_a_no_sync_column() {
    let (conn, hlc, dev) = make_fixture();
    // Arguably the most useful target of all: scope a scan by bookkeeping
    // that never travels. The column restricts the rows without appearing
    // in the result — membership is checked against the whole schema, not
    // the emitted data columns.
    create_crdt_table(&conn, "items", "bucket_no_sync TEXT, name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket_no_sync, name) VALUES ('i1', 'b1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, bucket_no_sync, name) VALUES ('i2', 'b2', 'b')",
    );

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            column_eq: Some(("bucket_no_sync", "b1")),
            ..Default::default()
        },
    )
    .unwrap();
    let cols: Vec<&str> = changes.iter().map(|c| c.column_name.as_str()).collect();
    assert_eq!(
        cols,
        vec!["name"],
        "the filter column restricts rows without shipping: {changes:?}"
    );
    assert_eq!(changes[0].row_pks, r#"{"id":"i1"}"#);
}
