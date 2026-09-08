//! Filter tests for [`scan_table_for_local_changes`]: origin-node,
//! PK allow-list, and single-column equality.

use super::*;

// -----------------------------------------------------------------------
// origin_node_filter (ping-pong prevention)
// -----------------------------------------------------------------------

#[test]
fn origin_node_filter_emits_only_this_nodes_writes() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'ours')",
    );
    // A row whose per-column HLC carries a DIFFERENT node id (simulates a
    // row applied from a remote peer). Insert directly (no transformer) so
    // we control the HLC node-id encoded in the column-HLC map. Node IDs
    // are 32-hex-char (16-byte) uhlc IDs.
    let foreign_hlc = "42/deadbeefdeadbeefdeadbeefdeadbe";
    let foreign_column_hlcs = format!("{{\"name\":\"{foreign_hlc}\"}}");
    conn.execute(
        &format!(
            "INSERT INTO items (id, name, {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN}) \
             VALUES ('i2', 'theirs', ?1, ?2)"
        ),
        [foreign_hlc, foreign_column_hlcs.as_str()],
    )
    .unwrap();

    let our_node = device_uuid_to_hlc_node(&dev.to_string()).expect("dev uuid parses");
    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            origin_node: Some(our_node),
            ..Default::default()
        },
    )
    .unwrap();

    // Only i1 (our write) should surface; i2 was authored elsewhere.
    let names: Vec<&JsonValue> = changes.iter().map(|c| &c.value).collect();
    assert_eq!(names, vec![&json!("ours")]);
}

// -----------------------------------------------------------------------
// row_pks_filter (allow-list)
// -----------------------------------------------------------------------

#[test]
fn row_pks_filter_restricts_scan_to_allow_listed_rows() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'a')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i2', 'b')",
    );
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i3', 'c')",
    );

    let mut wanted = HashSet::new();
    wanted.insert(r#"{"id":"i1"}"#.to_string());
    wanted.insert(r#"{"id":"i3"}"#.to_string());

    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            row_pks: Some(&wanted),
            ..Default::default()
        },
    )
    .unwrap();
    let pks: HashSet<&str> = changes.iter().map(|c| c.row_pks.as_str()).collect();
    assert_eq!(pks.len(), 2);
    assert!(pks.contains(r#"{"id":"i1"}"#));
    assert!(pks.contains(r#"{"id":"i3"}"#));
    assert!(!pks.contains(r#"{"id":"i2"}"#));
}

#[test]
fn row_pks_filter_composite_pk_matches_schema_declaration_order() {
    let (conn, hlc, _dev) = make_fixture();
    // Composite PK declared as (col_b, col_a) — schema order is
    // deliberately non-alphabetical so the test catches accidental
    // sorted-key encoding.
    conn.execute(
        &format!(
            "CREATE TABLE composites (
                 col_b TEXT NOT NULL,
                 col_a TEXT NOT NULL,
                 body TEXT,
                 {HLC_TIMESTAMP_COLUMN} TEXT,
                 {COLUMN_HLCS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
                 {COLUMN_SIGS_COLUMN} TEXT NOT NULL DEFAULT '{{}}',
                 PRIMARY KEY (col_b, col_a)
             )"
        ),
        [],
    )
    .unwrap();
    {
        let tx = conn.unchecked_transaction().unwrap();
        setup_triggers_for_table(&tx, "composites", false).unwrap();
        tx.commit().unwrap();
    }
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO composites (col_b, col_a, body) VALUES ('yy', 'xx', 'hi')",
    );

    let dev = "test-device";
    // Alphabetical key encoding would be `{"col_a":"xx","col_b":"yy"}` —
    // the scanner MUST NOT accept that; only the schema-declaration form
    // `{"col_b":"yy","col_a":"xx"}` matches.
    let mut wrong = HashSet::new();
    wrong.insert(r#"{"col_a":"xx","col_b":"yy"}"#.to_string());
    let changes = scan_table_for_local_changes(
        &conn,
        "composites",
        None,
        dev,
        ScanFilters {
            row_pks: Some(&wrong),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        changes.is_empty(),
        "alphabetical-order key encoding must not match; got: {changes:?}"
    );

    let mut right = HashSet::new();
    right.insert(r#"{"col_b":"yy","col_a":"xx"}"#.to_string());
    let changes = scan_table_for_local_changes(
        &conn,
        "composites",
        None,
        dev,
        ScanFilters {
            row_pks: Some(&right),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!changes.is_empty(), "schema-declaration form must match");
    for c in &changes {
        assert_eq!(c.row_pks, r#"{"col_b":"yy","col_a":"xx"}"#);
    }
}

// -----------------------------------------------------------------------
// column_eq_filter (SQL-level row restriction)
// -----------------------------------------------------------------------

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
