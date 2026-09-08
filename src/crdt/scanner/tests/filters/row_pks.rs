//! `row_pks`: the PK allow-list, including its composite-PK
//! schema-declaration-order contract.

use super::super::*;

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
