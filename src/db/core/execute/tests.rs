//! Parse-level checks of the CRDT write helpers. The behaviour of a write
//! against a database (HLC stamping, dirty marking, column HLCs, size limit)
//! is tested through `Database::write` in `crate::database::tests::write`.

use super::*;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};

fn written(sql: &str) -> Vec<String> {
    explicitly_written_columns(&parse_single_statement(sql).unwrap())
}

#[test]
fn insert_columns_are_lowercased() {
    assert_eq!(
        written("INSERT INTO MyTable (Foo, Bar) VALUES (1, 2)"),
        vec!["foo".to_string(), "bar".to_string()]
    );
}

#[test]
fn a_columnless_insert_names_no_columns() {
    assert!(written("INSERT INTO items VALUES (1, 'x')").is_empty());
}

#[test]
fn update_assignments_are_lowercased() {
    assert_eq!(
        written("UPDATE Items SET Name = 'x' WHERE id = 1"),
        vec!["name".to_string()]
    );
}

#[test]
fn every_tuple_assignment_target_counts() {
    assert_eq!(
        written("UPDATE Items SET (Name, Body) = ('x', 'y') WHERE id = 1"),
        vec!["name".to_string(), "body".to_string()]
    );
}

#[test]
fn select_delete_and_ddl_write_no_named_columns() {
    assert!(written("SELECT * FROM t").is_empty());
    assert!(written("DELETE FROM t WHERE id = 1").is_empty());
    assert!(written("CREATE TABLE t (id INTEGER)").is_empty());
}

fn forbidden_column(sql: &str) -> String {
    match parse_crdt_write(sql) {
        Err(DatabaseError::CrdtMetaColumnWriteForbidden { column }) => column,
        other => panic!("expected CrdtMetaColumnWriteForbidden for {sql}, got {other:?}"),
    }
}

#[test]
fn an_insert_into_the_row_hlc_is_rejected() {
    let sql = format!("INSERT INTO items (id, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'forged')");
    assert_eq!(forbidden_column(&sql), HLC_TIMESTAMP_COLUMN);
}

#[test]
fn an_update_of_the_column_hlcs_is_rejected() {
    let sql = format!("UPDATE items SET {COLUMN_HLCS_COLUMN} = '{{}}' WHERE id = 'i1'");
    assert_eq!(forbidden_column(&sql), COLUMN_HLCS_COLUMN);
}

#[test]
fn a_meta_column_inside_a_tuple_assignment_is_rejected() {
    for protected in [HLC_TIMESTAMP_COLUMN, COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN] {
        let sql = format!("UPDATE items SET (name, {protected}) = ('x', 'forged') WHERE id = 'i1'");
        assert_eq!(forbidden_column(&sql), protected);
    }
}

#[test]
fn an_insert_or_update_behind_a_with_clause_is_rejected() {
    for sql in [
        format!("WITH c AS (SELECT 1) UPDATE items SET {HLC_TIMESTAMP_COLUMN} = 'forged'"),
        format!(
            "WITH c AS (SELECT 1) INSERT INTO items (id, {HLC_TIMESTAMP_COLUMN}) VALUES ('i1', 'f')"
        ),
        "WITH c AS (SELECT 1) UPDATE items SET name = 'x'".to_string(),
    ] {
        assert!(
            matches!(
                parse_crdt_write(&sql),
                Err(DatabaseError::UnsupportedStatement { .. })
            ),
            "{sql}"
        );
    }
    assert!(parse_crdt_write("WITH c AS (SELECT 1) SELECT * FROM c").is_ok());
    assert!(parse_crdt_write("WITH c AS (SELECT 1) DELETE FROM items").is_ok());
}

#[test]
fn ordinary_writes_parse() {
    assert!(parse_crdt_write("INSERT INTO items (id, name) VALUES ('i1', 'a')").is_ok());
    assert!(parse_crdt_write("UPDATE items SET name = 'b' WHERE id = 'i1'").is_ok());
}
