use super::*;

#[test]
fn parses_a_single_select() {
    let sql = "SELECT * FROM users WHERE id = ?";
    let stmt = parse_single_statement(sql).unwrap();
    assert!(matches!(stmt, Statement::Query(_)));
}

#[test]
fn errors_on_invalid_sql_with_parse_variant() {
    let sql = "INVALID SQL STATEMENT";
    let err = parse_single_statement(sql).unwrap_err();
    assert!(matches!(err, DatabaseError::ParseError { .. }));
}

#[test]
fn errors_on_empty_input() {
    // sqlparser returns an empty statement list for whitespace-only input;
    // the wrapper must surface this as an error, not silently succeed.
    let err = parse_single_statement("   ").unwrap_err();
    assert!(matches!(err, DatabaseError::EmptyStatement { .. }));
}

#[test]
fn parses_multiple_statements_when_asked() {
    let sql = "SELECT 1; SELECT 2;";
    let stmts = parse_sql_statements(sql).unwrap();
    assert_eq!(stmts.len(), 2);
}

#[test]
fn preserves_whitespace_inside_string_literals() {
    let stmts = parse_sql_statements("SELECT 'a  b'").unwrap();
    assert_eq!(stmts[0].to_string(), "SELECT 'a  b'");
}

#[test]
fn statement_has_returning_flags_insert_with_returning_clause() {
    let with = parse_single_statement("INSERT INTO t (id) VALUES (1) RETURNING id").unwrap();
    let without = parse_single_statement("INSERT INTO t (id) VALUES (1)").unwrap();
    assert!(statement_has_returning(&with));
    assert!(!statement_has_returning(&without));
}

#[test]
fn statement_has_returning_flags_update_and_delete() {
    let upd = parse_single_statement("UPDATE t SET a = 1 RETURNING a").unwrap();
    let del = parse_single_statement("DELETE FROM t WHERE id = 1 RETURNING id").unwrap();
    assert!(statement_has_returning(&upd));
    assert!(statement_has_returning(&del));
}

#[test]
fn statement_has_returning_is_false_for_select() {
    let stmt = parse_single_statement("SELECT * FROM t").unwrap();
    assert!(!statement_has_returning(&stmt));
}
