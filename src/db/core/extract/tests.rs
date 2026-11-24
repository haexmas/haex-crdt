use super::*;

#[test]
fn extracts_simple_select() {
    assert_eq!(
        extract_table_names_from_sql("SELECT * FROM users").unwrap(),
        vec!["users"]
    );
}

#[test]
fn extracts_select_with_join_in_from_order() {
    assert_eq!(
        extract_table_names_from_sql(
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id"
        )
        .unwrap(),
        vec!["users", "posts"]
    );
}

#[test]
fn extracts_insert_and_insert_select_source() {
    assert_eq!(
        extract_table_names_from_sql("INSERT INTO dst (id) SELECT id FROM src").unwrap(),
        vec!["dst", "src"]
    );
}

#[test]
fn extracts_update_and_delete() {
    assert_eq!(
        extract_table_names_from_sql("UPDATE users SET name = ? WHERE id = ?").unwrap(),
        vec!["users"]
    );
    assert_eq!(
        extract_table_names_from_sql("DELETE FROM users WHERE id = ?").unwrap(),
        vec!["users"]
    );
}

#[test]
fn extracts_from_ddl_statements() {
    assert_eq!(
        extract_table_names_from_sql("CREATE TABLE new_t (id INTEGER)").unwrap(),
        vec!["new_t"]
    );
    assert_eq!(
        extract_table_names_from_sql("ALTER TABLE t ADD COLUMN a TEXT").unwrap(),
        vec!["t"]
    );
    assert_eq!(
        extract_table_names_from_sql("DROP TABLE t").unwrap(),
        vec!["t"]
    );
    assert_eq!(
        extract_table_names_from_sql("CREATE INDEX i ON t (a)").unwrap(),
        vec!["t"]
    );
}

#[test]
fn recurses_into_from_subquery() {
    assert_eq!(
        extract_table_names_from_sql("SELECT * FROM (SELECT id FROM users) AS sub").unwrap(),
        vec!["users"]
    );
}

#[test]
fn recurses_into_where_subquery_and_in_subquery() {
    let out = extract_table_names_from_sql(
        "SELECT u.name FROM users u WHERE u.id IN (SELECT id FROM active_ids)",
    )
    .unwrap();
    assert!(out.contains(&"users".to_string()));
    assert!(out.contains(&"active_ids".to_string()));
}

#[test]
fn recurses_into_where_between_subquery() {
    let out = extract_table_names_from_sql(
        "SELECT u.name FROM users u \
         WHERE u.created_at > (SELECT MIN(created_at) FROM sessions)",
    )
    .unwrap();
    assert!(out.contains(&"users".to_string()));
    assert!(out.contains(&"sessions".to_string()));
}

#[test]
fn primary_table_returns_first_reference() {
    assert_eq!(
        extract_primary_table_name_from_sql(
            "SELECT u.name FROM users u JOIN posts p ON u.id = p.user_id"
        )
        .unwrap(),
        Some("users".to_string())
    );
    assert_eq!(
        extract_primary_table_name_from_sql("SELECT 1").unwrap(),
        None
    );
}

#[test]
fn invalid_sql_surfaces_parse_error() {
    assert!(extract_table_names_from_sql("INVALID SQL").is_err());
}
