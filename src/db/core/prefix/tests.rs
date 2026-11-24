use super::*;

#[test]
fn strips_main_prefix_from_select_from_clause() {
    let out = strip_main_schema_prefix("SELECT * FROM main.users");
    assert!(!out.contains("main.users"), "Got: {out}");
    assert!(out.to_uppercase().contains("FROM USERS") || out.contains("FROM users"));
}

#[test]
fn preserves_main_prefix_inside_string_literal() {
    let out = strip_main_schema_prefix("SELECT * FROM main.users WHERE note = 'main.foo'");
    assert!(!out.contains("main.users"), "table ref not stripped: {out}");
    assert!(out.contains("main.foo"), "literal must survive: {out}");
}

#[test]
fn strips_from_insert_target_and_source_subquery() {
    let out = strip_main_schema_prefix("INSERT INTO main.dst (id) SELECT id FROM main.src");
    assert!(!out.contains("main.dst"), "Got: {out}");
    assert!(!out.contains("main.src"), "Got: {out}");
}

#[test]
fn strips_from_update_and_delete() {
    let upd = strip_main_schema_prefix("UPDATE main.t SET a = 1 WHERE id = 2");
    assert!(!upd.contains("main.t"), "Got: {upd}");
    let del = strip_main_schema_prefix("DELETE FROM main.t WHERE id = 1");
    assert!(!del.contains("main.t"), "Got: {del}");
}

#[test]
fn strips_from_create_table_and_create_index_and_drop() {
    assert!(!strip_main_schema_prefix("CREATE TABLE main.t (id INTEGER)").contains("main.t"));
    // For CREATE INDEX the walker only strips from the target table
    // (matches haex-vault); the index-name qualifier is intentionally
    // left alone because SQLite accepts a qualified index name.
    let idx = strip_main_schema_prefix("CREATE INDEX i ON main.t (id)");
    assert!(!idx.contains("main.t"), "Got: {idx}");
    assert!(!strip_main_schema_prefix("DROP TABLE main.t").contains("main.t"));
}

#[test]
fn leaves_unparseable_sql_unchanged() {
    // Genuinely unparseable input cannot be rewritten safely without
    // risking changes inside literals or comments.
    let sql = "this is not sql at all main.foo more garbage";
    assert_eq!(strip_main_schema_prefix(sql), sql);
}

#[test]
fn preserves_main_prefix_inside_unparseable_literals_and_comments() {
    let sql = "not valid 'main.literal' -- main.comment";
    assert_eq!(strip_main_schema_prefix(sql), sql);
}

#[test]
fn leaves_sql_without_main_prefix_unchanged_semantically() {
    // Round-trip through sqlparser normalizes whitespace/case but never
    // introduces a `main.` prefix.
    let out = strip_main_schema_prefix("SELECT * FROM users");
    assert!(!out.contains("main."), "Got: {out}");
    assert!(out.to_uppercase().contains("FROM USERS") || out.contains("FROM users"));
}

#[test]
fn strips_from_join_and_nested_join() {
    let out = strip_main_schema_prefix(
        "SELECT * FROM main.a JOIN main.b ON a.id = b.a_id JOIN main.c ON b.id = c.b_id",
    );
    assert!(!out.contains("main.a"), "Got: {out}");
    assert!(!out.contains("main.b"), "Got: {out}");
    assert!(!out.contains("main.c"), "Got: {out}");
}

#[test]
fn strips_main_prefix_from_exists_subquery() {
    let out = strip_main_schema_prefix(
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM main.inner WHERE inner.id = 1)",
    );
    assert!(!out.contains("main.inner"), "Got: {out}");
}
