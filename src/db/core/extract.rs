//! Extracts table names referenced by a SQL statement via AST traversal.
//! Used by the executor to route permission checks and CRDT decisions per
//! target table.

use crate::db::core::parsing::parse_single_statement;
use crate::db::error::DatabaseError;
use sqlparser::ast::{Expr, Query, Select, SetExpr, Statement, TableFactor, TableObject};

pub fn extract_table_names_from_sql(sql: &str) -> Result<Vec<String>, DatabaseError> {
    let statement = parse_single_statement(sql)?;
    Ok(extract_table_names_from_statement(&statement))
}

/// Returns the first table reference from the statement, or `None` if none
/// is present (e.g. a bare `SELECT 1`).
pub fn extract_primary_table_name_from_sql(sql: &str) -> Result<Option<String>, DatabaseError> {
    let table_names = extract_table_names_from_sql(sql)?;
    Ok(table_names.into_iter().next())
}

pub fn extract_table_names_from_statement(statement: &Statement) -> Vec<String> {
    let mut tables = Vec::new();

    match statement {
        Statement::Query(query) => {
            extract_tables_from_query_recursive(query, &mut tables);
        }
        Statement::Insert(insert) => {
            if let TableObject::TableName(name) = &insert.table {
                tables.push(name.to_string());
            }
            if let Some(source) = &insert.source {
                extract_tables_from_query_recursive(source, &mut tables);
            }
        }
        Statement::Update(update) => {
            extract_tables_from_table_factor(&update.table.relation, &mut tables);
            for assignment in &update.assignments {
                extract_tables_from_expr_recursive(&assignment.value, &mut tables);
            }
            if let Some(selection) = &update.selection {
                extract_tables_from_expr_recursive(selection, &mut tables);
            }
        }
        Statement::Delete(delete) => {
            use sqlparser::ast::FromTable;
            match &delete.from {
                FromTable::WithFromKeyword(table_refs) | FromTable::WithoutKeyword(table_refs) => {
                    for table_ref in table_refs {
                        extract_tables_from_table_factor(&table_ref.relation, &mut tables);
                    }
                }
            }
            for table_name in &delete.tables {
                tables.push(table_name.to_string());
            }
            if let Some(selection) = &delete.selection {
                extract_tables_from_expr_recursive(selection, &mut tables);
            }
        }
        Statement::CreateTable(create) => {
            tables.push(create.name.to_string());
        }
        Statement::AlterTable(alter) => {
            tables.push(alter.name.to_string());
        }
        Statement::Drop { names, .. } => {
            for name in names {
                tables.push(name.to_string());
            }
        }
        Statement::CreateIndex(create_index) => {
            tables.push(create_index.table_name.to_string());
        }
        Statement::Truncate(truncate) => {
            for table_name in &truncate.table_names {
                tables.push(table_name.to_string());
            }
        }
        _ => {}
    }

    tables
}

fn extract_tables_from_query_recursive(query: &Query, tables: &mut Vec<String>) {
    extract_tables_from_set_expr_recursive(&query.body, tables);
}

fn extract_tables_from_select(select: &Select, tables: &mut Vec<String>) {
    for table_ref in &select.from {
        extract_tables_from_table_factor(&table_ref.relation, tables);
        for join in &table_ref.joins {
            extract_tables_from_table_factor(&join.relation, tables);
        }
    }
    if let Some(selection) = &select.selection {
        extract_tables_from_expr_recursive(selection, tables);
    }
}

fn extract_tables_from_expr_recursive(expr: &Expr, tables: &mut Vec<String>) {
    match expr {
        Expr::Subquery(subquery) => {
            extract_tables_from_query_recursive(subquery, tables);
        }
        Expr::BinaryOp { left, right, .. } => {
            extract_tables_from_expr_recursive(left, tables);
            extract_tables_from_expr_recursive(right, tables);
        }
        Expr::UnaryOp { expr, .. } => {
            extract_tables_from_expr_recursive(expr, tables);
        }
        Expr::InSubquery { expr, subquery, .. } => {
            extract_tables_from_expr_recursive(expr, tables);
            extract_tables_from_query_recursive(subquery, tables);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            extract_tables_from_expr_recursive(expr, tables);
            extract_tables_from_expr_recursive(low, tables);
            extract_tables_from_expr_recursive(high, tables);
        }
        _ => {}
    }
}

fn extract_tables_from_table_factor(table_factor: &TableFactor, tables: &mut Vec<String>) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_query_recursive(subquery, tables);
        }
        TableFactor::TableFunction { .. } => {}
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_tables_from_table_factor(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                extract_tables_from_table_factor(&join.relation, tables);
            }
        }
        _ => {}
    }
}

fn extract_tables_from_set_expr_recursive(set_expr: &SetExpr, tables: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            extract_tables_from_select(select, tables);
        }
        SetExpr::Query(sub_query) => {
            extract_tables_from_set_expr_recursive(&sub_query.body, tables);
        }
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr_recursive(left, tables);
            extract_tables_from_set_expr_recursive(right, tables);
        }

        SetExpr::Values(_)
        | SetExpr::Table(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Merge(_)
        | SetExpr::Delete(_) => {}
    }
}

#[cfg(test)]
mod tests {
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
}
