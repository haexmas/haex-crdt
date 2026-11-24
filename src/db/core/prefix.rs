//! Removes the `main.` schema qualifier that `sqlparser` inserts when it
//! re-serializes SQLite AST nodes. SQLite doesn't need this prefix and
//! surfaces `no such table` when it appears — see the sqlparser upstream
//! discussion in the same-named haex-vault module.
//!
//! An AST walker strips the qualifier only from actual table references, so
//! string literals that happen to contain `main.foo` are preserved. Inputs
//! that fail to parse are returned unchanged because they cannot be safely
//! rewritten without quote-aware SQL tokenization.

use sqlparser::ast::{
    Expr, FromTable, ObjectName, ObjectNamePart, Query, Select, SetExpr, Statement, TableFactor,
    TableObject,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

/// Removes the `main.` schema qualifier from parsed SQL references.
pub fn strip_main_schema_prefix(sql: &str) -> String {
    let dialect = SQLiteDialect {};
    if let Ok(mut statements) = Parser::parse_sql(&dialect, sql) {
        for statement in &mut statements {
            strip_main_from_statement(statement);
        }
        statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        sql.to_string()
    }
}

fn strip_main_from_object_name(name: &mut ObjectName) {
    if name.0.len() >= 2 {
        if let Some(ObjectNamePart::Identifier(ident)) = name.0.first() {
            if ident.value.eq_ignore_ascii_case("main") {
                name.0.remove(0);
            }
        }
    }
}

fn strip_main_from_statement(statement: &mut Statement) {
    match statement {
        Statement::Query(query) => {
            strip_main_from_query(query);
        }
        Statement::Insert(insert) => {
            if let TableObject::TableName(ref mut name) = insert.table {
                strip_main_from_object_name(name);
            }
            if let Some(ref mut source) = insert.source {
                strip_main_from_query(source);
            }
        }
        Statement::Update(update) => {
            strip_main_from_table_factor(&mut update.table.relation);
            if let Some(ref mut selection) = update.selection {
                strip_main_from_expr(selection);
            }
        }
        Statement::Delete(delete) => {
            match &mut delete.from {
                FromTable::WithFromKeyword(ref mut table_refs)
                | FromTable::WithoutKeyword(ref mut table_refs) => {
                    for table_ref in table_refs.iter_mut() {
                        strip_main_from_table_factor(&mut table_ref.relation);
                        for join in &mut table_ref.joins {
                            strip_main_from_table_factor(&mut join.relation);
                        }
                    }
                }
            }
            for name in &mut delete.tables {
                strip_main_from_object_name(name);
            }
            if let Some(ref mut selection) = delete.selection {
                strip_main_from_expr(selection);
            }
        }
        Statement::CreateTable(create) => {
            strip_main_from_object_name(&mut create.name);
        }
        Statement::AlterTable(alter) => {
            strip_main_from_object_name(&mut alter.name);
        }
        Statement::Drop { ref mut names, .. } => {
            for name in names.iter_mut() {
                strip_main_from_object_name(name);
            }
        }
        Statement::CreateIndex(create_index) => {
            strip_main_from_object_name(&mut create_index.table_name);
        }
        _ => {}
    }
}

fn strip_main_from_query(query: &mut Query) {
    strip_main_from_set_expr(&mut query.body);
}

fn strip_main_from_set_expr(set_expr: &mut SetExpr) {
    match set_expr {
        SetExpr::Select(select) => {
            strip_main_from_select(select);
        }
        SetExpr::Query(query) => {
            strip_main_from_query(query);
        }
        SetExpr::SetOperation {
            ref mut left,
            ref mut right,
            ..
        } => {
            strip_main_from_set_expr(left);
            strip_main_from_set_expr(right);
        }
        _ => {}
    }
}

fn strip_main_from_select(select: &mut Select) {
    for table_ref in &mut select.from {
        strip_main_from_table_factor(&mut table_ref.relation);
        for join in &mut table_ref.joins {
            strip_main_from_table_factor(&mut join.relation);
        }
    }
    if let Some(ref mut selection) = select.selection {
        strip_main_from_expr(selection);
    }
}

fn strip_main_from_table_factor(table_factor: &mut TableFactor) {
    match table_factor {
        TableFactor::Table { ref mut name, .. } => {
            strip_main_from_object_name(name);
        }
        TableFactor::Derived {
            ref mut subquery, ..
        } => {
            strip_main_from_query(subquery);
        }
        TableFactor::NestedJoin {
            ref mut table_with_joins,
            ..
        } => {
            strip_main_from_table_factor(&mut table_with_joins.relation);
            for join in &mut table_with_joins.joins {
                strip_main_from_table_factor(&mut join.relation);
            }
        }
        _ => {}
    }
}

fn strip_main_from_expr(expr: &mut Expr) {
    match expr {
        Expr::Exists {
            ref mut subquery, ..
        } => {
            strip_main_from_query(subquery);
        }
        Expr::Subquery(ref mut subquery) => {
            strip_main_from_query(subquery);
        }
        Expr::BinaryOp {
            ref mut left,
            ref mut right,
            ..
        } => {
            strip_main_from_expr(left);
            strip_main_from_expr(right);
        }
        Expr::UnaryOp { ref mut expr, .. } => {
            strip_main_from_expr(expr);
        }
        Expr::InSubquery {
            ref mut expr,
            ref mut subquery,
            ..
        } => {
            strip_main_from_expr(expr);
            strip_main_from_query(subquery);
        }
        Expr::Between {
            ref mut expr,
            ref mut low,
            ref mut high,
            ..
        } => {
            strip_main_from_expr(expr);
            strip_main_from_expr(low);
            strip_main_from_expr(high);
        }
        Expr::Nested(ref mut inner) => {
            strip_main_from_expr(inner);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests;
