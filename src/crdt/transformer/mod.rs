// src-tauri/src/crdt/transformer.rs

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::insert_transformer::InsertTransformer;
use crate::db::error::DatabaseError;
use sqlparser::ast::{
    AlterTable, Assignment, AssignmentTarget, ColumnDef, ColumnOption, ColumnOptionDef,
    CreateTable, DataType, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SetExpr,
    Statement, TableFactor, TableObject, Value,
};
use std::borrow::Cow;
use uhlc::Timestamp;

/// Konfiguration für CRDT-Spalten
#[derive(Clone)]
struct CrdtColumns {
    hlc_timestamp: &'static str,
    column_hlcs: &'static str,
    column_sigs: &'static str,
}

impl CrdtColumns {
    const DEFAULT: Self = Self {
        hlc_timestamp: HLC_TIMESTAMP_COLUMN,
        column_hlcs: COLUMN_HLCS_COLUMN,
        column_sigs: COLUMN_SIGS_COLUMN,
    };

    /// Erstellt eine HLC-Zuweisung für UPDATE
    fn create_hlc_assignment(&self, timestamp: &Timestamp) -> Assignment {
        Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                Ident::new(self.hlc_timestamp),
            )])),
            value: Expr::Value(Value::SingleQuotedString(timestamp.to_string()).into()),
        }
    }

    /// Fügt CRDT-Spalten zu einer Tabellendefinition hinzu
    /// Überschreibt vorhandene Spalten mit den gleichen Namen, um korrekte Datentypen zu garantieren
    fn add_to_table_definition(&self, columns: &mut Vec<ColumnDef>) {
        // Remove existing CRDT columns if present
        columns.retain(|c| {
            c.name.value != self.hlc_timestamp
                && c.name.value != self.column_hlcs
                && c.name.value != self.column_sigs
        });

        // Add all CRDT columns with correct types
        columns.push(ColumnDef {
            name: Ident::new(self.hlc_timestamp),
            data_type: DataType::String(None),
            options: vec![],
        });

        columns.push(ColumnDef {
            name: Ident::new(self.column_hlcs),
            data_type: DataType::String(None),
            options: vec![],
        });

        // Per-column author signatures (parallel to column_hlcs). Emitted as
        // `haex_column_sigs TEXT NOT NULL DEFAULT '{}'` so that pre-existing
        // rows and inserts that don't yet know about the column still land
        // with a valid empty map.
        columns.push(ColumnDef {
            name: Ident::new(self.column_sigs),
            data_type: DataType::String(None),
            options: vec![
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::NotNull,
                },
                ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Default(Expr::Value(
                        Value::SingleQuotedString("{}".to_string()).into(),
                    )),
                },
            ],
        });
    }
}

pub struct CrdtTransformer {
    columns: CrdtColumns,
}

impl Default for CrdtTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl CrdtTransformer {
    pub fn new() -> Self {
        Self {
            columns: CrdtColumns::DEFAULT,
        }
    }

    /// Prüft, ob eine Tabelle CRDT-Synchronisation unterstützen soll
    ///
    /// Tables are excluded from CRDT if they end with `_no_sync`.
    ///
    /// This applies to both:
    /// - Internal tables (e.g., `haex_crdt_configs_no_sync`)
    /// - Extension tables (e.g., `ext_myapp_session_no_sync`)
    ///
    /// Examples:
    /// - `haex_extensions` → CRDT-enabled (synced)
    /// - `haex_crdt_configs_no_sync` → No CRDT (internal metadata)
    /// - `ext_myapp_settings` → CRDT-enabled (synced)
    /// - `ext_myapp_cache_no_sync` → No CRDT (local cache)
    fn is_crdt_sync_table(&self, name: &ObjectName) -> bool {
        let table_name = self.normalize_table_name(name);

        // Exclude tables ending with _no_sync
        if table_name.ends_with("_no_sync") {
            return false;
        }

        true
    }

    /// Normalisiert Tabellennamen (entfernt Anführungszeichen und Schema-Präfix wie "main.")
    fn normalize_table_name(&self, name: &ObjectName) -> Cow<'_, str> {
        // Get the last part of the ObjectName (the actual table name without schema)
        // This handles cases like "main.tablename" where we only want "tablename"
        let table_name = name
            .0
            .last()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => ident.value.clone(),
                ObjectNamePart::Function(func) => func.name.to_string(),
            })
            .unwrap_or_else(|| name.to_string());

        let name_str = table_name.to_lowercase();
        Cow::Owned(name_str.trim_matches('`').trim_matches('"').to_string())
    }

    /// Transformiert ein SELECT Statement rekursiv (FROM- und JOIN-Subqueries).
    /// Deletes sind Hard-Deletes plus ein Event-Row im Delete-Log — Haupt-
    /// Tabellen führen keine Soft-Delete-Spalte, daher gibt es hier nichts
    /// mehr zu filtern. Die Funktion bleibt als Rekursionseinstieg für
    /// verschachtelte Queries.
    fn transform_select(&self, select: &mut Select) {
        for table_with_joins in &mut select.from {
            self.transform_table_factor(&mut table_with_joins.relation);
            for join in &mut table_with_joins.joins {
                self.transform_table_factor(&mut join.relation);
            }
        }
    }

    /// Transformiert einen TableFactor rekursiv (für Subqueries in FROM)
    fn transform_table_factor(&self, table_factor: &mut TableFactor) {
        match table_factor {
            TableFactor::Derived { subquery, .. } => {
                // Rekursiv die Subquery transformieren
                self.transform_query(subquery);
            }
            TableFactor::TableFunction { .. } => {
                // Table functions können auch Subqueries enthalten, aber das ist selten
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                // Nested joins rekursiv behandeln
                self.transform_table_factor(&mut table_with_joins.relation);
                for join in &mut table_with_joins.joins {
                    self.transform_table_factor(&mut join.relation);
                }
            }
            _ => {
                // Table, UNNEST, etc. - keine weitere Transformation nötig
            }
        }
    }

    /// Transformiert eine Query rekursiv (SELECT, UNION, etc.)
    pub fn transform_query(&self, query: &mut Query) {
        self.transform_set_expr(&mut query.body);
    }

    /// Transformiert einen SetExpr rekursiv
    fn transform_set_expr(&self, set_expr: &mut SetExpr) {
        match set_expr {
            SetExpr::Select(select) => {
                self.transform_select(select);
            }
            SetExpr::Query(query) => {
                self.transform_query(query);
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.transform_set_expr(left);
                self.transform_set_expr(right);
            }
            _ => {
                // Values, Insert, Update, Delete, Merge, Table - keine Transformation nötig
            }
        }
    }

    fn add_crdt_columns_to_create_table(
        &self,
        create_table: &mut CreateTable,
    ) -> Result<(), DatabaseError> {
        if create_table.query.is_some() {
            return Err(DatabaseError::UnsupportedStatement {
                sql: create_table.to_string(),
                reason: "CREATE TABLE ... AS SELECT is not supported for CRDT-synced tables"
                    .to_string(),
            });
        }

        self.columns
            .add_to_table_definition(&mut create_table.columns);
        Ok(())
    }

    // =================================================================
    // ÖFFENTLICHE API-METHODEN
    // =================================================================

    /// Transformiert ein SQL Statement für CRDT-Synchronisation
    ///
    /// Gibt `Some(table_name)` zurück wenn das Schema modifiziert wurde (CREATE TABLE, ALTER TABLE)
    /// Gibt `None` zurück für Daten-Operationen (INSERT, UPDATE, DELETE)
    pub fn transform_execute_statement(
        &self,
        stmt: &mut Statement,
        hlc_timestamp: &Timestamp,
    ) -> Result<Option<String>, DatabaseError> {
        match stmt {
            Statement::Query(query) => {
                // Recurse into subqueries. Hard-delete + delete-log model: main
                // tables never carry a soft-delete column, so nothing to filter.
                self.transform_query(query);
                Ok(None)
            }
            Statement::CreateTable(create_table) => {
                if self.is_crdt_sync_table(&create_table.name) {
                    self.add_crdt_columns_to_create_table(create_table)?;
                    Ok(Some(
                        self.normalize_table_name(&create_table.name).into_owned(),
                    ))
                } else {
                    Ok(None)
                }
            }
            Statement::Insert(insert_stmt) => {
                if let TableObject::TableName(name) = &insert_stmt.table {
                    if self.is_crdt_sync_table(name) {
                        // Hard Delete: Kein Schema-Lookup mehr nötig (kein ON CONFLICT)
                        let insert_transformer = InsertTransformer::new();
                        insert_transformer.transform_insert(insert_stmt, hlc_timestamp)?;
                    }
                }
                Ok(None)
            }
            Statement::Update(update) => {
                if let TableFactor::Table { name, .. } = &update.table.relation {
                    if self.is_crdt_sync_table(name) {
                        // Add HLC timestamp assignment. Hard-delete + delete-log
                        // model: no soft-deleted rows live in the target table,
                        // so there is nothing to filter out.
                        update
                            .assignments
                            .push(self.columns.create_hlc_assignment(hlc_timestamp));
                    }
                }
                Ok(None)
            }
            Statement::Delete(_) => {
                // DELETE stays DELETE. The BEFORE-DELETE trigger writes a row
                // into haex_deleted_rows, and the CRDT apply-path propagates
                // that to the target table on remotes.
                Ok(None)
            }
            Statement::AlterTable(AlterTable { name, .. }) => {
                if self.is_crdt_sync_table(name) {
                    Ok(Some(self.normalize_table_name(name).into_owned()))
                } else {
                    Ok(None)
                }
            }
            Statement::CreateIndex(_) => {
                // No partial-index rewrite anymore — UNIQUE indexes stay full so
                // they remain FK-parent-eligible.
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Transforms a DDL statement (CREATE TABLE) to add CRDT columns. Used by
    /// migrations to ensure all syncable tables get `haex_hlc` and
    /// `haex_column_hlcs` columns.
    ///
    /// Returns the transformed SQL string, or the original if no transformation was needed.
    pub fn transform_ddl_statement(&self, sql: &str) -> Result<String, DatabaseError> {
        use sqlparser::dialect::SQLiteDialect;
        use sqlparser::parser::Parser;

        let dialect = SQLiteDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| DatabaseError::ParseError {
                reason: e.to_string(),
                sql: sql.to_string(),
            })?;

        if statements.is_empty() {
            return Ok(sql.to_string());
        }

        let mut changed = false;
        for stmt in &mut statements {
            if let Statement::CreateTable(create_table) = stmt {
                if self.is_crdt_sync_table(&create_table.name) {
                    self.add_crdt_columns_to_create_table(create_table)?;
                    changed = true;
                }
            }
        }

        if !changed {
            return Ok(sql.to_string());
        }

        Ok(statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "))
    }
}

#[cfg(test)]
mod tests;
