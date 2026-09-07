// src-tauri/src/crdt/insert_transformer.rs
// INSERT-spezifische CRDT-Transformationen (ON CONFLICT, RETURNING)

use crate::crdt::columns::HLC_TIMESTAMP_COLUMN;
use crate::db::error::DatabaseError;
use sqlparser::ast::{Expr, Ident, Insert, ObjectName, SelectItem, SetExpr, Value};
use uhlc::Timestamp;

/// Helper-Struct für INSERT-Transformationen
pub struct InsertTransformer {
    hlc_timestamp_column: &'static str,
}

impl Default for InsertTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl InsertTransformer {
    /// Creates a transformer that injects the crate's row-level HLC column.
    pub fn new() -> Self {
        Self {
            hlc_timestamp_column: HLC_TIMESTAMP_COLUMN,
        }
    }

    /// sqlparser 0.62 widened `Insert.columns` from `Vec<Ident>` to
    /// `Vec<ObjectName>` so columns can be schema-qualified (e.g. `t.col`).
    /// For the timestamp column we only ever care about the trailing name
    /// part — match against `.as_ident()` of the last part.
    fn find_or_add_column(columns: &mut Vec<ObjectName>, col_name: &'static str) -> usize {
        match columns.iter().position(|c| {
            c.0.last()
                .and_then(|part| part.as_ident())
                .map(|i| i.value == col_name)
                .unwrap_or(false)
        }) {
            Some(index) => index,
            None => {
                columns.push(ObjectName::from(Ident::new(col_name)));
                columns.len() - 1
            }
        }
    }

    /// Wenn der Index == der Länge ist, wird der Wert stattdessen gepusht.
    fn set_or_push_value(row: &mut Vec<Expr>, index: usize, value: Expr) {
        if index < row.len() {
            // Spalte war vorhanden, Wert (wahrscheinlich `?` oder NULL) ersetzen
            row[index] = value;
        } else {
            // Spalte war nicht vorhanden, Wert hinzufügen
            row.push(value);
        }
    }

    fn set_or_push_projection(projection: &mut Vec<SelectItem>, index: usize, value: Expr) {
        let item = SelectItem::UnnamedExpr(value);
        if index < projection.len() {
            projection[index] = item;
        } else {
            projection.push(item);
        }
    }

    /// Transformiert INSERT-Statements (fügt HLC-Timestamp hinzu)
    /// Hard Delete: Kein ON CONFLICT mehr nötig - gelöschte Einträge sind wirklich weg
    ///
    /// LIMITATION / TODO: `ON CONFLICT ... DO UPDATE SET` wird NICHT unterstützt.
    /// Diese Transformation hängt die HLC-Spalte nur an die INSERT-Spalten/-Werte an,
    /// aber nicht an die `DO UPDATE SET`-Assignments. Ein Upsert mit DO UPDATE
    /// erzeugt daher entweder ungültiges SQL oder eine Zeile mit veraltetem
    /// HLC-Timestamp (→ CRDT-Sync-Konflikte). Extensions müssen stattdessen
    /// `onConflictDoNothing()` + ein separates `UPDATE` verwenden (siehe z. B.
    /// stores/vault/settings.ts::setInitialSyncCompleteAsync im Host und
    /// haex-mail persistEnvelopesAsync). Ein echter Fix müsste die HLC-Spalte
    /// auch in die DO-UPDATE-Assignments injizieren.
    pub fn transform_insert(
        &self,
        insert_stmt: &mut Insert,
        timestamp: &Timestamp,
    ) -> Result<(), DatabaseError> {
        if insert_stmt.on.is_some() {
            return Err(DatabaseError::UnsupportedStatement {
                sql: insert_stmt.to_string(),
                reason: "INSERT with a conflict clause is not supported".to_string(),
            });
        }

        // The rewrite relies on the positional correspondence between the
        // explicit column list and the values/projection. Without a column
        // list, index 0 would overwrite the first caller-supplied value.
        if insert_stmt.columns.is_empty() {
            return Err(DatabaseError::UnsupportedStatement {
                sql: insert_stmt.to_string(),
                reason: "INSERT without an explicit column list is not supported".to_string(),
            });
        }

        // A wildcard projection does not expose its arity to the rewriter,
        // so appending the HLC at a positional index would be unsafe.
        if let Some(query) = insert_stmt.source.as_ref() {
            if let SetExpr::Select(select) = &*query.body {
                if select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                    )
                }) {
                    return Err(DatabaseError::UnsupportedStatement {
                        sql: insert_stmt.to_string(),
                        reason: "INSERT SELECT with a wildcard projection is not supported"
                            .to_string(),
                    });
                }
            }
        }

        // Add the row-level HLC column if not present
        let hlc_col_index =
            Self::find_or_add_column(&mut insert_stmt.columns, self.hlc_timestamp_column);

        match insert_stmt.source.as_mut() {
            Some(query) => match &mut *query.body {
                SetExpr::Values(values) => {
                    for row in &mut values.rows {
                        let hlc_value =
                            Expr::Value(Value::SingleQuotedString(timestamp.to_string()).into());

                        Self::set_or_push_value(row, hlc_col_index, hlc_value);
                    }
                }
                SetExpr::Select(select) => {
                    let hlc_value =
                        Expr::Value(Value::SingleQuotedString(timestamp.to_string()).into());

                    Self::set_or_push_projection(&mut select.projection, hlc_col_index, hlc_value);
                }
                _ => {
                    return Err(DatabaseError::UnsupportedStatement {
                        sql: insert_stmt.to_string(),
                        reason: "INSERT with unsupported source type".to_string(),
                    });
                }
            },
            None => {
                return Err(DatabaseError::UnsupportedStatement {
                    reason: "INSERT statement has no source".to_string(),
                    sql: insert_stmt.to_string(),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Direct unit tests for `InsertTransformer`. The outer
    //! `crate::crdt::transformer` tests exercise the same paths at
    //! statement-transformation level; these focus on the specific
    //! contracts of `transform_insert`.

    use super::*;
    use sqlparser::ast::Statement;
    use sqlparser::dialect::SQLiteDialect;
    use sqlparser::parser::Parser;
    use uhlc::HLC;

    fn hlc_now() -> Timestamp {
        HLC::default().new_timestamp()
    }

    fn parse_insert(sql: &str) -> Statement {
        Parser::parse_sql(&SQLiteDialect {}, sql)
            .unwrap_or_else(|e| panic!("parse `{sql}`: {e}"))
            .into_iter()
            .next()
            .expect("no statement")
    }

    fn transform(sql: &str) -> String {
        let mut stmt = parse_insert(sql);
        if let Statement::Insert(ref mut insert) = stmt {
            InsertTransformer::new()
                .transform_insert(insert, &hlc_now())
                .expect("transform_insert must succeed");
            stmt.to_string()
        } else {
            panic!("not an INSERT: {sql}");
        }
    }

    fn transform_err(sql: &str) -> DatabaseError {
        let mut stmt = parse_insert(sql);
        if let Statement::Insert(ref mut insert) = stmt {
            InsertTransformer::new()
                .transform_insert(insert, &hlc_now())
                .expect_err("transform_insert must fail")
        } else {
            panic!("not an INSERT: {sql}");
        }
    }

    #[test]
    fn single_row_values_gets_hlc_column_and_value_appended() {
        let out = transform("INSERT INTO t (id, name) VALUES ('x', 'a')");
        assert!(
            out.contains(HLC_TIMESTAMP_COLUMN),
            "HLC column must be added; got: {out}"
        );
    }

    #[test]
    fn multi_row_values_gets_hlc_appended_to_each_row() {
        // With three rows and no pre-existing HLC column, each row must
        // carry the HLC value in the appended position.
        let out = transform("INSERT INTO t (id) VALUES ('x'), ('y'), ('z')");
        let occurrences = out.matches('/').count(); // uhlc timestamps look like `<ns>/<node>`
        assert!(
            occurrences >= 3,
            "expected 3 HLC values (one per row); got: {out}"
        );
    }

    #[test]
    fn caller_supplied_hlc_column_value_is_overwritten() {
        // If the caller supplies the HLC column themselves, the transformer
        // must overwrite the value with its own (authoritative) HLC —
        // otherwise a client could inject a bogus HLC and skew LWW ordering.
        let sql =
            format!("INSERT INTO t (id, {HLC_TIMESTAMP_COLUMN}) VALUES ('x', 'attacker-hlc')");
        let out = transform(&sql);
        assert!(
            !out.contains("attacker-hlc"),
            "caller-supplied HLC must be replaced; got: {out}"
        );
    }

    #[test]
    fn insert_select_with_explicit_projection_gets_hlc_projected() {
        // Non-wildcard SELECT source: projection must gain the HLC literal at
        // the new column's index.
        let out = transform("INSERT INTO t (id) SELECT other_id FROM other");
        assert!(
            out.contains(HLC_TIMESTAMP_COLUMN),
            "HLC column must be added to INSERT SELECT; got: {out}"
        );
    }

    #[test]
    fn missing_column_list_is_rejected_with_unsupported_statement() {
        let err = transform_err("INSERT INTO t VALUES ('x', 'a')");
        assert!(
            matches!(err, DatabaseError::UnsupportedStatement { .. }),
            "expected UnsupportedStatement, got: {err:?}"
        );
    }

    #[test]
    fn wildcard_projection_is_rejected_with_unsupported_statement() {
        let err = transform_err("INSERT INTO t (id) SELECT * FROM other");
        assert!(
            matches!(err, DatabaseError::UnsupportedStatement { .. }),
            "expected UnsupportedStatement, got: {err:?}"
        );
    }

    #[test]
    fn qualified_wildcard_projection_is_rejected_with_unsupported_statement() {
        let err = transform_err("INSERT INTO t (id) SELECT other.* FROM other");
        assert!(
            matches!(err, DatabaseError::UnsupportedStatement { .. }),
            "expected UnsupportedStatement, got: {err:?}"
        );
    }

    #[test]
    fn conflict_clauses_are_rejected_before_transformation() {
        for sql in [
            "INSERT INTO t (id, name) VALUES ('x', 'a') ON CONFLICT DO NOTHING",
            "INSERT INTO t (id, name) VALUES ('x', 'a') ON CONFLICT (id) DO UPDATE SET name = excluded.name",
        ] {
            let err = transform_err(sql);
            assert!(
                matches!(err, DatabaseError::UnsupportedStatement { .. }),
                "expected conflict clause to be rejected: {err:?}"
            );
        }
    }
}
