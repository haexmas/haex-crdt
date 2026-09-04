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

impl InsertTransformer {
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
    /// Diese Transformation hängt `haex_hlc` nur an die INSERT-Spalten/-Werte an,
    /// aber nicht an die `DO UPDATE SET`-Assignments. Ein Upsert mit DO UPDATE
    /// erzeugt daher entweder ungültiges SQL oder eine Zeile mit veraltetem
    /// HLC-Timestamp (→ CRDT-Sync-Konflikte). Extensions müssen stattdessen
    /// `onConflictDoNothing()` + ein separates `UPDATE` verwenden (siehe z. B.
    /// stores/vault/settings.ts::setInitialSyncCompleteAsync im Host und
    /// haex-mail persistEnvelopesAsync). Ein echter Fix müsste `haex_hlc` auch
    /// in die DO-UPDATE-Assignments injizieren.
    pub fn transform_insert(
        &self,
        insert_stmt: &mut Insert,
        timestamp: &Timestamp,
    ) -> Result<(), DatabaseError> {
        // Add haex_hlc column if not exists
        let hlc_col_index =
            Self::find_or_add_column(&mut insert_stmt.columns, self.hlc_timestamp_column);

        // ON CONFLICT Logik komplett entfernt!
        // Bei Hard Deletes gibt es keine Tombstone-Einträge mehr zu reaktivieren
        // UNIQUE Constraint Violations sind echte Fehler
        // (ON CONFLICT DO UPDATE ist bewusst nicht unterstützt — siehe Doc-Kommentar oben)

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
