// src-tauri/src/crdt/insert_transformer.rs
// INSERT-spezifische CRDT-Transformationen (ON CONFLICT, RETURNING)

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::db::error::DatabaseError;
use sqlparser::ast::{
    Assignment, AssignmentTarget, DoUpdate, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, Insert, ObjectName, ObjectNamePart,
    OnConflictAction, OnInsert, SelectItem, SetExpr, Value,
};
use uhlc::Timestamp;

/// Helper-Struct für INSERT-Transformationen
pub struct InsertTransformer {
    hlc_timestamp_column: &'static str,
    column_hlcs_column: &'static str,
    column_sigs_column: &'static str,
}

impl Default for InsertTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl InsertTransformer {
    /// Creates a transformer that injects the crate's row-level HLC column
    /// and, on the DO UPDATE branch of `INSERT ... ON CONFLICT ...`, the
    /// per-column-HLC JSON blob.
    pub fn new() -> Self {
        Self {
            hlc_timestamp_column: HLC_TIMESTAMP_COLUMN,
            column_hlcs_column: COLUMN_HLCS_COLUMN,
            column_sigs_column: COLUMN_SIGS_COLUMN,
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

    /// Extracts the trailing-name string of each column an assignment
    /// target refers to. `SET foo = 1` → `["foo"]`; `SET (a, b) = (1, 2)`
    /// → `["a", "b"]`. Schema qualifiers are dropped — the crate only ever
    /// cares about the bare column name.
    fn assignment_column_names(target: &AssignmentTarget) -> Vec<&str> {
        match target {
            AssignmentTarget::ColumnName(name) => name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .map(|i| vec![i.value.as_str()])
                .unwrap_or_default(),
            AssignmentTarget::Tuple(names) => names
                .iter()
                .filter_map(|n| {
                    n.0.last()
                        .and_then(|p| p.as_ident())
                        .map(|i| i.value.as_str())
                })
                .collect(),
        }
    }

    /// A column the transformer owns and must not let a caller hand-set
    /// in a DO UPDATE SET assignment.
    fn is_owned_metadata_column(&self, name: &str) -> bool {
        name == self.hlc_timestamp_column
            || name == self.column_hlcs_column
            || name == self.column_sigs_column
    }

    /// Reject the on-conflict clause if it uses an unsupported variant or
    /// if the caller tries to hand-set the CRDT metadata columns in the
    /// DO UPDATE SET assignments — the whole point of the transformer is
    /// to own that metadata.
    fn validate_on_insert(&self, on: &OnInsert, stmt: &Insert) -> Result<(), DatabaseError> {
        match on {
            OnInsert::OnConflict(conflict) => match &conflict.action {
                OnConflictAction::DoNothing => Ok(()),
                OnConflictAction::DoUpdate(du) => self.reject_caller_metadata(du, stmt),
            },
            // MySQL's ON DUPLICATE KEY UPDATE (and any future non-exhaustive
            // variant) stays rejected — the target dialect is SQLite.
            _ => Err(DatabaseError::UnsupportedStatement {
                sql: stmt.to_string(),
                reason: "INSERT with ON DUPLICATE KEY UPDATE is not supported".to_string(),
            }),
        }
    }

    fn reject_caller_metadata(
        &self,
        do_update: &DoUpdate,
        stmt: &Insert,
    ) -> Result<(), DatabaseError> {
        for assignment in &do_update.assignments {
            for col in Self::assignment_column_names(&assignment.target) {
                if self.is_owned_metadata_column(col) {
                    return Err(DatabaseError::UnsupportedStatement {
                        sql: stmt.to_string(),
                        reason: format!(
                            "INSERT ... ON CONFLICT DO UPDATE SET may not assign CRDT metadata column '{col}'; the transformer owns it"
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    /// Appends the two metadata assignments to the DO UPDATE SET clause:
    /// `haex_hlc_no_trigger = '<ts>'` and
    /// `haex_column_hlcs_no_trigger = json_set(<column>, '$.<col1>', '<ts>', ...)`
    /// so a conflict-resolution UPDATE also carries fresh HLC metadata and
    /// remains LWW-observable to peers even when the AFTER-UPDATE trigger
    /// is disabled (as it is on the apply path).
    ///
    /// Signatures (`haex_column_sigs_no_trigger`) stay untouched — those
    /// are the `SignatureProvider`'s business and get populated by the
    /// crate's post-write hook.
    fn augment_do_update(&self, do_update: &mut DoUpdate, timestamp: &Timestamp) {
        // Snapshot the caller's assigned business columns before we push
        // our own metadata assignments (otherwise json_set's path list
        // would include our own metadata column names).
        let touched: Vec<String> = do_update
            .assignments
            .iter()
            .flat_map(|a| {
                Self::assignment_column_names(&a.target)
                    .into_iter()
                    .map(str::to_string)
            })
            .filter(|c| !self.is_owned_metadata_column(c))
            .collect();

        let ts_str = timestamp.to_string();

        // haex_hlc_no_trigger = '<ts>'
        do_update.assignments.push(Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                Ident::new(self.hlc_timestamp_column),
            )])),
            value: Expr::Value(Value::SingleQuotedString(ts_str.clone()).into()),
        });

        // haex_column_hlcs_no_trigger = json_set(<column>, '$.<col1>', '<ts>', ...)
        do_update.assignments.push(Assignment {
            target: AssignmentTarget::ColumnName(ObjectName(vec![ObjectNamePart::Identifier(
                Ident::new(self.column_hlcs_column),
            )])),
            value: Self::build_column_hlcs_json_set(self.column_hlcs_column, &touched, &ts_str),
        });
    }

    /// Builds `json_set(<column_hlcs>, '$.<c1>', '<ts>', '$.<c2>', '<ts>', ...)`
    /// as an `Expr`. When `touched` is empty the call degenerates to
    /// `json_set(<column_hlcs>)`, which SQLite treats as a no-op — that's
    /// the honest thing to emit when the DO UPDATE SET only targets our
    /// own metadata (a case we normally reject upstream).
    fn build_column_hlcs_json_set(column_hlcs: &str, touched: &[String], ts: &str) -> Expr {
        let mut args: Vec<FunctionArg> = Vec::with_capacity(1 + touched.len() * 2);
        args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
            Expr::Identifier(Ident::new(column_hlcs)),
        )));
        for col in touched {
            args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                Value::SingleQuotedString(format!("$.{col}")).into(),
            ))));
            args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                Value::SingleQuotedString(ts.to_string()).into(),
            ))));
        }
        Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("json_set"))]),
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
        })
    }

    /// Transformiert INSERT-Statements (fügt HLC-Timestamp hinzu)
    ///
    /// `ON CONFLICT DO NOTHING` und `ON CONFLICT ... DO UPDATE SET ...` sind
    /// unterstützt: die INSERT-Spalten/-Werte bekommen weiterhin die
    /// `haex_hlc_no_trigger`-Spalte, und die DO UPDATE SET-Zuweisungen
    /// bekommen zusätzlich `haex_hlc_no_trigger = '<ts>'` sowie
    /// `haex_column_hlcs_no_trigger = json_set(...)` angehängt, sodass auch
    /// eine Konflikt-Auflösung frische HLC-Metadaten trägt.
    ///
    /// MySQL's `ON DUPLICATE KEY UPDATE` bleibt abgelehnt (kein SQLite-Feature).
    pub fn transform_insert(
        &self,
        insert_stmt: &mut Insert,
        timestamp: &Timestamp,
    ) -> Result<(), DatabaseError> {
        if let Some(on) = &insert_stmt.on {
            self.validate_on_insert(on, insert_stmt)?;
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

        // Augment the DO UPDATE SET branch (if any) with the two metadata
        // assignments. DO NOTHING needs no help — the INSERT-path metadata
        // handles causality when the insert actually lands.
        if let Some(OnInsert::OnConflict(conflict)) = &mut insert_stmt.on {
            if let OnConflictAction::DoUpdate(do_update) = &mut conflict.action {
                self.augment_do_update(do_update, timestamp);
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

    /// Extract the `DO UPDATE SET` fragment of the transformed SQL as a
    /// lowercased substring, for assertions that don't depend on exact
    /// whitespace. Panics if the SQL has no such clause.
    fn do_update_fragment(sql: &str) -> String {
        let lower = sql.to_lowercase();
        let idx = lower
            .find("do update set")
            .unwrap_or_else(|| panic!("no DO UPDATE SET in: {sql}"));
        lower[idx..].to_string()
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

    // ---------------------------------------------------------------------
    // ON CONFLICT ... DO UPDATE / DO NOTHING support
    // ---------------------------------------------------------------------

    #[test]
    fn on_conflict_do_update_set_appends_haex_hlc_to_update_clause() {
        let out = transform(
            "INSERT INTO t (id, name) VALUES ('x', 'a') \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name",
        );
        let fragment = do_update_fragment(&out);
        assert!(
            fragment.contains(HLC_TIMESTAMP_COLUMN),
            "DO UPDATE SET must gain the row-level HLC assignment; got: {out}"
        );
    }

    #[test]
    fn on_conflict_do_update_set_appends_column_hlcs_json() {
        let out = transform(
            "INSERT INTO t (id, name) VALUES ('x', 'a') \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name",
        );
        let fragment = do_update_fragment(&out);
        assert!(
            fragment.contains(COLUMN_HLCS_COLUMN),
            "DO UPDATE SET must gain the per-column HLC JSON assignment; got: {out}"
        );
        // The assignment must be a json_set call that touches the caller's
        // assigned column so peer merges see the same per-column HLC map
        // even when the AFTER-UPDATE trigger is disabled (apply path).
        assert!(
            fragment.contains("json_set"),
            "DO UPDATE SET must build column_hlcs via json_set; got: {out}"
        );
        assert!(
            fragment.contains("'$.name'"),
            "json_set must include a JSON path for the caller-assigned column; got: {out}"
        );
    }

    #[test]
    fn on_conflict_do_nothing_still_injects_insert_path_metadata() {
        let out =
            transform("INSERT INTO t (id, name) VALUES ('x', 'a') ON CONFLICT (id) DO NOTHING");
        // No DO UPDATE SET augmentation for the DO NOTHING branch.
        assert!(
            out.to_lowercase().contains("do nothing"),
            "DO NOTHING must be preserved; got: {out}"
        );
        // The INSERT-path metadata still lands on the row.
        assert!(
            out.contains(HLC_TIMESTAMP_COLUMN),
            "INSERT-path HLC column must still be injected; got: {out}"
        );
    }

    #[test]
    fn on_conflict_do_update_rejects_caller_supplied_haex_hlc() {
        let sql = format!(
            "INSERT INTO t (id, name) VALUES ('x', 'a') \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, \
             {HLC_TIMESTAMP_COLUMN} = 'attacker-hlc'"
        );
        let err = transform_err(&sql);
        assert!(
            matches!(err, DatabaseError::UnsupportedStatement { .. }),
            "caller-supplied haex_hlc_no_trigger must be rejected; got: {err:?}"
        );
    }

    #[test]
    fn on_conflict_do_update_rejects_caller_supplied_column_hlcs() {
        // The transformer owns column_hlcs too — a caller must not set it
        // in the DO UPDATE SET assignments.
        let sql = format!(
            "INSERT INTO t (id, name) VALUES ('x', 'a') \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, \
             {COLUMN_HLCS_COLUMN} = '{{}}'"
        );
        let err = transform_err(&sql);
        assert!(
            matches!(err, DatabaseError::UnsupportedStatement { .. }),
            "caller-supplied column_hlcs must be rejected; got: {err:?}"
        );
    }

    #[test]
    fn on_conflict_variant_that_targets_composite_pk_transforms_correctly() {
        // Composite conflict target: `(a, b) DO UPDATE SET c = excluded.c`.
        let out = transform(
            "INSERT INTO t (a, b, c) VALUES ('x', 'y', 'z') \
             ON CONFLICT (a, b) DO UPDATE SET c = excluded.c",
        );
        let fragment = do_update_fragment(&out);
        assert!(
            fragment.contains(HLC_TIMESTAMP_COLUMN),
            "composite-PK DO UPDATE SET must gain the HLC assignment; got: {out}"
        );
        assert!(
            fragment.contains("'$.c'"),
            "json_set must include a JSON path for the assigned column c; got: {out}"
        );
        // Conflict target must still name both PK columns (sqlparser
        // emits the parenthesised form with no space after `CONFLICT`).
        assert!(
            out.to_lowercase().contains("on conflict(a, b)"),
            "composite conflict target must be preserved; got: {out}"
        );
    }

    #[test]
    fn insert_without_on_conflict_still_transforms_unchanged() {
        // Regression lock: the plain INSERT-VALUES path is not affected by
        // the new ON CONFLICT handling. A single haex_hlc_no_trigger column
        // is appended, no DO UPDATE SET clause appears.
        let out = transform("INSERT INTO t (id, name) VALUES ('x', 'a')");
        assert!(
            !out.to_lowercase().contains("on conflict"),
            "plain INSERT must not gain an ON CONFLICT clause; got: {out}"
        );
        assert!(
            !out.to_lowercase().contains("do update set"),
            "plain INSERT must not gain a DO UPDATE SET clause; got: {out}"
        );
        assert!(
            out.contains(HLC_TIMESTAMP_COLUMN),
            "plain INSERT still gets the row-level HLC column; got: {out}"
        );
    }
}
