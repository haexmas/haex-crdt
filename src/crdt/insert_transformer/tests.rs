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
    let sql = format!("INSERT INTO t (id, {HLC_TIMESTAMP_COLUMN}) VALUES ('x', 'attacker-hlc')");
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
    let out = transform("INSERT INTO t (id, name) VALUES ('x', 'a') ON CONFLICT (id) DO NOTHING");
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
        "caller-supplied haex_hlc_no_sync must be rejected; got: {err:?}"
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
    // the new ON CONFLICT handling. A single haex_hlc_no_sync column
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
