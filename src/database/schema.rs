//! Schema changes inside a guarded write.
//!
//! A `CREATE TABLE` or `ALTER TABLE` run through
//! [`super::CrdtTransaction::execute`] in a guarded write keeps the CRDT
//! triggers in step with the schema, in the same transaction:
//!
//! - `CREATE TABLE` of a synced table gets the CRDT columns (from the
//!   transformer) and its triggers. `_no_sync` tables and, in local mode,
//!   every new table get neither.
//! - `ALTER TABLE` of a table with CRDT columns drops the table's triggers
//!   before the statement (SQLite refuses `DROP COLUMN` on a column a
//!   trigger names) and recreates them afterwards, so a new column is
//!   tracked. `RENAME TO` drops the triggers of the old name and creates
//!   them under the new one. Renaming across the `_no_sync` boundary is
//!   refused: the table would keep or lose its CRDT columns but change how
//!   it is synced.
//! - The statement and these steps share a savepoint: if any of them fails,
//!   the schema and the triggers are as they were before the statement.
//!
//! A table rebuild (Drizzle's `CREATE TABLE __new_X` / `INSERT INTO __new_X
//! (…) SELECT … FROM X` / `DROP TABLE X` / `ALTER TABLE __new_X RENAME TO X`)
//! runs in schema mode, with the copy through
//! [`super::CrdtTransaction::copy_rows_verbatim`]: it copies the CRDT columns
//! of every row unchanged with the triggers switched off, so no row is
//! stamped anew and concurrent changes from other devices keep winning or
//! losing exactly as before.

use std::collections::BTreeSet;

use rusqlite::{OptionalExtension, Transaction};
use sqlparser::ast::{
    AlterTableOperation, Expr, Ident, ObjectName, RenameTableNameKind, SelectItem, SetExpr,
    Statement, TableFactor, TableObject,
};

use crate::crdt::apply::toggle_triggers;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::crdt::trigger::{drop_triggers_for_table, setup_triggers_for_table};
use crate::db::error::DatabaseError;
use crate::db::init::CONFIG_KEY_TRIGGERS_ENABLED;
use crate::error::Result;
use crate::table_names::TABLE_CRDT_CONFIGS;

const SAVEPOINT: &str = "haex_crdt_schema_change";
const NO_SYNC_SUFFIX: &str = "_no_sync";

/// A statement that changes the schema of a synced table.
#[derive(Debug)]
pub(crate) enum SchemaChange {
    Created {
        table: String,
    },
    Altered {
        table: String,
        renamed_to: Option<String>,
    },
}

impl SchemaChange {
    /// Builds the change for `statement`, given the table the transformer
    /// reported as schema-modified.
    pub(crate) fn of(statement: &Statement, table: String) -> Option<Self> {
        match statement {
            Statement::CreateTable(create) if !create.temporary => {
                Some(SchemaChange::Created { table })
            }
            Statement::AlterTable(alter) => Some(SchemaChange::Altered {
                table,
                renamed_to: renamed_to(&alter.operations),
            }),
            _ => None,
        }
    }
}

/// Refuses `ALTER TABLE … RENAME TO` across the `_no_sync` boundary.
pub(crate) fn check_rename(statement: &Statement) -> Result<()> {
    let Statement::AlterTable(alter) = statement else {
        return Ok(());
    };
    let (Some(old), Some(new)) = (last_ident(&alter.name), renamed_to(&alter.operations)) else {
        return Ok(());
    };
    if is_no_sync(&old) != is_no_sync(&new) {
        return Err(DatabaseError::UnsupportedStatement {
            sql: statement.to_string(),
            reason: "renaming a table across the _no_sync boundary is not supported".to_string(),
        }
        .into());
    }
    Ok(())
}

/// Local mode: whether `statement` still goes through the CRDT transformer.
/// `CREATE TABLE` never does, and neither does a write to a table without
/// CRDT columns.
pub(crate) fn transforms_in_local_mode(tx: &Transaction, statement: &Statement) -> Result<bool> {
    let target = match statement {
        Statement::CreateTable(_) => return Ok(false),
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => last_ident(name),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            TableFactor::Table { name, .. } => last_ident(name),
            _ => None,
        },
        _ => return Ok(true),
    };
    match target {
        Some(table) => Ok(crdt_table(tx, &table)?.is_some()),
        None => Ok(true),
    }
}

/// Opens the savepoint of a schema change and, for an `ALTER TABLE` of a
/// table with CRDT columns, drops its triggers.
pub(crate) fn begin(tx: &Transaction, change: &SchemaChange) -> Result<()> {
    tx.execute_batch(&format!("SAVEPOINT {SAVEPOINT}"))
        .map_err(DatabaseError::from)?;
    let dropped = match change {
        SchemaChange::Altered { table, .. } => match crdt_table(tx, table) {
            Ok(Some(actual)) => drop_triggers_for_table(tx, &actual)
                .map_err(|error| DatabaseError::from(error).into()),
            Ok(None) => Ok(()),
            Err(error) => Err(error),
        },
        SchemaChange::Created { .. } => Ok(()),
    };
    dropped.inspect_err(|_| rollback(tx))
}

/// Recreates the triggers of the changed table (under its new name after a
/// rename) and releases the savepoint. On `Err` the caller calls
/// [`rollback`].
pub(crate) fn complete(tx: &Transaction, change: &SchemaChange) -> Result<()> {
    let table = match change {
        SchemaChange::Created { table } => table,
        SchemaChange::Altered { table, renamed_to } => renamed_to.as_ref().unwrap_or(table),
    };
    if let Some(actual) = crdt_table(tx, table)? {
        setup_triggers_for_table(tx, &actual, true).map_err(DatabaseError::from)?;
    }
    release(tx)
}

/// Rolls back to the savepoint and releases it. A transaction SQLite has
/// already rolled back has no savepoint left; the caller notices that
/// through `is_autocommit`.
pub(crate) fn rollback(tx: &Transaction) {
    let _ = tx.execute_batch(&format!("ROLLBACK TO {SAVEPOINT}; RELEASE {SAVEPOINT}"));
}

fn release(tx: &Transaction) -> Result<()> {
    tx.execute_batch(&format!("RELEASE {SAVEPOINT}"))
        .map_err(DatabaseError::from)?;
    Ok(())
}

/// Checks the shape of a rebuild copy — `INSERT INTO T (cols) SELECT exprs
/// FROM S` without joins, wildcards, `ON CONFLICT` or `RETURNING` — and,
/// when both tables carry CRDT columns, appends those columns to the column
/// list and the projection so they are copied unchanged.
pub(crate) fn rewrite_rebuild_copy(tx: &Transaction, statement: &mut Statement) -> Result<()> {
    let unsupported = |reason: &str| -> crate::Error {
        DatabaseError::UnsupportedStatement {
            sql: statement.to_string(),
            reason: format!("copy_rows_verbatim: {reason}"),
        }
        .into()
    };
    let Statement::Insert(insert) = &*statement else {
        return Err(unsupported("expected INSERT INTO … SELECT … FROM …"));
    };
    if insert.columns.is_empty() || insert.on.is_some() || insert.returning.is_some() {
        return Err(unsupported(
            "needs a column list and no ON CONFLICT or RETURNING",
        ));
    }
    let target = match &insert.table {
        TableObject::TableName(name) => main_table(name),
        _ => None,
    };
    let source = insert.source.as_ref().and_then(|query| match &*query.body {
        SetExpr::Select(select) if select.from.len() == 1 && select.from[0].joins.is_empty() => {
            match &select.from[0].relation {
                TableFactor::Table { name, .. } => main_table(name),
                _ => None,
            }
        }
        _ => None,
    });
    let (Some(target), Some(source)) = (target, source) else {
        return Err(unsupported(
            "expected INSERT INTO <table> (…) SELECT … FROM <table> on main tables",
        ));
    };
    let wildcard = insert
        .source
        .as_ref()
        .is_some_and(|query| match &*query.body {
            SetExpr::Select(select) => select.projection.iter().any(|item| {
                matches!(
                    item,
                    SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
                )
            }),
            _ => true,
        });
    if wildcard {
        return Err(unsupported("a wildcard projection is not supported"));
    }
    match (
        crdt_table(tx, &target)?.is_some(),
        crdt_table(tx, &source)?.is_some(),
    ) {
        (false, false) => return Ok(()),
        (true, true) => {}
        _ => {
            return Err(unsupported(
                "both tables must carry CRDT columns, or neither",
            ))
        }
    }

    let Statement::Insert(insert) = statement else {
        return Ok(());
    };
    let meta = [HLC_TIMESTAMP_COLUMN, COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN];
    insert.columns.extend(
        meta.iter()
            .map(|column| ObjectName::from(Ident::new(*column))),
    );
    if let Some(SetExpr::Select(select)) = insert.source.as_mut().map(|query| &mut *query.body) {
        select.projection.extend(
            meta.iter()
                .map(|column| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(*column)))),
        );
    }
    Ok(())
}

/// Opens the savepoint of a rebuild copy and switches the triggers off.
/// Returns the previous value of the trigger gate for [`end_verbatim_copy`].
pub(crate) fn begin_verbatim_copy(tx: &Transaction) -> Result<String> {
    tx.execute_batch(&format!("SAVEPOINT {SAVEPOINT}"))
        .map_err(DatabaseError::from)?;
    let previous = tx
        .query_row(
            &format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?1"),
            [CONFIG_KEY_TRIGGERS_ENABLED],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(DatabaseError::from)
        .map(|value| value.unwrap_or_else(|| "1".to_string()));
    let switched = previous.map_err(Into::into).and_then(|previous| {
        toggle_triggers(tx, "0")?;
        Ok(previous)
    });
    switched.inspect_err(|_| rollback(tx))
}

/// Restores the trigger gate and releases the savepoint of a rebuild copy.
pub(crate) fn end_verbatim_copy(tx: &Transaction, previous: &str) -> Result<()> {
    toggle_triggers(tx, previous)?;
    release(tx)
}

/// Runs `PRAGMA foreign_key_check`; any violation is
/// [`DatabaseError::ForeignKeyCheckFailed`] with the child tables.
pub(crate) fn foreign_key_check(tx: &Transaction) -> Result<()> {
    let mut statement = tx
        .prepare("PRAGMA foreign_key_check")
        .map_err(DatabaseError::from)?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(DatabaseError::from)?
        .collect::<rusqlite::Result<BTreeSet<String>>>()
        .map_err(DatabaseError::from)?;
    if tables.is_empty() {
        return Ok(());
    }
    Err(DatabaseError::ForeignKeyCheckFailed {
        tables: tables.into_iter().collect(),
    }
    .into())
}

/// The stored name of `table` (matched without regard to case) when it
/// exists and carries the row-level HLC column — the crate's definition of
/// a synced table (see [`crate::discover_crdt_tables`]).
///
/// Fails when a temporary table of the same name shadows it: unqualified
/// SQL, the trigger setup and `DROP TRIGGER` would then reach the temporary
/// table or drop the synced table's triggers.
fn crdt_table(tx: &Transaction, table: &str) -> Result<Option<String>> {
    let Some(actual) = main_crdt_table(tx, table)? else {
        return Ok(None);
    };
    let shadowed = tx
        .query_row(
            "SELECT 1 FROM sqlite_temp_master WHERE type = 'table' AND name = ?1 COLLATE NOCASE",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map_err(DatabaseError::from)?
        .is_some();
    if shadowed {
        return Err(DatabaseError::UnsupportedStatement {
            sql: actual,
            reason: "a temporary table shadows this synced table".to_string(),
        }
        .into());
    }
    Ok(Some(actual))
}

fn main_crdt_table(tx: &Transaction, table: &str) -> Result<Option<String>> {
    Ok(tx
        .query_row(
            &format!(
                "SELECT m.name FROM sqlite_master m JOIN pragma_table_info(m.name) p \
                 WHERE m.type = 'table' AND m.name = ?1 COLLATE NOCASE \
                   AND p.name = '{HLC_TIMESTAMP_COLUMN}'"
            ),
            [table],
            |row| row.get(0),
        )
        .optional()
        .map_err(DatabaseError::from)?)
}

fn renamed_to(operations: &[AlterTableOperation]) -> Option<String> {
    operations.iter().find_map(|operation| match operation {
        AlterTableOperation::RenameTable {
            table_name: RenameTableNameKind::To(name) | RenameTableNameKind::As(name),
        } => last_ident(name),
        _ => None,
    })
}

/// The table name when `name` is unqualified or qualified with `main`.
fn main_table(name: &ObjectName) -> Option<String> {
    match name.0.as_slice() {
        [_] => last_ident(name),
        [schema, _] if schema.as_ident()?.value.eq_ignore_ascii_case("main") => last_ident(name),
        _ => None,
    }
}

fn last_ident(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
}

fn is_no_sync(table: &str) -> bool {
    table.to_ascii_lowercase().ends_with(NO_SYNC_SUFFIX)
}
