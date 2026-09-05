//! Post-write hook the executor invokes inside the SQL transaction, before
//! commit. Consumers register one or more [`PostWriteHook`] implementations
//! on [`crate::execute_with_crdt`]; each is called after the CRDT
//! transformer has written the row (and the after-insert / after-update
//! triggers have populated `haex_column_hlcs` and the dirty-tables entry),
//! but before `tx.commit()` runs. Any implementation returning `Err` rolls
//! the whole transaction back — the write plus the dirty-tables entry plus
//! every earlier hook's derived rows disappear atomically.
//!
//! # What consumers use this for
//!
//! The hook is a generic seam, not a signing-specific API. Whatever a
//! consumer wants to run atomically with the write goes here. Examples:
//!
//! - **Per-column signing** (haex-vault's F1/F2/B.3 passes over
//!   `haex_column_sigs`, using UCAN / DID identities the crate does not
//!   know about).
//! - **Audit logging** — append an audit row to a consumer-owned journal
//!   in the same transaction, so audit and data commit or roll back
//!   together.
//! - **Denormalized-view maintenance** — update a materialized summary
//!   row that has to stay consistent with the base table.
//! - **Cross-table integrity checks** — inspect the affected rows and
//!   reject with `Err(…)` if a policy is violated, forcing the write to
//!   roll back.
//! - **Custom metadata columns** — populate consumer-defined "last
//!   modified by" / "revision" columns using the write's HLC.
//!
//! What ties these together: they all need to observe the write and
//! optionally add related writes / abort, and they all need atomicity
//! with the write itself. The crate cannot know what any given consumer
//! needs there — the hook is where the crate hands off.
//!
//! # Why not a pre-write hook (yet)
//!
//! A `PostWriteHook` already has veto power via `Err`, and the
//! transaction rolls back cleanly. The only pre-write use case a
//! post-write hook can't cover is rejecting an expensive write before
//! it runs. No current consumer needs that; a `PreWriteHook` will land
//! when a real need appears.

use crate::db::error::DatabaseError;
use rusqlite::Transaction;
use sqlparser::ast::Statement;
use uhlc::Timestamp;

/// Case-folded table name of an INSERT or UPDATE target. Lowercased once at
/// extraction time so downstream consumers can compare against their own
/// canonical constants with `==` instead of `eq_ignore_ascii_case`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TouchedTable(String);

impl TouchedTable {
    /// Returns the canonical lowercase table name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Constructs from a raw table name. Not part of the public API — the
    /// executor calls it; consumers receive already-constructed values via
    /// [`WriteContext`].
    pub(crate) fn from_raw(name: &str) -> Self {
        TouchedTable(name.to_ascii_lowercase())
    }
}

/// Which columns of the target table a statement writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TouchedColumns {
    /// The statement names its columns: `INSERT INTO t (a, b) …` /
    /// `UPDATE t SET a = …`. Exactly these, nothing else. Case-folded to
    /// lowercase.
    Explicit(Vec<String>),
    /// `INSERT INTO t VALUES (…)` with no column list. The write covers
    /// every column of the table positionally, so hooks that want to know
    /// what changed must fall back to the table's schema instead of
    /// treating "no names" as "nothing written".
    AllColumns,
}

impl TouchedColumns {
    /// Column names the caller spelled out. Empty for
    /// [`Self::AllColumns`] — there are no names to inspect; that case is
    /// caught later against the real schema by consumers who care.
    pub fn explicit(&self) -> &[String] {
        match self {
            Self::Explicit(cols) => cols,
            Self::AllColumns => &[],
        }
    }

    /// Whether the statement writes every column positionally.
    pub fn is_all_columns(&self) -> bool {
        matches!(self, Self::AllColumns)
    }
}

/// Everything a [`PostWriteHook`] needs to decide what to do for a
/// single `execute_with_crdt` invocation.
///
/// `touched` is `None` for statements the crate does not treat as a
/// column write (SELECT, DELETE, DDL). Hooks that only care about
/// INSERT/UPDATE should early-return in that case.
#[derive(Debug)]
pub struct WriteContext<'a> {
    /// The parsed statement after `crate::crdt::transformer` has rewritten
    /// it (so `haex_hlc` etc. are already present in the AST).
    pub statement: &'a Statement,

    /// `(target_table, columns)` for INSERT/UPDATE; `None` for statements
    /// the hook does not need to inspect.
    pub touched: Option<(TouchedTable, TouchedColumns)>,

    /// Transaction-scoped HLC used to stamp this write. Same value the
    /// transformer wrote into `haex_hlc` on every touched row and the same
    /// value `current_hlc()` returns for the rest of the transaction.
    pub hlc: &'a Timestamp,
}

/// Post-write hook. Called with the live `&Transaction` after the main
/// write has landed but before commit. See the module docs for the
/// atomicity contract and the range of use cases.
///
/// Multiple hooks registered with `execute_with_crdt` run in registration
/// order. The first `Err` aborts the transaction — later hooks are
/// skipped.
pub trait PostWriteHook: Send + Sync {
    /// Applies post-write logic in the active transaction. Returning an
    /// error aborts the transaction and skips later hooks.
    fn on_after_write(&self, tx: &Transaction, ctx: &WriteContext<'_>)
        -> Result<(), DatabaseError>;
}

/// Executor-side default: does nothing, accepts any batch. Consumers who
/// don't need any post-write behavior simply pass no hooks at all — this
/// type is a convenience for tests that want to assert "yes, the hook did
/// fire but did nothing".
pub struct NoopPostWriteHook;

impl PostWriteHook for NoopPostWriteHook {
    fn on_after_write(
        &self,
        _tx: &Transaction,
        _ctx: &WriteContext<'_>,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn touched_table_lowercases_on_construction() {
        let t = TouchedTable::from_raw("MyTable");
        assert_eq!(t.as_str(), "mytable");
    }

    #[test]
    fn touched_columns_explicit_returns_names() {
        let c = TouchedColumns::Explicit(vec!["a".into(), "b".into()]);
        assert_eq!(c.explicit(), &["a".to_string(), "b".to_string()]);
        assert!(!c.is_all_columns());
    }

    #[test]
    fn touched_columns_all_columns_has_no_explicit_names() {
        let c = TouchedColumns::AllColumns;
        assert!(c.explicit().is_empty());
        assert!(c.is_all_columns());
    }

    #[test]
    fn post_write_hook_is_object_safe_via_dyn_dispatch() {
        // Executor stores hooks as Arc<dyn PostWriteHook>; make sure the
        // trait actually is object-safe.
        let _: Arc<dyn PostWriteHook> = Arc::new(NoopPostWriteHook);
    }
}
