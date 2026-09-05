//! Post-write hook the executor invokes inside the SQL transaction, before
//! commit. Consumers register one or more [`PostWriteSigner`] implementations
//! on [`crate::execute_with_crdt`]; each is called after the CRDT
//! transformer has written the row (and the after-insert / after-update
//! triggers have populated `haex_column_hlcs` and the dirty-tables entry),
//! but before `tx.commit()` runs. Any implementation returning `Err` rolls
//! the whole transaction back — the write plus the dirty-tables entry plus
//! the tombstone-log row all disappear atomically.
//!
//! # Why this exists
//!
//! The CRDT layer maintains three metadata columns per row —
//! `haex_hlc`, `haex_column_hlcs`, `haex_column_sigs`. The first two are
//! written by the transformer + triggers here in the crate; the third holds
//! per-column signatures whose computation depends on the consumer's
//! identity system (UCAN / DID / MLS in `haex-vault`, plain device pubkeys
//! in `holzi` if it ever grows one, nothing at all under
//! [`crate::signature::NoopSignatureProvider`]).
//!
//! The crate cannot compute those signatures — but they MUST be written
//! inside the same transaction as the row, otherwise a crash between "row
//! committed" and "signature computed" leaves a signed table with an
//! unsigned row. The `PostWriteSigner` hook is that atomic seam: the
//! executor hands the trait a live `&Transaction` plus the [`WriteContext`]
//! describing what just changed, and the trait's implementation writes any
//! derived rows / columns it needs into that same transaction.

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
    /// every column of the table positionally, so a signer must fall back
    /// to the table's schema instead of treating "no names" as "nothing
    /// written" — otherwise the row lands unsigned.
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

/// Everything a [`PostWriteSigner`] needs to decide what to sign for a
/// single `execute_with_crdt` invocation.
///
/// `touched` is `None` for statements the crate does not treat as a
/// column write (SELECT, DELETE, DDL). Signers that only care about
/// INSERT/UPDATE should early-return in that case.
#[derive(Debug)]
pub struct WriteContext<'a> {
    /// The parsed statement after `crate::crdt::transformer` has rewritten
    /// it (so `haex_hlc` etc. are already present in the AST).
    pub statement: &'a Statement,

    /// `(target_table, columns)` for INSERT/UPDATE; `None` for statements
    /// the signer does not need to inspect.
    pub touched: Option<(TouchedTable, TouchedColumns)>,

    /// Transaction-scoped HLC used to stamp this write. Same value the
    /// transformer wrote into `haex_hlc` on every touched row and the same
    /// value `current_hlc()` returns for the rest of the transaction.
    pub hlc: &'a Timestamp,
}

/// Post-write hook. Called with the live `&Transaction` after the main
/// write has landed but before commit. See the module docs for the
/// atomicity contract.
///
/// Multiple signers registered with `execute_with_crdt` run in registration
/// order. The first `Err` aborts the transaction — later signers are
/// skipped.
pub trait PostWriteSigner: Send + Sync {
    /// Applies post-write policy or derived writes in the active transaction.
    /// Returning an error aborts the transaction and skips later signers.
    fn on_after_write(&self, tx: &Transaction, ctx: &WriteContext<'_>)
        -> Result<(), DatabaseError>;
}

/// Executor-side default: does nothing, accepts any batch. Consumers who
/// don't need per-column signing (e.g. `NoopSignatureProvider` users)
/// simply pass no signers at all — this type is a convenience for tests
/// that want to assert "yes, the hook did fire but did nothing".
pub struct NoopPostWriteSigner;

impl PostWriteSigner for NoopPostWriteSigner {
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
    fn post_write_signer_is_object_safe_via_dyn_dispatch() {
        // Executor stores signers as Arc<dyn PostWriteSigner>; make sure the
        // trait actually is object-safe.
        let _: Arc<dyn PostWriteSigner> = Arc::new(NoopPostWriteSigner);
    }
}
