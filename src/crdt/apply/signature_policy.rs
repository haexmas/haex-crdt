//! Adapter that lets a [`SignatureProvider`]-based caller keep working
//! unchanged against the widened [`super::ApplyPolicy`] engine — including
//! the `Database` facade.
//!
//! `preflight` reproduces exactly today's `SignatureProvider`-based
//! preflight sequence: [`SignatureProvider::on_before_apply`] then
//! [`crate::crdt::apply::verify_all_signatures`]. `prepare_row` accepts
//! every eligible column unconditionally — per-column signature
//! verification already ran for the whole batch in `preflight`, exactly as
//! today's write loop never re-checks a signature once preflight has
//! passed — and proposes [`SignatureWrite::Replace`] with the change's raw
//! `sig`, reproducing today's flat `[column]` replace-or-remove map via the
//! core's generic write.

use rusqlite::Transaction;

use crate::crdt::apply::policy::ApplyPolicy;
use crate::crdt::apply::policy_types::{ColumnDecision, RowDecision, RowInput, SignatureWrite};
use crate::crdt::apply::preflight::verify_all_signatures;
use crate::db::core::ValueConverter;
use crate::error::Result;
use crate::signature::{RemoteChanges, SignatureProvider};

/// Wraps a [`SignatureProvider`] to drive the engine exactly as
/// `Database::apply_remote_changes` does today.
pub struct SignatureApplyPolicy<'a> {
    provider: &'a dyn SignatureProvider,
}

impl<'a> SignatureApplyPolicy<'a> {
    pub fn new(provider: &'a dyn SignatureProvider) -> Self {
        Self { provider }
    }
}

impl<'a> ApplyPolicy for SignatureApplyPolicy<'a> {
    fn preflight(&mut self, changes: &RemoteChanges) -> Result<()> {
        self.provider.on_before_apply(changes)?;
        verify_all_signatures(changes, self.provider)?;
        Ok(())
    }

    fn prepare_row(&mut self, _tx: &Transaction<'_>, row: RowInput<'_>) -> Result<RowDecision> {
        let mut decisions = Vec::with_capacity(row.eligible_indices.len());
        for &idx in row.eligible_indices {
            let change = row.changes[idx].change;
            let value = ValueConverter::json_to_rusqlite_value(&change.value)?;
            decisions.push(ColumnDecision::Accept {
                value,
                signature: SignatureWrite::Replace(change.sig.clone()),
            });
        }
        Ok(RowDecision::Columns(decisions))
    }
}
