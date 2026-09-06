//! Per-connection state for transaction-scoped CRDT operations.
//!
//! Holds the HLC timestamp that is shared across all writes inside a single
//! SQLite transaction, plus a `write_pending` guard. The slot is reset by the
//! connection's commit_hook / rollback_hook so every new transaction (explicit
//! or auto-commit) starts fresh.
//!
//! The `write_pending` flag prevents a stray read-only `SELECT current_hlc()`
//! from poisoning the HLC of a later write transaction: the cache is only
//! reused when the update_hook has observed at least one row-level
//! INSERT/UPDATE/DELETE in the current transaction.

use crate::crdt::hlc::{HlcError, HlcService};
use std::sync::{Arc, Mutex};
use uhlc::Timestamp;

#[derive(Clone)]
pub struct ConnectionContext {
    tx_hlc_slot: Arc<Mutex<Option<Timestamp>>>,
    write_pending: Arc<Mutex<bool>>,
}

impl ConnectionContext {
    pub fn new() -> Self {
        ConnectionContext {
            tx_hlc_slot: Arc::new(Mutex::new(None)),
            write_pending: Arc::new(Mutex::new(false)),
        }
    }

    /// Returns the HLC for the current transaction. Before any write, each
    /// call draws a fresh timestamp — read-only probes therefore never pin a
    /// value that a later write transaction could inherit. Once
    /// [`Self::mark_write_pending`] fires from the update_hook, subsequent
    /// calls within the same transaction return the first cached value until
    /// commit or rollback.
    pub fn current_or_new_tx_hlc(&self, hlc_service: &HlcService) -> Result<Timestamp, HlcError> {
        let writes = *self
            .write_pending
            .lock()
            .map_err(|_| HlcError::MutexPoisoned)?;
        let mut slot = self
            .tx_hlc_slot
            .lock()
            .map_err(|_| HlcError::MutexPoisoned)?;
        if writes {
            if let Some(existing) = slot.as_ref() {
                return Ok(*existing);
            }
        }
        let ts = hlc_service.new_timestamp()?;
        *slot = Some(ts);
        Ok(ts)
    }

    /// Signals that a row-level write happened in the current transaction.
    /// Called from the connection's `update_hook` on every INSERT/UPDATE/DELETE
    /// so the next `current_or_new_tx_hlc` call can safely treat the cached
    /// slot as transaction-scoped instead of a stale read-only probe.
    pub fn mark_write_pending(&self) {
        if let Ok(mut w) = self.write_pending.lock() {
            *w = true;
        }
    }

    /// Clears the slot. Called from commit_hook and rollback_hook — must
    /// never panic, so a poisoned mutex is silently ignored (the slot is
    /// unusable anyway and any further `current_or_new_tx_hlc` call surfaces
    /// the error).
    pub fn reset_tx_slot(&self) {
        if let Ok(mut slot) = self.tx_hlc_slot.lock() {
            *slot = None;
        }
        if let Ok(mut w) = self.write_pending.lock() {
            *w = false;
        }
    }
}

impl Default for ConnectionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_share_timestamp_once_write_pending_is_set() {
        let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
        let ctx = ConnectionContext::new();

        let first = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
        ctx.mark_write_pending();
        let second = ctx.current_or_new_tx_hlc(&hlc).expect("second hlc");

        assert_eq!(
            first, second,
            "once write_pending is set, repeated calls must return the cached timestamp"
        );
    }

    #[test]
    fn readonly_calls_do_not_pin_a_stale_slot() {
        // Without a write_pending signal, every call must draw a fresh
        // timestamp — otherwise a bare `SELECT current_hlc()` could dictate
        // the HLC of the next write transaction.
        let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
        let ctx = ConnectionContext::new();

        let a = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
        let b = ctx.current_or_new_tx_hlc(&hlc).expect("second hlc");

        assert_ne!(
            a, b,
            "read-only invocations must not reuse a cached slot that no write has claimed"
        );
    }

    #[test]
    fn reset_produces_fresh_timestamp() {
        let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
        let ctx = ConnectionContext::new();

        let first = ctx.current_or_new_tx_hlc(&hlc).expect("first hlc");
        ctx.mark_write_pending();
        let pinned = ctx.current_or_new_tx_hlc(&hlc).expect("pinned hlc");
        assert_eq!(first, pinned, "write_pending must pin the first value");

        ctx.reset_tx_slot();
        let fresh = ctx.current_or_new_tx_hlc(&hlc).expect("post-reset hlc");

        assert_ne!(first, fresh, "after reset a new timestamp must be produced");
    }

    #[test]
    fn context_is_clone_and_shares_state() {
        // The context is passed by clone into commit/rollback/update hooks;
        // both handles must see the same underlying slot.
        let hlc = HlcService::new_with_uuid(uuid::Uuid::new_v4());
        let ctx_a = ConnectionContext::new();
        let ctx_b = ctx_a.clone();

        ctx_a.mark_write_pending();
        let first = ctx_a.current_or_new_tx_hlc(&hlc).expect("first hlc");
        let second = ctx_b
            .current_or_new_tx_hlc(&hlc)
            .expect("second hlc via clone");
        assert_eq!(first, second, "cloned context must share the pinned slot");

        ctx_b.reset_tx_slot();
        let after_reset = ctx_a
            .current_or_new_tx_hlc(&hlc)
            .expect("post-reset hlc via original");
        assert_ne!(
            first, after_reset,
            "reset on either clone must clear the shared slot"
        );
    }
}
