//! Apply pipeline — the receiver side of the CRDT sync loop (plan §4.2).
//!
//! Every remote change arrives as a [`crate::ColumnChange`] with the same
//! shape the scanner emits on the sender side. The apply pipeline takes a
//! batch, preflights it, and merges the surviving changes into the local
//! database under column-level LWW.
//!
//! Public entry point: [`apply_remote_changes`]. Its generic-merge decisions
//! (LWW, row identity, delete-log shadow/resurrection/propagation, the SQL
//! writers, the reserved-column and `_no_sync` guards, HLC drift, the local
//! clock advance) stay entirely the crate's own; a consumer plugs in
//! selective admission, exact value decoding, and atomic-with-the-merge work
//! through [`ApplyPolicy`] — see `policy.rs` for the full usage contract.
//! [`SignatureApplyPolicy`] adapts today's [`crate::SignatureProvider`]-based
//! callers (including the [`crate::Database`] facade) to the new trait with
//! unchanged behavior.
//!
//! Sub-modules are internal-only helpers except where re-exported below; the
//! public surface stays flat.

mod delete_propagation;
mod engine;
mod grouping;
mod policy;
mod policy_types;
mod preflight;
mod preimage;
mod report;
mod row;
mod signature_policy;
mod write;

pub use engine::apply_remote_changes;
pub use policy::ApplyPolicy;
pub use policy_types::{
    ColumnDecision, ConstraintDecision, IndexedChange, RowDecision, RowInput, RowWrite,
    SignatureWrite, WrittenColumn,
};
pub use preflight::{preflight_batch, verify_all_signatures};
pub use preimage::{column_sig_preimage, column_sig_preimage_from_parts};
pub use report::{ApplyOutcome, ApplyReport, SkipReason, SkippedChange};
pub use signature_policy::SignatureApplyPolicy;

pub use delete_propagation::{delete_shadows_insert, should_propagate_delete};

#[cfg(test)]
mod tests;
