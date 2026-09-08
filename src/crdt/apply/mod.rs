//! Apply pipeline — the receiver side of the CRDT sync loop (plan §4.2).
//!
//! Every remote change arrives as a [`crate::ColumnChange`] with the same
//! shape the scanner emits on the sender side. The apply pipeline takes a
//! batch, preflights it, and merges the surviving changes into the local
//! database under column-level LWW.
//!
//! Public entry point: [`apply_remote_changes`]. Its contract is spelled out
//! on [`crate::SignatureProvider`] — preflight refusals are no-write results,
//! and transaction write failures roll back as a unit. The HLC advancement
//! after commit may still return an error after the batch has landed. The
//! `preflight` module owns the no-write phase; `engine` owns the write loop
//! that follows it.
//!
//! Sub-modules are internal-only helpers; the public surface stays flat.

mod delete_propagation;
mod engine;
mod grouping;
mod preflight;
mod preimage;
mod report;
mod write;

pub use engine::apply_remote_changes;
pub use preimage::{column_sig_preimage, column_sig_preimage_from_parts};
pub use report::ApplyReport;

pub use delete_propagation::{delete_shadows_insert, should_propagate_delete};

#[cfg(test)]
mod tests;
