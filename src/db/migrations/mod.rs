//! Migration engine with two independent journals (see plan §4.3).
//!
//! - Crate-owned CRDT bookkeeping migrations, compiled into `haex-crdt` and
//!   listed in [`bootstrap::CRATE_MIGRATIONS`], journaled in
//!   [`crate::table_names::TABLE_CRDT_MIGRATIONS`].
//! - Consumer-owned schema migrations, supplied by the consumer via
//!   [`crate::MigrationSource`], journaled in
//!   [`crate::table_names::TABLE_APP_MIGRATIONS`].
//!
//! [`run_migrations`] is the sole entry point. See its documentation for the
//! reconcile-then-apply sequence.

pub mod bootstrap;
mod engine;

pub use bootstrap::CRATE_MIGRATIONS;
pub use engine::{run_migrations, MigrationReport};

#[cfg(test)]
mod tests;
