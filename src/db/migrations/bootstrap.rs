//! Crate-owned CRDT bookkeeping migrations, compiled into `haex-crdt`.
//!
//! These are journaled in [`crate::table_names::TABLE_CRDT_MIGRATIONS`] and
//! versioned with the crate; consumers do **not** supply them (see plan §4.3).
//! The engine reconciles this list against the crate's own journal only —
//! never against a consumer's [`crate::MigrationSource`] — so a valid
//! crate-owned migration is never wrongly reported as missing from a
//! consumer source.
//!
//! Applied content is frozen. Once a version of this crate has shipped a
//! migration, its SQL text is immutable — the engine's SHA-256 drift check
//! aborts open on any post-hoc edit. New CRDT-bookkeeping work goes into a
//! new migration name, never into an existing one.
//!
//! The freeze binds from the first **tagged release** containing the
//! migration, and that precondition is worth checking before invoking the
//! rule: editing a migration that exists only in an unreleased version is a
//! normal change, not a drift exception, because no journal anywhere holds
//! the old digest. Confirm with `git tag` and
//! `git show <tag>:src/db/migrations/sql/<name>.sql` rather than assuming a
//! migration present in the working tree has shipped — a working-tree
//! database from an unreleased version is expected to be deleted and
//! recreated, not migrated forward.

/// The compiled-in list of crate-owned migrations, in apply order.
///
/// Entries are `(migration_name, sql_content)`. `migration_name` is the
/// canonical identifier stored in the journal — keep it stable across
/// releases. `sql_content` may contain multiple statements separated by the
/// Drizzle-style `--> statement-breakpoint` marker (see
/// [`crate::DRIZZLE_STATEMENT_BREAKPOINT`]).
pub const CRATE_MIGRATIONS: &[(&str, &str)] = &[(
    "0001_crdt_bootstrap",
    include_str!("sql/0001_crdt_bootstrap.sql"),
)];
