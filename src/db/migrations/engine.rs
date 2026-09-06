//! Two-journal migration engine (see plan §4.3).
//!
//! Every open reconciles two journals against two independent sources:
//! - [`TABLE_CRDT_MIGRATIONS`] against [`CRATE_MIGRATIONS`]
//! - [`TABLE_APP_MIGRATIONS`] against the consumer's [`MigrationSource`]
//!
//! Reconciliation runs each source independently and never crosses over:
//! a crate-owned entry can never be reported as missing from the consumer
//! source (or vice versa) — the [`MigrationJournal`] discriminant on
//! [`Error::MigrationMissingFromSource`] makes that unambiguous.
//!
//! # Guarantees
//! - Each migration applies in its own `IMMEDIATE` transaction so a partial
//!   failure never leaves the DB half-migrated.
//! - Content drift on any journaled entry aborts open with
//!   [`Error::MigrationContentDrift`]; the engine never re-runs a migration
//!   whose content differs from what was applied.
//! - Consumer-owned migrations pass through [`CrdtTransformer`] so
//!   `CREATE TABLE` statements gain the three CRDT metadata columns.
//!   Crate-owned migrations skip the transformer — their SQL is authored
//!   here and carries the columns it needs verbatim.

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::crdt::transformer::CrdtTransformer;
use crate::db::core::DRIZZLE_STATEMENT_BREAKPOINT;
use crate::error::{Error, MigrationJournal, Result};
use crate::migration::{MigrationName, MigrationSource};
use crate::table_names::{TABLE_APP_MIGRATIONS, TABLE_CRDT_MIGRATIONS};

use super::bootstrap::CRATE_MIGRATIONS;

/// Outcome of a [`run_migrations`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Crate-owned CRDT bookkeeping migrations applied on this call.
    pub crate_applied: usize,
    /// Consumer-owned schema migrations applied on this call.
    pub consumer_applied: usize,
}

/// Reconciles both journals against their sources and applies any pending
/// migrations in order (crate-owned first, then consumer-owned).
///
/// Behavior:
/// - Creates both journal tables via `CREATE TABLE IF NOT EXISTS` if absent.
/// - Aborts with [`Error::MigrationMissingFromSource`] if a name in either
///   journal is not returned by its own source. The `journal` field
///   distinguishes the two cases.
/// - Aborts with [`Error::MigrationContentDrift`] if any journaled entry's
///   current source content produces a different SHA-256 digest than the one
///   stored when it was applied.
/// - Otherwise applies pending migrations one at a time, each in its own
///   transaction. Returns a [`MigrationReport`] with the counts.
pub fn run_migrations(
    conn: &mut Connection,
    consumer_source: &dyn MigrationSource,
) -> Result<MigrationReport> {
    ensure_journal_tables(conn)?;

    let crate_applied = reconcile_and_apply(
        conn,
        MigrationJournal::CrateOwned,
        TABLE_CRDT_MIGRATIONS,
        &crate_migration_names(),
        &|name| load_crate_migration(name),
        /* transform_ddl = */ false,
    )?;

    let consumer_applied = reconcile_and_apply(
        conn,
        MigrationJournal::ConsumerOwned,
        TABLE_APP_MIGRATIONS,
        &consumer_source.list_migrations()?,
        &|name| consumer_source.load_migration(name),
        /* transform_ddl = */ true,
    )?;

    Ok(MigrationReport {
        crate_applied,
        consumer_applied,
    })
}

fn crate_migration_names() -> Vec<MigrationName> {
    CRATE_MIGRATIONS
        .iter()
        .map(|(name, _)| MigrationName::from(*name))
        .collect()
}

fn load_crate_migration(name: &MigrationName) -> Result<String> {
    CRATE_MIGRATIONS
        .iter()
        .find(|(n, _)| *n == name.as_str())
        .map(|(_, content)| (*content).to_string())
        .ok_or_else(|| Error::MigrationMissingFromSource {
            journal: MigrationJournal::CrateOwned,
            name: name.as_str().to_string(),
        })
}

fn ensure_journal_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {TABLE_CRDT_MIGRATIONS} (
             migration_name TEXT PRIMARY KEY NOT NULL,
             sha256_digest TEXT NOT NULL,
             applied_at TEXT NOT NULL DEFAULT (datetime('now'))
         );
         CREATE TABLE IF NOT EXISTS {TABLE_APP_MIGRATIONS} (
             migration_name TEXT PRIMARY KEY NOT NULL,
             sha256_digest TEXT NOT NULL,
             applied_at TEXT NOT NULL DEFAULT (datetime('now'))
         );"
    ))?;
    Ok(())
}

type Loader<'a> = dyn Fn(&MigrationName) -> Result<String> + 'a;

fn reconcile_and_apply(
    conn: &mut Connection,
    journal: MigrationJournal,
    journal_table: &str,
    source_list: &[MigrationName],
    load: &Loader<'_>,
    transform_ddl: bool,
) -> Result<usize> {
    let journaled = read_journal(conn, journal_table)?;

    for (name, stored_digest) in &journaled {
        let source_name = MigrationName::from(name.as_str());
        if !source_list.contains(&source_name) {
            return Err(Error::MigrationMissingFromSource {
                journal,
                name: name.clone(),
            });
        }
        let content = load(&source_name)?;
        let current = sha256_hex(content.as_bytes());
        if &current != stored_digest {
            return Err(Error::MigrationContentDrift {
                name: name.clone(),
                expected: stored_digest.clone(),
                found: current,
            });
        }
    }

    let mut applied = 0usize;
    for name in source_list {
        let content = load(name)?;
        if apply_single_migration(conn, journal_table, name, &content, transform_ddl)? {
            applied += 1;
        }
    }

    Ok(applied)
}

fn read_journal(conn: &Connection, journal_table: &str) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT migration_name, sha256_digest FROM {journal_table} ORDER BY migration_name ASC"
    ))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn apply_single_migration(
    conn: &mut Connection,
    journal_table: &str,
    name: &MigrationName,
    content: &str,
    transform_ddl: bool,
) -> Result<bool> {
    let digest = sha256_hex(content.as_bytes());
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let existing_digest = tx
        .query_row(
            &format!("SELECT sha256_digest FROM {journal_table} WHERE migration_name = ?1"),
            params![name.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    match existing_digest {
        Some(stored_digest) if stored_digest == digest => {
            tx.commit()?;
            return Ok(false);
        }
        Some(stored_digest) => {
            return Err(Error::MigrationContentDrift {
                name: name.as_str().to_string(),
                expected: stored_digest,
                found: digest,
            });
        }
        None => {}
    }

    execute_statements(&tx, content, transform_ddl)?;
    tx.execute(
        &format!("INSERT INTO {journal_table} (migration_name, sha256_digest) VALUES (?1, ?2)"),
        params![name.as_str(), digest],
    )?;
    tx.commit()?;
    Ok(true)
}

fn execute_statements(tx: &Transaction<'_>, content: &str, transform_ddl: bool) -> Result<()> {
    let transformer = if transform_ddl {
        Some(CrdtTransformer::new())
    } else {
        None
    };
    for statement in content
        .split(DRIZZLE_STATEMENT_BREAKPOINT)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let sql = match &transformer {
            Some(t) => t
                .transform_ddl_statement(statement)
                .unwrap_or_else(|_| statement.to_string()),
            None => statement.to_string(),
        };
        tx.execute(&sql, [])?;
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let out = hasher.finalize();
    let mut s = String::with_capacity(out.len() * 2);
    for byte in out {
        use std::fmt::Write as _;
        let _ = write!(s, "{byte:02x}");
    }
    s
}
