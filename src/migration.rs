use std::collections::BTreeMap;

use crate::error::{Error, MigrationJournal, Result};

/// Stable identifier for a migration. Encodes the applied ordinal in a
/// lexicographic way so `list_migrations` can return migrations in a total
/// order simply by sorting.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MigrationName(pub String);

impl MigrationName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for MigrationName {
    fn from(s: &str) -> Self {
        MigrationName(s.to_string())
    }
}

impl From<String> for MigrationName {
    fn from(s: String) -> Self {
        MigrationName(s)
    }
}

/// Supplies consumer-owned migration SQL. See plan §4.3 for the two-journal
/// separation between crate-owned CRDT bookkeeping migrations and
/// consumer-owned schema migrations.
///
/// # Contract (plan §4.3)
///
/// - `list_migrations` MUST return unique names in a stable, total order
///   (lexicographic on `MigrationName`). Two calls at the same version of
///   a shipped consumer MUST return identical sequences.
/// - `load_migration` MUST return byte-identical content for the same name
///   across releases of the consumer. Applied migration content is frozen;
///   new work goes into a new migration name.
/// - The engine stores a SHA-256 digest of each applied migration's SQL in
///   the journal. On subsequent starts a mismatch aborts with
///   `Error::MigrationContentDrift`.
/// - On every open, each journal is reconciled independently against its
///   own source. Missing entries abort with
///   `Error::MigrationMissingFromSource { journal: ConsumerOwned, .. }`
///   for this trait's journal.
pub trait MigrationSource: Send + Sync {
    fn load_migration(&self, name: &MigrationName) -> Result<String>;
    fn list_migrations(&self) -> Result<Vec<MigrationName>>;
}

/// In-memory migration source for tests and simple consumers.
pub struct StaticMigrationSource(pub BTreeMap<MigrationName, String>);

impl MigrationSource for StaticMigrationSource {
    fn load_migration(&self, name: &MigrationName) -> Result<String> {
        self.0.get(name).cloned().ok_or_else(|| {
            Error::MigrationMissingFromSource {
                journal: MigrationJournal::ConsumerOwned,
                name: name.0.clone(),
            }
        })
    }

    fn list_migrations(&self) -> Result<Vec<MigrationName>> {
        Ok(self.0.keys().cloned().collect())
    }
}
