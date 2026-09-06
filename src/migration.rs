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
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| Error::MigrationMissingFromSource {
                journal: MigrationJournal::ConsumerOwned,
                name: name.0.clone(),
            })
    }

    fn list_migrations(&self) -> Result<Vec<MigrationName>> {
        Ok(self.0.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn source(entries: &[(&str, &str)]) -> StaticMigrationSource {
        StaticMigrationSource(
            entries
                .iter()
                .map(|(n, s)| (MigrationName::from(*n), s.to_string()))
                .collect(),
        )
    }

    #[test]
    fn migration_name_from_str_wraps_string() {
        let name = MigrationName::from("0001_init");
        assert_eq!(name.as_str(), "0001_init");
    }

    #[test]
    fn migration_name_from_string_wraps_string() {
        let name = MigrationName::from("0001_init".to_string());
        assert_eq!(name.as_str(), "0001_init");
    }

    #[test]
    fn migration_name_ord_is_lexicographic() {
        // Sort order matters for `list_migrations`'s total-order contract.
        let mut names = vec![
            MigrationName::from("0002_b"),
            MigrationName::from("0001_a"),
            MigrationName::from("0010_z"),
        ];
        names.sort();
        assert_eq!(
            names,
            vec![
                MigrationName::from("0001_a"),
                MigrationName::from("0002_b"),
                MigrationName::from("0010_z"),
            ]
        );
    }

    #[test]
    fn static_source_load_returns_stored_content() {
        let src = source(&[("0001_init", "CREATE TABLE t (id INTEGER);")]);
        let content = src
            .load_migration(&MigrationName::from("0001_init"))
            .unwrap();
        assert_eq!(content, "CREATE TABLE t (id INTEGER);");
    }

    #[test]
    fn static_source_load_missing_reports_consumer_owned_journal() {
        // Plan §4.3 requires the journal field so a valid crate-owned
        // migration is never wrongly reported as missing from a consumer source.
        let src = source(&[]);
        let err = src
            .load_migration(&MigrationName::from("0001_missing"))
            .unwrap_err();
        match err {
            Error::MigrationMissingFromSource { journal, name } => {
                assert_eq!(journal, MigrationJournal::ConsumerOwned);
                assert_eq!(name, "0001_missing");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn static_source_list_returns_lexicographic_total_order() {
        // BTreeMap orders by key; list_migrations promises the same across
        // calls (plan §4.3).
        let src = source(&[("0002_b", "b"), ("0001_a", "a"), ("0010_z", "z")]);
        let listed = src.list_migrations().unwrap();
        assert_eq!(
            listed,
            vec![
                MigrationName::from("0001_a"),
                MigrationName::from("0002_b"),
                MigrationName::from("0010_z"),
            ]
        );
    }

    #[test]
    fn static_source_list_returns_identical_sequence_across_calls() {
        let src = source(&[("0001_a", "a"), ("0002_b", "b")]);
        assert_eq!(
            src.list_migrations().unwrap(),
            src.list_migrations().unwrap()
        );
    }

    #[test]
    fn static_source_list_empty_when_no_migrations() {
        let src = source(&[]);
        assert!(src.list_migrations().unwrap().is_empty());
    }

    #[test]
    fn migration_source_is_object_safe_via_dyn_dispatch() {
        let src: Arc<dyn MigrationSource> = Arc::new(source(&[("0001_a", "a")]));
        assert_eq!(src.list_migrations().unwrap().len(), 1);
    }
}
