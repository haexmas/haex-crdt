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
