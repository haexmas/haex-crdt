//! Pins the documented limitation that a primary-key-only synced table has
//! no column to ship, while a table with one non-key column does ship.

use super::super::*;
use super::{source, Fixture};

const LINKS: (&str, &str) = (
    "0001_links",
    "CREATE TABLE links (a TEXT NOT NULL, b TEXT NOT NULL, PRIMARY KEY (a, b));",
);
const LINKS_WITH_COLUMN: (&str, &str) = (
    "0001_links",
    "CREATE TABLE links (a TEXT NOT NULL, b TEXT NOT NULL, note TEXT, PRIMARY KEY (a, b));",
);

fn scanned(migration: (&'static str, &'static str)) -> usize {
    let fx = Fixture::with_source(source(&[migration]));
    let db = Database::open(fx.config.clone()).unwrap();
    db.write(|tx| tx.execute("INSERT INTO links (a, b) VALUES ('x', 'y')", &[]))
        .unwrap();
    db.scan_table_for_local_changes("links", None, ScanFilters::default())
        .unwrap()
        .len()
}

#[test]
fn a_primary_key_only_table_ships_nothing_and_a_non_key_column_fixes_it() {
    assert_eq!(scanned(LINKS), 0);
    assert_eq!(scanned(LINKS_WITH_COLUMN), 1);
}
