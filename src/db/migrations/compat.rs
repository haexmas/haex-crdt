//! Compatibility handling for databases created before the v0.2 schema names.
//!
//! This runs before the current journal tables or the immutable bootstrap
//! migration are created. The conversion is deliberately rename-based so
//! existing HLC, per-column HLC, signature, journal, and dirty-table values
//! remain intact.

use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};
use crate::error::{Error, Result};
use crate::table_names::{
    TABLE_APP_MIGRATIONS, TABLE_CRDT_CONFIGS, TABLE_CRDT_DIRTY_TABLES, TABLE_CRDT_MIGRATIONS,
};

use super::bootstrap::CRATE_MIGRATIONS;

/// Pre-v0.2 metadata column names, positionally paired with the current
/// ones in [`migrate_legacy_metadata_columns`]. These are the only names a
/// tagged release ever wrote into a database (v0.1.0); the intermediate
/// names that the unreleased 0.2.0 line carried for a while need no entry
/// here, because no shipped version produced them.
const LEGACY_COLUMNS: &[&str] = &["haex_hlc", "haex_column_hlcs", "haex_column_sigs"];

const LEGACY_TABLES: &[(&str, &str)] = &[
    ("haex_crdt_configs", TABLE_CRDT_CONFIGS),
    ("haex_crdt_dirty_tables", TABLE_CRDT_DIRTY_TABLES),
    ("haex_crdt_migrations", TABLE_CRDT_MIGRATIONS),
    ("haex_app_migrations", TABLE_APP_MIGRATIONS),
];

/// Migrate every known legacy identifier before current-schema bootstrap.
pub(crate) fn prepare_legacy_schema(conn: &mut Connection) -> Result<bool> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    migrate_legacy_table_names(&tx)?;
    migrate_legacy_metadata_columns(&tx)?;

    let bootstrap_tables = [
        TABLE_CRDT_CONFIGS,
        TABLE_CRDT_DIRTY_TABLES,
        "haex_deleted_rows",
    ];
    let mut present = 0;
    for table in bootstrap_tables {
        if table_exists(&tx, table)? {
            present += 1;
        }
    }
    if present != 0 && present != bootstrap_tables.len() {
        return Err(compatibility_error(
            "only part of the CRDT bootstrap schema exists; refusing to replay or discard it"
                .to_string(),
        ));
    }
    tx.commit()?;
    Ok(present == bootstrap_tables.len())
}

/// Journal the immutable bootstrap migration when its schema came from the
/// legacy tables. Without this marker, the unchanged `CREATE TABLE` statements
/// would be replayed against the renamed tables and fail with "already exists".
pub(crate) fn record_legacy_bootstrap(conn: &Connection) -> Result<()> {
    let (name, content) = CRATE_MIGRATIONS
        .first()
        .expect("CRATE_MIGRATIONS must contain the bootstrap migration");
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO {TABLE_CRDT_MIGRATIONS} \
             (migration_name, sha256_digest) VALUES (?1, ?2)"
        ),
        rusqlite::params![name, sha256_hex(content.as_bytes())],
    )?;
    Ok(())
}

/// Rename legacy metadata columns in all existing tables. This is also called
/// by `install_crdt` so its direct setup path cannot silently discard metadata
/// from a table that predates the current naming convention.
pub(crate) fn migrate_legacy_metadata_columns(conn: &Connection) -> Result<()> {
    let current_columns = [HLC_TIMESTAMP_COLUMN, COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN];
    let tables = list_user_tables(conn)?;
    for table in tables {
        // Tracked across renames rather than read once. With a single legacy
        // generation no two pairs share a target column, so this cannot
        // currently change an outcome — but a second generation mapping onto
        // an already-renamed name would otherwise hit a raw SQLite
        // "duplicate column name" instead of the compatibility error below.
        let mut columns = table_columns(conn, &table)?;
        for (legacy, current) in LEGACY_COLUMNS.iter().zip(current_columns.iter()) {
            let has_legacy = columns.iter().any(|column| column == legacy);
            let has_current = columns.iter().any(|column| column == current);
            if has_legacy && has_current {
                return Err(compatibility_error(format!(
                    "table '{table}' contains both legacy column '{legacy}' and current column '{current}'"
                )));
            }
            if has_legacy {
                conn.execute(
                    &format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        quote_identifier(&table),
                        quote_identifier(legacy),
                        quote_identifier(current),
                    ),
                    [],
                )?;
                columns.retain(|column| column != legacy);
                columns.push((*current).to_string());
            }
        }
    }
    Ok(())
}

fn migrate_legacy_table_names(tx: &Transaction<'_>) -> Result<()> {
    for &(legacy, current) in LEGACY_TABLES {
        if !table_exists(tx, legacy)? {
            continue;
        }
        if !table_exists(tx, current)? {
            tx.execute(
                &format!(
                    "ALTER TABLE {} RENAME TO {}",
                    quote_identifier(legacy),
                    quote_identifier(current),
                ),
                [],
            )?;
        } else {
            merge_legacy_table(tx, legacy, current)?;
        }
    }
    Ok(())
}

fn merge_legacy_table(tx: &Transaction<'_>, legacy: &str, current: &str) -> Result<()> {
    let (key_column, value_columns): (&str, &[&str]) = match current {
        TABLE_CRDT_CONFIGS => ("key", &["type", "value"]),
        TABLE_CRDT_DIRTY_TABLES => ("table_name", &["last_modified"]),
        TABLE_CRDT_MIGRATIONS | TABLE_APP_MIGRATIONS => {
            ("migration_name", &["sha256_digest", "applied_at"])
        }
        _ => {
            return Err(compatibility_error(format!(
                "unknown legacy CRDT table '{legacy}'"
            )))
        }
    };

    let differences = value_columns
        .iter()
        .map(|column| format!("legacy.{column} IS NOT current.{column}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let conflict_sql = format!(
        "SELECT 1 FROM {legacy} AS legacy JOIN {current} AS current \
         ON legacy.{key_column} = current.{key_column} WHERE {differences} LIMIT 1",
        legacy = quote_identifier(legacy),
        current = quote_identifier(current),
        key_column = quote_identifier(key_column),
    );
    if tx.query_row(&conflict_sql, [], |_| Ok(())).is_ok() {
        return Err(compatibility_error(format!(
            "legacy table '{legacy}' conflicts with current table '{current}'"
        )));
    }

    tx.execute(
        &format!(
            "INSERT OR IGNORE INTO {} SELECT * FROM {}",
            quote_identifier(current),
            quote_identifier(legacy),
        ),
        [],
    )?;
    tx.execute(&format!("DROP TABLE {}", quote_identifier(legacy)), [])?;
    Ok(())
}

fn list_user_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let tables = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(tables)
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", quote_identifier(table)))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(columns)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )?)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn compatibility_error(reason: String) -> Error {
    Error::MigrationCompatibility { reason }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
