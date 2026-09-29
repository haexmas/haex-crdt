//! Stamps rows written before the HLC existed (open lifecycle step 6).
//!
//! The consumer's `DatabaseBootstrap` hook, and any migration that seeds
//! data, run before [`HlcService::initialize_in_place`]. Their rows land
//! with a NULL row-level HLC and a NULL column-HLC map, since the insert
//! trigger only fires `WHEN` the row HLC is set. The scanner skips a column
//! with no usable HLC, so a peer could never reconstruct those rows.
//!
//! This pass gives each such row what the insert trigger would have given
//! it had the HLC existed: one fresh HLC for the whole pass as the row HLC,
//! a column-HLC map carrying that HLC for every tracked column, and a
//! dirty mark on the table. It only touches rows whose row HLC is NULL, so
//! a second open finds nothing and issues no HLC.

use rusqlite::{Connection, TransactionBehavior};

use crate::crdt::columns::{COLUMN_HLCS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::HlcService;
use crate::crdt::scanner::partition_columns;
use crate::crdt::trigger::{get_table_schema, is_safe_identifier};
use crate::db::error::DatabaseError;
use crate::db::init::discover_crdt_tables;
use crate::error::Result;

use super::install::{build_column_hlcs_json, mark_dirty};

/// Stamp every row of every CRDT table whose row HLC is NULL, in one
/// IMMEDIATE transaction. Returns the number of rows stamped. The HLC must
/// already be initialized.
pub(super) fn stamp_unstamped_rows(conn: &mut Connection, hlc: &HlcService) -> Result<usize> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(DatabaseError::from)?;

    // `(table, tracked columns)` for every table holding at least one
    // unstamped row. Collected first so a clean database issues no HLC.
    let mut pending: Vec<(String, Vec<String>)> = Vec::new();
    for table_name in discover_crdt_tables(&tx)? {
        // `discover_crdt_tables` identifies opt-in `_no_sync` tables by their
        // CRDT metadata columns. Tables that remain local never have those
        // columns, while a table installed explicitly through `install_crdt`
        // must still be stamped on a later open.
        if table_name == DELETED_ROWS_TABLE || !is_safe_identifier(&table_name) {
            continue;
        }
        let schema = get_table_schema(&tx, &table_name).map_err(DatabaseError::from)?;
        if !schema.iter().any(|c| c.name == COLUMN_HLCS_COLUMN) {
            continue;
        }
        let unstamped: bool = tx
            .query_row(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM \"{table_name}\" \
                     WHERE {HLC_TIMESTAMP_COLUMN} IS NULL)"
                ),
                [],
                |r| r.get(0),
            )
            .map_err(DatabaseError::from)?;
        if unstamped {
            let (_, data_columns) = partition_columns(&schema);
            let tracked = data_columns.iter().map(|c| c.name.clone()).collect();
            pending.push((table_name, tracked));
        }
    }

    if pending.is_empty() {
        return Ok(0);
    }

    let hlc_str = hlc
        .new_timestamp_and_persist(&tx)
        .map_err(DatabaseError::from)?
        .to_string();
    let mut stamped = 0;
    for (table_name, tracked) in pending {
        // Only metadata columns change, so the column-scoped update trigger
        // does not fire; the dirty mark is set here instead.
        stamped += tx
            .execute(
                &format!(
                    "UPDATE \"{table_name}\" SET {HLC_TIMESTAMP_COLUMN} = ?1, \
                     {COLUMN_HLCS_COLUMN} = ?2 WHERE {HLC_TIMESTAMP_COLUMN} IS NULL"
                ),
                [&hlc_str, &build_column_hlcs_json(&tracked, &hlc_str)],
            )
            .map_err(DatabaseError::from)?;
        mark_dirty(&tx, &table_name)?;
    }
    tx.commit().map_err(DatabaseError::from)?;
    Ok(stamped)
}
