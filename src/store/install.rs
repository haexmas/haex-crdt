//! `install_crdt` backfill contract (plan §6).
//!
//! Adding CRDT metadata columns to a table that already carries rows leaves
//! those rows with `NULL` in `haex_hlc` and `'{}'` in
//! `haex_column_hlcs` — from the CRDT engine's point of view they look
//! non-existent, no column has ever been written to, and the row could not
//! participate in an LWW comparison. That is a silent-corruption vector.
//!
//! The backfill pass runs inside a single IMMEDIATE transaction so the
//! addition of columns + trigger install + row rewrite is atomic; a partial
//! install that leaves rows without HLC metadata is not a reachable state.
//!
//! Every pre-existing row shares one causal instant — the HLC freshly issued
//! *inside* this transaction. From peers' point of view the whole legacy
//! dataset materialised "just now, at once".

use rusqlite::{params, Connection, Transaction, TransactionBehavior};
use serde_json::Value as JsonValue;

use crate::crdt::hlc::HlcService;
use crate::crdt::trigger::{
    ensure_crdt_columns_and_triggers, get_table_schema, is_safe_identifier,
};
use crate::db::error::DatabaseError;
use crate::error::{Error, Result};
use crate::signature::SignatureProvider;
use crate::store::config::InstallCrdtOptions;
use crate::table_names::TABLE_CRDT_DIRTY_TABLES;

use crate::crdt::apply::column_sig_preimage_from_parts;
use crate::crdt::columns::{COLUMN_HLCS_COLUMN, COLUMN_SIGS_COLUMN, HLC_TIMESTAMP_COLUMN};

/// Install CRDT metadata + triggers on `table_name`, backfilling any
/// pre-existing rows so they immediately participate in LWW. See the module
/// docs for the atomicity guarantees.
pub fn install_crdt(
    conn: &mut Connection,
    table_name: &str,
    opts: InstallCrdtOptions,
    hlc: &HlcService,
    provider: &dyn SignatureProvider,
) -> Result<()> {
    if !is_safe_identifier(table_name) {
        return Err(DatabaseError::ValidationError {
            reason: format!("install_crdt: unsafe table identifier '{table_name}'"),
        }
        .into());
    }

    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(DatabaseError::from)?;

    let already_managed = {
        let cols = get_table_schema(&tx, table_name).map_err(DatabaseError::from)?;
        !cols.is_empty()
            && cols.iter().any(|c| c.name == HLC_TIMESTAMP_COLUMN)
            && cols.iter().any(|c| c.name == COLUMN_HLCS_COLUMN)
            && cols.iter().any(|c| c.name == COLUMN_SIGS_COLUMN)
    };

    if already_managed {
        if !opts.allow_reinstall {
            return Err(Error::CrdtAlreadyInstalled {
                table: table_name.to_string(),
            });
        }
        // Reinstall path: refresh triggers, skip backfill.
        ensure_crdt_columns_and_triggers(&tx, table_name)
            .map_err(|e| DatabaseError::CrdtSetup(e.to_string()))?;
        tx.commit().map_err(DatabaseError::from)?;
        return Ok(());
    }

    ensure_crdt_columns_and_triggers(&tx, table_name)
        .map_err(|e| DatabaseError::CrdtSetup(e.to_string()))?;

    let touched = backfill_existing_rows(&tx, table_name, hlc, provider)?;

    if touched > 0 {
        mark_dirty(&tx, table_name)?;
    }

    tx.commit().map_err(DatabaseError::from)?;
    Ok(())
}

/// Populate metadata on every pre-existing row of `table_name`. Returns the
/// number of rows touched. All rows share the single HLC issued inside `tx`.
fn backfill_existing_rows(
    tx: &Transaction<'_>,
    table_name: &str,
    hlc: &HlcService,
    provider: &dyn SignatureProvider,
) -> Result<usize> {
    let schema = get_table_schema(tx, table_name).map_err(DatabaseError::from)?;
    let data_columns: Vec<String> = schema
        .iter()
        .filter(|c| {
            c.name != HLC_TIMESTAMP_COLUMN
                && c.name != COLUMN_HLCS_COLUMN
                && c.name != COLUMN_SIGS_COLUMN
        })
        .map(|c| c.name.clone())
        .collect();
    let pk_columns: Vec<String> = schema
        .iter()
        .filter(|c| c.is_pk)
        .map(|c| c.name.clone())
        .collect();

    if pk_columns.is_empty() {
        return Err(DatabaseError::CrdtSetup(format!(
            "install_crdt: table '{table_name}' has no primary key — cannot backfill"
        ))
        .into());
    }

    let row_count: i64 = tx
        .query_row(
            &format!("SELECT COUNT(*) FROM \"{table_name}\""),
            [],
            |r| r.get(0),
        )
        .map_err(DatabaseError::from)?;
    if row_count == 0 {
        return Ok(0);
    }

    // One HLC for every legacy row — cheaper and semantically identical to
    // issuing per-row HLCs, and matches plan §6 ("all pre-existing rows
    // share one causal instant").
    let hlc_ts = hlc
        .new_timestamp_and_persist(tx)
        .map_err(|e| DatabaseError::HlcError { reason: e.to_string() })?;
    let hlc_str = hlc_ts.to_string();

    // Serialize the two metadata JSON blobs once — they are identical for
    // every backfilled row of this table.
    let column_hlcs_json = build_column_hlcs_json(&data_columns, &hlc_str);
    let column_sigs_json =
        build_column_sigs_json(table_name, &pk_columns, &data_columns, &hlc_str, provider)?;

    let update_sql = format!(
        "UPDATE \"{table_name}\" \
         SET {HLC_TIMESTAMP_COLUMN} = ?1, \
             {COLUMN_HLCS_COLUMN} = ?2, \
             {COLUMN_SIGS_COLUMN} = ?3 \
         WHERE {HLC_TIMESTAMP_COLUMN} IS NULL"
    );
    let touched = tx
        .execute(
            &update_sql,
            params![&hlc_str, &column_hlcs_json, &column_sigs_json],
        )
        .map_err(DatabaseError::from)?;
    Ok(touched)
}

fn build_column_hlcs_json(data_columns: &[String], hlc_str: &str) -> String {
    let mut map = serde_json::Map::with_capacity(data_columns.len());
    for col in data_columns {
        map.insert(col.clone(), JsonValue::String(hlc_str.to_string()));
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// The sig JSON for backfilled rows is built from the provider's raw
/// `sign_column` bytes wrapped as `{ "bytes": "<hex>" }` — a minimal opaque
/// shape the crate owns for its own generated preimages. Consumers whose
/// wire format differs are expected to install CRDT before writing legacy
/// data; the backfill path is for the "just added a peer" case where no
/// prior sig existed to relay.
fn build_column_sigs_json(
    table_name: &str,
    pk_columns: &[String],
    data_columns: &[String],
    hlc_str: &str,
    provider: &dyn SignatureProvider,
) -> Result<String> {
    if data_columns.is_empty() {
        return Ok("{}".to_string());
    }
    // Under NoopSignatureProvider, sign_column returns an empty vec; drop the
    // entry rather than emit an empty payload masquerading as a signature.
    let mut map = serde_json::Map::new();
    for col in data_columns {
        // Preimage for backfill uses the same layout as apply — but with
        // `row_pks` deliberately left empty: the sig is a table+column+hlc
        // commitment, not row-bound. A real provider that requires row
        // binding should install CRDT before it has legacy rows.
        let _preimage = column_sig_preimage_from_parts(
            table_name,
            &placeholder_row_pks_json(pk_columns),
            col,
            hlc_str,
            &JsonValue::Null,
        );
        let bytes = provider.sign_column(&_preimage)?;
        if bytes.is_empty() {
            continue;
        }
        map.insert(col.clone(), JsonValue::String(hex(&bytes)));
    }
    Ok(serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string()))
}

fn placeholder_row_pks_json(pk_columns: &[String]) -> String {
    let mut map = serde_json::Map::new();
    for c in pk_columns {
        map.insert(c.clone(), JsonValue::Null);
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn mark_dirty(tx: &Transaction<'_>, table_name: &str) -> Result<()> {
    tx.execute(
        &format!(
            "INSERT INTO {TABLE_CRDT_DIRTY_TABLES} (table_name, last_modified) \
             VALUES (?1, datetime('now')) \
             ON CONFLICT(table_name) DO UPDATE SET last_modified = excluded.last_modified"
        ),
        params![table_name],
    )
    .map_err(DatabaseError::from)?;
    Ok(())
}
