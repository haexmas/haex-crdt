//! `install_crdt` backfill contract (plan §6).
//!
//! Adding CRDT metadata columns to a table that already carries rows leaves
//! those rows with `NULL` in the row-level HLC and `'{}'` in the
//! column-HLC map — from the CRDT engine's point of view they look
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
use crate::database::config::InstallCrdtOptions;
use crate::db::core::convert_value_ref_to_json;
use crate::db::error::DatabaseError;
use crate::error::{Error, Result};
use crate::signature::SignatureProvider;
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
            !c.is_pk
                && c.name != HLC_TIMESTAMP_COLUMN
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

    // Read the legacy values before updating metadata. PK JSON is serialized
    // in schema order, matching the scanner's wire format, while the owned
    // SQL PK values are retained for an exact row update (including BLOBs).
    let selected_columns: Vec<String> = pk_columns
        .iter()
        .chain(data_columns.iter())
        .cloned()
        .collect();
    let select_sql = format!(
        "SELECT {} FROM \"{table_name}\"",
        selected_columns
            .iter()
            .map(|column| format!("\"{column}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut stmt = tx.prepare(&select_sql).map_err(DatabaseError::from)?;
    let mut rows = stmt.query([]).map_err(DatabaseError::from)?;
    let mut legacy_rows = Vec::new();
    while let Some(row) = rows.next().map_err(DatabaseError::from)? {
        let mut pk_json_values = Vec::with_capacity(pk_columns.len());
        let mut pk_sql_values = Vec::with_capacity(pk_columns.len());
        for index in 0..pk_columns.len() {
            pk_json_values.push(convert_value_ref_to_json(row.get_ref(index)?).map_err(|e| {
                DatabaseError::SerializationError {
                    reason: e.to_string(),
                }
            })?);
            pk_sql_values.push(row.get(index)?);
        }
        let mut data_values = Vec::with_capacity(data_columns.len());
        for index in pk_columns.len()..selected_columns.len() {
            data_values.push(convert_value_ref_to_json(row.get_ref(index)?).map_err(|e| {
                DatabaseError::SerializationError {
                    reason: e.to_string(),
                }
            })?);
        }
        let row_pks = serialize_row_pks(&pk_columns, &pk_json_values)?;
        legacy_rows.push((row_pks, pk_sql_values, data_values));
    }
    drop(rows);
    drop(stmt);

    if legacy_rows.is_empty() {
        return Ok(0);
    }

    // One HLC for every legacy row — cheaper and semantically identical to
    // issuing per-row HLCs, and matches plan §6 ("all pre-existing rows
    // share one causal instant").
    let hlc_ts = hlc
        .new_timestamp_and_persist(tx)
        .map_err(|e| DatabaseError::HlcError {
            reason: e.to_string(),
        })?;
    let hlc_str = hlc_ts.to_string();

    // Serialize the HLC metadata once — it is identical for every
    // backfilled row — but sign each row's actual values separately.
    let column_hlcs_json = build_column_hlcs_json(&data_columns, &hlc_str);
    let mut touched = 0;
    for (row_pks, pk_sql_values, data_values) in legacy_rows {
        let column_sigs_json = build_column_sigs_json(
            table_name,
            &row_pks,
            &data_columns,
            &data_values,
            &hlc_str,
            provider,
        )?;
        let where_clause = pk_columns
            .iter()
            .map(|column| format!("\"{}\" = ?", column))
            .collect::<Vec<_>>()
            .join(" AND ");
        let update_sql = format!(
            "UPDATE \"{table_name}\" SET {HLC_TIMESTAMP_COLUMN} = ?, \
             {COLUMN_HLCS_COLUMN} = ?, {COLUMN_SIGS_COLUMN} = ? WHERE {where_clause}"
        );
        let mut values = vec![
            rusqlite::types::Value::Text(hlc_str.clone()),
            rusqlite::types::Value::Text(column_hlcs_json.clone()),
            rusqlite::types::Value::Text(column_sigs_json),
        ];
        values.extend(pk_sql_values);
        let params: Vec<&dyn rusqlite::ToSql> = values
            .iter()
            .skip(3)
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        let mut all_params: Vec<&dyn rusqlite::ToSql> = values[..3]
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        all_params.extend(params);
        touched += tx
            .execute(&update_sql, &*all_params)
            .map_err(DatabaseError::from)?;
    }
    Ok(touched)
}

/// Build the shared per-column HLC map for a backfill batch.
fn build_column_hlcs_json(data_columns: &[String], hlc_str: &str) -> String {
    let mut map = serde_json::Map::with_capacity(data_columns.len());
    for col in data_columns {
        map.insert(col.clone(), JsonValue::String(hlc_str.to_string()));
    }
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

/// Serialize primary-key values in schema-declaration order, matching the
/// canonical row identity emitted by the outbound scanner.
fn serialize_row_pks(pk_columns: &[String], values: &[JsonValue]) -> Result<String> {
    let mut json = String::from("{");
    for (index, (column, value)) in pk_columns.iter().zip(values).enumerate() {
        if index > 0 {
            json.push(',');
        }
        json.push_str(&serde_json::to_string(column).map_err(|e| {
            DatabaseError::SerializationError {
                reason: format!("serialize primary-key column '{column}': {e}"),
            }
        })?);
        json.push(':');
        json.push_str(&serde_json::to_string(value).map_err(|e| {
            DatabaseError::SerializationError {
                reason: format!("serialize primary-key value for '{column}': {e}"),
            }
        })?);
    }
    json.push('}');
    Ok(json)
}

/// The sig JSON for backfilled rows stores the provider's raw `sign_column`
/// bytes as a hex string — a minimal opaque shape the crate owns for its own
/// generated preimages. Consumers whose wire format differs are expected to
/// install CRDT before writing legacy data; the backfill path is for the
/// "just added a peer" case where no prior sig existed to relay.
fn build_column_sigs_json(
    table_name: &str,
    row_pks: &str,
    data_columns: &[String],
    data_values: &[JsonValue],
    hlc_str: &str,
    provider: &dyn SignatureProvider,
) -> Result<String> {
    if data_columns.is_empty() {
        return Ok("{}".to_string());
    }
    // Under NoopSignatureProvider, sign_column returns an empty vec; drop the
    // entry rather than emit an empty payload masquerading as a signature.
    let mut map = serde_json::Map::new();
    for (col, value) in data_columns.iter().zip(data_values) {
        let _preimage = column_sig_preimage_from_parts(table_name, row_pks, col, hlc_str, value);
        let bytes = provider.sign_column(&_preimage)?;
        if bytes.is_empty() {
            continue;
        }
        map.insert(col.clone(), JsonValue::String(hex(&bytes)));
    }
    Ok(serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string()))
}

/// Encode provider output as the opaque JSON string stored by the scanner.
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Mark a table for the next outbound scan after a successful backfill.
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
