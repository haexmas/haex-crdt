//! BLOB values and BLOB primary keys across a full scan → apply round trip.
//!
//! Replica A writes through its triggers, the scanner reads the changes (and
//! A's delete log), and `apply_remote_changes` lands them on replica B. The
//! point is the storage type on B: a BLOB must arrive as a BLOB, and a BLOB
//! key must keep naming the same row through insert, update and delete.

use rusqlite::{params, Connection};
use serde_json::json;

use super::make_fixture;
use crate::crdt::apply::{apply_remote_changes, SignatureApplyPolicy};
use crate::crdt::columns::{DELETED_ROWS_TABLE, HLC_FUNCTION_NAME, HLC_TIMESTAMP_COLUMN};
use crate::crdt::hlc::HlcService;
use crate::crdt::scanner::{scan_table_for_local_changes, ColumnChange, ScanFilters};
use crate::crdt::trigger::ensure_crdt_columns_and_triggers;
use crate::signature::NoopSignatureProvider;

const KEY: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];

/// `blobs` carries a BLOB key, a BLOB payload and a TEXT note; both replicas
/// get the same schema with CRDT columns and triggers.
fn replica() -> (Connection, HlcService) {
    let (conn, hlc, _dev) = make_fixture();
    conn.execute_batch(
        "CREATE TABLE blobs (
             id BLOB PRIMARY KEY NOT NULL,
             payload BLOB,
             note TEXT
         );",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    ensure_crdt_columns_and_triggers(&tx, "blobs").unwrap();
    tx.commit().unwrap();
    (conn, hlc)
}

fn scan(conn: &Connection, table: &str) -> Vec<ColumnChange> {
    scan_table_for_local_changes(conn, table, None, "device-a", ScanFilters::default()).unwrap()
}

fn apply(conn: &mut Connection, hlc: &HlcService, changes: Vec<ColumnChange>) {
    apply_remote_changes(
        conn,
        changes,
        hlc,
        &mut SignatureApplyPolicy::new(&NoopSignatureProvider),
    )
    .unwrap();
}

fn insert_on(conn: &Connection, id: &[u8], payload: &[u8], note: &str) {
    conn.execute(
        &format!(
            "INSERT INTO blobs (id, payload, note, {HLC_TIMESTAMP_COLUMN}) \
             VALUES (?1, ?2, ?3, {HLC_FUNCTION_NAME}())"
        ),
        params![id, payload, note],
    )
    .unwrap();
}

/// `(typeof(id), id, typeof(payload), payload)` of every row, in key order.
fn rows_of(conn: &Connection) -> Vec<(String, Vec<u8>, String, Vec<u8>)> {
    let mut stmt = conn
        .prepare("SELECT typeof(id), id, typeof(payload), payload FROM blobs ORDER BY id")
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap();
    rows.collect::<Result<_, _>>().unwrap()
}

#[test]
fn blob_value_and_key_survive_scan_and_apply() {
    let (a, _hlc_a) = replica();
    let (mut b, hlc_b) = replica();
    insert_on(&a, KEY, &[0x00, 0xFF, 0x10], "hello");

    let changes = scan(&a, "blobs");
    let payload = changes.iter().find(|c| c.column_name == "payload").unwrap();
    assert_eq!(payload.row_pks, r#"{"id":{"$blob_hex":"deadbeef"}}"#);
    assert_eq!(payload.value, json!({"$blob_hex": "00ff10"}));
    apply(&mut b, &hlc_b, changes);

    assert_eq!(
        rows_of(&b),
        vec![(
            "blob".to_string(),
            KEY.to_vec(),
            "blob".to_string(),
            vec![0x00, 0xFF, 0x10]
        )]
    );
    let payload: Vec<u8> = b
        .query_row("SELECT payload FROM blobs WHERE id = ?1", [KEY], |r| {
            r.get(0)
        })
        .expect("a consumer reads the BLOB back as bytes");
    assert_eq!(payload, vec![0x00, 0xFF, 0x10]);
}

#[test]
fn update_and_delete_on_a_reach_the_same_blob_keyed_row_on_b() {
    let (a, _hlc_a) = replica();
    let (mut b, hlc_b) = replica();
    insert_on(&a, KEY, &[0x01], "v1");
    apply(&mut b, &hlc_b, scan(&a, "blobs"));

    a.execute(
        &format!(
            "UPDATE blobs SET payload = ?1, {HLC_TIMESTAMP_COLUMN} = {HLC_FUNCTION_NAME}() \
             WHERE id = ?2"
        ),
        params![vec![0x02_u8, 0x03], KEY],
    )
    .unwrap();
    apply(&mut b, &hlc_b, scan(&a, "blobs"));

    assert_eq!(
        rows_of(&b),
        vec![(
            "blob".to_string(),
            KEY.to_vec(),
            "blob".to_string(),
            vec![0x02, 0x03]
        )],
        "the update must land on the existing row, not insert a second one"
    );

    a.execute("DELETE FROM blobs WHERE id = ?1", [KEY]).unwrap();
    let delete_log = scan(&a, DELETED_ROWS_TABLE);
    let logged_pks = delete_log
        .iter()
        .find(|c| c.column_name == "row_pks")
        .unwrap();
    assert_eq!(
        logged_pks.value,
        json!(r#"{"id":{"$blob_hex":"deadbeef"}}"#)
    );
    apply(&mut b, &hlc_b, delete_log);

    assert!(
        rows_of(&b).is_empty(),
        "the propagated delete must match the BLOB key"
    );
}

#[test]
fn delete_log_for_a_blob_key_shadows_a_late_insert() {
    // The shadow check compares the delete log's trigger-written `row_pks`
    // with the scanner's: both must spell the BLOB key the same way.
    let (a, _hlc_a) = replica();
    let (mut b, hlc_b) = replica();
    insert_on(&a, KEY, &[0x01], "v1");
    let late_insert = scan(&a, "blobs");
    a.execute("DELETE FROM blobs WHERE id = ?1", [KEY]).unwrap();
    apply(&mut b, &hlc_b, scan(&a, DELETED_ROWS_TABLE));

    let outcome = apply_remote_changes(
        &mut b,
        late_insert,
        &hlc_b,
        &mut SignatureApplyPolicy::new(&NoopSignatureProvider),
    )
    .unwrap();

    assert_eq!(outcome.report.applied, 0);
    assert!(outcome.report.skipped_shadowed_by_delete > 0);
    assert!(rows_of(&b).is_empty());
}

#[test]
fn text_spelling_the_blob_tag_stays_text() {
    let (a, _hlc_a) = replica();
    let (mut b, hlc_b) = replica();
    let lookalike = r#"{"$blob_hex":"deadbeef"}"#;
    a.execute(
        &format!(
            "INSERT INTO blobs (id, payload, note, {HLC_TIMESTAMP_COLUMN}) \
             VALUES (?1, ?1, ?1, {HLC_FUNCTION_NAME}())"
        ),
        [lookalike],
    )
    .unwrap();

    let changes = scan(&a, "blobs");
    assert!(changes.iter().all(|c| c.value == json!(lookalike)));
    apply(&mut b, &hlc_b, changes);

    let (id_type, payload_type, note_type, note): (String, String, String, String) = b
        .query_row(
            "SELECT typeof(id), typeof(payload), typeof(note), note FROM blobs",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        (id_type.as_str(), payload_type.as_str(), note_type.as_str()),
        ("text", "text", "text")
    );
    assert_eq!(note, lookalike);
}

#[test]
fn malformed_blob_key_tag_is_an_invalid_row_identity() {
    let (mut b, hlc_b) = replica();
    let change = ColumnChange {
        table_name: "blobs".to_string(),
        row_pks: r#"{"id":{"$blob_hex":"DEADBEEF"}}"#.to_string(),
        column_name: "note".to_string(),
        hlc_timestamp: "0000000000000001/abcdef0000000000000000000000".to_string(),
        value: json!("x"),
        device_id: String::new(),
        sig: None,
    };
    let outcome = apply_remote_changes(
        &mut b,
        vec![change],
        &hlc_b,
        &mut SignatureApplyPolicy::new(&NoopSignatureProvider),
    )
    .unwrap();

    assert_eq!(outcome.report.applied, 0);
    assert_eq!(
        outcome.skipped[0].reason,
        crate::crdt::apply::SkipReason::InvalidRowIdentity
    );
    assert!(rows_of(&b).is_empty());
}
