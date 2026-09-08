//! `origin_node`: ping-pong prevention.

use super::super::*;

#[test]
fn origin_node_filter_emits_only_this_nodes_writes() {
    let (conn, hlc, dev) = make_fixture();
    create_crdt_table(&conn, "items", "name TEXT");
    insert_row_via_transformer(
        &conn,
        &hlc,
        "INSERT INTO items (id, name) VALUES ('i1', 'ours')",
    );
    // A row whose per-column HLC carries a DIFFERENT node id (simulates a
    // row applied from a remote peer). Insert directly (no transformer) so
    // we control the HLC node-id encoded in the column-HLC map. Node IDs
    // are 32-hex-char (16-byte) uhlc IDs.
    let foreign_hlc = "42/deadbeefdeadbeefdeadbeefdeadbe";
    let foreign_column_hlcs = format!("{{\"name\":\"{foreign_hlc}\"}}");
    conn.execute(
        &format!(
            "INSERT INTO items (id, name, {HLC_TIMESTAMP_COLUMN}, {COLUMN_HLCS_COLUMN}) \
             VALUES ('i2', 'theirs', ?1, ?2)"
        ),
        [foreign_hlc, foreign_column_hlcs.as_str()],
    )
    .unwrap();

    let our_node = device_uuid_to_hlc_node(&dev.to_string()).expect("dev uuid parses");
    let changes = scan_table_for_local_changes(
        &conn,
        "items",
        None,
        &dev.to_string(),
        ScanFilters {
            origin_node: Some(our_node),
            ..Default::default()
        },
    )
    .unwrap();

    // Only i1 (our write) should surface; i2 was authored elsewhere.
    let names: Vec<&JsonValue> = changes.iter().map(|c| &c.value).collect();
    assert_eq!(names, vec![&json!("ours")]);
}
