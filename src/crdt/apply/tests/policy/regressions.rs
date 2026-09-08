//! Regression coverage for policy admission and successful-write boundaries.

use super::*;
use crate::crdt::columns::{COLUMN_SIGS_COLUMN, DELETED_ROWS_TABLE, HLC_TIMESTAMP_COLUMN};

#[test]
fn incomplete_numeric_hlc_is_skipped_without_panicking() {
    let (mut conn, hlc, _) = make_fixture();
    create_crdt_table(&conn, "items", "body TEXT");
    let outcome = apply_remote_changes(
        &mut conn,
        vec![
            change("items", "r1", "body", "1", json!("invalid")),
            change("items", "r2", "body", HLC1, json!("valid")),
        ],
        &hlc,
        &mut AcceptAllPolicy,
    )
    .unwrap();
    assert_eq!(outcome.report.applied, 1);
    assert_eq!(outcome.skipped.len(), 1);
    assert_eq!(outcome.skipped[0].input_index, 0);
    assert_eq!(outcome.skipped[0].reason, SkipReason::Stale);
    let ids: String = conn
        .query_row("SELECT id FROM items", [], |r| r.get(0))
        .unwrap();
    assert_eq!(ids, "r2");
}

#[test]
fn rejected_delete_replay_does_not_delete_but_admitted_stale_replay_does() {
    for mut policy in [
        Box::new(SkipRowPolicy) as Box<dyn ApplyPolicy>,
        Box::new(SkipNamedColumnPolicy {
            column_to_skip: "table_name",
        }),
    ] {
        let (mut conn, hlc, _) = make_fixture();
        create_crdt_table(&conn, "items", "body TEXT");
        // A persisted tombstone whose target still needs propagation.
        conn.execute(
            &format!(
                "INSERT INTO {DELETED_ROWS_TABLE}
                 (id, table_name, row_pks, {HLC_TIMESTAMP_COLUMN})
                 VALUES ('del-1', 'items', '{{\"id\":\"r1\"}}', ?1)"
            ),
            [HLC2],
        )
        .unwrap();
        conn.execute(
            &format!(
                "INSERT INTO items (id, body, {HLC_TIMESTAMP_COLUMN}) VALUES ('r1', 'kept', ?1)"
            ),
            [HLC1],
        )
        .unwrap();
        let batch = vec![change(
            DELETED_ROWS_TABLE,
            "del-1",
            "table_name",
            HLC2,
            json!("items"),
        )];
        let outcome =
            apply_remote_changes(&mut conn, batch.clone(), &hlc, policy.as_mut()).unwrap();
        assert_eq!(outcome.report.applied, 0);
        assert_eq!(outcome.skipped[0].reason, SkipReason::Policy);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "a rejected replay must not propagate the stored tombstone"
        );

        // Admit the same replay twice; the second is stale but must still
        // propagate.
        apply_remote_changes(&mut conn, batch.clone(), &hlc, &mut AcceptAllPolicy).unwrap();
        conn.execute("INSERT INTO items (id, body) VALUES ('r1', 'restored')", [])
            .unwrap();
        let outcome = apply_remote_changes(&mut conn, batch, &hlc, &mut AcceptAllPolicy).unwrap();
        assert_eq!(outcome.report.applied, 0);
        assert_eq!(outcome.skipped[0].reason, SkipReason::Stale);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "an admitted stale replay must still propagate");
    }
}

#[test]
fn keep_signature_metadata_preserves_its_bytes_and_sqlite_type() {
    for metadata in [
        SqlValue::Text("{ \"body\": {\"space\": \"signature\"} }".into()),
        SqlValue::Blob(vec![0, 255, 127]),
        SqlValue::Text("[\"consumer\", \"metadata\"]".into()),
    ] {
        let (mut conn, hlc, _) = make_fixture();
        create_crdt_table(&conn, "items", "body TEXT");
        conn.execute(
            &format!("INSERT INTO items (id, body, {COLUMN_SIGS_COLUMN}) VALUES ('r1', 'old', ?1)"),
            [&metadata],
        )
        .unwrap();
        let outcome = apply_remote_changes(
            &mut conn,
            vec![change("items", "r1", "body", HLC1, json!("new"))],
            &hlc,
            &mut AcceptAllPolicy,
        )
        .unwrap();
        assert_eq!(outcome.report.applied, 1);
        let (body, stored): (String, SqlValue) = conn
            .query_row(
                &format!("SELECT body, {COLUMN_SIGS_COLUMN} FROM items WHERE id = 'r1'"),
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(body, "new");
        assert_eq!(
            stored, metadata,
            "Keep must not parse or rewrite consumer metadata"
        );
    }
}

#[test]
fn ignored_insert_or_update_aborts_before_after_row() {
    for row_exists in [false, true] {
        let (mut conn, hlc, _) = make_fixture();
        create_crdt_table(&conn, "items", "body TEXT UNIQUE ON CONFLICT IGNORE");
        conn.execute(
            "INSERT INTO items (id, body) VALUES ('other', 'duplicate')",
            [],
        )
        .unwrap();
        if row_exists {
            conn.execute("INSERT INTO items (id, body) VALUES ('r1', 'original')", [])
                .unwrap();
        }
        let future = hlc_ahead_of_now(&hlc, Duration::from_secs(6 * 60 * 60));
        let err = apply_remote_changes(
            &mut conn,
            vec![change("items", "r1", "body", &future, json!("duplicate"))],
            &hlc,
            &mut FailAfterRowPolicy,
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::Sqlite(rusqlite::Error::StatementChangedRows(0))),
            "ignored writes must fail before after_row is called, got {err:?}"
        );
        let changed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM items WHERE id = 'r1' AND body = 'duplicate'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(changed, 0);
        assert!(hlc_is_newer(
            &future,
            &hlc.new_timestamp().unwrap().to_string()
        ));
    }
}
