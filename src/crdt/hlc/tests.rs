//! Tests for the HLC service and the HLC-string helpers.
//!
//! Split out of `hlc.rs` to keep both files under the repo's 500-LoC cap.
//! Every `uhlc::HLC` a test needs comes from [`super::build_hlc`], so a test
//! can never pin a drift tolerance other than [`super::MAX_REMOTE_HLC_DRIFT`].

use super::*;
use crate::device_id::StaticDeviceId;
use rusqlite::Connection;
use std::str::FromStr;
use uhlc::NTP64;

/// One hour, the margin the tolerance tests sit either side of
/// [`MAX_REMOTE_HLC_DRIFT`] by.
const MARGIN: Duration = Duration::from_secs(60 * 60);

/// An HLC `offset` beyond the live wall clock, carrying `svc`'s node id.
///
/// Derived from the clock rather than hardcoded so a value chosen to sit
/// just outside the tolerance cannot rot into it (or vice versa) as the
/// tolerance changes or time passes. Reads the clock directly instead of
/// taking `svc`'s own timestamp, so an earlier accepted future timestamp
/// cannot drag the base forward and make the offset mean something else.
fn hlc_ahead_of_now(svc: &HlcService, offset: Duration) -> String {
    let id = *svc.new_timestamp().expect("timestamp").get_id();
    let shifted = system_time_clock().as_u64() + NTP64::from(offset).as_u64();
    format!("{shifted}/{id}")
}

fn fresh_configs_table(conn: &Connection) {
    conn.execute(
        &format!(
            "CREATE TABLE {TABLE_CRDT_CONFIGS} (key TEXT PRIMARY KEY, type TEXT NOT NULL, value TEXT NOT NULL)"
        ),
        [],
    )
    .expect("Should create table");
}

#[test]
fn test_timestamp_format() {
    // Verify that uhlc uses the "time/node_id_hex" format
    let node_id = ID::try_from([1u8; 16]).unwrap();
    let hlc = build_hlc(Some(node_id));

    let timestamp = hlc.new_timestamp();
    let formatted = timestamp.to_string();

    assert!(formatted.contains('/'), "Timestamp should contain '/'");
    let parts: Vec<&str> = formatted.split('/').collect();
    assert_eq!(parts.len(), 2, "Timestamp should have exactly 2 parts");

    let time_part = parts[0].parse::<u64>();
    assert!(time_part.is_ok(), "Time part should be a valid u64");

    assert!(
        parts[1].len() <= 32,
        "Node ID hex should be at most 32 characters (16 bytes)"
    );
    assert!(!parts[1].is_empty(), "Node ID hex should not be empty");
}

#[test]
fn test_timestamp_parsing() {
    let node_id = ID::try_from([2u8; 16]).unwrap();
    let hlc = build_hlc(Some(node_id));

    let original = hlc.new_timestamp();
    let formatted = original.to_string();

    let parsed = Timestamp::from_str(&formatted).expect("Should parse timestamp");

    assert_eq!(original, parsed, "Parsed timestamp should equal original");
}

#[test]
fn test_timestamp_ordering() {
    let node_id = ID::try_from([4u8; 16]).unwrap();
    let hlc = build_hlc(Some(node_id));

    let ts1 = hlc.new_timestamp();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let ts2 = hlc.new_timestamp();
    std::thread::sleep(std::time::Duration::from_millis(1));
    let ts3 = hlc.new_timestamp();

    assert!(ts1 < ts2, "ts1 should be less than ts2");
    assert!(ts2 < ts3, "ts2 should be less than ts3");
}

#[test]
fn test_hlc_persistence() {
    let mut conn = Connection::open_in_memory().expect("Should create in-memory DB");
    fresh_configs_table(&conn);

    let node_id = ID::try_from([5u8; 16]).unwrap();
    let hlc = build_hlc(Some(node_id));

    let original_timestamp = hlc.new_timestamp();

    {
        let tx = conn.transaction().expect("Should start transaction");
        HlcService::persist_timestamp(&tx, &original_timestamp).expect("Should persist timestamp");
        tx.commit().expect("Should commit");
    }

    let loaded_timestamp = HlcService::load_last_timestamp(&conn).expect("Should load timestamp");

    assert!(loaded_timestamp.is_some(), "Should have loaded a timestamp");
    assert_eq!(
        loaded_timestamp.unwrap(),
        original_timestamp,
        "Loaded timestamp should match original"
    );
}

#[test]
fn try_initialize_from_provider_reads_persisted_timestamp() {
    let mut conn = Connection::open_in_memory().expect("open");
    fresh_configs_table(&conn);

    // Seed a persisted timestamp created by a foreign node so we can
    // check try_initialize consumes it.
    let foreign_node = ID::try_from([9u8; 16]).unwrap();
    let foreign_hlc = build_hlc(Some(foreign_node));
    let foreign_ts = foreign_hlc.new_timestamp();
    {
        let tx = conn.transaction().expect("tx");
        HlcService::persist_timestamp(&tx, &foreign_ts).expect("persist");
        tx.commit().expect("commit");
    }

    let uuid = Uuid::from_bytes([7u8; 16]);
    let provider = StaticDeviceId(uuid);
    let svc = HlcService::try_initialize(&conn, &provider).expect("init");
    let next = svc.new_timestamp().expect("timestamp");

    assert!(
        next > foreign_ts,
        "next local timestamp must dominate the persisted foreign one"
    );
}

#[test]
fn compare_treats_node_ids_numerically_not_lexically() {
    let with_leading = "5/01";
    let without_leading = "5/1";
    assert_eq!(
        compare_hlc_strings(with_leading, without_leading),
        std::cmp::Ordering::Equal,
        "node ids '01' and '1' must compare as equal"
    );
}

#[test]
fn compare_with_wide_node_ids_orders_numerically() {
    assert_eq!(
        compare_hlc_strings("5/02", "5/10"),
        std::cmp::Ordering::Less,
        "node 0x02 must compare less than 0x10 numerically"
    );
}

#[test]
fn advance_past_remote_rejects_malformed_string() {
    let svc = HlcService::new_with_uuid(Uuid::from_bytes([1u8; 16]));
    let result = svc.advance_past_remote("not-a-timestamp");
    assert!(
        matches!(result, Err(HlcError::Parse(_))),
        "Expected Err(HlcError::Parse), got: {:?}",
        result
    );
}

#[test]
fn advance_past_remote_reports_drift_not_a_parse_failure() {
    let svc = HlcService::new_with_uuid(Uuid::from_bytes([3u8; 16]));
    let beyond = hlc_ahead_of_now(&svc, MAX_REMOTE_HLC_DRIFT + MARGIN);
    let err = svc
        .advance_past_remote(&beyond)
        .expect_err("a timestamp past the drift limit must be refused");

    assert!(
        matches!(err, HlcError::RemoteTimestampOutOfTolerance { .. }),
        "a well-formed but out-of-tolerance timestamp is clock skew, \
         not a parse failure: {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("drift tolerance") && !message.contains("parse"),
        "the message must send the reader at the clock, not the parser: {message}"
    );
}

#[test]
fn advance_past_remote_errors_when_uninitialized() {
    let svc = HlcService::new();
    let initialized = HlcService::new_with_uuid(Uuid::from_bytes([2u8; 16]));
    let ts_str = initialized.new_timestamp().unwrap().to_string();

    let result = svc.advance_past_remote(&ts_str);
    assert!(
        matches!(result, Err(HlcError::NotInitialized)),
        "Expected Err(HlcError::NotInitialized), got: {:?}",
        result
    );
}

#[test]
fn advance_past_remote_ok_on_empty_string() {
    let svc = HlcService::new_with_uuid(Uuid::from_bytes([3u8; 16]));
    let result = svc.advance_past_remote("");
    assert!(result.is_ok(), "Expected Ok(()), got: {:?}", result);
}

/// The `new_for_testing` shim must derive its UUID deterministically
/// from the input string: two constructions from the same string must
/// yield HLC services with the same node id.
#[cfg(feature = "test-shims")]
#[allow(deprecated)]
#[test]
fn new_for_testing_is_deterministic() {
    let svc_a = HlcService::new_for_testing("test-device-a");
    let svc_b = HlcService::new_for_testing("test-device-a");
    let ts_a = svc_a.new_timestamp().expect("timestamp a").to_string();
    let ts_b = svc_b.new_timestamp().expect("timestamp b").to_string();
    let node_a = hlc_node_id_suffix(&ts_a).expect("node id a");
    let node_b = hlc_node_id_suffix(&ts_b).expect("node id b");
    assert_eq!(
        node_a, node_b,
        "the shim must hash equal inputs to equal UUIDs"
    );
    // uhlc strips leading zeros; a 16-byte UUID hex is 1..=32 chars.
    assert!(!node_a.is_empty(), "node id must be non-empty");
    assert!(node_a.len() <= 32, "node id must be at most 16 bytes hex");
    // Sanity check that we are not accidentally returning a constant:
    // a different input must hash to a *different* node id.
    let svc_c = HlcService::new_for_testing("test-device-b");
    let ts_c = svc_c.new_timestamp().expect("timestamp c").to_string();
    let node_c = hlc_node_id_suffix(&ts_c).expect("node id c");
    assert_ne!(
        node_a, node_c,
        "different inputs must hash to different UUIDs"
    );
}

/// Locks the `HlcError::DeviceStore` variant name at the type level.
///
/// haex-vault (and other pre-extraction consumers) pattern-match on this
/// name; a rename would be a silent semver break for them. This test
/// fails at *compile* time if the variant is renamed, which is exactly
/// the tripwire we want.
#[test]
fn device_store_variant_name_is_stable() {
    let err = HlcError::DeviceStore("boom".to_string());
    match err {
        HlcError::DeviceStore(msg) => assert_eq!(msg, "boom"),
        other => panic!("expected DeviceStore variant, got {other:?}"),
    }
}

/// `grep` can show there is only one place the tolerance is set; only a
/// behavioural test can show that a constructor actually reaches it.
/// `new_with_uuid` used to skip it entirely and inherit uhlc's 500 ms
/// default — a silent divergence no compile-time check would have caught.
#[test]
fn new_with_uuid_carries_the_crate_drift_tolerance() {
    let svc = HlcService::new_with_uuid(Uuid::from_bytes([11u8; 16]));

    svc.advance_past_remote(&hlc_ahead_of_now(&svc, MAX_REMOTE_HLC_DRIFT - MARGIN))
        .expect("an hour inside the tolerance must be accepted");
    let err = svc
        .advance_past_remote(&hlc_ahead_of_now(&svc, MAX_REMOTE_HLC_DRIFT + MARGIN))
        .expect_err("an hour past the tolerance must be refused");
    assert!(
        matches!(err, HlcError::RemoteTimestampOutOfTolerance { .. }),
        "expected a drift refusal, got: {err:?}"
    );
}

/// The other production constructor, pinned to the same tolerance. It set
/// one second before [`MAX_REMOTE_HLC_DRIFT`] existed, so which constructor
/// a consumer called decided how much clock skew it could survive.
#[test]
fn try_initialize_carries_the_crate_drift_tolerance() {
    let conn = Connection::open_in_memory().expect("open");
    fresh_configs_table(&conn);
    let provider = StaticDeviceId(Uuid::from_bytes([12u8; 16]));
    let svc = HlcService::try_initialize(&conn, &provider).expect("init");

    svc.advance_past_remote(&hlc_ahead_of_now(&svc, MAX_REMOTE_HLC_DRIFT - MARGIN))
        .expect("an hour inside the tolerance must be accepted");
    let err = svc
        .advance_past_remote(&hlc_ahead_of_now(&svc, MAX_REMOTE_HLC_DRIFT + MARGIN))
        .expect_err("an hour past the tolerance must be refused");
    assert!(
        matches!(err, HlcError::RemoteTimestampOutOfTolerance { .. }),
        "expected a drift refusal, got: {err:?}"
    );
}

#[test]
fn remote_hlc_drift_measures_only_future_drift() {
    let svc = HlcService::new_with_uuid(Uuid::from_bytes([13u8; 16]));
    let offset = Duration::from_secs(6 * 60 * 60);
    let drift = remote_hlc_drift(&hlc_ahead_of_now(&svc, offset))
        .expect("a future timestamp must report drift");
    // The two clock reads straddle a `format!`, so they differ by
    // microseconds; bound the slack rather than demanding equality.
    assert!(
        drift <= offset && drift + Duration::from_secs(5) > offset,
        "drift must be the offset, give or take the elapsed wall clock: {drift:?}"
    );

    let now = svc.new_timestamp().expect("timestamp").to_string();
    assert_eq!(
        remote_hlc_drift(&now),
        None,
        "a timestamp at or behind local now has no future drift"
    );
}

#[test]
fn remote_hlc_drift_is_none_for_a_malformed_timestamp() {
    assert_eq!(remote_hlc_drift("not-a-timestamp"), None);
    assert_eq!(remote_hlc_drift(""), None, "the empty HLC is not a drift");
    // A parseable *time* part does not make a timestamp: uhlc rejects a node
    // id with a leading zero. Drift stays undefined for it, so the apply
    // gate lets it through to `compare_hlc_strings`, which reads it as
    // ancient — that comparator, not this function, governs such a change.
    assert_eq!(
        remote_hlc_drift("18446744073709551615/0abc"),
        None,
        "an unparseable node id leaves the drift undefined"
    );
}
