//! Hybrid Logical Clock service. Owns per-device HLC state, persists the
//! latest timestamp in `haex_crdt_configs_no_sync`, and exposes helpers used by the
//! scanner and apply pipeline.
//!
//! Extracted from `haex-vault`. The only change of substance is the
//! Tauri-store lookup for the device UUID — it now comes from the
//! consumer-supplied [`DeviceIdProvider`], per plan §4.1.

use crate::device_id::DeviceIdProvider;
use crate::table_names::TABLE_CRDT_CONFIGS;
use rusqlite::{params, Connection, Transaction};
use std::{
    fmt::Debug,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use uhlc::{HLCBuilder, Timestamp, HLC, ID};
use uuid::Uuid;

const HLC_TIMESTAMP_TYPE: &str = "hlc_timestamp";

#[derive(Error, Debug)]
pub enum HlcError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Failed to parse persisted HLC timestamp: {0}")]
    ParseTimestamp(String),
    #[error("Failed to parse persisted HLC state: {0}")]
    Parse(String),
    #[error("Failed to parse HLC Node ID: {0}")]
    ParseNodeId(String),
    #[error("HLC mutex was poisoned")]
    MutexPoisoned,
    #[error("Failed to create node ID: {0}")]
    CreateNodeId(#[from] uhlc::SizeError),
    #[error("No database connection available")]
    NoConnection,
    #[error("HLC service not initialized")]
    NotInitialized,
    #[error("Hex decode error: {0}")]
    HexDecode(String),
    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(String),
    /// The consumer-supplied [`DeviceIdProvider`] failed to yield a device
    /// UUID. Named after the Tauri-store terminology used on the vault side
    /// (where these errors typically originate) rather than after the trait,
    /// so callers can pattern-match a single "device store lookup failed"
    /// arm regardless of which provider implementation is in play.
    #[error("Device store error: {0}")]
    DeviceStore(String),
}

/// A thread-safe, persistent HLC service.
#[derive(Clone)]
pub struct HlcService {
    hlc: Arc<Mutex<Option<HLC>>>,
}

impl HlcService {
    /// Creates a new HLC service. The HLC will be initialized on first database access.
    pub fn new() -> Self {
        HlcService {
            hlc: Arc::new(Mutex::new(None)),
        }
    }

    /// Deprecated compatibility shim used by pre-extraction test code that
    /// created HLC services with an arbitrary device-id *string* rather than
    /// a UUID. Hashes the input with a stable derivation (BLAKE3 truncated
    /// to 16 bytes) and delegates to [`Self::new_with_uuid`].
    ///
    /// Do NOT use in new code — take a `Uuid` and call `new_with_uuid`
    /// directly. Wire-visible behavior is undefined if two call sites hash
    /// different inputs to the same UUID (BLAKE3 collision resistance makes
    /// this vanishingly unlikely, but it is nonetheless not a contract).
    #[cfg(feature = "test-shims")]
    #[deprecated(
        note = "Take a Uuid and call new_with_uuid; this shim exists only to bridge haex-vault test fixtures."
    )]
    pub fn new_for_testing(device_id_str: &str) -> Self {
        let hash = blake3::hash(device_id_str.as_bytes());
        let bytes = hash.as_bytes();
        let mut uuid_bytes = [0u8; 16];
        uuid_bytes.copy_from_slice(&bytes[..16]);
        Self::new_with_uuid(Uuid::from_bytes(uuid_bytes))
    }

    /// Create an HLC service with a fixed device UUID. Useful for tests
    /// and for consumers that already hold a persisted UUID and want to
    /// skip the provider indirection at construction time.
    ///
    /// Falls back to `HLCBuilder::default()` (which seeds a random
    /// `ID::rand()`) if the UUID's byte pattern is all zeros — `uhlc::ID`
    /// rejects the all-zero id.
    pub fn new_with_uuid(device_uuid: Uuid) -> Self {
        let hlc = match ID::try_from(*device_uuid.as_bytes()) {
            Ok(node_id) => HLCBuilder::new().with_id(node_id).build(),
            Err(_) => HLCBuilder::new().build(),
        };

        HlcService {
            hlc: Arc::new(Mutex::new(Some(hlc))),
        }
    }

    /// Initializes this instance in-place from the given DB connection.
    ///
    /// Unlike [`try_initialize`] this mutates the existing `Arc<Mutex<Option<HLC>>>`,
    /// so clones held by previously registered closures (e.g. the `current_hlc`
    /// UDF) immediately see the initialized state.
    pub fn initialize_in_place(
        &self,
        conn: &Connection,
        device_id: &dyn DeviceIdProvider,
    ) -> Result<(), HlcError> {
        let hlc = Self::build_hlc(conn, device_id)?;

        let mut slot = self.hlc.lock().map_err(|_| HlcError::MutexPoisoned)?;
        *slot = Some(hlc);
        Ok(())
    }

    /// Factory: create and initialize a fresh HLC service from an already
    /// open DB connection and a device-id provider. Preferred entry point
    /// for consumers that don't need to reuse an existing `HlcService`
    /// slot.
    pub fn try_initialize(
        conn: &Connection,
        device_id: &dyn DeviceIdProvider,
    ) -> Result<Self, HlcError> {
        let hlc = Self::build_hlc(conn, device_id)?;

        Ok(HlcService {
            hlc: Arc::new(Mutex::new(Some(hlc))),
        })
    }

    fn build_hlc(conn: &Connection, device_id: &dyn DeviceIdProvider) -> Result<HLC, HlcError> {
        let uuid = device_id
            .device_id()
            .map_err(|e| HlcError::DeviceStore(e.to_string()))?;

        let node_id = ID::try_from(*uuid.as_bytes()).map_err(|e| {
            HlcError::ParseNodeId(format!("Invalid node ID format from device store: {e:?}"))
        })?;

        let hlc = HLCBuilder::new()
            .with_id(node_id)
            .with_max_delta(Duration::from_secs(1))
            .build();

        if let Some(last_timestamp) = Self::load_last_timestamp(conn)? {
            hlc.update_with_timestamp(&last_timestamp).map_err(|e| {
                HlcError::Parse(format!(
                    "Failed to update HLC with persisted timestamp: {e:?}"
                ))
            })?;
        }

        Ok(hlc)
    }

    /// Generate a new timestamp and persist the new HLC state inside the
    /// caller's transaction.
    pub fn new_timestamp_and_persist<'tx>(
        &self,
        tx: &Transaction<'tx>,
    ) -> Result<Timestamp, HlcError> {
        let mut hlc_guard = self.hlc.lock().map_err(|_| HlcError::MutexPoisoned)?;
        let hlc = hlc_guard.as_mut().ok_or(HlcError::NotInitialized)?;

        let new_timestamp = hlc.new_timestamp();
        Self::persist_timestamp(tx, &new_timestamp)?;

        Ok(new_timestamp)
    }

    /// Generate a new timestamp without persisting it (read-side use).
    pub fn new_timestamp(&self) -> Result<Timestamp, HlcError> {
        let mut hlc_guard = self.hlc.lock().map_err(|_| HlcError::MutexPoisoned)?;
        let hlc = hlc_guard.as_mut().ok_or(HlcError::NotInitialized)?;

        Ok(hlc.new_timestamp())
    }

    /// Update the HLC with an external timestamp received during sync.
    pub fn update_with_timestamp(&self, timestamp: &Timestamp) -> Result<(), HlcError> {
        let mut hlc_guard = self.hlc.lock().map_err(|_| HlcError::MutexPoisoned)?;
        let hlc = hlc_guard.as_mut().ok_or(HlcError::NotInitialized)?;

        hlc.update_with_timestamp(timestamp)
            .map_err(|e| HlcError::Parse(format!("Failed to update HLC: {e:?}")))
    }

    /// Advances the HLC clock past a remote HLC timestamp string.
    ///
    /// Call this after applying remote CRDT changes to ensure all future local
    /// timestamps are strictly greater than any received remote timestamp.
    /// Without this, locally created rows can get HLC timestamps that are
    /// filtered out during push (causing incomplete rows on the server).
    /// The caller decides whether a failure is retryable.
    pub fn advance_past_remote(&self, hlc_string: &str) -> Result<(), HlcError> {
        if hlc_string.is_empty() {
            return Ok(());
        }
        let remote_ts = Timestamp::from_str(hlc_string).map_err(|e| {
            HlcError::Parse(format!(
                "Failed to parse remote HLC timestamp '{hlc_string}': {e:?}"
            ))
        })?;
        self.update_with_timestamp(&remote_ts)
    }

    fn load_last_timestamp(conn: &Connection) -> Result<Option<Timestamp>, HlcError> {
        let query =
            format!("SELECT value FROM {TABLE_CRDT_CONFIGS} WHERE key = ?1 AND type = 'hlc'");

        match conn.query_row(&query, params![HLC_TIMESTAMP_TYPE], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(state_str) => {
                let timestamp = Timestamp::from_str(&state_str).map_err(|e| {
                    HlcError::ParseTimestamp(format!("Invalid timestamp format: {e:?}"))
                })?;
                Ok(Some(timestamp))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(HlcError::Database(e)),
        }
    }

    /// Persist a timestamp inside the caller's transaction.
    pub fn persist_timestamp(tx: &Transaction, timestamp: &Timestamp) -> Result<(), HlcError> {
        let timestamp_str = timestamp.to_string();
        tx.execute(
            &format!(
                "INSERT INTO {TABLE_CRDT_CONFIGS} (key, type, value) VALUES (?1, 'hlc', ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            ),
            params![HLC_TIMESTAMP_TYPE, timestamp_str],
        )?;
        Ok(())
    }
}

impl Default for HlcService {
    fn default() -> Self {
        Self::new()
    }
}

/// Compares two HLC timestamp strings numerically.
/// Format: `<u64_ntp_nanoseconds>/<node_id_hex>`.
///
/// Returns Ordering based on the numeric time component, then a **numeric**
/// comparison of the hex node id (uhlc strips leading zeros when serialising
/// 16-byte node ids, so `"01"` and `"1"` are the *same* node — a string
/// comparison gets that wrong).
///
/// **Parse failures fall back to `(0, 0)`** so a malformed or empty HLC
/// compares as "ancient" (oldest) — the safe default for last-write-wins.
///
/// This is a pure comparator invoked from hot `sort_by`/`max_by`/`min_by`
/// paths, so it intentionally does **NOT** log. An earlier version
/// `eprintln!`-ed on every parse failure, which produced one log line *per
/// comparison* and flooded the logs whenever a single corrupt row (empty
/// row-level HLC) was present. Malformed/empty HLCs are detected and kept
/// off the wire at the ingestion boundary in the scanner instead.
pub fn compare_hlc_strings(a: &str, b: &str) -> std::cmp::Ordering {
    fn parse(s: &str) -> (u64, u128) {
        let (time_str, node_str) = match s.split_once('/') {
            Some((t, n)) => (t, n),
            None => (s, ""),
        };
        // Silent fallback to 0 on any parse failure (including empty strings);
        // see the function doc for why this comparator must not log.
        let time = time_str.parse::<u64>().unwrap_or(0);
        let node = if node_str.is_empty() {
            0
        } else {
            parse_hlc_node_hex(node_str).unwrap_or(0)
        };
        (time, node)
    }
    let (a_time, a_node) = parse(a);
    let (b_time, b_node) = parse(b);
    a_time.cmp(&b_time).then_with(|| a_node.cmp(&b_node))
}

/// Returns true if `a` is strictly newer than `b`.
pub fn hlc_is_newer(a: &str, b: &str) -> bool {
    compare_hlc_strings(a, b) == std::cmp::Ordering::Greater
}

/// Returns the maximum HLC timestamp string from an iterator, using numeric comparison.
pub fn hlc_max<'a>(iter: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    iter.max_by(|a, b| compare_hlc_strings(a, b))
}

/// Returns the minimum HLC timestamp string from an iterator, using numeric comparison.
///
/// Lexicographic `.min()` is unsafe on HLC strings: time components have
/// variable width (`"99/x"` lex-precedes `"100/x"`) and node-id hex digits
/// are not zero-padded.
pub fn hlc_min<'a>(iter: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    iter.min_by(|a, b| compare_hlc_strings(a, b))
}

/// Returns the node-id suffix of an HLC timestamp string.
/// Format: `<u64_ntp_nanoseconds>/<node_id_hex>`.
pub fn hlc_node_id_suffix(hlc: &str) -> Option<&str> {
    hlc.split_once('/').map(|(_, node)| node)
}

/// Parse the hex node-id suffix of an HLC into a `u128`. uhlc strips leading
/// zeros when serialising the 16-byte ID (`format!("{:x}", _)`), so a numeric
/// comparison is required for robust equality — string equality would treat
/// `"01"` and `"1"` as different node-ids.
pub fn parse_hlc_node_hex(node_hex: &str) -> Option<u128> {
    u128::from_str_radix(node_hex, 16).ok()
}

/// Convert a device UUID (the same form passed to `HlcService`) to the `u128`
/// representation used by uhlc node-ids. uhlc serialises the 16-byte ID
/// **little-endian** — the least significant byte of the resulting u128 is
/// `uuid.as_bytes()[0]`, not `[15]`.
pub fn device_uuid_to_hlc_node(uuid_str: &str) -> Option<u128> {
    let uuid = Uuid::parse_str(uuid_str).ok()?;
    Some(u128::from_le_bytes(*uuid.as_bytes()))
}

/// Returns true iff the HLC timestamp's node-id matches `expected_node`.
/// Returns `false` for malformed HLCs (no `/`, non-hex suffix).
pub fn hlc_is_from_node(hlc: &str, expected_node: u128) -> bool {
    hlc_node_id_suffix(hlc)
        .and_then(parse_hlc_node_hex)
        .map(|n| n == expected_node)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device_id::StaticDeviceId;
    use rusqlite::Connection;
    use std::str::FromStr;

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
        let hlc = HLCBuilder::new()
            .with_id(node_id)
            .with_max_delta(Duration::from_secs(1))
            .build();

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
        let hlc = HLCBuilder::new()
            .with_id(node_id)
            .with_max_delta(Duration::from_secs(1))
            .build();

        let original = hlc.new_timestamp();
        let formatted = original.to_string();

        let parsed = Timestamp::from_str(&formatted).expect("Should parse timestamp");

        assert_eq!(original, parsed, "Parsed timestamp should equal original");
    }

    #[test]
    fn test_timestamp_ordering() {
        let node_id = ID::try_from([4u8; 16]).unwrap();
        let hlc = HLCBuilder::new()
            .with_id(node_id)
            .with_max_delta(Duration::from_secs(1))
            .build();

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
        let hlc = HLCBuilder::new()
            .with_id(node_id)
            .with_max_delta(Duration::from_secs(1))
            .build();

        let original_timestamp = hlc.new_timestamp();

        {
            let tx = conn.transaction().expect("Should start transaction");
            HlcService::persist_timestamp(&tx, &original_timestamp)
                .expect("Should persist timestamp");
            tx.commit().expect("Should commit");
        }

        let loaded_timestamp =
            HlcService::load_last_timestamp(&conn).expect("Should load timestamp");

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
        let foreign_hlc = HLCBuilder::new()
            .with_id(foreign_node)
            .with_max_delta(Duration::from_secs(1))
            .build();
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
}
