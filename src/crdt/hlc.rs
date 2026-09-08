//! Hybrid Logical Clock service. Owns a logical replica's in-memory HLC,
//! persists the latest timestamp in `haex_crdt_configs_no_sync`, and exposes
//! helpers used by the scanner and apply pipeline.
//!
//! The HLC node UUID comes from the consumer-supplied [`DeviceIdProvider`].
//! Consumers may use different UUIDs for different logical replicas opening
//! the same DB file; the provider remains responsible for returning a stable
//! UUID when the same replica reopens it.

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
use uhlc::{system_time_clock, HLCBuilder, Timestamp, HLC, ID};
use uuid::Uuid;

const HLC_TIMESTAMP_TYPE: &str = "hlc_timestamp";

/// How far ahead of local now an incoming remote HLC timestamp may lie and
/// still be treated as a clock reading: within this, the timestamp is stored
/// *and* the local clock is advanced past it; beyond it, the change is
/// refused (see `apply_remote_changes`).
///
/// **One-sided.** `uhlc` only rejects drift into the *future*
/// (`msg_time > now && msg_time - now > delta`), so a timestamp in the past
/// is always accepted — it simply loses last-write-wins. This bound is
/// therefore a ceiling on how far a peer may push our clock forward, not a
/// window around now.
///
/// **12 hours, not `uhlc`'s 500 ms default.** Peers are other people's
/// devices: a laptop resuming from sleep, a phone that spent a day offline,
/// a VM with no NTP. At half a second of tolerance those honest devices fall
/// out of tolerance routinely, and the cost of a false refusal is not one
/// lost write but a peer that cannot sync at all. 12 h swallows ordinary
/// device skew (including a whole-timezone misconfiguration) while still
/// bounding how far a hostile or broken peer can drag this device's clock —
/// and thus its own future writes' LWW position — forward.
///
/// **Set explicitly at every construction site**, which deliberately makes
/// `uhlc`'s `UHLC_MAX_DELTA_MS` environment variable inert: sync tolerance
/// is a property of the protocol, not of whoever launched the process.
pub const MAX_REMOTE_HLC_DRIFT: Duration = Duration::from_secs(12 * 60 * 60);

/// The crate's **only** `uhlc::HLC` construction site.
///
/// Every constructor — production and test — funnels through here so that
/// [`MAX_REMOTE_HLC_DRIFT`] cannot be set to a second value by omission:
/// before this existed, `new_with_uuid` inherited `uhlc`'s 500 ms default
/// while `try_initialize` set one second, so a device's drift tolerance
/// depended on which constructor its consumer happened to call.
///
/// `None` yields `HLCBuilder`'s random `ID::rand()`, which is the fallback
/// for a device UUID `uhlc::ID` will not accept (it rejects all-zero ids).
fn build_hlc(node_id: Option<ID>) -> HLC {
    let builder = HLCBuilder::new().with_max_delta(MAX_REMOTE_HLC_DRIFT);
    match node_id {
        Some(id) => builder.with_id(id),
        None => builder,
    }
    .build()
}

/// How far `hlc` lies beyond local now, or `None` if it does not lie beyond
/// it at all (including for a `hlc` that is not a well-formed timestamp).
///
/// Read this against [`MAX_REMOTE_HLC_DRIFT`] to decide whether a remote
/// timestamp is a clock reading. It measures against the same physical clock
/// and applies the same logical-counter masking that
/// `uhlc::HLC::update_with_timestamp` does internally, so a timestamp this
/// function reports as within tolerance is one `update_with_timestamp` will
/// also accept — unless the wall clock runs backwards in between. That
/// equivalence is what lets `apply_remote_changes` refuse over-drift input
/// *before* it opens its transaction and still be sure the clock advance it
/// performs after committing cannot fail for drift.
///
/// A malformed `hlc` returns `None` rather than an error: drift is undefined
/// for a string that is not a timestamp. The apply preflight validates
/// complete `<time>/<node>` timestamps separately; strings without that full
/// shape still reach [`compare_hlc_strings`], which reads them as ancient so
/// they lose LWW instead of being applied.
pub fn remote_hlc_drift(hlc: &str) -> Option<Duration> {
    let remote = *Timestamp::from_str(hlc).ok()?.get_time();
    // uhlc masks off the logical-counter bits of its physical reading before
    // comparing; mask identically so the two comparisons cannot disagree at
    // the boundary by the counter's worth of nanoseconds.
    let mut now = system_time_clock();
    now.0 &= !((1u64 << uhlc::CSIZE) - 1);
    if remote > now {
        Some((remote - now).to_duration())
    } else {
        None
    }
}

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
    /// A remote timestamp lay outside `uhlc`'s drift tolerance, so the local
    /// clock was NOT advanced past it. Distinct from [`Self::Parse`] because
    /// the timestamp was well-formed — the two clocks disagree — and the
    /// fix is a clock-skew investigation, not a parser one.
    #[error("Remote HLC '{hlc}' is outside the local clock's drift tolerance: {reason}")]
    RemoteTimestampOutOfTolerance { hlc: String, reason: String },
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
    /// Legacy spelling retained for source compatibility with consumers that
    /// matched this public error variant before the `DeviceStore` rename.
    #[deprecated(note = "use HlcError::DeviceStore")]
    #[error("Device id provider error: {0}")]
    DeviceIdProvider(String),
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
    /// Falls back to a random `ID::rand()` if the UUID's byte pattern is all
    /// zeros — `uhlc::ID` rejects the all-zero id.
    pub fn new_with_uuid(device_uuid: Uuid) -> Self {
        let hlc = build_hlc(ID::try_from(*device_uuid.as_bytes()).ok());

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
        let hlc = Self::build_hlc_from_db(conn, device_id)?;

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
        let hlc = Self::build_hlc_from_db(conn, device_id)?;

        Ok(HlcService {
            hlc: Arc::new(Mutex::new(Some(hlc))),
        })
    }

    /// Build an HLC for the provider's logical replica and fold in the latest
    /// timestamp persisted in this DB, so a restart cannot hand out timestamps
    /// already used in this file. Distinct from the module-level [`build_hlc`],
    /// which owns only the `uhlc` configuration.
    fn build_hlc_from_db(
        conn: &Connection,
        device_id: &dyn DeviceIdProvider,
    ) -> Result<HLC, HlcError> {
        let uuid = device_id
            .device_id()
            .map_err(|e| HlcError::DeviceStore(e.to_string()))?;

        let node_id = ID::try_from(*uuid.as_bytes()).map_err(|e| {
            HlcError::ParseNodeId(format!("Invalid node ID format from device store: {e:?}"))
        })?;

        let hlc = build_hlc(Some(node_id));

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
        // Inlined rather than delegating to `update_with_timestamp` so a
        // drift refusal reports as clock skew and names the offending
        // timestamp, instead of inheriting that method's generic parse
        // wrapper — which sent readers of `ExceedingDeltaError` looking for
        // a corrupt persisted state.
        let mut hlc_guard = self.hlc.lock().map_err(|_| HlcError::MutexPoisoned)?;
        let hlc = hlc_guard.as_mut().ok_or(HlcError::NotInitialized)?;
        hlc.update_with_timestamp(&remote_ts)
            .map_err(|e| HlcError::RemoteTimestampOutOfTolerance {
                hlc: hlc_string.to_string(),
                reason: format!("{e:?}"),
            })
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
/// row-level HLC) was present. Complete malformed timestamps are rejected by
/// apply preflight before this comparator can influence a write; malformed or
/// empty strings that reach other comparator call sites retain the ancient
/// fallback.
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
mod tests;
