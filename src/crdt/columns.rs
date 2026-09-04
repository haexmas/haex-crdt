//! Well-known implicit column and function names used by the CRDT
//! transformer, trigger installer, and scanner. Extracted so files that
//! only need the names (e.g. the SQL transformer) don't have to pull in
//! the full trigger module.
//!
//! Re-exported from `crate::crdt::trigger` once that module is ported.

/// Hybrid Logical Clock timestamp for the row (row-scoped).
pub const HLC_TIMESTAMP_COLUMN: &str = "haex_hlc";

/// Per-column HLC timestamp map, JSON-encoded.
pub const COLUMN_HLCS_COLUMN: &str = "haex_column_hlcs";

/// Per-column signature map, JSON-encoded. Empty payloads when the store
/// uses `NoopSignatureProvider`.
pub const COLUMN_SIGS_COLUMN: &str = "haex_column_sigs";

/// Tombstone table for row-level deletes.
pub const DELETED_ROWS_TABLE: &str = "haex_deleted_rows";

/// UDF that returns a fresh UUIDv4, exposed on every SQLCipher connection.
pub const UUID_FUNCTION_NAME: &str = "gen_uuid";

/// UDF that returns the current HLC timestamp, exposed on every SQLCipher
/// connection. Bound to `HlcService` per connection.
pub const HLC_FUNCTION_NAME: &str = "current_hlc";
