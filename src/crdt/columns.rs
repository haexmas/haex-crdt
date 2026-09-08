//! Well-known implicit column and function names used by the CRDT
//! transformer, trigger installer, and scanner. Extracted so files that
//! only need the names (e.g. the SQL transformer) don't have to pull in
//! the full trigger module.
//!
//! Re-exported from `crate::crdt::trigger` once that module is ported.
//!
//! # The name-suffix rule
//!
//! One suffix governs CRDT participation: **`_no_sync`**, on tables *and*
//! columns. It means the thing is not part of CRDT sync at all — never
//! shipped, and not tracked either. Not shipped because the state must
//! stay on this device (per-device sync cursors above all, since shipping
//! one to another device of the same user would clobber that device's
//! cursor). Not tracked because a column that cannot travel must not
//! advance the row's bookkeeping — tracking it would let a write that can
//! never travel queue a sync round for nothing.
//!
//! Because the rule is one rule, neither the trigger installer nor
//! [`crate::crdt::scanner::scan_table_for_local_changes`] needs an
//! exception list: both reduce to "not a PK and not `_no_sync`-suffixed",
//! and the crate's own three metadata columns
//! ([`HLC_TIMESTAMP_COLUMN`], [`COLUMN_HLCS_COLUMN`],
//! [`COLUMN_SIGS_COLUMN`]) are caught by the same predicate because their
//! names carry the suffix too. There is no way for the two sides to
//! disagree about which columns participate.
//!
//! Enforcement differs by scope, deliberately. At **column** scope the
//! rule is absolute. At **table** scope it governs the automatic path only
//! — `CrdtTransformer` leaves such a table's DDL and writes alone, but
//! trigger discovery keys on the presence of [`HLC_TIMESTAMP_COLUMN`]
//! rather than on the name (see [`crate::db::init`]), so a consumer can
//! still opt a `_no_sync` table in explicitly with `install_crdt`; this
//! crate's own tests do exactly that. There is no per-column equivalent of
//! that opt-in, so nothing overrides the column rule.

/// Hybrid Logical Clock timestamp for the row (row-scoped). The `_no_sync`
/// suffix keeps this structural CRDT metadata out of both the trigger
/// installer's tracked-columns list and the scanner's emitted columns —
/// shipping the CRDT's own bookkeeping as a data change would be
/// meaningless (see the module docs above).
pub const HLC_TIMESTAMP_COLUMN: &str = "haex_hlc_no_sync";

/// Per-column HLC timestamp map, JSON-encoded. The `_no_sync` suffix keeps
/// this structural CRDT metadata out of the tracked-columns list and off
/// the wire (see the module docs above).
pub const COLUMN_HLCS_COLUMN: &str = "haex_column_hlcs_no_sync";

/// Per-column signature map, JSON-encoded. Empty payloads when the store
/// uses `NoopSignatureProvider`. The `_no_sync` suffix keeps this
/// structural CRDT metadata out of the tracked-columns list and off the
/// wire (see the module docs above).
pub const COLUMN_SIGS_COLUMN: &str = "haex_column_sigs_no_sync";

/// Delete-event log: one row per hard-delete on a CRDT-managed table.
/// Business tables carry no soft-delete column; the BEFORE-DELETE trigger
/// writes a row here with `table_name`, `row_pks`, and the transaction HLC
/// so the scanner can propagate the delete on the next sync.
pub const DELETED_ROWS_TABLE: &str = "haex_deleted_rows";

/// UDF that returns a fresh UUIDv4, exposed on every SQLCipher connection.
pub const UUID_FUNCTION_NAME: &str = "gen_uuid";

/// UDF that returns the current HLC timestamp, exposed on every SQLCipher
/// connection. Bound to `HlcService` per connection.
pub const HLC_FUNCTION_NAME: &str = "current_hlc";
