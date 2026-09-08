//! Well-known implicit column and function names used by the CRDT
//! transformer, trigger installer, and scanner. Extracted so files that
//! only need the names (e.g. the SQL transformer) don't have to pull in
//! the full trigger module.
//!
//! Re-exported from `crate::crdt::trigger` once that module is ported.
//!
//! # The two name-suffix rules
//!
//! Two name suffixes govern CRDT participation. They answer different
//! questions and must not be conflated:
//!
//! - **`_no_trigger`** (columns) — fires no trigger, so the column gets no
//!   entry in [`COLUMN_HLCS_COLUMN`] and can never itself drive sync: a
//!   write to it marks nothing dirty. It is **still shipped**, riding the
//!   row-level HLC whenever a tracked sibling syncs. For a column whose
//!   value should travel but whose changes should not schedule traffic of
//!   their own (`updated_at_no_trigger`, …).
//! - **`_no_sync`** (tables *and* columns) — not part of CRDT sync at all:
//!   never shipped, and not tracked either, since a column that cannot
//!   travel must not advance the row's bookkeeping — tracking it would let
//!   a write that can never travel queue a sync round for nothing. So
//!   `_no_sync` implies `_no_trigger`'s effect. For anything that must stay
//!   on this device: per-device sync cursors above all, since shipping one
//!   to another device of the same user would clobber that device's cursor.
//!
//! Enforcement differs by scope, deliberately. At **column** scope both
//! rules are absolute: the trigger installer tracks neither suffix, and
//! [`crate::crdt::scanner::scan_table_for_local_changes`] withholds
//! `_no_sync` columns. At **table** scope `_no_sync` governs the automatic
//! path only — `CrdtTransformer` leaves such a table's DDL and writes
//! alone, but trigger discovery keys on the presence of
//! [`HLC_TIMESTAMP_COLUMN`] rather than on the name (see
//! [`crate::db::init`]), so a consumer can still opt a `_no_sync` table in
//! explicitly with `install_crdt`; this crate's own tests do exactly that.
//! There is no per-column equivalent of that opt-in, so nothing overrides
//! the column rules.
//!
//! # Why the crate's own metadata columns are `_no_trigger`
//!
//! [`HLC_TIMESTAMP_COLUMN`], [`COLUMN_HLCS_COLUMN`] and
//! [`COLUMN_SIGS_COLUMN`] must never ship, so by the rules above they
//! "ought" to be named `*_no_sync`. They are not, and that is deliberate:
//! the scanner withholds them by explicit name instead. Naming them there
//! is the crate describing its own internals — shipping the CRDT's own
//! bookkeeping as data changes would be meaningless — not a consumer
//! exception list of the kind these suffix rules exist to remove. Renaming
//! them would be their third rename and would force a full migration
//! regeneration downstream for no behavioural gain, so the asymmetry
//! stays. It is not an inconsistency to file.

/// Hybrid Logical Clock timestamp for the row (row-scoped). The
/// `_no_trigger` suffix opts the column out of the trigger installer's
/// tracked-columns list; the scanner withholds it by name (see the module
/// docs above on the two suffix rules and on why this column is not named
/// `_no_sync`).
pub const HLC_TIMESTAMP_COLUMN: &str = "haex_hlc_no_trigger";

/// Per-column HLC timestamp map, JSON-encoded. The `_no_trigger` suffix
/// keeps this structural CRDT metadata out of the tracked-columns list;
/// the scanner withholds it by name (see the module docs above).
pub const COLUMN_HLCS_COLUMN: &str = "haex_column_hlcs_no_trigger";

/// Per-column signature map, JSON-encoded. Empty payloads when the store
/// uses `NoopSignatureProvider`. The `_no_trigger` suffix keeps this
/// structural CRDT metadata out of the tracked-columns list; the scanner
/// withholds it by name (see the module docs above).
pub const COLUMN_SIGS_COLUMN: &str = "haex_column_sigs_no_trigger";

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
