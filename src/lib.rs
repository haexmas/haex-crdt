//! `haex-crdt` — SQLite + SQLCipher storage with column-level LWW CRDT sync.
//!
//! This crate is the extraction target for the CRDT/storage layer currently
//! living inside `haex-vault`. See the extraction plan
//! (`holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md`) for scope,
//! trait boundaries, and the sequence by which modules move here.
//!
//! Public surface is a working skeleton: types and trait shapes are in place;
//! the CRDT implementation itself (triggers, scanner, apply pipeline, HLC
//! service) is ported from `haex-vault` in a follow-up step.

pub mod crdt;
pub mod db;
pub mod device_id;
pub mod error;
pub mod migration;
pub mod signature;
pub mod table_names;

pub use device_id::{DeviceIdProvider, StaticDeviceId};
pub use error::{Error, MigrationJournal, Result};
pub use migration::{MigrationName, MigrationSource, StaticMigrationSource};
pub use signature::{AuthorId, NoopSignatureProvider, RemoteChanges, SignatureProvider};

pub use crdt::hlc::{
    compare_hlc_strings, device_uuid_to_hlc_node, hlc_is_from_node, hlc_is_newer, hlc_max,
    hlc_min, hlc_node_id_suffix, parse_hlc_node_hex, HlcError, HlcService,
};
