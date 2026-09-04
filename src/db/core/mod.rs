//! Core SQL primitives: parsing, value conversion, table-name extraction,
//! prefix stripping, connection wrapper, connection init, and the read-only
//! `select` / `select_with_crdt` entry points. The write path (`execute`,
//! `execute_with_crdt`) lands in Batch C2 alongside the post-write signing
//! hook trait.

/// Statement breakpoint marker used by Drizzle-generated migrations.
pub const DRIZZLE_STATEMENT_BREAKPOINT: &str = "--> statement-breakpoint";

pub mod connection;
pub mod extract;
pub mod init;
pub mod parsing;
pub mod prefix;
pub mod select;
pub mod value;

pub use connection::with_connection;
pub use extract::{
    extract_primary_table_name_from_sql, extract_table_names_from_sql,
    extract_table_names_from_statement,
};
pub use init::{install_tx_hlc_hooks, open_and_init_db, register_current_hlc_udf};
pub use parsing::{parse_single_statement, parse_sql_statements, statement_has_returning};
pub use prefix::strip_main_schema_prefix;
pub use select::{select, select_with_crdt};
pub use value::{convert_value_ref_to_json, ValueConverter};

#[cfg(test)]
mod tests;
