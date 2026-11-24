//! The one definition of HLC order, in Rust and in SQL.
//!
//! An HLC is stored as `<time>/<node>`: the time is a decimal integer, the
//! node a hex integer without leading zeros. Neither part is ever parsed into
//! a fixed-width integer here. A part is reduced to a *key* (its digits without
//! leading zeros; empty means zero or malformed) and keys are ordered by
//! length first, then digit by digit. That is the numeric order for numbers of
//! any width, so no SQLite `CAST ... AS INTEGER` (signed 64 bit) and no Rust
//! `u64`/`u128` bound limits it. What still bounds a *new* timestamp is `uhlc`
//! itself, whose time is a `u64`.
//!
//! Malformed input (no digits, non-digit characters) has the key of zero, so
//! it loses every comparison instead of aborting one.

use std::cmp::Ordering;

fn split(hlc: &str) -> (&str, &str) {
    hlc.split_once('/').unwrap_or((hlc, ""))
}

fn key(part: &str, is_digit: fn(&u8) -> bool) -> &str {
    if part.bytes().all(|b| is_digit(&b)) {
        part.trim_start_matches('0')
    } else {
        ""
    }
}

pub(crate) fn time_key(hlc: &str) -> &str {
    key(split(hlc).0, u8::is_ascii_digit)
}

pub(crate) fn node_key(hlc: &str) -> &str {
    key(split(hlc).1, u8::is_ascii_hexdigit)
}

/// The time key of a `u64` timestamp, for binding to the SQL helpers below.
pub fn u64_time_key(time: u64) -> String {
    if time == 0 {
        String::new()
    } else {
        time.to_string()
    }
}

fn cmp_keys(a: &str, b: &str) -> Ordering {
    a.len().cmp(&b.len()).then_with(|| {
        a.bytes()
            .map(|c| c.to_ascii_lowercase())
            .cmp(b.bytes().map(|c| c.to_ascii_lowercase()))
    })
}

pub(crate) fn compare(a: &str, b: &str) -> Ordering {
    cmp_keys(time_key(a), time_key(b)).then_with(|| cmp_keys(node_key(a), node_key(b)))
}

/// Whether the node part of `hlc` is `node`, compared with the same key order
/// as [`compare`] (so `"01"` and `"1"` are one node). A missing or malformed
/// node part never matches, not even node zero.
pub(crate) fn is_node(hlc: &str, node: u128) -> bool {
    let (_, part) = split(hlc);
    !part.is_empty()
        && part.bytes().all(|b| b.is_ascii_hexdigit())
        && cmp_keys(
            node_key(hlc),
            key(&format!("{node:x}"), u8::is_ascii_hexdigit),
        ) == Ordering::Equal
}

/// SQL expression for the time key of the HLC in `column` (an already quoted
/// identifier). Mirrors [`time_key`]; NULL stays NULL.
pub(crate) fn sql_time_key(column: &str) -> String {
    let time = format!(
        "substr({column}, 1, CASE WHEN instr({column}, '/') = 0 \
         THEN length({column}) ELSE instr({column}, '/') - 1 END)"
    );
    format!("(CASE WHEN {time} GLOB '*[^0-9]*' THEN '' ELSE ltrim({time}, '0') END)")
}

/// `time(column) < ?` where `param` is a SQL parameter holding a time key.
pub fn sql_time_lt(column: &str, param: &str) -> String {
    let k = sql_time_key(column);
    format!("(length({k}) < length({param}) OR (length({k}) = length({param}) AND {k} < {param}))")
}

/// `time(column) >= ?` where `param` is a SQL parameter holding a time key.
pub(crate) fn sql_time_ge(column: &str, param: &str) -> String {
    let k = sql_time_key(column);
    format!("(length({k}) > length({param}) OR (length({k}) = length({param}) AND {k} >= {param}))")
}

/// `ORDER BY` terms putting the newest time first.
pub(crate) fn sql_time_order_desc(column: &str) -> String {
    let k = sql_time_key(column);
    format!("length({k}) DESC, {k} DESC")
}

#[cfg(test)]
mod tests;
