//! Database layer: opaque connection handling, SQL execution, migrations,
//! at-rest encryption. Only `error` is populated in Batch A; the rest
//! ports in later batches.

pub mod error;
