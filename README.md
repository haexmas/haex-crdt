# haex-crdt

SQLite + SQLCipher storage with column-level LWW CRDT sync (uhlc-based Hybrid Logical Clocks). Extracted from `haex-vault` so both `haex-vault` and `holzi` can consume it as a Rust crate dependency.

**Status**: `v0.1.0`. Trait foundation, HLC service, SQL transformers, trigger installer, scanner, cleanup, migration engine, apply pipeline, public `Database` facade, cross-process file locking, and the plan §8 end-to-end acceptance test all land. Dual-licensed **MIT OR Apache-2.0**. Consume via `haex-crdt = { git = "https://github.com/haexmas/haex-crdt", tag = "v0.1.0" }`.

**Ownership**: source lived in `haex-vault`. This repository is the extraction target. Both `haex-vault` and `holzi` will depend on tagged releases here.

## Scope

Provided by this crate:

- Encrypted SQLite storage (SQLCipher).
- A migration engine with two journals — crate-owned CRDT bookkeeping and consumer-owned schema, per [plan §4.3](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md).
- HLC service (uhlc-based Hybrid Logical Clocks with SQLite-persisted state).
- Column-level LWW CRDT infrastructure: schema transformer, trigger installer, scanner, apply pipeline.
- Cleanup / retention utilities for deleted-row logs.
- Pluggable [`DeviceIdProvider`](src/device_id.rs) — consumers supply the durable device UUID.
  A mismatch on reopen is resolved by `DeviceIdPolicy`: `Reject` (default) fails the open,
  `AdoptOnMismatch` takes the supplied UUID over so a relocated `.db` can be adopted as a
  device handover.
- Pluggable [`SignatureProvider`](src/signature.rs) — consumers add per-column signing without this crate depending on any identity system. `NoopSignatureProvider` is bundled for consumers that already have an authenticated transport (e.g. an MLS group or an attested iroh channel).
- Pluggable [`MigrationSource`](src/migration.rs) — consumers control where schema migration SQL comes from.

Not provided (intentionally):

- Sync transport (iroh, WebSocket, cloud). Scanner + apply are transport-neutral; wire them up in your consumer.
- Identity, UCAN, or MLS. `SignatureProvider` is the interface.
- Any Tauri or async framework opinion beyond what `rusqlite` already implies (blocking I/O).

## rusqlite version contract

`db.with_connection(|connection| { ... })` is exposed behind the `raw-connection` feature (default off). When enabled, all consumers of `haex-crdt` in one dependency tree must resolve to the same `rusqlite` version this crate pins; otherwise `Connection`'s `ToSql`/`FromSql` types belong to different crate instances and cannot be passed through the callback. See plan §6.

## Usage scope

A `Database` handle owns one `rusqlite::Connection` behind an internal `Mutex`. The intended shape is **one `Database` per DB file per process**, shared across threads / async tasks via `Database.clone()` — the internal `Arc` makes clones cheap and every clone routes through the same lock.

Opening the same DB file from **two processes** is rejected. As the very first step of `Database::open`, an fs2-backed advisory lock is acquired on `<path>.lock`; a second process (or a second in-process `Database::open` while a live handle still holds the lock) fails immediately with `VaultAlreadyOpenElsewhere`. Different DB files remain independently openable, and the OS releases the lock on process exit so a crash cannot strand a database.

## Trust contract for `NoopSignatureProvider`

`NoopSignatureProvider` performs **no transport attestation** — it signs with empty payloads and accepts empty incoming signatures. Consumers using it MUST deliver remote changes over an already-authenticated transport. Rejects non-empty incoming signatures it cannot verify, so a downgrade from a real provider on an already-signed vault fails loudly.

## Roadmap

Extracted from [plan §5](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md). Each slice ends with `cargo test` green.

- [x] **Batch A** — trait foundation, HLC service, transformers, error types (this slice).
- [x] **Batch B** — trigger installer, scanner, cleanup ported and trimmed to the CRDT-generic surface.
- [x] **Batch C** — `database/*` core (read shell, write path with `PostWriteHook`, trigger bootstrap) ported.
- [x] **Batch D** — migrations engine with two journals (`haex_crdt_migrations_no_sync` crate-owned, `haex_app_migrations_no_sync` consumer-owned) and SHA-256 drift detection.
- [x] **Batch E** — apply pipeline with all-or-nothing signature preflight per plan §4.2.
- [x] **Batch F** — public `Database` facade (`src/database/`) tying `DeviceIdProvider`, `SignatureProvider`, `MigrationSource` and the SQLCipher key together, with `install_crdt` backfill contract per plan §6.
- [x] **Batch F.5** — port haex-vault's `vault_lock.rs` to `src/db/lock.rs`; wired into `Database::open` as the first step so cross-process (and in-process concurrent) opens fail fast with `VaultAlreadyOpenElsewhere`.
- [x] **Batch G** — plan §8 end-to-end acceptance test in `tests/end_to_end.rs`: two `Database`s on two SQLCipher files, backfill on A, plain install on B, local write → scan → apply → readback with device-id contract enforced. Gated behind `raw-connection` since the local write goes through `with_connection`. The true "consumable from a tagged git commit" check lands with the `v0.1.0` tag in Batch H.
- [x] **Batch H** — dual-licensed MIT OR Apache-2.0, CI on GitHub Actions (fmt + clippy + tests on base and `raw-connection` feature configs, plus the plan §8 acceptance test), `v0.1.0` tag on the merge commit.

## Sequence deviation from plan §7

The [extraction plan §7](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md) proposes a three-step sequence:

1. Introduce traits inside `haex-vault` first (in-place refactor, no new repo).
2. Cut a sub-crate under a `haex-vault` Cargo workspace.
3. Move the sub-crate to a standalone repo, preserving git history via `git filter-repo`.

This repository skipped Steps 1 and 2 by explicit operator direction. Tradeoff: no git history from `haex-vault` (clean-start), no continuous-integration safety net during the extraction (haex-vault's tests aren't re-running against the moving code), and trait shapes are validated by the ported tests here rather than by the existing haex-vault test suite. Trait ergonomics may need adjustment when `haex-vault` and `holzi` first integrate.

## License

Dual-licensed under either of

- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)

at your option. Redistributors must comply with the applicable license terms, including retaining applicable copyright, patent, trademark, and attribution notices and adding prominent notices to modified files as required by Apache-2.0. `haex-vault` did not have a `LICENSE` file at extraction time (plan §9 called this a to-be-settled item); this crate settles it here.

### Contributions

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual-licensed as above, without any additional terms or conditions.
