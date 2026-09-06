# haex-crdt

SQLite + SQLCipher storage with column-level LWW CRDT sync (uhlc-based Hybrid Logical Clocks). Extracted from `haex-vault` so both `haex-vault` and `holzi` can consume it as a Rust crate dependency.

**Status**: pre-alpha. Trait foundation, HLC service, SQL transformers, trigger installer, scanner, cleanup, migration engine, apply pipeline, public `Database` facade, and cross-process file locking all land. A standalone-git-tag acceptance test remains before the first tagged release — see [Roadmap](#roadmap).

**Ownership**: source lived in `haex-vault`. This repository is the extraction target. Both `haex-vault` and `holzi` will depend on tagged releases here.

## Scope

Provided by this crate:

- Encrypted SQLite storage (SQLCipher).
- A migration engine with two journals — crate-owned CRDT bookkeeping and consumer-owned schema, per [plan §4.3](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md).
- HLC service (uhlc-based Hybrid Logical Clocks with SQLite-persisted state).
- Column-level LWW CRDT infrastructure: schema transformer, trigger installer, scanner, apply pipeline.
- Cleanup / retention utilities for deleted-row logs.
- Pluggable [`DeviceIdProvider`](src/device_id.rs) — consumers supply the durable device UUID.
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
- [x] **Batch D** — migrations engine with two journals (`haex_crdt_migrations` crate-owned, `haex_app_migrations` consumer-owned) and SHA-256 drift detection.
- [x] **Batch E** — apply pipeline with all-or-nothing signature preflight per plan §4.2.
- [x] **Batch F** — public `Database` facade (`src/database/`) tying `DeviceIdProvider`, `SignatureProvider`, `MigrationSource` and the SQLCipher key together, with `install_crdt` backfill contract per plan §6.
- [x] **Batch F.5** — port haex-vault's `vault_lock.rs` to `src/db/lock.rs`; wired into `Database::open` as the first step so cross-process (and in-process concurrent) opens fail fast with `VaultAlreadyOpenElsewhere`.
- [ ] **Batch G** — port relevant integration tests from haex-vault. Add the standalone-git-tag acceptance test (plan §8) as a `tests/` binary.
- [ ] **Batch H** — LICENSE decision (plan §9 defers this), CI, first `v0.1.0` tag.

## Sequence deviation from plan §7

The [extraction plan §7](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md) proposes a three-step sequence:

1. Introduce traits inside `haex-vault` first (in-place refactor, no new repo).
2. Cut a sub-crate under a `haex-vault` Cargo workspace.
3. Move the sub-crate to a standalone repo, preserving git history via `git filter-repo`.

This repository skipped Steps 1 and 2 by explicit operator direction. Tradeoff: no git history from `haex-vault` (clean-start), no continuous-integration safety net during the extraction (haex-vault's tests aren't re-running against the moving code), and trait shapes are validated by the ported tests here rather than by the existing haex-vault test suite. Trait ergonomics may need adjustment when `haex-vault` and `holzi` first integrate.

## License

TBD — see [plan §9](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md). `haex-vault` has no `LICENSE` file at the time of extraction; a license is settled before the first tagged release.
