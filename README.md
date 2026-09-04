# haex-crdt

SQLite + SQLCipher storage with column-level LWW CRDT sync (uhlc-based Hybrid Logical Clocks). Extracted from `haex-vault` so both `haex-vault` and `holzi` can consume it as a Rust crate dependency.

**Status**: pre-alpha. First slice landed: trait foundation, HLC service, SQL transformers. Trigger installer, scanner, cleanup, migration engine, apply pipeline, and public `Store` facade are pending — see [Roadmap](#roadmap).

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

`Store::with_connection(&rusqlite::Connection)` will be exposed behind the `raw-connection` feature (default off). When enabled, all consumers of `haex-crdt` in one dependency tree must resolve to the same `rusqlite` version this crate pins; otherwise `Connection`'s `ToSql`/`FromSql` types belong to different crate instances and cannot be passed through the callback. See plan §6.

## Trust contract for `NoopSignatureProvider`

`NoopSignatureProvider` performs **no transport attestation** — it signs with empty payloads and accepts empty incoming signatures. Consumers using it MUST deliver remote changes over an already-authenticated transport. Rejects non-empty incoming signatures it cannot verify, so a downgrade from a real provider on an already-signed vault fails loudly.

## Roadmap

Extracted from [plan §5](../holzi/docs/plans/2026-09-04-haex-crdt-extraction-plan.md). Each slice ends with `cargo test` green.

- [x] **Batch A** — trait foundation, HLC service, transformers, error types (this slice).
- [ ] **Batch B** — port `trigger.rs`, `scanner.rs`, `cleanup.rs` from haex-vault. Route `column_sig` call sites through `SignatureProvider`.
- [ ] **Batch C** — port `database/{connection_context, row, stats, constants, paths, listing, maintenance, import_delete, core/*, vault_lock}`. Drop Tauri command shims.
- [ ] **Batch D** — port migrations engine. Swap `tauri::path::BaseDirectory` lookup for `MigrationSource`. Split into two journals (`haex_crdt_migrations` for crate-owned, `haex_app_migrations` for consumer-owned).
- [ ] **Batch E** — port `crdt/commands/apply/*`. Strip `#[tauri::command]` shims. Route `registry_row_sig` policy through `SignatureProvider::on_before_apply`. Implement all-or-nothing apply transaction per plan §4.2.
- [ ] **Batch F** — build public `Store` facade in `src/store.rs` per plan §6.
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
