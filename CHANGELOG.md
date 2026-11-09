# Changelog

## 1.0.1 — 2026-11-09

- Configurable WAL group commit with immediate sync after idle periods and early wakeup under contention. Set `wal_group_window_us` to `0` to skip deliberate collection time.
- Encode WAL frames outside the publication lock and streamline unique index maintenance for a single mutation with revision checks.
- Negotiate binary WAL pages with weighted `Accept` and keep JSON fallback for 1.0.0 peers. Verify cursor padding and enforce the shared response size limit.
- Keep in-flight binary frames charged to global memory after handing them to HTTP. Release tenant charges at handoff so a slow peer consumes less of the tenant's write budget.
- Add a quota-boundary regression with a negative control and document the WAL window benchmarks.

See the [1.0.1 release notes](docs/releases/v1.0.1.md) for distribution, compatibility and validation.

## 1.0.0 — 2026-11-09

First stable release of the document, lookup, published-dataset, and temporal identity APIs.

### Included

- Embedded Rust API, authenticated HTTP /v1, generated OpenAPI 3.1.
- Per-tenant WAL/LSM/SST storage, MVCC snapshots, conditional atomic batches.
- Compound, unique and covering indexes; batch lookups; immutable published datasets.
- Parameterized DSL and prepared statements, including resolve/resolvemany/ids/history.
- Resumable subject index migration and retention GC.
- Asynchronous primary/replica replication, fencing, planned switchover and explicit emergency promotion.
- Local/S3/fs backups, offline import, archived WAL prefix repair.
- Resource budgets, Prometheus metrics and OTLP telemetry.

### Final maintenance fixes

- Reuse SST cursors across subject migration and link GC batches.
- Increase history GC to at most 64 physical records per step with a 10ms cooperative budget.
- Preserve physical paging, predecessor baselines, atomic progress and retry limits.
- Add million-row, multi-tenant and SIGKILL/restart qualification.

### Distribution and compatibility

- Static Linux ARM64 and x86_64 archives, three production tools and SHA-256 checksums.
- Rust 1.99.1; package and commands remain tenant-kv, Rust imports tenant_kv.
- Registry 6, replication protocol 4, WAL/SST 1. Upgrade both peers and offline tools together.
- English README, API, DSL, deployment and release guides.

See [release notes](docs/releases/v1.0.0.md). Earlier 0.x development history is preserved in [dated reports](docs/README.md) and Git commits.
