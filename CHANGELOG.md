# Changelog

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
