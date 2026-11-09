# idkv documentation

Stable release: **1.0.1**. HTTP routes use /v1; application, DSL grammar, storage, and replication versions are independent.

## English guides

| Guide | Scope |
|---|---|
| [Deployment](deployment.md) | Linux install, auth, systemd, resource planning, upgrades |
| [HTTP API](api.md) | Routes, read/write semantics, errors and retries |
| [DSL](dsl.md) | All commands, parameters, identity queries, prepared statements |
| [1.0.1 release](releases/v1.0.1.md) | Compatibility, distribution, validation and limits |
| [1.0.0 release](releases/v1.0.0.md) | First stable release and its original qualification |
| [Release engineering](releasing.md) | Rebuild, package and verify artifacts |
| [OpenAPI](../openapi.json) | OpenAPI 3.1 machine-readable contract |

## Detailed references

These references currently contain Chinese text:

| Reference | Scope |
|---|---|
| [Operations](operations.md) | Options, quotas, memory, I/O and repair |
| [Identity](identity.md) | Business/knowledge time, batch matching, reverse indexes, history |
| [KV composition](kv-capabilities.md) | Conditional batches, read_batch, published datasets |
| [Client reads](read-controls.md) | Deadlines, cancellation, minimum positions, routing |
| [Middleware](middleware.md) | Stable subjects, retry receipts, upgrades |
| [Replication](replication.md) | Asynchronous pair, fencing, failover |
| [Backup](backup.md) | S3/fs, jobs, import, pruning |
| [Retention](retention.md) | Retry tokens, expiry, historical baselines, GC |
| [Telemetry](telemetry.md) | Prometheus and OTLP |
| [Storage format](format.md) | WAL, SST, manifests, ordering, persistence |

## Engineering evidence

Reports preserve their original versions and environments; a historical drill is not implicitly repeated for every release.

- [Million-row migration and GC](subject-maintenance-scale-validation-20261109.md): two datasets, concurrent writes, SIGKILL/restart and acknowledged-data checks.
- [Subject maintenance](subject-maintenance-validation-20261109.md), [subject queries](subject-queries-validation-20261109.md), [batch resolve](resolve-many-validation-20261109.md).
- [Recovery drill](recovery-drill.md), [parallel qualification](production-qualification.md), [tail latency](tail-latency-optimization.md).
- Other dated review and version reports remain available in this directory and in Git history.
