<div align="center">

# idkv

**Documents, batch lookups, and temporal identity. On your own storage.**

[![Release](https://img.shields.io/badge/release-v1.0.2-2563eb)](https://github.com/SamuelSupe/idkv/releases/tag/v1.0.2)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.99.1-orange)](rust-toolchain.toml)
[![Platforms](https://img.shields.io/badge/Linux-ARM64%20%7C%20x86__64-334155)](https://github.com/SamuelSupe/idkv/releases/tag/v1.0.2)

[Quick start](#quick-start) · [API](docs/api.md) · [DSL](docs/dsl.md) · [Deployment](docs/deployment.md) · [Release notes](docs/releases/v1.0.2.md)

</div>

idkv is a Rust storage engine for tenant-scoped JSON documents, indexed batch enrichment, and identities that change over time. Embed it in a Rust application or run the HTTP server. Both use the same native WAL, LSM, SST, MVCC, index, recovery, and compaction implementation.

There is no external database dependency. Each tenant has its own database directory, lock, WAL, sequence, indexes, and cache. Global memory and I/O budgets bound shared work.

> **1.0.2 is the current stable release.** Linux ARM64 and x86_64 archives include three production executables, configuration examples, the OpenAPI contract, and documentation. Existing package and command names remain `tenant-kv`; Rust imports remain `tenant_kv`.

## What you can build

| Need | idkv provides |
|---|---|
| Application configuration or entity storage | JSON documents, conditional writes, atomic batches, same-snapshot reads |
| Enriching logs or events in batches | Compound and covering indexes, named lookups, ordered results |
| Reproducible reference datasets | Immutable published versions with version-bound reads |
| Resolving an account, device, or address over time | Business-time bindings, durable knowledge versions, `resolve_many` |
| Understanding identity changes | Subject identifier lookup, paged change history, auditable corrections |
| Keeping another node ready | One-primary/one-replica asynchronous replication and explicit switchover |
| Operating a local service | Per-tenant quotas, backups, checks, metrics, traces, and recovery tools |

The DSL is a small interface to native operations:

```text
from assets | where env == "prod" and score >= $min | fields _key, owner | limit 100
lookup assets using owners with $inputs
resolvemany with $request
ids with $request
history with $request
```

It supports parameter binding and prepared statements. It does not execute scripts or provide SQL, joins, or aggregation.

## Quick start

On Linux, choose the archive for your CPU:

| Architecture | Release archive |
|---|---|
| ARM64 / aarch64 | [idkv-v1.0.2-aarch64-unknown-linux-musl.tar.gz](https://github.com/SamuelSupe/idkv/releases/download/v1.0.2/idkv-v1.0.2-aarch64-unknown-linux-musl.tar.gz) |
| x86_64 / amd64 | [idkv-v1.0.2-x86_64-unknown-linux-musl.tar.gz](https://github.com/SamuelSupe/idkv/releases/download/v1.0.2/idkv-v1.0.2-x86_64-unknown-linux-musl.tar.gz) |

Both use static musl linking, so they do not require a particular glibc version. Outbound HTTPS integrations still need the host's CA certificates.

```sh
# Linux ARM64. Use x86_64-unknown-linux-musl on x86_64.
TARGET=aarch64-unknown-linux-musl
BASE=https://github.com/SamuelSupe/idkv/releases/download/v1.0.2
curl -fLO "$BASE/idkv-v1.0.2-$TARGET.tar.gz"
curl -fLO "$BASE/SHA256SUMS"
grep "  idkv-v1.0.2-$TARGET.tar.gz$" SHA256SUMS | sha256sum -c -
tar -xzf "idkv-v1.0.2-$TARGET.tar.gz"
cd "idkv-v1.0.2-$TARGET"
./bin/tenant-kv --version
```

Create a local administrator credential with Python 3. The secret is saved to a file rather than printed:

```sh
umask 077
python3 - <<'PY'
import hashlib, json, pathlib, secrets
token = secrets.token_hex(32)
pathlib.Path("token.txt").write_text(token + "\n")
pathlib.Path("auth.json").write_text(json.dumps({"tokens": [{
    "sha256": hashlib.sha256(token.encode()).hexdigest(),
    "subject": "local-admin", "tenant": None, "role": "admin"
}]}))
PY
./bin/tenant-kv serve --root ./data --auth ./auth.json --listen 127.0.0.1:8080
```

In another terminal in the extracted directory:

```sh
BASE=http://127.0.0.1:8080
TOKEN=$(cat token.txt)
TENANT=$(curl -fsS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"demo"}' \
  "$BASE/v1/tenants" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
API="$BASE/v1/tenants/$TENANT"
curl -fsS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"assets"}' "$API/collections"
curl -fsS -X PUT -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"document":{"_key":"gateway","env":"prod","owner":"platform"},"if_rev":0}' \
  "$API/collections/assets/documents"
curl -fsS -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"statement":"from assets | where env == $env | fields _key, owner","params":{"env":"prod"}}' \
  "$API/dsl"
```

Use [the deployment guide](docs/deployment.md) for systemd, TLS termination, permissions, resource sizing, and upgrades. The data filesystem must support locks, atomic rename, and file/directory fsync.

## Embed in Rust

```toml
[dependencies]
tenant-kv = { git = "https://github.com/SamuelSupe/idkv", tag = "v1.0.2" }
serde_json = "1"
```

```rust,no_run
use serde_json::json;
use tenant_kv::{Options, Result, Store};

fn main() -> Result<()> {
    let store = Store::open("data", Options::default())?;
    let tenant = store.tenant(store.create_tenant("example")?.id)?;
    tenant.create_collection("assets")?;
    let written = tenant.put(
        "assets", json!({"_key": "gateway", "owner": "platform"}), Some(0)
    )?;
    println!("revision: {}, value: {:?}", written.rev, tenant.get("assets", "gateway")?);
    Ok(())
}
```

This example needs a new root; tenant and collection creation are not an idempotent application bootstrap. See [embedded usage](examples/embedded.rs), [read controls](examples/read_controls.rs), and [identity queries](examples/identity_subjects.rs).

## Durability and operating boundaries

- Success acknowledges **local durable WAL**, including expanded index and receipt records. A lost response or `COMMIT_UNKNOWN` requires checking the original operation; use stable operation IDs and history-aware retry conditions.
- Replication is **asynchronous**. Emergency promotion may lose acknowledged writes that did not reach the replica. Fence the old primary externally. There is no quorum or automatic leader election.
- Quotas and task scheduling bound resources. They do not guarantee physical disk bandwidth, IOPS, or tail-latency isolation; sustained maintenance can affect light tenants.
- Queries and atomic batches have byte, scan, time, and memory limits. Identity audit/index records count toward the expanded batch limit.
- Current formats: **registry 6, replication protocol 4, WAL/SST 1**. Older-root upgrades require writable space and block older writers. Upgrade both peers and offline tools together.

## Documentation

| Guide | Contents |
|---|---|
| [Deployment](docs/deployment.md) | Install, authenticate, configure, run, observe, upgrade |
| [HTTP API](docs/api.md) / [OpenAPI 3.1](openapi.json) | Routes, authorization, data types, reads, writes, errors |
| [DSL](docs/dsl.md) | All 11 commands, parameters, prepared statements, pagination |
| [1.0.2 release notes](docs/releases/v1.0.2.md) | Compatibility, assets, validation, known limits |
| [KV composition](docs/kv-capabilities.md) | Cross-collection reads, conditional batches, published datasets |
| [Temporal identity](docs/identity.md) | Valid time, knowledge time, batch resolve, subjects, history |
| [Replication](docs/replication.md) | Pair deployment, fencing, promotion, rebuilding |
| [Backups](docs/backup.md) / [Operations](docs/operations.md) | S3/fs backups, restore, quotas, repair |
| [Documentation index](docs/README.md) | Current guides and dated validation evidence |

Detailed operational references and historical reports currently include Chinese content. The README, API, DSL, deployment, and 1.0 release guides are English.

## Build and validate

Rust **1.99.1** is pinned in [rust-toolchain.toml](rust-toolchain.toml). Unix filesystem semantics are required; Linux is the release platform. Run source commands in a checkout of v1.0.2; binary archives are not complete Rust source checkouts.

```sh
cargo build --locked --release --bin tenant-kv --bin tenant-kv-restore --bin tenant-kv-repair
cargo test --locked --all-features --no-fail-fast -- --test-threads=4
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Rebuild static Linux artifacts with [scripts/build-release.sh](scripts/build-release.sh); see [release engineering](docs/releasing.md). Failpoint instrumentation is excluded from production archives.

The [million-row qualification](docs/subject-maintenance-scale-validation-20261109.md) exercised migration, multi-tenant writes, GC, and process termination/restart. Dated evidence states its actual scope; it is not a universal production certification.

## License

[Apache License 2.0](LICENSE).
