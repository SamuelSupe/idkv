# Deploy idkv 1.0

This guide covers the Linux ARM64 and x86_64 binary releases. For Rust embedding use the [README](../README.md#embed-in-rust). For every HTTP request shape use [the API guide](api.md) and [OpenAPI](../openapi.json).

## Install and verify

Download from [v1.0.2](https://github.com/SamuelSupe/idkv/releases/tag/v1.0.2). Archives use static musl linking and include:

```text
idkv-v1.0.2-TARGET/
  bin/tenant-kv
  bin/tenant-kv-restore
  bin/tenant-kv-repair
  BUILD-INFO.json
  RUSTC.txt
  openapi.json
  README.md
  LICENSE
  docs/
  examples/
  deploy/tenant-kv.service
  scripts/
```

Choose aarch64-unknown-linux-musl for ARM64 or x86_64-unknown-linux-musl for amd64. These are Linux binaries, not macOS executables. Inspect BUILD-INFO.json for the exact source commit and target.

```sh
VERSION=1.0.2
case "$(uname -m)" in
  aarch64|arm64) TARGET=aarch64-unknown-linux-musl ;;
  x86_64|amd64) TARGET=x86_64-unknown-linux-musl ;;
  *) echo 'Unsupported CPU' >&2; exit 1 ;;
esac
BASE="https://github.com/SamuelSupe/idkv/releases/download/v$VERSION"
ARCHIVE="idkv-v$VERSION-$TARGET.tar.gz"
curl -fLO "$BASE/$ARCHIVE"
curl -fLO "$BASE/SHA256SUMS"
grep "  $ARCHIVE$" SHA256SUMS | sha256sum -c -
tar -xzf "$ARCHIVE"
cd "idkv-v$VERSION-$TARGET"
./bin/tenant-kv --version
./bin/tenant-kv-restore --version
./bin/tenant-kv-repair --version
```

SHA256SUMS detects download changes relative to that release file; it is not a detached signature or an independent trust root.

Use a local filesystem with working exclusive locks, atomic rename, and file/directory fsync. Give the service its own writable root. A tenant UUID maps to a directory, and opening the same root/tenant twice is rejected. Do not copy a live directory as if it were a consistent backup; use checkpoints/backups.

## Credentials

The HTTP server requires --auth. Secrets should be generated randomly, kept outside the repository, and readable only by the service user. The [quick start](../README.md#quick-start) creates an administrator token and its SHA-256 auth entry without printing the secret. Alternatively:

```sh
./bin/tenant-kv token-hash < token.txt
```

An auth entry is:

```json
{"tokens":[
  {"sha256":"REPLACE_WITH_SHA256_HEX","subject":"ops-admin","tenant":null,"role":"admin"},
  {"sha256":"REPLACE_WITH_ANOTHER_HASH","subject":"ingest-service","tenant":"TENANT_UUID","role":"write"}
]}
```

The placeholders are not usable credentials. Assign read/write/identity roles and a tenant UUID to application tokens; reserve tenant:null/admin for management. A stable subject identifies a principal across token rotation. Auth configuration is loaded at startup; restart to apply changes. Peer replication token-file rotation has separate rules in [replication.md](replication.md).

## Configuration

Omitted Options fields use defaults; unknown fields are rejected. Copy [examples/server.json](../examples/server.json) and adjust budgets to the actual host:

```sh
./bin/tenant-kv serve --root ./data --auth ./auth.json \
  --config ./examples/server.json --listen 127.0.0.1:8080
```

| Default | Value |
|---|---:|
| Global / per-tenant engine memory budget | 4 GiB / 512 MiB |
| Per-tenant disk quota | 100 GiB |
| Per-tenant cache / memtable threshold | 128 MiB / 16 MiB |
| I/O workers / maximum maintenance workers | 4 / 4 |
| Concurrent WAL group collection window | 1000 µs |
| Input batch / response bytes | 16 MiB / 16 MiB |
| Expanded atomic records / read keys | 1000 / 1000 |
| Physical scan / query deadline | 100,000 / 5 seconds |
| Snapshot lifetime | 60 seconds |
| HTTP connections / ordinary executing requests | 1024 / 64 |
| Request-header and keep-alive idle wait | 5 seconds |

Set `wal_group_window_us` to an integer from 0 to 10,000. A value of 0 skips the deliberate collection delay. Writes still wait for WAL fsync before acknowledgement, and pending transactions can share an fsync. A single write after an idle period starts fsync immediately at any setting.

Budgets are not process RSS: allow extra capacity for allocation overhead, networking, TLS/exporters and caller-held responses. Set a cgroup limit above the engine budget and observe both. Use [operations.md](operations.md) for validation bounds and detailed memory/I/O accounting.

Keep disk space for WAL, compaction output, checkpoints, restore staging and backups, not only live SSTs. Local backup totals are bounded separately and have a default 1 GiB cap per tenant. Remote staging can need more than one checkpoint-sized copy. Full compaction, large indexes and bootstrap have workload-dependent I/O cost.

## Run under systemd

From a verified extracted archive:

```sh
sudo useradd --system --home /var/lib/idkv --shell /usr/sbin/nologin idkv
sudo install -d -o root -g root /opt/idkv
sudo cp -a bin docs examples scripts deploy README.md LICENSE openapi.json BUILD-INFO.json RUSTC.txt /opt/idkv/
sudo install -d -m 0750 -o root -g idkv /etc/idkv
sudo install -m 0640 -o root -g idkv auth.json /etc/idkv/auth.json
sudo install -m 0640 -o root -g idkv examples/server.json /etc/idkv/server.json
sudo install -m 0644 deploy/tenant-kv.service /etc/systemd/system/tenant-kv.service
sudo systemctl daemon-reload
sudo systemctl enable --now tenant-kv
systemctl status tenant-kv
journalctl -u tenant-kv -f
```

The unit creates /var/lib/idkv, runs as idkv, binds loopback, restricts writes to the state directory and allows 40 seconds for shutdown. Use a service override for extra backup paths, configuration flags or a different listen address. Do not put remote-backup staging in the read-only installation tree.

## Network and readiness

The built-in listener speaks HTTP. Put it behind your TLS reverse proxy/ingress for remote use; do not expose plaintext Bearer tokens over untrusted networks. Keep authorization, content types, methods, streaming/cancellation and request bodies intact. Align proxy body/time limits with your configured budgets; synchronous index builds or maintenance can outlast a normal read deadline.

```sh
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/ready
curl -fsS http://127.0.0.1:8080/ready/write
curl -fsS http://127.0.0.1:8080/v1/capabilities
```

/health is liveness, /ready is read readiness, /ready/write requires the writable role. They are unauthenticated. A replica can be ready to read while lagging. Use minimum history/sequence read controls for read-after-write guarantees.

On SIGTERM/SIGINT, readiness withdraws for two seconds before admission drains and the listener closes. Existing requests receive a bounded drain period. Configure load-balancer health checks accordingly; a process forced to exit during a write can leave its outcome unknown.

## Backups and recovery

Local backups use POST T/backup and DELETE T/backup/{backup}. For remote fs/S3 backups add --backup-config, using [fs](../examples/backup-fs.json) or [S3](../examples/backup-s3.json). Edit storage paths, credentials, staging and byte limits first. For the supplied systemd unit, choose a writable path under /var/lib/idkv or explicitly allow the destination.

If the configured remote backup manager fails to start, data service can still become ready. Remote backup routes return HTTP 503 and `tkv_backup_startup_degraded` reports the failure. Repair the backup state or configuration and restart.

Remote submissions return 202 jobs; poll until completed. Failure is not permission to blindly repeat a restore: publication and cleanup have distinct outcomes. Test restores into a separate root.

```sh
# Offline import into a root where TENANT_UUID is not registered.
./bin/tenant-kv-restore NEW_ROOT BACKUP_CONFIG TENANT_UUID BACKUP_UUID
```

Recovery reports `CORRUPTION` and preserves the original WAL when a frame header or payload is incomplete. Rebuild a damaged replica from the authoritative primary. For offline repair, preserve the original data and determine a valid repair offset before using:

```sh
./bin/tenant-kv-repair ROOT TENANT_UUID WAL_UUID \
  --at VERIFIED_OFFSET --archive NEW_ARCHIVE_DIRECTORY --accept-data-loss \
  --options /etc/idkv/server.json
```

This is an offline, explicitly lossy administrative operation. It archives the complete original database, changes history/replication epoch, and requires replicas to reinitialize. Inspect its JSON and stderr warnings; a published result with cleanup/report warnings must not be blindly repaired again. See [operations](operations.md) and the dated [repair review](reviewc3b-20261108.md).

## Primary and replica

Use two independent roots and the same cluster UUID. Start with the [primary](../examples/replication-primary.json) and [replica](../examples/replication-replica.json) configurations; set real peer URLs and a protected peer-token file before passing --replication-config. Remote peer URLs require HTTPS; HTTP is permitted only for loopback development. Both nodes need the matching replication auth entry.

Replication is asynchronous: local write success does not mean the replica has it. Planned switchover durably fences the old primary and catches up its successor. Emergency promotion requires external isolation of the old primary and explicit data-loss acceptance. There is no automatic election or split-brain fencing service. Follow the complete [replication runbook](replication.md), including old-primary rebuild.

## Observe

Prometheus is enabled by default at /metrics and requires a global admin token. GET /v1/metrics provides JSON resource statistics. OTLP logs/traces are independently enabled with --telemetry-config; [examples/telemetry.json](../examples/telemetry.json) enables both and expects a collector.

Monitor resource rejection/backpressure, tenant maintenance_error, disk headroom, memory pressure, replication lag/reinitializations, failed backup jobs and foreground p99. Subject-index migration progress is available through GET T/retention as identity_subjects even without retention enabled.

Quotas and task-level fairness do not provide a hard disk latency SLA. The [million-row qualification](subject-maintenance-scale-validation-20261109.md) still observed light-tenant p99 growth during GC.

## Upgrade from 1.0.0 or 1.0.1

Verify a backup and install the 1.0.2 server and offline tools together on each node. Registry 6, replication protocol 4 and WAL/SST format 1 support a rolling upgrade within 1.0. Peers on 1.0.0 exchange JSON log pages. The `wal_group_window_us` option is supported from 1.0.1. Check replication lag, tenant errors and backup readiness after each restart.

An incomplete WAL frame now requires intervention before the affected tenant can participate in a planned switchover. Preserve the original files and follow the [replication runbook](replication.md) for replica rebuild or the [offline repair procedure](operations.md#离线-wal-前缀修复) for primary recovery.

## Upgrade to 1.0

1. Take and verify a backup; keep the previous binaries and a recoverable pre-upgrade root.
2. Stop new writes, drain clients and stop both peers.
3. Install the current server and both offline tools on each node.
4. Ensure writable metadata space, additional reverse-index/GC capacity, and compatible limits on both nodes.
5. Start primary then replica; check capabilities, role, tenant failures, migration progress and replication catch-up before reopening writes.

The 1.0 series uses registry **6**, replication **4**, and WAL/SST **1**. Older registries upgrade before serving. To roll back an upgraded pre-1.0 root, restore the pre-upgrade root and its matching binaries and configuration. The DSL grammar has its own version.

Identity audit/link/TTL expansion can reduce effective batch capacity to 199 or 110 typical bindings under the default 1000-record limit. Test actual requests and coordinate any limit increase across peers. During backfill ids returns INDEX_NOT_READY; completed identifiers may already be reclaimed safely by history GC. Earlier audit events that did not exist cannot be reconstructed.
