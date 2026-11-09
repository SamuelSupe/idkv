# HTTP API and data contracts — 1.0

Base URL for tenant data: `/v1/tenants/{tenant_uuid}`. All examples assume Bearer authentication and Content-Type: application/json. The [deployment guide](deployment.md) covers installation and credentials.

`GET /openapi.json` serves OpenAPI 3.1, generated once for the router's configured limits. `tenant-kv openapi` exports the default-limit contract; [openapi.json](../openapi.json) is the release copy. Use the schema for exact request and response shapes.

Synchronous success returns 200; remote backup submissions return 202. Errors have the form:

```json
{"error":{"code":"INVALID_ARGUMENT","message":"..."}}
```

The server's X-TKV-History header comes from internal registration/replication state, never from a user's document. Do not infer commit history from application JSON.

<a id="接口"></a>
## Routes and authorization

Here **T** means /v1/tenants/{tenant} and **C** means T/collections/{collection}.

| Method and path | Behavior |
|---|---|
| POST /v1/tenants | Global admin: create with {"name":"customer-a"} |
| GET /v1/tenants | Global admin: list active tenants |
| GET T | Tenant lifecycle, quota, open state, failures and statistics |
| PATCH T | Global admin: update memory_bytes/disk_bytes after drain and validation |
| DELETE T | Global admin: delete; UUID remains permanently non-reusable |
| POST T/close | Admin: drain and close; active long-lived handles may cause BUSY |
| GET / PUT T/retention | Admin: status / {"retention_seconds":3600} |
| POST T/retry-context | Writable principal: obtain a persistent retry token |
| POST T/dsl | Native operations through the [DSL](dsl.md) |
| POST / GET T/collections | Create / list collections |
| DELETE C | Remove catalog atomically, reclaim contents in background |
| PUT C/documents | {"document":{"_key":"a","owner":"platform"},"if_rev":0} |
| GET C/documents/{key} | Read document; NOT_FOUND if absent |
| DELETE C/documents/{key} | Optional if_rev and write_options |
| POST C/batch | Atomic Put/Delete/Check operations |
| POST C/multi_get | {"keys":["a","missing","a"]}; ordered results, null on miss |
| POST T/read_batch | Same-snapshot Get/MultiGet/Query across collections |
| POST / GET C/indexes | Build an index / list BUILDING, READY or FAILED |
| GET / DELETE C/indexes/{index} | Inspect / delete an index |
| POST / GET C/lookups | Define / list named lookups |
| DELETE C/lookups/{lookup} | Delete a lookup definition |
| POST C/lookups/{lookup}/execute | {"inputs":[{"ip":"10.0.0.1"}]} |
| POST C/query | Query, pagination, plan and measured read statistics |
| POST C/explain | Plan only |
| DELETE T/cursors/{cursor} | Release a paging snapshot early |
| GET T/context | Current history, applied sequence and write role |
| GET / PUT T/resources/{resource} | Inspect / publish an immutable dataset |
| POST T/resources/{resource}/execute | Run the published lookup at a version |
| POST T/resources/{resource}/read | Version-bound Get/MultiGet/Query composition |
| POST T/resources/{resource}/query | Query a published version and continue its cursor |
| POST T/checkpoint | Flush and produce a durable checkpoint |
| POST T/backup | Create local backup; returns {"backup":"UUID"} |
| DELETE T/backup/{backup} | Delete this tenant's local backup |
| POST T/restore | Restore this tenant's local {"backup":"UUID"} |
| GET / POST T/remote-backups | List / asynchronously create remote backup |
| POST T/remote-backups/{backup}/verify | Submit SHA-256 verification |
| POST T/remote-backups/{backup}/restore | Submit verified restore |
| POST T/remote-backups/{backup}/delete | Explicitly delete remote metadata, including bad manifests |
| POST T/remote-backups/prune | {"keep_last":7}; reference-aware pruning |
| GET T/backup-jobs | List persistent jobs |
| GET / DELETE T/backup-jobs/{job} | Inspect / forget a terminal job and staging |
| POST T/backup-jobs/{job}/retry | Retry eligible failed create/verify/prune jobs, not restore |
| POST T/check | Verify current SST contents, ordering and checksums |
| POST T/compact | Flush and perform a full compaction |
| POST T/replication/wait | Wait for {"epoch":"UUID","seq":123,"timeout_ms":5000} |

Public probes: GET /health, /ready, /ready/write, /v1/capabilities, /openapi.json. Readiness returns 503 while draining or when the role cannot serve the requested class.

Global admin telemetry: GET /v1/metrics (JSON), GET /metrics (Prometheus; 404 when disabled). Subject migration progress is available as identity_subjects in T/retention even without a retention policy; its durable counters differ from the locally cached status/Prometheus observations.

Roles:

- **read**: document/query/lookup/identity reads and cursor release.
- **write**: read plus ordinary document writes and retry context.
- **identity_write / identity_correct**: managed temporal writes; manual corrections require identity_correct or admin.
- **admin**: tenant management, indexes, datasets and backups. A global admin has tenant:null and can create/delete tenants and change quotas.
- **replication**: dedicated global credential for peer status/log/checkpoint and validated promotion; no ordinary business API, fence, switchover or emergency promotion.

Tokens have a stable subject. Rotate credentials without changing subject when existing retries must remain attributable to the same principal. The trusted embedded Store is a management interface; do not expose it directly to untrusted plugins.

## Replication management

Global admin routes are GET /v1/replication/status and POST /v1/replication/{fence,promote,follow,switchover,force-promote}. Switchover is initiated on the current primary. Forced promotion requires explicit source identity, external fencing, and acceptance of data loss. Internal log/snapshot routes and exact request schemas are in OpenAPI.

Ordinary restore/import is rejected on replication-enabled roots. Use the [replication procedures](replication.md) and rebuild the old primary after emergency promotion. Replication is asynchronous and is not a quorum durability acknowledgement.

<a id="文档与写入"></a>
## Documents, conditional writes and retries

PUT replaces the whole document. Missing _key generates a UUID string; explicit keys are 1–512 UTF-8 bytes and unique within a collection. Input cannot supply _rev; output _rev is the sequence that last modified the document.

| if_rev | Meaning |
|---|---|
| Omitted | Unconditional replacement or deletion |
| 0 | Require absence |
| Positive | Require that revision and the original write_options.expected_history |

Use the history from the original read. A sequence alone is unsafe after restore or failover.

```json
{
  "document":{"_key":"gateway","owner":"platform"},
  "if_rev":123,
  "write_options":{
    "expected_history":"original-read-history",
    "operation_id":"update-gateway-0001"
  }
}
```

Stable operation_id receipts support safe retry of ordinary Put/Delete/Batch and DSL writes. Persist the original request, expected history and operation ID before sending. Once retention is enabled, obtain retry_token before first submission and save/reuse it too; never refresh it to blindly repeat an uncertain operation. Same operation ID with different contents returns OPERATION_CONFLICT.

Deleting an absent key is an idempotent success unless a condition fails. A batch belongs to one tenant and one collection. Its Put/Delete operations see preceding modifications, including index uniqueness; all mutations share one commit. Check operations are evaluated before mutations, do not change document revisions, and do not add Put/Delete results. Cross-collection **reads** use read_batch; cross-collection writes are not provided.

Numeric values preserve i64, u64 and finite f64 semantics: 1 differs from 1.0, and strings are not coerced. Missing differs from null. Objects/arrays may be stored and projected, but index matching fields and comparison operands must be scalars.

## Read controls and consistency

Supported read routes accept:

| Header | Meaning |
|---|---|
| X-TKV-Read-Timeout-Ms | Shared deadline, clamped to server query_ms |
| X-TKV-Max-Scan | Shared physical scan limit, clamped to max_scan |
| X-TKV-Allow-Full-Scan | false rejects collection-scan execution; explain can still show the plan |
| X-TKV-Min-History + X-TKV-Min-Seq | Read at least a confirmed position in that history |

Successful supported reads acknowledge X-TKV-Read-Controls: 1. Waiting and execution share the request deadline; version/tombstone visits count against scans. Minimum-position waiting is bounded to five seconds. Without a minimum, a replica may serve lagging local data.

Read controls are rejected for DSL writes. Retry-context is a write, not a read-only consistency endpoint. See [complete read-control semantics](read-controls.md) for cancellation, client routing and deadline behavior.

<a id="索引和-lookup"></a>
## Indexes and batch lookup

```json
{
  "name":"ip",
  "fields":[{"field":"/ip","order":"asc"}],
  "unique":true,
  "include":["/owner","/service"]
}
```

An index supports up to 8 matching fields and 32 covering fields; a collection supports up to 16 indexes. Unique indexes are sparse: all matching fields must exist and be non-null. Non-unique indexes include missing/null and can support complete ordering.

Creation waits for READY or failure while concurrent writes continue. Management drain can interrupt a build with BUSY; its durable BUILDING state can resume. Backpressure admission during the build waits at most five seconds per admission attempt. Inspect status before deciding whether to retry creation.

```json
{"name":"owners","index":"ip","select":["/owner","/service"],"mode":"all","max_matches":100}
```

Lookup inputs have the same nested structure as documents and must supply all matching paths. Missing input, or null against a sparse unique index, yields no match. Results have an ordered matches array: one list per input, [] for misses; duplicate inputs remain duplicated. Each call uses one snapshot.

Non-unique matches are ordered by _key ASC. mode:first returns the first match; mode:all fails the entire call with MATCH_LIMIT_EXCEEDED above max_matches (default 100, range 1–1000), rather than silently truncating. Only selected fields are returned; covering fields avoid document fetches. Multi-range lookups reuse SST cursors and decoded blocks but still charge held memory.

<a id="条件查询与分页"></a>
## Query planning and pagination

```json
{
  "filter":{"and":[
    {"field":"/env","op":"eq","value":"prod"},
    {"field":"/score","op":"gte","value":80}
  ]},
  "sort":[{"field":"/score","order":"desc"}],
  "select":["/_key","/owner"],
  "limit":100
}
```

Filters: eq, ne, in, gt, gte, lt, lte, exists, and/or/not. exists takes a boolean; eq null matches explicit null only, while ne can also match missing. Ranges compare the same scalar type. Mixed-type ordering is missing, null, bool, i64, u64, f64, string; sorting objects/arrays is invalid.

Default ordering is _key ASC; an explicit sort gets _key ASC as a unique tie-breaker if absent. Omitted/empty select returns full documents. Plans can use primary-key string bounds, indexes, residual filters or budgeted scans. Equality/IN index prefixes expand to at most 256 ranges and 1 MiB of encoded prefixes; larger expansions use a shorter prefix or scan, preserving the full predicate. Sparse unique indexes are used only when existence and non-nullness are proven for all fields.

Queries without index ordering use bounded Top-K. Multi-range ordering is currently conservative and may need extra_sort. No deep offset, external sort, index intersection, joins or aggregation is provided. explain reports the plan; query.stats reports actual blocks, decompression, cache hits, document reads and physical records.

Continue with the same query, filters, sort, selection and limit plus cursor. Cursors bind tenant, collection, principal, runtime generation, replication history and snapshot. Default lifetime is **60 seconds from the first page**, without renewal. Restart/restore/history changes expire them. Successful continuation consumes the old token; concurrent reuse has at most one success. Failed execution does not consume a valid token.

Per tenant, persistent cursors can consume at most half of max_snapshots; one principal can consume at most a quarter (rounded, at least one). Defaults are 128/64. max_snapshots=1 disables persistent paging. This is a shared quota, not a fairness guarantee. Release cursors early when finished.

## Temporal identity and subjects

| Method and path under T/identity | Purpose |
|---|---|
| POST /initialize | Admin: initialize managed identity collection |
| POST /write | Create/update entities, bind/rebind/unbind/replace identifiers |
| POST /resolve | One identifier at valid_at and optional known_at |
| POST /resolve_many | Ordered batch; one snapshot, knowledge version and shared budget |
| POST /ids | SubjectIdentifiersRequest: reverse subject identifiers, paged |
| POST /history | SubjectHistoryRequest: identity change events, paged |
| POST /timeline | Inspect an identifier timeline |
| GET /entities/{entity} | Inspect an entity |
| GET /operations/{operation} | Inspect an operation receipt |

known_at is latest or a durable knowledge UUID; valid_at is business time. resolve_many defaults to at most 1000 items, preserves duplicates, and fails atomically on any error. ids/history return coverage and paging metadata; legacy migration may make ids return INDEX_NOT_READY. Missing legacy audit events are not invented.

The identity_temporal collection is managed: direct CRUD, DSL writes and DDL return MANAGED_COLLECTION. Read permissions still apply. Expanded event/link/pointer/TTL records count toward atomic limits: typical same-subject new bindings fit 199 items without retention or 110 with retention at the default 1000-record limit, with other budgets potentially binding earlier.

Detailed schemas, correction authorization, coverage and history boundaries are in [identity.md](identity.md) and OpenAPI. Identity read commands also exist in the [DSL](dsl.md).

<a id="错误与重试"></a>
## Errors and recovery

| Code | HTTP | Action |
|---|---:|---|
| UNAUTHENTICATED / FORBIDDEN | 401 / 403 | Fix credentials or tenant/role scope |
| INVALID_ARGUMENT | 400 | Fix input; DSL syntax errors may include location |
| NOT_FOUND | 404 | Resource absent; DSL get instead returns null |
| HISTORY_REQUIRED / RETRY_TOKEN_REQUIRED | 428 | Supply original history / obtain first-submission context |
| REVISION_CONFLICT / UNIQUE_CONFLICT / HISTORY_CONFLICT | 409 | Read current context and make a new decision |
| BUSY / INDEX_NOT_READY / CONFLICT | 409 | Allow drain/build/initialization to progress |
| READ_ONLY / REPLICA_LAG / REPLICATION_CONFLICT | 409 | Use the writable node / wait / check source history |
| CANCELLED | 409 | Check cancellation cause; checkpoint copying can stop for source WAL recovery |
| CURSOR_EXPIRED / LOG_GONE / SNAPSHOT_EXPIRED | 410 | Restart paging / replica initialization |
| IDEMPOTENCY_EXPIRED / VERSION_UNAVAILABLE | 410 | Do not refresh credentials to repeat an uncertain operation |
| RESOURCE_EXHAUSTED / MATCH_LIMIT_EXCEEDED | 429 | Reduce scope, back off or adjust a measured resource limit |
| DEADLINE_EXCEEDED | 408 | Reduce scope or inspect backlog |
| SCAN_NOT_ALLOWED | 422 | Add a usable index or explicitly allow a budgeted full scan |
| CORRUPTION / IO_ERROR / COMMIT_UNKNOWN | 500 | Isolate/diagnose; uncertain writes may already exist |
| UNAVAILABLE / RECOVERY_REQUIRED / REPLICATION_UNAVAILABLE | 503 | Inspect state, recover or reconnect; no blind overwrite |
| RESPONSE_UNAVAILABLE | 503 | Operation already ran; use the original safe retry identity |

Backup restore may have published data even if cleanup remains pending; inspect job/tenant state before repeating it. Local backup publication can return BACKUP_COMMIT_UNKNOWN with the backup UUID. Unknown lifecycle publication outcomes require reopening Store to resolve the durable journal.

A diagnostic read-only engine after failed WAL recovery refuses ordinary business reads with RECOVERY_REQUIRED; it does not turn potentially missing acknowledged values into normal 404 responses. Offline repair requires explicit data-loss acceptance and archives the original database.
