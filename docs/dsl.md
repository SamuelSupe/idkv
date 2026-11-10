# DSL reference — idkv 1.0

The DSL is a parameterized interface to native document queries, batch lookups, writes, and temporal identity reads. It shares the JSON API's authorization, snapshot, durability and resource rules. HTTP, Rust and prepared execution return the corresponding native results.

There are **11 commands**: from, explain, get, mget, lookup, resolve, resolvemany, ids, history, put, delete. Collections, indexes, lookup definitions, datasets and backups are managed through the API. There is no SQL compatibility, join, aggregation, UDF or script execution.

## First request

Create a tenant and assets collection using the [quick start](../README.md#quick-start), then:

```sh
curl -fsS -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"statement":"from assets | where env == $env | fields _key, owner | limit $count","params":{"env":"prod","count":100}}' \
  "http://127.0.0.1:8080/v1/tenants/$TENANT/dsl"
```

For runnable examples:

```sh
cargo run --locked --example dsl -- /var/tmp/idkv-dsl-new
python3 scripts/dsl_smoke.py http://127.0.0.1:8080 token.txt
```

The HTTP smoke creates and deletes its own test tenant; use a test environment and a global administrator credential.

## Command overview

Each line below is a separate request:

```text
put assets {"_key":"gateway","ip":"10.0.0.1","env":"prod","score":95,"owner":"platform"} if_rev 0
get assets "gateway"
mget assets ["gateway","missing","gateway"]
from assets | where env == "prod" and score >= 80 | sort score desc | fields _key, owner | limit 100
explain from assets | where ip == "10.0.0.1" | fields owner
lookup assets using owners with [{"ip":"10.0.0.1"},{"ip":"absent"}]
resolve with $request
resolvemany with $request
ids with $request
history with $request
delete assets "gateway"
```

Create owners and its index through the management API before executing lookup. Put replaces the full document; input cannot supply _rev. if_rev 0 means insert only; an omitted condition is unconditional.

## HTTP envelope and parameters

`POST /v1/tenants/{tenant_uuid}/dsl` accepts:

```json
{
  "statement":"from assets | where score >= $min | sort score desc | fields _key, owner | limit $count",
  "params":{"min":80,"count":100},
  "cursor":null
}
```

Writes can additionally carry write_options. Query results contain documents, cursor, snapshot_seq, plan and stats. Lookup returns matches and stats; writes return WriteResult. get returns JSON null on a miss, unlike document GET's 404; mget preserves order, duplicates and null misses.

Parameters substitute **one complete JSON value**, never source text. Parameter names must match exactly: missing and extra parameters are rejected; a name may be used repeatedly. Collection names, fields, lookup names and operators cannot be parameters. Quotes, pipes, semicolons and keywords inside bound values are data.

JSON does not interpolate nested strings: {"owner":"$name"} stores the literal $name. For a dynamic document, use put assets $document and bind the whole document.

The authorized URL or TenantHandle selects the tenant. There is no use-tenant command. Reads require read permission, put/delete require write; a replica rejects writes with READ_ONLY. Cached syntax never caches an authorization decision.

## Conditional writes and safe retries

```json
{
  "statement":"put assets $document if_rev $revision",
  "params":{"document":{"_key":"gateway","owner":"platform"},"revision":123},
  "write_options":{
    "expected_history":"history-from-original-read",
    "operation_id":"gateway-update-0001"
  }
}
```

Positive if_rev requires the original expected_history; use both, not a sequence alone. The same rule applies to delete assets $key if_rev $revision.

Successful DSL writes wait for the same local durable WAL as native writes. Save the original statement, parameters and write_options for retry. With retention enabled, write_options also needs the first-submission retry_token. Never replace an expired token to blindly re-run an uncertain operation. See [API write semantics](api.md#documents-conditional-writes-and-retries) and [retention](retention.md).

## Grammar

```text
query       := "from" collection ["| where" expression]
               ["| sort" field ["asc" | "desc"] {"," field ["asc" | "desc"]}]
               ["| fields" field {"," field}]
               ["| limit" value]
explain     := "explain" query
get         := "get" collection value
mget        := "mget" collection value
lookup      := "lookup" collection "using" lookup_name "with" value
resolve     := "resolve" "with" value
resolvemany := "resolvemany" "with" value
ids         := "ids" "with" value
history     := "history" "with" value
put         := "put" collection value ["if_rev" value]
delete      := "delete" collection value ["if_rev" value]
value       := JSON_literal | "$" parameter_name
```

Query stages must appear in where → sort → fields → limit order, at most once each; all are optional. Default limit is 100, positive and bounded by max_page_size. Default sort is _key ASC; _key ASC is appended as a unique tie-breaker if not explicitly included. Omitted fields returns full documents.

Keywords are lowercase and case-sensitive. Bare identifiers match [A-Za-z_][A-Za-z0-9_-]*. Collection and lookup names may be JSON strings, including Unicode. JSON uses double quotes. Outside JSON, # starts a line comment. Semicolons, multiple statements, assignments and interpolation are unsupported.

## Fields and expressions

owner.team maps to /owner/team. Use field("/pointer") for special characters, array positions or names that conflict with expression keywords:

```text
from assets | where field("/tags/0") == "prod" | fields field("/a~1b"), field("/设备")
```

/a~1b means the key a/b; ~0 escapes a tilde. The root pointer is unsupported.

| Expression | Meaning |
|---|---|
| score == 80 / score != 80 | Strict typed equality / negation |
| score > 80, >=, <, <= | Same scalar type range comparison |
| env in ["prod","staging"] | Strict membership; right side is an array of scalars |
| exists(owner) | Field is present, including explicit null |
| not exists(owner) | Field is absent |
| owner == null | Explicit null only |
| a == 1 and (b == 2 or not exists(c)) | Boolean composition |

Precedence: comparison/existence, not, and, or. Inequality can match missing fields. Numbers retain i64/u64/finite f64 semantics: 1 differs from 1.0 and "1" is not coerced. Objects and arrays may be stored or projected but not used as scalar comparison operands.

## Temporal identity commands

```json
{
  "statement":"resolvemany with $request",
  "params":{"request":{
    "known_at":"latest",
    "items":[
      {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-09T10:00:00Z"},
      {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-09T11:00:00Z"}
    ]
  }}
}
```

resolve accepts the /identity/resolve body; resolvemany accepts /identity/resolve_many. They share a snapshot and knowledge version per execution, preserve input order and duplicates, and return binding:null on a miss. Any error fails the batch. Neither accepts cursor, write_options or pipelines.

Subject queries:

```json
{"statement":"ids with $request","params":{"request":{"entity_id":"ENTITY_UUID","valid_at":"2026-11-09T10:00:00Z","known_at":"latest","limit":100}}}
```

```json
{"statement":"history with $request","params":{"request":{"entity_id":"ENTITY_UUID","recorded_from":"2026-11-01T00:00:00Z","limit":100}}}
```

ids accepts SubjectIdentifiersRequest, history accepts SubjectHistoryRequest. Both are read-only and omit a collection name. They return native /identity/ids and /identity/history results, including knowledge, coverage, cursor and stats.

For the next subject page, repeat statement and params and put cursor at the **envelope's top level**. A cursor nested inside the bound request is rejected. During legacy reverse-index migration ids can return INDEX_NOT_READY. Missing pre-upgrade audit events are not synthesized. Read the [identity contract](identity.md) for valid time, knowledge time, retention, coverage and correction semantics.

The managed identity_temporal collection may be read under normal authorization, but ordinary put/delete/DDL are rejected. Managed temporal writes use the native identity API.

## Pagination and read controls

Only from, ids and history accept a cursor. Continue with identical bound filters, sort, projection and limit. A query fingerprint is based on bound values, so formatting changes do not change it.

Cursors bind tenant, principal, collection, runtime generation, replication history and snapshot. Default lifetime is 60 seconds from the first page, without renewal. Successful continuation consumes the old token; concurrent reuse succeeds at most once. Execution failures do not consume a still-valid token. Restart/restore/history changes expire it.

Read statements support shared deadlines, scan limits, full-scan rejection and minimum replication positions through the [read-control headers](api.md#read-controls-and-consistency). Writes reject read-only controls. Identity batch work shares one budget; it is not reset for each item.

## Lookup and explain

lookup accepts an array of document-shaped inputs. Required matching fields must exist; sparse unique null inputs miss. Results correspond to original input positions, including duplicates. mode:first or mode:all and max_matches are properties of the lookup definition, not implicit DSL behavior. Excess matches fail the call instead of truncating it.

explain only plans. Actual file, block, scan, decompression and document-fetch counts belong to query/lookup stats. Covering indexes can avoid document fetches. The planner and snapshot-aware catalogs are shared with the native API.

Syntax errors return INVALID_ARGUMENT and:

```json
{"error":{"code":"INVALID_ARGUMENT","message":"...","location":{"byte_offset":25,"line":1,"column":26}}}
```

byte_offset is zero-based; line and character column are one-based. Binding, storage and quota errors may not include source locations.

## Prepared statements in Rust

```rust,ignore
use serde_json::json;
use tenant_kv::dsl::Output;

let prepared = tenant.prepare_dsl(
    "from assets | where ip == $ip | fields owner"
)?;
let bindings = json!({"ip":"10.0.0.1"});
let Output::Query(page) =
    prepared.execute(&tenant, bindings.as_object().unwrap(), None)?
else { unreachable!() };
```

Retain Arc<Prepared> to avoid reparsing and cache lookups. TenantHandle::execute_dsl uses the tenant cache automatically. Prepared::parse allocates under the trusted caller; prepare_dsl charges engine budgets.

Prepared syntax retains no parameter values, results, principal, knowledge head, snapshot or index generation. Execute it with the currently authorized handle. latest observes a new head on a new execution; explicit knowledge UUIDs fix interpretation. Prepared::execute_read uses TenantReader's deadline, cancellation and scan budget.

## Limits

| Limit | Default / boundary |
|---|---|
| Statement | 64 KiB |
| Distinct parameters / filter nodes | 128 / 128 |
| Filter nesting / JSON syntax nesting | 16 / 64 |
| Explicit sort / projected fields | 8 / 64 |
| IN operands | 1000 scalar values |
| Batch reads / query scan / query time | 1000 / 100,000 physical records / 5 seconds |
| Response | 16 MiB by default |
| Tenant syntax cache | 256 entries / 4 MiB, 8 shards, FIFO eviction |

Document validation still uses native limits, including depth 32. Bound parameter bytes are limited by max_batch_bytes. Repeated large values are accounted before cloning and remain subject to max_query_bytes and tenant/global budgets.

The syntax cache does not eliminate physical planning or cache results. Oversized syntax executes without cache admission; externally held prepared statements remain charged until their last reference is dropped. See [operations](operations.md) for all limits and [release notes](releases/v1.0.2.md) for compatibility.
