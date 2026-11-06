# 请求级读取控制与主从客户端（v0.10.1）

KV 调用方可以为每次读取收紧耗时、扫描量和扫描策略。现有 Query JSON、DSL 语法、数据格式和复制协议不变。业务 Schema、业务权限和审计仍由上层负责。

## Rust 嵌入式调用

```rust
use tenant_kv::{ReadOptions, ReadCancellation, Query};

let cancel = ReadCancellation::default();
let options = ReadOptions {
    timeout_ms: Some(200),
    max_scan: Some(5_000),
    allow_full_scan: false,
    cancellation: Some(cancel.clone()),
};
let reader = tenant.read_with(&options)?;
let query: Query = serde_json::from_value(serde_json::json!({
    "filter": {"field":"/_key", "op":"gte", "value":"asset-"},
    "select": ["/_key", "/owner"],
    "limit": 100
}))?;
let result = reader.query("assets", query)?;
if let Some(cursor) = result.cursor { tenant.release_cursor(&cursor)?; }
```

`TenantReader` 提供 get、multi_get、query、explain、lookup_many、read_batch、published_dataset、read_published、query_published、lookup_published 和 wait_context。原 TenantHandle 方法继续可用。

- deadline 从 `read_with` 创建成功时开始计时；复用同一个 reader 的调用共享该截止时间。需要新预算时创建新 reader。
- 每次调用独立取得快照。只有 read_batch / read_published 内所有子操作使用同一快照，并累计扫描量；连续调用 reader.query 不等于跨调用事务。
- timeout_ms 和 max_scan 只能收紧引擎 query_ms / max_scan；零值非法。租户和全局内存、并发、输出字节预算照常生效。
- max_scan 使用执行统计中的 scanned_entries：物理扫描包含历史版本和墓碑，查询/Lookup 的点定位命中也计数。原生 Get/MultiGet 不消耗扫描计数，受键数量、输入/输出字节与时间限制；它们不会退化为集合扫描。
- allow_full_scan=false 在执行正文扫描前拒绝含集合扫描分支的计划，返回 SCAN_NOT_ALLOWED。OR 中任一分支需要全扫描也会失败。主键范围、索引范围仍可执行，但范围可能很宽，因此仍需要扫描预算。explain 始终可以返回被禁止的计划，便于调用方调整索引。
- `cancel.cancel()` 是不可复位的协作式取消，可从另一线程调用。扫描在物理记录间检查，原生批量读取在文件、索引分区和块读取边界检查；保留原有去重、分块和有界并行。v0.10.1 将预算传入正文及索引块的 I/O 等待：等待线程每至多 10 ms 检查取消，已取消、过期或被放弃的排队读取在出队时跳过，不再读取磁盘。已进入的 OS I/O、解压或编码不会被强制中断，部分集合/索引元信息读取仍在调用边界检查。取消不会影响其他请求，也不会中止或回滚写入。
- 预算失败不会消费输入查询游标；失败的 read_batch 会释放该批新建游标。成功续页仍只允许消费一次；响应丢失后不能假设旧游标可重试。

完整可运行示例：[examples/read_controls.rs](../examples/read_controls.rs)。

## HTTP

在通用 KV 读取路径上增加可选请求头：

| 请求头 | 含义 |
|---|---|
| X-TKV-Read-Timeout-Ms | 正整数，单请求超时，受 query_ms 上限约束 |
| X-TKV-Max-Scan | 正整数，累计扫描上限，受 max_scan 上限约束 |
| X-TKV-Allow-Full-Scan | 严格为 true 或 false，默认 true |

```sh
curl -X POST "$BASE/v1/tenants/$TENANT/collections/assets/query" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'X-TKV-Read-Timeout-Ms: 200' -H 'X-TKV-Max-Scan: 5000' \
  -H 'X-TKV-Allow-Full-Scan: false' \
  -d '{"filter":{"field":"/_key","op":"eq","value":"asset-0"},"limit":1}'
```

支持文档 GET、MultiGet、Query、Explain、Lookup execute、read_batch、发布元信息 GET、发布 read/query/execute，以及 DSL 只读语句。管理、写入、身份模块和复制管理路径不接收这些控制头；DSL 写语句同样拒绝。未知值、重复控制头、零值返回 400/INVALID_ARGUMENT。

同一截止时间覆盖请求体接收、工作线程排队、X-TKV-Min-History / X-TKV-Min-Seq 一致性等待和读取执行，不在子查询处重新计时。一致性等待额外保留五秒上限。指定控制头后耗尽总截止时间返回 408/DEADLINE_EXCEEDED；未指定时，一致性等待耗尽仍保持旧版 409/REPLICA_LAG。扫描超限为 429/RESOURCE_EXHAUSTED，禁止集合扫描为 422/SCAN_NOT_ALLOWED。

HTTP handler 的 future 被释放时会通知其阻塞读任务取消；单个底层 I/O 不能抢占，网络断开被传递的时机也取决于 HTTP 运行时。快照租约仍负责回收丢失响应产生的游标，默认 60 秒，不允许无限延长。

受控只读成功响应和 `/context` 响应携带 `X-TKV-Read-Controls: 1`。`/v1/capabilities` 增加 request_read_controls_v1。新客户端会验证这个确认，防止旧节点忽略控制头；应先升级主从，再更新客户端。OpenAPI 包含控制头和错误响应。

## Rust HTTP 客户端

```rust
use tenant_kv::client::{Client, ClientReadOptions, ReadPosition};
use tenant_kv::ReadOptions;

let client = Client::new(&nodes, token, tenant_id)?;
let options = ClientReadOptions {
    minimum: Some(ReadPosition::from(&write_receipt)),
    read: ReadOptions {
        timeout_ms: Some(500),
        max_scan: Some(5_000),
        allow_full_scan: false,
        ..Default::default()
    },
};
let page = client.query("assets", query, &options).await?;
if let Some(cursor) = page.cursor { client.release_cursor(&cursor).await?; }
```

客户端新增 MultiGet、Query、Explain、Lookup、read_batch、published_dataset、read_published、query_published、lookup_published 和 release_cursor。`get(collection,key,minimum)` 保持原签名；需要读取控制时用 get_with_options。ReadPosition 可以从 WriteResult、ReadBatchResult 或 ReadContext 生成。需要把读取结果用于条件写时，使用 read_batch 返回的同快照 history 和文档 _rev，不要用新获取的 write_context 替换原读取的历史。

新读取优先使用从库。v0.10.1 在一个 Client 及其 clone 之间共享一秒的只读路由缓存；同时到来的冷请求合并为一次发现。复用 Client 才能持续命中缓存，每次重新创建 Client 会重新发现。发现阶段最多并发探测两个配置节点，每个探测不超过两秒及剩余预算的四分之一，为实际读取留下时间。等待其他调用刷新缓存同样受本次调用的截止时间和取消控制，没有常驻刷新任务。

缓存中的角色、history 和已应用序号只用于选择地址。若缓存显示从库落后于最低位置，优先尝试主库，即使缓存中的主库序号也已过时；原始最低 history/seq 仍完整发送给服务端校验。缓存 history 与最低位置不符时重新发现，不能把旧位置改成新历史。每次数据响应仍检查读取控制能力。没有最低位置时允许读取从库本地已应用状态。

连接失败、UNAVAILABLE、REPLICA_LAG 会使对应路由缓存失效，并尝试另一个已配置节点；如果原来只发现一个可用节点，最多额外发现一次，每个节点至多执行一次数据请求。资源超限、超时、取消、历史冲突和权限错误直接返回，避免失败后绕过限制；历史冲突和能力不兼容也清除旧缓存。写入仍重新发现主节点并检查双主，不使用读取缓存授权写入。max_scan 限制每次服务端执行；如果传输中断，客户端无法精确累计已丢失响应那次尝试的扫描量。

一次客户端读取的默认总预算为八秒，显式 timeout_ms 可收紧；发现、回退、响应下载及取消共享该预算。发送到服务端的是剩余时间，服务端再按 query_ms 收紧。客户端取消检查的轮询间隔为 10 ms；这不是底层 I/O 或整个请求完成时间的严格保证。

客户端返回 `tkv1.<origin-and-tenant-hash>.<server-cursor>` 形式的不透明游标。绑定当前租户和配置的节点 origin，不能注入任意 URL；原始游标仍由服务端校验查询、快照、租户及存储代次。普通、read_batch 和发布查询全部使用此包装。续页直接访问原节点，不做发现、回退或自动重试；节点不可用时返回错误，需要调用方决定是否重新开始整个查询。发布分页仍固定原数据集版本。

release_cursor 仅访问原节点，read 角色即可释放其授权租户内的游标；成功后不可继续分页；节点不可达时由租约回收。SDK 游标只用于 SDK，不能直接提交到原始 HTTP；相同租户与节点配置的新 Client 可以继续尚未过期的 SDK 游标。如果续页快照早于请求的最低序号，返回 409/SNAPSHOT_TOO_OLD，并保留原游标，不会用当前节点已追平来伪装原快照满足新位置。主从切换、恢复、服务重启仍会使旧快照失效。客户端不会自行升主。
