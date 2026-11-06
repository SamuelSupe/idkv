# 通用 KV 组合能力（v0.9）

v0.10 提供[请求级读取控制](read-controls.md)：读取截止时间、扫描预算、禁止全扫描和取消。现有请求正文与 DSL 语法保持不变。

业务层解释 Schema、来源规则、数据完整性及时间区间。KV 提供条件提交、一致读取和不可变数据集发布。既有身份模块与 API 保持兼容；本次不新增身份规则、来源同步器、通用 Validator 或集合级权限。HTTP 沿用租户与角色授权，可信嵌入式宿主负责调用方授权。

注册表 v4、复制协议 3、WAL/SST v1 不变。新 HTTP 接口需要 v0.9；先升级两个节点再让客户端使用新能力。Rust 的 WriteOp 增加 Check，穷尽匹配该枚举的下游代码需要增加分支。Publication.lookup 在 Rust 中保持 String；空字符串表示无默认 Lookup，JSON 可省略。现有带 Lookup 发布及回执继续兼容。

## 1. 条件批次

`POST /v1/tenants/{tenant}/collections/{collection}/batch`：

```json
{
  "operations": [
    {"op":"check","key":"rules","if_rev":12},
    {"op":"check","key":"progress","if_rev":0},
    {"op":"put","document":{"_key":"record-1","value":"ready"},"if_rev":0},
    {"op":"put","document":{"_key":"progress","offset":1},"if_rev":0}
  ],
  "write_options": {
    "expected_history":"原读取返回的 history",
    "operation_id":"load-1",
    "retry_token":"首次提交前取得并随原请求持久保存的 UUID"
  }
}
```

- Check 必须提供 if_rev；0 检查不存在，正数检查精确版本且必须带原读取的 history。
- **所有 Check 检查批次开始前的状态，与在数组中的位置无关。** 同一键可以先被检查再被修改；把 Check 放在 Put 后面也不会检查刚写入的新值。
- Put/Delete 继续按输入顺序执行，条件与唯一性维护看到前序修改。
- Check 不更新文档、索引或文档 `_rev`，响应数组只包含 Put/Delete 的结果，按修改操作的输入顺序排列。只有 Check 且未指定 operation_id 的批次不产生 WAL 提交；指定操作 ID 可以持久化空结果回执。
- 检查失败返回 REVISION_CONFLICT，整个批次不提交。检查与修改共同计入批次数量和输入字节限制。仍限定同租户同集合。
- 有效幂等回执命中时返回原结果，不重新检查后来已改变的条件；否则成功的旧操作会被错误报告为失败。已过期重试沿用 IDEMPOTENCY_EXPIRED，不授权重新执行。
- Check 不单独构成后续写入的锁；依赖该检查的修改必须放进同一批次。

Rust 使用 `WriteOp::Check` 和现有 `write_batch_with`。仅包含 Check 的请求仍是写接口，要求 write 权限，并在主库执行。

## 2. 一个快照内组合读取

`POST /v1/tenants/{tenant}/read_batch`，需 read 权限：

```json
{
  "operations":[
    {"collection":"state","request":{"op":"get","key":"rules"}},
    {"collection":"state","request":{"op":"multi_get","keys":["record-1","missing","record-1"]}},
    {"collection":"metadata","request":{"op":"query","query":{"limit":10,"select":["/_key","/name"]}}}
  ]
}
```

Rust 对应 `tenant.read_batch(&[ReadOperation { collection, request }, ...])`。允许同租户跨集合读取，不提供跨集合写事务。

返回 `results`、`history`、`snapshot_seq` 和整批 `stats`。结果与输入操作同序；Get 未命中为 null；MultiGet 保留重复键、顺序及未命中位置。每个结果的 op 表明类型，Query 的 result 保持 QueryResult 结构。

所有操作使用同一快照，包括集合和索引定义。所有 Get/MultiGet 合并到同一次存储 MultiGet，复用块读取。Query 复用现有执行器。任何操作失败，整批返回错误；此前为这批创建的游标会释放。

查询输入不得携带 cursor，因为已有游标可能属于不同快照。首次 Query 可返回 cursor；随后用普通集合 query 和相同查询参数续页，继续原快照。不要把续页结果当成另一次 read_batch 的共同快照。租约仍默认 60 秒，不因续页无限延长。

整批共同限制：

| 限制 | 采用现有配置 |
|---|---|
| 操作数量、输入字节 | max_batch_operations / max_batch_bytes |
| Get 数 + MultiGet 输入键数 + Query limit 总和 | max_read_keys，默认 1000；重复和未命中也计费 |
| 累计扫描、总耗时 | max_scan / query_ms |
| 整个响应，包括元信息 | max_response_bytes |
| 累计保留的结果内存 | max_query_bytes，另受租户和全局预算限制 |

history 是快照读取所属数据分支；snapshot_seq 是本批实际快照序号。恢复/角色历史切换期间发现分支变化返回 HISTORY_CONFLICT。HTTP 同样支持既有 X-TKV-Min-History / X-TKV-Min-Seq 等待约束。

## 3. 不依赖 Lookup 的数据集发布

导入方创建新集合，完成数据写入、业务校验和索引构建后，使用既有发布接口：

```http
PUT /v1/tenants/{tenant}/resources/catalog
```

```json
{
  "version":"调用方生成的发布请求 UUID",
  "collection":"catalog_next",
  "expected_version":"上次发布响应的实际 version，首次为 null",
  "retry_token":"发布主体的首次提交凭据"
}
```

lookup 现在可省略或为空；提供时仍验证对应定义和索引。所有现存索引必须 READY。成功时原子冻结集合、保存版本并切换资源指针；生成的实际 version 不复用。中断的导入没有调用发布时，不改变当前指针。KV 不判断导入数据在业务上是否完整。

Rust 使用 `publish_dataset` / `publish_dataset_with`，元信息使用 `published_dataset`。已有 `publish_lookup` 仍要求非空 Lookup 名称。

按版本读取：`POST T/resources/catalog/read`，需 read 权限：

```json
{
  "version":"发布响应的实际 version；也可省略以固定请求开始时的当前版本",
  "operations":[
    {"op":"get","key":"record-1"},
    {"op":"multi_get","keys":["record-1","missing"]},
    {"op":"query","query":{"limit":100}}
  ]
}
```

对应 `read_published(resource, version, requests)`，返回 `{version,result:ReadBatchResult}`。版本解析与全部读取使用同一快照，不会在发布切换时读到另一集合。请求不能自行指定集合，从资源版本确定集合。

发布数据集的 Query 分页使用 `POST T/resources/catalog/query`，正文为 `{ "version": "可选 UUID", "query": { "limit": 100, "cursor": "上页游标" } }`，对应 `query_published`。首请求可省略 cursor；续页必须保持查询参数。游标绑定资源、版本、租户、存储代次和查询；不能拿到其他资源、版本或普通集合 query 接口使用。续页省略 version 仍继续游标中的原版本，不改为 latest。

旧版 `/resources/{resource}/execute` 保持默认 Lookup 批量执行。未定义默认 Lookup 的数据集调用 execute 返回 INVALID_ARGUMENT；使用 read/query。

## 4. 保留、失败与恢复

沿用唯一的租户 retention_seconds。当前资源版本持续保留；旧版本从被替代时计时；回执从提交时计时。新请求访问过期版本返回 VERSION_UNAVAILABLE。已获准的查询游标可在原快照租约内继续，后台 GC/合并保留所需文件，不延长旧版本对新请求的可访问时间。

发布集合不可变，后续刷新准备新集合。最后一个保留版本引用消失后分批回收集合。业务自行保存的普通历史文档不会被 KV 推断成“旧状态”并删除；这与显式数据集版本管理不同。

复制和备份沿用同一 WAL/检查点。副本不执行 Check 或重新选择发布版本，而是回放主库已提交的数据、元信息和回执。异步复制的 RPO 不变；恢复或故障升主后的安全重试仍遵守 history/retry_token 契约。游标不跨服务重启、恢复和复制历史切换。

## 5. 完整示例与边界

不可变发布数据集支持按主键边界分段读取。例如固定同一个实际 version，每次查询 `/_key > 上次最后一个_key`，默认按 `_key ASC` 返回，再保存最后一个主键。v0.9.1 会直接定位主键范围。与游标不同，每段是新请求，仍须该发布版本尚未过期；版本到期或恢复后不能保证继续。可变集合的多页一致性应继续使用快照游标，不能用反复新建快照的主键边界查询代替。

read_batch 中的 Query、发布版本 Query 和 DSL Query 共用优化后的规划与执行路径。非唯一 Lookup 批量范围复用，以及 IN 展开上限详见 [查询契约](api.md)。

```sh
cargo run --locked --example kv_capabilities -- /var/tmp/tenant-kv-capabilities-demo
```

[示例](../examples/kv_capabilities.rs) 完成一致读取、业务检查、条件提交数据与游标、幂等重放、无 Lookup 发布以及固定版本读取。

本轮没有增加跨租户/跨集合写事务、持久交互式读事务、集合 Validator、集合级授权或业务变更订阅。现有 DSL 语法保持不变，新组合能力通过 Rust API 与 JSON HTTP API 使用；主从 HTTP Client 的 write_batch 可直接携带 Check，新读取接口可由标准 HTTP 调用。
