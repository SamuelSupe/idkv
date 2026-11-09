# API 与查询契约

v0.10 提供[请求级读取控制](read-controls.md)：读取截止时间、扫描预算、禁止全扫描和取消。现有请求正文与 DSL 语法保持不变。

HTTP 所有数据接口以 `/v1/tenants/{tenant_uuid}` 开头。`GET /openapi.json` 提供 OpenAPI 3.1，`tenant-kv openapi` 可导出同一份定义。HTTP 响应为 JSON，同步成功为 200，远端备份任务提交为 202；错误为 `{"error":{"code":"...","message":"..."}}`。

响应头 `x-tkv-history` 从内部注册表及复制状态读取，不打开租户引擎，也不从用户文档中的同名字段推断；文档可以合法保存任意 JSON `history` 值。读取状态、关闭、修改配额或恢复后的响应编码不会为填充此头重新打开实例。

## 接口

以下 `T` 表示 `/v1/tenants/{tenant}`，`C` 表示 `T/collections/{collection}`。

| 方法与路径 | 请求 / 行为 |
|---|---|
| `POST /v1/tenants` | `{"name":"customer-a"}`，全局管理员创建 |
| `GET /v1/tenants` | 列出活跃租户，全局管理员 |
| `PUT / GET T/retention` | 设置/查看唯一参数 retention_seconds 与回收状态，admin；当前状态不失效 |
| `POST T/retry-context` | 写入主体获取首次提交凭据；必须与原请求一同保存并复用 |
| `GET T` | 生命周期、配额、打开状态、错误和运行统计 |
| `POST T/dsl` | `{"statement":"get assets $key","params":{"key":"a"}}`，可选 cursor；按命令校验读/写权限 |
| `PATCH T` | `{"memory_bytes":536870912,"disk_bytes":107374182400}`；关闭空闲实例后更新 |
| `DELETE T` | 删除租户，UUID 留作不可复用墓碑 |
| `POST T/close` | 关闭无活动句柄/快照的租户，否则 BUSY |
| `POST T/collections` | `{"name":"assets"}` |
| `GET T/collections` | 列出集合 |
| `DELETE C` | 原子移除目录与定义，后台分批回收内容 |
| `PUT C/documents` | `{"document":{"_key":"a","ip":"10.0.0.1"},"if_rev":0}` |
| `GET C/documents/{key}` | 文档，不存在为 NOT_FOUND |
| `DELETE C/documents/{key}` | 可选正文 `{"if_rev":123,"write_options":{"expected_history":"原读取标识"}}` |
| `POST C/batch` | `{"operations":[{"op":"put","document":{}},{"op":"delete","key":"a"}]}` |
| `POST C/multi_get` | `{"keys":["a","b","a"]}`；同序返回，未命中为 null |
| `POST T/read_batch` | 同租户跨集合组合 Get/MultiGet/Query；同一快照、整批预算；见 [组合能力](kv-capabilities.md) |
| `POST C/indexes` | IndexSpec；调用等待构建完成，其他请求可并发写入；每次 L0/内存表背压准入等待最多 5 秒，见运维文档 |
| `GET C/indexes` | 状态为 BUILDING、READY 或 FAILED |
| `GET / DELETE C/indexes/{index}` | 查看或删除索引 |
| `POST C/lookups` | 创建固定 Lookup 定义 |
| `GET C/lookups` | 列出定义 |
| `DELETE C/lookups/{lookup}` | 删除定义 |
| `POST C/lookups/{lookup}/execute` | `{"inputs":[{"ip":"10.0.0.1"}]}` |
| `POST C/query` | Query，返回文档、游标、快照序号、计划和实际统计 |
| `POST C/explain` | Query，仅返回计划；实际 I/O 统计在 query 结果中 |
| `DELETE T/cursors/{cursor}` | 提前释放分页快照，read 角色即可，仍校验租户 |
| `POST T/checkpoint` | 刷盘并建立持久 checkpoint |
| `POST T/backup` | 返回 `{"backup":"UUID"}`；本地总额/数量/空闲空间受限 |
| `DELETE T/backup/{backup}` | 删除本租户本地备份，需要 admin；不删除远端备份 |
| `POST T/restore` | `{"backup":"UUID"}`，仅允许本租户备份 |
| `GET / POST T/remote-backups` | 列出远端备份 / 提交异步创建任务 |
| `POST T/remote-backups/{backup}/verify` | 提交全文件 SHA-256 校验 |
| `POST T/remote-backups/{backup}/restore` | 提交校验及恢复任务 |
| `POST T/remote-backups/{backup}/delete` | 显式删除远端备份元数据，含坏清单；返回异步任务 |
| `POST T/remote-backups/prune` | `{"keep_last":7}`，按引用保留清理 |
| `GET T/backup-jobs` | 持久任务列表 |
| `GET / DELETE T/backup-jobs/{job}` | 查询任务 / 忘记终态任务与本地暂存 |
| `POST T/backup-jobs/{job}/retry` | 重试失败的创建、校验或清理，禁止自动重试恢复 |
| `POST T/check` | 校验当前 SST 内容、顺序和校验和 |
| `POST T/compact` | 刷盘并执行完整合并 |
| `POST T/replication/wait` | `{"epoch":"UUID","seq":123,"timeout_ms":5000}`；需 read，等待本地已应用位置 |
| `GET /metrics` | Prometheus 文本；全局管理员；metrics=false 时返回 404 |
| `GET T/context` | 当前 history、已应用序号与可写状态 |
| `GET / PUT T/resources/{resource}` | 读取 / 原子发布不可变数据集；lookup 可省略，详见中间件契约 |
| `POST T/resources/{resource}/execute` | 按指定 version 执行批量 Lookup |
| `POST T/resources/{resource}/read` | 指定 version 的 Get/MultiGet/Query 组合读取，省略版本固定当前指针 |
| `POST T/resources/{resource}/query` | 按发布版本查询与续页，游标固定原资源和版本 |
| `GET /ready`、`GET /ready/write` | 无认证的读/写就绪检查，排空或角色不可用时 503 |
| `GET /v1/capabilities` | 无认证的软件/协议能力 |
| `GET /v1/metrics` | 全局内存、memory_pressure 回收状态与各租户统计，仅全局管理员；字段见[运维说明](operations.md) |
| `GET /v1/replication/status` | 全局管理员或复制凭证读取节点角色与租户位置 |
| `POST /v1/replication/switchover` | `{}`；全局管理员向当前主库发起计划切换 |
| `POST /v1/replication/fence` | `{"target":"node UUID"}`；全局管理员持久停写 |
| `POST /v1/replication/promote` | `{"fence":"UUID"}`；验证旧主停写凭据并追平后升主 |
| `POST /v1/replication/follow` | `{}`；全局管理员将已停写的旧主切为其指定接任者的从库 |
| `POST /v1/replication/force-promote` | 明确源身份、外部隔离及接受数据损失；见复制文档 |

`read` 可读取、查询、执行 Lookup；`write` 额外可修改文档；租户 `admin` 可管理集合、索引、Lookup 和该租户备份恢复。创建/删除租户和修改租户配额需全局管理员。缩小磁盘配额会先排空并统计当前占用，必须保留前台写入空间，拒绝把租户缩到无法继续删除数据的状态。缩小内存配额会预检新预算下能否重新打开和校验存储（副本配额同步会先用旧预算刷盘）；验证拒绝不会更改原配额。创建或配额发布的 I/O 错误可能表示持久结果不确定，应重开 Store 并检查名称、生命周期和配额后再操作。库 API 的 `Store` 是可信管理入口，`TenantHandle` 是已绑定租户的数据入口。

`replication` 是绑定全局的专用凭证，只允许复制状态、日志、检查点和经验证的计划升主。它不能调用普通业务 API，也不能停写、发起切换或强制升主。复制内部传输接口见 OpenAPI；配置、切换顺序及重试约束见 [replication.md](replication.md)。启用复制后普通 restore/import 被拒绝；备份仍可使用。

租户删除遇到持久状态发布或清理错误时，结果可能不确定，错误响应不代表租户仍然可写。v0.5.2 会将受影响租户隔离为 `UNAVAILABLE`；必须重启 Store 根据持久 journal 恢复，`close` 不能解除隔离。计划切换的首次请求会先检查接任角色和复制来源，不匹配时在停写前拒绝；已经持久停写的中断仍须按复制文档重试。

远端备份接口的 GET 同样需要 admin；202 不代表备份已完成。状态机、权限、恢复和错误处理见 [OpenDAL 备份](backup.md)。新机器导入通过离线 CLI 或可信 Rust API，不通过 HTTP 创建未经注册的租户身份。

DSL 的完整语法与 Rust 准备语句示例见 [dsl.md](dsl.md)。它复用下述查询/写入语义；DSL `get` 未命中为 JSON null，区别于原有文档 GET 的 404。语法错误的 error 对象额外包含 `location: {byte_offset, line, column}`，偏移从 0 起，行与字符列从 1 起；其他错误可能没有该字段。

## 文档与写入

`put` 为整文档替换。缺省 `_key` 时生成 UUID 字符串；`_key` 在集合内唯一，长度 1–512 UTF-8 字节。输入不能包含 `_rev`，输出 `_rev` 是该文档最后一次修改的提交序号。

- 省略 `if_rev`：无条件写入。
- `if_rev: 0`：仅文档不存在时允许写入。
- 正数 `if_rev`：必须提供原读取的 `write_options.expected_history` 且匹配旧文档版本，否则整个批次失败。
- 删除不存在文档为幂等成功；带条件时仍校验条件。
- 批次只能操作同租户同集合。Put/Delete 看到前序修改的新状态，包括唯一性及版本；所有修改共同提交一个序号。
- v0.9 增加 `{"op":"check","key":"rules","if_rev":123}`。所有 Check 在任何修改前检查，0 表示不存在；检查不修改正文/索引/文档版本，结果数组只包含 Put/Delete。详细重试及预算语义见 [组合能力](kv-capabilities.md)。

写入回执携带 `history`，文档读取响应带 `X-TKV-History`。v0.7 的历史标识包含租户、数据分支和复制 epoch，必须作为不透明字符串使用；普通写入的持久化 operation_id、稳定 subject、最小已应用位置请求头及原子 Lookup 发布见 [中间件契约](middleware.md)。从库业务写入返回 READ_ONLY；异步确认仍仅保证主库本地持久化。

JSON 数值支持 i64、u64 和有限 f64。`1` 与 `1.0` 属于不同类型域，字符串不转数字。整数越界拒绝。缺失与 null 不同。对象/数组可保存、投影，但索引匹配路径必须是标量；写入对象或数组到已有索引路径会失败。

字段路径采用非根 JSON Pointer，`~1` 表示 `/`，`~0` 表示 `~`。`/items/1/name` 支持按数组下标读取；数组不展开为多条索引。投影保留源容器形状，未选择的前置数组元素以 null 占位。选择整个对象/数组会保留其全部内容。

## 索引和 Lookup

```json
{
  "name":"ip",
  "fields":[{"field":"/ip","order":"asc"}],
  "unique":true,
  "include":["/owner","/service"]
}
```

索引最多 8 个匹配字段、32 个覆盖字段；每集合最多 16 个索引。唯一索引只收录所有匹配字段存在且非 null 的文档。非唯一索引收录缺失和 null，因此可用于完整排序。唯一索引不会用于推断缺失/null 的完整结果。

```json
{
  "name":"asset_owner",
  "index":"ip",
  "select":["/ip","/owner","/service"],
  "mode":"all",
  "max_matches":100
}
```

每个 Lookup 输入必须提供全部索引匹配路径，数据结构与文档相同。缺失输入视为未匹配；稀疏唯一索引的 null 输入也未匹配。返回与输入等长的 `matches` 数组，每项为列表，未匹配为 `[]`。重复输入保持重复位置。一次调用固定一个快照。

非唯一结果按 `_key ASC`；`mode: first` 只取第一项。`all` 超过 `max_matches` 时整次调用返回 MATCH_LIMIT_EXCEEDED，不悄悄截断。默认上限 100，可设置 1–1000。输出只包含 select 字段，调用方自行合并事件。已覆盖字段直接从索引读取，未覆盖字段批量回读正文。

v0.9.1 将去重后的非唯一匹配值按物理键排序，在同一快照内复用文件游标、索引分区和当前已解压数据块；直接跳到下一匹配范围，不扫描范围之间的数据。匹配值靠近且落在相同数据块时收益更明显；分散范围仍可能各自读取不同块。结果继续恢复为原输入顺序。缓存和持有的数据块沿用租户内存计费。

## 条件查询与分页

```json
{
  "filter":{"and":[
    {"field":"/env","op":"eq","value":"prod"},
    {"field":"/score","op":"gte","value":80}
  ]},
  "sort":[{"field":"/score","order":"desc"}],
  "select":["/_key","/owner","/service"],
  "limit":100
}
```

支持 eq、ne、in、gt、gte、lt、lte、exists 及 and/or/not。exists 的 value 必须为布尔。eq null 仅匹配显式 null；ne 也可匹配缺失字段。范围比较要求相同标量类型。混合类型排序顺序为：缺失、null、bool、i64、u64、f64、string；按对象或数组排序返回参数错误。

默认按 `_key ASC` 排序。指定的排序未包含 `_key` 时，在末尾追加 `_key ASC`，确保稳定唯一顺序。省略 select 或传空列表返回完整文档。无适合索引时执行受预算约束的扫描；不能利用索引排序时使用有界 Top-K。首版不提供深度 offset、磁盘外排或索引交集。根 OR 分支分别规划并去重；嵌套布尔表达式仍正确执行，可能退化为残余过滤或扫描。

v0.9.1 的查询定位规则：

- `/_key` 的字符串 gt/gte/lt/lte 条件可直接限定正文键范围，包含和排除边界遵循原比较语义。仍优先使用可选的二级索引，主键范围替代原来的全集合扫描；降序请求在范围内做 Top-K。
- 二级索引连续前缀可组合 eq/in，去重后最多展开 256 段，前缀键总长上限 1 MiB。超出时使用较短前缀或受预算限制的扫描，原过滤条件仍全部执行，不截断结果。完整唯一键集合使用存储 MultiGet；其他多段访问复用范围游标。
- 稀疏唯一索引仅在 AND 条件能证明每个索引字段均存在且非 null 时用于范围或 IN 查询。未受约束的复合字段、含 null 的 IN、exists 条件不会单独构成这种证明。
- 多段索引查询当前保守地执行有界 Top-K，分页在原快照内重新读取各段并排除前页结果；没有实现跨段合并排序优化。`explain` 会明确标记 `extra_sort`。

以上路径共享原扫描、时间、返回字节与临时内存预算；旧版本和删除标记仍计入扫描量。read_batch 中每个 Query 使用这些路径，整个批次保持同一快照和累计预算；不同 Query 尚未融合成一次存储扫描。

下一页提交完全相同的 query 并添加返回的 cursor；limit 也必须相同。游标绑定租户、集合、查询、运行代次、复制历史和快照，成功继续后旧 token 被移除。默认租约从第一页起 60 秒，不随翻页续期；到期、重启、恢复或切换复制历史后返回 CURSOR_EXPIRED。可提前 DELETE 游标释放文件和版本引用。持久游标总数不超过 max_snapshots 的一半，每个主体不超过四分之一（取整，至少 1）；默认分别为 128 与 64，为点读及维护留出名额。max_snapshots 为 1 时不保留分页游标。HTTP 游标同时绑定 token 的稳定 subject，其他主体不能续页；嵌入式调用共享默认主体配额。

游标总额由同一租户的所有主体共享，不按主体预留名额。默认配置下两个主体各保留 64 个游标后，第三个主体的新分页会被拒绝，直至游标释放或过期；点读仍可使用保留的快照名额。这是容量限制，不提供主体之间的分页公平保证。

同一个 cursor 不可并发消费多次：仅一个续页调用成功，其余返回 CURSOR_EXPIRED。查询执行或输出超预算失败时，不消费原 token（租约仍按原时刻计算）。MultiGet 的响应限制包含全部文档、未命中的 null、逗号和数组括号，原生库接口和 HTTP 均不能依靠未命中项绕过该限制。

`query.plan` 标识索引、覆盖、额外排序及全扫描。`query.stats` 给出候选文件、物理扫描条目、实际读取块、解压字节、缓存命中和正文读取次数。旧版本和墓碑也计入扫描预算。

## 错误与重试

| code | HTTP | 调用方行为 |
|---|---|---|
| UNAUTHENTICATED / FORBIDDEN | 401 / 403 | 修正凭证或租户权限 |
| INVALID_ARGUMENT | 400 | 修正参数 |
| NOT_FOUND | 404 | 资源不存在 |
| REVISION_CONFLICT / UNIQUE_CONFLICT | 409 | 读取当前状态后重新决策 |
| BUSY / INDEX_NOT_READY / CONFLICT | 409 | 等待活动请求或修改定义 |
| CANCELLED | 409 | 操作已取消；检查点复制因源 WAL 故障取消时，等待恢复后重试备份 |
| READ_ONLY | 409 | 连接当前可写主库；不能通过重启旧配置解锁 |
| REPLICATION_CONFLICT / REPLICA_LAG | 409 | 核对历史和来源 / 等待追平；禁止跳过日志 |
| LOG_GONE / SNAPSHOT_EXPIRED | 410 | 复制服务重新取得检查点 |
| RESOURCE_EXHAUSTED / MATCH_LIMIT_EXCEEDED | 429 | 降低请求规模、等待背压或调整配额 |
| CURSOR_EXPIRED | 410 | 从新快照重新查询 |
| DEADLINE_EXCEEDED | 408 | 缩小查询范围；索引构建准入超时则检查维护积压及索引状态 |
| CORRUPTION | 500 | 隔离租户并检查/恢复 |
| COMMIT_UNKNOWN | 500 | 批次可能已提交；重新打开后核对 `_key`/`_rev`，避免盲目重试 |
| UNAVAILABLE | 503 | 查询租户状态与维护错误 |
| REPLICATION_UNAVAILABLE | 503 | 核对对端连接及双方持久角色；切换超时后观察并重试 |

普通 put/delete/batch/DSL 已支持持久化 operation_id。启用租户保留后，新幂等写入需要 retry_token；过期或缺失的旧重试明确拒绝，不视为未执行。见 [保留契约](retention.md)。

## v0.6 时态身份 API

新增 `/v1/tenants/{tenant}/identity/*` 和 Rust `TenantHandle::identity_*`。HTTP 写入允许 admin、identity_write 或 identity_correct；手动纠错要求 admin 或 identity_correct，初始化要求 admin，读取沿用租户 read 权限。完整请求、区间/知识语义、幂等性与示例见 [identity.md](identity.md)。保留集合 `identity_temporal` 拒绝普通 CRUD、DSL 写入及集合/索引/Lookup 管理修改，返回 `MANAGED_COLLECTION`；读取仍授权开放。新错误包括 `INTERVAL_CONFLICT`、`OPERATION_CONFLICT`、`KNOWLEDGE_UNAVAILABLE`（HTTP 409）和 `CLOCK_SKEW`（HTTP 503）。

租户级统一过期时间、最新状态保护、历史查询到期和 v0.8 发布版本兼容性见 [retention.md](retention.md)。IDEMPOTENCY_EXPIRED/VERSION_UNAVAILABLE 返回 410，RETRY_TOKEN_REQUIRED 返回 428；KNOWLEDGE_UNAVAILABLE 保持 409。

`POST T/retry-context` 是会写入 retry token 的操作：read 角色仍被拒绝，响应按写入路径处理，不附加只读响应语义或接受一致性读控制。`GET /openapi.json` 的配置专属文档在创建 HTTP router 时生成并编码一次，各请求复用同一份内容。

恢复未完成的只读诊断实例拒绝普通业务读取，返回 RECOVERY_REQUIRED（503），而非旧视图下的正常 404。本地备份在 rename 后同步结果不确定时返回 BACKUP_COMMIT_UNKNOWN，message 包含可检查/删除的 UUID。

`POST /v1/tenants/{tenant}/identity/resolve_many` 和 DSL `resolvemany with $request` 提供同快照批量时态解析。顶层 known_at 固定整批知识版本，每项 identifier/valid_at 独立；返回有序 results、knowledge_version、history、snapshot_seq 和累计 stats。默认最多 1000 项，任一错误导致整批失败。`resolve with $request` 对应单条解析。两个端点均支持请求级读取控制和最低复制位置，详见 [identity.md](identity.md)。

主体查询（read 权限）：`POST /v1/tenants/{tenant}/identity/ids` 接收 `SubjectIdentifiersRequest`，返回 `SubjectIdentifiersResult`；`POST /v1/tenants/{tenant}/identity/history` 接收 `SubjectHistoryRequest`，返回 `SubjectHistoryResult`。支持相同的读取控制、最低复制位置及节点绑定游标；参数、事件和覆盖范围详见[身份模块](identity.md#主体反查和变更历史)，准确结构见 OpenAPI。

租户 retention 状态中的 `identity_subjects` 返回 `SubjectMaintenanceProgress` 或 null；即使未启用保留策略也可查看身份迁移是否 ready、已处理历史行数/写入 link 数及 link 回收行数。该接口沿用原管理权限，计数随维护事务持久化，详情见[迁移进度](identity.md#迁移进度)。
