# 时态身份与主体查询（1.0）

当前正式版为 1.0.1，包含批量解析、主体标识反查、事件历史与可恢复迁移。英文入口见 [API](api.md)、[DSL](dsl.md)、[部署](deployment.md)；下文 v0.x 编号说明功能引入历史。

v0.6.0 提供受管理的身份写入和单标识基础解析，用于回答“这个来源标识在事件发生时属于谁”和“按照当时已知的信息会怎样判断”。

当前只有独占 `identifies` 关系。每租户保留集合名 `identity_temporal`，内部仍使用该租户原有 WAL、LSM、备份和复制链路。普通 CRUD、DSL 写入、集合删除、索引及 Lookup 定义修改不能修改这个集合。普通授权读取可以查看其中的审计记录。嵌入式 `Engine` 原始键值接口属于可信宿主代码，不是隔离恶意进程内调用者的安全边界。

现在支持原生 `resolve_many`，以及 DSL `resolve` / `resolvemany`；主体反查 `ids` 和变更历史 `history` 支持固定知识分页。尚无 Log/Trace/RUM 查询协调器、多值关系、实体删除或全量 `replace_bindings`。批量写入采用增量 merge；未列出的标识保持原状。

## 运行示例

```sh
cargo run --locked --example identity -- /var/tmp/tenant-kv-identity-demo
```

[完整 Rust 示例](../examples/identity.rs) 创建 Alice、Bob，将账号 `10086` 在 `[09:00,12:00)` 绑定给 Alice，再将 `[10:00,11:00)` 修正为 Bob。查询 10:30：最新知识返回 Bob，修正前知识版本仍返回 Alice。输出包含实体 UUID、三个时间片段、修改收据、知识 UUID 和读取统计。

HTTP 服务沿用现有启动方式和 Bearer token 配置。以下路径均以 `/v1/tenants/{tenant}/identity` 为前缀；`{tenant}` 必须通过 token 的租户授权。

| 方法与路径 | 功能 | HTTP 权限 |
|---|---|---|
| `POST /initialize` | 创建受管集合；相同配置重复初始化返回当前知识头 | admin |
| `POST /write` | 创建实体、更新属性、原子修改多个标识 | admin / identity_write / identity_correct；纠错需 admin 或 identity_correct |
| `POST /resolve` | 指定业务时间、当前或历史知识解析 | read/write/admin |
| `POST /resolve_many` | 一份快照、一个知识版本下按输入顺序批量解析 | read/write/admin |
| `POST /timeline` | 获取当前完整时间线和修订令牌 | read/write/admin |
| `GET /entities/{entity}` | 当前实体属性与两类修订令牌 | read/write/admin |
| `GET /operations/{operation}` | 已提交操作的原始收据 | read/write/admin |

可运行 [HTTP 时态示例](../scripts/identity_smoke.py)：

```sh
python3 scripts/identity_smoke.py http://127.0.0.1:8080 /path/to/admin-token.txt
```

脚本创建独立示例租户，完成初始化、历史修正、幂等重试、解析、OpenDAL 备份恢复（加 `--remote-backups`）、恢复后旧版本检测与租户隔离检查。调用此脚本需要全局 admin，实际业务可以使用租户 admin。

### 1. 初始化和创建实体

`POST /initialize`：

```json
{"max_segments":1000,"clock_tolerance_ms":1000}
```

两项均可省略。配置创建后不可修改。已有普通集合占用保留名称时拒绝初始化，不接管旧数据。

`POST /write`：

```json
{"action":"create_entity","operation_id":"create-alice-001","attributes":{"name":"Alice","team":"platform"}}
```

从响应的 `entity.entity_id` 取得数据库生成的 UUID。实体 ID 不允许指定或复用。实体属性更新使用 `update_entity`、`entity_id`、`expected_revision` 和 `attributes`；属性按顶层键合并，未提供的字段保留。属性更新不会结束绑定，也不会改变 `bindings_revision`。`revision` 约束属性更新，`bindings_revision` 跟踪绑定集合变化。

### 2. 自动绑定与变更

`POST /write`，将示例 UUID 替换为上一步生成的实体：

```json
{
  "action":"bindings",
  "operation_id":"bind-account-001",
  "changes":[{
    "identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},
    "expected_revision":"absent",
    "change":{"action":"bind","entity_id":"00000000-0000-4000-8000-000000000001"}
  }]
}
```

省略 `effective` 等价于 `{"mode":"auto"}`。事务在主库写入协调器内选定一个时间 `T`，所有自动操作共用这个 `T`。响应 `effective_time_us` 返回实际值。它是处理时间，不是落盘完成时刻。

已有时间线必须提供其 revision UUID，即使当前没有有效片段，也不能重新使用 `absent`。`POST /timeline` 请求体就是 `identifier` 对象；响应为时间线或 `null`。修改后从收据 `timelines[].revision` 获取新令牌。对不存在的时间线执行无效果解除返回 `revision:null`，不创建占位时间线。

自动操作范围为 `[T, 下一条已知业务边界)`，保留显式到期与未来计划。`rebind` 需要目标实体、修订令牌和 `reason`；`unbind` 需要修订令牌和 `reason`。`bind` 与其他实体重叠时返回 `INTERVAL_CONFLICT`。相同实体、来源及证据的等价设置不切分时间线、不改变时间线 revision，但新 operation_id 仍产生可审计知识记录和收据。

`replace_identifier` 在一个批次内解除旧标识并绑定新标识，需要 `entity_id`、`replacement`、`replacement_expected_revision` 和 `reason`。旧标识必须在整个指定区间属于该实体；目标标识不能与不同实体冲突。自动模式同时保留两条时间线中最近的下一边界。一个批次不能重复修改任何标识，包括作为 replacement 出现的标识。

### 3. 手动区间修正

```json
{
  "action":"bindings",
  "operation_id":"handover-correction-001",
  "changes":[{
    "identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},
    "expected_revision":"00000000-0000-4000-8000-000000000002",
    "effective":{"mode":"manual","from":"2026-11-05T10:00:00Z","to":"2026-11-05T11:00:00Z"},
    "change":{"action":"rebind","entity_id":"00000000-0000-4000-8000-000000000003","reason":"迟到的账号交接记录","evidence_id":"handover-42"}
  }]
}
```

示例中的 revision 和 entity_id 必须使用实际值。区间采用 `[from,to)`，`to:null` 或省略表示无上界。输入必须为带时区 RFC3339，精确到微秒；拒绝无时区、非零纳秒精度、闰秒、空区间和反向区间。内部和返回区间统一使用有符号 UTC 微秒整数，字段名以 `_us` 结尾，不经过浮点数中转。

所有手动操作需要修正权限与非空原因，包括手动 `bind`。有限修正保留区间之外的历史和未来；开放结束的手动修正会修改之后全部计划，响应 `replaced` 列出受影响的原片段及原边界。实体普通属性没有历史语义，不能用当前属性解释过去事件。

### 4. 按业务时间与知识版本查询

`POST /resolve`：

```json
{
  "identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},
  "valid_at":"2026-11-05T10:30:00Z",
  "known_at":"latest"
}
```

`known_at` 可以省略、为 `null`、`latest`，或先前收据中的 `knowledge.version` UUID。每次调用固定一份租户快照。返回实际知识版本、时间线 revision、`binding` 和 `stats`；没有有效绑定时 `binding:null`。指定 UUID 不属于本租户当前历史、已因恢复回退而丢失，或从库尚未收到时返回 `KNOWLEDGE_UNAVAILABLE`，不退回最新解释。UUID 是跨重启的业务知识版本；`commit_seq` 仅供复制进度诊断，不能代替知识 UUID。

### 5. 批量事件匹配

`POST /v1/tenants/{tenant}/identity/resolve_many`：

```json
{
  "known_at": "latest",
  "items": [
    {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-05T10:30:00Z"},
    {"identifier":{"kind":"account","namespace":"crm/prod","value":"10086"},"valid_at":"2026-11-05T11:00:00Z"},
    {"identifier":{"kind":"account","namespace":"crm/prod","value":"missing"},"valid_at":"2026-11-05T11:00:00Z"}
  ]
}
```

返回 `knowledge_version`、`history`、`snapshot_seq`、`results` 和整批 `stats`。`results` 与输入等长、顺序相同；每项为 `{ "timeline_revision": UUID或null, "binding": Binding或null }`。重复输入保留重复结果，同一标识不同时间共享时间线读取和解码。区间仍为 `[from,to)`；未命中属于正常结果，时间线不存在时 revision 也为 null。只返回归属和原始绑定证据，不把当前实体属性当作历史属性。

整批只取得一次快照、一次知识头。`known_at` 只能放在顶层，省略/null/latest 固定本次知识头，UUID 固定历史解释。所有输入先校验，任一输入错误、知识不可用、损坏或预算失败都会使整批失败，不返回部分结果。跨批次需要稳定解释时，把首批返回的 `knowledge_version` 放入后续请求；这不会延长保留期，版本过期后仍返回 `KNOWLEDGE_UNAVAILABLE`。`history` / `snapshot_seq` 是复制一致性位置，不能代替业务知识 UUID。

Rust 类型位于 `tenant_kv::identity`：`ResolveItem`、`ResolveManyRequest`、`ResolveMatch`、`ResolveManyResult`。调用 `TenantHandle::identity_resolve_many(&request)`；需要共享截止时间或取消时，使用 `tenant.read_with(&options)?.identity_resolve_many(&request)`。单条 `identity_resolve` 也支持 `TenantReader`，原返回结构保持不变。

HTTP 两个解析端点与 DSL 都支持[读取控制](read-controls.md)及 `X-TKV-Min-History` / `X-TKV-Min-Seq`。指定尚未复制的知识 UUID 会报错；需要等待主库已确认的位置时同时传最低位置。Rust HTTP 客户端提供 `Client::identity_resolve_many(&request, &ClientReadOptions)` 和 `identity_resolve`；`ReadPosition::from(&batch_result)` 可复用批量位置。客户端不会把一批请求拆成多次单条调用。能力标记为 `identity_resolve_many_v1`，应先升级服务端，再启用新客户端/DSL。

每批 1..`max_read_keys` 条，默认 1000。输入、响应和临时内存分别受 `max_batch_bytes`、`max_response_bytes`、`max_query_bytes` 约束，默认 16 MiB、16 MiB、64 MiB。验证、读取和匹配共享默认 5 秒截止时间；历史扫描的 `max_scan` 累计整批物理记录（包括版本和墓碑），默认 100000，不按标识重新计数。latest 使用分组 MultiGet，历史查询使用有界多范围扫描，不进行集合全扫描，因此可与 `allow_full_scan=false` 一起使用。查询阶段按小批保留原始值、逐条解码时间线，输出内存累计计费；超过预算时调用方应拆小批次。

性能对比程序会逐项核对顺序调用、原生批量和 DSL 的结果，并交替执行顺序与批量计时：

```sh
cargo run --locked --release --example identity_resolve_many -- /var/tmp/identity-batch-new 20
```

覆盖 1/32/128/1000 条、不同标识与重复标识、1/64 片段时间线、latest/固定知识，以及启用/禁用块缓存。禁用块缓存仍会命中操作系统页缓存，不能视为真实冷盘测试。实测结果与回归范围见[验证记录](resolve-many-validation-20261109.md)。

## 可信来源和幂等性

v0.7 HTTP 写入口允许 admin、identity_write 和 identity_correct；手动有效区间等纠错需 admin 或 identity_correct。初始化仍仅 admin。来源固定为 `http`，操作者来自凭证配置中的稳定 subject，轮换哈希但保持 subject 不改变重试身份。请求中的 `source`、`actor`、`assurance` 等未知字段会被拒绝。证据引用是该管理员声明可使用的审计引用，结果标记为 `asserted`；本阶段不接第三方证据验证器，不会伪造 `verified`。连接器应在可信宿主中完成来源、规范化及证据授权，然后调用带 `IdentityContext` 的 Rust 接口。该上下文不能直接来自未经授权的 JSON。

kind、namespace、value 使用精确区分大小写的 UTF-8，分别限 64、256、2048 字节且禁止控制字符；不会对所有来源强制转小写。来源连接器负责一致归一化和正确划分 namespace。仅可将可信来源或人工确认的账号归属写入独占映射；IP、共享设备或客户端自报字符串应保留为线索，不能当作已验证身份。

operation_id 在回执保留期内按租户去重，限 128 字节；不能把旧业务操作换新 token 再提交。同一认证操作者和来源以同一规范化请求重试，返回第一次的时间、UUID 和完整结果；不同内容或操作者使用同一 ID 返回 `OPERATION_CONFLICT`。成功返回前，已观察到的原提交必须完成 WAL 同步，包括并发重试命中尚在 group commit 中的收据。`COMMIT_UNKNOWN` 后应先恢复该引擎再以相同 ID 和请求重试，不能把失败响应视为未提交。

恢复到较旧备份或异步故障升主可能丢失之后的操作收据；此时不能保证跨回退恰好一次。丢失的知识 UUID 明确不可用，即使提交序号或知识 ordinal 再次使用，也不会将新历史冒充旧版本。

## 存储、预算与运行边界

同一集合存放 entity、timeline、history、head、knowledge、operation 文档，正文使用现有 DOC1 格式。每次修改保存一份不可变的完整时间线历史，键为标识摘要与降序知识 ordinal；历史查询用有界范围定位最近的一份历史，再在已解码的片段中定位业务时间。当前读取直接 Get 时间线。v0.6.1 对固定结构的时间线和知识记录直接解码为 Rust 类型，省去中间 JSON 树，并复用已有的快照感知集合身份缓存；动态实体属性和收据继续沿用 JSON 解码。阶段一每次仍要解码整条时间线，写入也会保存整份历史，成本随片段数增长；没有宣称实现第二阶段的原生批量前驱查询和覆盖索引。

每条时间线默认最多 1000 段，可在初始化时下调；单记录仍受 1 MiB 默认文档限制。展开后的正文、历史、知识头、受影响实体与收据共同受原子批次条数/字节限制，超限整次拒绝。查询临时内存、扫描条目、返回字节和时间分别沿用租户/全局预算；单次返回的 `stats.document_reads` 计入知识与时间线等文档读取。正常自动操作不扫描全部历史。

未配置租户保留策略时，当前历史分支的知识和收据保留到租户删除或恢复替换。v0.8 配置 retention_seconds 后，旧知识从被覆盖时起计时，回执从提交时起计时；当前知识头、实体与当前完整时间线不失效。边界基线保留规则见 [统一保留](retention.md)。磁盘配额不足时背压，不能无限存储。高频修改、长时间线需要评估写放大和容量，不能把短基准视为长期规模验证。

时钟检查对比进程内单调时钟与墙钟，并校验上次持久记录时间；成功采样会刷新锚点，避免正常渐进校时相对于首次锚点累计漂移。失败采样不移动锚点，也不自动接受大跳变；应校正系统时间，必要时释放句柄后关闭重开租户，使锚点重建，仍受上次持久记录时间约束。超过 `clock_tolerance_ms` 返回 `CLOCK_SKEW`，容忍范围内的并列或微小回退受控递增。进程重启后没有外部可信时源，无法识别所有向前跳变；部署需要正常时间同步。

备份自动包含这些普通物理文件；复制从库回放相同 WAL 时间、UUID 和收据，不重新选择生效时间。主从切换保留已复制知识；异步复制原有 RPO 约束仍然存在。**当前主体查询版本使用复制协议 4，主从必须同时升级；含主体索引及回收数据的目录不能降级交给旧服务写入。** 旧版本不认识受管集合保护。WAL/SST 物理编码仍为 v1，协议版本检查明确拒绝旧节点混跑。

可用 `cargo run --locked --release --example identity_perf -- /var/tmp/identity-perf 1000` 测量 1、10、100、1000 片段下的当前与历史解析成本，原始结果见 [v0.6 验证报告](v0.6-validation.md)，v0.6.1 的同负载前后对比及限制见 [本轮优化报告](v0.6.1-validation.md)。

启用统一保留后，新操作使用 `identity_write_with` 或 HTTP 的 `write_options` 携带首次取得的 retry_token/history。operation_id 仍放在时态请求顶层。凭据与请求不能在重试时刷新；已过期回执不授权重新执行。

## 主体反查和变更历史

主体沿用 `Entity.entity_id`，与认证凭证的 `subject` 无关。新增只读端点：

| 能力 | HTTP | DSL | Rust / HTTP Client |
|---|---|---|---|
| 按主体反查标识 | `POST /v1/tenants/{tenant}/identity/ids` | `ids with $request` | `get_subject_identifiers` |
| 主体身份变更历史 | `POST /v1/tenants/{tenant}/identity/history` | `history with $request` | `get_subject_history` |

反查请求示例：

```json
{"entity_id":"<UUID>","valid_at":"2026-11-09T10:00:00Z","known_at":"latest","kind":"account","namespace":"crm/prod","limit":100}
```

`valid_at` 必填，`kind`、`namespace` 可选，均精确区分大小写。返回 `entity_id`、`knowledge_version`、`history`、`snapshot_seq`、`identifiers`、`cursor`、`stats`。每个匹配项含 `identifier`、`timeline_revision`、`binding`。在相同知识版本和业务时间下，反查返回该主体的标识集合，与逐一 `resolve` 的归属一致；重绑定后仍能在旧知识版本下查询原主体。结果按标识摘要稳定排序，不按 value 字典序排序。不存在的主体返回 `NOT_FOUND`，存在但没有匹配标识时返回空列表；未知、过期或尚未复制的知识 UUID 返回 `KNOWLEDGE_UNAVAILABLE`。

历史请求示例：

```json
{"entity_id":"<UUID>","known_at":"latest","recorded_from":"2026-11-01T00:00:00Z","recorded_to":"2026-12-01T00:00:00Z","limit":100}
```

`recorded_from`、`recorded_to` 可省略，区间为 `[from,to)`，筛选操作的**记录时间**，不是绑定的业务生效时间。返回同样的位置和游标元数据，以及 `events`、`coverage`。事件按知识 ordinal 降序、同一操作内 change_index 升序排列。事件类型包括 `entity_created`、`attributes_updated`、`identifier_bound`、`identifier_rebound`、`identifier_unbound`、`identifier_replaced`。每项包含 event_id、operation_id、完整 knowledge（含来源、操作者和记录时间）、受影响 entity_ids、reason，以及属性或绑定变更。绑定的 before/after 仅包含变更区间内的片段；replace 为一条含两个标识变更的事件。重绑定事件可从原主体和新主体两侧读取，event_id 相同。属性记录实际变化字段，`{present:false,value:null}` 与已存在的 JSON null 区分。同一操作的幂等重试不新增事件；无变化的操作保留原收据，但不生成变更事件。这是变更日志，不提供属性的任意历史时点快照，也不记录未提交的失败尝试。

两个接口默认每页 100 项，范围为 1..`max_page_size`（默认 1000）。使用 `SubjectIdentifiersRequest/Result`、`SubjectHistoryRequest/Result`，通过 `TenantHandle`、`TenantReader` 或异步 HTTP `Client` 调用。每页受读取超时、取消、物理扫描、临时内存、响应字节和最低复制位置限制。过滤和历史跨度较大时应减小页大小。单页最多推进 256 条物理索引记录，也可能在达到返回项数前结束；**空列表加非空 cursor 不代表结束，只有 cursor=null 才结束**。

下一页重复相同筛选、limit、known_at，并提交返回的 cursor；首次 `latest` 在整个游标生命周期固定为一份知识头和快照。DSL 的游标放在顶层 `cursor`，不能放进 `$request`。游标默认 60 秒，绑定租户、认证主体、引擎代际、复制 epoch、筛选和发放节点；与普通查询共享快照/游标名额。成功消费一次，预算失败不消费旧游标；并发消费只有一个成功。恢复或切换历史后失效。HTTP SDK 返回 `tkv1` 包装游标并将后续请求固定到原节点；可用现有 `release_cursor` 提前释放。页面保留在快照中，但逻辑保留期仍可能在翻页间到期，历史事件不会绕过到期校验。

反向索引只存主体与标识的候选关系及知识版本；最终匹配读取同一快照下的权威时间线。关系变化、事件正文、主体事件索引、知识头和幂等收据在**同一个 WAL 事务**提交，复制和检查点携带相同记录，不存在异步漏记窗口。事件正文只保存一份，多主体通过指针关联。所有展开记录和字节计入现有原子批次上限；开启 retention 后也计入到期记录，超限整次回滚。

### 升级与保留

能力标记 `identity_subjects_v1`。本次根注册表版本为 **6**、复制协议为 **4**。启动时先持久升级根注册表再服务，旧程序会拒绝打开；这用于防止旧写入器漏维护新索引。升级前停写并备份，同时更新主从和离线工具；首次升级需要少量可写空间，不能直接降级。WAL/SST 编码未变。

新初始化的身份集合立即可查。既有身份集合在主节点打开后，由维护线程按小事务从保留中的时间线历史重建反向索引，持久记录进度；重启可续建，新写入同时维护新索引。每次迁移最多访问 64 个历史键、写入 64 个 link，以 1 MiB 输入和 10 ms 协作时间预算提前交还执行权；至少允许一个合法大记录取得进展，单个文件 I/O、编码及提交同步不能被该时间预算抢占。条数/字节/内存限制触发的提交前失败会缩小批次重试，WAL 追加或同步结果未知时不自动重放。重建使用维护内存和租户/全局资源计费，不套用客户端的 query_ms/max_query_bytes。

迁移和 link 回收在每个事务内复用范围扫描器，逐行读取而不逐行重开 SST 游标；跨标识跳过已回收尾部时才重新定位。历史 GC 同样流式处理，每步默认最多 64 条物理记录、10 ms 协作预算，较小的原子批次限制会继续缩小实际批次。10 ms 不包含提交后的同步等待，也不承诺前台请求的延迟上界；共享磁盘繁忙时，其他租户的 fsync 尾延迟仍可能上升。

构建期间反查返回 `INDEX_NOT_READY`，不退化为全租户扫描。历史 GC 可先回收已完整迁移的标识；仍在迁移的标识及后续标识继续保留所需时间线。已被保留水位淘汰的版本只需重建一份前驱基线，之后直接跳过该标识的更旧尾部；过期知识仍不可查询。从库只复制这些记录，不独立重建。旧检查点恢复也使用同样流程。link 回收同样按最多 64 行的小事务推进，保留有效知识和正/负前驱基线，完成后在水位不变时不反复写状态；后续扫过已经回收的版本尾部时直接跳至下一主体/标识。

升级前无法可靠补全的审计事件不会被伪造：`coverage.recording_started` 表示开始记录的知识水位，`legacy_history_missing=true` 明示此前历史不完整；新集合为 false。`coverage.retention_enabled=true` 表示事件历史可能已经被截短，即使当前页非空也不能声称完整生命周期审计。

启用租户 retention 后，事件正文和各主体指针分别安排到期任务，避免一次清理大范围纠错时形成超大事务；后启用策略的旧事件也分批补入。反向关系按最低保留知识版本清理，保留一份前驱基线（包括 absent 基线），保证尚可查询知识的反查不被破坏。未配置 retention 时，事件与知识保留到删除租户或替换历史；合规用途需要另外制定留存及备份策略。

回归、Linux 执行范围和高关联数主体的分页实测见[主体验证记录](subject-queries-validation-20261109.md)；本轮批量迁移、历史回收、容量边界及 Rust 1.99.1 验证见[迁移优化验证记录](subject-maintenance-validation-20261109.md)。示例可运行 `cargo run --release --example identity_subjects -- /var/tmp/idkv-subjects-new`，它会核对正反向归属和所有已确认更新对应的事件。

百万行旧历史、多租户持续写入及强制终止后的迁移/GC 续建见[规模验证记录](subject-maintenance-scale-validation-20261109.md)，包含可重复执行的分阶段命令和观测边界。

### 批量写入的升级容量变化

**引入主体事件和反向索引后，原来能成功的批量写入可能超出原子上限。** `max_batch_operations` 仍限制实际展开后的键值修改数，事件、指针、link 和 TTL 元数据均计入；`max_batch_bytes`、文档和内存限制也继续生效。不会把内部记录排除后允许超大 WAL 帧，失败整批回滚，不能将超限当作已经部分提交。

对每个实际改变绑定的 change，记 `T` 为改写的时间线数、`L` 为主体成员变化数、`S` 为该事件关联主体数；整个请求共有 `U` 个主体需要更新 bindings_revision。已初始化集合中的通常计数为：

| 项目 | 未启用 retention | 启用 retention |
|---|---:|---:|
| 每条时间线：当前值 + 历史 | `2T` | `2T` |
| 成员关系 link | `L` | `L` |
| 一份事件 + 主体指针 | `1 + S` | `1 + S` |
| 事件及各指针的到期记录 | 0 | `2(1 + S)` |
| 全请求主体 revision 更新 | `U` | `U` |
| 全请求知识头、知识、收据及相应保留元数据 | 3 | 8 |

合计 `C + U + Σ[2T + L + 1 + S + 2r(1 + S)]`，`r=0/1`，`C=3/8`。迁移状态首次创建和到期游标回退等还可能增加元数据记录；上表不包含单独取得 retry-context 的提交。无效果变更不写事件/时间线，完全匹配的幂等重试不新增记录。replace_identifier 仍是一条事件，但 T、L、S 取它所有实际改变的标识和主体。

在默认 `max_batch_operations=1000`、同一主体的一批全新短标识、其他预算充足时，未启用 retention 是 `5N+4`，最多 **199** 项；启用后是 `9N+9`，最多 **110** 项。200/111 项分别超限，回归已核对整批回滚。多个独立主体、再绑定或属性大时容量还会不同。例如一次完整纠错涉及 300 个旧主体和一个新主体，且 L=U=S=301，启用 retention 后至少需要 **1518** 条修改；一条 change 也可能超出 1000 的限制。

升级前应以真实请求验算，缩小互相独立的批次；需要保持原子性的纠错应先在主从两端协调提高 max_batch_operations（上述例子可从 2000 评估），并同时验证字节、单文档和内存预算，不能盲目拆成多次已确认提交。注册表 v4/v5 首次升到 v6 仍需可写空间；已为 v6 的健康根不会因为这次格式检查反复重写注册表。

### 迁移进度

`GET /v1/tenants/{tenant}/retention` 即使未启用 retention，也返回 `identity_subjects`（未初始化身份集合时为 null）：`ready`、`processed_history_rows`、`written_links`、`gc_scanned_rows`、`gc_removed_rows`。这些计数随维护状态原子持久化，恢复/重启后续计；前两项数量统计迁移工作，不含前台写入，不代表标识总数或总行数。旧元数据新增计数从本版继续观测时起累计，不追溯已完成的工作。可按两次采样差值估算处理速率；未执行昂贵全量计数，不提供虚假的完成百分比。

单租户 status.stats.identity_subjects 和 Prometheus 的 `tkv_open_engines_identity_subjects_*` 使用本节点维护完成后的缓存，不为抓指标打开冷租户或扫描历史；权威持久进度使用上述 retention 接口。历史 recorded_from 为闭下界，遇到第一条更旧的事件即结束，避免连续空页；recorded_to 仍为开上界过滤，尚无独立的记录时间索引。
