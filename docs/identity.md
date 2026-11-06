# 时态身份：第一阶段

v0.6.0 提供受管理的身份写入和单标识基础解析，用于回答“这个来源标识在事件发生时属于谁”和“按照当时已知的信息会怎样判断”。

当前只有独占 `identifies` 关系。每租户保留集合名 `identity_temporal`，内部仍使用该租户原有 WAL、LSM、备份和复制链路。普通 CRUD、DSL 写入、集合删除、索引及 Lookup 定义修改不能修改这个集合。普通授权读取可以查看其中的审计记录。嵌入式 `Engine` 原始键值接口属于可信宿主代码，不是隔离恶意进程内调用者的安全边界。

本阶段没有原生 `resolve_many`、反向身份展开、固定知识分页、Log/Trace/RUM 查询协调器、多值关系、实体删除或全量 `replace_bindings`。批量写入采用增量 merge；未列出的标识保持原状。

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

## 可信来源和幂等性

v0.7 HTTP 写入口允许 admin、identity_write 和 identity_correct；手动有效区间等纠错需 admin 或 identity_correct。初始化仍仅 admin。来源固定为 `http`，操作者来自凭证配置中的稳定 subject，轮换哈希但保持 subject 不改变重试身份。请求中的 `source`、`actor`、`assurance` 等未知字段会被拒绝。证据引用是该管理员声明可使用的审计引用，结果标记为 `asserted`；本阶段不接第三方证据验证器，不会伪造 `verified`。连接器应在可信宿主中完成来源、规范化及证据授权，然后调用带 `IdentityContext` 的 Rust 接口。该上下文不能直接来自未经授权的 JSON。

kind、namespace、value 使用精确区分大小写的 UTF-8，分别限 64、256、2048 字节且禁止控制字符；不会对所有来源强制转小写。来源连接器负责一致归一化和正确划分 namespace。仅可将可信来源或人工确认的账号归属写入独占映射；IP、共享设备或客户端自报字符串应保留为线索，不能当作已验证身份。

operation_id 在回执保留期内按租户去重，限 128 字节；不能把旧业务操作换新 token 再提交。同一认证操作者和来源以同一规范化请求重试，返回第一次的时间、UUID 和完整结果；不同内容或操作者使用同一 ID 返回 `OPERATION_CONFLICT`。成功返回前，已观察到的原提交必须完成 WAL 同步，包括并发重试命中尚在 group commit 中的收据。`COMMIT_UNKNOWN` 后应先恢复该引擎再以相同 ID 和请求重试，不能把失败响应视为未提交。

恢复到较旧备份或异步故障升主可能丢失之后的操作收据；此时不能保证跨回退恰好一次。丢失的知识 UUID 明确不可用，即使提交序号或知识 ordinal 再次使用，也不会将新历史冒充旧版本。

## 存储、预算与运行边界

同一集合存放 entity、timeline、history、head、knowledge、operation 文档，正文使用现有 DOC1 格式。每次修改保存一份不可变的完整时间线历史，键为标识摘要与降序知识 ordinal；历史查询用有界范围定位最近的一份历史，再在已解码的片段中定位业务时间。当前读取直接 Get 时间线。v0.6.1 对固定结构的时间线和知识记录直接解码为 Rust 类型，省去中间 JSON 树，并复用已有的快照感知集合身份缓存；动态实体属性和收据继续沿用 JSON 解码。阶段一每次仍要解码整条时间线，写入也会保存整份历史，成本随片段数增长；没有宣称实现第二阶段的原生批量前驱查询和覆盖索引。

每条时间线默认最多 1000 段，可在初始化时下调；单记录仍受 1 MiB 默认文档限制。展开后的正文、历史、知识头、受影响实体与收据共同受原子批次条数/字节限制，超限整次拒绝。查询临时内存、扫描条目、返回字节和时间分别沿用租户/全局预算；单次返回的 `stats.document_reads` 计入知识与时间线等文档读取。正常自动操作不扫描全部历史。

未配置租户保留策略时，当前历史分支的知识和收据保留到租户删除或恢复替换。v0.8 配置 retention_seconds 后，旧知识从被覆盖时起计时，回执从提交时起计时；当前知识头、实体与当前完整时间线不失效。边界基线保留规则见 [统一保留](retention.md)。磁盘配额不足时背压，不能无限存储。高频修改、长时间线需要评估写放大和容量，不能把短基准视为长期规模验证。

时钟检查对比进程内单调时钟与墙钟，并校验上次持久记录时间；超过 `clock_tolerance_ms` 返回 `CLOCK_SKEW`，容忍范围内的并列或微小回退受控递增。进程重启后没有外部可信时源，无法识别所有向前跳变；部署需要正常时间同步。

备份自动包含这些普通物理文件；复制从库回放相同 WAL 时间、UUID 和收据，不重新选择生效时间。主从切换保留已复制知识；异步复制原有 RPO 约束仍然存在。**当前 v0.8 要求主从两端都升级到 v0.8，复制协议为 3；含回收数据的目录不能降级交给旧服务写入。** 旧版本不认识受管集合保护。WAL/SST 物理编码仍为 v1，协议版本检查明确拒绝旧节点混跑。

可用 `cargo run --locked --release --example identity_perf -- /var/tmp/identity-perf 1000` 测量 1、10、100、1000 片段下的当前与历史解析成本，原始结果见 [v0.6 验证报告](v0.6-validation.md)，v0.6.1 的同负载前后对比及限制见 [本轮优化报告](v0.6.1-validation.md)。

启用统一保留后，新操作使用 `identity_write_with` 或 HTTP 的 `write_options` 携带首次取得的 retry_token/history。operation_id 仍放在时态请求顶层。凭据与请求不能在重试时刷新；已过期回执不授权重新执行。
