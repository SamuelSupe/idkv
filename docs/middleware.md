# 中间件集成契约（v0.10.1）

v0.10 增加[请求级读取控制和主从客户端读取 API](read-controls.md)。新读取客户端要求 X-TKV-Read-Controls: 1，旧节点会明确失败，先完成主从升级。存储及复制格式不变。

v0.10.1 的 Client 读取与批量写入共享一秒的路由缓存，并合并同时发生的发现请求。缓存仅提供路由提示，实际读取仍由服务端校验原始最低位置；写入遇到连接失败、READ_ONLY 或未知提交等可恢复错误会失效缓存，只有原有稳定 operation_id/history 条件允许自动重试。write_context/retry_context 仍执行实时发现；游标继续绑定原节点。受控正文/索引读取在 I/O 排队等待期间也检查取消，未开始的废弃读取跳过执行。

v0.9 在保持现有接口的基础上增加条件 Check、同快照 read_batch、通用数据集发布读取，见 [通用 KV 组合能力](kv-capabilities.md)。注册表 v4、复制协议 3、WAL/SST v1 不变；先升级主从再使用新 API。Rust WriteOp 的穷尽匹配需要处理新增 Check 分支。以下 v0.8 升级规则仍适用于从旧版本迁移。

v0.8.1 是可靠性补丁，保留注册表 v4、复制协议 3、WAL/SST v1 和现有 API，不需要再次转换数据或修改请求字段。建议主从都升级，使两侧具备一致的过期时间判断和复制配额行为；v0.8.0 节点仍存在已修复边界问题。验证范围见 [v0.8.1 验证报告](v0.8.1-validation.md)。

## 升级边界

当前程序的新建及后续注册表发布使用 v5；健康 v4 根的单纯打开和读取不会强制迁移。首次修改注册表后，旧版程序不能直接打开根目录，服务与离线恢复工具须一起更新。`GET /v1/capabilities` 的 storage_registry=5 及 OpenAPI 常量表示程序写出的格式，实际根文件可能仍为 v4。历史状态及已删 UUID 不可复用语义保持，WAL/SST v1 与复制协议 3 不变。

先备份、停止写入并停下两个节点，升级主从程序及 auth 配置，再启动主库、启动从库并检查追平，最后开放写入。v0.8 打开根目录时将注册表升级为 v4；旧程序拒绝打开，不能直接二进制回退。需要回退时使用升级前备份和旧配置。WAL/SST 编码仍为 v1。复制协议升级为 3，双方显式检查，v0.7/v0.8 混跑不受支持；协议不匹配会在传输业务文件或停写旧主前拒绝。

`GET /v1/capabilities` 无需认证，返回软件版本、注册表版本、复制协议与能力名。v0.8 的 WriteOptions/Publication 增加 retry_token，发布消费者改用响应顶层 version；启用回收前须完成客户端迁移。v0.7 的 `Token` 增加必填 subject，`WriteResult` 增加 history，DSL Request 增加 write_options；这是需要调用方适配的 API 版本。

## 带历史的条件写与可靠重试

`_rev` 仍是租户提交序号，恢复旧备份后可能重复。正数 `if_rev` 必须同时提供读取时的历史标识。缺失返回 428/HISTORY_REQUIRED；不匹配返回 409/HISTORY_CONFLICT。`if_rev:0` 保持“仅不存在时新增”的语义；省略则为无条件写入。

Rust 使用 `get_with_context` 获取文档和上下文，然后调用 `write_batch_with`。HTTP 文档读取在 `X-TKV-History` 返回不透明标识；写入回执自身携带 `history`。不要从序号推断历史，也不要在写入前用新的 context 替换旧读结果的 history。

```json
{
  "document": {"_key":"gateway","owner":"platform"},
  "if_rev": 123,
  "write_options": {
    "expected_history": "从原读取响应取得的完整标识",
    "operation_id": "ingest-batch-0000123"
  }
}
```

普通 put/delete/batch 和 DSL 写命令都支持 write_options；DSL 语法本身不变。operation_id 绑定**租户、集合生命周期、已认证 subject**。同 ID 与相同请求重试返回原主键、提交序号和 history；同 ID 不同请求返回 OPERATION_CONFLICT。缺省 `_key` 也不会因重试生成第二条文档。正文、索引和回执同一 WAL 批次持久化。回执随复制和检查点保留；计划切换后可以用原请求重试已存在回执，不产生第二次提交。若旧主未复制的操作在新主不存在，旧 history 会阻止盲目重做。

未配置保留策略时，回执保留到集合删除；配置后按租户统一时长到期，计入租户磁盘预算；删除再创建的同名集合是新的生命周期，不承诺跨该边界去重。启用保留后，新幂等操作需固定 retry_token，见 [统一保留与安全重试](retention.md)。恢复改变历史标识；恢复前未包含在备份里的回执不会被伪装成已经存在。COMMIT_UNKNOWN/连接断开不能等同于“没写入”。HTTP 操作完成后若序列化或响应预算不足，返回 503/RESPONSE_UNAVAILABLE，明确表示已执行，不会把它伪装成提交前的 429；只能用原 operation_id 安全重试。跨历史重做属于调用方重新读取、确认后的新业务操作。

## 稳定主体与权限

```json
{"tokens":[
  {"sha256":"填写64位token哈希","subject":"pipeline-ingest","tenant":"填写租户UUID","role":"write"},
  {"sha256":"填写另一token哈希","subject":"identity-sync","tenant":"填写租户UUID","role":"identity_write"}
]}
```

轮换密钥时保持 subject 不变，可在过渡期同时配置两个哈希，避免重试身份改变。只有管理员能配置 subject；客户端请求不能覆盖它。read 只读，write 增加普通文档写入；identity_write 增加时态写入但不能进行要求纠错权限的手动区间操作，identity_correct 允许此类纠错；admin 保留管理权限；replication 仅访问复制接口。identity 初始化仍要求 admin。时态 HTTP 来源固定为 `http`，actor 为稳定 subject，证据等级仍为 asserted。凭证配置在启动时加载，本版不含在线 IAM/密钥分发系统。v0.6 身份回执使用旧凭证派生 actor 与 http_admin 来源；升级后新 HTTP 主体不能直接复用这些旧操作 ID。升级前先结束未确认的身份操作，或读取旧回执核对；不能假设旧凭证派生 actor 已自动迁移。

## 不可变数据集与 Lookup 发布

v0.9 的 HTTP 发布可省略 lookup，Rust 对应 publish_dataset / publish_dataset_with，Publication.lookup 传空字符串。通用数据集通过 resources/{resource}/read 或 /query 按版本读取。以下带 Lookup 的发布和 execute 用法继续支持。

导入方在新集合准备完整数据、建好索引和 Lookup，然后原子发布：

```text
PUT /v1/tenants/{tenant}/resources/asset_owner
{"version":"调用方生成的非零UUID","collection":"assets_20261105","lookup":"owner","expected_version":null}

GET /v1/tenants/{tenant}/resources/asset_owner

POST /v1/tenants/{tenant}/resources/asset_owner/execute
{"version":"入队时固定的响应顶层version","inputs":[{"ip":"10.0.0.1"}]}
```

首次发布 expected_version 为 null；更新时必须等于当前响应顶层 version，防止两个发布者互相覆盖。请求 version 是发布重试 ID；响应顶层 version 是引擎生成的实际版本。重复相同发布在回执保留期内返回原回执；即使指针后来更新，也不会把旧版本重新发布为当前版本。发布要求集合内所有索引 READY，并在同一事务中冻结该集合的文档、索引、Lookup 与集合删除操作。后续刷新使用新集合。批量执行在一个快照解析指针并查询；省略 version 读取当前发布版本，排队消费者应明确传入入队版本。

历史发布版本跨重启、复制和备份保留，不依赖 60 秒游标租约。未配置保留时旧版本保留到租户删除；配置后，只有被替代版本才从替代时起计时，受集合数量和磁盘配额限制。最后一个保留引用消失后回收冻结集合。对外的 TenantHandle API 执行冻结约束，低层 Engine/存储模块是受信任实现接口，宿主不能把它们交给不可信插件。

## 客户端与一致性

`tenant_kv::client::Client::new` 接受一个或两个节点地址、调用凭证和租户 UUID。写入通过 `/context` 发现主节点；两个节点都可写时拒绝并要求运维隔离。客户端不会自行升主。节点地址只能是 HTTPS origin，HTTP 仅允许回环地址；不跟随重定向。

`write_context()` 提供准备新写入所需的 history。`write_batch()` 仅在同时有 operation_id 和 expected_history 时最多尝试两次，使用原请求。`get(collection,key,Some(&write_result))` 优先读从库，要求其已应用指定写入；发现从库位置落后或节点不可用时可退回主库。请求总预算耗尽、资源超限、取消或历史冲突直接返回。历史变化明确失败，不将旧序号映射到新主的历史。

HTTP 自行集成可在只读请求同时设置 `X-TKV-Min-History` 和 `X-TKV-Min-Seq`，最多等待五秒；通用 KV 读取的一致性等待与执行共享请求截止时间，等待和读取之间也复核历史。未设置时使用节点本地已应用快照，可能有复制延迟。原 epoch + seq 的 replication/wait 仍保留给复制管理使用，不能再把完整 X-TKV-History 当作 epoch UUID。

## 生命周期、预算与观测

`/health` 仅为存活检查。`/ready` 判断可读服务角色与排空状态，`/ready/write` 判断可写主节点；隔离中的节点返回 503。单租户的故障仍通过租户状态单独报告，不关闭其他租户。收到 SIGINT/SIGTERM 后排空活动请求再关闭观测导出；请求中的数据库提交不会因为客户端取消而中途回滚。

HTTP 请求体按实际接收分段预付内存，解析和响应按实际大小计费，不信任 Content-Length。预算同时落到租户与全局；大请求或慢客户端仍受大小、超时和并发限制。指标、OTLP 链路及日志的独立开关与使用方式见 [可观测性](telemetry.md)。

建索引请求遇到租户管理排空可返回 BUSY；已登记的 BUILDING 会在后续打开后自动重建，可通过索引列表观察状态，不把这类中断视为数据回滚或 FAILED。嵌入式应用仍需释放自己长期保留的 TenantHandle；构建协作退出只释放构建自身的引用。

若管理排空因其他句柄失败而继续复用原引擎，恢复工作者也会重新检查中断的 BUILDING；构建退出和恢复缓存发布使用同一把构建锁，避免晚到的检查覆盖失效标记。

内置 HTTP/1 服务默认在请求头或空闲 keep-alive 等待超过 5 秒时关闭连接；Rust Client 默认连接池空闲回收为 4 秒。使用其他客户端或更短的服务端期限时，连接池空闲回收应相应提前。连接关闭竞态不能证明请求未执行，写重试仍须使用原操作 ID 和条件版本；这里没有新增非幂等 POST 的自动重试。
