# 指标、OpenTelemetry 链路与日志

主体维护增加 `tkv_open_engines_identity_subjects_ready_tenants`、`tkv_open_engines_identity_subjects_migrating_tenants`、`tkv_open_engines_identity_subjects_processed_history_rows`、`tkv_open_engines_identity_subjects_written_links`、`tkv_open_engines_identity_subjects_gc_scanned_rows` 和 `tkv_open_engines_identity_subjects_gc_removed_rows`。这些是本节点已观测维护进度的打开租户汇总 gauge：租户关闭时会下降，不应视作进程累计 counter；抓取不打开冷租户、不做存储扫描。按租户查看持久 ready/计数（包括从库收到的状态）使用 `GET /v1/tenants/{tenant}/retention` 的 identity_subjects 字段，缓存 status.stats 可能尚未初始化或落后。以进度差值、持续 not-ready 时长及 maintenance_error 组合判断迁移是否停滞，具体批次与保留边界见[身份文档](identity.md#迁移进度)。

v0.7 的三个开关相互独立，服务启动时读取配置。默认开启指标，关闭链路与日志导出；环境中的 OTEL 配置不会自行开启导出。全关配置为 `{"metrics":false,"traces":false,"logs":false}`。配置修改后重启服务生效。

```sh
tenant-kv serve --root data --auth auth.json --telemetry-config examples/telemetry.json
```

配置示例：

```json
{
  "metrics": true,
  "traces": true,
  "logs": true,
  "service_name": "tenant-kv",
  "endpoint": "http://127.0.0.1:4318",
  "sample_ratio": 0.1,
  "queue_size": 1024,
  "export_timeout_ms": 2000
}
```

`endpoint` 是 Collector 的 HTTP(S) 基址。使用官方 OpenTelemetry Rust SDK 与 OTLP HTTP/JSON exporter，分别发送到 `/v1/traces`、`/v1/logs`。显式端点和超时覆盖环境默认值；Collector 认证可通过 SDK 支持的 `OTEL_EXPORTER_OTLP_HEADERS` 或各信号专用 headers 环境变量配置，勿把凭证放入 URL。HTTP exporter 不跟随重定向。

| 开关 | 能力 | 关闭行为 |
|---|---|---|
| metrics | `GET /metrics` 的 Prometheus 文本，以及 `/v1/metrics` 原有详细 JSON | 两个接口返回 404，不记录请求指标 |
| traces | W3C traceparent/tracestate 传播、HTTP server span、查询/Lookup 的 db.read I/O 事件 | 不创建 tracer provider 或导出线程 |
| logs | 请求完成事件、HTTP 状态、耗时、已认证 subject、trace/span 关联 | 不创建 logger provider 或导出线程 |

两个指标接口都要求全局 admin；不接受租户 token 或复制 token。指标使用路由模板，不使用文档键、原始 URL、subject 或租户名作为标签。HTTP 请求数、错误数、耗时直方图和在途数可用于速率、P95/P99 与错误率；查询/Lookup 的读块、扫描量、正文回读等为进程累计计数。`tkv_open_engines_*` 汇总当前打开引擎的提交、WAL、刷盘、合并、fsync、锁等待、磁盘、快照与 L0 文件数据，使用 gauge：冷租户关闭后这些值可能下降。详细租户统计保留在 JSON 接口；不把累计提交序号误报为复制延迟。

全局回收指标为 `tkv_memory_pressure_active`（刷盘压力，0/1）、`tkv_memory_pressure_cache_active`（数据块缓存准入压力，0/1）、`tkv_memory_pressure_foreground_limit_bytes`、`tkv_memory_pressure_headroom_bytes`、`tkv_memory_pressure_headroom_in_use_bytes`。JSON 对应 `/v1/metrics` 的 `memory_pressure` 对象。headroom_in_use_bytes 是 `max(memory_bytes - foreground_limit_bytes, 0)`，包括仍由 SST 元数据或快照引用保留的计费内存；它不是历史分配量。普通刷盘的预付工作空间计入全局 reserved_bytes，不能把这部分预留直接等同于 RSS；active 和 cache_active 可以不同，缓存可先于刷盘压力恢复。可对保护容量占用持续五分钟且压力仍激活设置告警，结合 RESOURCE_EXHAUSTED、维护错误、快照数、复制延迟判断是否需要释放引用或扩容。短暂占用属于正常回收，不应每次刷盘就告警。

v0.10.1 的详细引擎统计 `scheduler.cancelled_jobs` 记录出队时跳过的过期、取消或被放弃的读取任务；`completed_jobs` 只统计实际执行的任务。取消项仍占据有界队列，直到被取出；已开始执行的 I/O 不计入跳过数。`queue_wait_ns` 包含这些跳过任务的排队时间。这组嵌套统计可通过引擎 metrics / 详细 JSON 查看，目前不汇总为 Prometheus 指标。

启用复制时，`/metrics` 增加下列服务实例累计计数，不随冷租户关闭而重置，进程/Service 重建后归零；不使用租户标签。`/v1/replication/status` 提供去掉前缀及 `_total` 的同名字段。计数直接从服务状态读取，Prometheus 抓取不额外获取租户位置。

| Prometheus 指标 | 含义 |
|---|---|
| tkv_replication_initialization_attempts_total | 实际启动的检查点初始化尝试，包含新租户和日志过期重建；重复调度在途任务不计数 |
| tkv_replication_initialization_failures_total | 初始化任务失败并由复制循环收集的次数，包含对端 BUSY/超时等失败；正常停止取消不计数 |
| tkv_replication_reinitializations_total | 本地已有租户因 LOG_GONE 实际启动重建的次数；是尝试数，不是成功数 |
| tkv_registry_metadata_bytes | 当前持久根注册表大小，包含校验封装 |
| tkv_registry_metadata_capacity_bytes | 根注册表格式上限，当前 16 MiB |
| tkv_registry_deleted_tenants | 保留 UUID 和状态信息的历史删除租户数 |

可对 `tkv_registry_metadata_bytes / tkv_registry_metadata_capacity_bytes > 0.8` 持续五分钟告警，并根据租户周转率提前安排容量管理。v5 压缩历史记录，格式容量仍有界，删除租户不代表删除其身份墓碑。

可以在从库对 `increase(tkv_replication_reinitializations_total[15m]) >= 3` 持续 5 分钟告警，并结合失败计数、last_error 及实际源/副本位置检查日志窗口和传输吞吐；阈值需按租户数和部署条件调整。首次安装反复失败时还应监测初始化尝试与失败计数，不能只看重建计数。此处提供告警表达式，不自动安装监控规则。

复制状态另有 initializing 和 waiting_for_readers 两个租户 UUID 列表。已下载检查点等待句柄释放时保持同一个任务，不重复增加初始化尝试/失败计数；下载前因活动句柄检查失败则计为失败并指数退避。sync_once 返回 Ok 不代表后台初始化已经完成，应结合这些状态及实际位置判断。

链路采样为 ParentBased + TraceIdRatioBased，`sample_ratio` 只控制根链路，范围 0–1；上游采样决定会被保留。日志独立于链路采样。HTTP 层不会导出请求/响应正文、DSL 文本、文档键、Authorization 或任意 baggage；日志 subject 来自管理员配置，不接受请求自报。启动/退出消息仍输出到标准错误，数据库请求事件通过此日志开关导出。

队列大小限制为每信号 16–65,536 条，默认 1,024；导出批次最多 256 条，正常每 500 ms 发送。采集端不可用、超时或队列满不会阻塞数据库请求；遥测可能丢弃，操作日志不能代替要求不丢记录的持久审计系统。SDK 队列内存由 queue_size 控制，不包含在引擎 memory_bytes 计数中。导出超时为 100–10,000 ms。SIGINT/SIGTERM 后先停止接收并排空请求，再限时关闭 exporter。HTTP 中断记录为 499；后台已开始的写入仍可能提交，应使用原 operation_id 查询式重试。

嵌入式应用显式创建 `Telemetry`，传给 `http::app_with_runtime`。构造 exporter 和 `shutdown` 在阻塞线程执行；组件不会安装全局 tracer、logger 或 tracing subscriber，不接管宿主日志配置。自定义非 HTTP 调用可用 `Telemetry::begin(...).context()` 传递上下文并在完成后调用 `finish(status)`；默认只自动观测 HTTP 适配器，不声称已经自动覆盖宿主所有 Rust 函数。

依赖接口参考：[官方 Rust SDK](https://opentelemetry.io/docs/languages/rust/)、[固定 OTLP 0.31.1 API](https://docs.rs/opentelemetry-otlp/0.31.1/opentelemetry_otlp/)。本版本选择并固定这组兼容版本，未依赖 latest 文档的接口。

读取计数和 `db.read` trace 事件只接收执行器返回的类型化 ReadStats（查询、Lookup、组合/发布读取及身份解析），不再从响应 JSON 推断。普通 Get/MultiGet 与 DSL Get 返回的用户 `stats` 或 `result.stats` 字段仅作为业务数据返回。累计读取计数使用饱和加法，达到 u64 上限后不回绕。

`tkv_wal_corrupt_recoveries_total` 统计拒绝恢复的 WAL 帧校验错误；`tkv_wal_torn_tail_recoveries_total` 统计成功重写不完整尾部的次数。两者为进程累计计数，错误日志带 WAL UUID、偏移及证据位置；长度残缺恢复保留一份 TORN-TAIL 后缀，CRC 错误仍保留完整 .corrupt 原件。查询/Lookup 读取计数表示已发生的查询工作，即使最后因 history 变化拒绝响应仍计入；成功/失败响应数量使用 HTTP 指标区分。

启用远端备份服务时，`/metrics` 提供 `tkv_backup_prune_clock_unsafe_total`：因异常清单时间拒绝 prune 的任务累计数，BackupService 重建后归零，无租户标签。可对 `increase(tkv_backup_prune_clock_unsafe_total[15m]) > 0` 告警；日志包含 tenant/job UUID，任务保留 `BACKUP_CLOCK_UNSAFE` 错误。核对恢复主机时间与清单后再重试，不能为消除告警盲目删除备份；verify/restore 不受此保护性拒绝影响。
