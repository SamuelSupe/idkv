# 指标、OpenTelemetry 链路与日志

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

v0.10.1 的详细引擎统计 `scheduler.cancelled_jobs` 记录出队时跳过的过期、取消或被放弃的读取任务；`completed_jobs` 只统计实际执行的任务。取消项仍占据有界队列，直到被取出；已开始执行的 I/O 不计入跳过数。`queue_wait_ns` 包含这些跳过任务的排队时间。这组嵌套统计可通过引擎 metrics / 详细 JSON 查看，目前不汇总为 Prometheus 指标。

启用复制时，`/metrics` 增加下列服务实例累计计数，不随冷租户关闭而重置，进程/Service 重建后归零；不使用租户标签。`/v1/replication/status` 提供去掉前缀及 `_total` 的同名字段。计数直接从服务状态读取，Prometheus 抓取不额外获取租户位置。

| Prometheus 指标 | 含义 |
|---|---|
| tkv_replication_initialization_attempts_total | 实际启动的检查点初始化尝试，包含新租户和日志过期重建；重复调度在途任务不计数 |
| tkv_replication_initialization_failures_total | 初始化任务失败并由复制循环收集的次数，包含对端 BUSY/超时等失败；正常停止取消不计数 |
| tkv_replication_reinitializations_total | 本地已有租户因 LOG_GONE 实际启动重建的次数；是尝试数，不是成功数 |

可以在从库对 `increase(tkv_replication_reinitializations_total[15m]) >= 3` 持续 5 分钟告警，并结合失败计数、last_error 及实际源/副本位置检查日志窗口和传输吞吐；阈值需按租户数和部署条件调整。首次安装反复失败时还应监测初始化尝试与失败计数，不能只看重建计数。此处提供告警表达式，不自动安装监控规则。

链路采样为 ParentBased + TraceIdRatioBased，`sample_ratio` 只控制根链路，范围 0–1；上游采样决定会被保留。日志独立于链路采样。HTTP 层不会导出请求/响应正文、DSL 文本、文档键、Authorization 或任意 baggage；日志 subject 来自管理员配置，不接受请求自报。启动/退出消息仍输出到标准错误，数据库请求事件通过此日志开关导出。

队列大小限制为每信号 16–65,536 条，默认 1,024；导出批次最多 256 条，正常每 500 ms 发送。采集端不可用、超时或队列满不会阻塞数据库请求；遥测可能丢弃，操作日志不能代替要求不丢记录的持久审计系统。SDK 队列内存由 queue_size 控制，不包含在引擎 memory_bytes 计数中。导出超时为 100–10,000 ms。SIGINT/SIGTERM 后先停止接收并排空请求，再限时关闭 exporter。HTTP 中断记录为 499；后台已开始的写入仍可能提交，应使用原 operation_id 查询式重试。

嵌入式应用显式创建 `Telemetry`，传给 `http::app_with_runtime`。构造 exporter 和 `shutdown` 在阻塞线程执行；组件不会安装全局 tracer、logger 或 tracing subscriber，不接管宿主日志配置。自定义非 HTTP 调用可用 `Telemetry::begin(...).context()` 传递上下文并在完成后调用 `finish(status)`；默认只自动观测 HTTP 适配器，不声称已经自动覆盖宿主所有 Rust 函数。

依赖接口参考：[官方 Rust SDK](https://opentelemetry.io/docs/languages/rust/)、[固定 OTLP 0.31.1 API](https://docs.rs/opentelemetry-otlp/0.31.1/opentelemetry_otlp/)。本版本选择并固定这组兼容版本，未依赖 latest 文档的接口。
